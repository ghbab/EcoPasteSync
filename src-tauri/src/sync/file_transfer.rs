//! 远端文件传输引擎。
//!
//! 设计要点（与旧版「缓存预落盘」完全不同）：
//! - 不设全局缓存目录：文件直接传输到用户点粘贴时前台资源管理器所在的目录；
//! - 每个文件先写同目录临时文件（`原名.EcoPasteSync.tmp`），校验通过后再原子改名；
//! - 断点续传：临时文件 + [`transfer_jobs`] 里的 `done` 字节能精确恢复进度；
//! - 一次只跑一个任务，其余排队；支持暂停、取消；
//! - 本地副本复用：上次落盘的文件若未被修改，再次粘贴时直接本地复制，不再走网络；
//! - 完成后把远端条目的创建 / 修改时间还原到本地文件。
//!
//! 崩溃恢复：应用启动时调用 [`cleanup_on_startup`] 清理未完成任务的临时文件，
//! 只删除后缀固定为 [`TMP_SUFFIX`] 的文件，不会误删用户文件。

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tauri::{AppHandle, Emitter, Manager};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::core::{AppError, Result};
use crate::db::models::ClipboardItem;
use crate::sync::client;
use crate::sync::devices;
use crate::sync::file_state::{self, SyncFileRecord, SyncFileState};
use crate::sync::file_time;
use crate::sync::protocol::{SyncFileEntryDto, FILE_CHUNK_SIZE};
use crate::sync::transfer_jobs::{
    self, TransferFileJournal, TransferJobRecord, STATUS_CANCELLED, STATUS_DONE, STATUS_FAILED,
    STATUS_PAUSED, STATUS_QUEUED, STATUS_RUNNING, STATUS_SKIPPED, TMP_SUFFIX,
};

pub use crate::sync::protocol::EVENT_FILE_PROGRESS;

/// 文件名保留的最大字符数，避免中文等多字节字符超出文件系统 255 字节限制。
const MAX_FILE_NAME_CHARS: usize = 120;
/// 进度事件最小间隔：太频繁会拖慢 WebView。
const PROGRESS_INTERVAL: Duration = Duration::from_millis(200);
/// 暂停时的轮询间隔。
const PAUSE_POLL_INTERVAL: Duration = Duration::from_millis(200);

/// 待执行任务队列（job_id）。
static QUEUE: LazyLock<Mutex<VecDeque<String>>> = LazyLock::new(|| Mutex::new(VecDeque::new()));
/// 是否已有 worker 在消费队列。
static WORKER_RUNNING: AtomicBool = AtomicBool::new(false);
/// 每个任务的控制开关（暂停 / 取消）。
static CONTROLS: LazyLock<Mutex<HashMap<String, JobControl>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
/// 每个任务的最近一次状态快照，供传输窗口重建后主动拉取。
static SNAPSHOTS: LazyLock<Mutex<HashMap<String, TransferSnapshot>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
/// 最近一次产生快照的任务 id：窗口刚创建、事件已经错过时用它兜底拉取。
static LATEST_JOB: LazyLock<Mutex<Option<String>>> = LazyLock::new(|| Mutex::new(None));

/// 任务运行控制。
#[derive(Debug, Clone, Copy, Default)]
struct JobControl {
    paused: bool,
    cancelled: bool,
}

/// 传输窗口展示所需的状态快照。
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferSnapshot {
    pub job_id: String,
    pub item_id: String,
    /// queued / running / paused / failed / done / cancelled / conflicts。
    pub status: String,
    pub target_dir: String,
    /// 当前正在传输的文件名。
    pub file_name: Option<String>,
    /// 当前文件在本次任务中的序号（从 1 开始）。
    pub file_index: usize,
    pub file_count: usize,
    pub transferred_bytes: u64,
    pub total_bytes: u64,
    /// 瞬时速度（字节/秒）。
    pub speed_bps: u64,
    pub error: Option<String>,
    /// 与目标目录同名的顶层路径列表（等待用户选择覆盖策略）。
    pub conflicts: Vec<String>,
    /// 兼容旧前端进度条：任务是否已结束。
    pub done: bool,
}

/// 发生冲突时用户选择的处理方式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictPolicy {
    /// 全部覆盖（同名文件替换，同名目录合并）。
    Overwrite,
    /// 全部跳过（已存在的顶层项不再传输）。
    Skip,
    /// 全部保留两者（自动追加 `(2)` 序号）。
    KeepBoth,
    /// 逐项：`overwrite` 列表里的覆盖，其余保留两者。
    Custom,
}

impl ConflictPolicy {
    /// 把前端传入的标签解析为策略。
    pub fn from_tag(tag: &str) -> Option<Self> {
        match tag {
            "overwrite" => Some(Self::Overwrite),
            "skip" => Some(Self::Skip),
            "keepBoth" => Some(Self::KeepBoth),
            "custom" => Some(Self::Custom),
            _ => None,
        }
    }
}

/// 本地副本指纹：用于判断上次落盘的文件是否被用户改过。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalManifestEntry {
    /// 远端文件的绝对路径，与清单里的 `path` 对应。
    pub remote_path: String,
    /// 上次落盘到的本地绝对路径。
    pub local_path: String,
    pub size: u64,
    /// 落盘时设置的修改时间（Unix 毫秒）。
    pub mtime_ms: Option<i64>,
}

/// 单个顶层条目的归组结果。
struct TopItem<'a> {
    remote_root: String,
    is_dir: bool,
    complete: bool,
    entries: Vec<&'a SyncFileEntryDto>,
}

/// 计划阶段的结果。
struct PlannedJob {
    files: Vec<TransferFileJournal>,
    total_bytes: u64,
    /// 与目标目录同名的顶层路径；非空表示需要用户先决定冲突策略。
    conflicts: Vec<String>,
}

// ---- 对外入口 ----

/// 判断条目是否是远端 files 条目；返回来源设备 id。
pub async fn remote_device_of(app: &AppHandle, item: &ClipboardItem) -> Result<Option<String>> {
    if item.kind != crate::db::models::ClipboardKind::Files {
        return Ok(None);
    }

    let pool = devices::pool(app).await;
    let record = file_state::find(&pool, &item.id).await?;

    Ok(record.map(|record| record.device_id))
}

/// 粘贴触发一次文件传输。
///
/// 返回任务 id；任务可能处于 `conflicts` 状态（目标目录存在同名项），
/// 此时需要前端调用 [`resolve_conflicts`] 后再执行。
pub async fn start_paste_transfer(
    app: &AppHandle,
    item: &ClipboardItem,
    target_dir: PathBuf,
) -> Result<String> {
    let pool = devices::pool(app).await;
    let Some(record) = file_state::find(&pool, &item.id).await? else {
        return Err(AppError::Other(anyhow::anyhow!(
            "clipboard item is not from a synced device"
        )));
    };
    if record.state == SyncFileState::Unavailable {
        return Err(AppError::Other(anyhow::anyhow!(
            "remote file manifest is unavailable"
        )));
    }

    let files = decode_manifest(&record)?;
    let manifest = decode_local_manifest(record.local_manifest_json.as_deref());

    // 先按「未知策略」规划一次，只为了找出冲突项；无冲突则直接按保留两者执行
    // （没有同名文件时保留两者与覆盖结果完全一致）。
    let preview = plan_job(
        &item.content,
        &files,
        &manifest,
        &target_dir,
        None,
        &HashSet::new(),
    )?;

    let job_id = uuid::Uuid::new_v4().to_string();
    let file_count = preview.files.iter().filter(|file| !file.is_dir).count();

    if preview.conflicts.is_empty() {
        let job = TransferJobRecord {
            job_id: job_id.clone(),
            item_id: item.id.clone(),
            device_id: record.device_id.clone(),
            target_dir: target_dir.to_string_lossy().into_owned(),
            status: STATUS_QUEUED.to_owned(),
            total_bytes: preview.total_bytes,
            done_bytes: 0,
            files: preview.files,
        };
        transfer_jobs::insert(&pool, &job).await?;

        let snapshot = TransferSnapshot {
            job_id: job_id.clone(),
            item_id: item.id.clone(),
            status: STATUS_QUEUED.to_owned(),
            target_dir: job.target_dir.clone(),
            file_name: None,
            file_index: 0,
            file_count,
            transferred_bytes: 0,
            total_bytes: job.total_bytes,
            speed_bps: 0,
            error: None,
            conflicts: Vec::new(),
            done: false,
        };
        store_snapshot(&snapshot);
        emit_snapshot(app, &snapshot);

        enqueue(app, job_id.clone());
    } else {
        // 冲突态：也要落库，用户可能在解决前关闭窗口。
        let job = TransferJobRecord {
            job_id: job_id.clone(),
            item_id: item.id.clone(),
            device_id: record.device_id.clone(),
            target_dir: target_dir.to_string_lossy().into_owned(),
            status: "conflicts".to_owned(),
            total_bytes: preview.total_bytes,
            done_bytes: 0,
            files: preview.files,
        };
        transfer_jobs::insert(&pool, &job).await?;

        let snapshot = TransferSnapshot {
            job_id: job_id.clone(),
            item_id: item.id.clone(),
            status: "conflicts".to_owned(),
            target_dir: job.target_dir.clone(),
            file_name: None,
            file_index: 0,
            file_count,
            transferred_bytes: 0,
            total_bytes: job.total_bytes,
            speed_bps: 0,
            error: None,
            conflicts: preview.conflicts,
            done: false,
        };
        store_snapshot(&snapshot);
        emit_snapshot(app, &snapshot);

        // 冲突态不排队，等待 resolve_conflicts。
    }

    // 任务创建后立即显示传输窗口；窗口创建失败不影响传输本身。
    if let Err(err) = crate::window::show_file_transfer_window(app) {
        log::warn!("sync transfer: show transfer window failed: {err}");
    }

    Ok(job_id)
}

/// 用户在冲突窗口里做出选择后继续执行。
///
/// `policy` 取 `overwrite` / `skip` / `keepBoth` / `custom`；
/// `custom` 时 `overwrite` 参数列出需要覆盖的冲突顶层路径，其余保留两者。
pub async fn resolve_conflicts(
    app: &AppHandle,
    job_id: &str,
    policy_tag: &str,
    overwrite: &[String],
) -> Result<()> {
    let Some(policy) = ConflictPolicy::from_tag(policy_tag) else {
        return Err(AppError::Other(anyhow::anyhow!("unknown conflict policy")));
    };

    let pool = devices::pool(app).await;
    let Some(job) = transfer_jobs::find(&pool, job_id).await? else {
        return Err(AppError::Other(anyhow::anyhow!("transfer job not found")));
    };
    if job.status != "conflicts" {
        return Err(AppError::Other(anyhow::anyhow!(
            "transfer job is not waiting for conflicts"
        )));
    }

    let Some(record) = file_state::find(&pool, &job.item_id).await? else {
        return Err(AppError::Other(anyhow::anyhow!(
            "sync file state is missing"
        )));
    };
    let files = decode_manifest(&record)?;
    let manifest = decode_local_manifest(record.local_manifest_json.as_deref());
    let target_dir = PathBuf::from(&job.target_dir);

    let overwrite_set: HashSet<String> = overwrite.iter().cloned().collect();
    let remote_content = remote_content_of(app, &job.item_id).await?;
    let planned = plan_job(
        &remote_content,
        &files,
        &manifest,
        &target_dir,
        Some(policy),
        &overwrite_set,
    )?;

    // 用新计划覆盖 journal（临时文件路径可能因改名策略变化）。
    let updated = TransferJobRecord {
        job_id: job.job_id.clone(),
        item_id: job.item_id.clone(),
        device_id: job.device_id.clone(),
        target_dir: job.target_dir.clone(),
        status: STATUS_QUEUED.to_owned(),
        total_bytes: planned.total_bytes,
        done_bytes: 0,
        files: planned.files,
    };

    // 全部冲突都被跳过、且没有其它待传文件时，任务没有任何实际内容。
    // 直接清理任务记录与临时文件并结束，不进入排队和进度窗口，避免进度条一闪而过。
    if updated.files.is_empty() {
        remove_temp_files(&job).await;
        transfer_jobs::delete(&pool, job_id).await?;
        set_snapshot_status(app, job_id, STATUS_SKIPPED, None);
        return Ok(());
    }

    sqlx::query(
        "UPDATE sync_transfer_jobs SET status = ?, total_bytes = ?, done_bytes = 0, \
         files_json = ?, updated_at = ? WHERE job_id = ?",
    )
    .bind(STATUS_QUEUED)
    .bind(updated.total_bytes as i64)
    .bind(serde_json::to_string(&updated.files).map_err(|err| AppError::Other(err.into()))?)
    .bind(chrono::Utc::now())
    .bind(&updated.job_id)
    .execute(&pool)
    .await
    .map_err(|err| AppError::Other(anyhow::anyhow!("update transfer job failed: {err}")))?;

    set_snapshot_status(app, job_id, STATUS_QUEUED, None);
    if let Err(err) = crate::window::show_file_transfer_window(app) {
        log::warn!("sync transfer: show transfer window failed: {err}");
    }
    enqueue(app, job_id.to_owned());

    Ok(())
}

/// 暂停 / 继续任务。
pub async fn set_paused(app: &AppHandle, job_id: &str, paused: bool) -> Result<()> {
    {
        let mut controls = CONTROLS.lock().expect("transfer controls poisoned");
        let control = controls.entry(job_id.to_owned()).or_default();
        control.paused = paused;
    }

    let status = if paused {
        STATUS_PAUSED
    } else {
        STATUS_RUNNING
    };
    let pool = devices::pool(app).await;
    transfer_jobs::set_status(&pool, job_id, status).await?;
    set_snapshot_status(app, job_id, status, None);

    Ok(())
}

/// 取消任务：停止传输、删除临时文件、清除 journal。
pub async fn cancel(app: &AppHandle, job_id: &str) -> Result<()> {
    {
        let mut controls = CONTROLS.lock().expect("transfer controls poisoned");
        let control = controls.entry(job_id.to_owned()).or_default();
        control.cancelled = true;
    }

    // 已结束的任务直接删除记录即可；运行中的任务由 worker 在检查点自行退出。
    let pool = devices::pool(app).await;
    let Some(job) = transfer_jobs::find(&pool, job_id).await? else {
        return Ok(());
    };

    if job.status != STATUS_RUNNING && job.status != STATUS_PAUSED {
        remove_temp_files(&job).await;
        transfer_jobs::delete(&pool, job_id).await?;
        set_snapshot_status(app, job_id, STATUS_CANCELLED, None);
    }

    Ok(())
}

/// 读取任务快照（传输窗口重建后主动拉取）。
pub fn snapshot(job_id: &str) -> Option<TransferSnapshot> {
    let snapshots = SNAPSHOTS.lock().expect("transfer snapshots poisoned");
    snapshots.get(job_id).cloned()
}

/// 读取最近一次任务快照：窗口首帧加载时用它补齐可能错过的事件。
pub fn latest_snapshot() -> Option<TransferSnapshot> {
    let latest = {
        let job = LATEST_JOB.lock().expect("transfer latest job poisoned");
        job.clone()
    };
    match latest {
        Some(job_id) => snapshot(&job_id),
        None => None,
    }
}

/// 失败后改存到其它目录（或目标目录不可写时的兜底入口）。
pub async fn retry_with_dir(app: &AppHandle, job_id: &str, dir: PathBuf) -> Result<()> {
    let pool = devices::pool(app).await;
    let Some(job) = transfer_jobs::find(&pool, job_id).await? else {
        return Err(AppError::Other(anyhow::anyhow!("transfer job not found")));
    };
    let Some(record) = file_state::find(&pool, &job.item_id).await? else {
        return Err(AppError::Other(anyhow::anyhow!(
            "sync file state is missing"
        )));
    };

    let files = decode_manifest(&record)?;
    let manifest = decode_local_manifest(record.local_manifest_json.as_deref());
    let remote_content = remote_content_of(app, &job.item_id).await?;
    // 改存目录通常希望保留已有文件，因此默认「保留两者」。
    let planned = plan_job(
        &remote_content,
        &files,
        &manifest,
        &dir,
        Some(ConflictPolicy::KeepBoth),
        &HashSet::new(),
    )?;

    remove_temp_files(&job).await;
    let target_dir = dir.to_string_lossy().into_owned();
    sqlx::query(
        "UPDATE sync_transfer_jobs SET target_dir = ?, status = ?, total_bytes = ?, \
         done_bytes = 0, files_json = ?, updated_at = ? WHERE job_id = ?",
    )
    .bind(&target_dir)
    .bind(STATUS_QUEUED)
    .bind(planned.total_bytes as i64)
    .bind(serde_json::to_string(&planned.files).map_err(|err| AppError::Other(err.into()))?)
    .bind(chrono::Utc::now())
    .bind(&job.job_id)
    .execute(&pool)
    .await
    .map_err(|err| AppError::Other(anyhow::anyhow!("update transfer job failed: {err}")))?;

    {
        let mut controls = CONTROLS.lock().expect("transfer controls poisoned");
        controls.insert(job.job_id.clone(), JobControl::default());
    }

    set_snapshot_status(app, &job.job_id, STATUS_QUEUED, None);
    update_snapshot_target(
        app,
        &job.job_id,
        &target_dir,
        planned.files.len(),
        planned.total_bytes,
    );
    enqueue(app, job.job_id.clone());

    Ok(())
}

/// 启动时清理上次异常退出留下的临时文件与任务记录。
pub async fn cleanup_on_startup(app: &AppHandle) -> Result<()> {
    let pool = devices::pool(app).await;
    let removed = transfer_jobs::cleanup_interrupted(&pool).await?;
    if removed > 0 {
        log::info!("sync transfer: cleaned {removed} interrupted transfer jobs");
    }
    Ok(())
}

// ---- 计划构建 ----

/// 把清单按顶层路径归组。
fn group_tops<'a>(remote_content: &str, files: &'a [SyncFileEntryDto]) -> Vec<TopItem<'a>> {
    let mut roots: Vec<String> = Vec::new();
    for line in remote_content.split('\n') {
        if line.is_empty() {
            continue;
        }
        roots.push(line.to_owned());
    }

    let mut groups: Vec<TopItem<'a>> = Vec::with_capacity(roots.len());
    for root in roots.iter() {
        groups.push(TopItem {
            remote_root: root.clone(),
            is_dir: false,
            complete: true,
            entries: Vec::new(),
        });
    }

    for entry in files {
        let mut target: Option<usize> = None;
        if let Some(root_index) = entry.root_index {
            if root_index < groups.len() {
                target = Some(root_index);
            }
        }
        if target.is_none() {
            for (index, group) in groups.iter().enumerate() {
                if group.remote_root == entry.path {
                    target = Some(index);
                    break;
                }
            }
        }
        let Some(index) = target else {
            continue;
        };
        groups[index].entries.push(entry);
    }

    for group in groups.iter_mut() {
        for entry in &group.entries {
            if entry.is_dir && entry.rel_path.is_none() {
                group.is_dir = true;
                // 旧版本发送端没有 complete 字段，按不完整处理。
                group.complete = entry.complete.unwrap_or(false);
            }
        }
    }

    groups
}

/// 构建一次传输计划。
///
/// `policy = None` 时只做冲突探测：计划里的路径按「保留两者」生成，
/// 同时把冲突的顶层路径收集到 `conflicts`，由调用方决定是否等待用户选择。
fn plan_job(
    remote_content: &str,
    files: &[SyncFileEntryDto],
    manifest: &[LocalManifestEntry],
    target_dir: &Path,
    policy: Option<ConflictPolicy>,
    overwrite: &HashSet<String>,
) -> Result<PlannedJob> {
    let groups = group_tops(remote_content, files);
    if groups.is_empty() {
        return Err(AppError::Other(anyhow::anyhow!(
            "remote file manifest is empty"
        )));
    }

    // 本地副本索引：远端路径 → 上次落盘信息。
    let mut local_index: HashMap<String, LocalManifestEntry> = HashMap::new();
    for entry in manifest {
        local_index.insert(entry.remote_path.clone(), entry.clone());
    }

    let mut used_top_names: HashSet<String> = HashSet::new();
    let mut planned_files: Vec<TransferFileJournal> = Vec::new();
    let mut conflicts: Vec<String> = Vec::new();
    let mut total_bytes: u64 = 0;

    for (index, group) in groups.iter().enumerate() {
        // 顶层名称：远端路径的文件名（目录名同样适用）。
        let base_name = top_base_name(&group.remote_root, index);
        let preferred = target_dir.join(&base_name);
        let preferred_text = preferred.to_string_lossy().into_owned();
        let exists = preferred.exists();

        let mut skip_group = false;
        let final_top: PathBuf;

        if !exists {
            final_top = preferred;
        } else {
            // 命中冲突：policy 为空时先记录下来，路径暂时按「保留两者」生成。
            conflicts.push(preferred_text.clone());

            let decision = policy.unwrap_or(ConflictPolicy::KeepBoth);
            let use_overwrite = match decision {
                ConflictPolicy::Overwrite => true,
                ConflictPolicy::Custom => overwrite.contains(&preferred_text),
                ConflictPolicy::Skip => {
                    skip_group = true;
                    false
                }
                ConflictPolicy::KeepBoth => false,
            };

            if use_overwrite {
                final_top = preferred;
            } else {
                final_top = unique_top_path(target_dir, &base_name, &mut used_top_names);
            }
        }

        if skip_group {
            continue;
        }

        let final_top_text = final_top.to_string_lossy().into_owned();

        if group.is_dir && !group.complete {
            return Err(AppError::Other(anyhow::anyhow!(
                "remote directory is not fully enumerated: {}",
                group.remote_root
            )));
        }

        if !group.is_dir {
            // 顶层普通文件。
            let Some(entry) = group
                .entries
                .iter()
                .find(|entry| !entry.is_dir && entry.rel_path.is_none())
            else {
                return Err(AppError::Other(anyhow::anyhow!(
                    "file fingerprint is missing: {}",
                    group.remote_root
                )));
            };
            let Some(size) = entry.size.filter(|size| *size >= 0) else {
                return Err(AppError::Other(anyhow::anyhow!(
                    "file size is unknown: {}",
                    entry.path
                )));
            };

            let planned = build_file_plan(
                &local_index,
                &entry.path,
                size as u64,
                &final_top,
                entry.hash.clone(),
                entry.mtime_ms,
                entry.ctime_ms,
                true,
            );
            total_bytes = total_bytes.saturating_add(planned.size);
            planned_files.push(planned);
            continue;
        }

        // 目录：先建目录记录，再逐个映射子树文件。
        let dir_root = group
            .entries
            .iter()
            .find(|entry| entry.is_dir && entry.rel_path.is_none());

        planned_files.push(TransferFileJournal {
            source: group.remote_root.clone(),
            remote_path: group.remote_root.clone(),
            tmp: String::new(),
            final_path: final_top_text.clone(),
            size: 0,
            done: 0,
            is_dir: true,
            from_local: false,
            is_top: true,
            hash: None,
            mtime_ms: dir_root.and_then(|entry| entry.mtime_ms),
            ctime_ms: dir_root.and_then(|entry| entry.ctime_ms),
        });

        let mut mapper = RelPathMapper::new();
        for entry in &group.entries {
            let Some(rel_path) = entry.rel_path.as_deref() else {
                continue;
            };
            let mapped = if entry.is_dir {
                mapper.map_dir(rel_path)
            } else {
                mapper.map(rel_path)
            };
            let Some(relative) = mapped else {
                return Err(AppError::Other(anyhow::anyhow!(
                    "invalid relative path from remote: {rel_path}"
                )));
            };
            let dest = final_top.join(&relative);

            if entry.is_dir {
                planned_files.push(TransferFileJournal {
                    source: entry.path.clone(),
                    remote_path: entry.path.clone(),
                    tmp: String::new(),
                    final_path: dest.to_string_lossy().into_owned(),
                    size: 0,
                    done: 0,
                    is_dir: true,
                    from_local: false,
                    is_top: false,
                    hash: None,
                    mtime_ms: entry.mtime_ms,
                    ctime_ms: entry.ctime_ms,
                });
                continue;
            }

            let Some(size) = entry.size.filter(|size| *size >= 0) else {
                return Err(AppError::Other(anyhow::anyhow!(
                    "file size is unknown: {}",
                    entry.path
                )));
            };
            let planned = build_file_plan(
                &local_index,
                &entry.path,
                size as u64,
                &dest,
                entry.hash.clone(),
                entry.mtime_ms,
                entry.ctime_ms,
                false,
            );
            total_bytes = total_bytes.saturating_add(planned.size);
            planned_files.push(planned);
        }
    }

    Ok(PlannedJob {
        files: planned_files,
        total_bytes,
        conflicts,
    })
}

/// 构建单个文件的传输计划：优先复用未修改的本地副本。
#[allow(clippy::too_many_arguments)]
fn build_file_plan(
    local_index: &HashMap<String, LocalManifestEntry>,
    remote_path: &str,
    size: u64,
    final_path: &Path,
    hash: Option<String>,
    mtime_ms: Option<i64>,
    ctime_ms: Option<i64>,
    is_top: bool,
) -> TransferFileJournal {
    let mut from_local = false;
    let mut source = remote_path.to_owned();
    let mut effective_size = size;

    if let Some(local) = local_index.get(remote_path) {
        if local_is_fresh(local) {
            from_local = true;
            source = local.local_path.clone();
            effective_size = local.size;
        }
    }

    let tmp_name = format!("{}{}", file_name_of(final_path), TMP_SUFFIX);
    let tmp = final_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(tmp_name);

    TransferFileJournal {
        source,
        remote_path: remote_path.to_owned(),
        tmp: tmp.to_string_lossy().into_owned(),
        final_path: final_path.to_string_lossy().into_owned(),
        size: effective_size,
        done: 0,
        is_dir: false,
        from_local,
        is_top,
        hash,
        mtime_ms,
        ctime_ms,
    }
}

/// 本地副本是否仍然可用：文件存在、大小一致、修改时间在容差内。
fn local_is_fresh(entry: &LocalManifestEntry) -> bool {
    let path = Path::new(&entry.local_path);
    let meta = match std::fs::metadata(path) {
        Ok(meta) => meta,
        Err(_) => return false,
    };
    if !meta.is_file() {
        return false;
    }
    if meta.len() != entry.size {
        return false;
    }

    let Some(recorded) = entry.mtime_ms else {
        // 没有记录时间时只信大小：可能被同名同大小文件替换，保守起见不复用。
        return false;
    };
    let modified = match meta.modified() {
        Ok(time) => time,
        Err(_) => return false,
    };
    let local_ms = match modified.duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as i64,
        Err(_) => return false,
    };

    file_time::mtime_close(recorded, local_ms)
}

// ---- 执行 ----

/// 入队并确保 worker 在运行。
fn enqueue(app: &AppHandle, job_id: String) {
    {
        let mut queue = QUEUE.lock().expect("transfer queue poisoned");
        queue.push_back(job_id);
    }

    if WORKER_RUNNING.swap(true, Ordering::SeqCst) {
        return;
    }

    let app = app.clone();
    tauri::async_runtime::spawn(async move {
        loop {
            let next = {
                let mut queue = QUEUE.lock().expect("transfer queue poisoned");
                queue.pop_front()
            };

            match next {
                Some(job_id) => run_job(app.clone(), job_id).await,
                None => {
                    WORKER_RUNNING.store(false, Ordering::SeqCst);
                    let has_pending = {
                        let queue = QUEUE.lock().expect("transfer queue poisoned");
                        !queue.is_empty()
                    };
                    if !has_pending {
                        break;
                    }
                    // 重新抢占 worker 标记，避免与新的 enqueue 竞态导致任务饿死。
                    if WORKER_RUNNING.swap(true, Ordering::SeqCst) {
                        break;
                    }
                }
            }
        }
    });
}

/// 执行一个任务。
async fn run_job(app: AppHandle, job_id: String) {
    let pool = devices::pool(&app).await;
    let job = match transfer_jobs::find(&pool, &job_id).await {
        Ok(Some(job)) => job,
        Ok(None) => return,
        Err(err) => {
            log::warn!("sync transfer: load job {job_id} failed: {err}");
            return;
        }
    };

    // 任务已被取消（排队期间点了取消）。
    if control_of(&job_id).cancelled {
        remove_temp_files(&job).await;
        let _ = transfer_jobs::delete(&pool, &job_id).await;
        set_snapshot_status(&app, &job_id, STATUS_CANCELLED, None);
        return;
    }

    if let Err(err) = transfer_jobs::set_status(&pool, &job_id, STATUS_RUNNING).await {
        log::warn!("sync transfer: mark job running failed: {err}");
    }
    set_snapshot_status(&app, &job_id, STATUS_RUNNING, None);

    let shared_key = match load_shared_key(&app, &job.device_id).await {
        Ok(key) => key,
        Err(err) => {
            fail_job(&app, &job, &pool, err.to_string()).await;
            return;
        }
    };
    let address = match resolve_address(&app, &job.device_id).await {
        Ok(address) => address,
        Err(err) => {
            fail_job(&app, &job, &pool, err.to_string()).await;
            return;
        }
    };

    let file_count = job.files.iter().filter(|file| !file.is_dir).count();
    let mut transferred: u64 = 0;
    let mut file_index: usize = 0;
    let mut last_emit = Instant::now();
    let mut last_bytes = 0u64;
    let mut last_speed_at = Instant::now();
    let mut speed_bps: u64 = 0;
    let mut files = job.files.clone();

    for position in 0..files.len() {
        // 暂停 / 取消检查点。
        if let ControlDecision::Cancel = wait_for_control(&app, &job_id).await {
            cancel_job_now(&app, &pool, &job, &files, transferred).await;
            return;
        }

        let is_dir = files[position].is_dir;
        let final_path = files[position].final_path.clone();

        if is_dir {
            if let Err(err) = tokio::fs::create_dir_all(&final_path).await {
                fail_job_with_files(
                    &app,
                    &job,
                    &pool,
                    &files,
                    transferred,
                    &format!("create directory {final_path} failed: {err}"),
                )
                .await;
                return;
            }
            continue;
        }

        file_index += 1;

        // 目标与来源完全相同时跳过（把文件粘贴回它所在目录）。
        if files[position].from_local && files[position].source == final_path {
            transferred = transferred.saturating_add(files[position].size);
            files[position].done = files[position].size;
            continue;
        }

        // 先建父目录（目录条目可能因冲突策略被跳过，这里兜底）。
        if let Some(parent) = Path::new(&final_path).parent() {
            if let Err(err) = tokio::fs::create_dir_all(parent).await {
                fail_job_with_files(
                    &app,
                    &job,
                    &pool,
                    &files,
                    transferred,
                    &format!("create directory {parent:?} failed: {err}"),
                )
                .await;
                return;
            }
        }

        let result = transfer_one_file(
            &app,
            &job_id,
            &mut files[position],
            &shared_key,
            &address,
            transferred,
            file_count,
            file_index,
            &mut speed_bps,
            &mut last_emit,
            &mut last_speed_at,
            &mut last_bytes,
        )
        .await;

        match result {
            Ok(()) => {}
            Err(err) => {
                if err.is_cancelled() {
                    cancel_job_now(&app, &pool, &job, &files, transferred).await;
                    return;
                }
                fail_job_with_files(&app, &job, &pool, &files, transferred, &err.to_string()).await;
                return;
            }
        }

        transferred = transferred.saturating_add(files[position].size);
        files[position].done = files[position].size;

        // 每个文件完成都落一次 journal，续传粒度足够细。
        if let Err(err) = transfer_jobs::update_progress(&pool, &job_id, transferred, &files).await
        {
            log::warn!("sync transfer: persist progress failed: {err}");
        }
    }

    // 全部文件完成后，按逆序还原目录时间（子项写入会改变目录 mtime）。
    for position in (0..files.len()).rev() {
        if !files[position].is_dir {
            continue;
        }
        let path = Path::new(&files[position].final_path);
        if let Err(err) =
            file_time::apply_times(path, files[position].mtime_ms, files[position].ctime_ms)
        {
            log::warn!(
                "sync transfer: restore directory time {:?} failed: {err}",
                files[position].final_path
            );
        }
    }

    finish_job(&app, &job, &pool, &files, transferred).await;
}

/// 传输单个文件：本地副本直接复制，远端文件分块拉取（支持续传）。
#[allow(clippy::too_many_arguments)]
async fn transfer_one_file(
    app: &AppHandle,
    job_id: &str,
    file: &mut TransferFileJournal,
    shared_key: &str,
    address: &str,
    transferred_base: u64,
    file_count: usize,
    file_index: usize,
    speed_bps: &mut u64,
    last_emit: &mut Instant,
    last_speed_at: &mut Instant,
    last_bytes: &mut u64,
) -> std::result::Result<(), TransferError> {
    let tmp = PathBuf::from(&file.tmp);
    let final_path = PathBuf::from(&file.final_path);

    if file.from_local {
        // 本地副本复制：直接覆盖临时文件（复制很快，不需要续传）。
        if let Err(err) = tokio::fs::copy(&file.source, &tmp).await {
            return Err(TransferError::Failed(format!(
                "copy local file {:?} failed: {err}",
                file.source
            )));
        }
        file.done = file.size;
    } else {
        pull_remote_file(
            app,
            job_id,
            file,
            shared_key,
            address,
            transferred_base,
            file_count,
            file_index,
            speed_bps,
            last_emit,
            last_speed_at,
            last_bytes,
        )
        .await?;
    }

    // 覆盖同名文件：Windows 的 rename 不能覆盖已存在文件，先删后改名。
    if final_path.exists() {
        if let Err(err) = tokio::fs::remove_file(&final_path).await {
            return Err(TransferError::Failed(format!(
                "remove existing {final_path:?} failed: {err}"
            )));
        }
    }

    if let Err(err) = tokio::fs::rename(&tmp, &final_path).await {
        return Err(TransferError::Failed(format!(
            "rename {tmp:?} to {final_path:?} failed: {err}"
        )));
    }

    // 时间还原失败只告警：内容已完整落盘。
    if let Err(err) = file_time::apply_times(&final_path, file.mtime_ms, file.ctime_ms) {
        log::warn!(
            "sync transfer: restore times for {:?} failed: {err}",
            file.final_path
        );
    }

    Ok(())
}

/// 从远端分块拉取到临时文件，支持从 `done` 处续传。
#[allow(clippy::too_many_arguments)]
async fn pull_remote_file(
    app: &AppHandle,
    job_id: &str,
    file: &mut TransferFileJournal,
    shared_key: &str,
    address: &str,
    transferred_base: u64,
    file_count: usize,
    file_index: usize,
    speed_bps: &mut u64,
    last_emit: &mut Instant,
    last_speed_at: &mut Instant,
    last_bytes: &mut u64,
) -> std::result::Result<(), TransferError> {
    let tmp = PathBuf::from(&file.tmp);

    // 续传：done 必须不超过文件总大小，否则按从头开始处理。
    if file.done > file.size {
        file.done = 0;
    }

    let mut handle = match tokio::fs::OpenOptions::new()
        .create(true)
        // 续传要靠已有前缀重建哈希，必须保留文件原内容，不能截断。
        .truncate(false)
        .read(true)
        .write(true)
        .open(&tmp)
        .await
    {
        Ok(handle) => handle,
        Err(err) => {
            return Err(TransferError::Failed(format!(
                "open temp file {tmp:?} failed: {err}"
            )));
        }
    };

    // 已写入部分的哈希要重新计算，否则续传后整文件校验必然失败。
    let mut hasher = blake3::Hasher::new();
    if file.done > 0 {
        let mut existing = vec![0u8; 256 * 1024];
        let mut read_total: u64 = 0;
        if let Err(err) = handle.seek(std::io::SeekFrom::Start(0)).await {
            return Err(TransferError::Failed(format!(
                "seek temp file {tmp:?} failed: {err}"
            )));
        }
        while read_total < file.done {
            let want = (file.done - read_total).min(existing.len() as u64) as usize;
            let read = match handle.read(&mut existing[..want]).await {
                Ok(read) => read,
                Err(err) => {
                    return Err(TransferError::Failed(format!(
                        "read temp file {tmp:?} failed: {err}"
                    )));
                }
            };
            if read == 0 {
                // 临时文件比记录短，说明被外部截断，从头再来。
                file.done = 0;
                hasher = blake3::Hasher::new();
                break;
            }
            hasher.update(&existing[..read]);
            read_total += read as u64;
        }
    }

    if file.done == 0 {
        if let Err(err) = handle.set_len(0).await {
            return Err(TransferError::Failed(format!(
                "truncate temp file {tmp:?} failed: {err}"
            )));
        }
    }

    if let Err(err) = handle.seek(std::io::SeekFrom::Start(file.done)).await {
        return Err(TransferError::Failed(format!(
            "seek temp file {tmp:?} failed: {err}"
        )));
    }

    let mut offset = file.done;
    let mut file_transferred = offset;

    while offset < file.size {
        // 每块都检查暂停 / 取消，保证大文件也能及时响应。
        if let ControlDecision::Cancel = wait_for_control(app, job_id).await {
            let _ = handle.flush().await;
            file.done = offset;
            return Err(TransferError::Cancelled);
        }

        let len = (file.size - offset).min(FILE_CHUNK_SIZE as u64);
        let chunk =
            match client::fetch_file_chunk(app, shared_key, address, &file.source, offset, len)
                .await
            {
                Ok(chunk) => chunk,
                Err(err) => {
                    // 保留临时文件与 done，用户重试时可续传。
                    let _ = handle.flush().await;
                    file.done = offset;
                    return Err(TransferError::Failed(format!(
                        "fetch file chunk failed: {err}"
                    )));
                }
            };

        if chunk.len() as u64 != len {
            let _ = handle.flush().await;
            file.done = offset;
            return Err(TransferError::Failed(format!(
                "file {} chunk {offset}..{} returned {} bytes",
                file.source,
                offset + len,
                chunk.len()
            )));
        }

        if let Err(err) = handle.write_all(&chunk).await {
            let _ = handle.flush().await;
            file.done = offset;
            return Err(TransferError::Failed(format!(
                "write temp file {tmp:?} failed: {err}"
            )));
        }

        hasher.update(&chunk);
        offset += len;
        file_transferred += len;
        file.done = offset;

        // 速度与进度事件节流。
        let now = Instant::now();
        let elapsed = now.duration_since(*last_speed_at);
        if elapsed >= Duration::from_millis(500) {
            let delta = file_transferred.saturating_sub(*last_bytes);
            *speed_bps = (delta as f64 / elapsed.as_secs_f64()) as u64;
            *last_speed_at = now;
            *last_bytes = file_transferred;
        }

        if now.duration_since(*last_emit) >= PROGRESS_INTERVAL {
            *last_emit = now;
            let snapshot_bytes = transferred_base.saturating_add(file_transferred);
            update_snapshot_progress(
                app,
                job_id,
                &file.final_path,
                file_index,
                file_count,
                snapshot_bytes,
                *speed_bps,
            );
        }
    }

    if let Err(err) = handle.flush().await {
        return Err(TransferError::Failed(format!(
            "flush temp file {tmp:?} failed: {err}"
        )));
    }

    // 整文件校验：发送端没有提供哈希时（超大条目）只以长度为准。
    if let Some(expected) = file.hash.as_deref() {
        let actual = hasher.finalize().to_hex().to_string();
        if actual != expected {
            let _ = tokio::fs::remove_file(&tmp).await;
            file.done = 0;
            return Err(TransferError::Failed(
                "远端文件已变更，请重新复制".to_owned(),
            ));
        }
    }

    Ok(())
}

/// 任务失败：记录错误并保留临时文件（方便重试续传）。
async fn fail_job(
    app: &AppHandle,
    job: &TransferJobRecord,
    pool: &sqlx::SqlitePool,
    error: String,
) {
    fail_job_with_files(app, job, pool, &job.files, job.done_bytes, &error).await;
}

/// 任务失败（携带最新文件进度）。
async fn fail_job_with_files(
    app: &AppHandle,
    job: &TransferJobRecord,
    pool: &sqlx::SqlitePool,
    files: &[TransferFileJournal],
    done_bytes: u64,
    error: &str,
) {
    if let Err(err) = transfer_jobs::update_progress(pool, &job.job_id, done_bytes, files).await {
        log::warn!("sync transfer: persist failed progress failed: {err}");
    }
    if let Err(err) = transfer_jobs::set_status(pool, &job.job_id, STATUS_FAILED).await {
        log::warn!("sync transfer: mark job failed failed: {err}");
    }
    set_snapshot_status(app, &job.job_id, STATUS_FAILED, Some(error.to_owned()));
}

/// 任务成功：写本地副本快照、清理 journal、推送完成状态。
async fn finish_job(
    app: &AppHandle,
    job: &TransferJobRecord,
    pool: &sqlx::SqlitePool,
    files: &[TransferFileJournal],
    done_bytes: u64,
) {
    // 顶层路径直接取计划里标记的顶层条目（顶层文件或目录根）。
    let mut top_paths: Vec<String> = Vec::new();
    let mut manifest: Vec<LocalManifestEntry> = Vec::new();

    for file in files {
        if file.is_dir {
            if file.is_top {
                top_paths.push(file.final_path.clone());
            }
            continue;
        }
        manifest.push(manifest_entry_for(file));
        if file.is_top {
            top_paths.push(file.final_path.clone());
        }
    }

    let paths_json = serde_json::to_string(&top_paths).unwrap_or_else(|_| "[]".to_owned());
    let manifest_json = serde_json::to_string(&manifest).unwrap_or_else(|_| "[]".to_owned());
    if let Err(err) =
        file_state::set_local_snapshot(pool, &job.item_id, &paths_json, &manifest_json).await
    {
        log::warn!("sync transfer: save local snapshot failed: {err}");
    }

    if let Err(err) = transfer_jobs::update_progress(pool, &job.job_id, done_bytes, files).await {
        log::warn!("sync transfer: persist final progress failed: {err}");
    }
    if let Err(err) = transfer_jobs::delete(pool, &job.job_id).await {
        log::warn!("sync transfer: delete finished job failed: {err}");
    }

    set_snapshot_status(app, &job.job_id, STATUS_DONE, None);
    emit_done(app, &job.job_id);
}

/// 为成功落盘的文件生成副本快照（用落盘后的真实修改时间）。
fn manifest_entry_for(file: &TransferFileJournal) -> LocalManifestEntry {
    let path = Path::new(&file.final_path);
    let meta = std::fs::metadata(path).ok();
    let mut size = file.size;
    let mut mtime = file.mtime_ms;

    if let Some(meta) = meta {
        size = meta.len();
        if let Ok(modified) = meta.modified() {
            if let Ok(duration) = modified.duration_since(std::time::UNIX_EPOCH) {
                mtime = Some(duration.as_millis() as i64);
            }
        }
    }

    LocalManifestEntry {
        remote_path: file.remote_path.clone(),
        local_path: file.final_path.clone(),
        size,
        mtime_ms: mtime,
    }
}

/// 控制检查结果。
enum ControlDecision {
    Continue,
    Cancel,
}

/// 等待控制指令：暂停时原地等待，取消时返回 [`ControlDecision::Cancel`]。
async fn wait_for_control(app: &AppHandle, job_id: &str) -> ControlDecision {
    let mut paused_reported = false;

    loop {
        let control = control_of(job_id);
        if control.cancelled {
            return ControlDecision::Cancel;
        }
        if !control.paused {
            return ControlDecision::Continue;
        }

        // 暂停状态只在进入暂停时广播一次，避免每 200ms 重复发事件。
        if !paused_reported {
            paused_reported = true;
            set_snapshot_status(app, job_id, STATUS_PAUSED, None);
        }
        tokio::time::sleep(PAUSE_POLL_INTERVAL).await;
    }
}

/// 立即取消任务：删除临时文件、清除记录、广播状态。
async fn cancel_job_now(
    app: &AppHandle,
    pool: &sqlx::SqlitePool,
    job: &TransferJobRecord,
    files: &[TransferFileJournal],
    done_bytes: u64,
) {
    let current = TransferJobRecord {
        job_id: job.job_id.clone(),
        item_id: job.item_id.clone(),
        device_id: job.device_id.clone(),
        target_dir: job.target_dir.clone(),
        status: STATUS_CANCELLED.to_owned(),
        total_bytes: job.total_bytes,
        done_bytes,
        files: files.to_vec(),
    };
    remove_temp_files(&current).await;
    let _ = transfer_jobs::delete(pool, &job.job_id).await;
    set_snapshot_status(app, &job.job_id, STATUS_CANCELLED, None);
}

/// 读取任务控制开关。
fn control_of(job_id: &str) -> JobControl {
    let controls = CONTROLS.lock().expect("transfer controls poisoned");
    controls.get(job_id).copied().unwrap_or_default()
}

/// 删除任务涉及的临时文件。
async fn remove_temp_files(job: &TransferJobRecord) {
    for file in &job.files {
        if file.is_dir || file.tmp.is_empty() || !file.tmp.ends_with(TMP_SUFFIX) {
            continue;
        }
        if let Err(err) = tokio::fs::remove_file(&file.tmp).await {
            if err.kind() != std::io::ErrorKind::NotFound {
                log::warn!(
                    "sync transfer: remove temp file {:?} failed: {err}",
                    file.tmp
                );
            }
        }
    }
}

// ---- 远端连接信息 ----

/// 取来源设备的共享密钥。
async fn load_shared_key(app: &AppHandle, device_id: &str) -> Result<String> {
    let pool = devices::pool(app).await;
    let Some(device) = devices::find_device(&pool, device_id).await? else {
        return Err(AppError::Other(anyhow::anyhow!(
            "source device is not paired"
        )));
    };
    if !device.allow_receive {
        return Err(AppError::Other(anyhow::anyhow!(
            "source device is not allowed to send"
        )));
    }

    Ok(device.shared_key)
}

/// 解析来源设备地址。
async fn resolve_address(app: &AppHandle, device_id: &str) -> Result<String> {
    let pool = devices::pool(app).await;
    let Some(device) = devices::find_device(&pool, device_id).await? else {
        return Err(AppError::Other(anyhow::anyhow!(
            "source device is not paired"
        )));
    };

    let manager = app.state::<std::sync::Arc<crate::sync::SyncManager>>();
    match manager.device_address(device_id, device.last_address.as_deref()) {
        Some(address) => Ok(address),
        None => Err(AppError::Other(anyhow::anyhow!(
            "source device address is unknown"
        ))),
    }
}

/// 读取条目当前的远端路径串（journal 不保存 content）。
async fn remote_content_of(app: &AppHandle, item_id: &str) -> Result<String> {
    let pool = app.state::<crate::db::DatabaseState>().pool().await;
    let item = crate::db::items::find_item_by_id(&pool, item_id)
        .await?
        .ok_or_else(|| AppError::Other(anyhow::anyhow!("clipboard item not found")))?;

    Ok(item.content)
}

// ---- 状态快照与事件 ----

/// 写入内存快照并广播。
fn store_snapshot(snapshot: &TransferSnapshot) {
    {
        let mut latest = LATEST_JOB.lock().expect("transfer latest job poisoned");
        *latest = Some(snapshot.job_id.clone());
    }
    let mut snapshots = SNAPSHOTS.lock().expect("transfer snapshots poisoned");
    snapshots.insert(snapshot.job_id.clone(), snapshot.clone());
}

/// 广播快照。
fn emit_snapshot(app: &AppHandle, snapshot: &TransferSnapshot) {
    if let Err(err) = app.emit(EVENT_FILE_PROGRESS, snapshot) {
        log::warn!("sync transfer: emit progress failed: {err}");
    }
}

/// 更新快照状态并广播。
fn set_snapshot_status(app: &AppHandle, job_id: &str, status: &str, error: Option<String>) {
    let updated = {
        let mut snapshots = SNAPSHOTS.lock().expect("transfer snapshots poisoned");
        let Some(snapshot) = snapshots.get_mut(job_id) else {
            return;
        };
        snapshot.status = status.to_owned();
        if let Some(error) = error {
            snapshot.error = Some(error);
        }
        if status == STATUS_DONE || status == STATUS_CANCELLED || status == STATUS_SKIPPED {
            snapshot.done = true;
        }
        snapshot.clone()
    };
    emit_snapshot(app, &updated);
}

/// 更新进度字段并广播。
fn update_snapshot_progress(
    app: &AppHandle,
    job_id: &str,
    file_name: &str,
    file_index: usize,
    file_count: usize,
    transferred_bytes: u64,
    speed_bps: u64,
) {
    let updated = {
        let mut snapshots = SNAPSHOTS.lock().expect("transfer snapshots poisoned");
        let Some(snapshot) = snapshots.get_mut(job_id) else {
            return;
        };
        snapshot.file_name = Some(file_name.to_owned());
        snapshot.file_index = file_index;
        snapshot.file_count = file_count;
        snapshot.transferred_bytes = transferred_bytes;
        snapshot.speed_bps = speed_bps;
        snapshot.clone()
    };
    emit_snapshot(app, &updated);
}

/// 更新任务目标目录（改存到其它目录时）。
fn update_snapshot_target(
    app: &AppHandle,
    job_id: &str,
    target_dir: &str,
    file_count: usize,
    total_bytes: u64,
) {
    let updated = {
        let mut snapshots = SNAPSHOTS.lock().expect("transfer snapshots poisoned");
        let Some(snapshot) = snapshots.get_mut(job_id) else {
            return;
        };
        snapshot.target_dir = target_dir.to_owned();
        snapshot.file_count = file_count;
        snapshot.total_bytes = total_bytes;
        snapshot.transferred_bytes = 0;
        snapshot.status = STATUS_QUEUED.to_owned();
        snapshot.error = None;
        snapshot.conflicts = Vec::new();
        snapshot.done = false;
        snapshot.clone()
    };
    emit_snapshot(app, &updated);
}

/// 任务完成后延迟移除内存快照，给前端留出渲染「已完成」的时间。
fn emit_done(_app: &AppHandle, job_id: &str) {
    let job_id = job_id.to_owned();
    tauri::async_runtime::spawn(async move {
        tokio::time::sleep(Duration::from_secs(3)).await;
        let mut snapshots = SNAPSHOTS.lock().expect("transfer snapshots poisoned");
        snapshots.remove(&job_id);
    });
}

// ---- 清单编解码 ----

/// 解析远端文件指纹。
fn decode_manifest(record: &SyncFileRecord) -> Result<Vec<SyncFileEntryDto>> {
    let Some(raw) = record.files_json.as_deref() else {
        return Err(AppError::Other(anyhow::anyhow!(
            "remote file manifest is missing"
        )));
    };
    let files: Vec<SyncFileEntryDto> = serde_json::from_str(raw)
        .map_err(|err| AppError::Other(anyhow::anyhow!("decode file manifest failed: {err}")))?;
    if files.is_empty() {
        return Err(AppError::Other(anyhow::anyhow!(
            "remote file manifest is empty"
        )));
    }

    Ok(files)
}

/// 解析本地副本快照；缺失或损坏时按「没有副本」处理。
fn decode_local_manifest(raw: Option<&str>) -> Vec<LocalManifestEntry> {
    let Some(raw) = raw else {
        return Vec::new();
    };
    match serde_json::from_str::<Vec<LocalManifestEntry>>(raw) {
        Ok(entries) => entries,
        Err(err) => {
            log::warn!("sync transfer: decode local manifest failed: {err}");
            Vec::new()
        }
    }
}

// ---- 路径工具 ----

/// 取最终路径的文件名。
fn file_name_of(path: &Path) -> String {
    match path.file_name().and_then(|name| name.to_str()) {
        Some(name) => name.to_owned(),
        None => "file".to_owned(),
    }
}

/// 顶层条目的默认名称：远端路径的文件名；非法或为空时退回序号。
fn top_base_name(remote_root: &str, index: usize) -> String {
    let raw = Path::new(remote_root)
        .file_name()
        .and_then(|name| name.to_str());
    match raw {
        Some(name) => {
            let sanitized = sanitize_file_name(name);
            if sanitized.is_empty() {
                format!("file-{index}")
            } else {
                sanitized
            }
        }
        None => format!("file-{index}"),
    }
}

/// 在目标目录内取一个不冲突的顶层路径（冲突时追加序号）。
fn unique_top_path(dir: &Path, base: &str, used: &mut HashSet<String>) -> PathBuf {
    let mut candidate = dir.join(base);
    let mut suffix: usize = 2;

    loop {
        let key = candidate.to_string_lossy().to_lowercase();
        if !used.contains(&key) && !candidate.exists() {
            used.insert(key);
            return candidate;
        }

        let name = match base.rsplit_once('.') {
            Some((stem, ext)) if !stem.is_empty() && !ext.is_empty() => {
                format!("{stem} ({suffix}).{ext}")
            }
            _ => format!("{base} ({suffix})"),
        };
        candidate = dir.join(name);
        suffix += 1;
    }
}

/// 过滤 Windows 不允许的文件名字符，并避开保留设备名与超长文件名。
fn sanitize_file_name(name: &str) -> String {
    let replaced: String = name
        .chars()
        .map(|ch| match ch {
            '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*' => '_',
            _ => ch,
        })
        .collect();

    let trimmed = replaced.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    // Windows 保留名（CON / NUL / COM1 等）无法直接落盘，统一加前缀规避。
    let stem = trimmed.split('.').next().unwrap_or(trimmed);
    let sanitized = if is_windows_reserved_name(stem) {
        format!("_{trimmed}")
    } else {
        trimmed.to_owned()
    };

    sanitized.chars().take(MAX_FILE_NAME_CHARS).collect()
}

/// 判断是否 Windows 保留设备名（大小写不敏感）。
fn is_windows_reserved_name(stem: &str) -> bool {
    let upper = stem.to_ascii_uppercase();
    matches!(
        upper.as_str(),
        "CON"
            | "PRN"
            | "AUX"
            | "NUL"
            | "COM1"
            | "COM2"
            | "COM3"
            | "COM4"
            | "COM5"
            | "COM6"
            | "COM7"
            | "COM8"
            | "COM9"
            | "LPT1"
            | "LPT2"
            | "LPT3"
            | "LPT4"
            | "LPT5"
            | "LPT6"
            | "LPT7"
            | "LPT8"
            | "LPT9"
    )
}

/// 把远端相对路径映射为本地相对路径，阻止 `..` 逃逸。
struct RelPathMapper {
    dirs: HashMap<String, PathBuf>,
    files: HashMap<String, PathBuf>,
    used: HashMap<String, HashSet<String>>,
}

impl RelPathMapper {
    fn new() -> Self {
        Self {
            dirs: HashMap::new(),
            files: HashMap::new(),
            used: HashMap::new(),
        }
    }

    /// 映射文件路径；遇到非法段时返回 `None`。
    fn map(&mut self, rel_path: &str) -> Option<PathBuf> {
        if let Some(cached) = self.files.get(rel_path) {
            return Some(cached.clone());
        }
        if rel_path.is_empty() || rel_path.starts_with('/') {
            return None;
        }

        let (rel_dir, file_name) = match rel_path.rsplit_once('/') {
            Some((dir, name)) => (dir, name),
            None => ("", rel_path),
        };
        if file_name.is_empty() || file_name == "." || file_name == ".." {
            return None;
        }

        let mut local = self.map_dirs(rel_dir)?;
        let base = sanitize_file_name(file_name);
        if base.is_empty() {
            return None;
        }

        let scope = local.to_string_lossy().to_lowercase();
        let used = self.used.entry(scope).or_default();
        let name = unique_in_scope(&base, used);
        local.push(name);

        self.files.insert(rel_path.to_owned(), local.clone());
        Some(local)
    }

    /// 映射目录路径（清单里的目录条目，含空目录）；重复调用返回同一结果。
    fn map_dir(&mut self, rel_path: &str) -> Option<PathBuf> {
        if rel_path.is_empty() || rel_path.starts_with('/') {
            return None;
        }
        self.map_dirs(rel_path)
    }

    /// 逐段映射目录链。
    fn map_dirs(&mut self, rel_dir: &str) -> Option<PathBuf> {
        let mut local = PathBuf::new();
        if rel_dir.is_empty() {
            return Some(local);
        }

        let mut remote = String::new();
        for part in rel_dir.split('/') {
            if part.is_empty() || part == "." || part == ".." {
                return None;
            }
            if !remote.is_empty() {
                remote.push('/');
            }
            remote.push_str(part);

            if let Some(cached) = self.dirs.get(&remote) {
                local = cached.clone();
                continue;
            }

            let base = sanitize_file_name(part);
            if base.is_empty() {
                return None;
            }
            let scope = local.to_string_lossy().to_lowercase();
            let used = self.used.entry(scope).or_default();
            let name = unique_in_scope(&base, used);
            local.push(name);
            self.dirs.insert(remote.clone(), local.clone());
        }

        Some(local)
    }
}

/// 在同一父目录内取唯一名称：冲突时在扩展名前追加序号。
fn unique_in_scope(base: &str, used: &mut HashSet<String>) -> String {
    let key = base.to_lowercase();
    if used.insert(key) {
        return base.to_owned();
    }

    let mut suffix: usize = 2;
    loop {
        let candidate = match base.rsplit_once('.') {
            Some((stem, ext)) if !stem.is_empty() && !ext.is_empty() => {
                format!("{stem} ({suffix}).{ext}")
            }
            _ => format!("{base} ({suffix})"),
        };

        if used.insert(candidate.to_lowercase()) {
            return candidate;
        }
        suffix += 1;
    }
}

// ---- 错误类型 ----

/// 传输过程中的内部错误。
#[derive(Debug)]
enum TransferError {
    /// 用户取消。
    Cancelled,
    /// 传输失败。
    Failed(String),
}

impl TransferError {
    /// 是否为用户取消。
    fn is_cancelled(&self) -> bool {
        matches!(self, Self::Cancelled)
    }
}

impl std::fmt::Display for TransferError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cancelled => write!(formatter, "transfer cancelled"),
            Self::Failed(message) => write!(formatter, "{message}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_replaces_windows_illegal_chars() {
        assert_eq!(sanitize_file_name("a:b*c?.txt"), "a_b_c_.txt");
        assert_eq!(sanitize_file_name("  spaced  "), "spaced");
    }

    #[test]
    fn sanitize_prefixes_reserved_names() {
        assert_eq!(sanitize_file_name("CON"), "_CON");
        assert_eq!(sanitize_file_name("nul.txt"), "_nul.txt");
        assert_eq!(sanitize_file_name("console"), "console");
    }

    #[test]
    fn rel_path_mapper_builds_nested_path() {
        let mut mapper = RelPathMapper::new();

        assert_eq!(
            mapper.map("docs/2024/a.txt"),
            Some(PathBuf::from("docs/2024/a.txt"))
        );
        assert_eq!(mapper.map("docs/b.txt"), Some(PathBuf::from("docs/b.txt")));
    }

    #[test]
    fn rel_path_mapper_rejects_escaping_segments() {
        let mut mapper = RelPathMapper::new();

        assert_eq!(mapper.map("../secret.txt"), None);
        assert_eq!(mapper.map("docs/../../secret.txt"), None);
        assert_eq!(mapper.map("/etc/passwd"), None);
        assert_eq!(mapper.map("docs//a.txt"), None);
        assert_eq!(mapper.map(""), None);
    }

    #[test]
    fn rel_path_mapper_dedupes_sanitized_siblings() {
        let mut mapper = RelPathMapper::new();

        assert_eq!(
            mapper.map("docs/a:b.txt"),
            Some(PathBuf::from("docs/a_b.txt"))
        );
        assert_eq!(
            mapper.map("docs/a?b.txt"),
            Some(PathBuf::from("docs/a_b (2).txt"))
        );
        assert_eq!(
            mapper.map("docs/A_B.TXT"),
            Some(PathBuf::from("docs/A_B (3).TXT"))
        );
    }

    #[test]
    fn top_base_name_falls_back_to_index() {
        assert_eq!(top_base_name("/tmp/a.txt", 0), "a.txt");
        assert_eq!(top_base_name("", 3), "file-3");
    }

    #[test]
    fn local_copy_is_not_fresh_when_size_differs() {
        let entry = LocalManifestEntry {
            remote_path: "/remote/a.txt".to_owned(),
            local_path: "/definitely/not/exists/a.txt".to_owned(),
            size: 10,
            mtime_ms: Some(0),
        };

        assert!(!local_is_fresh(&entry));
    }
}
