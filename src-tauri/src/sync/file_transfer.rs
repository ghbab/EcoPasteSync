//! 文件条目的分块拉取与缓存落盘。
//!
//! 两种触发时机共用本模块：
//! - 条目内文件总大小不超过直接传输阈值 → 接收端在入库前立即拉取；
//! - 超过阈值 → 只同步文件清单，粘贴时再按需拉取。
//!
//! 目录按「根目录 + 子树文件相对路径」重建：先建目录，再把每个子树文件拉进对应位置。
//! 缓存目录按「远端路径列表」的 blake3 命名，重复接收同一批路径时复用同一目录。
//! 传输不做断点续传：中途失败或用户取消一律丢弃半成品，下次从头再拉。

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex};

use serde_json::json;
use tauri::{AppHandle, Emitter, Manager};
use tokio::io::AsyncWriteExt;

use crate::core::{paths, AppError, Result};
use crate::sync::client;
use crate::sync::devices;
use crate::sync::file_state::{self, SyncFileState};
use crate::sync::protocol::{
    file_manifest_exceeds_limits, SyncFileEntryDto, EVENT_FILE_PROGRESS, FILE_CHUNK_SIZE,
};
use crate::sync::SyncManager;

/// 同步文件缓存的子目录名（位于 `<app_data_dir>/cache` 下）。
const CACHE_SUBDIR: &str = "sync-files";
/// 缓存文件名保留的最大字符数，避免中文等多字节字符超出文件系统 255 字节限制。
const MAX_FILE_NAME_CHARS: usize = 120;

/// 正在拉取的条目 id：同一卡片连点会并发清空同一缓存目录，必须串行化。
static TRANSFERRING: LazyLock<Mutex<HashSet<String>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

/// 单个待拉取文件的落盘计划。
struct PullJob {
    /// 发送端文件绝对路径，分块接口按此寻址。
    source: String,
    size: u64,
    /// 发送端给出的整文件哈希；缺失时仅校验长度。
    hash: Option<String>,
    /// 本地缓存内的目标路径。
    dest: PathBuf,
}

/// 单次条目传输的进度上下文；`item_id` 缺失时不上报进度。
struct TransferTarget<'a> {
    app: &'a AppHandle,
    item_id: &'a str,
    /// 当前文件之前的累计字节数。
    transferred_base: u64,
    total_bytes: u64,
}

impl TransferTarget<'_> {
    /// 上报当前文件已写入 `local_offset` 字节后的整体进度。
    fn report(&self, local_offset: u64) {
        emit_transfer_progress(
            self.app,
            Some(self.item_id),
            self.transferred_base + local_offset,
            self.total_bytes,
            false,
        );
    }
}

/// 把远端条目涉及的文件全部拉到本地缓存目录，返回本地顶层路径列表。
///
/// 目录按其「根 + 子树相对路径」重建：先建目录，再把子树文件逐个拉进对应位置。
/// 调用方需保证目录已被发送端完整枚举；这里对缺大小、非法相对路径或不可传输的
/// 输入直接报错，避免产生「有路径没有内容」的误导性缓存。
pub async fn pull_files(
    app: &AppHandle,
    remote_content: &str,
    source_device_id: &str,
    files: &[SyncFileEntryDto],
    item_id: Option<&str>,
) -> Result<Vec<String>> {
    // 清单规模上限校验：入库时已校验一次，按需拉取还要再挡一次，
    // 避免从数据库读出的陈旧清单绕过检查。
    if file_manifest_exceeds_limits(files) {
        return Err(AppError::Other(anyhow::anyhow!(
            "file manifest exceeds limits"
        )));
    }

    let pool = devices::pool(app).await;
    let device = match devices::find_device(&pool, source_device_id).await {
        Ok(Some(device)) => device,
        Ok(None) => {
            return Err(AppError::Other(anyhow::anyhow!(
                "source device is not paired"
            )));
        }
        Err(err) => return Err(err),
    };
    if !device.allow_receive {
        return Err(AppError::Other(anyhow::anyhow!(
            "source device is not allowed to send"
        )));
    }

    let manager = app.state::<std::sync::Arc<SyncManager>>();
    let Some(address) = manager.device_address(source_device_id, device.last_address.as_deref())
    else {
        return Err(AppError::Other(anyhow::anyhow!(
            "source device address is unknown"
        )));
    };

    // 缓存目录以远端路径列表的哈希命名：同一条目重复接收时落在同一目录，
    // 且哈希值天然是合法目录名，不依赖远端内容。
    let cache_key = blake3::hash(remote_content.as_bytes()).to_hex().to_string();
    let root = paths::cache_dir(app)?.join(CACHE_SUBDIR).join(cache_key);

    // 用户选择「取消即丢弃」，因此每次拉取都从空目录开始。
    if let Err(err) = tokio::fs::remove_dir_all(&root).await {
        if err.kind() != std::io::ErrorKind::NotFound {
            return Err(AppError::Other(anyhow::anyhow!(
                "clear sync file cache {root:?} failed: {err}"
            )));
        }
    }
    tokio::fs::create_dir_all(&root)
        .await
        .map_err(|err| AppError::Other(anyhow::anyhow!("create {root:?} failed: {err}")))?;

    // 清单按顶层条目归组：目录的子树文件也归到所属顶层目录下。
    let top_paths = crate::sync::split_paths(remote_content);
    let mut groups: Vec<Vec<&SyncFileEntryDto>> = vec![Vec::new(); top_paths.len()];

    for entry in files {
        let index = match entry.root_index {
            Some(index) if index < top_paths.len() => index,
            _ => match top_paths.iter().position(|path| path == &entry.path) {
                Some(index) => index,
                None => continue,
            },
        };
        groups[index].push(entry);
    }

    let mut names: HashMap<String, usize> = HashMap::new();
    let mut jobs: Vec<PullJob> = Vec::new();
    let mut local_paths: Vec<String> = Vec::with_capacity(top_paths.len());

    for (index, source) in top_paths.iter().enumerate() {
        let name = unique_file_name(source, index, &mut names);
        let local_root = root.join(&name);
        // 先取顶层本地路径文本：分支里 `local_root` 会被移进落盘计划。
        let local_root_text = local_root.to_string_lossy().into_owned();
        let group = &groups[index];
        let is_dir = group
            .iter()
            .any(|entry| entry.is_dir && entry.rel_path.is_none());

        if is_dir {
            // 目录根先落盘；空目录（没有子树文件）也会保留。
            if let Err(err) = tokio::fs::create_dir_all(&local_root).await {
                discard(&root).await;
                return Err(AppError::Other(anyhow::anyhow!(
                    "create {local_root:?} failed: {err}"
                )));
            }

            let mut children: Vec<&SyncFileEntryDto> = group
                .iter()
                .copied()
                .filter(|entry| entry.rel_path.is_some())
                .collect();
            // 按相对路径排序，保证多文件写入顺序稳定、进度推进可预期。
            children.sort_by(|left, right| left.rel_path.cmp(&right.rel_path));

            // 每个顶层目录独立维护名称作用域：清洗后同名的兄弟路径追加序号，
            // 避免不同远端路径映射到同一本地路径互相覆盖。
            let mut rel_mapper = RelPathMapper::new();

            for child in children {
                let Some(rel_path) = child.rel_path.as_deref() else {
                    continue;
                };
                let Some(relative) = rel_mapper.map(rel_path) else {
                    discard(&root).await;
                    return Err(AppError::Other(anyhow::anyhow!(
                        "invalid relative path from remote: {rel_path}"
                    )));
                };
                let dest = local_root.join(&relative);

                // 子树目录（含空目录）先建好；文件稍后落到同一层级。
                if child.is_dir {
                    if let Err(err) = tokio::fs::create_dir_all(&dest).await {
                        discard(&root).await;
                        return Err(AppError::Other(anyhow::anyhow!(
                            "create {dest:?} failed: {err}"
                        )));
                    }
                    continue;
                }

                let Some(size) = child.size.filter(|size| *size >= 0) else {
                    discard(&root).await;
                    return Err(AppError::Other(anyhow::anyhow!(
                        "file size is unknown: {}",
                        child.path
                    )));
                };

                if let Some(parent) = dest.parent() {
                    if let Err(err) = tokio::fs::create_dir_all(parent).await {
                        discard(&root).await;
                        return Err(AppError::Other(anyhow::anyhow!(
                            "create {parent:?} failed: {err}"
                        )));
                    }
                }

                jobs.push(PullJob {
                    source: child.path.clone(),
                    size: size as u64,
                    hash: child.hash.clone(),
                    dest,
                });
            }
        } else {
            let Some(entry) = group
                .iter()
                .find(|entry| !entry.is_dir && entry.rel_path.is_none())
            else {
                discard(&root).await;
                return Err(AppError::Other(anyhow::anyhow!(
                    "file fingerprint is missing: {source}"
                )));
            };
            let Some(size) = entry.size.filter(|size| *size >= 0) else {
                discard(&root).await;
                return Err(AppError::Other(anyhow::anyhow!(
                    "file size is unknown: {}",
                    entry.path
                )));
            };

            jobs.push(PullJob {
                source: entry.path.clone(),
                size: size as u64,
                hash: entry.hash.clone(),
                dest: local_root,
            });
        }

        local_paths.push(local_root_text);
    }

    let total_bytes: u64 = jobs
        .iter()
        .fold(0u64, |acc, job| acc.saturating_add(job.size));
    let mut transferred_base: u64 = 0;

    for job in &jobs {
        let target = item_id.map(|item_id| TransferTarget {
            app,
            item_id,
            transferred_base,
            total_bytes,
        });
        let pulled = pull_one_file(app, &device.shared_key, &address, job, target.as_ref()).await;

        if let Err(err) = pulled {
            // 任一文件失败即整体放弃：保留半套文件会让粘贴内容不完整。
            discard(&root).await;
            return Err(err);
        }

        transferred_base = transferred_base.saturating_add(job.size);
    }

    Ok(local_paths)
}

/// 把远端文件条目的文件拉到本地缓存，返回可写入剪贴板的本地路径。
///
/// 本机条目（没有同步状态记录）返回 `None`；远端条目返回 `Some(paths)`。
/// 缓存仍然有效时直接复用，不重复传输。
pub async fn ensure_item_files(
    app: &AppHandle,
    item_id: &str,
    remote_content: &str,
) -> Result<Option<Vec<String>>> {
    let pool = devices::pool(app).await;
    let Some(record) = file_state::find(&pool, item_id).await? else {
        return Ok(None);
    };

    if record.state == SyncFileState::Unavailable {
        return Err(command_error(
            app,
            crate::i18n::commands::Key::SyncFileUnavailable,
        ));
    }

    if let Some(paths) = read_local_paths(record.local_paths_json.as_deref()) {
        return Ok(Some(paths));
    }

    let Some(files_json) = record.files_json.as_deref() else {
        return Err(command_error(
            app,
            crate::i18n::commands::Key::SyncFileUnavailable,
        ));
    };
    let files: Vec<SyncFileEntryDto> = serde_json::from_str(files_json).map_err(|err| {
        AppError::Other(anyhow::anyhow!(
            "decode synced file fingerprints failed: {err}"
        ))
    })?;
    if files.is_empty() {
        return Err(command_error(
            app,
            crate::i18n::commands::Key::SyncFileUnavailable,
        ));
    }

    // 同一 item 并发拉取会互相清空缓存目录，直接拒绝后到的请求。
    let started = TRANSFERRING
        .lock()
        .expect("sync transferring poisoned")
        .insert(item_id.to_owned());
    if !started {
        return Err(command_error(
            app,
            crate::i18n::commands::Key::SyncFileTransferBusy,
        ));
    }

    let total_bytes = files
        .iter()
        .filter_map(|entry| entry.size)
        .fold(0u64, |acc, size| {
            if size > 0 {
                acc.saturating_add(size as u64)
            } else {
                acc
            }
        });
    emit_transfer_progress(app, Some(item_id), 0, total_bytes, false);

    let result = pull_files(
        app,
        remote_content,
        &record.device_id,
        &files,
        Some(item_id),
    )
    .await;

    TRANSFERRING
        .lock()
        .expect("sync transferring poisoned")
        .remove(item_id);

    match result {
        Ok(paths) => {
            if let Ok(json) = serde_json::to_string(&paths) {
                if let Err(err) = file_state::set_local_paths(&pool, item_id, &json).await {
                    log::warn!("sync file transfer: save local paths failed: {err}");
                }
            }
            emit_transfer_progress(app, Some(item_id), total_bytes, total_bytes, true);
            Ok(Some(paths))
        }
        Err(err) => {
            log::warn!("sync file transfer for item {item_id} failed: {err}");
            emit_transfer_progress(app, Some(item_id), 0, total_bytes, true);
            Err(command_error(
                app,
                crate::i18n::commands::Key::SyncFileTransferFailed,
            ))
        }
    }
}

/// 读取并校验缓存的本地路径：全部顶层路径（文件或目录）仍存在才算命中。
fn read_local_paths(raw: Option<&str>) -> Option<Vec<String>> {
    let paths: Vec<String> = serde_json::from_str(raw?).ok()?;
    if paths.is_empty() {
        return None;
    }
    if !paths.iter().all(|path| Path::new(path).exists()) {
        return None;
    }

    Some(paths)
}

/// 上报文件传输进度；`item_id` 缺失时不发事件。
fn emit_transfer_progress(
    app: &AppHandle,
    item_id: Option<&str>,
    transferred_bytes: u64,
    total_bytes: u64,
    done: bool,
) {
    let Some(item_id) = item_id else {
        return;
    };

    let payload = json!({
        "itemId": item_id,
        "transferredBytes": transferred_bytes,
        "totalBytes": total_bytes,
        "done": done,
    });
    if let Err(err) = app.emit(EVENT_FILE_PROGRESS, payload) {
        log::warn!("sync file transfer: emit progress failed: {err}");
    }
}

/// 构造命令层可展示的错误文案（跟随界面语言）。
fn command_error(app: &AppHandle, key: crate::i18n::commands::Key) -> AppError {
    let lang = crate::i18n::current_language(app);
    AppError::Other(anyhow::anyhow!(crate::i18n::commands::label(lang, key)))
}

/// 拉取单个文件的全部块并校验完整性。
async fn pull_one_file(
    app: &AppHandle,
    shared_key: &str,
    address: &str,
    job: &PullJob,
    target: Option<&TransferTarget<'_>>,
) -> Result<()> {
    let local_path = job.dest.as_path();
    let mut file = tokio::fs::File::create(local_path)
        .await
        .map_err(|err| AppError::Other(anyhow::anyhow!("create {local_path:?} failed: {err}")))?;

    let mut hasher = blake3::Hasher::new();
    let mut offset: u64 = 0;

    while offset < job.size {
        let len = (job.size - offset).min(FILE_CHUNK_SIZE as u64);
        let chunk =
            client::fetch_file_chunk(app, shared_key, address, &job.source, offset, len).await?;

        // 对端返回的长度与请求不一致说明源文件已变化，立即失败而不是写入错位内容。
        if chunk.len() as u64 != len {
            return Err(AppError::Other(anyhow::anyhow!(
                "file {} chunk {offset}..{} returned {} bytes",
                job.source,
                offset + len,
                chunk.len()
            )));
        }

        file.write_all(&chunk).await.map_err(|err| {
            AppError::Other(anyhow::anyhow!("write {local_path:?} failed: {err}"))
        })?;
        hasher.update(&chunk);
        offset += len;

        if let Some(target) = target {
            target.report(offset);
        }
    }

    file.flush()
        .await
        .map_err(|err| AppError::Other(anyhow::anyhow!("flush {local_path:?} failed: {err}")))?;

    // 发送端只对需要按需拉取的条目采集整文件哈希，缺失时仅以长度为准。
    if let Some(expected) = job.hash.as_deref() {
        let actual = hasher.finalize().to_hex().to_string();
        if actual != expected {
            return Err(AppError::Other(anyhow::anyhow!(
                "file {} checksum mismatch",
                job.source
            )));
        }
    }

    Ok(())
}

/// 丢弃未完成的缓存目录。
async fn discard(root: &Path) {
    if let Err(err) = tokio::fs::remove_dir_all(root).await {
        log::warn!("sync file transfer: discard {root:?} failed: {err}");
    }
}

/// 把远端相对路径映射为缓存内的本地相对路径。
///
/// 每段都经 [`sanitize_file_name`] 清洗；同一父目录下清洗结果冲突时追加序号，
/// 避免不同远端路径映射到同一本地路径互相覆盖。只接受相对路径：空段、
/// `.`、`..` 与绝对路径一律拒绝，防止写出缓存目录。
struct RelPathMapper {
    /// 父目录（本地相对路径的小写文本）→ 该目录下已用的小写名称。
    used: HashMap<String, HashSet<String>>,
}

impl RelPathMapper {
    fn new() -> Self {
        Self {
            used: HashMap::new(),
        }
    }

    /// 返回本地相对路径；遇到非法段时返回 `None`。
    fn map(&mut self, rel_path: &str) -> Option<PathBuf> {
        let mut local = PathBuf::new();

        for part in rel_path.split('/') {
            if part.is_empty() || part == "." || part == ".." {
                return None;
            }
            let base = sanitize_file_name(part);
            if base.is_empty() {
                return None;
            }

            // 以父目录的本地相对路径作为作用域，不同目录下允许同名。
            let scope = local.to_string_lossy().to_lowercase();
            let used = self.used.entry(scope).or_default();
            let name = unique_in_scope(&base, used);
            local.push(name);
        }

        if local.as_os_str().is_empty() {
            return None;
        }

        Some(local)
    }
}

/// 在同一父目录内取唯一名称：冲突时在扩展名前追加序号。
///
/// 比较时统一转小写：Windows 文件系统不区分大小写，`A.txt` 与 `a.txt`
/// 落盘后会互相覆盖。
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

/// 生成缓存内文件名：保留原名便于识别，过滤跨平台非法字符，同批次重名追加序号。
fn unique_file_name(path: &str, index: usize, used: &mut HashMap<String, usize>) -> String {
    let base = Path::new(path)
        .file_name()
        .and_then(|name| name.to_str())
        .map(sanitize_file_name)
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| format!("file-{index}"));

    let count = used.entry(base.clone()).or_insert(0);
    if *count == 0 {
        *count = 1;
        return base;
    }

    *count += 1;
    let suffix = *count;

    // 重名时序号插在扩展名前，保留可双击打开的关联。
    match base.rsplit_once('.') {
        Some((stem, ext)) if !stem.is_empty() && !ext.is_empty() => {
            format!("{stem} ({suffix}).{ext}")
        }
        _ => format!("{base} ({suffix})"),
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
    fn unique_file_name_appends_suffix_on_duplicate() {
        let mut used = HashMap::new();

        assert_eq!(unique_file_name("/tmp/a.txt", 0, &mut used), "a.txt");
        assert_eq!(unique_file_name("/other/a.txt", 1, &mut used), "a (2).txt");
        assert_eq!(unique_file_name("/third/a.txt", 2, &mut used), "a (3).txt");
        assert_eq!(unique_file_name("/tmp/noext", 3, &mut used), "noext");
        assert_eq!(unique_file_name("/other/noext", 4, &mut used), "noext (2)");
    }

    #[test]
    fn unique_file_name_falls_back_when_basename_missing() {
        let mut used = HashMap::new();

        assert_eq!(unique_file_name("", 7, &mut used), "file-7");
    }

    #[tokio::test]
    async fn discard_ignores_missing_directory() {
        let root = std::env::temp_dir().join(format!(
            "ecopaste-sync-file-transfer-{}",
            uuid::Uuid::new_v4()
        ));

        discard(&root).await;
    }

    #[test]
    fn rel_path_mapper_builds_nested_path() {
        let mut mapper = RelPathMapper::new();

        assert_eq!(
            mapper.map("docs/2024/a.txt"),
            Some(PathBuf::from("docs/2024/a.txt"))
        );
        // 不同目录下允许同名。
        assert_eq!(mapper.map("docs/b.txt"), Some(PathBuf::from("docs/b.txt")));
    }

    #[test]
    fn rel_path_mapper_rejects_escaping_segments() {
        let mut mapper = RelPathMapper::new();

        // 目录穿越、绝对路径与空段一律拒绝，避免写到缓存目录之外。
        assert_eq!(mapper.map("../secret.txt"), None);
        assert_eq!(mapper.map("docs/../../secret.txt"), None);
        assert_eq!(mapper.map("/etc/passwd"), None);
        assert_eq!(mapper.map("docs//a.txt"), None);
        assert_eq!(mapper.map(""), None);
    }

    #[test]
    fn rel_path_mapper_dedupes_sanitized_siblings() {
        let mut mapper = RelPathMapper::new();

        // `a:b.txt` 与 `a?b.txt` 清洗后同名，第二个必须追加序号而不是覆盖。
        assert_eq!(
            mapper.map("docs/a:b.txt"),
            Some(PathBuf::from("docs/a_b.txt"))
        );
        assert_eq!(
            mapper.map("docs/a?b.txt"),
            Some(PathBuf::from("docs/a_b (2).txt"))
        );
        // 大小写不同的同名也要去重（Windows 文件系统不区分大小写）。
        assert_eq!(
            mapper.map("docs/A_B.TXT"),
            Some(PathBuf::from("docs/A_B (3).TXT"))
        );
    }
}
