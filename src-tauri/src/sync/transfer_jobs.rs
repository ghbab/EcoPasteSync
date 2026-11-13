//! 文件传输任务日志（`sync_transfer_jobs`）。
//!
//! 传输过程中每个文件都会先写入同目录的临时文件（后缀固定为
//! [`TMP_SUFFIX`]），再改名成最终路径。把「临时文件路径 + 已完成字节数」写进
//! 本表，应用异常退出后启动时就能精确删除残留临时文件，并知道每个文件续传的
//! 起始偏移。
//!
//! 安全约束：清理时只删除路径以 [`TMP_SUFFIX`] 结尾的文件，路径本身也来自本表，
//! 绝不按目录扫描删除用户文件。

use anyhow::Context;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;

use crate::core::Result;

/// 传输中间文件的固定后缀；用户看到该后缀即可确认是失败残留的临时文件。
pub const TMP_SUFFIX: &str = ".EcoPasteSync.tmp";

/// 任务状态：排队中。
pub const STATUS_QUEUED: &str = "queued";
/// 任务状态：传输中。
pub const STATUS_RUNNING: &str = "running";
/// 任务状态：用户暂停（保留临时文件与进度，可续传）。
pub const STATUS_PAUSED: &str = "paused";
/// 任务状态：失败（保留临时文件，等待用户重试或取消）。
pub const STATUS_FAILED: &str = "failed";
/// 任务状态：全部完成。
pub const STATUS_DONE: &str = "done";
/// 任务状态：用户取消。
pub const STATUS_CANCELLED: &str = "cancelled";

/// 单个文件的传输进度记录。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferFileJournal {
    /// 实际读取来源：本地副本路径或远端文件的绝对路径。
    pub source: String,
    /// 远端文件的绝对路径（清单里的 `path`），用于回写本地副本快照。
    pub remote_path: String,
    /// 本地临时文件绝对路径（与最终文件同目录）。
    pub tmp: String,
    /// 传输完成后的最终绝对路径。
    pub final_path: String,
    /// 文件总字节数；目录为 0。
    pub size: u64,
    /// 已经写入临时文件的字节数，续传时从这里继续。
    pub done: u64,
    /// 是否为目录（只建目录，不写临时文件）。
    pub is_dir: bool,
    /// 是否复用本地副本（true 时不走网络）。
    pub from_local: bool,
    /// 是否顶层条目（顶层文件或目录根），用于汇总本地路径列表。
    pub is_top: bool,
    /// 发送端给出的整文件哈希；缺失时只校验长度。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
    /// 远端修改时间（Unix 毫秒）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mtime_ms: Option<i64>,
    /// 远端创建时间（Unix 毫秒）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ctime_ms: Option<i64>,
}

/// 一条传输任务的完整快照。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferJobRecord {
    pub job_id: String,
    pub item_id: String,
    pub device_id: String,
    /// 本次传输的目标目录（用户点粘贴时前台资源管理器所在目录）。
    pub target_dir: String,
    pub status: String,
    pub total_bytes: u64,
    pub done_bytes: u64,
    pub files: Vec<TransferFileJournal>,
}

/// 新建任务并落库。
pub async fn insert(pool: &SqlitePool, job: &TransferJobRecord) -> Result<()> {
    let now = Utc::now();
    let files_json = serde_json::to_string(&job.files)
        .map_err(|err| crate::core::AppError::Other(err.into()))?;

    sqlx::query(
        "INSERT INTO sync_transfer_jobs \
         (job_id, item_id, device_id, target_dir, status, total_bytes, done_bytes, \
          files_json, created_at, updated_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&job.job_id)
    .bind(&job.item_id)
    .bind(&job.device_id)
    .bind(&job.target_dir)
    .bind(&job.status)
    .bind(job.total_bytes as i64)
    .bind(job.done_bytes as i64)
    .bind(files_json)
    .bind(now)
    .bind(now)
    .execute(pool)
    .await
    .context("failed to insert sync transfer job")?;

    Ok(())
}

/// 更新进度：整体已完成字节数 + 每文件已完成字节数。
pub async fn update_progress(
    pool: &SqlitePool,
    job_id: &str,
    done_bytes: u64,
    files: &[TransferFileJournal],
) -> Result<()> {
    let files_json =
        serde_json::to_string(files).map_err(|err| crate::core::AppError::Other(err.into()))?;

    sqlx::query(
        "UPDATE sync_transfer_jobs SET done_bytes = ?, files_json = ?, updated_at = ? \
         WHERE job_id = ?",
    )
    .bind(done_bytes as i64)
    .bind(files_json)
    .bind(Utc::now())
    .bind(job_id)
    .execute(pool)
    .await
    .context("failed to update sync transfer job progress")?;

    Ok(())
}

/// 更新任务状态。
pub async fn set_status(pool: &SqlitePool, job_id: &str, status: &str) -> Result<()> {
    sqlx::query("UPDATE sync_transfer_jobs SET status = ?, updated_at = ? WHERE job_id = ?")
        .bind(status)
        .bind(Utc::now())
        .bind(job_id)
        .execute(pool)
        .await
        .context("failed to update sync transfer job status")?;

    Ok(())
}

/// 读取单个任务快照。
pub async fn find(pool: &SqlitePool, job_id: &str) -> Result<Option<TransferJobRecord>> {
    let row = sqlx::query_as::<_, (String, String, String, String, String, i64, i64, String)>(
        "SELECT job_id, item_id, device_id, target_dir, status, total_bytes, done_bytes, \
         files_json FROM sync_transfer_jobs WHERE job_id = ?",
    )
    .bind(job_id)
    .fetch_optional(pool)
    .await
    .context("failed to query sync transfer job")?;

    let Some(row) = row else {
        return Ok(None);
    };
    Ok(Some(record_from_row(row)?))
}

/// 读取全部未完成任务（启动清理用）。
pub async fn list_unfinished(pool: &SqlitePool) -> Result<Vec<TransferJobRecord>> {
    let rows = sqlx::query_as::<_, (String, String, String, String, String, i64, i64, String)>(
        "SELECT job_id, item_id, device_id, target_dir, status, total_bytes, done_bytes, \
         files_json FROM sync_transfer_jobs WHERE status <> ? AND status <> ?",
    )
    .bind(STATUS_DONE)
    .bind(STATUS_CANCELLED)
    .fetch_all(pool)
    .await
    .context("failed to list unfinished sync transfer jobs")?;

    let mut jobs = Vec::with_capacity(rows.len());
    for row in rows {
        jobs.push(record_from_row(row)?);
    }
    Ok(jobs)
}

/// 删除任务记录。
pub async fn delete(pool: &SqlitePool, job_id: &str) -> Result<()> {
    sqlx::query("DELETE FROM sync_transfer_jobs WHERE job_id = ?")
        .bind(job_id)
        .execute(pool)
        .await
        .context("failed to delete sync transfer job")?;

    Ok(())
}

/// 启动时清理上次中断的传输。
///
/// 只删除 journal 里登记、且路径确实以 [`TMP_SUFFIX`] 结尾的临时文件；
/// 随后删除这些未完成任务行。返回清理的任务数。
pub async fn cleanup_interrupted(pool: &SqlitePool) -> Result<usize> {
    let jobs = list_unfinished(pool).await?;
    if jobs.is_empty() {
        return Ok(0);
    }

    for job in &jobs {
        for file in &job.files {
            if !file.is_dir && file.tmp.ends_with(TMP_SUFFIX) {
                if let Err(err) = tokio::fs::remove_file(&file.tmp).await {
                    if err.kind() != std::io::ErrorKind::NotFound {
                        log::warn!(
                            "sync transfer cleanup: remove temp file {:?} failed: {err}",
                            file.tmp
                        );
                    }
                }
            }
        }
        if let Err(err) = delete(pool, &job.job_id).await {
            log::warn!(
                "sync transfer cleanup: delete job {} failed: {err}",
                job.job_id
            );
        }
    }

    log::info!(
        "sync transfer cleanup: removed {} interrupted jobs and their temp files",
        jobs.len()
    );
    Ok(jobs.len())
}

/// 把数据库行还原为任务快照。
fn record_from_row(
    row: (String, String, String, String, String, i64, i64, String),
) -> Result<TransferJobRecord> {
    let (job_id, item_id, device_id, target_dir, status, total_bytes, done_bytes, files_json) = row;

    let files: Vec<TransferFileJournal> = serde_json::from_str(&files_json).unwrap_or_else(|err| {
        log::warn!("sync transfer job {job_id} has invalid files_json: {err}");
        Vec::new()
    });

    Ok(TransferJobRecord {
        job_id,
        item_id,
        device_id,
        target_dir,
        status,
        total_bytes: u64::try_from(total_bytes).unwrap_or(0),
        done_bytes: u64::try_from(done_bytes).unwrap_or(0),
        files,
    })
}
