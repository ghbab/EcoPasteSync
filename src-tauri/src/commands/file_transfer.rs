//! 文件传输进度窗口的命令入口。
//!
//! 传输本身由 [`crate::sync::file_transfer`] 在后台执行，这里只负责：
//! 窗口拉取快照、暂停 / 继续、取消、冲突策略选择、失败后改存目录、仅复制路径。

use std::path::PathBuf;
use std::sync::Arc;

use tauri::{AppHandle, State};

use crate::clipboard::{ImageStore, WritebackGuard};
use crate::core::{AppError, Result};
use crate::db::items::find_item_by_id;
use crate::db::DatabaseState;
use crate::sync::file_transfer::{self, TransferSnapshot};

/// 读取最近一次传输任务快照（窗口首帧加载时使用）。
#[tauri::command]
pub fn get_latest_file_transfer() -> Option<TransferSnapshot> {
    file_transfer::latest_snapshot()
}

/// 按任务 id 读取快照。
#[tauri::command]
pub fn get_file_transfer_snapshot(job_id: String) -> Option<TransferSnapshot> {
    file_transfer::snapshot(&job_id)
}

/// 暂停传输（保留临时文件与进度，可继续）。
#[tauri::command]
pub async fn pause_file_transfer(app: AppHandle, job_id: String) -> Result<()> {
    file_transfer::set_paused(&app, &job_id, true).await
}

/// 继续传输（从断点续传）。
#[tauri::command]
pub async fn resume_file_transfer(app: AppHandle, job_id: String) -> Result<()> {
    file_transfer::set_paused(&app, &job_id, false).await
}

/// 取消传输：删除临时文件并结束任务。
#[tauri::command]
pub async fn cancel_file_transfer(app: AppHandle, job_id: String) -> Result<()> {
    file_transfer::cancel(&app, &job_id).await
}

/// 解决同名冲突。
///
/// `policy`：`overwrite` / `skip` / `keepBoth` / `custom`；
/// `custom` 时 `overwrite` 列出需要覆盖的冲突顶层路径，其余保留两者。
#[tauri::command]
pub async fn resolve_file_transfer_conflicts(
    app: AppHandle,
    job_id: String,
    policy: String,
    overwrite: Option<Vec<String>>,
) -> Result<()> {
    let overwrite_paths = overwrite.unwrap_or_default();
    file_transfer::resolve_conflicts(&app, &job_id, &policy, &overwrite_paths).await
}

/// 失败后改存到其它目录。
#[tauri::command]
pub async fn retry_file_transfer_to_dir(
    app: AppHandle,
    job_id: String,
    directory: String,
) -> Result<()> {
    file_transfer::retry_with_dir(&app, &job_id, PathBuf::from(directory)).await
}

/// 兜底入口：只把该条目对应的远端原始路径写到剪贴板，不传输文件。
#[tauri::command]
pub async fn copy_file_transfer_paths(
    db: State<'_, DatabaseState>,
    store: State<'_, ImageStore>,
    guard: State<'_, Arc<WritebackGuard>>,
    job_id: String,
) -> Result<()> {
    let snapshot = file_transfer::snapshot(&job_id)
        .ok_or_else(|| AppError::Other(anyhow::anyhow!("transfer job not found")))?;

    let pool = db.pool().await;
    let item = find_item_by_id(&pool, &snapshot.item_id)
        .await?
        .ok_or_else(|| AppError::Other(anyhow::anyhow!("clipboard item not found")))?;

    // 强制按文本写回：files 条目此时写的是远端原始路径串，而不是 CF_HDROP。
    crate::clipboard::write_to_clipboard(&store, guard.inner().as_ref(), &item, true)?;

    Ok(())
}
