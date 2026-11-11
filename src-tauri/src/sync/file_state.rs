//! 同步文件条目的传输状态仓储（`sync_file_states`）。
//!
//! 只有「超过本体传输阈值、仅同步路径」的文件条目才会落库：接收端据此知道哪些
//! 条目仍可按需拉取。传输不做断点续传，任何中断都丢弃半成品、下次从头再拉。

use anyhow::Context;
use chrono::Utc;
use sqlx::SqlitePool;

use crate::core::Result;
use crate::sync::protocol::FILE_CHUNK_SIZE;

/// 文件条目在本机的可用状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncFileState {
    /// 尚未拉取，粘贴时可发起按需拉取。
    Pending,
    /// 不可拉取（超出上限、指纹缺失或源文件已变化），仅路径可用。
    Unavailable,
}

impl SyncFileState {
    /// 落库用的小写标签，与其它枚举列同一约定。
    pub fn as_tag(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Unavailable => "unavailable",
        }
    }
}

/// 写入（或覆盖）一条文件条目的传输状态。重复收到同一条目时以最新快照为准。
pub async fn upsert(
    pool: &SqlitePool,
    item_id: &str,
    device_id: &str,
    state: SyncFileState,
    total_bytes: i64,
    file_count: i64,
) -> Result<()> {
    let now = Utc::now();

    sqlx::query(
        "INSERT INTO sync_file_states (item_id, device_id, state, total_bytes, \
         transferred_bytes, chunk_size, file_count, chunk_bitmap, created_at, updated_at) \
         VALUES (?, ?, ?, ?, 0, ?, ?, '', ?, ?) \
         ON CONFLICT(item_id) DO UPDATE SET \
         device_id = excluded.device_id, state = excluded.state, \
         total_bytes = excluded.total_bytes, chunk_size = excluded.chunk_size, \
         file_count = excluded.file_count, updated_at = excluded.updated_at",
    )
    .bind(item_id)
    .bind(device_id)
    .bind(state.as_tag())
    .bind(total_bytes)
    .bind(FILE_CHUNK_SIZE)
    .bind(file_count)
    .bind(now)
    .bind(now)
    .execute(pool)
    .await
    .context("failed to upsert sync file state")?;

    Ok(())
}
