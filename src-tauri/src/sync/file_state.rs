//! 同步文件条目的传输状态仓储（`sync_file_states`）。
//!
//! 只有「超过本体传输阈值、仅同步路径」的文件条目才会落库：接收端据此知道哪些
//! 条目仍可按需拉取、已完成到哪一块。块级进度以位图字符串保存，重启后可续传。

use anyhow::Context;
use chrono::Utc;
use sqlx::SqlitePool;

use crate::core::Result;

/// 分块大小（字节）。与协议层保持一致，重传粒度适中。
pub const FILE_CHUNK_SIZE: i64 = 1024 * 1024;

/// 文件条目在本机的可用状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncFileState {
    /// 尚未开始拉取。
    Pending,
    /// 拉取中断，已完成块已落盘，可续传。
    Partial,
    /// 文件已在本机可用（本地已存在或拉取完成）。
    Available,
    /// 不可拉取（超出上限、指纹缺失或源文件已变化），仅路径可用。
    Unavailable,
}

impl SyncFileState {
    /// 落库用的小写标签，与其它枚举列同一约定。
    pub fn as_tag(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Partial => "partial",
            Self::Available => "available",
            Self::Unavailable => "unavailable",
        }
    }

    /// 反解析；未知标签按 `Unavailable` 处理（保守：不主动发起拉取）。
    fn from_tag(tag: &str) -> Self {
        match tag {
            "pending" => Self::Pending,
            "partial" => Self::Partial,
            "available" => Self::Available,
            _ => Self::Unavailable,
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

/// 查询条目的传输状态；未登记过（例如本地创建的条目）返回 `None`。
pub async fn find_state(pool: &SqlitePool, item_id: &str) -> Result<Option<SyncFileState>> {
    let tag: Option<String> =
        sqlx::query_scalar("SELECT state FROM sync_file_states WHERE item_id = ?")
            .bind(item_id)
            .fetch_optional(pool)
            .await
            .context("failed to query sync file state")?;

    Ok(tag.map(|tag| SyncFileState::from_tag(&tag)))
}
