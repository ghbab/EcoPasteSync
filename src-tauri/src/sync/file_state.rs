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

    /// 从落库标签还原状态；未知标签按不可拉取处理。
    fn from_tag(tag: &str) -> Self {
        match tag {
            "pending" => Self::Pending,
            _ => Self::Unavailable,
        }
    }
}

/// 一条远端文件条目的传输状态快照。
#[derive(Debug, Clone)]
pub struct SyncFileRecord {
    pub device_id: String,
    pub state: SyncFileState,
    /// 发送端推送的每文件指纹（JSON），按需拉取的唯一依据。
    pub files_json: Option<String>,
    /// 最近一次拉取成功后的本地缓存路径列表（JSON）。
    pub local_paths_json: Option<String>,
}

/// 写入（或覆盖）一条文件条目的传输状态。重复收到同一条目时以最新快照为准。
pub async fn upsert(
    pool: &SqlitePool,
    item_id: &str,
    device_id: &str,
    state: SyncFileState,
    total_bytes: i64,
    file_count: i64,
    files_json: Option<&str>,
) -> Result<()> {
    let now = Utc::now();

    sqlx::query(
        "INSERT INTO sync_file_states (item_id, device_id, state, total_bytes, \
         transferred_bytes, chunk_size, file_count, chunk_bitmap, files_json, local_paths_json, \
         created_at, updated_at) \
         VALUES (?, ?, ?, ?, 0, ?, ?, '', ?, NULL, ?, ?) \
         ON CONFLICT(item_id) DO UPDATE SET \
         device_id = excluded.device_id, state = excluded.state, \
         total_bytes = excluded.total_bytes, chunk_size = excluded.chunk_size, \
         file_count = excluded.file_count, files_json = excluded.files_json, \
         local_paths_json = NULL, updated_at = excluded.updated_at",
    )
    .bind(item_id)
    .bind(device_id)
    .bind(state.as_tag())
    .bind(total_bytes)
    .bind(FILE_CHUNK_SIZE)
    .bind(file_count)
    .bind(files_json)
    .bind(now)
    .bind(now)
    .execute(pool)
    .await
    .context("failed to upsert sync file state")?;

    Ok(())
}

/// 读取条目对应的远端文件状态；本机条目返回 `None`。
pub async fn find(pool: &SqlitePool, item_id: &str) -> Result<Option<SyncFileRecord>> {
    let row = sqlx::query_as::<_, (String, String, Option<String>, Option<String>)>(
        "SELECT device_id, state, files_json, local_paths_json \
         FROM sync_file_states WHERE item_id = ?",
    )
    .bind(item_id)
    .fetch_optional(pool)
    .await
    .context("failed to query sync file state")?;

    Ok(row.map(
        |(device_id, state, files_json, local_paths_json)| SyncFileRecord {
            device_id,
            state: SyncFileState::from_tag(&state),
            files_json,
            local_paths_json,
        },
    ))
}

/// 记录拉取成功后的本地缓存路径，后续粘贴可直接复用。
pub async fn set_local_paths(
    pool: &SqlitePool,
    item_id: &str,
    local_paths_json: &str,
) -> Result<()> {
    sqlx::query(
        "UPDATE sync_file_states SET local_paths_json = ?, updated_at = ? WHERE item_id = ?",
    )
    .bind(local_paths_json)
    .bind(Utc::now())
    .bind(item_id)
    .execute(pool)
    .await
    .context("failed to update sync file local paths")?;

    Ok(())
}
