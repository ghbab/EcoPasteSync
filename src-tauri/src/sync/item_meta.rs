//! 同步入库条目的来源设备（`sync_item_meta`）。
//!
//! 数据位于独立同步库，与主库 `clipboard_items` 按 `item_id` 关联：
//! - 列表标记条目来自远端设备；
//! - 文件拉取只允许本机产生的条目登记路径，防止已配对设备借拉取接口读取本机文件。

use std::collections::HashMap;

use chrono::Utc;
use sqlx::{QueryBuilder, Sqlite, SqlitePool};

use crate::core::{AppError, Result};

/// 记录条目来自哪台已配对设备；重复写入时覆盖。
pub async fn set_source(pool: &SqlitePool, item_id: &str, source_device_id: &str) -> Result<()> {
    let now = Utc::now().to_rfc3339();

    sqlx::query(
        "INSERT INTO sync_item_meta (item_id, source_device_id, created_at, updated_at) \
         VALUES (?, ?, ?, ?) \
         ON CONFLICT(item_id) DO UPDATE SET source_device_id = excluded.source_device_id, \
         updated_at = excluded.updated_at",
    )
    .bind(item_id)
    .bind(source_device_id)
    .bind(&now)
    .bind(&now)
    .execute(pool)
    .await
    .map_err(|err| AppError::Other(err.into()))?;

    Ok(())
}

/// 查询单条条目的来源设备；本机条目返回 `None`。
pub async fn source_of(pool: &SqlitePool, item_id: &str) -> Result<Option<String>> {
    let row: Option<(String,)> =
        sqlx::query_as("SELECT source_device_id FROM sync_item_meta WHERE item_id = ?")
            .bind(item_id)
            .fetch_optional(pool)
            .await
            .map_err(|err| AppError::Other(err.into()))?;

    Ok(row.map(|(device_id,)| device_id))
}

/// 批量查询来源设备，返回 `item_id → source_device_id`；本机条目不出现在结果里。
pub async fn sources_for(
    pool: &SqlitePool,
    item_ids: &[String],
) -> Result<HashMap<String, String>> {
    if item_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new(
        "SELECT item_id, source_device_id FROM sync_item_meta WHERE item_id IN (",
    );
    let mut separated = qb.separated(", ");
    for item_id in item_ids {
        separated.push_bind(item_id);
    }
    separated.push_unseparated(")");

    let rows: Vec<(String, String)> = qb
        .build_query_as()
        .fetch_all(pool)
        .await
        .map_err(|err| AppError::Other(err.into()))?;

    Ok(rows.into_iter().collect())
}

/// 删除条目的来源记录；不存在时 no-op。
pub async fn remove_for_item(pool: &SqlitePool, item_id: &str) -> Result<()> {
    sqlx::query("DELETE FROM sync_item_meta WHERE item_id = ?")
        .bind(item_id)
        .execute(pool)
        .await
        .map_err(|err| AppError::Other(err.into()))?;

    Ok(())
}

/// 清空全部来源记录。
///
/// 备份覆盖导入会替换整个主库，旧来源标记全部失效；继续保留会让备份里的
/// files 条目被误判为远端条目（或被误判为本机可拉取），因此整体清空。
pub async fn clear(pool: &SqlitePool) -> Result<()> {
    sqlx::query("DELETE FROM sync_item_meta")
        .execute(pool)
        .await
        .map_err(|err| AppError::Other(err.into()))?;

    Ok(())
}
