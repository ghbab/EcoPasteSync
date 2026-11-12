//! 局域网同步独立数据库：已配对设备、黑名单、文件传输状态与条目来源。
//!
//! 上游 EcoPaste 与 fork 共用同一个 `clipboard.db`，而 fork 的 migration 版本会让
//! 上游启动时因版本缺失直接失败。这里把 fork 专属表放进独立文件，主库只保留上游
//! 的 `0001_init.sql`；跨库关联（条目来源）在应用层按 `item_id` 拼装。

use std::path::PathBuf;

use anyhow::Context;
use chrono::Utc;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{ConnectOptions, Row, SqlitePool};
use tauri::AppHandle;

use crate::core::Result;

/// 同步库文件名，与主库同目录，WAL sidecar 也随目录落盘。
const SYNC_DB_FILENAME: &str = "sync.db";
/// 旧版 fork 写入主库的首个 migration 版本；拆分后必须从主库删除这些记录。
const FIRST_FORK_MIGRATION_VERSION: i64 = 2;
/// 旧版 fork 写入主库的最后一个 migration 版本。
///
/// 只清理这个已知区间，避免把上游未来新增的 migration 记录一起删掉。
const LAST_FORK_MIGRATION_VERSION: i64 = 6;

/// 同步库连接池的 Tauri 托管状态。
///
/// 与主库 `DatabaseState` 分开：主库可能在备份覆盖导入时热替换，同步库不参与替换；
/// 但切换数据目录时需要一起换到新目录，因此用互斥量持有当前连接池。
pub struct SyncDatabaseState {
    pool: tokio::sync::Mutex<SqlitePool>,
}

impl SyncDatabaseState {
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool: tokio::sync::Mutex::new(pool),
        }
    }

    pub async fn pool(&self) -> SqlitePool {
        self.pool.lock().await.clone()
    }

    /// 数据目录切换后把连接池指向新目录的同步库。
    pub async fn replace(&self, next: SqlitePool) {
        *self.pool.lock().await = next;
    }
}

/// 同步库文件路径。
pub fn sync_db_path(app: &AppHandle) -> Result<PathBuf> {
    let dir = crate::core::paths::db_dir(app)?;

    std::fs::create_dir_all(&dir).with_context(|| format!("failed to create db dir at {dir:?}"))?;

    Ok(dir.join(SYNC_DB_FILENAME))
}

/// 打开同步库并应用同步专属 migration。
pub async fn init(app: &AppHandle) -> Result<SqlitePool> {
    let path = sync_db_path(app)?;

    let options = SqliteConnectOptions::new()
        .filename(&path)
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .foreign_keys(true)
        .disable_statement_logging();

    let pool = SqlitePoolOptions::new()
        .connect_with(options)
        .await
        .with_context(|| format!("failed to open sync sqlite database at {path:?}"))?;

    sqlx::migrate!("./migrations_sync")
        .run(&pool)
        .await
        .context("failed to run sync sqlite migrations")?;

    log::info!("sync sqlite pool ready at {path:?}");
    Ok(pool)
}

/// 把旧版 fork 留在主库里的同步数据搬到同步库，并清理主库的 fork 结构。
///
/// 必须先于主库 migration 执行：旧主库的 `_sqlx_migrations` 带有 2..=6 的记录，
/// 而这些脚本已从主库 migration 目录移除，不清理会让 sqlx 报版本缺失。
/// 拆分本身幂等：复制使用 `INSERT OR REPLACE`，清理全部带 `IF EXISTS`。
pub async fn migrate_legacy(main: &SqlitePool, sync: &SqlitePool) -> Result<()> {
    // 逐项探测旧结构；全新安装时全部为 false，直接跳过。
    let has_devices = table_exists(main, "sync_devices").await?;
    let has_blacklist = table_exists(main, "sync_blacklist").await?;
    let has_file_states = table_exists(main, "sync_file_states").await?;
    let has_source_column = column_exists(main, "clipboard_items", "sync_source_device_id").await?;
    let has_fork_migrations = migration_rows_after(main, FIRST_FORK_MIGRATION_VERSION).await?;

    if !has_devices
        && !has_blacklist
        && !has_file_states
        && !has_source_column
        && !has_fork_migrations
    {
        return Ok(());
    }

    log::info!(
        "splitting legacy sync data out of main database: devices={has_devices}, \
         blacklist={has_blacklist}, fileStates={has_file_states}, \
         sourceColumn={has_source_column}, forkMigrations={has_fork_migrations}"
    );

    // 先复制（幂等），失败时主库保持原样，下次启动可重试。
    if has_devices {
        copy_sync_devices(main, sync).await?;
    }
    if has_blacklist {
        copy_sync_blacklist(main, sync).await?;
    }
    if has_file_states {
        copy_sync_file_states(main, sync).await?;
    }
    if has_source_column {
        copy_sync_item_meta(main, sync).await?;
    }

    // 复制成功后再清理主库，避免中途失败丢数据。
    cleanup_main(
        main,
        has_devices,
        has_blacklist,
        has_file_states,
        has_source_column,
        has_fork_migrations,
    )
    .await?;

    log::info!("legacy sync data split finished");
    Ok(())
}

/// 主库是否存在指定表。
async fn table_exists(pool: &SqlitePool, table: &str) -> Result<bool> {
    let row = sqlx::query("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?")
        .bind(table)
        .fetch_optional(pool)
        .await
        .map_err(|err| crate::core::AppError::Other(err.into()))?;

    Ok(row.is_some())
}

/// 主库指定表是否存在某列。
async fn column_exists(pool: &SqlitePool, table: &str, column: &str) -> Result<bool> {
    if !table_exists(pool, table).await? {
        return Ok(false);
    }

    // 表名只来自代码内常量，不存在注入面；sqlx 0.9 要求动态 SQL 显式标记为安全。
    let sql = format!("PRAGMA table_info({table})");
    let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
        .fetch_all(pool)
        .await
        .map_err(|err| crate::core::AppError::Other(err.into()))?;

    for row in rows {
        let name: String = row
            .try_get("name")
            .map_err(|err| crate::core::AppError::Other(err.into()))?;

        if name == column {
            return Ok(true);
        }
    }

    Ok(false)
}

/// 主库是否还有指定版本之后的 migration 记录。
async fn migration_rows_after(pool: &SqlitePool, version: i64) -> Result<bool> {
    if !table_exists(pool, "_sqlx_migrations").await? {
        return Ok(false);
    }

    let row = sqlx::query("SELECT 1 FROM _sqlx_migrations WHERE version >= ? LIMIT 1")
        .bind(version)
        .fetch_optional(pool)
        .await
        .map_err(|err| crate::core::AppError::Other(err.into()))?;

    Ok(row.is_some())
}

/// 复制已配对设备。
async fn copy_sync_devices(main: &SqlitePool, sync: &SqlitePool) -> Result<()> {
    let rows = sqlx::query(
        "SELECT device_id, name, platform, shared_key, allow_send, allow_receive, \
         last_seen_at, last_address, last_pull_at, created_at, updated_at FROM sync_devices",
    )
    .fetch_all(main)
    .await
    .map_err(|err| crate::core::AppError::Other(err.into()))?;

    for row in rows {
        let device_id: String = read_column(&row, "device_id")?;
        let name: String = read_column(&row, "name")?;
        let platform: String = read_column(&row, "platform")?;
        let shared_key: String = read_column(&row, "shared_key")?;
        let allow_send: i64 = read_column(&row, "allow_send")?;
        let allow_receive: i64 = read_column(&row, "allow_receive")?;
        let last_seen_at: Option<String> = read_column(&row, "last_seen_at")?;
        let last_address: Option<String> = read_column(&row, "last_address")?;
        let last_pull_at: Option<String> = read_column(&row, "last_pull_at")?;
        let created_at: String = read_column(&row, "created_at")?;
        let updated_at: String = read_column(&row, "updated_at")?;

        sqlx::query(
            "INSERT OR REPLACE INTO sync_devices \
             (device_id, name, platform, shared_key, allow_send, allow_receive, \
              last_seen_at, last_address, last_pull_at, created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(device_id)
        .bind(name)
        .bind(platform)
        .bind(shared_key)
        .bind(allow_send)
        .bind(allow_receive)
        .bind(last_seen_at)
        .bind(last_address)
        .bind(last_pull_at)
        .bind(created_at)
        .bind(updated_at)
        .execute(sync)
        .await
        .map_err(|err| crate::core::AppError::Other(err.into()))?;
    }

    Ok(())
}

/// 复制黑名单。
async fn copy_sync_blacklist(main: &SqlitePool, sync: &SqlitePool) -> Result<()> {
    let rows = sqlx::query("SELECT device_id, created_at, updated_at FROM sync_blacklist")
        .fetch_all(main)
        .await
        .map_err(|err| crate::core::AppError::Other(err.into()))?;

    for row in rows {
        let device_id: String = read_column(&row, "device_id")?;
        let created_at: String = read_column(&row, "created_at")?;
        let updated_at: String = read_column(&row, "updated_at")?;

        sqlx::query(
            "INSERT OR REPLACE INTO sync_blacklist (device_id, created_at, updated_at) \
             VALUES (?, ?, ?)",
        )
        .bind(device_id)
        .bind(created_at)
        .bind(updated_at)
        .execute(sync)
        .await
        .map_err(|err| crate::core::AppError::Other(err.into()))?;
    }

    Ok(())
}

/// 复制文件传输状态；旧库可能还没跑过 payload 列 migration。
async fn copy_sync_file_states(main: &SqlitePool, sync: &SqlitePool) -> Result<()> {
    let has_payloads = column_exists(main, "sync_file_states", "files_json").await?;
    let sql = if has_payloads {
        "SELECT item_id, device_id, state, total_bytes, transferred_bytes, chunk_size, \
         file_count, chunk_bitmap, cache_dir, files_json, local_paths_json, created_at, \
         updated_at FROM sync_file_states"
    } else {
        "SELECT item_id, device_id, state, total_bytes, transferred_bytes, chunk_size, \
         file_count, chunk_bitmap, cache_dir, NULL AS files_json, NULL AS local_paths_json, \
         created_at, updated_at FROM sync_file_states"
    };

    let rows = sqlx::query(sql)
        .fetch_all(main)
        .await
        .map_err(|err| crate::core::AppError::Other(err.into()))?;

    for row in rows {
        let item_id: String = read_column(&row, "item_id")?;
        let device_id: String = read_column(&row, "device_id")?;
        let state: String = read_column(&row, "state")?;
        let total_bytes: i64 = read_column(&row, "total_bytes")?;
        let transferred_bytes: i64 = read_column(&row, "transferred_bytes")?;
        let chunk_size: i64 = read_column(&row, "chunk_size")?;
        let file_count: i64 = read_column(&row, "file_count")?;
        let chunk_bitmap: String = read_column(&row, "chunk_bitmap")?;
        let cache_dir: Option<String> = read_column(&row, "cache_dir")?;
        let files_json: Option<String> = read_column(&row, "files_json")?;
        let local_paths_json: Option<String> = read_column(&row, "local_paths_json")?;
        let created_at: String = read_column(&row, "created_at")?;
        let updated_at: String = read_column(&row, "updated_at")?;

        sqlx::query(
            "INSERT OR REPLACE INTO sync_file_states \
             (item_id, device_id, state, total_bytes, transferred_bytes, chunk_size, \
              file_count, chunk_bitmap, cache_dir, files_json, local_paths_json, \
              created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(item_id)
        .bind(device_id)
        .bind(state)
        .bind(total_bytes)
        .bind(transferred_bytes)
        .bind(chunk_size)
        .bind(file_count)
        .bind(chunk_bitmap)
        .bind(cache_dir)
        .bind(files_json)
        .bind(local_paths_json)
        .bind(created_at)
        .bind(updated_at)
        .execute(sync)
        .await
        .map_err(|err| crate::core::AppError::Other(err.into()))?;
    }

    Ok(())
}

/// 把主库 `clipboard_items.sync_source_device_id` 搬到 `sync_item_meta`。
async fn copy_sync_item_meta(main: &SqlitePool, sync: &SqlitePool) -> Result<()> {
    let rows = sqlx::query(
        "SELECT id, sync_source_device_id FROM clipboard_items \
                     WHERE sync_source_device_id IS NOT NULL AND sync_source_device_id <> ''",
    )
    .fetch_all(main)
    .await
    .map_err(|err| crate::core::AppError::Other(err.into()))?;

    let now = Utc::now().to_rfc3339();

    for row in rows {
        let item_id: String = read_column(&row, "id")?;
        let source_device_id: String = read_column(&row, "sync_source_device_id")?;

        sqlx::query(
            "INSERT OR REPLACE INTO sync_item_meta \
             (item_id, source_device_id, created_at, updated_at) VALUES (?, ?, ?, ?)",
        )
        .bind(item_id)
        .bind(source_device_id)
        .bind(&now)
        .bind(&now)
        .execute(sync)
        .await
        .map_err(|err| crate::core::AppError::Other(err.into()))?;
    }

    Ok(())
}

/// 删除主库里的 fork 表、来源列与 migration 记录。
async fn cleanup_main(
    main: &SqlitePool,
    has_devices: bool,
    has_blacklist: bool,
    has_file_states: bool,
    has_source_column: bool,
    has_fork_migrations: bool,
) -> Result<()> {
    let mut tx = main
        .begin()
        .await
        .map_err(|err| crate::core::AppError::Other(err.into()))?;

    if has_file_states {
        sqlx::query("DROP TABLE IF EXISTS sync_file_states")
            .execute(&mut *tx)
            .await
            .map_err(|err| crate::core::AppError::Other(err.into()))?;
    }
    if has_blacklist {
        sqlx::query("DROP TABLE IF EXISTS sync_blacklist")
            .execute(&mut *tx)
            .await
            .map_err(|err| crate::core::AppError::Other(err.into()))?;
    }
    if has_devices {
        sqlx::query("DROP TABLE IF EXISTS sync_devices")
            .execute(&mut *tx)
            .await
            .map_err(|err| crate::core::AppError::Other(err.into()))?;
    }

    if has_source_column {
        // SQLite 3.35+ 支持 DROP COLUMN；失败时保留该列也不影响上游使用，
        // 因此只告警不中断迁移。
        if let Err(err) =
            sqlx::query("ALTER TABLE clipboard_items DROP COLUMN sync_source_device_id")
                .execute(&mut *tx)
                .await
        {
            log::warn!("drop legacy column sync_source_device_id failed, keeping it: {err}");
        }
    }

    if has_fork_migrations {
        sqlx::query("DELETE FROM _sqlx_migrations WHERE version >= ? AND version <= ?")
            .bind(FIRST_FORK_MIGRATION_VERSION)
            .bind(LAST_FORK_MIGRATION_VERSION)
            .execute(&mut *tx)
            .await
            .map_err(|err| crate::core::AppError::Other(err.into()))?;
    }

    tx.commit()
        .await
        .map_err(|err| crate::core::AppError::Other(err.into()))?;

    Ok(())
}

/// 从查询行读取指定列，统一转成仓储错误。
fn read_column<T>(row: &sqlx::sqlite::SqliteRow, name: &str) -> Result<T>
where
    T: for<'a> sqlx::Decode<'a, sqlx::Sqlite> + sqlx::Type<sqlx::Sqlite>,
{
    row.try_get(name)
        .map_err(|err| crate::core::AppError::Other(err.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_support::{memory_pool, sync_memory_pool};

    /// 旧版 fork 把同步表写进主库：拆分后数据进同步库，主库结构与 migration 记录被清理。
    #[tokio::test]
    async fn migrate_legacy_moves_fork_data_to_sync_database() {
        let main = memory_pool().await;
        let sync = sync_memory_pool().await;

        // 模拟旧版 fork 留在主库里的 `sync_devices` 表。
        sqlx::query(
            "CREATE TABLE sync_devices (device_id TEXT PRIMARY KEY, name TEXT NOT NULL, \
             platform TEXT NOT NULL, shared_key TEXT NOT NULL, \
             allow_send INTEGER NOT NULL DEFAULT 1, allow_receive INTEGER NOT NULL DEFAULT 1, \
             last_seen_at TEXT, last_address TEXT, last_pull_at TEXT, \
             created_at TEXT NOT NULL, updated_at TEXT NOT NULL)",
        )
        .execute(&main)
        .await
        .expect("create legacy sync_devices");

        sqlx::query(
            "INSERT INTO sync_devices \
             (device_id, name, platform, shared_key, created_at, updated_at) \
             VALUES ('peer', 'Peer', 'windows', 'ab', \
             '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00')",
        )
        .execute(&main)
        .await
        .expect("insert legacy device");

        // 模拟旧版 fork 的主库 migration 记录。
        sqlx::query(
            "INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time) \
             VALUES (2, 'legacy fork', 1, x'00', 0)",
        )
        .execute(&main)
        .await
        .expect("insert legacy migration row");

        migrate_legacy(&main, &sync).await.expect("migrate legacy");

        let moved: (String,) =
            sqlx::query_as("SELECT device_id FROM sync_devices WHERE device_id = 'peer'")
                .fetch_one(&sync)
                .await
                .expect("device moved to sync database");
        assert_eq!(moved.0, "peer");

        let legacy_table = sqlx::query_as::<_, (String,)>(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'sync_devices'",
        )
        .fetch_optional(&main)
        .await
        .expect("query main tables");
        assert!(legacy_table.is_none(), "legacy table removed from main db");

        let legacy_migration =
            sqlx::query_as::<_, (i64,)>("SELECT version FROM _sqlx_migrations WHERE version = 2")
                .fetch_optional(&main)
                .await
                .expect("query main migrations");
        assert!(
            legacy_migration.is_none(),
            "fork migration row removed from main db"
        );
    }
}
