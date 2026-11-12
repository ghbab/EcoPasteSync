use anyhow::Context;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::ConnectOptions;
use sqlx::SqlitePool;
use tauri::AppHandle;

use crate::core::Result;
use crate::db::db_path;

/// 打开主库连接池，不执行 migration。
///
/// 启动流程需要先打开主库探测旧版 fork 结构，再决定是否把同步数据拆到独立库，
/// 最后才应用 migration，因此拆成独立步骤。
pub async fn open(app: &AppHandle) -> Result<SqlitePool> {
    let path = db_path(app)?;

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
        .with_context(|| format!("failed to open sqlite database at {path:?}"))?;

    log::info!("sqlite pool ready at {path:?}");
    Ok(pool)
}

/// 对主库应用 migration。
///
/// fork 专属表已拆到 `sync.db`，主库目录只保留上游 `0001_init.sql`，
/// 这样原版 EcoPaste 打开同一个库时 migration 列表仍然完整。
pub async fn migrate(pool: &SqlitePool) -> Result<()> {
    sqlx::migrate!("./migrations")
        .run(pool)
        .await
        .context("failed to run sqlite migrations")?;

    Ok(())
}

pub async fn init(app: &AppHandle) -> Result<SqlitePool> {
    let pool = open(app).await?;

    migrate(&pool).await?;

    Ok(pool)
}
