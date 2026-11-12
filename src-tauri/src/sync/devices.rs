//! 同步设备仓储：`sync_devices`（已配对互信设备）与 `sync_blacklist`（忽略名单）的 CRUD。
//!
//! 全部走 `SqlitePool`，遵循项目 sqlx 约定（`query` / `query_as`，不用宏）。
//! 共享密钥存系统钥匙串；认证热路径优先读进程内缓存，避免逐请求访问钥匙串。

#[cfg(not(test))]
use std::collections::HashMap;
#[cfg(not(test))]
use std::sync::{LazyLock, RwLock};

use chrono::{DateTime, Utc};
use sqlx::{QueryBuilder, Sqlite, SqlitePool};
use tauri::AppHandle;

use crate::core::{AppError, Result};
use crate::db::models::Platform;
use crate::sync::secrets;

/// 进程内共享密钥缓存（device_id → 密钥 hex）。
///
/// 系统钥匙串读取是同步阻塞调用；认证是每个同步请求的热路径，逐请求读取会
/// 阻塞异步 worker，局域网内高频请求即可放大为资源耗尽。缓存只在配对写入与
/// 解除配对时更新，进程重启后按需回填。
#[cfg(not(test))]
static SHARED_KEY_CACHE: LazyLock<RwLock<HashMap<String, String>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// 读取共享密钥：优先缓存，未命中再访问系统钥匙串并回填。
#[cfg(not(test))]
fn load_shared_key(device_id: &str) -> Result<Option<String>> {
    if let Some(cached) = SHARED_KEY_CACHE
        .read()
        .expect("shared key cache poisoned")
        .get(device_id)
        .cloned()
    {
        return Ok(Some(cached));
    }

    let loaded = secrets::load(device_id)?;
    if let Some(key) = loaded.as_ref() {
        SHARED_KEY_CACHE
            .write()
            .expect("shared key cache poisoned")
            .insert(device_id.to_owned(), key.clone());
    }

    Ok(loaded)
}

/// 测试构建直接读内存钥匙串，避免全局缓存在并行用例间串数据。
#[cfg(test)]
fn load_shared_key(device_id: &str) -> Result<Option<String>> {
    secrets::load(device_id)
}

/// 配对写入后同步缓存，认证热路径立即命中。
#[cfg(not(test))]
fn cache_shared_key(device_id: &str, key: &str) {
    SHARED_KEY_CACHE
        .write()
        .expect("shared key cache poisoned")
        .insert(device_id.to_owned(), key.to_owned());
}

#[cfg(test)]
fn cache_shared_key(_device_id: &str, _key: &str) {}

/// 解除配对 / 拉黑后移除缓存，避免残留已失效密钥。
#[cfg(not(test))]
fn forget_shared_key(device_id: &str) {
    SHARED_KEY_CACHE
        .write()
        .expect("shared key cache poisoned")
        .remove(device_id);
}

#[cfg(test)]
fn forget_shared_key(_device_id: &str) {}

/// 已配对设备记录（DB 行结构，`shared_key` 不落库，放系统钥匙串）。
#[derive(Debug, Clone)]
pub struct SyncDeviceRecord {
    pub device_id: String,
    pub name: String,
    pub platform: Platform,
    /// 共享密钥 hex（64 字符），仅存在于内存与系统钥匙串。
    pub shared_key: String,
    pub allow_send: bool,
    pub allow_receive: bool,
    pub last_seen_at: Option<DateTime<Utc>>,
    /// 最近一次成功通信的 `host:port`，mDNS 失联时兜底。
    pub last_address: Option<String>,
    /// 最近一次 pull 游标（对端返回条目的最大 created_at）。
    pub last_pull_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

const SELECT_DEVICE: &str = "SELECT device_id, name, platform, allow_send, \
     allow_receive, last_seen_at, last_address, last_pull_at, created_at \
     FROM sync_devices";

/// platform 列以小写字符串存储，与 `clipboard_items.platform` 同一约定。
fn parse_platform(raw: &str) -> Platform {
    match raw {
        "macos" => Platform::Macos,
        _ => Platform::Windows,
    }
}

fn platform_tag(platform: Platform) -> &'static str {
    match platform {
        Platform::Macos => "macos",
        Platform::Windows => "windows",
    }
}

/// 新增或覆盖一条已配对设备（配对确认时调用）。
pub async fn upsert_device(pool: &SqlitePool, record: &SyncDeviceRecord) -> Result<()> {
    // 共享密钥只写系统钥匙串，数据库列保留但不存明文。
    secrets::store(&record.device_id, &record.shared_key)?;

    let now = Utc::now();
    sqlx::query(
        "INSERT INTO sync_devices \
         (device_id, name, platform, shared_key, allow_send, allow_receive, last_seen_at, \
          last_address, last_pull_at, created_at, updated_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(device_id) DO UPDATE SET \
           name = excluded.name, \
           platform = excluded.platform, \
           shared_key = excluded.shared_key, \
           allow_send = excluded.allow_send, \
           allow_receive = excluded.allow_receive, \
           last_seen_at = excluded.last_seen_at, \
           last_address = excluded.last_address, \
           last_pull_at = excluded.last_pull_at, \
           updated_at = excluded.updated_at",
    )
    .bind(record.device_id.as_str())
    .bind(record.name.as_str())
    .bind(platform_tag(record.platform))
    .bind("")
    .bind(record.allow_send)
    .bind(record.allow_receive)
    .bind(record.last_seen_at)
    .bind(record.last_address.as_deref())
    .bind(record.last_pull_at)
    .bind(record.created_at)
    .bind(now)
    .execute(pool)
    .await
    .map_err(|err| AppError::Other(err.into()))?;

    // SQL 成功后再回填缓存：数据库写入失败时不保留未落库设备的缓存。
    cache_shared_key(&record.device_id, &record.shared_key);
    Ok(())
}

/// 列出全部已配对设备，按名称排序（前端列表稳定展示）。
///
/// 钥匙串里没有密钥的记录（被系统清理或旧版本残留）不返回，前端按未配对处理。
pub async fn list_devices(pool: &SqlitePool) -> Result<Vec<SyncDeviceRecord>> {
    let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new(SELECT_DEVICE);
    qb.push(" ORDER BY name ASC");

    let rows = qb
        .build_query_as::<RawDeviceRow>()
        .fetch_all(pool)
        .await
        .map_err(|err| AppError::Other(err.into()))?;

    let mut devices = Vec::with_capacity(rows.len());
    for row in rows {
        if let Some(record) = row.into_record()? {
            devices.push(record);
        }
    }

    Ok(devices)
}

/// 按 device_id 查找已配对设备（服务端鉴权热路径）。
///
/// 钥匙串里没有密钥时返回 `None`：记录虽在但无法认证，应按未配对处理。
pub async fn find_device(pool: &SqlitePool, device_id: &str) -> Result<Option<SyncDeviceRecord>> {
    let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new(SELECT_DEVICE);
    qb.push(" WHERE device_id = ")
        .push_bind(device_id.to_owned());

    let row = qb
        .build_query_as::<RawDeviceRow>()
        .fetch_optional(pool)
        .await
        .map_err(|err| AppError::Other(err.into()))?;

    match row {
        Some(row) => row.into_record(),
        None => Ok(None),
    }
}

/// 删除已配对设备（解除配对）。返回是否存在。
pub async fn remove_device(pool: &SqlitePool, device_id: &str) -> Result<bool> {
    let result = sqlx::query("DELETE FROM sync_devices WHERE device_id = ?")
        .bind(device_id)
        .execute(pool)
        .await
        .map_err(|err| AppError::Other(err.into()))?;

    // 数据库记录可能早已不存在，但钥匙串条目仍要清理，避免残留密钥。
    // 先清缓存再删钥匙串：即使钥匙串删除失败，认证热路径也不会再命中旧密钥。
    forget_shared_key(device_id);
    secrets::remove(device_id)?;

    Ok(result.rows_affected() > 0)
}

/// 更新设备备注名。
pub async fn rename_device(pool: &SqlitePool, device_id: &str, name: &str) -> Result<()> {
    let name = name.trim();
    if name.is_empty() {
        return Err(AppError::Other(anyhow::anyhow!("device name is empty")));
    }

    sqlx::query("UPDATE sync_devices SET name = ?, updated_at = ? WHERE device_id = ?")
        .bind(name)
        .bind(Utc::now())
        .bind(device_id)
        .execute(pool)
        .await
        .map_err(|err| AppError::Other(err.into()))?;
    Ok(())
}

/// 更新单台设备的发送 / 接收许可（`None` 表示该字段不变）。
pub async fn set_device_permissions(
    pool: &SqlitePool,
    device_id: &str,
    allow_send: Option<bool>,
    allow_receive: Option<bool>,
) -> Result<()> {
    let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new("UPDATE sync_devices SET updated_at = ");
    qb.push_bind(Utc::now());
    if let Some(allow_send) = allow_send {
        qb.push(", allow_send = ").push_bind(allow_send);
    }
    if let Some(allow_receive) = allow_receive {
        qb.push(", allow_receive = ").push_bind(allow_receive);
    }
    qb.push(" WHERE device_id = ")
        .push_bind(device_id.to_owned());

    qb.build()
        .execute(pool)
        .await
        .map_err(|err| AppError::Other(err.into()))?;
    Ok(())
}

/// 更新设备在线信息（发现层每次 Resolved 调用）。
pub async fn touch_device(pool: &SqlitePool, device_id: &str, address: &str) -> Result<()> {
    sqlx::query(
        "UPDATE sync_devices \
         SET last_seen_at = ?, last_address = ?, updated_at = ? \
         WHERE device_id = ?",
    )
    .bind(Utc::now())
    .bind(address)
    .bind(Utc::now())
    .bind(device_id)
    .execute(pool)
    .await
    .map_err(|err| AppError::Other(err.into()))?;
    Ok(())
}

/// 推进 pull 游标。
pub async fn set_last_pull_at(
    pool: &SqlitePool,
    device_id: &str,
    pulled_until: DateTime<Utc>,
) -> Result<()> {
    sqlx::query("UPDATE sync_devices SET last_pull_at = ?, updated_at = ? WHERE device_id = ?")
        .bind(pulled_until)
        .bind(Utc::now())
        .bind(device_id)
        .execute(pool)
        .await
        .map_err(|err| AppError::Other(err.into()))?;
    Ok(())
}

/// 设备是否在黑名单。
pub async fn is_blacklisted(pool: &SqlitePool, device_id: &str) -> Result<bool> {
    let found: Option<(String,)> =
        sqlx::query_as("SELECT device_id FROM sync_blacklist WHERE device_id = ?")
            .bind(device_id)
            .fetch_optional(pool)
            .await
            .map_err(|err| AppError::Other(err.into()))?;
    Ok(found.is_some())
}

/// 拉黑 / 取消拉黑。拉黑时连带删除配对记录（对端视为陌生设备）。
pub async fn set_blacklisted(pool: &SqlitePool, device_id: &str, blocked: bool) -> Result<()> {
    if blocked {
        let now = Utc::now();
        sqlx::query(
            "INSERT INTO sync_blacklist (device_id, created_at, updated_at) VALUES (?, ?, ?) \
             ON CONFLICT(device_id) DO UPDATE SET updated_at = excluded.updated_at",
        )
        .bind(device_id)
        .bind(now)
        .bind(now)
        .execute(pool)
        .await
        .map_err(|err| AppError::Other(err.into()))?;
        remove_device(pool, device_id).await?;
    } else {
        sqlx::query("DELETE FROM sync_blacklist WHERE device_id = ?")
            .bind(device_id)
            .execute(pool)
            .await
            .map_err(|err| AppError::Other(err.into()))?;
    }
    Ok(())
}

/// 列出黑名单 device_id。
pub async fn list_blacklisted_ids(pool: &SqlitePool) -> Result<Vec<String>> {
    let rows: Vec<(String,)> = sqlx::query_as("SELECT device_id FROM sync_blacklist")
        .fetch_all(pool)
        .await
        .map_err(|err| AppError::Other(err.into()))?;
    Ok(rows.into_iter().map(|(id,)| id).collect())
}

/// DB 原始行：时间列以 TEXT 存储，这里显式映射 `DateTime<Utc>`。
#[derive(Debug, Clone, sqlx::FromRow)]
struct RawDeviceRow {
    device_id: String,
    name: String,
    platform: String,
    allow_send: bool,
    allow_receive: bool,
    last_seen_at: Option<DateTime<Utc>>,
    last_address: Option<String>,
    last_pull_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
}

impl RawDeviceRow {
    /// 转换为记录，并从系统钥匙串加载共享密钥。
    ///
    /// 钥匙串里没有密钥时返回 `None`：设备记录残留但无法认证，
    /// 应按未配对处理（允许重新配对，而不是永远 409）。
    fn into_record(self) -> Result<Option<SyncDeviceRecord>> {
        // 认证热路径走进程内缓存，避免每个同步请求都同步读取系统钥匙串。
        let Some(shared_key) = load_shared_key(&self.device_id)? else {
            return Ok(None);
        };

        Ok(Some(SyncDeviceRecord {
            device_id: self.device_id,
            name: self.name,
            platform: parse_platform(&self.platform),
            shared_key,
            allow_send: self.allow_send,
            allow_receive: self.allow_receive,
            last_seen_at: self.last_seen_at,
            last_address: self.last_address,
            last_pull_at: self.last_pull_at,
            created_at: self.created_at,
        }))
    }
}

/// 从 `AppHandle` 取连接池（同步命令 / 事件处理统一入口）。
pub async fn pool(app: &AppHandle) -> sqlx::SqlitePool {
    use tauri::Manager;

    app.state::<crate::db::DatabaseState>().pool().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_support::memory_pool;

    fn sample_record(device_id: &str) -> SyncDeviceRecord {
        let now = Utc::now();
        SyncDeviceRecord {
            device_id: device_id.to_owned(),
            name: format!("Device {device_id}"),
            platform: Platform::Windows,
            shared_key: "ab".repeat(32),
            allow_send: true,
            allow_receive: true,
            last_seen_at: None,
            last_address: Some("192.168.1.20:39782".to_owned()),
            last_pull_at: None,
            created_at: now,
        }
    }

    #[tokio::test]
    async fn upsert_find_rename_remove_roundtrip() {
        let pool = memory_pool().await;
        let record = sample_record("dev-1");
        upsert_device(&pool, &record).await.unwrap();

        let found = find_device(&pool, "dev-1").await.unwrap().unwrap();
        assert_eq!(found.name, "Device dev-1");
        assert_eq!(found.platform, Platform::Windows);
        assert_eq!(found.shared_key, "ab".repeat(32));

        rename_device(&pool, "dev-1", "  我的笔记本  ")
            .await
            .unwrap();
        let found = find_device(&pool, "dev-1").await.unwrap().unwrap();
        assert_eq!(found.name, "我的笔记本");

        assert!(remove_device(&pool, "dev-1").await.unwrap());
        assert!(find_device(&pool, "dev-1").await.unwrap().is_none());
        assert!(!remove_device(&pool, "dev-1").await.unwrap());
    }

    #[tokio::test]
    async fn upsert_overwrites_existing_row() {
        let pool = memory_pool().await;
        upsert_device(&pool, &sample_record("dev-1")).await.unwrap();

        let mut updated = sample_record("dev-1");
        updated.name = "Renamed".to_owned();
        updated.allow_receive = false;
        upsert_device(&pool, &updated).await.unwrap();

        let devices = list_devices(&pool).await.unwrap();
        assert_eq!(devices.len(), 1);
        assert_eq!(devices[0].name, "Renamed");
        assert!(!devices[0].allow_receive);
    }

    #[tokio::test]
    async fn rename_rejects_empty_name() {
        let pool = memory_pool().await;
        upsert_device(&pool, &sample_record("dev-1")).await.unwrap();
        assert!(rename_device(&pool, "dev-1", "   ").await.is_err());
    }

    #[tokio::test]
    async fn permissions_update_partial_fields() {
        let pool = memory_pool().await;
        upsert_device(&pool, &sample_record("dev-1")).await.unwrap();

        set_device_permissions(&pool, "dev-1", Some(false), None)
            .await
            .unwrap();
        let found = find_device(&pool, "dev-1").await.unwrap().unwrap();
        assert!(!found.allow_send);
        assert!(found.allow_receive);

        set_device_permissions(&pool, "dev-1", None, Some(false))
            .await
            .unwrap();
        let found = find_device(&pool, "dev-1").await.unwrap().unwrap();
        assert!(!found.allow_send);
        assert!(!found.allow_receive);
    }

    #[tokio::test]
    async fn touch_and_pull_cursor_update() {
        let pool = memory_pool().await;
        upsert_device(&pool, &sample_record("dev-1")).await.unwrap();

        touch_device(&pool, "dev-1", "10.0.0.5:40000")
            .await
            .unwrap();
        let found = find_device(&pool, "dev-1").await.unwrap().unwrap();
        assert_eq!(found.last_address.as_deref(), Some("10.0.0.5:40000"));
        assert!(found.last_seen_at.is_some());

        let cursor = Utc::now();
        set_last_pull_at(&pool, "dev-1", cursor).await.unwrap();
        let found = find_device(&pool, "dev-1").await.unwrap().unwrap();
        assert_eq!(found.last_pull_at, Some(cursor));
    }

    #[tokio::test]
    async fn blacklist_roundtrip_and_unpair() {
        let pool = memory_pool().await;
        upsert_device(&pool, &sample_record("dev-1")).await.unwrap();
        assert!(!is_blacklisted(&pool, "dev-1").await.unwrap());

        set_blacklisted(&pool, "dev-1", true).await.unwrap();
        assert!(is_blacklisted(&pool, "dev-1").await.unwrap());
        // 拉黑时配对记录被清除。
        assert!(find_device(&pool, "dev-1").await.unwrap().is_none());
        assert_eq!(list_blacklisted_ids(&pool).await.unwrap(), vec!["dev-1"]);

        set_blacklisted(&pool, "dev-1", false).await.unwrap();
        assert!(!is_blacklisted(&pool, "dev-1").await.unwrap());
    }
}
