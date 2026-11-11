//! 局域网同步命令（薄封装）：参数校验 + 转发 `sync` 模块，不写业务逻辑。

use std::sync::Arc;

use tauri::{AppHandle, Manager, State};

use crate::core::{AppError, Result};
use crate::sync::{client, devices, SyncManager, SyncStateDto};

/// 获取同步状态快照（附近设备 / 已配对 / 黑名单 + 本机标识）。
#[tauri::command]
pub async fn get_sync_state(app: AppHandle) -> Result<SyncStateDto> {
    // 未初始化（理论不会发生）时返回空态。
    if app.try_state::<Arc<SyncManager>>().is_none() {
        return Err(AppError::Other(anyhow::anyhow!("sync not initialized")));
    }

    Ok(crate::sync::sync_state_payload(&app).await)
}

/// 发起配对：向附近设备发起请求，结果经 `sync://pair-result` 事件返回。
#[tauri::command]
pub async fn request_sync_pair(
    app: AppHandle,
    manager: State<'_, Arc<SyncManager>>,
    device_id: String,
) -> Result<()> {
    if device_id.trim().is_empty() {
        return Err(AppError::Other(anyhow::anyhow!("device id is empty")));
    }

    // 未开启同步时附近列表为空，提前拦截。
    if !manager.is_running() {
        return Err(AppError::Other(anyhow::anyhow!("sync is disabled")));
    }

    client::start_pairing(app, manager.inner().clone(), device_id);
    Ok(())
}

/// 应答收到的配对请求（接收端用户在确认弹窗点「允许 / 拒绝」）。
#[tauri::command]
pub async fn respond_sync_pair(
    app: AppHandle,
    manager: State<'_, Arc<SyncManager>>,
    device_id: String,
    accept: bool,
) -> Result<()> {
    if device_id.trim().is_empty() {
        return Err(AppError::Other(anyhow::anyhow!("device id is empty")));
    }

    manager.respond_pair(&app, &device_id, accept).await
}

/// 解除配对：删除已配对设备（对端下次推送将被拒）。
#[tauri::command]
pub async fn remove_sync_device(app: AppHandle, device_id: String) -> Result<()> {
    let pool = app.state::<crate::db::DatabaseState>().pool().await;
    if !devices::remove_device(&pool, &device_id).await? {
        return Err(AppError::Other(anyhow::anyhow!("device not found")));
    }

    crate::sync::emit_devices_updated(&app);
    Ok(())
}

/// 修改设备备注名。
#[tauri::command]
pub async fn rename_sync_device(app: AppHandle, device_id: String, name: String) -> Result<()> {
    devices::rename_device(
        &app.state::<crate::db::DatabaseState>().pool().await,
        &device_id,
        &name,
    )
    .await?;
    crate::sync::emit_devices_updated(&app);
    Ok(())
}

/// 更新设备的发送 / 接收许可（`None` 表示该字段不变）。
#[tauri::command]
pub async fn set_sync_device_permissions(
    app: AppHandle,
    device_id: String,
    allow_send: Option<bool>,
    allow_receive: Option<bool>,
) -> Result<()> {
    if allow_send.is_none() && allow_receive.is_none() {
        return Err(AppError::Other(anyhow::anyhow!("no permission to update")));
    }

    devices::set_device_permissions(
        &app.state::<crate::db::DatabaseState>().pool().await,
        &device_id,
        allow_send,
        allow_receive,
    )
    .await?;
    crate::sync::emit_devices_updated(&app);
    Ok(())
}

/// 拉黑 / 取消拉黑设备。拉黑时连带解除配对。
#[tauri::command]
pub async fn set_sync_device_blocked(
    app: AppHandle,
    device_id: String,
    blocked: bool,
) -> Result<()> {
    devices::set_blacklisted(
        &app.state::<crate::db::DatabaseState>().pool().await,
        &device_id,
        blocked,
    )
    .await?;
    crate::sync::emit_devices_updated(&app);
    Ok(())
}

/// 手动刷新附近设备：清空缓存并立即触发一轮增量拉取补偿。
#[tauri::command]
pub async fn refresh_sync_devices(app: AppHandle) -> Result<()> {
    let manager = app.state::<Arc<SyncManager>>();
    if !manager.is_running() {
        return Ok(());
    }

    manager.clear_nearby();

    // 对已配对且最近有地址的设备立即补拉（绕过节流的一次性操作）。
    let pool = app.state::<crate::db::DatabaseState>().pool().await;
    for device in devices::list_devices(&pool).await? {
        if manager.device_address(&device.device_id, device.last_address.as_deref()).is_some() {
            let app = app.clone();
            let device_id = device.device_id.clone();
            tauri::async_runtime::spawn(async move {
                crate::sync::pull_from_device(&app, &device_id).await;
            });
        }
    }

    crate::sync::emit_devices_updated(&app);
    Ok(())
}
