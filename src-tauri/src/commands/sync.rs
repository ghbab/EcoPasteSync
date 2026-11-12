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

    // 以用户设置为准：服务是否已启动不影响发起配对，避免运行态偏差直接阻断操作。
    if !sync_enabled(&app) {
        return Err(sync_disabled_error(&app));
    }

    client::start_pairing(app, manager.inner().clone(), device_id);
    Ok(())
}

/// 手动指定地址发起配对：输入 IP（可带端口），成功受理后等待对端确认。
#[tauri::command]
pub async fn request_sync_pair_by_address(
    app: AppHandle,
    manager: State<'_, Arc<SyncManager>>,
    address: String,
) -> Result<crate::sync::NearbyDeviceDto> {
    if address.trim().is_empty() {
        return Err(AppError::Other(anyhow::anyhow!("address is empty")));
    }

    // 手动配对是主动外连，不依赖本机服务运行态；只要用户已开启同步就允许发起。
    if !sync_enabled(&app) {
        return Err(sync_disabled_error(&app));
    }

    // 默认端口取本机设置：两端通常使用同一个默认端口，手动输入只需 IP。
    let default_port = app
        .state::<crate::settings::SettingsStore>()
        .snapshot()
        .sync
        .port;

    client::start_pairing_with_address(app, manager.inner().clone(), &address, default_port).await
}

/// 读取最近一条待确认的配对请求。
///
/// 配对窗口由 Rust 按需创建，WebView 加载晚于事件发送，页面挂载后用它补拉；
/// 无有效请求时返回 `null`，窗口据此自动关闭。
#[tauri::command]
pub async fn get_pending_sync_pair(
    manager: State<'_, Arc<SyncManager>>,
) -> Result<Option<crate::sync::PendingPairDto>> {
    Ok(manager.pending_pair_dto())
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
        if manager
            .device_address(&device.device_id, device.last_address.as_deref())
            .is_some()
        {
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

/// 读取当前设置里的同步开关（用户意图，不反映服务运行态）。
fn sync_enabled(app: &AppHandle) -> bool {
    app.state::<crate::settings::SettingsStore>()
        .snapshot()
        .sync
        .enabled
}

/// 构造「同步未开启」错误，根因文案跟随当前界面语言。
fn sync_disabled_error(app: &AppHandle) -> AppError {
    let lang = crate::i18n::current_language(app);

    AppError::Other(anyhow::anyhow!(crate::i18n::commands::label(
        lang,
        crate::i18n::commands::Key::SyncDisabled
    )))
}
