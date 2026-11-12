//! mDNS 设备发现：注册本机 `_ecopaste._tcp.local.` 服务并监听同类服务。
//!
//! TXT 记录携带 device_id / name / platform / version；端口随实例广播。
//! 发现已配对设备（Resolved）时触发增量 pull（断线补偿）。

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
use std::sync::Arc;
use std::time::Duration;

use tauri::{AppHandle, Manager};

use crate::core::{AppError, Result};
use crate::sync::protocol;
use crate::sync::{NearbyEntry, SyncManager};

/// mDNS 事件循环里两次 emit 设备列表事件的合并窗口。
const EMIT_DEBOUNCE: Duration = Duration::from_millis(500);

/// 启动发现：注册自身服务 + 监听同类服务。
/// daemon 句柄交由 [`SyncManager`] 持有，关停时统一 shutdown。
pub fn spawn_discovery(
    app: AppHandle,
    manager: Arc<SyncManager>,
    device_name: String,
    port: u16,
) -> Result<()> {
    let device_info = manager.device_info(&app);

    let daemon = mdns_sd::ServiceDaemon::new()
        .map_err(|err| AppError::Other(anyhow::anyhow!("create mDNS daemon failed: {err}")))?;

    // 实例名用 device_id 保证唯一（设备名可重复、可随时改）。
    let instance = device_info.device_id.clone();
    let id_prefix_len = device_info.device_id.len().min(8);
    let host_name = format!("ecopaste-{}", &device_info.device_id[..id_prefix_len]);
    let properties: HashMap<String, String> = HashMap::from([
        ("device_id".to_owned(), device_info.device_id.clone()),
        ("name".to_owned(), device_name),
        ("platform".to_owned(), device_info.platform.clone()),
        ("version".to_owned(), device_info.version),
    ]);

    let ip = local_lan_ip().to_string();
    // mdns-sd 0.13 的构造函数签名：(服务类型, 实例名, 主机名, IP, 端口, TXT 属性)，返回 Result。
    let service = mdns_sd::ServiceInfo::new(
        protocol::SERVICE_TYPE,
        &instance,
        &host_name,
        ip.as_str(),
        port,
        properties,
    )
    .map_err(|err| AppError::Other(anyhow::anyhow!("build mDNS service info failed: {err}")))?
    .enable_addr_auto();

    daemon
        .register(service)
        .map_err(|err| AppError::Other(anyhow::anyhow!("register mDNS service failed: {err}")))?;

    let receiver = daemon
        .browse(protocol::SERVICE_TYPE)
        .map_err(|err| AppError::Other(anyhow::anyhow!("browse mDNS services failed: {err}")))?;

    manager.set_daemon(daemon);

    // 发现事件处理任务：recv_async 驱动，daemon shutdown 后通道关闭自然退出。
    {
        let app = app.clone();
        let manager = manager.clone();
        tauri::async_runtime::spawn(async move {
            let mut last_emit = std::time::Instant::now() - EMIT_DEBOUNCE;

            while let Ok(event) = receiver.recv_async().await {
                match event {
                    mdns_sd::ServiceEvent::ServiceResolved(info) => {
                        let Some(entry) = resolve_nearby_entry(&info) else {
                            continue;
                        };
                        // 忽略自己（多网卡回声）。
                        if entry.device_id == manager.device_id() {
                            continue;
                        }

                        let fullname = info.get_fullname().to_owned();
                        let is_paired = {
                            let pool = app.state::<crate::db::DatabaseState>().pool().await;
                            crate::sync::devices::find_device(&pool, &entry.device_id)
                                .await
                                .map(|found| found.is_some())
                                .unwrap_or(false)
                        };

                        manager.update_nearby(entry.clone(), fullname);

                        // 已配对设备上线：更新在线信息并触发增量拉取。
                        if is_paired {
                            let pool = app.state::<crate::db::DatabaseState>().pool().await;
                            let _ = crate::sync::devices::touch_device(
                                &pool,
                                &entry.device_id,
                                &entry.address,
                            )
                            .await;
                            crate::sync::pull_from_device(&app, &entry.device_id).await;
                        }

                        // 限频 emit，避免 mDNS 抖动打爆前端。
                        if last_emit.elapsed() >= EMIT_DEBOUNCE {
                            last_emit = std::time::Instant::now();
                            crate::sync::emit_devices_updated(&app);
                        }
                    }
                    mdns_sd::ServiceEvent::ServiceRemoved(_, fullname) => {
                        if manager.remove_nearby(&fullname) {
                            crate::sync::emit_devices_updated(&app);
                        }
                    }
                    mdns_sd::ServiceEvent::SearchStarted(_) => {
                        // 忽略：浏览开始的提示事件。
                    }
                    mdns_sd::ServiceEvent::ServiceFound(_, _) => {
                        // 忽略：等 Resolved 携带完整 TXT 再处理。
                    }
                    mdns_sd::ServiceEvent::SearchStopped(_) => {
                        // daemon 关停，退出任务。
                        break;
                    }
                }
            }

            log::info!("sync discovery task exited");
        });
    }

    Ok(())
}

/// 从 mDNS resolved 服务解析附近设备条目；TXT 缺关键字段或无地址时返回 None。
fn resolve_nearby_entry(info: &mdns_sd::ServiceInfo) -> Option<NearbyEntry> {
    let device_id = info.get_property_val_str("device_id")?.to_string();
    if device_id.is_empty() {
        return None;
    }

    let name = info
        .get_property_val_str("name")
        .unwrap_or("EcoPasteSync")
        .to_string();
    let platform = info
        .get_property_val_str("platform")
        .unwrap_or("windows")
        .to_string();

    // 优先 IPv4（局域网同步主场景）。
    let addr = info
        .get_addresses()
        .iter()
        .find(|addr| addr.is_ipv4())
        .or_else(|| info.get_addresses().iter().next())?;
    let address = SocketAddr::new(*addr, info.get_port()).to_string();

    Some(NearbyEntry {
        device_id,
        name,
        platform,
        address,
    })
}

/// 探测本机局域网 IP：UDP connect 仅让内核选默认路由，不实际发包。
/// 依次尝试国内 / 国际公共 DNS 与常见网关，全部失败则回落回环地址。
fn local_lan_ip() -> IpAddr {
    let probes: [(&str, u16); 3] = [("223.5.5.5", 80), ("8.8.8.8", 80), ("192.168.1.1", 80)];

    for (host, port) in probes {
        let Ok(socket) = UdpSocket::bind("0.0.0.0:0") else {
            continue;
        };
        let Ok(target) = format!("{host}:{port}").parse::<SocketAddr>() else {
            continue;
        };
        if socket.connect(target).is_ok() {
            if let Ok(local) = socket.local_addr() {
                if local.ip() != IpAddr::V4(Ipv4Addr::LOCALHOST) {
                    return local.ip();
                }
            }
        }
    }

    log::warn!("sync discovery: local LAN IP not detected, falling back to loopback");
    IpAddr::V4(Ipv4Addr::LOCALHOST)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_ip_returns_usable_addr() {
        let ip = local_lan_ip();
        // 回落值也必须是合法 IPv4；正常环境应拿到非回环地址。
        assert!(ip.is_ipv4());
    }
}
