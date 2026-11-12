//! 局域网剪贴板同步（参考 CrossPaste 设计的轻量实现）。
//!
//! 架构（Rust-First，全部在本模块内）：
//! - **发现**：mDNS（`mdns-sd`）注册 `_ecopaste._tcp.local.` 服务并监听同类服务，
//!   TXT 记录携带 device_id / 设备名 / 平台，端口随实例广播。
//! - **传输**：本机常驻 axum HTTP 服务器，配对设备间以 reqwest 互推 / 互拉；
//!   请求经 blake3 keyed hash 签名认证，报文体经 ChaCha20-Poly1305 加密。
//! - **信任**：显式配对——双方交换临时 X25519 公钥并各自派生共享密钥，
//!   接收方用户在前端确认（并比对 SAS 验证码）后密钥才入库；未配对设备
//!   无法推送任何内容。
//! - **数据流**：新剪贴板条目实时推送（图片推元数据、接收端回源拉取字节）；
//!   已配对设备重新上线时按 `last_pull_at` 游标增量拉取做断线补偿。
//!
//! 事件契约（前端 `src/constants/events.ts`）：
//! `sync://devices-updated` / `sync://pair-request` / `sync://pair-sas` /
//! `sync://pair-result`。

pub(crate) mod client;
mod crypto;
pub(crate) mod devices;
mod discovery;
mod file_state;
mod file_transfer;
mod protocol;
mod secrets;
mod server;

use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use serde::Serialize;
use serde_json::json;
use tauri::{AppHandle, Emitter, Manager};
use tokio::sync::watch;

use crate::core::{AppError, Result};
use crate::db::items::{self, upsert_item};
use crate::db::models::{ClipboardItem, ClipboardKind, ClipboardSubKind, Platform};
use crate::settings::{Settings, SettingsStore};
use file_state::SyncFileState;

use protocol::{
    file_manifest_exceeds_limits, PairAcceptedDto, PairRequestDto, SyncClipboardItemDto,
    SyncFileEntryDto, EVENT_DEVICES_UPDATED, EVENT_PAIR_REQUEST, EVENT_PAIR_RESULT, EVENT_PAIR_SAS,
};
pub use protocol::{
    BlockedDeviceDto, DeviceInfoDto, NearbyDeviceDto, PairStatusDto, PairedDeviceDto,
};

/// 配对请求的受理有效期（秒）：超时后 B 端不再接受确认。
const PAIR_PENDING_TTL_SECS: u64 = 120;
/// 待确认配对请求的最大数量，避免大量陌生设备堆满内存与前端弹窗队列。
const PAIR_PENDING_LIMIT: usize = 8;
/// 同一源 IP 两次配对请求的最小间隔（防反复骚扰配对弹窗）。
const PAIR_REQUEST_INTERVAL: Duration = Duration::from_secs(3);
/// 发起方轮询配对结果的总时长（秒）。
const PAIR_POLL_TIMEOUT_SECS: u64 = 150;
/// 同一设备两次 pull 的最小间隔（mDNS 反复 Resolved 时节流）。
const PULL_THROTTLE: Duration = Duration::from_secs(30);
/// 已配对设备可达性探测间隔。
const PROBE_INTERVAL: Duration = Duration::from_secs(30);
/// 同步 identity 文件名（config 目录下）。
const IDENTITY_FILE: &str = "sync-identity.json";
/// 文件指纹只读头部这么多字节：足以发现文件被替换 / 截断，成本与文件大小无关。
const FILE_HEAD_HASH_BYTES: u64 = 64 * 1024;
/// 单条 files 条目最多采集多少个顶层路径；超出部分接收端按「仅路径可用」处理。
const FILE_FINGERPRINT_LIMIT: usize = 64;
/// 目录条目最多展开多少个条目（文件与子目录都计数）；超出即标记该目录不完整。
const DIR_FILE_LIMIT: usize = 4096;
/// 目录递归深度上限，避免异常目录结构拖垮枚举。
const DIR_DEPTH_LIMIT: usize = 32;
/// 对外共享的文件路径授权保留时长：超时后对端不能再拉取，需要重新复制一次。
const SHARED_PATH_TTL: Duration = Duration::from_secs(24 * 60 * 60);
/// 共享授权表的容量上限，避免长期运行无限增长。
const SHARED_PATH_LIMIT: usize = 8192;

/// 附近设备缓存条目。
#[derive(Debug, Clone)]
pub struct NearbyEntry {
    pub device_id: String,
    pub name: String,
    pub platform: String,
    pub address: String,
}

/// B 端收到的待确认配对请求。
#[derive(Debug, Clone)]
struct PendingPair {
    request: PairRequestDto,
    received_at: Instant,
    /// 接收方临时私钥：只在内存保留到用户应答或超时，从不上线。
    secret: [u8; 32],
    /// 配对验证码：接收方弹窗展示，供用户与发起方屏幕上的数字人工比对。
    sas: String,
    /// 发起方来源 IP：轮询配对状态时用它绑定调用方，防同网段其它主机偷看结果。
    peer_ip: Option<IpAddr>,
    /// 发起方回连地址（TCP 源 IP + 请求上报端口）；跨网段无法靠 mDNS 得到时用它。
    peer_address: Option<String>,
}

/// B 端已作出的配对决定（供 A 轮询），带过期时间与来源 IP。
#[derive(Debug, Clone, Copy)]
struct PairDecisionEntry {
    decision: PairDecision,
    at: Instant,
    peer_ip: Option<IpAddr>,
}

/// B 端已作出的配对决定（供 A 轮询），带过期时间自动清理。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PairDecision {
    Accepted,
    Rejected,
}

/// 判断配对状态查询来源是否匹配登记时的来源 IP。
///
/// 登记时没有来源 IP（极端环境拿不到连接信息）时不做限制；登记了来源
/// 但本次查询拿不到来源（不应发生）按不匹配处理，保持默认拒绝。
fn peer_ip_matches(expected: Option<IpAddr>, actual: Option<IpAddr>) -> bool {
    match (expected, actual) {
        (Some(expected), Some(actual)) => expected == actual,
        (None, _) => true,
        (Some(_), None) => false,
    }
}

/// 按字符边界截断字符串，避免把对端可控的超长文本带进 UI。
fn truncate_chars(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

/// 运行中的同步实例指纹：任一字段变化即整体重启。
#[derive(Debug, Clone, PartialEq, Eq)]
struct RunningFingerprint {
    device_name: String,
    port: u16,
}

/// 同步管理器：发现缓存、配对状态机、服务器生命周期。
/// 以 `Arc<SyncManager>` 注册进 Tauri State。
pub struct SyncManager {
    /// 当前运行指纹；`None` 表示同步功能关闭。
    fingerprint: RwLock<Option<RunningFingerprint>>,
    /// 持久设备 id（uuid，跨重启稳定）。
    device_id: String,
    /// 附近设备缓存（device_id → 条目）。
    nearby: RwLock<HashMap<String, NearbyEntry>>,
    /// mDNS fullname → device_id（ServiceRemoved 事件只带 fullname）。
    fullname_map: RwLock<HashMap<String, String>>,
    /// 可达性探测结果为在线的已配对设备（mDNS 发现不到时仍能标记在线）。
    online: RwLock<HashSet<String>>,
    /// 在线探测任务是否已启动（只启动一次，避免同步重启产生多个循环）。
    probe_started: AtomicBool,
    /// B 端待确认配对请求。
    pending_pairs: Mutex<HashMap<String, PendingPair>>,
    /// B 端配对决定（device_id → 决定 + 时间 + 来源 IP）。
    pair_decisions: Mutex<HashMap<String, PairDecisionEntry>>,
    /// 配对请求按源 IP 限速（最近一次受理时间）。
    pair_rate: Mutex<HashMap<IpAddr, Instant>>,
    /// 每设备 pull 节流。
    pull_throttle: Mutex<HashMap<String, Instant>>,
    /// 允许对端按需拉取的文件路径 → 最近一次对外共享时间。
    ///
    /// 只登记本机主动广播或响应 pull 的路径：同步入库的远端路径不进这里，
    /// 避免已配对设备借拉取接口读取本机任意文件。进程重启后授权表清空，
    /// 旧条目需要重新复制一次才能按需拉取。
    shared_paths: Mutex<HashMap<String, Instant>>,
    /// axum 服务器关闭信号。
    shutdown_tx: Mutex<Option<watch::Sender<bool>>>,
    /// mDNS daemon 句柄（发现层注册后回填，关停时统一 shutdown）。
    daemon: Mutex<Option<mdns_sd::ServiceDaemon>>,
    /// 本机 mDNS 服务描述（发现层回填）：运行时按「允许被发现」开关动态注册 / 注销。
    service: Mutex<Option<mdns_sd::ServiceInfo>>,
    /// 本机服务当前是否已注册到 mDNS。
    discoverable: AtomicBool,
}

/// 同步状态快照（`get_sync_state` 命令与 `sync://devices-updated` 事件共用）。
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncStateDto {
    pub enabled: bool,
    pub device_id: String,
    pub device_name: String,
    pub port: u16,
    pub nearby: Vec<NearbyDeviceDto>,
    pub paired: Vec<PairedDeviceDto>,
    pub blocked: Vec<BlockedDeviceDto>,
}

/// 发起端配对结果事件载荷。
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct PairResultEvent {
    device_id: String,
    accepted: bool,
    reason: Option<String>,
}

/// 待确认配对请求快照（配对窗口加载后主动拉取，避免事件早于页面挂载而丢失）。
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PendingPairDto {
    pub device_id: String,
    pub name: String,
    pub platform: String,
    /// 6 位配对验证码。
    pub sas: String,
}

impl SyncManager {
    /// 创建管理器并加载（或首次生成）持久设备 id。
    pub fn new(app: &AppHandle) -> Result<Self> {
        let device_id = load_or_create_identity(app)?;
        Ok(Self {
            fingerprint: RwLock::new(None),
            device_id,
            nearby: RwLock::new(HashMap::new()),
            fullname_map: RwLock::new(HashMap::new()),
            online: RwLock::new(HashSet::new()),
            probe_started: AtomicBool::new(false),
            pending_pairs: Mutex::new(HashMap::new()),
            pair_decisions: Mutex::new(HashMap::new()),
            pair_rate: Mutex::new(HashMap::new()),
            pull_throttle: Mutex::new(HashMap::new()),
            shared_paths: Mutex::new(HashMap::new()),
            shutdown_tx: Mutex::new(None),
            daemon: Mutex::new(None),
            service: Mutex::new(None),
            discoverable: AtomicBool::new(false),
        })
    }

    /// 发现层回填 daemon 句柄。
    pub fn set_daemon(&self, daemon: mdns_sd::ServiceDaemon) {
        *self.daemon.lock().expect("sync poisoned") = Some(daemon);
    }

    /// 发现层回填本机服务描述（运行时注册 / 注销要用）。
    pub fn set_service(&self, service: mdns_sd::ServiceInfo) {
        *self.service.lock().expect("sync poisoned") = Some(service);
    }

    /// 运行时同步「允许被发现」状态：只注册 / 注销 mDNS 服务，不重启 HTTP 服务器。
    ///
    /// 重启服务器会重新探测端口，已配对设备记录的旧地址可能失效；
    /// 因此这个开关必须走增量路径。
    pub fn set_discoverable(&self, enabled: bool) {
        if self.discoverable.load(Ordering::SeqCst) == enabled {
            return;
        }

        let daemon_guard = self.daemon.lock().expect("sync poisoned");
        let Some(daemon) = daemon_guard.as_ref() else {
            return;
        };
        let service_guard = self.service.lock().expect("sync poisoned");
        let Some(service) = service_guard.as_ref() else {
            return;
        };

        // unregister 会返回注销进度通道；这里不等待回执，注销消息已进入 daemon 队列。
        let result: std::result::Result<(), mdns_sd::Error> = if enabled {
            daemon.register(service.clone())
        } else {
            daemon
                .unregister(service.get_fullname())
                .map(|_receiver| ())
        };

        match result {
            Ok(()) => {
                self.discoverable.store(enabled, Ordering::SeqCst);

                if !enabled {
                    // 关闭发现即不再受理任何配对：清空待确认队列，配对窗口拉取不到请求会自行关闭。
                    self.pending_pairs.lock().expect("sync poisoned").clear();
                }

                log::info!(
                    "sync discovery {}",
                    if enabled { "enabled" } else { "disabled" }
                );
            }
            Err(err) => log::warn!("sync discovery toggle failed: {err}"),
        }
    }

    pub fn device_id(&self) -> &str {
        &self.device_id
    }

    /// 有效设备名：用户设置优先，空则回落主机名。
    pub fn effective_device_name(&self, settings: &Settings) -> String {
        let configured = settings.sync.device_name.trim();
        if !configured.is_empty() {
            return configured.to_owned();
        }

        let hostname = tauri_plugin_os::hostname();
        if hostname.trim().is_empty() {
            return "EcoPasteSync".to_owned();
        }

        hostname
    }

    /// 当前对外可见的设备信息（`/api/info` 响应）。
    pub fn device_info(&self, app: &AppHandle) -> DeviceInfoDto {
        let settings = app.state::<SettingsStore>().snapshot();
        DeviceInfoDto {
            device_id: self.device_id.clone(),
            name: self.effective_device_name(&settings),
            platform: current_platform_tag().to_owned(),
            version: crate::core::version::display_version().to_owned(),
        }
    }

    pub fn is_running(&self) -> bool {
        self.fingerprint.read().expect("sync poisoned").is_some()
    }

    fn running_port(&self) -> Option<u16> {
        self.fingerprint
            .read()
            .expect("sync poisoned")
            .as_ref()
            .map(|f| f.port)
    }

    /// 应用同步设置：关闭 / 首次启动 / 参数变化重启，均幂等。
    pub fn apply(&self, app: &AppHandle, settings: &Settings) {
        if !settings.sync.enabled {
            self.stop(app);
            return;
        }

        let desired = RunningFingerprint {
            device_name: self.effective_device_name(settings),
            port: settings.sync.port,
        };
        let current = self.fingerprint.read().expect("sync poisoned").clone();
        if current.as_ref() == Some(&desired) {
            // 运行参数未变：仅增量同步「允许被发现」状态。重启服务器会重新选端口，
            // 已配对设备记录的回连地址可能因此失效。
            self.set_discoverable(settings.sync.discoverable);
            return;
        }

        self.stop(app);
        if let Err(err) = self.start(app, &desired, settings.sync.discoverable) {
            log::error!("sync start failed: {err:?}");
        }
    }

    /// 启动 HTTP 服务器与 mDNS 发现。
    fn start(
        &self,
        app: &AppHandle,
        desired: &RunningFingerprint,
        discoverable: bool,
    ) -> Result<()> {
        let ctx = Arc::new(server::ServerCtx {
            app: app.clone(),
            manager: app.state::<Arc<SyncManager>>().inner().clone(),
        });

        let (port, shutdown) = server::spawn_server(ctx, desired.port)?;
        *self.shutdown_tx.lock().expect("sync poisoned") = Some(shutdown);

        let manager = app.state::<Arc<SyncManager>>().inner().clone();
        // mDNS 失败（端口被占用、系统限制本地网络权限等）不应阻断同步：
        // HTTP 服务仍可用于手动配对与已配对设备互推，只是附近列表暂不可用。
        if let Err(err) = discovery::spawn_discovery(
            app.clone(),
            manager,
            desired.device_name.clone(),
            port,
            discoverable,
        ) {
            log::warn!("sync discovery start failed: {err:?}");
        }

        *self.fingerprint.write().expect("sync poisoned") = Some(desired.clone());
        log::info!("sync started on port {port} as {}", self.device_id);
        self.ensure_health_probe(app);
        emit_devices_updated(app);
        Ok(())
    }

    /// 启动已配对设备可达性探测任务；进程内只启动一次。
    fn ensure_health_probe(&self, app: &AppHandle) {
        if self.probe_started.swap(true, Ordering::SeqCst) {
            return;
        }

        let app = app.clone();
        let manager = app.state::<Arc<SyncManager>>().inner().clone();
        tauri::async_runtime::spawn(async move {
            loop {
                tokio::time::sleep(PROBE_INTERVAL).await;
                if !manager.is_running() {
                    continue;
                }
                probe_paired_devices(&app, &manager).await;
            }
        });
    }

    /// 关停服务器与发现，清空附近缓存。
    fn stop(&self, app: &AppHandle) {
        if self
            .fingerprint
            .write()
            .expect("sync poisoned")
            .take()
            .is_none()
        {
            return;
        }

        if let Some(tx) = self.shutdown_tx.lock().expect("sync poisoned").take() {
            let _ = tx.send(true);
        }
        if let Some(daemon) = self.daemon.lock().expect("sync poisoned").take() {
            if let Err(err) = daemon.shutdown() {
                log::warn!("sync stop: mDNS daemon shutdown failed: {err}");
            }
        }
        *self.service.lock().expect("sync poisoned") = None;
        self.discoverable.store(false, Ordering::SeqCst);
        self.nearby.write().expect("sync poisoned").clear();
        self.fullname_map.write().expect("sync poisoned").clear();
        self.online.write().expect("sync poisoned").clear();
        self.pending_pairs.lock().expect("sync poisoned").clear();
        log::info!("sync stopped");
        emit_devices_updated(app);
    }

    // ---- 附近设备缓存 ----

    /// 更新附近设备缓存。同一设备仅地址 / 名称变化才算变更。
    pub fn update_nearby(&self, entry: NearbyEntry, fullname: String) -> bool {
        let mut map = self.nearby.write().expect("sync poisoned");
        let changed = map
            .get(&entry.device_id)
            .map(|old| old.address != entry.address || old.name != entry.name)
            .unwrap_or(true);
        self.fullname_map
            .write()
            .expect("sync poisoned")
            .insert(fullname, entry.device_id.clone());
        map.insert(entry.device_id.clone(), entry);
        changed
    }

    /// mDNS 服务移除时清理对应附近条目。
    pub fn remove_nearby(&self, fullname: &str) -> bool {
        let device_id = self
            .fullname_map
            .write()
            .expect("sync poisoned")
            .remove(fullname);
        let Some(device_id) = device_id else {
            return false;
        };

        self.nearby
            .write()
            .expect("sync poisoned")
            .remove(&device_id)
            .is_some()
    }

    /// 清空附近缓存（手动刷新时）。
    pub fn clear_nearby(&self) {
        self.nearby.write().expect("sync poisoned").clear();
        self.fullname_map.write().expect("sync poisoned").clear();
    }

    /// 取附近设备条目快照；配对发起端用它固定地址与展示信息。
    pub fn nearby_entry(&self, device_id: &str) -> Option<NearbyEntry> {
        self.nearby
            .read()
            .expect("sync poisoned")
            .get(device_id)
            .cloned()
    }

    pub fn nearby_snapshot(&self) -> Vec<NearbyDeviceDto> {
        self.nearby
            .read()
            .expect("sync poisoned")
            .values()
            .cloned()
            .map(|entry| NearbyDeviceDto {
                device_id: entry.device_id,
                name: entry.name,
                platform: entry.platform,
                address: entry.address,
            })
            .collect()
    }

    /// 取设备当前地址：优先附近缓存，回落配对记录里的 last_address。
    pub fn device_address(&self, device_id: &str, fallback: Option<&str>) -> Option<String> {
        if let Some(entry) = self.nearby.read().expect("sync poisoned").get(device_id) {
            return Some(entry.address.clone());
        }

        fallback.map(str::to_owned)
    }

    // ---- 配对状态机 ----

    /// 配对请求按源 IP 限速；返回 `false` 表示本次请求应直接拒绝。
    ///
    /// 必须由网络入口在访问数据库 / 系统钥匙串之前调用：未认证请求若先触发
    /// 设备查询，攻击者可用高频配对请求放大资源消耗。
    pub fn allow_pair_attempt(&self, peer_ip: Option<IpAddr>) -> bool {
        let Some(ip) = peer_ip else {
            return true;
        };

        let mut rate = self.pair_rate.lock().expect("sync poisoned");
        let now = Instant::now();
        rate.retain(|_, last| now.duration_since(*last) < PAIR_REQUEST_INTERVAL);
        if rate.contains_key(&ip) {
            return false;
        }

        rate.insert(ip, now);
        true
    }

    /// B 端受理配对请求：生成临时密钥对、登记待确认并发事件给前端。
    ///
    /// 返回携带接收方临时公钥的受理响应时表示已受理；返回 `None` 表示拒绝
    /// （设备 id 非法、公钥非法或与自身相同）。真正的密钥要等用户在弹窗中
    /// 确认后才落库。
    ///
    /// `peer_ip` 是发起方连接本机时的 TCP 源地址；与请求上报的端口组合后，
    /// 接收端即可得到回连地址，跨网段手动配对也能双向推送。
    ///
    /// 按源 IP 的限速由网络入口（[`crate::sync::server`]）在调用本函数前完成。
    pub fn handle_pair_request(
        &self,
        app: &AppHandle,
        mut request: PairRequestDto,
        peer_ip: Option<IpAddr>,
    ) -> Option<PairAcceptedDto> {
        // 对端可控的展示字段先做归一化与长度收敛：超长名称会撑破固定尺寸的配对窗口。
        request.device_id = request.device_id.trim().to_owned();
        request.name = truncate_chars(request.name.trim(), protocol::PAIR_DEVICE_NAME_MAX_CHARS);
        request.platform =
            truncate_chars(request.platform.trim(), protocol::PAIR_PLATFORM_MAX_CHARS);

        if request.device_id == self.device_id {
            return None;
        }

        // 解析发起方临时公钥：非法输入直接拒绝，不进入待确认状态。
        let peer_public = match crypto::public_key_from_hex(&request.public_key) {
            Ok(public) => public,
            Err(_) => return None,
        };

        // 生成接收方临时密钥对并预派生共享密钥，双方据此得到相同的 SAS。
        let keypair = crypto::generate_pairing_keypair();
        let key = match crypto::derive_pairing_key(
            &keypair.secret,
            &peer_public,
            &self.device_id,
            &request.device_id,
        ) {
            Ok(key) => key,
            Err(_) => return None,
        };
        let sas = crypto::pairing_sas(&key);

        // 先取出事件所需字段，避免 `request` move 进 HashMap 后再借用。
        let device_id = request.device_id.clone();
        let name = request.name.clone();
        let platform = request.platform.clone();
        let peer_address = match (peer_ip, request.port) {
            (Some(ip), Some(port)) if port != 0 => Some(SocketAddr::new(ip, port).to_string()),
            _ => None,
        };

        {
            let mut pending = self.pending_pairs.lock().expect("sync poisoned");
            let now = Instant::now();
            // 过期清理 + 同设备旧请求覆盖。
            pending.retain(|_, pair| {
                now.duration_since(pair.received_at).as_secs() < PAIR_PENDING_TTL_SECS
            });

            // 待确认请求总量上限：同设备重发允许覆盖，新设备超限直接拒绝。
            if !pending.contains_key(&device_id) && pending.len() >= PAIR_PENDING_LIMIT {
                return None;
            }

            pending.insert(
                device_id.clone(),
                PendingPair {
                    request,
                    received_at: now,
                    secret: keypair.secret,
                    sas: sas.clone(),
                    peer_ip,
                    peer_address,
                },
            );
        }

        // 配对窗口独立于剪贴板面板：直接拉到桌面正中央，用户无需先打开粘贴面板。
        if let Err(err) = crate::window::show_pair_request_window(app) {
            log::warn!("show pair request window failed: {err:?}");
        }

        let event = json!({
            "deviceId": device_id,
            "name": name,
            "platform": platform,
            "sas": sas,
        });
        if let Err(err) = app.emit(EVENT_PAIR_REQUEST, event) {
            log::warn!("emit pair request event failed: {err}");
        }

        Some(PairAcceptedDto {
            status: PairStatusDto::Pending,
            public_key: crypto::public_key_to_hex(&keypair.public),
        })
    }

    fn pending_request(&self, device_id: &str) -> Option<PendingPair> {
        let mut pending = self.pending_pairs.lock().expect("sync poisoned");
        // 查询时也清理过期请求：长期没有新请求时不让过期条目滞留内存。
        let now = Instant::now();
        pending.retain(|_, pair| {
            now.duration_since(pair.received_at).as_secs() < PAIR_PENDING_TTL_SECS
        });
        pending.get(device_id).cloned()
    }

    /// 取最近一条待确认配对请求；无有效请求时返回 `None`。
    ///
    /// 配对窗口可能晚于事件创建，页面挂载后用它拉取当前请求；
    /// 用户处理完一个请求后也用它判断是否还有其它设备在等待。
    pub fn pending_pair_dto(&self) -> Option<PendingPairDto> {
        let mut pending = self.pending_pairs.lock().expect("sync poisoned");
        let now = Instant::now();
        pending.retain(|_, pair| {
            now.duration_since(pair.received_at).as_secs() < PAIR_PENDING_TTL_SECS
        });

        // FIFO：始终返回最早收到的请求。新请求不能顶替用户正在核对的弹窗内容，
        // 否则存在「SAS 看到的是 A、点击时已换成 B」的确认竞态。
        let (_, pair) = pending.iter().min_by_key(|(_, pair)| pair.received_at)?;

        Some(PendingPairDto {
            device_id: pair.request.device_id.clone(),
            name: pair.request.name.clone(),
            platform: pair.request.platform.clone(),
            sas: pair.sas.clone(),
        })
    }

    /// B 端用户应答配对；接受则派生密钥并入库，无论接受与否都记录决定供 A 轮询。
    pub async fn respond_pair(&self, app: &AppHandle, device_id: &str, accept: bool) -> Result<()> {
        let request = {
            let mut pending = self.pending_pairs.lock().expect("sync poisoned");
            pending.remove(device_id)
        };
        let Some(pair) = request else {
            return Err(AppError::Other(anyhow::anyhow!(
                "pairing request not found or expired"
            )));
        };
        // 记录来源 IP：轮询端必须来自同一地址才能看到决定结果。
        let pair_peer_ip = pair.peer_ip;

        if accept {
            // 用待确认记录里的临时私钥 + 发起方公钥完成 ECDH 派生共享密钥。
            let peer_public = crypto::public_key_from_hex(&pair.request.public_key)?;
            let key =
                crypto::derive_pairing_key(&pair.secret, &peer_public, &self.device_id, device_id)?;
            // 同广播域优先用 mDNS 解析到的地址；跨网段回落到配对请求携带的回连地址。
            let address = self
                .device_address(device_id, None)
                .or_else(|| pair.peer_address.clone());
            let now = chrono::Utc::now();
            let record = devices::SyncDeviceRecord {
                device_id: device_id.to_owned(),
                name: pair.request.name.clone(),
                platform: parse_platform_tag(&pair.request.platform),
                shared_key: crypto::key_to_hex(&key),
                allow_send: true,
                allow_receive: true,
                last_seen_at: Some(now),
                last_address: address,
                last_pull_at: None,
                created_at: now,
            };
            let pool = devices::pool(app).await;
            devices::upsert_device(&pool, &record).await?;
            // 配对请求刚到达，对端必然在线。
            self.set_online(device_id, true);
        }

        {
            let mut decisions = self.pair_decisions.lock().expect("sync poisoned");
            let now = Instant::now();
            decisions
                .retain(|_, entry| now.duration_since(entry.at).as_secs() < PAIR_POLL_TIMEOUT_SECS);
            let decision = if accept {
                PairDecision::Accepted
            } else {
                PairDecision::Rejected
            };
            decisions.insert(
                device_id.to_owned(),
                PairDecisionEntry {
                    decision,
                    at: now,
                    peer_ip: pair_peer_ip,
                },
            );
        }
        emit_devices_updated(app);
        Ok(())
    }

    /// B 端视角的配对状态（供 A 轮询）。
    ///
    /// `peer_ip` 是本次轮询的来源地址：只有与发起配对请求时相同的地址才能
    /// 读到状态，避免同网段其它主机靠猜 device_id 探测配对结果。
    pub fn pair_status_for(&self, device_id: &str, peer_ip: Option<IpAddr>) -> PairStatusDto {
        if let Some(entry) = self
            .pair_decisions
            .lock()
            .expect("sync poisoned")
            .get(device_id)
            .copied()
        {
            if entry.at.elapsed().as_secs() < PAIR_POLL_TIMEOUT_SECS
                && peer_ip_matches(entry.peer_ip, peer_ip)
            {
                return match entry.decision {
                    PairDecision::Accepted => PairStatusDto::Accepted,
                    PairDecision::Rejected => PairStatusDto::Rejected,
                };
            }
        }

        if let Some(pair) = self.pending_request(device_id) {
            if pair.received_at.elapsed().as_secs() < PAIR_PENDING_TTL_SECS
                && peer_ip_matches(pair.peer_ip, peer_ip)
            {
                return PairStatusDto::Pending;
            }
        }

        // 无记录或来源不匹配一律视为拒绝，发起方按失败处理。
        PairStatusDto::Rejected
    }

    /// A 端保存配对结果（轮询到 Accepted 后调用）。
    ///
    /// 传入发起时固定的附近条目：轮询期间附近缓存可能被刷新清空，
    /// 但配对结果仍应按发起时的地址与设备信息入库。
    pub async fn save_paired_device(
        &self,
        app: &AppHandle,
        entry: &NearbyEntry,
        key: &[u8; 32],
    ) -> Result<()> {
        let device_id = entry.device_id.as_str();
        let now = chrono::Utc::now();
        let record = devices::SyncDeviceRecord {
            device_id: device_id.to_owned(),
            name: entry.name.clone(),
            platform: parse_platform_tag(&entry.platform),
            shared_key: crypto::key_to_hex(key),
            allow_send: true,
            allow_receive: true,
            last_seen_at: Some(now),
            last_address: Some(entry.address.clone()),
            last_pull_at: None,
            created_at: now,
        };
        let pool = devices::pool(app).await;
        devices::upsert_device(&pool, &record).await?;
        // 轮询刚拿到对端确认，对端在线。
        self.set_online(device_id, true);
        emit_devices_updated(app);
        Ok(())
    }

    /// 更新已配对设备的可达状态，返回状态是否发生变化。
    fn set_online(&self, device_id: &str, online: bool) -> bool {
        let mut known = self.online.write().expect("sync poisoned");
        if online {
            known.insert(device_id.to_owned())
        } else {
            known.remove(device_id)
        }
    }

    /// 设备是否在线：mDNS 附近可见，或可达性探测成功。
    ///
    /// 跨网段手动配对的设备不会出现在 mDNS 里，只靠探测结果判断。
    pub fn is_device_online(&self, device_id: &str) -> bool {
        if self
            .nearby
            .read()
            .expect("sync poisoned")
            .contains_key(device_id)
        {
            return true;
        }

        self.online
            .read()
            .expect("sync poisoned")
            .contains(device_id)
    }

    // ---- 文件共享授权 ----

    /// 登记本机对外共享过的文件路径（广播或响应 pull 时调用）。
    pub fn remember_shared_paths(&self, paths: &[String]) {
        if paths.is_empty() {
            return;
        }

        let mut shared = self.shared_paths.lock().expect("sync poisoned");
        let now = Instant::now();
        shared.retain(|_, at| now.duration_since(*at) < SHARED_PATH_TTL);

        for path in paths {
            // 容量满时只更新已有条目，不再接收新路径，保证长期运行不无限增长。
            if !shared.contains_key(path) && shared.len() >= SHARED_PATH_LIMIT {
                continue;
            }
            shared.insert(path.clone(), now);
        }
    }

    /// 路径是否仍在共享授权窗口内（对端可凭此拉取该文件）。
    pub fn is_shared_path(&self, path: &str) -> bool {
        let mut shared = self.shared_paths.lock().expect("sync poisoned");
        match shared.get(path).copied() {
            Some(at) if at.elapsed() < SHARED_PATH_TTL => true,
            Some(_) => {
                shared.remove(path);
                false
            }
            None => false,
        }
    }

    // ---- pull 节流 ----

    /// 判断是否允许对设备发起 pull（节流窗口外返回 true 并占用本次窗口）。
    pub fn try_acquire_pull_slot(&self, device_id: &str) -> bool {
        let mut throttle = self.pull_throttle.lock().expect("sync poisoned");
        let now = Instant::now();
        match throttle.get(device_id) {
            Some(last) if now.duration_since(*last) < PULL_THROTTLE => false,
            _ => {
                throttle.insert(device_id.to_owned(), now);
                true
            }
        }
    }
}

fn parse_platform_tag(tag: &str) -> Platform {
    match tag {
        "macos" => Platform::Macos,
        _ => Platform::Windows,
    }
}

pub(crate) fn current_platform_tag() -> &'static str {
    #[cfg(target_os = "macos")]
    {
        "macos"
    }
    #[cfg(target_os = "windows")]
    {
        "windows"
    }
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    {
        compile_error!("EcoPaste only supports macOS and Windows")
    }
}

// ---- 模块级入口（供其它模块调用）----

/// 粘贴/复制远端 files 条目时，把文件拉到本地缓存并返回可写回剪贴板的路径。
///
/// 返回 `None` 表示本机条目或已内联拉取完成，调用方直接使用原 content；
/// 返回 `Some(paths)` 时调用方应改用这些本地路径写回剪贴板。
pub async fn ensure_files_local(
    app: &AppHandle,
    item: &ClipboardItem,
) -> Result<Option<Vec<String>>> {
    if item.kind != ClipboardKind::Files {
        return Ok(None);
    }
    if item.sync_source_device_id.is_none() {
        return Ok(None);
    }

    file_transfer::ensure_item_files(app, &item.id, &item.content).await
}

/// 初始化：注册管理器并按设置决定是否启动。
pub fn init(app: &AppHandle) -> Result<()> {
    let manager = Arc::new(SyncManager::new(app)?);
    app.manage(manager.clone());

    let settings = app.state::<SettingsStore>().snapshot();
    manager.apply(app, &settings);
    Ok(())
}

/// 设置变化后重新应用（`update_settings` 检测到 `sync` 段变更时调用）。
pub fn apply_settings(app: &AppHandle, settings: &Settings) {
    if let Some(manager) = app.try_state::<Arc<SyncManager>>() {
        manager.apply(app, settings);
    }
}

/// 新剪贴板条目入库后调用：向所有允许发送且在线的配对设备推送。
/// 由 [`crate::clipboard::watcher::persist_and_notify`] 触发；同步接收路径不经过这里，天然无回环。
pub async fn maybe_broadcast_item(app: &AppHandle, item: &ClipboardItem) {
    let Some(manager) = app.try_state::<Arc<SyncManager>>() else {
        return;
    };
    if !manager.is_running() {
        return;
    }

    let settings = app.state::<SettingsStore>().snapshot();
    if !settings.sync.enabled {
        return;
    }

    // 按本机类型开关过滤。
    let allowed = match item.kind {
        ClipboardKind::Text => settings.sync.sync_text,
        ClipboardKind::Image => settings.sync.sync_image,
        ClipboardKind::Files => settings.sync.sync_files,
    };
    if !allowed {
        return;
    }

    let pool = app.state::<crate::db::DatabaseState>().pool().await;
    let targets = match devices::list_devices(&pool).await {
        Ok(list) => list,
        Err(err) => {
            log::warn!("sync broadcast: list devices failed: {err}");
            return;
        }
    };

    let dto = item_to_dto(
        item,
        megabytes_to_bytes(settings.sync.file_threshold),
        megabytes_to_bytes(settings.sync.file_max_size),
    );
    if item.kind == ClipboardKind::Files && item.sync_source_device_id.is_none() {
        // 只登记本机真实共享出去的路径（含目录内文件），供对端按需拉取时校验。
        manager.remember_shared_paths(&collect_shared_paths(&item.content));
    }

    for device in targets {
        if !device.allow_send {
            continue;
        }
        let Some(address) =
            manager.device_address(&device.device_id, device.last_address.as_deref())
        else {
            continue;
        };

        let app = app.clone();
        let dto = dto.clone();
        tauri::async_runtime::spawn(async move {
            if let Err(err) = client::push_item(&app, &device.shared_key, &address, &dto).await {
                log::warn!(
                    "sync push to {} ({address}) failed: {err}",
                    device.device_id
                );
            }
        });
    }
}

/// 接收端对 files 条目的落盘决策。
enum IncomingFiles {
    /// 不需要本地字节：对端未提供指纹，或路径本机已存在。
    Ignore,
    /// 已内联拉取完成，`content` 换成缓存路径。
    Pulled(Vec<String>),
    /// 需要登记状态表：`Pending` 等待粘贴时按需拉取，`Unavailable` 仅路径可用。
    Deferred {
        state: SyncFileState,
        total_bytes: i64,
        file_count: i64,
    },
}

/// 单个顶层条目在接收端的传输计划。
struct IncomingRoot<'a> {
    /// 顶层条目是否为目录。
    is_dir: bool,
    /// 目录是否已由发送端完整枚举；非目录恒为 `true`。
    complete: bool,
    /// 可直接拉取的文件条目：顶层文件自身，或目录子树文件。
    files: Vec<&'a SyncFileEntryDto>,
}

/// 把文件清单按顶层条目归组，并标记目录是否可完整重建。
///
/// 新版本发送端带 `root_index`，直接按序号归组；旧版本没有该字段，回退为按
/// 路径与顶层路径相等匹配。无法归属的条目直接忽略（对应顶层条目会判为不可传输）。
fn group_incoming_roots<'a>(content: &str, files: &'a [SyncFileEntryDto]) -> Vec<IncomingRoot<'a>> {
    let top_paths: Vec<&str> = content
        .split('\n')
        .filter(|path| !path.is_empty())
        .collect();
    let mut groups: Vec<Vec<&SyncFileEntryDto>> = vec![Vec::new(); top_paths.len()];

    for entry in files {
        let index = match entry.root_index {
            Some(index) if index < top_paths.len() => index,
            _ => match top_paths
                .iter()
                .position(|path| *path == entry.path.as_str())
            {
                Some(index) => index,
                None => continue,
            },
        };
        groups[index].push(entry);
    }

    let mut roots = Vec::with_capacity(groups.len());
    for group in groups {
        let dir_root = group
            .iter()
            .find(|entry| entry.is_dir && entry.rel_path.is_none());
        let is_dir = dir_root.is_some();
        // 旧版本发送端不会带 complete 标记，按不完整处理，保持「仅路径可用」。
        let complete = match dir_root {
            Some(entry) => entry.complete == Some(true),
            None => true,
        };
        let children = group
            .iter()
            .copied()
            .filter(|entry| {
                if entry.is_dir && entry.rel_path.is_none() {
                    // 目录根自身不是可拉取文件。
                    return false;
                }
                entry.rel_path.is_some() == is_dir
            })
            .collect();

        roots.push(IncomingRoot {
            is_dir,
            complete,
            files: children,
        });
    }

    roots
}

/// 决定 incoming files 条目是否需要把字节落到本机。
///
/// 分档依据是「条目内文件总大小」（目录按子树文件大小累加）：
/// - 不超过直接传输阈值 → 立即拉取，粘贴时无需等待；
/// - 超过阈值但不超过传输上限 → 只记 `Pending`，粘贴时再按需拉取；
/// - 目录无法完整枚举、指纹不完整或超过上限 → `Unavailable`，仅保留远端路径。
async fn resolve_incoming_files(
    app: &AppHandle,
    source_device_id: &str,
    dto: &SyncClipboardItemDto,
) -> IncomingFiles {
    let files = dto.files.as_deref().unwrap_or_default();
    if files.is_empty() {
        return IncomingFiles::Ignore;
    }

    // 接收端独立校验清单规模：超限清单只保留路径，不归组、不重建目录。
    if file_manifest_exceeds_limits(files) {
        return IncomingFiles::Deferred {
            state: SyncFileState::Unavailable,
            total_bytes: 0,
            file_count: files.len() as i64,
        };
    }

    let roots = group_incoming_roots(&dto.content, files);
    if roots.is_empty() {
        return IncomingFiles::Ignore;
    }

    // 目录必须完整枚举（含子树大小）才能按块重建；顶层文件缺指纹同样只能保留路径。
    let mut transferable = true;
    let mut total_bytes: i64 = 0;
    let mut file_count: i64 = 0;

    for root in &roots {
        if root.is_dir && !root.complete {
            transferable = false;
            break;
        }
        if root.files.is_empty() && !root.is_dir {
            transferable = false;
            break;
        }

        for entry in &root.files {
            // 目录条目只用于还原目录结构，没有字节大小。
            if entry.is_dir {
                continue;
            }
            match entry.size {
                Some(size) if size >= 0 => {
                    total_bytes = total_bytes.saturating_add(size);
                    file_count = file_count.saturating_add(1);
                }
                _ => {
                    transferable = false;
                    break;
                }
            }
        }
        if !transferable {
            break;
        }
    }

    let settings = app.state::<SettingsStore>().snapshot();
    let threshold_bytes = megabytes_to_bytes(settings.sync.file_threshold);
    let max_bytes = megabytes_to_bytes(settings.sync.file_max_size);

    if !transferable || total_bytes as u64 > max_bytes {
        return IncomingFiles::Deferred {
            state: SyncFileState::Unavailable,
            total_bytes,
            file_count,
        };
    }

    if total_bytes as u64 > threshold_bytes {
        return IncomingFiles::Deferred {
            state: SyncFileState::Pending,
            total_bytes,
            file_count,
        };
    }

    // 内联拉取失败时不阻断入库：降级为按需拉取，粘贴时重试。
    match file_transfer::pull_files(app, &dto.content, source_device_id, files, None).await {
        Ok(paths) => IncomingFiles::Pulled(paths),
        Err(err) => {
            log::warn!("sync ingest: pull files of item from {source_device_id} failed: {err}");
            IncomingFiles::Deferred {
                state: SyncFileState::Pending,
                total_bytes,
                file_count,
            }
        }
    }
}

/// MB 设置值换算为字节数（文件阈值按条目内文件总大小计）。
fn megabytes_to_bytes(value: u64) -> u64 {
    value.saturating_mul(1024 * 1024)
}

/// 把 files 条目的换行分隔路径串拆成列表。
pub(crate) fn split_paths(content: &str) -> Vec<String> {
    content
        .split('\n')
        .filter(|path| !path.is_empty())
        .map(str::to_owned)
        .collect()
}

/// 接收端入库远端条目：类型门控 + 图片回源拉取 + upsert + emit。
pub async fn ingest_remote_item(
    app: &AppHandle,
    source_device_id: &str,
    dto: &SyncClipboardItemDto,
) {
    let settings = app.state::<SettingsStore>().snapshot();
    if !settings.sync.enabled {
        return;
    }

    let kind = match dto.kind.as_str() {
        "text" => ClipboardKind::Text,
        "image" => ClipboardKind::Image,
        "files" => ClipboardKind::Files,
        other => {
            log::warn!("sync ingest: unknown kind '{other}', skipped");
            return;
        }
    };

    let allowed = match kind {
        ClipboardKind::Text => settings.sync.sync_text,
        ClipboardKind::Image => settings.sync.sync_image,
        ClipboardKind::Files => settings.sync.sync_files,
    };
    if !allowed {
        return;
    }

    let mut content = dto.content.clone();
    let mut width = dto.width;
    let mut height = dto.height;
    let mut size = dto.size;

    // files：远端路径本机不可用时按阈值决定内联拉取，或登记状态留待粘贴时拉取。
    let files_plan = if kind == ClipboardKind::Files {
        resolve_incoming_files(app, source_device_id, dto).await
    } else {
        IncomingFiles::Ignore
    };
    let deferred_files = match files_plan {
        IncomingFiles::Pulled(paths) => {
            if !paths.is_empty() {
                content = paths.join("\n");
            }
            None
        }
        IncomingFiles::Deferred {
            state,
            total_bytes,
            file_count,
        } => Some((state, total_bytes, file_count)),
        IncomingFiles::Ignore => None,
    };

    // 图片：按发送方文件名回源拉取原图字节并落盘。两端字节一致 → 文件名一致 → 天然去重。
    if kind == ClipboardKind::Image {
        let pool = app.state::<crate::db::DatabaseState>().pool().await;
        let Ok(Some(device)) = devices::find_device(&pool, source_device_id).await else {
            log::warn!("sync ingest: source device {source_device_id} not paired");
            return;
        };
        if !device.allow_receive {
            log::warn!("sync ingest: source device {source_device_id} receive disabled");
            return;
        }

        let manager = app.state::<Arc<SyncManager>>();
        let Some(address) =
            manager.device_address(source_device_id, device.last_address.as_deref())
        else {
            log::warn!("sync ingest: source device {source_device_id} address unknown");
            return;
        };

        let bytes = match client::fetch_image(app, &device.shared_key, &address, &dto.content).await
        {
            Ok(bytes) => bytes,
            Err(err) => {
                log::warn!(
                    "sync ingest: fetch image {} from {source_device_id} failed: {err}",
                    dto.content
                );
                return;
            }
        };

        // 宽高缺失时本地解码补齐（store 不解码图片）。
        let (w, h) = match (dto.width, dto.height) {
            (Some(w), Some(h)) if w > 0 && h > 0 => (w, h),
            _ => match image::load_from_memory(&bytes) {
                Ok(decoded) => (i64::from(decoded.width()), i64::from(decoded.height())),
                Err(_) => (0, 0),
            },
        };

        let store = app.state::<crate::clipboard::ImageStore>().inner().clone();
        match store.store(&crate::clipboard::ImagePayload {
            bytes,
            width: w.clamp(0, i64::from(u32::MAX)) as u32,
            height: h.clamp(0, i64::from(u32::MAX)) as u32,
        }) {
            Ok(stored) => {
                content = stored.file_name;
                width = Some(stored.width);
                height = Some(stored.height);
                size = Some(stored.size);
            }
            Err(err) => {
                log::warn!("sync ingest: store remote image failed: {err}");
                return;
            }
        }
    }

    let created_at = chrono::DateTime::parse_from_rfc3339(&dto.created_at)
        .map(|ts| ts.with_timezone(&chrono::Utc))
        .unwrap_or_else(|_| chrono::Utc::now());
    // 防御：远端时钟超前时夹回当前时间，避免历史列表排序错乱。
    let now = chrono::Utc::now();
    let created_at = created_at.min(now);

    let item = ClipboardItem {
        id: uuid::Uuid::new_v4().to_string(),
        kind,
        sub_kind: dto.sub_kind.as_deref().map(parse_sub_kind_tag),
        group_id: None,
        source_app_id: None,
        content_hash: items::content_hash(kind, &content),
        content,
        search_text: dto.search_text.clone(),
        summary: dto.summary.clone(),
        file_types: dto.file_types.clone(),
        size,
        width,
        height,
        use_count: 1,
        is_favorite: false,
        is_pinned: false,
        is_sensitive: dto.is_sensitive,
        platform: parse_platform_tag(&dto.platform),
        sync_source_device_id: Some(source_device_id.to_owned()),
        note: None,
        created_at,
        updated_at: created_at,
        source_app_name: None,
        source_app_icon_file: None,
        source_app_icon_path: None,
        image_thumbnail_path: None,
        file_entries: None,
        files_preview_kind: None,
        available_actions: Vec::new(),
        color_preview: None,
        display_created_at: String::new(),
    };

    let pool = app.state::<crate::db::DatabaseState>().pool().await;
    match upsert_item(&pool, &item).await {
        Ok(result) => {
            if let Some((state, total_bytes, file_count)) = deferred_files {
                // 保存发送端文件指纹：粘贴时没有它就无法按块拉取。
                let files_json = dto
                    .files
                    .as_deref()
                    .and_then(|files| serde_json::to_string(files).ok());
                if let Err(err) = file_state::upsert(
                    &pool,
                    &result.id,
                    source_device_id,
                    state,
                    total_bytes,
                    file_count,
                    files_json.as_deref(),
                )
                .await
                {
                    log::warn!("sync ingest: upsert file state failed: {err}");
                }
            }

            if let Err(err) = app.emit(
                crate::clipboard::watcher::CLIPBOARD_UPDATED_EVENT,
                json!({
                    "id": result.id,
                    "kind": item.kind,
                    "deduplicated": result.deduplicated,
                }),
            ) {
                log::warn!("emit clipboard updated (synced) failed: {err}");
            }
        }
        Err(err) => {
            log::warn!("sync ingest: upsert remote item failed: {err}");
        }
    }
}

/// 已配对设备重新上线（或手动刷新）触发的增量拉取。
pub async fn pull_from_device(app: &AppHandle, device_id: &str) {
    let Some(manager) = app.try_state::<Arc<SyncManager>>() else {
        return;
    };
    if !manager.is_running() || !manager.try_acquire_pull_slot(device_id) {
        return;
    }

    let pool = app.state::<crate::db::DatabaseState>().pool().await;
    let Some(device) = (match devices::find_device(&pool, device_id).await {
        Ok(device) => device,
        Err(err) => {
            log::warn!("sync pull: find device {device_id} failed: {err}");
            return;
        }
    }) else {
        return;
    };

    // 「接收」权限关闭时，不再主动从该设备拉取内容。
    if !device.allow_receive {
        return;
    }

    let Some(address) = manager.device_address(device_id, device.last_address.as_deref()) else {
        return;
    };

    if let Err(err) = client::pull_items(app, &device, &address).await {
        log::warn!("sync pull from {device_id} ({address}) failed: {err}");
    }
}

/// 广播设备列表变化事件给所有窗口（异步取库，故 spawn）。
pub fn emit_devices_updated(app: &AppHandle) {
    let app = app.clone();
    tauri::async_runtime::spawn(async move {
        let payload = sync_state_payload(&app).await;
        if let Err(err) = app.emit(EVENT_DEVICES_UPDATED, payload) {
            log::warn!("emit devices updated event failed: {err}");
        }
    });
}

/// 广播发起端配对结果事件。
pub fn emit_pair_result(app: &AppHandle, device_id: &str, accepted: bool, reason: Option<String>) {
    let event = PairResultEvent {
        device_id: device_id.to_owned(),
        accepted,
        reason,
    };
    if let Err(err) = app.emit(EVENT_PAIR_RESULT, event) {
        log::warn!("emit pair result event failed: {err}");
    }
}

/// 广播配对 SAS 验证码事件（发起端展示，接收端确认后同样展示）。
pub fn emit_pair_sas(app: &AppHandle, device_id: &str, sas: &str) {
    let event = json!({
        "deviceId": device_id,
        "sas": sas,
    });
    if let Err(err) = app.emit(EVENT_PAIR_SAS, event) {
        log::warn!("emit pair sas event failed: {err}");
    }
}

/// 探测已配对设备的可达性，并回写最近可用地址。
///
/// mDNS 只在同一广播域生效；跨网段手动配对的设备靠这一步维持在线显示，
/// 探测成功的地址也会写回 `last_address`，供后续推送与拉取使用。
async fn probe_paired_devices(app: &AppHandle, manager: &Arc<SyncManager>) {
    let pool = app.state::<crate::db::DatabaseState>().pool().await;
    let list = match devices::list_devices(&pool).await {
        Ok(list) => list,
        Err(err) => {
            log::warn!("sync probe: list devices failed: {err}");
            return;
        }
    };

    let mut changed = false;
    for device in list {
        // 地址优先用 mDNS 缓存，其次用上次成功通信的地址。
        let address = manager.device_address(&device.device_id, device.last_address.as_deref());
        let Some(address) = address else {
            changed |= manager.set_online(&device.device_id, false);
            continue;
        };

        // 用认证探测判断在线：只有持有共享密钥的真实对端才算在线，
        // 避免同网段其它程序占用同一地址时误判。
        let online = client::verify_peer(manager.device_id(), &device.shared_key, &address).await;
        changed |= manager.set_online(&device.device_id, online);

        if online {
            let _ = devices::touch_device(&pool, &device.device_id, &address).await;
        }
    }

    if changed {
        emit_devices_updated(app);
    }
}

/// 组装同步状态快照（命令与事件共用）。
pub async fn sync_state_payload(app: &AppHandle) -> SyncStateDto {
    let settings = app.state::<SettingsStore>().snapshot();
    let manager = app.state::<Arc<SyncManager>>();

    let nearby = manager.nearby_snapshot();
    let nearby_ids: HashMap<String, &NearbyDeviceDto> = nearby
        .iter()
        .map(|entry| (entry.device_id.clone(), entry))
        .collect();

    let pool = app.state::<crate::db::DatabaseState>().pool().await;
    let paired = devices::list_devices(&pool).await.unwrap_or_default();
    let blocked_ids = devices::list_blacklisted_ids(&pool)
        .await
        .unwrap_or_default();

    let paired = paired
        .into_iter()
        .map(|record| PairedDeviceDto {
            device_id: record.device_id.clone(),
            name: record.name,
            platform: platform_tag(record.platform).to_owned(),
            allow_send: record.allow_send,
            allow_receive: record.allow_receive,
            online: manager.is_device_online(&record.device_id),
            last_seen_at: record.last_seen_at.map(|ts| ts.to_rfc3339()),
            last_pull_at: record.last_pull_at.map(|ts| ts.to_rfc3339()),
        })
        .collect();

    let blocked = blocked_ids
        .into_iter()
        .map(|device_id| {
            let nearby = nearby_ids.get(&device_id);
            BlockedDeviceDto {
                name: nearby.map(|entry| entry.name.clone()),
                platform: nearby.map(|entry| entry.platform.clone()),
                device_id,
            }
        })
        .collect();

    SyncStateDto {
        enabled: settings.sync.enabled,
        device_id: manager.device_id().to_owned(),
        device_name: manager.effective_device_name(&settings),
        port: manager.running_port().unwrap_or(settings.sync.port),
        nearby,
        paired,
        blocked,
    }
}

fn platform_tag(platform: Platform) -> &'static str {
    match platform {
        Platform::Macos => "macos",
        Platform::Windows => "windows",
    }
}

fn kind_tag(kind: ClipboardKind) -> &'static str {
    match kind {
        ClipboardKind::Text => "text",
        ClipboardKind::Image => "image",
        ClipboardKind::Files => "files",
    }
}

fn sub_kind_tag(kind: ClipboardSubKind) -> &'static str {
    match kind {
        ClipboardSubKind::Rtf => "rtf",
        ClipboardSubKind::Html => "html",
        ClipboardSubKind::Url => "url",
        ClipboardSubKind::Email => "email",
        ClipboardSubKind::Color => "color",
        ClipboardSubKind::Path => "path",
    }
}

fn parse_sub_kind_tag(tag: &str) -> ClipboardSubKind {
    match tag {
        "rtf" => ClipboardSubKind::Rtf,
        "html" => ClipboardSubKind::Html,
        "email" => ClipboardSubKind::Email,
        "color" => ClipboardSubKind::Color,
        "path" => ClipboardSubKind::Path,
        _ => ClipboardSubKind::Url,
    }
}

/// 目录展开出的单个条目（尚未附带整文件哈希）。
struct GatheredFile {
    /// 条目在本机的绝对路径，接收端按此请求分块。
    source: String,
    /// 相对顶层目录的路径（`/` 分隔）；顶层文件为 `None`。
    rel_path: Option<String>,
    /// 是否为目录（嵌套空目录也需要在接收端还原）。
    is_dir: bool,
    size: i64,
    mtime_ms: Option<i64>,
}

/// 单个顶层条目的枚举结果。
struct GatheredRoot {
    source: String,
    is_dir: bool,
    /// 目录是否完整枚举；普通文件恒为 `true`。
    complete: bool,
    files: Vec<GatheredFile>,
}

/// 采集 files 条目的传输指纹（大小 / 修改时间 / 头部哈希 / 整文件哈希）。
///
/// 目录会递归展开：目录本身一条 `is_dir` 记录，子树文件各带相对路径。
/// 超出 [`DIR_FILE_LIMIT`]、超过深度上限、含符号链接或读取失败时把目录标记为
/// 不完整，接收端据此退化为「仅路径可用」，不会误判为可拉取。
///
/// 整文件哈希只在该条目需要按需拉取（文件总大小超过 `inline_threshold_bytes`
/// 且不超过 `max_bytes`）时计算：它要完整顺序读一遍文件，小文件内联传输不需要
/// 这份校验，超上限的条目也不会被拉取。
fn collect_file_fingerprints(
    content: &str,
    inline_threshold_bytes: u64,
    max_bytes: u64,
) -> Option<Vec<SyncFileEntryDto>> {
    let paths: Vec<&str> = content
        .split('\n')
        .filter(|path| !path.is_empty())
        .take(FILE_FINGERPRINT_LIMIT)
        .collect();
    if paths.is_empty() {
        return None;
    }

    let mut remaining_files = DIR_FILE_LIMIT;
    let mut roots: Vec<GatheredRoot> = Vec::with_capacity(paths.len());
    let mut total_bytes: u64 = 0;

    for path in paths {
        let meta = std::fs::metadata(path).ok();
        match meta {
            Some(meta) if meta.is_dir() => {
                let mut files = Vec::new();
                let complete = walk_directory(path, &mut files, &mut remaining_files);
                for file in &files {
                    total_bytes = total_bytes.saturating_add(file.size.max(0) as u64);
                }
                roots.push(GatheredRoot {
                    source: path.to_owned(),
                    is_dir: true,
                    complete,
                    files,
                });
            }
            Some(meta) => {
                let size = meta.len() as i64;
                total_bytes = total_bytes.saturating_add(size.max(0) as u64);
                roots.push(GatheredRoot {
                    source: path.to_owned(),
                    is_dir: false,
                    complete: true,
                    files: vec![GatheredFile {
                        source: path.to_owned(),
                        rel_path: None,
                        is_dir: false,
                        size,
                        mtime_ms: file_mtime_ms(&meta),
                    }],
                });
            }
            // 元信息不可读：不产出文件条目，接收端按「仅路径可用」处理。
            None => roots.push(GatheredRoot {
                source: path.to_owned(),
                is_dir: false,
                complete: false,
                files: Vec::new(),
            }),
        }
    }

    let need_integrity_hash = total_bytes > inline_threshold_bytes && total_bytes <= max_bytes;
    let mut entries = Vec::new();

    for (index, root) in roots.into_iter().enumerate() {
        if root.is_dir {
            // 目录根只带路径与完整标记，字节由子树文件承担。
            entries.push(SyncFileEntryDto {
                path: root.source,
                is_dir: true,
                size: None,
                mtime_ms: None,
                head_hash: None,
                hash: None,
                root_index: Some(index),
                rel_path: None,
                complete: Some(root.complete),
            });
        }

        for file in root.files {
            let GatheredFile {
                source,
                rel_path,
                is_dir,
                size,
                mtime_ms,
            } = file;

            if is_dir {
                // 嵌套目录：只带相对路径，接收端据此还原空目录结构。
                entries.push(SyncFileEntryDto {
                    path: source,
                    is_dir: true,
                    size: None,
                    mtime_ms: None,
                    head_hash: None,
                    hash: None,
                    root_index: Some(index),
                    rel_path,
                    complete: None,
                });
                continue;
            }

            // 子树文件可能很多，不再计算头部哈希，避免复制大目录时反复读盘。
            let head_digest = if rel_path.is_none() {
                head_hash(&source)
            } else {
                None
            };
            let hash = if need_integrity_hash {
                file_hash(&source)
            } else {
                None
            };

            entries.push(SyncFileEntryDto {
                path: source,
                is_dir: false,
                size: Some(size),
                mtime_ms,
                head_hash: head_digest,
                hash,
                root_index: Some(index),
                rel_path,
                complete: None,
            });
        }
    }

    Some(entries)
}

/// 递归枚举目录内容（文件与子目录）；返回是否完整（未超限、无符号链接、无读取错误）。
fn walk_directory(root: &str, out: &mut Vec<GatheredFile>, remaining: &mut usize) -> bool {
    walk_directory_inner(Path::new(root), Path::new(""), 0, out, remaining)
}

/// [`walk_directory`] 的递归实现；`rel_prefix` 是相对目录根的路径。
fn walk_directory_inner(
    dir: &Path,
    rel_prefix: &Path,
    depth: usize,
    out: &mut Vec<GatheredFile>,
    remaining: &mut usize,
) -> bool {
    if depth >= DIR_DEPTH_LIMIT {
        return false;
    }

    let reader = match std::fs::read_dir(dir) {
        Ok(reader) => reader,
        Err(_) => return false,
    };
    let mut complete = true;

    for entry in reader {
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => {
                complete = false;
                continue;
            }
        };
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(_) => {
                complete = false;
                continue;
            }
        };
        // 符号链接可能成环或指向目录外，统一不传输。
        if file_type.is_symlink() {
            complete = false;
            continue;
        }

        let path = entry.path();
        let rel = rel_prefix.join(entry.file_name());

        if file_type.is_dir() {
            if *remaining == 0 {
                return false;
            }
            // 嵌套目录本身也进清单：空目录没有子文件，只能靠这条还原本地结构。
            if let Some(rel_path) = relative_path_string(&rel) {
                out.push(GatheredFile {
                    source: path.to_string_lossy().into_owned(),
                    rel_path: Some(rel_path),
                    is_dir: true,
                    size: 0,
                    mtime_ms: None,
                });
                *remaining -= 1;
            } else {
                complete = false;
            }

            if !walk_directory_inner(&path, &rel, depth + 1, out, remaining) {
                complete = false;
            }
            continue;
        }
        if !file_type.is_file() {
            complete = false;
            continue;
        }
        if *remaining == 0 {
            return false;
        }

        let meta = match entry.metadata() {
            Ok(meta) => meta,
            Err(_) => {
                complete = false;
                continue;
            }
        };
        let Some(rel_path) = relative_path_string(&rel) else {
            complete = false;
            continue;
        };

        out.push(GatheredFile {
            source: path.to_string_lossy().into_owned(),
            rel_path: Some(rel_path),
            is_dir: false,
            size: meta.len() as i64,
            mtime_ms: file_mtime_ms(&meta),
        });
        *remaining -= 1;
    }

    complete
}

/// 把相对路径渲染成 `a/b/c` 形式；含非 UTF-8 组件时返回 `None`。
fn relative_path_string(path: &Path) -> Option<String> {
    let mut parts: Vec<String> = Vec::new();

    for component in path.components() {
        match component {
            std::path::Component::Normal(part) => parts.push(part.to_str()?.to_owned()),
            _ => return None,
        }
    }

    if parts.is_empty() {
        return None;
    }

    Some(parts.join("/"))
}

/// 展开 files 条目的对外共享路径：顶层路径 + 目录内全部文件。
///
/// 仅用于登记允许对端分块读取的路径；目录本身不能按块读取，故只登记文件。
pub(crate) fn collect_shared_paths(content: &str) -> Vec<String> {
    let mut shared = Vec::new();
    let mut remaining_files = DIR_FILE_LIMIT;

    for path in content
        .split('\n')
        .filter(|path| !path.is_empty())
        .take(FILE_FINGERPRINT_LIMIT)
    {
        let meta = std::fs::metadata(path).ok();
        match meta {
            Some(meta) if meta.is_dir() => {
                let mut files = Vec::new();
                walk_directory(path, &mut files, &mut remaining_files);
                for file in files {
                    // 目录本身不能按块读取，只登记文件。
                    if file.is_dir {
                        continue;
                    }
                    shared.push(file.source);
                }
            }
            Some(_) => shared.push(path.to_owned()),
            None => {}
        }
    }

    shared
}

/// 文件修改时间（Unix 毫秒）；系统不支持或时间早于纪元时返回 `None`。
fn file_mtime_ms(meta: &std::fs::Metadata) -> Option<i64> {
    let modified = meta.modified().ok()?;
    let since_epoch = modified.duration_since(std::time::UNIX_EPOCH).ok()?;

    i64::try_from(since_epoch.as_millis()).ok()
}

/// 读文件头部做 blake3；不可读时返回 `None`（接收端按仅路径处理）。
fn head_hash(path: &str) -> Option<String> {
    use std::io::Read;

    let file = std::fs::File::open(path).ok()?;
    let mut reader = std::io::BufReader::new(file);
    let mut buffer = vec![0u8; FILE_HEAD_HASH_BYTES as usize];
    let read = reader.read(&mut buffer).ok()?;

    Some(blake3::hash(&buffer[..read]).to_hex().to_string())
}

/// 顺序读完整文件求 blake3；读失败返回 `None`（接收端退化为仅按大小校验）。
fn file_hash(path: &str) -> Option<String> {
    let file = std::fs::File::open(path).ok()?;
    let mut reader = std::io::BufReader::new(file);
    let mut hasher = blake3::Hasher::new();
    std::io::copy(&mut reader, &mut hasher).ok()?;

    Some(hasher.finalize().to_hex().to_string())
}

/// 本地条目 → 同步 DTO。
///
/// `inline_threshold_bytes` / `max_bytes` 决定 files 条目是否需要附带整文件哈希
/// （供按需拉取校验）。
pub(crate) fn item_to_dto(
    item: &ClipboardItem,
    inline_threshold_bytes: u64,
    max_bytes: u64,
) -> SyncClipboardItemDto {
    SyncClipboardItemDto {
        kind: kind_tag(item.kind).to_owned(),
        sub_kind: item.sub_kind.map(sub_kind_tag).map(str::to_owned),
        content: item.content.clone(),
        search_text: item.search_text.clone(),
        summary: item.summary.clone(),
        file_types: item.file_types.clone(),
        files: if item.kind == ClipboardKind::Files {
            collect_file_fingerprints(&item.content, inline_threshold_bytes, max_bytes)
        } else {
            None
        },
        size: item.size,
        width: item.width,
        height: item.height,
        is_sensitive: item.is_sensitive,
        platform: current_platform_tag().to_owned(),
        created_at: item.created_at.to_rfc3339(),
    }
}

/// 读取（或首次创建）持久设备 id。
fn load_or_create_identity(app: &AppHandle) -> Result<String> {
    #[derive(serde::Deserialize, serde::Serialize)]
    struct Identity {
        device_id: String,
    }

    let dir = crate::core::paths::config_dir(app)?;
    std::fs::create_dir_all(&dir)
        .map_err(|err| AppError::Other(anyhow::anyhow!("create config dir failed: {err}")))?;
    let path = dir.join(IDENTITY_FILE);

    if path.exists() {
        if let Ok(content) = std::fs::read_to_string(&path) {
            if let Ok(identity) = serde_json::from_str::<Identity>(&content) {
                if !identity.device_id.trim().is_empty() {
                    return Ok(identity.device_id);
                }
            }
        }
    }

    let identity = Identity {
        device_id: uuid::Uuid::new_v4().to_string(),
    };
    let content = serde_json::to_string_pretty(&identity)
        .map_err(|err| AppError::Other(anyhow::anyhow!("serialize identity failed: {err}")))?;
    std::fs::write(&path, content)
        .map_err(|err| AppError::Other(anyhow::anyhow!("write identity failed: {err}")))?;
    Ok(identity.device_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 构造一条文件清单条目，供归组测试复用。
    fn sample_entry(
        path: &str,
        is_dir: bool,
        root_index: Option<usize>,
        rel_path: Option<&str>,
        complete: Option<bool>,
    ) -> SyncFileEntryDto {
        SyncFileEntryDto {
            path: path.to_owned(),
            is_dir,
            size: if is_dir { None } else { Some(1) },
            mtime_ms: None,
            head_hash: None,
            hash: None,
            root_index,
            rel_path: rel_path.map(str::to_owned),
            complete,
        }
    }

    #[test]
    fn collect_file_fingerprints_expands_directory_tree() {
        let root = std::env::temp_dir().join(format!("ecopaste-sync-dir-{}", uuid::Uuid::new_v4()));
        let nested = root.join("nested");
        std::fs::create_dir_all(&nested).expect("create nested dir");
        std::fs::write(root.join("a.txt"), b"a").expect("write a.txt");
        std::fs::write(nested.join("b.txt"), b"bb").expect("write b.txt");

        let entries =
            collect_file_fingerprints(root.to_str().expect("utf8 root"), 5, 10).expect("manifest");

        // 目录根 + 一个子目录 + 两个文件。
        assert_eq!(entries.len(), 4);
        assert!(entries[0].is_dir);
        assert_eq!(entries[0].complete, Some(true));
        assert_eq!(entries[0].root_index, Some(0));

        let rels: Vec<&str> = entries
            .iter()
            .filter_map(|entry| entry.rel_path.as_deref())
            .collect();
        assert_eq!(rels.len(), 3);
        assert!(rels.contains(&"nested"));
        assert!(rels.contains(&"a.txt"));
        assert!(rels.contains(&"nested/b.txt"));

        let nested_dir = entries
            .iter()
            .find(|entry| entry.rel_path.as_deref() == Some("nested"))
            .expect("nested dir entry");
        assert!(nested_dir.is_dir);

        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn group_incoming_roots_collects_directory_children() {
        let files = vec![
            sample_entry("/src/photos", true, Some(0), None, Some(true)),
            sample_entry("/src/photos/a.jpg", false, Some(0), Some("a.jpg"), None),
            sample_entry(
                "/src/photos/sub/b.jpg",
                false,
                Some(0),
                Some("sub/b.jpg"),
                None,
            ),
        ];

        let roots = group_incoming_roots("/src/photos", &files);

        assert_eq!(roots.len(), 1);
        assert!(roots[0].is_dir);
        assert!(roots[0].complete);
        assert_eq!(roots[0].files.len(), 2);
    }

    #[test]
    fn group_incoming_roots_treats_legacy_directory_as_incomplete() {
        // 旧版本发送端的目录条目没有 root_index / complete，只能按仅路径处理。
        let files = vec![sample_entry("/src/photos", true, None, None, None)];

        let roots = group_incoming_roots("/src/photos", &files);

        assert_eq!(roots.len(), 1);
        assert!(roots[0].is_dir);
        assert!(!roots[0].complete);
    }
}
