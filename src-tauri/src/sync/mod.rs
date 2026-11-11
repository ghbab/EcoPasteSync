//! 局域网剪贴板同步（参考 CrossPaste 设计的轻量实现）。
//!
//! 架构（Rust-First，全部在本模块内）：
//! - **发现**：mDNS（`mdns-sd`）注册 `_ecopaste._tcp.local.` 服务并监听同类服务，
//!   TXT 记录携带 device_id / 设备名 / 平台，端口随实例广播。
//! - **传输**：本机常驻 axum HTTP 服务器，配对设备间以 reqwest 互推 / 互拉；
//!   请求经 blake3 keyed hash 签名认证，报文体经 ChaCha20-Poly1305 加密。
//! - **信任**：显式配对——发起方携带随机 seed 请求，接收方用户在前端确认后
//!   双方各自派生一致共享密钥入库；未配对设备无法推送任何内容。
//! - **数据流**：新剪贴板条目实时推送（图片推元数据、接收端回源拉取字节）；
//!   已配对设备重新上线时按 `last_pull_at` 游标增量拉取做断线补偿。
//!
//! 事件契约（前端 `src/constants/events.ts`）：
//! `sync://devices-updated` / `sync://pair-request` / `sync://pair-result`。

pub(crate) mod client;
mod crypto;
pub(crate) mod devices;
mod discovery;
mod protocol;
mod server;

use std::collections::HashMap;
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

pub use protocol::{
    BlockedDeviceDto, DeviceInfoDto, NearbyDeviceDto, PairStatusDto, PairedDeviceDto,
};
use protocol::{
    PairRequestDto, SyncClipboardItemDto, EVENT_DEVICES_UPDATED, EVENT_PAIR_REQUEST,
    EVENT_PAIR_RESULT,
};

/// 配对请求的受理有效期（秒）：超时后 B 端不再接受确认。
const PAIR_PENDING_TTL_SECS: u64 = 120;
/// 发起方轮询配对结果的总时长（秒）。
const PAIR_POLL_TIMEOUT_SECS: u64 = 150;
/// 同一设备两次 pull 的最小间隔（mDNS 反复 Resolved 时节流）。
const PULL_THROTTLE: Duration = Duration::from_secs(30);
/// 同步 identity 文件名（config 目录下）。
const IDENTITY_FILE: &str = "sync-identity.json";

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
}

/// B 端已作出的配对决定（供 A 轮询），带过期时间自动清理。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PairDecision {
    Accepted,
    Rejected,
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
    /// B 端待确认配对请求。
    pending_pairs: Mutex<HashMap<String, PendingPair>>,
    /// B 端配对决定（device_id → 决定 + 时间）。
    pair_decisions: Mutex<HashMap<String, (PairDecision, Instant)>>,
    /// 每设备 pull 节流。
    pull_throttle: Mutex<HashMap<String, Instant>>,
    /// axum 服务器关闭信号。
    shutdown_tx: Mutex<Option<watch::Sender<bool>>>,
    /// mDNS daemon 句柄（发现层注册后回填，关停时统一 shutdown）。
    daemon: Mutex<Option<mdns_sd::ServiceDaemon>>,
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

impl SyncManager {
    /// 创建管理器并加载（或首次生成）持久设备 id。
    pub fn new(app: &AppHandle) -> Result<Self> {
        let device_id = load_or_create_identity(app)?;
        Ok(Self {
            fingerprint: RwLock::new(None),
            device_id,
            nearby: RwLock::new(HashMap::new()),
            fullname_map: RwLock::new(HashMap::new()),
            pending_pairs: Mutex::new(HashMap::new()),
            pair_decisions: Mutex::new(HashMap::new()),
            pull_throttle: Mutex::new(HashMap::new()),
            shutdown_tx: Mutex::new(None),
            daemon: Mutex::new(None),
        })
    }

    /// 发现层回填 daemon 句柄。
    pub fn set_daemon(&self, daemon: mdns_sd::ServiceDaemon) {
        *self.daemon.lock().expect("sync poisoned") = Some(daemon);
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
            return "EcoPaste".to_owned();
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
            version: app.package_info().version.to_string(),
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
            return;
        }

        self.stop(app);
        if let Err(err) = self.start(app, &desired) {
            log::error!("sync start failed: {err:?}");
        }
    }

    /// 启动 HTTP 服务器与 mDNS 发现。
    fn start(&self, app: &AppHandle, desired: &RunningFingerprint) -> Result<()> {
        let ctx = Arc::new(server::ServerCtx {
            app: app.clone(),
            manager: app.state::<Arc<SyncManager>>().inner().clone(),
        });

        let (port, shutdown) = server::spawn_server(ctx, desired.port)?;
        *self.shutdown_tx.lock().expect("sync poisoned") = Some(shutdown);

        let manager = app.state::<Arc<SyncManager>>().inner().clone();
        discovery::spawn_discovery(app.clone(), manager, desired.device_name.clone(), port)?;

        *self.fingerprint.write().expect("sync poisoned") = Some(desired.clone());
        log::info!("sync started on port {port} as {}", self.device_id);
        emit_devices_updated(app);
        Ok(())
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
        self.nearby.write().expect("sync poisoned").clear();
        self.fullname_map.write().expect("sync poisoned").clear();
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

    /// B 端受理配对请求：登记待确认并发事件给前端，返回是否受理。
    pub fn handle_pair_request(&self, app: &AppHandle, request: PairRequestDto) -> bool {
        if request.device_id == self.device_id {
            return false;
        }

        // 先取出事件所需字段，避免 `request` move 进 HashMap 后再借用。
        let device_id = request.device_id.clone();
        let name = request.name.clone();
        let platform = request.platform.clone();

        {
            let mut pending = self.pending_pairs.lock().expect("sync poisoned");
            let now = Instant::now();
            // 过期清理 + 同设备旧请求覆盖。
            pending.retain(|_, pair| {
                now.duration_since(pair.received_at).as_secs() < PAIR_PENDING_TTL_SECS
            });
            pending.insert(
                device_id.clone(),
                PendingPair {
                    request,
                    received_at: now,
                },
            );
        }

        let event = json!({
            "deviceId": device_id,
            "name": name,
            "platform": platform,
        });
        if let Err(err) = app.emit(EVENT_PAIR_REQUEST, event) {
            log::warn!("emit pair request event failed: {err}");
        }
        true
    }

    fn pending_request(&self, device_id: &str) -> Option<PendingPair> {
        let pending = self.pending_pairs.lock().expect("sync poisoned");
        pending.get(device_id).cloned()
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

        if accept {
            let seed = crypto::seed_from_hex(&pair.request.seed)?;
            let key = crypto::derive_shared_key(&seed, &self.device_id, device_id);
            let address = self.device_address(device_id, None);
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
        }

        {
            let mut decisions = self.pair_decisions.lock().expect("sync poisoned");
            let now = Instant::now();
            decisions
                .retain(|_, (_, at)| now.duration_since(*at).as_secs() < PAIR_POLL_TIMEOUT_SECS);
            let decision = if accept {
                PairDecision::Accepted
            } else {
                PairDecision::Rejected
            };
            decisions.insert(device_id.to_owned(), (decision, now));
        }
        emit_devices_updated(app);
        Ok(())
    }

    /// B 端视角的配对状态（供 A 轮询）。
    pub fn pair_status_for(&self, device_id: &str) -> PairStatusDto {
        if let Some((decision, at)) = self
            .pair_decisions
            .lock()
            .expect("sync poisoned")
            .get(device_id)
            .copied()
        {
            if at.elapsed().as_secs() < PAIR_POLL_TIMEOUT_SECS {
                return match decision {
                    PairDecision::Accepted => PairStatusDto::Accepted,
                    PairDecision::Rejected => PairStatusDto::Rejected,
                };
            }
        }

        if let Some(pair) = self.pending_request(device_id) {
            if pair.received_at.elapsed().as_secs() < PAIR_PENDING_TTL_SECS {
                return PairStatusDto::Pending;
            }
        }

        // 无记录视为拒绝，发起方按失败处理。
        PairStatusDto::Rejected
    }

    /// A 端保存配对结果（轮询到 Accepted 后调用）。
    pub async fn save_paired_device(
        &self,
        app: &AppHandle,
        device_id: &str,
        seed: &str,
    ) -> Result<()> {
        let Some(entry) = self
            .nearby
            .read()
            .expect("sync poisoned")
            .get(device_id)
            .cloned()
        else {
            return Err(AppError::Other(anyhow::anyhow!(
                "device is no longer nearby"
            )));
        };

        let seed_bytes = crypto::seed_from_hex(seed)?;
        let key = crypto::derive_shared_key(&seed_bytes, &self.device_id, device_id);
        let now = chrono::Utc::now();
        let record = devices::SyncDeviceRecord {
            device_id: device_id.to_owned(),
            name: entry.name.clone(),
            platform: parse_platform_tag(&entry.platform),
            shared_key: crypto::key_to_hex(&key),
            allow_send: true,
            allow_receive: true,
            last_seen_at: Some(now),
            last_address: Some(entry.address.clone()),
            last_pull_at: None,
            created_at: now,
        };
        let pool = devices::pool(app).await;
        devices::upsert_device(&pool, &record).await?;
        emit_devices_updated(app);
        Ok(())
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

    let dto = item_to_dto(item);
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
            online: nearby_ids.contains_key(&record.device_id),
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

/// 本地条目 → 同步 DTO。
pub(crate) fn item_to_dto(item: &ClipboardItem) -> SyncClipboardItemDto {
    SyncClipboardItemDto {
        kind: kind_tag(item.kind).to_owned(),
        sub_kind: item.sub_kind.map(sub_kind_tag).map(str::to_owned),
        content: item.content.clone(),
        search_text: item.search_text.clone(),
        summary: item.summary.clone(),
        file_types: item.file_types.clone(),
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
