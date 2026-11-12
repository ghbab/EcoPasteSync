//! 同步协议：跨端契约常量与线上 DTO。
//!
//! 事件名与前端 `src/constants/events.ts` 的 `TAURI_EVENT` 保持一一对应；
//! API 路径由 [`crate::sync::server`] 与 [`crate::sync::client`] 共用。

use serde::{Deserialize, Serialize};

/// mDNS 服务类型（局域网设备发现的唯一标识）。
pub const SERVICE_TYPE: &str = "_ecopaste._tcp.local.";

/// 附近 / 已配对设备列表变化事件。
pub const EVENT_DEVICES_UPDATED: &str = "sync://devices-updated";
/// 收到配对请求事件（接收端前端弹确认框）。
pub const EVENT_PAIR_REQUEST: &str = "sync://pair-request";
/// 发起端配对结果事件（超时 / 对端拒绝 / 成功）。
pub const EVENT_PAIR_RESULT: &str = "sync://pair-result";
/// 远端文件按需传输进度事件（粘贴前拉取文件时上报）。
pub const EVENT_FILE_PROGRESS: &str = "sync://file-transfer-progress";

/// 明文端点：设备信息（配对前探测）。
pub const API_INFO: &str = "/api/info";
/// 明文端点：发起配对。
pub const API_PAIR: &str = "/api/pair";
/// 明文端点：配对结果轮询。
pub const API_PAIR_STATUS: &str = "/api/pair/status";
/// 加密端点：推送单条剪贴板条目。
pub const API_CLIPBOARD_PUSH: &str = "/api/clipboard/push";
/// 加密端点：按时间游标增量拉取剪贴板条目。
pub const API_CLIPBOARD_PULL: &str = "/api/clipboard/pull";
/// 加密端点：拉取图片原图字节。
pub const API_IMAGE: &str = "/api/image";
/// 加密端点：按块拉取文件内容（参数走 query：`path` / `offset` / `len`）。
pub const API_FILE_CHUNK: &str = "/api/file";

/// 文件分块大小（字节）：按需拉取与内联拉取共用同一粒度。
pub const FILE_CHUNK_SIZE: i64 = 1024 * 1024;

/// 认证头：调用方设备 id。
pub const HEADER_DEVICE_ID: &str = "x-ecopaste-device";
/// 认证头：Unix 秒级时间戳（容差窗口内防重放）。
pub const HEADER_TIMESTAMP: &str = "x-ecopaste-timestamp";
/// 认证头：blake3 keyed hash 签名。
pub const HEADER_SIGNATURE: &str = "x-ecopaste-signature";

/// 单次 pull 返回的最大条数（断线补偿上限，防止老设备首次全量拖垮对端）。
pub const PULL_PAGE_LIMIT: i64 = 50;

/// 设备信息（明文 `/api/info` 响应与 mDNS TXT 记录共用字段）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeviceInfoDto {
    pub device_id: String,
    pub name: String,
    /// `macos` / `windows`。
    pub platform: String,
    /// 应用版本号。
    pub version: String,
}

/// 配对请求（明文 `/api/pair` 请求体）。
///
/// `seed` 为发起方生成的 32 字节随机数的 hex，双方据此派生共享密钥；
/// 明文传输在「接收端用户尚未确认」的前提下可接受（与 CrossPaste V1 token 同级），
/// 派生后的密钥从不明文上线。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PairRequestDto {
    pub device_id: String,
    pub name: String,
    pub platform: String,
    pub seed: String,
    /// 发起方同步服务监听端口。
    ///
    /// 跨网段手动配对时接收端无法通过 mDNS 获得发起方地址，
    /// 用「TCP 源 IP + 该端口」拼出回连地址，保证配对后双向推送可用。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
}

/// 配对结果轮询响应。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PairStatusDto {
    /// 接收端已受理，等待用户确认。
    Pending,
    /// 接收端用户已同意。
    Accepted,
    /// 接收端用户已拒绝。
    Rejected,
}

/// 文件分块端点签名串。
///
/// 路径以 blake3 摘要参与签名：既避免分隔符歧义，也避开客户端与服务端在 URL
/// 百分号编码上的差异。块偏移与长度一并签名，防止复用签名读取任意区间。
pub fn file_chunk_signature_target(path: &str, offset: u64, len: u64) -> String {
    let digest = blake3::hash(path.as_bytes()).to_hex().to_string();
    format!("{API_FILE_CHUNK}|{digest}|{offset}|{len}")
}

/// 单个文件的传输指纹。
///
/// 目录、不可读路径以及超出采集上限的文件只带 `path`：接收端见到缺失指纹即按
/// 「仅路径可用」处理，不会误判为可拉取。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncFileEntryDto {
    pub path: String,
    pub is_dir: bool,
    /// 文件字节数；目录或不可读时为 `None`。
    pub size: Option<i64>,
    /// 修改时间（Unix 毫秒），拉取前用它快速判断源文件是否已变。
    pub mtime_ms: Option<i64>,
    /// 文件头部 64KB 的 blake3 hex，低成本校验内容未被替换。
    pub head_hash: Option<String>,
    /// 整文件 blake3 hex；只对需要按需拉取的条目采集，拉取完成后用它校验完整性。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
}

/// 一条待同步的剪贴板条目（加密载荷的明文结构）。
///
/// 字段语义与 [`crate::db::models::ClipboardItem`] 对齐，但只保留跨端有意义的部分：
/// `id` 仅作排查参考，接收方入库时重新生成；`content` 对 text/files 是内容本体，
/// 对 image 是落盘文件名（接收方再按需拉取字节）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncClipboardItemDto {
    pub kind: String,
    pub sub_kind: Option<String>,
    pub content: String,
    pub search_text: Option<String>,
    pub summary: Option<String>,
    pub file_types: Option<String>,
    /// files 类型条目的文件指纹；其它类型为 `None`。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files: Option<Vec<SyncFileEntryDto>>,
    pub size: Option<i64>,
    pub width: Option<i64>,
    pub height: Option<i64>,
    pub is_sensitive: bool,
    /// 发送方平台标记（接收方入库时保留，列表可显示「来自 Windows / macOS」）。
    pub platform: String,
    /// 发送方的创建时间（RFC3339），接收方用于排序与 pull 游标推进。
    pub created_at: String,
}

/// 附近设备快照项（发现层 → 前端）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NearbyDeviceDto {
    pub device_id: String,
    pub name: String,
    pub platform: String,
    /// `host:port`，供手动诊断；前端一般不直接使用。
    pub address: String,
}

/// 已配对设备快照项（设备仓储 → 前端）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PairedDeviceDto {
    pub device_id: String,
    pub name: String,
    pub platform: String,
    pub allow_send: bool,
    pub allow_receive: bool,
    /// 设备当前是否在线（mDNS 可见或可达性探测成功）。
    pub online: bool,
    pub last_seen_at: Option<String>,
    pub last_pull_at: Option<String>,
}

/// 黑名单设备项（前端「已忽略设备」管理）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockedDeviceDto {
    pub device_id: String,
    /// 附近可见时带上名称，便于识别。
    pub name: Option<String>,
    pub platform: Option<String>,
}
