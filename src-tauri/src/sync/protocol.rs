//! 同步协议：跨端契约常量与线上 DTO。
//!
//! 事件名与前端 `src/constants/events.ts` 的 `TAURI_EVENT` 保持一一对应；
//! API 路径由 [`crate::sync::server`] 与 [`crate::sync::client`] 共用。

use serde::{Deserialize, Serialize};

/// mDNS 服务类型（局域网设备发现的唯一标识）。
pub const SERVICE_TYPE: &str = "_ecopaste._tcp.local.";

/// 附近 / 已配对设备列表变化事件。
pub const EVENT_DEVICES_UPDATED: &str = "sync://devices-updated";
/// 收到配对请求事件（接收端前端弹确认框，载荷携带 SAS 供人工比对）。
pub const EVENT_PAIR_REQUEST: &str = "sync://pair-request";
/// 配对 SAS 验证码事件（发起端收到受理响应后展示，与接收端弹窗内的数字比对）。
pub const EVENT_PAIR_SAS: &str = "sync://pair-sas";
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
/// 加密端点：认证探测（只校验调用方持有共享密钥，用于地址变更确认与在线探测）。
pub const API_VERIFY: &str = "/api/verify";
/// 加密端点：推送单条剪贴板条目。
pub const API_CLIPBOARD_PUSH: &str = "/api/clipboard/push";
/// 加密端点：按时间游标增量拉取剪贴板条目。
pub const API_CLIPBOARD_PULL: &str = "/api/clipboard/pull";
/// 加密端点：通知对端把某条已有记录置顶（远端重复复制同一批文件时使用）。
pub const API_CLIPBOARD_BUMP: &str = "/api/clipboard/bump";
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
/// 接收端允许的清单条目上限（目录展开后的文件与目录总数）。
///
/// 接收端不信任发送端声明的规模：超过上限的清单按「仅路径可用」处理，
/// 避免海量条目在归组与目录重建时耗尽内存。
pub const FILE_MANIFEST_LIMIT: usize = 8192;
/// 接收端允许的目录相对路径最大层级（`/` 分隔的段数）。
pub const FILE_MANIFEST_DEPTH_LIMIT: usize = 32;

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

/// 配对请求里设备 id 的最大字符数（正常为 UUID 36 字符）。
///
/// 超过上限的请求在接收端直接按非法输入拒绝，避免异常值进入钥匙串条目名。
pub const PAIR_DEVICE_ID_MAX_CHARS: usize = 128;
/// 配对请求里设备名的最大字符数，防止超长文本撑破配对窗口。
pub const PAIR_DEVICE_NAME_MAX_CHARS: usize = 64;
/// 配对请求里平台标识的最大字符数。
pub const PAIR_PLATFORM_MAX_CHARS: usize = 16;

/// 配对请求（明文 `/api/pair` 请求体）。
///
/// `public_key` 是发起方的 X25519 临时公钥（hex）：接收方在 202 响应里返回
/// 自己的临时公钥，双方各自做 ECDH 派生共享密钥。私钥从不上线，即使配对
/// 请求被窃听也无法还原共享密钥；配对期间两端比对 SAS 可发现中间人。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PairRequestDto {
    pub device_id: String,
    pub name: String,
    pub platform: String,
    pub public_key: String,
    /// 发起方同步服务监听端口。
    ///
    /// 跨网段手动配对时接收端无法通过 mDNS 获得发起方地址，
    /// 用「TCP 源 IP + 该端口」拼出回连地址，保证配对后双向推送可用。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
}

/// 配对请求受理响应（202 响应体）。
///
/// 携带接收方临时公钥，发起方据此完成 ECDH 并派生共享密钥。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PairAcceptedDto {
    pub status: PairStatusDto,
    /// 接收方 X25519 临时公钥（hex）。
    pub public_key: String,
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

/// 增量拉取端点签名串。
///
/// `since` 游标纳入签名，避免重放认证头后修改游标、扩大或缩小拉取范围。
pub fn pull_signature_target(since: Option<&str>) -> String {
    match since {
        Some(value) if !value.is_empty() => format!("{API_CLIPBOARD_PULL}|since={value}"),
        _ => API_CLIPBOARD_PULL.to_owned(),
    }
}

/// 单个文件的传输指纹（目录会被递归展开为「目录根 + 子树文件」多条记录）。
///
/// 目录根记录带 `is_dir = true`、`complete` 标记是否完整枚举；子树文件带
/// `rel_path` 表示相对目录根的路径。不可读路径以及超出采集上限的条目只带
/// `path`：接收端见到缺失指纹即按「仅路径可用」处理，不会误判为可拉取。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncFileEntryDto {
    pub path: String,
    pub is_dir: bool,
    /// 文件字节数；目录或不可读时为 `None`。
    pub size: Option<i64>,
    /// 修改时间（Unix 毫秒），拉取前用它快速判断源文件是否已变。
    pub mtime_ms: Option<i64>,
    /// 创建时间（Unix 毫秒）；落盘后接收端会把本地文件的创建时间还原成该值。
    /// 旧版本发送端没有该字段，缺失时接收端保持本地默认时间。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ctime_ms: Option<i64>,
    /// 文件头部 64KB 的 blake3 hex，低成本校验内容未被替换。
    pub head_hash: Option<String>,
    /// 整文件 blake3 hex；只对需要按需拉取的条目采集，拉取完成后用它校验完整性。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
    /// 所属顶层条目在 `content` 中的序号；旧版本发送端没有该字段。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_index: Option<usize>,
    /// 目录子树文件的相对路径（`/` 分隔）；顶层条目或普通文件为 `None`。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rel_path: Option<String>,
    /// 目录是否已完整枚举；超限、含符号链接或读取失败时为 `false`。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub complete: Option<bool>,
}

/// 校验远端文件清单是否超出接收端的条目数 / 目录深度上限。
///
/// 发送端采集时已有自己的限制，但接收端不能依赖对端自律：伪造清单可能把
/// 归组、目录重建与内存占用放大到不可控规模。
pub fn file_manifest_exceeds_limits(files: &[SyncFileEntryDto]) -> bool {
    if files.len() > FILE_MANIFEST_LIMIT {
        return true;
    }

    for entry in files {
        let Some(rel_path) = entry.rel_path.as_deref() else {
            continue;
        };
        let depth = rel_path
            .split('/')
            .filter(|segment| !segment.is_empty())
            .count();
        if depth > FILE_MANIFEST_DEPTH_LIMIT {
            return true;
        }
    }

    false
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

/// 置顶端点签名串：内容哈希参与签名，避免重放签名置顶任意条目。
pub fn bump_signature_target(content_hash: &str) -> String {
    format!("{API_CLIPBOARD_BUMP}|{content_hash}")
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
