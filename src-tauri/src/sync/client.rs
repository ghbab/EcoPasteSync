//! 同步客户端（reqwest）：配对发起与轮询、条目推送、增量拉取、图片回源。
//!
//! 所有加密端点复用 [`crate::sync::server`] 的认证头与封包格式。

use std::time::Duration;

use reqwest::Client;
use tauri::{AppHandle, Manager};

use crate::core::{AppError, Result};
use crate::sync::crypto;
use crate::sync::devices::{self, SyncDeviceRecord};
use crate::sync::protocol::{
    file_chunk_signature_target, pull_signature_target, DeviceInfoDto, NearbyDeviceDto,
    PairAcceptedDto, PairRequestDto, PairStatusDto, SyncClipboardItemDto, API_CLIPBOARD_PULL,
    API_CLIPBOARD_PUSH, API_FILE_CHUNK, API_IMAGE, API_INFO, API_PAIR, API_PAIR_STATUS, API_VERIFY,
    HEADER_DEVICE_ID, HEADER_SIGNATURE, HEADER_TIMESTAMP,
};
use crate::sync::{emit_pair_result, emit_pair_sas, NearbyEntry, SyncManager};

/// 单次 HTTP 请求超时：推送大文本（可达数 MB）需要宽松上限。
const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
/// 发起方轮询配对结果的总时长。
const PAIR_POLL_TIMEOUT: Duration = Duration::from_secs(150);
/// 发起方轮询间隔。
const PAIR_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// 构建带认证头的加密请求 client。
fn client() -> Result<Client> {
    Client::builder()
        .timeout(REQUEST_TIMEOUT)
        // 不跟随重定向：避免对端用 3xx 把带签名的认证头转发到其它地址。
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|err| AppError::Other(anyhow::anyhow!("build sync http client failed: {err}")))
}

fn now_unix() -> i64 {
    chrono::Utc::now().timestamp()
}

/// 发起配对（A 端）：向附近设备发送明文请求，成功受理后轮询结果。
/// 结果通过 `sync://pair-result` 事件通知前端；接受时落库。
pub fn start_pairing(app: AppHandle, manager: std::sync::Arc<SyncManager>, device_id: String) {
    let Some(entry) = manager.nearby_entry(&device_id) else {
        emit_pair_result(&app, &device_id, false, Some("device-offline".to_owned()));
        return;
    };

    start_pairing_with_entry(app, manager, entry);
}

/// 用固定的设备条目发起配对；mDNS 发现与手动指定地址共用这一条路径。
pub fn start_pairing_with_entry(
    app: AppHandle,
    manager: std::sync::Arc<SyncManager>,
    entry: NearbyEntry,
) {
    tauri::async_runtime::spawn(async move {
        let device_id = entry.device_id.clone();
        let info = manager.device_info(&app);
        // 生成发起方临时密钥对：私钥只留在内存，公钥随请求明文发给接收方。
        let keypair = crypto::generate_pairing_keypair();
        // 本机服务端口：对端用它配合 TCP 源 IP 得到回连地址（跨网段也能双向同步）。
        let port = manager.running_port().unwrap_or_else(|| {
            app.state::<crate::settings::SettingsStore>()
                .snapshot()
                .sync
                .port
        });
        let request = PairRequestDto {
            device_id: info.device_id,
            name: info.name,
            platform: info.platform,
            public_key: crypto::public_key_to_hex(&keypair.public),
            port: Some(port),
        };

        let client = match client() {
            Ok(client) => client,
            Err(err) => {
                log::warn!("sync pair: build client failed: {err}");
                emit_pair_result(&app, &device_id, false, Some("internal-error".to_owned()));
                return;
            }
        };

        let url = format!("http://{}{API_PAIR}", entry.address);
        let sent = client
            .post(&url)
            .json(&request)
            .timeout(Duration::from_secs(5))
            .send()
            .await;

        match sent {
            Ok(resp) if resp.status().as_u16() == 202 => {
                // 受理成功：用响应里的接收方临时公钥完成 ECDH 派生共享密钥。
                let accepted = match resp.json::<PairAcceptedDto>().await {
                    Ok(accepted) => accepted,
                    Err(err) => {
                        log::warn!("sync pair: decode accepted response failed: {err}");
                        emit_pair_result(
                            &app,
                            &device_id,
                            false,
                            Some("internal-error".to_owned()),
                        );
                        return;
                    }
                };
                let peer_public = match crypto::public_key_from_hex(&accepted.public_key) {
                    Ok(public) => public,
                    Err(err) => {
                        log::warn!("sync pair: invalid peer public key: {err}");
                        emit_pair_result(
                            &app,
                            &device_id,
                            false,
                            Some("internal-error".to_owned()),
                        );
                        return;
                    }
                };
                let key = match crypto::derive_pairing_key(
                    &keypair.secret,
                    &peer_public,
                    manager.device_id(),
                    &device_id,
                ) {
                    Ok(key) => key,
                    Err(err) => {
                        log::warn!("sync pair: derive shared key failed: {err}");
                        emit_pair_result(
                            &app,
                            &device_id,
                            false,
                            Some("internal-error".to_owned()),
                        );
                        return;
                    }
                };
                // 展示 SAS：与接收端弹窗里的数字一致，才说明配对未被中间人劫持。
                emit_pair_sas(&app, &device_id, &crypto::pairing_sas(&key));
                poll_pair_result(app, manager, client, entry, key).await;
            }
            Ok(resp) => {
                log::warn!("sync pair: unexpected status {} from {url}", resp.status());
                emit_pair_result(&app, &device_id, false, Some("pair-rejected".to_owned()));
            }
            Err(err) => {
                log::warn!("sync pair: request {url} failed: {err}");
                emit_pair_result(&app, &device_id, false, Some("network-error".to_owned()));
            }
        }
    });
}

/// 手动指定地址发起配对：先读取对端设备信息，再走与 mDNS 相同的配对流程。
///
/// 适用于跨网段、mDNS 发现不到的设备；对端仍需在配对请求弹窗中确认。
pub async fn start_pairing_with_address(
    app: AppHandle,
    manager: std::sync::Arc<SyncManager>,
    raw_address: &str,
    default_port: u16,
) -> Result<NearbyDeviceDto> {
    let address = normalize_manual_address(raw_address, default_port)?;
    let info = fetch_device_info(&address).await?;

    if info.device_id == manager.device_id() {
        return Err(AppError::Other(anyhow::anyhow!(
            "cannot pair with the local device"
        )));
    }

    let pool = devices::pool(&app).await;
    if devices::find_device(&pool, &info.device_id)
        .await?
        .is_some()
    {
        return Err(AppError::Other(anyhow::anyhow!("device is already paired")));
    }
    if devices::is_blacklisted(&pool, &info.device_id).await? {
        return Err(AppError::Other(anyhow::anyhow!("device is blocked")));
    }

    let entry = NearbyEntry {
        device_id: info.device_id.clone(),
        name: info.name.clone(),
        platform: info.platform.clone(),
        address: address.clone(),
    };
    let dto = NearbyDeviceDto {
        device_id: info.device_id,
        name: info.name,
        platform: info.platform,
        address,
    };

    start_pairing_with_entry(app, manager, entry);
    Ok(dto)
}

/// 规范化手动输入的设备地址，返回 `host:port`。
///
/// 接受 `192.168.1.20`、`192.168.1.20:47654`、`http://host[:port]` 与
/// `[ipv6]:port`；未写端口时使用本机同步端口（两端默认配置一致）。
pub fn normalize_manual_address(raw: &str, default_port: u16) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(AppError::Other(anyhow::anyhow!("address is empty")));
    }

    let without_scheme = trimmed
        .strip_prefix("http://")
        .or_else(|| trimmed.strip_prefix("https://"))
        .unwrap_or(trimmed);
    let value = without_scheme.trim_end_matches('/');

    if value.is_empty()
        || value.contains('/')
        || value.contains('?')
        || value.contains('#')
        || value.contains('@')
        || value.chars().any(char::is_whitespace)
    {
        return Err(AppError::Other(anyhow::anyhow!(
            "address must be an IP address with an optional port"
        )));
    }

    // 方括号形式的 IPv6 地址，端口写在括号外。
    if value.starts_with('[') {
        let Some(end) = value.find(']') else {
            return Err(AppError::Other(anyhow::anyhow!(
                "address must be an IP address with an optional port"
            )));
        };

        let host = &value[..=end];
        let rest = &value[end + 1..];
        if rest.is_empty() {
            return Ok(format!("{host}:{default_port}"));
        }

        let Some(raw_port) = rest.strip_prefix(':') else {
            return Err(AppError::Other(anyhow::anyhow!(
                "address must be an IP address with an optional port"
            )));
        };

        return Ok(format!("{host}:{}", parse_port(raw_port)?));
    }

    match value.rsplit_once(':') {
        Some((host, raw_port)) => {
            if host.is_empty() || host.contains(':') {
                return Err(AppError::Other(anyhow::anyhow!(
                    "IPv6 addresses must use [address]:port"
                )));
            }

            Ok(format!("{host}:{}", parse_port(raw_port)?))
        }
        None => Ok(format!("{value}:{default_port}")),
    }
}

/// 解析端口：非法数字与 0 都拒绝。
fn parse_port(raw: &str) -> Result<u16> {
    let port: u16 = raw
        .parse()
        .map_err(|_| AppError::Other(anyhow::anyhow!("port is invalid")))?;

    if port == 0 {
        return Err(AppError::Other(anyhow::anyhow!("port is invalid")));
    }

    Ok(port)
}

/// 轮询对端的配对决定；`Accepted` 时保存配对记录并补一次增量拉取。
async fn poll_pair_result(
    app: AppHandle,
    manager: std::sync::Arc<SyncManager>,
    client: Client,
    entry: NearbyEntry,
    key: [u8; 32],
) {
    let device_id = entry.device_id.clone();
    let url = format!(
        "http://{}{API_PAIR_STATUS}?deviceId={}",
        entry.address,
        manager.device_id()
    );
    let started = std::time::Instant::now();

    loop {
        if started.elapsed() >= PAIR_POLL_TIMEOUT {
            emit_pair_result(&app, &device_id, false, Some("timeout".to_owned()));
            return;
        }

        tokio::time::sleep(PAIR_POLL_INTERVAL).await;

        let status = match client.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => match resp.json::<PairStatusDto>().await {
                Ok(status) => status,
                Err(err) => {
                    log::warn!("sync pair: decode status failed: {err}");
                    continue;
                }
            },
            Ok(resp) => {
                log::warn!("sync pair: status {} from {url}", resp.status());
                continue;
            }
            Err(err) => {
                log::warn!("sync pair: poll {url} failed: {err}");
                continue;
            }
        };

        match status {
            PairStatusDto::Pending => {
                continue;
            }
            PairStatusDto::Accepted => {
                if let Err(err) = manager.save_paired_device(&app, &entry, &key).await {
                    log::warn!("sync pair: save paired device failed: {err}");
                    emit_pair_result(&app, &device_id, false, Some("internal-error".to_owned()));
                    return;
                }
                // 新配对设备立即补一次近段历史。
                crate::sync::pull_from_device(&app, &device_id).await;
                emit_pair_result(&app, &device_id, true, None);
                return;
            }
            PairStatusDto::Rejected => {
                emit_pair_result(&app, &device_id, false, Some("pair-rejected".to_owned()));
                return;
            }
        }
    }
}

/// 推送单条剪贴板条目到对端（加密 + 认证）。
pub async fn push_item(
    app: &AppHandle,
    shared_key_hex: &str,
    address: &str,
    dto: &SyncClipboardItemDto,
) -> Result<()> {
    let key = crypto::key_from_hex(shared_key_hex)?;
    let manager = app.state::<std::sync::Arc<SyncManager>>();
    let self_id = manager.device_id().to_owned();

    let plain = serde_json::to_vec(dto).map_err(|err| AppError::Other(err.into()))?;
    let body = crypto::seal(&key, &plain)?;

    let timestamp = now_unix();
    let signature = crypto::sign_request(&key, &self_id, timestamp, API_CLIPBOARD_PUSH);

    let client = client()?;
    let url = format!("http://{address}{API_CLIPBOARD_PUSH}");
    let resp = client
        .post(&url)
        .header(HEADER_DEVICE_ID, &self_id)
        .header(HEADER_TIMESTAMP, timestamp.to_string())
        .header(HEADER_SIGNATURE, &signature)
        .body(body)
        .send()
        .await
        .map_err(|err| AppError::Other(anyhow::anyhow!("post to {url} failed: {err}")))?;

    if !resp.status().is_success() {
        return Err(AppError::Other(anyhow::anyhow!(
            "push to {url} returned {}",
            resp.status()
        )));
    }
    Ok(())
}

/// 拉取远端图片原图字节（加密 + 认证）。
pub async fn fetch_image(
    app: &AppHandle,
    shared_key_hex: &str,
    address: &str,
    file_name: &str,
) -> Result<Vec<u8>> {
    let key = crypto::key_from_hex(shared_key_hex)?;
    let manager = app.state::<std::sync::Arc<SyncManager>>();
    let self_id = manager.device_id().to_owned();

    // 复用 server 侧的路径格式（签名覆盖到具体文件，防挪用）。
    let path = format!("{API_IMAGE}/{file_name}");
    let timestamp = now_unix();
    let signature = crypto::sign_request(&key, &self_id, timestamp, &path);

    let client = client()?;
    let url = format!("http://{address}{path}");
    let resp = client
        .get(&url)
        .header(HEADER_DEVICE_ID, &self_id)
        .header(HEADER_TIMESTAMP, timestamp.to_string())
        .header(HEADER_SIGNATURE, &signature)
        .send()
        .await
        .map_err(|err| AppError::Other(anyhow::anyhow!("get {url} failed: {err}")))?;

    if !resp.status().is_success() {
        return Err(AppError::Other(anyhow::anyhow!(
            "get {url} returned {}",
            resp.status()
        )));
    }

    let sealed = resp
        .bytes()
        .await
        .map_err(|err| AppError::Other(anyhow::anyhow!("read body from {url} failed: {err}")))?;
    crypto::open(&key, &sealed)
}

/// 拉取文件块（加密 + 认证）。
///
/// 签名覆盖路径摘要、偏移与长度：服务端据此拒绝把同一签名挪用到其它区间，
/// 也不需要依赖两端对 URL 百分号编码的一致实现。
pub async fn fetch_file_chunk(
    app: &AppHandle,
    shared_key_hex: &str,
    address: &str,
    path: &str,
    offset: u64,
    len: u64,
) -> Result<Vec<u8>> {
    let key = crypto::key_from_hex(shared_key_hex)?;
    let manager = app.state::<std::sync::Arc<SyncManager>>();
    let self_id = manager.device_id().to_owned();

    let target = file_chunk_signature_target(path, offset, len);
    let timestamp = now_unix();
    let signature = crypto::sign_request(&key, &self_id, timestamp, &target);

    let mut url = reqwest::Url::parse(&format!("http://{address}{API_FILE_CHUNK}"))
        .map_err(|err| AppError::Other(anyhow::anyhow!("invalid sync file url: {err}")))?;
    url.query_pairs_mut()
        .append_pair("path", path)
        .append_pair("offset", &offset.to_string())
        .append_pair("len", &len.to_string());

    let client = client()?;
    let resp = client
        .get(url.clone())
        .header(HEADER_DEVICE_ID, &self_id)
        .header(HEADER_TIMESTAMP, timestamp.to_string())
        .header(HEADER_SIGNATURE, &signature)
        .send()
        .await
        .map_err(|err| AppError::Other(anyhow::anyhow!("get {url} failed: {err}")))?;

    if !resp.status().is_success() {
        return Err(AppError::Other(anyhow::anyhow!(
            "get {url} returned {}",
            resp.status()
        )));
    }

    let sealed = resp
        .bytes()
        .await
        .map_err(|err| AppError::Other(anyhow::anyhow!("read body from {url} failed: {err}")))?;
    crypto::open(&key, &sealed)
}

/// 增量拉取：按设备 `last_pull_at` 游标拉近段条目并逐条入库，随后推进游标。
pub async fn pull_items(app: &AppHandle, device: &SyncDeviceRecord, address: &str) -> Result<()> {
    let key = crypto::key_from_hex(&device.shared_key)?;
    let manager = app.state::<std::sync::Arc<SyncManager>>();
    let self_id = manager.device_id().to_owned();

    let since_raw = device.last_pull_at.map(|since| since.to_rfc3339());
    let mut url = reqwest::Url::parse(&format!("http://{address}{API_CLIPBOARD_PULL}"))
        .map_err(|err| AppError::Other(anyhow::anyhow!("invalid sync pull url: {err}")))?;
    if let Some(since) = &since_raw {
        // 用 URL 查询参数编码：避免 RFC3339 里的 `+` 在服务端被解码成空格。
        url.query_pairs_mut().append_pair("since", since);
    }

    let timestamp = now_unix();
    let signature_target = pull_signature_target(since_raw.as_deref());
    let signature = crypto::sign_request(&key, &self_id, timestamp, &signature_target);

    let client = client()?;
    let resp = client
        .get(url.clone())
        .header(HEADER_DEVICE_ID, &self_id)
        .header(HEADER_TIMESTAMP, timestamp.to_string())
        .header(HEADER_SIGNATURE, &signature)
        .send()
        .await
        .map_err(|err| AppError::Other(anyhow::anyhow!("get {url} failed: {err}")))?;

    if !resp.status().is_success() {
        return Err(AppError::Other(anyhow::anyhow!(
            "get {url} returned {}",
            resp.status()
        )));
    }

    let sealed = resp
        .bytes()
        .await
        .map_err(|err| AppError::Other(anyhow::anyhow!("read body from {url} failed: {err}")))?;
    let plain = crypto::open(&key, &sealed)?;
    let items: Vec<SyncClipboardItemDto> =
        serde_json::from_slice(&plain).map_err(|err| AppError::Other(err.into()))?;

    let mut max_created: Option<chrono::DateTime<chrono::Utc>> = None;
    for dto in items {
        let created = chrono::DateTime::parse_from_rfc3339(&dto.created_at)
            .map(|ts| ts.with_timezone(&chrono::Utc))
            .ok();
        if let Some(created) = created {
            let better = match max_created {
                Some(current) => created > current,
                None => true,
            };
            if better {
                max_created = Some(created);
            }
        }

        crate::sync::ingest_remote_item(app, &device.device_id, &dto).await;
    }

    // 只有成功拿到响应才推进游标（失败下次重试）。
    if let Some(pulled_until) = max_created {
        let pool = app.state::<crate::db::DatabaseState>().pool().await;
        crate::sync::devices::set_last_pull_at(&pool, &device.device_id, pulled_until).await?;
    }
    Ok(())
}

/// 认证探测：向目标地址发送带共享密钥签名的 `/api/verify`。
///
/// 只有持有该密钥的真实对端才能通过认证，用于确认 mDNS 上报的新地址
/// 不是冒充已配对设备的其它主机；同时用作已配对设备的在线探测。
/// `self_device_id` 必须是本机设备 id：服务端按请求头上的设备 id 查找密钥。
pub async fn verify_peer(self_device_id: &str, shared_key_hex: &str, address: &str) -> bool {
    let Ok(key) = crypto::key_from_hex(shared_key_hex) else {
        return false;
    };
    let Ok(client) = client() else {
        return false;
    };

    let timestamp = now_unix();
    let signature = crypto::sign_request(&key, self_device_id, timestamp, API_VERIFY);
    let url = format!("http://{address}{API_VERIFY}");
    let result = client
        .get(&url)
        .timeout(Duration::from_secs(3))
        .header(HEADER_DEVICE_ID, self_device_id)
        .header(HEADER_TIMESTAMP, timestamp.to_string())
        .header(HEADER_SIGNATURE, &signature)
        .send()
        .await;

    match result {
        Ok(resp) => resp.status().is_success(),
        Err(_) => false,
    }
}

/// 读取对端设备信息（明文端点，用于手动地址配对）；失败说明地址不可达或不是 EcoPaste。
pub async fn fetch_device_info(address: &str) -> Result<DeviceInfoDto> {
    let client = client()?;
    let url = format!("http://{address}{API_INFO}");
    let resp = client
        .get(&url)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .map_err(|err| AppError::Other(anyhow::anyhow!("get {url} failed: {err}")))?;

    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        // 对端关闭「允许被发现」后不再响应主动探测。
        return Err(AppError::Other(anyhow::anyhow!(
            "the device does not allow discovery"
        )));
    }

    if !resp.status().is_success() {
        return Err(AppError::Other(anyhow::anyhow!(
            "get {url} returned {}",
            resp.status()
        )));
    }

    resp.json::<DeviceInfoDto>()
        .await
        .map_err(|err| AppError::Other(anyhow::anyhow!("decode info from {url} failed: {err}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_manual_address_adds_default_port() {
        let address = normalize_manual_address(" 192.168.1.20 ", 47654).unwrap();

        assert_eq!(address, "192.168.1.20:47654");
    }

    #[test]
    fn normalize_manual_address_keeps_explicit_port_and_strips_scheme() {
        let address = normalize_manual_address("http://10.0.0.8:5000/", 47654).unwrap();

        assert_eq!(address, "10.0.0.8:5000");
    }

    #[test]
    fn normalize_manual_address_keeps_bracketed_ipv6() {
        let address = normalize_manual_address("[fe80::1]", 47654).unwrap();
        let with_port = normalize_manual_address("[fe80::1]:6000", 47654).unwrap();

        assert_eq!(address, "[fe80::1]:47654");
        assert_eq!(with_port, "[fe80::1]:6000");
    }

    #[test]
    fn normalize_manual_address_rejects_invalid_input() {
        assert!(normalize_manual_address("", 47654).is_err());
        assert!(normalize_manual_address("host:abc", 47654).is_err());
        assert!(normalize_manual_address("host:0", 47654).is_err());
        assert!(normalize_manual_address("fe80::1", 47654).is_err());
        assert!(normalize_manual_address("host/extra", 47654).is_err());
        assert!(normalize_manual_address("host?x=1", 47654).is_err());
    }
}
