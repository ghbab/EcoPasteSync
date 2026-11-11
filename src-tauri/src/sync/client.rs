//! 同步客户端（reqwest）：配对发起与轮询、条目推送、增量拉取、图片回源。
//!
//! 所有加密端点复用 [`crate::sync::server`] 的认证头与封包格式。

use std::time::Duration;

use reqwest::Client;
use tauri::{AppHandle, Manager};

use crate::core::{AppError, Result};
use crate::sync::crypto;
use crate::sync::devices::SyncDeviceRecord;
use crate::sync::protocol::{
    DeviceInfoDto, PairRequestDto, PairStatusDto, SyncClipboardItemDto, API_CLIPBOARD_PULL,
    API_CLIPBOARD_PUSH, API_IMAGE, API_PAIR, API_PAIR_STATUS, HEADER_DEVICE_ID, HEADER_SIGNATURE,
    HEADER_TIMESTAMP,
};
use crate::sync::{emit_pair_result, SyncManager};

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
        .build()
        .map_err(|err| AppError::Other(anyhow::anyhow!("build sync http client failed: {err}")))
}

fn now_unix() -> i64 {
    chrono::Utc::now().timestamp()
}

/// 发起配对（A 端）：发送明文请求，成功受理后轮询结果。
/// 结果通过 `sync://pair-result` 事件通知前端；接受时落库。
pub fn start_pairing(app: AppHandle, manager: std::sync::Arc<SyncManager>, device_id: String) {
    tauri::async_runtime::spawn(async move {
        let info = manager.device_info(&app);
        let Some(address) = manager.device_address(&device_id, None) else {
            emit_pair_result(&app, &device_id, false, Some("device-offline".to_owned()));
            return;
        };

        let seed = crypto::generate_pairing_seed();
        let request = PairRequestDto {
            device_id: info.device_id,
            name: info.name,
            platform: info.platform,
            seed: seed.clone(),
        };

        let client = match client() {
            Ok(client) => client,
            Err(err) => {
                log::warn!("sync pair: build client failed: {err}");
                emit_pair_result(&app, &device_id, false, Some("internal-error".to_owned()));
                return;
            }
        };

        let url = format!("http://{address}{API_PAIR}");
        let sent = client
            .post(&url)
            .json(&request)
            .timeout(Duration::from_secs(5))
            .send()
            .await;

        match sent {
            Ok(resp) if resp.status().as_u16() == 202 => {
                // 受理成功，进入轮询。
                poll_pair_result(app, manager, client, &address, &device_id, &seed).await;
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

/// 轮询对端的配对决定；`Accepted` 时保存配对记录并补一次增量拉取。
async fn poll_pair_result(
    app: AppHandle,
    manager: std::sync::Arc<SyncManager>,
    client: Client,
    address: &str,
    device_id: &str,
    seed: &str,
) {
    let url = format!(
        "http://{address}{API_PAIR_STATUS}?deviceId={}",
        manager.device_id()
    );
    let started = std::time::Instant::now();

    loop {
        if started.elapsed() >= PAIR_POLL_TIMEOUT {
            emit_pair_result(&app, device_id, false, Some("timeout".to_owned()));
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
                if let Err(err) = manager.save_paired_device(&app, device_id, seed).await {
                    log::warn!("sync pair: save paired device failed: {err}");
                    emit_pair_result(&app, device_id, false, Some("internal-error".to_owned()));
                    return;
                }
                // 新配对设备立即补一次近段历史。
                crate::sync::pull_from_device(&app, device_id).await;
                emit_pair_result(&app, device_id, true, None);
                return;
            }
            PairStatusDto::Rejected => {
                emit_pair_result(&app, device_id, false, Some("pair-rejected".to_owned()));
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

/// 增量拉取：按设备 `last_pull_at` 游标拉近段条目并逐条入库，随后推进游标。
pub async fn pull_items(app: &AppHandle, device: &SyncDeviceRecord, address: &str) -> Result<()> {
    let key = crypto::key_from_hex(&device.shared_key)?;
    let manager = app.state::<std::sync::Arc<SyncManager>>();
    let self_id = manager.device_id().to_owned();

    let mut url = format!("http://{address}{API_CLIPBOARD_PULL}");
    if let Some(since) = device.last_pull_at {
        url.push_str(&format!("?since={}", since.to_rfc3339()));
    }

    let timestamp = now_unix();
    let signature = crypto::sign_request(&key, &self_id, timestamp, API_CLIPBOARD_PULL);

    let client = client()?;
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

/// 探测对端设备信息（明文，用于诊断）。
#[allow(dead_code)]
pub async fn fetch_device_info(address: &str) -> Result<DeviceInfoDto> {
    let client = client()?;
    let url = format!("http://{address}/api/info");
    let resp = client
        .get(&url)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .map_err(|err| AppError::Other(anyhow::anyhow!("get {url} failed: {err}")))?;

    resp.json::<DeviceInfoDto>()
        .await
        .map_err(|err| AppError::Other(anyhow::anyhow!("decode info from {url} failed: {err}")))
}
