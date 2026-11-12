//! 同步 HTTP 服务器（axum）：配对端点 + 加密数据端点。
//!
//! 明文端点：`/api/info`、`/api/pair`、`/api/pair/status`（配对前无共享密钥）。
//! 加密端点：`/api/clipboard/push`、`/api/clipboard/pull`、`/api/image/{file}`，
//! 请求 / 响应体均为 `nonce(12) || ChaCha20-Poly1305 密文`，
//! 并带 `X-Device-Id` / `X-Timestamp` / `X-Signature` 认证头。

use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{ConnectInfo, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::json;
use tauri::{AppHandle, Manager};
use tokio::sync::watch;

use crate::core::Result;
use crate::sync::crypto;
use crate::sync::devices::{self, SyncDeviceRecord};
use crate::sync::protocol::{
    file_chunk_signature_target, pull_signature_target, PairRequestDto, SyncClipboardItemDto,
    API_CLIPBOARD_PULL, API_CLIPBOARD_PUSH, API_FILE_CHUNK, API_IMAGE, API_INFO, API_PAIR,
    API_PAIR_STATUS, API_VERIFY, FILE_CHUNK_SIZE, HEADER_DEVICE_ID, HEADER_SIGNATURE,
    HEADER_TIMESTAMP, PAIR_DEVICE_ID_MAX_CHARS, PULL_PAGE_LIMIT,
};
use crate::sync::{collect_shared_paths, item_to_dto, SyncManager};

/// 服务器共享上下文（路由 State）。
pub struct ServerCtx {
    pub app: AppHandle,
    pub manager: Arc<SyncManager>,
}

/// 启动服务器：从首选端口开始探测绑定（被占用则 +1，最多 16 次）。
/// 端口探测用 std 同步 bind（`apply` 可能来自异步命令上下文，避免嵌套 block_on），
/// 随后转为 tokio listener 并 spawn 服务任务。返回实际监听端口与关闭信号。
pub fn spawn_server(
    ctx: Arc<ServerCtx>,
    preferred_port: u16,
) -> Result<(u16, watch::Sender<bool>)> {
    let mut port = preferred_port;
    let listener = loop {
        match std::net::TcpListener::bind(("0.0.0.0", port)) {
            Ok(listener) => break listener,
            Err(err) => {
                if port >= preferred_port.saturating_add(PORT_PROBE_RANGE) {
                    return Err(crate::core::AppError::Other(anyhow::anyhow!(
                        "bind sync server port {preferred_port}..{} failed: {err}",
                        preferred_port.saturating_add(PORT_PROBE_RANGE)
                    )));
                }
                port += 1;
            }
        }
    };

    let actual_port = listener
        .local_addr()
        .map(|addr| addr.port())
        .unwrap_or(port);
    listener.set_nonblocking(true).map_err(|err| {
        crate::core::AppError::Other(anyhow::anyhow!("set nonblocking failed: {err}"))
    })?;
    let listener = tokio::net::TcpListener::from_std(listener).map_err(|err| {
        crate::core::AppError::Other(anyhow::anyhow!("convert listener failed: {err}"))
    })?;

    let router = build_router(ctx);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    tauri::async_runtime::spawn(async move {
        // 携带连接源地址：配对端点用它配合请求上报端口推导发起方回连地址。
        let server = axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(async move {
            let mut shutdown_rx = shutdown_rx;
            while shutdown_rx.changed().await.is_ok() {
                if *shutdown_rx.borrow() {
                    break;
                }
            }
        });

        if let Err(err) = server.await {
            log::error!("sync server stopped with error: {err}");
        }
    });

    Ok((actual_port, shutdown_tx))
}

fn build_router(ctx: Arc<ServerCtx>) -> Router {
    Router::new()
        .route(API_INFO, get(handle_info))
        .route(API_PAIR, post(handle_pair))
        .route(API_PAIR_STATUS, get(handle_pair_status))
        .route(API_VERIFY, get(handle_verify))
        .route(API_CLIPBOARD_PUSH, post(handle_push))
        .route(API_CLIPBOARD_PULL, get(handle_pull))
        .route(&format!("{API_IMAGE}/{{file}}"), get(handle_image))
        .route(API_FILE_CHUNK, get(handle_file_chunk))
        .with_state(ctx)
}

/// 端口被占用时的探测上限（依次 +1）。
const PORT_PROBE_RANGE: u16 = 16;

/// 常量时间比较两个十六进制签名，避免逐字节短路泄露信息。
fn constant_time_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }

    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// 认证并取回调用方设备记录与共享密钥。
///
/// `signature_target` 是参与签名的业务串：多数端点直接用请求路径，
/// 文件分块端点用 [`file_chunk_signature_target`] 生成的固定格式串。
async fn verify_auth(
    ctx: &ServerCtx,
    headers: &HeaderMap,
    signature_target: &str,
) -> std::result::Result<(SyncDeviceRecord, [u8; 32]), StatusCode> {
    let device_id = headers
        .get(HEADER_DEVICE_ID)
        .and_then(|v| v.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?
        .to_owned();
    let timestamp: i64 = headers
        .get(HEADER_TIMESTAMP)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let signature = headers
        .get(HEADER_SIGNATURE)
        .and_then(|v| v.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?
        .to_owned();

    let pool = devices::pool(&ctx.app).await;
    let Some(device) = devices::find_device(&pool, &device_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(StatusCode::UNAUTHORIZED);
    };

    let key = crypto::key_from_hex(&device.shared_key).map_err(|_| StatusCode::UNAUTHORIZED)?;
    let now = chrono::Utc::now().timestamp();
    if !crypto::timestamp_within_tolerance(timestamp, now) {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let expected = crypto::sign_request(&key, &device_id, timestamp, signature_target);
    if !constant_time_eq(&expected, &signature) {
        return Err(StatusCode::UNAUTHORIZED);
    }

    Ok((device, key))
}

/// 加密响应（application/octet-stream）。
fn sealed_response(key: &[u8; 32], payload: &impl serde::Serialize) -> Response {
    let sealed = serde_json::to_vec(payload)
        .map_err(|err| crate::core::AppError::Other(err.into()))
        .and_then(|plain| crypto::seal(key, &plain));

    match sealed {
        Ok(body) => (
            StatusCode::OK,
            [("content-type", "application/octet-stream")],
            body,
        )
            .into_response(),
        Err(err) => {
            log::warn!("sync server: seal response failed: {err}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// 加密原始字节响应（文件块）。
fn sealed_bytes_response(key: &[u8; 32], plain: &[u8]) -> Response {
    match crypto::seal(key, plain) {
        Ok(body) => (
            StatusCode::OK,
            [("content-type", "application/octet-stream")],
            body,
        )
            .into_response(),
        Err(err) => {
            log::warn!("sync server: seal bytes failed: {err}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

// ---- 明文端点 ----

/// 读取「允许被发现」设置：false 时不响应主动探测、不接受新的配对请求。
fn sync_discoverable(app: &AppHandle) -> bool {
    app.state::<crate::settings::SettingsStore>()
        .snapshot()
        .sync
        .discoverable
}

/// 设备信息：手动配对前探测用。
///
/// 关闭「允许被发现」后按不存在处理，避免未开放配对的设备仍能被主动探测到。
async fn handle_info(State(ctx): State<Arc<ServerCtx>>) -> Response {
    if !sync_discoverable(&ctx.app) {
        return StatusCode::NOT_FOUND.into_response();
    }

    Json(ctx.manager.device_info(&ctx.app)).into_response()
}

async fn handle_pair(
    State(ctx): State<Arc<ServerCtx>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Json(mut request): Json<PairRequestDto>,
) -> Response {
    // 「允许被发现」关闭时拒绝一切新的配对请求；已配对设备使用加密端点，不受影响。
    if !sync_discoverable(&ctx.app) {
        return StatusCode::FORBIDDEN.into_response();
    }

    // 限速必须早于任何数据库 / 钥匙串访问：未认证请求不能放大资源消耗。
    if !ctx.manager.allow_pair_attempt(Some(peer.ip())) {
        return StatusCode::TOO_MANY_REQUESTS.into_response();
    }

    // 先归一化设备 id：黑名单 / 已配对查询与后续 pending 必须使用同一个值，
    // 否则带前后空白的 id 可以绕过黑名单。
    request.device_id = request.device_id.trim().to_owned();

    // 空字段与长度防御：device_id / public_key 是后续派生密钥的关键输入。
    if request.device_id.is_empty()
        || request.device_id.chars().count() > PAIR_DEVICE_ID_MAX_CHARS
        || request.public_key.len() != 64
    {
        return StatusCode::BAD_REQUEST.into_response();
    }

    let pool = devices::pool(&ctx.app).await;
    match devices::is_blacklisted(&pool, &request.device_id).await {
        Ok(true) => return StatusCode::FORBIDDEN.into_response(),
        Ok(false) => {}
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }

    // 已配对设备不允许再次配对：否则任何能访问本端口的一方都能覆盖现有共享密钥。
    match devices::find_device(&pool, &request.device_id).await {
        Ok(Some(_)) => return StatusCode::CONFLICT.into_response(),
        Ok(None) => {}
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }

    match ctx
        .manager
        .handle_pair_request(&ctx.app, request, Some(peer.ip()))
    {
        Some(accepted) => (StatusCode::ACCEPTED, Json(accepted)).into_response(),
        None => StatusCode::CONFLICT.into_response(),
    }
}

async fn handle_pair_status(
    State(ctx): State<Arc<ServerCtx>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Response {
    let Some(device_id) = params.get("deviceId") else {
        return StatusCode::BAD_REQUEST.into_response();
    };

    Json(ctx.manager.pair_status_for(device_id, Some(peer.ip()))).into_response()
}

// ---- 加密端点 ----

/// 认证探测：只验证调用方持有共享密钥。
///
/// 发现层在已配对设备的 mDNS 地址变化时先调用它，确认新地址不是冒充设备；
/// 也用于已配对设备的在线探测。
async fn handle_verify(State(ctx): State<Arc<ServerCtx>>, headers: HeaderMap) -> Response {
    let (_, key) = match verify_auth(&ctx, &headers, API_VERIFY).await {
        Ok(pair) => pair,
        Err(status) => return status.into_response(),
    };

    sealed_response(&key, &json!({ "ok": true }))
}

async fn handle_push(
    State(ctx): State<Arc<ServerCtx>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let (device, key) = match verify_auth(&ctx, &headers, API_CLIPBOARD_PUSH).await {
        Ok(pair) => pair,
        Err(status) => return status.into_response(),
    };
    if !device.allow_receive {
        return StatusCode::FORBIDDEN.into_response();
    }

    let plain = match crypto::open(&key, &body) {
        Ok(plain) => plain,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    let dto: SyncClipboardItemDto = match serde_json::from_slice(&plain) {
        Ok(dto) => dto,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };

    crate::sync::ingest_remote_item(&ctx.app, &device.device_id, &dto).await;

    sealed_response(&key, &json!({ "ok": true }))
}

async fn handle_pull(
    State(ctx): State<Arc<ServerCtx>>,
    headers: HeaderMap,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Response {
    // 先取原始 since 字符串参与签名校验：认证头与游标绑定，不能改游标复用签名。
    let since_raw = params.get("since").cloned();
    let signature_target = pull_signature_target(since_raw.as_deref());
    let (device, key) = match verify_auth(&ctx, &headers, &signature_target).await {
        Ok(pair) => pair,
        Err(status) => return status.into_response(),
    };

    // 「发送」权限关闭后，该设备不能再从本机拉取任何条目。
    if !device.allow_send {
        return StatusCode::FORBIDDEN.into_response();
    }

    let since = since_raw.as_deref().and_then(|raw| {
        chrono::DateTime::parse_from_rfc3339(raw)
            .map(|ts| ts.with_timezone(&chrono::Utc))
            .ok()
    });

    let pool = ctx.app.state::<crate::db::DatabaseState>().pool().await;
    let items =
        match crate::db::items::list_recent_items_for_sync(&pool, since, PULL_PAGE_LIMIT).await {
            Ok(items) => items,
            Err(err) => {
                log::warn!("sync server: list items for pull failed: {err}");
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
        };

    let settings = ctx.app.state::<crate::settings::SettingsStore>().snapshot();
    let inline_threshold_bytes = settings.sync.file_threshold.saturating_mul(1024 * 1024);
    let max_bytes = settings.sync.file_max_size.saturating_mul(1024 * 1024);
    let dtos: Vec<SyncClipboardItemDto> = items
        .iter()
        .map(|item| item_to_dto(item, inline_threshold_bytes, max_bytes))
        .collect();

    // 响应 pull 也等于对外共享：登记本机产生的文件路径，允许对端随后按需拉取。
    // 来源设备记录在独立同步库，先批量取回；带来源记录的条目是远端条目，
    // 不能登记路径，否则已配对设备可以先推送伪造路径、再借拉取接口读取本机任意文件。
    let sync_pool = devices::pool(&ctx.app).await;
    let file_item_ids: Vec<String> = items
        .iter()
        .filter(|item| item.kind == crate::db::models::ClipboardKind::Files)
        .map(|item| item.id.clone())
        .collect();
    let remote_sources = crate::sync::item_meta::sources_for(&sync_pool, &file_item_ids)
        .await
        .unwrap_or_default();

    let mut shared_paths = Vec::new();
    for item in &items {
        if item.kind == crate::db::models::ClipboardKind::Files
            && !remote_sources.contains_key(&item.id)
        {
            shared_paths.extend(collect_shared_paths(&item.content));
        }
    }
    ctx.manager.remember_shared_paths(&shared_paths);

    sealed_response(&key, &dtos)
}

async fn handle_image(
    State(ctx): State<Arc<ServerCtx>>,
    headers: HeaderMap,
    Path(file): Path<String>,
) -> Response {
    // 严格校验文件名格式（blake3 hex + .png），防路径穿越。
    let valid = file.len() == 68
        && file.ends_with(".png")
        && file[..64].chars().all(|c| c.is_ascii_hexdigit());
    if !valid {
        return StatusCode::BAD_REQUEST.into_response();
    }

    let path = format!("{API_IMAGE}/{file}");
    let (device, key) = match verify_auth(&ctx, &headers, &path).await {
        Ok(pair) => pair,
        Err(status) => return status.into_response(),
    };

    // 「发送」权限关闭后，该设备不能再从本机拉取图片原图。
    if !device.allow_send {
        return StatusCode::FORBIDDEN.into_response();
    }

    let store = ctx
        .app
        .state::<crate::clipboard::ImageStore>()
        .inner()
        .clone();
    // 文件名已通过格式校验（64 位 hex + .png），origin_path 直接拼接分片目录。
    let origin = store.origin_path(&file);
    match tokio::fs::read(origin).await {
        Ok(bytes) => {
            let sealed = match crypto::seal(&key, &bytes) {
                Ok(sealed) => sealed,
                Err(err) => {
                    log::warn!("sync server: seal image failed: {err}");
                    return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                }
            };
            (
                StatusCode::OK,
                [("content-type", "application/octet-stream")],
                sealed,
            )
                .into_response()
        }
        Err(err) => {
            log::warn!("sync server: read image {file} failed: {err}");
            StatusCode::NOT_FOUND.into_response()
        }
    }
}

/// 文件分块端点：`GET /api/file?path=..&offset=..&len=..`。
///
/// 只允许读取本机主动共享过的路径（见 [`SyncManager::remember_shared_paths`]），
/// 避免已配对设备借拉取接口读取任意文件。
async fn handle_file_chunk(
    State(ctx): State<Arc<ServerCtx>>,
    headers: HeaderMap,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Response {
    let Some(path) = params
        .get("path")
        .filter(|value| !value.is_empty())
        .cloned()
    else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Some(offset) = params.get("offset").and_then(|raw| raw.parse::<u64>().ok()) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Some(len) = params.get("len").and_then(|raw| raw.parse::<u64>().ok()) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    if len == 0 || len > FILE_CHUNK_SIZE as u64 {
        return StatusCode::BAD_REQUEST.into_response();
    }

    let target = file_chunk_signature_target(&path, offset, len);
    let (device, key) = match verify_auth(&ctx, &headers, &target).await {
        Ok(pair) => pair,
        Err(status) => return status.into_response(),
    };

    // 「发送」权限关闭后，该设备不能再从本机拉取文件块。
    if !device.allow_send {
        return StatusCode::FORBIDDEN.into_response();
    }

    // 只允许拉取本机主动共享过的路径：同步入库的远端路径不在授权表里，
    // 已配对设备无法借这个接口读取本机任意文件。
    if !ctx.manager.is_shared_path(&path) {
        return StatusCode::FORBIDDEN.into_response();
    }

    // 只服务常规文件：目录、特殊设备或已删除路径一律按不存在处理。
    match tokio::fs::metadata(&path).await {
        Ok(meta) if meta.is_file() => {}
        _ => return StatusCode::NOT_FOUND.into_response(),
    }

    match read_file_range(&path, offset, len).await {
        Ok(bytes) => sealed_bytes_response(&key, &bytes),
        Err(err) => {
            log::warn!("sync server: read file chunk {path} at {offset} failed: {err}");
            StatusCode::NOT_FOUND.into_response()
        }
    }
}

/// 读取文件指定区间；文件尾部或并发截断时返回实际读到的字节。
async fn read_file_range(path: &str, offset: u64, len: u64) -> std::io::Result<Vec<u8>> {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    let mut file = tokio::fs::File::open(path).await?;
    file.seek(std::io::SeekFrom::Start(offset)).await?;

    let mut buffer = Vec::with_capacity(len as usize);
    file.take(len).read_to_end(&mut buffer).await?;
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constant_time_eq_basic() {
        assert!(constant_time_eq("abcdef", "abcdef"));
        assert!(!constant_time_eq("abcdef", "abcdeg"));
        assert!(!constant_time_eq("abc", "abcd"));
    }

    #[test]
    fn image_file_name_validation() {
        let valid = format!("{}.png", "ab".repeat(32));
        assert_eq!(valid.len(), 68);
        assert!(valid.ends_with(".png"));
        assert!(valid[..64].chars().all(|c| c.is_ascii_hexdigit()));

        assert!(!format!("{}.png", "zz".repeat(32))[..64]
            .chars()
            .all(|c| c.is_ascii_hexdigit()));
    }
}
