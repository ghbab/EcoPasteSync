//! 同步 HTTP 服务器（axum）：配对端点 + 加密数据端点。
//!
//! 明文端点：`/api/info`、`/api/pair`、`/api/pair/status`（配对前无共享密钥）。
//! 加密端点：`/api/clipboard/push`、`/api/clipboard/pull`、`/api/image/{file}`，
//! 请求 / 响应体均为 `nonce(12) || ChaCha20-Poly1305 密文`，
//! 并带 `X-Device-Id` / `X-Timestamp` / `X-Signature` 认证头。

use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
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
    PairRequestDto, PairStatusDto, SyncClipboardItemDto, API_CLIPBOARD_PULL, API_CLIPBOARD_PUSH,
    API_IMAGE, API_INFO, API_PAIR, API_PAIR_STATUS, HEADER_DEVICE_ID, HEADER_SIGNATURE,
    HEADER_TIMESTAMP, PULL_PAGE_LIMIT,
};
use crate::sync::{item_to_dto, SyncManager};

/// 服务器共享上下文（路由 State）。
pub struct ServerCtx {
    pub app: AppHandle,
    pub manager: Arc<SyncManager>,
}

/// 启动服务器：从首选端口开始探测绑定（被占用则 +1，最多 16 次）。
/// 端口探测用 std 同步 bind（`apply` 可能来自异步命令上下文，避免嵌套 block_on），
/// 随后转为 tokio listener 并 spawn 服务任务。返回实际监听端口与关闭信号。
pub fn spawn_server(ctx: Arc<ServerCtx>, preferred_port: u16) -> Result<(u16, watch::Sender<bool>)> {
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
        .map(SocketAddr::port)
        .unwrap_or(port);
    listener
        .set_nonblocking(true)
        .map_err(|err| crate::core::AppError::Other(anyhow::anyhow!("set nonblocking failed: {err}")))?;
    let listener = tokio::net::TcpListener::from_std(listener)
        .map_err(|err| crate::core::AppError::Other(anyhow::anyhow!("convert listener failed: {err}")))?;

    let router = build_router(ctx);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    tauri::async_runtime::spawn(async move {
        let server = axum::serve(listener, router).with_graceful_shutdown(async move {
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
        .route(API_CLIPBOARD_PUSH, post(handle_push))
        .route(API_CLIPBOARD_PULL, get(handle_pull))
        .route(
            &format!("{API_IMAGE}/{{file}}"),
            get(handle_image),
        )
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
async fn verify_auth(
    ctx: &ServerCtx,
    headers: &HeaderMap,
    path: &str,
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

    let pool = ctx.app.state::<crate::db::DatabaseState>().pool().await;
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

    let expected = crypto::sign_request(&key, &device_id, timestamp, path);
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

// ---- 明文端点 ----

async fn handle_info(State(ctx): State<Arc<ServerCtx>>) -> Json<crate::sync::DeviceInfoDto> {
    Json(ctx.manager.device_info(&ctx.app))
}

async fn handle_pair(
    State(ctx): State<Arc<ServerCtx>>,
    Json(request): Json<PairRequestDto>,
) -> Response {
    // 空字段防御：device_id / seed 是后续派生密钥的关键输入。
    if request.device_id.trim().is_empty() || request.seed.len() != 64 {
        return StatusCode::BAD_REQUEST.into_response();
    }

    let pool = ctx.app.state::<crate::db::DatabaseState>().pool().await;
    match devices::is_blacklisted(&pool, &request.device_id).await {
        Ok(true) => return StatusCode::FORBIDDEN.into_response(),
        Ok(false) => {}
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }

    if ctx.manager.handle_pair_request(&ctx.app, request) {
        (
            StatusCode::ACCEPTED,
            Json(PairStatusDto::Pending),
        )
            .into_response()
    } else {
        StatusCode::CONFLICT.into_response()
    }
}

async fn handle_pair_status(
    State(ctx): State<Arc<ServerCtx>>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Response {
    let Some(device_id) = params.get("deviceId") else {
        return StatusCode::BAD_REQUEST.into_response();
    };

    Json(ctx.manager.pair_status_for(device_id)).into_response()
}

// ---- 加密端点 ----

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
    let (device, key) = match verify_auth(&ctx, &headers, API_CLIPBOARD_PULL).await {
        Ok(pair) => pair,
        Err(status) => return status.into_response(),
    };

    let since = params.get("since").and_then(|raw| {
        chrono::DateTime::parse_from_rfc3339(raw)
            .map(|ts| ts.with_timezone(&chrono::Utc))
            .ok()
    });

    let pool = ctx.app.state::<crate::db::DatabaseState>().pool().await;
    let items = match crate::db::items::list_recent_items_for_sync(
        &pool,
        since,
        PULL_PAGE_LIMIT,
    )
    .await
    {
        Ok(items) => items,
        Err(err) => {
            log::warn!("sync server: list items for pull failed: {err}");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let dtos: Vec<SyncClipboardItemDto> = items.iter().map(item_to_dto).collect();
    let _ = device;
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
        && file[..64]
            .chars()
            .all(|c| c.is_ascii_hexdigit());
    if !valid {
        return StatusCode::BAD_REQUEST.into_response();
    }

    let path = format!("{API_IMAGE}/{file}");
    let (device, key) = match verify_auth(&ctx, &headers, &path).await {
        Ok(pair) => pair,
        Err(status) => return status.into_response(),
    };
    let _ = device;

    let store = ctx.app.state::<crate::clipboard::ImageStore>().inner().clone();
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
