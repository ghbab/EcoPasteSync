//! 文件条目的分块拉取与缓存落盘。
//!
//! 两种触发时机共用本模块：
//! - 条目内文件总大小不超过直接传输阈值 → 接收端在入库前立即拉取；
//! - 超过阈值 → 只同步路径，粘贴时再按需拉取。
//!
//! 缓存目录按「远端路径列表」的 blake3 命名，重复接收同一批路径时复用同一目录。
//! 传输不做断点续传：中途失败或用户取消一律丢弃半成品，下次从头再拉。

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{LazyLock, Mutex};

use serde_json::json;
use tauri::{AppHandle, Emitter, Manager};
use tokio::io::AsyncWriteExt;

use crate::core::{paths, AppError, Result};
use crate::sync::client;
use crate::sync::devices;
use crate::sync::file_state::{self, SyncFileState};
use crate::sync::protocol::{SyncFileEntryDto, EVENT_FILE_PROGRESS, FILE_CHUNK_SIZE};
use crate::sync::SyncManager;

/// 同步文件缓存的子目录名（位于 `<app_data_dir>/cache` 下）。
const CACHE_SUBDIR: &str = "sync-files";
/// 缓存文件名保留的最大字符数，避免中文等多字节字符超出文件系统 255 字节限制。
const MAX_FILE_NAME_CHARS: usize = 120;

/// 正在拉取的条目 id：同一卡片连点会并发清空同一缓存目录，必须串行化。
static TRANSFERRING: LazyLock<Mutex<HashSet<String>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

/// 单次条目传输的进度上下文；`item_id` 缺失时不上报进度。
struct TransferTarget<'a> {
    app: &'a AppHandle,
    item_id: &'a str,
    /// 当前文件之前的累计字节数。
    transferred_base: u64,
    total_bytes: u64,
}

impl TransferTarget<'_> {
    /// 上报当前文件已写入 `local_offset` 字节后的整体进度。
    fn report(&self, local_offset: u64) {
        emit_transfer_progress(
            self.app,
            Some(self.item_id),
            self.transferred_base + local_offset,
            self.total_bytes,
            false,
        );
    }
}

/// 把远端条目涉及的文件全部拉到本地缓存目录，返回本地路径列表。
///
/// 调用方负责保证 `files` 中不含目录、每项都带 `size`；这里对不可传输的输入
/// 直接报错，避免产生「有路径没有内容」的误导性缓存。
pub async fn pull_files(
    app: &AppHandle,
    remote_content: &str,
    source_device_id: &str,
    files: &[SyncFileEntryDto],
    item_id: Option<&str>,
) -> Result<Vec<String>> {
    let pool = devices::pool(app).await;
    let device = match devices::find_device(&pool, source_device_id).await {
        Ok(Some(device)) => device,
        Ok(None) => {
            return Err(AppError::Other(anyhow::anyhow!(
                "source device is not paired"
            )));
        }
        Err(err) => return Err(err),
    };
    if !device.allow_receive {
        return Err(AppError::Other(anyhow::anyhow!(
            "source device is not allowed to send"
        )));
    }

    let manager = app.state::<std::sync::Arc<SyncManager>>();
    let Some(address) = manager.device_address(source_device_id, device.last_address.as_deref())
    else {
        return Err(AppError::Other(anyhow::anyhow!(
            "source device address is unknown"
        )));
    };

    // 缓存目录以远端路径列表的哈希命名：同一条目重复接收时落在同一目录，
    // 且哈希值天然是合法目录名，不依赖远端内容。
    let cache_key = blake3::hash(remote_content.as_bytes()).to_hex().to_string();
    let root = paths::cache_dir(app)?.join(CACHE_SUBDIR).join(cache_key);

    // 用户选择「取消即丢弃」，因此每次拉取都从空目录开始。
    if let Err(err) = tokio::fs::remove_dir_all(&root).await {
        if err.kind() != std::io::ErrorKind::NotFound {
            return Err(AppError::Other(anyhow::anyhow!(
                "clear sync file cache {root:?} failed: {err}"
            )));
        }
    }
    tokio::fs::create_dir_all(&root)
        .await
        .map_err(|err| AppError::Other(anyhow::anyhow!("create {root:?} failed: {err}")))?;

    let mut names: HashMap<String, usize> = HashMap::new();
    let mut local_paths = Vec::with_capacity(files.len());
    let total_bytes = files
        .iter()
        .filter_map(|entry| entry.size)
        .fold(0u64, |acc, size| {
            if size > 0 {
                acc.saturating_add(size as u64)
            } else {
                acc
            }
        });
    let mut transferred_base: u64 = 0;

    for (index, entry) in files.iter().enumerate() {
        if entry.is_dir {
            discard(&root).await;
            return Err(AppError::Other(anyhow::anyhow!(
                "directory is not transferable: {}",
                entry.path
            )));
        }
        let Some(size) = entry.size.filter(|size| *size >= 0) else {
            discard(&root).await;
            return Err(AppError::Other(anyhow::anyhow!(
                "file size is unknown: {}",
                entry.path
            )));
        };

        let name = unique_file_name(&entry.path, index, &mut names);
        let local_path = root.join(&name);
        let target = item_id.map(|item_id| TransferTarget {
            app,
            item_id,
            transferred_base,
            total_bytes,
        });
        let pulled = pull_one_file(
            app,
            &device.shared_key,
            &address,
            entry,
            size as u64,
            &local_path,
            target.as_ref(),
        )
        .await;

        if let Err(err) = pulled {
            // 任一文件失败即整体放弃：保留半套文件会让粘贴内容不完整。
            discard(&root).await;
            return Err(err);
        }

        transferred_base = transferred_base.saturating_add(size as u64);
        local_paths.push(local_path.to_string_lossy().into_owned());
    }

    Ok(local_paths)
}

/// 把远端文件条目的文件拉到本地缓存，返回可写入剪贴板的本地路径。
///
/// 本机条目（没有同步状态记录）返回 `None`；远端条目返回 `Some(paths)`。
/// 缓存仍然有效时直接复用，不重复传输。
pub async fn ensure_item_files(
    app: &AppHandle,
    item_id: &str,
    remote_content: &str,
) -> Result<Option<Vec<String>>> {
    let pool = devices::pool(app).await;
    let Some(record) = file_state::find(&pool, item_id).await? else {
        return Ok(None);
    };

    if record.state == SyncFileState::Unavailable {
        return Err(command_error(
            app,
            crate::i18n::commands::Key::SyncFileUnavailable,
        ));
    }

    if let Some(paths) = read_local_paths(record.local_paths_json.as_deref()) {
        return Ok(Some(paths));
    }

    let Some(files_json) = record.files_json.as_deref() else {
        return Err(command_error(
            app,
            crate::i18n::commands::Key::SyncFileUnavailable,
        ));
    };
    let files: Vec<SyncFileEntryDto> = serde_json::from_str(files_json).map_err(|err| {
        AppError::Other(anyhow::anyhow!(
            "decode synced file fingerprints failed: {err}"
        ))
    })?;
    if files.is_empty() {
        return Err(command_error(
            app,
            crate::i18n::commands::Key::SyncFileUnavailable,
        ));
    }

    // 同一 item 并发拉取会互相清空缓存目录，直接拒绝后到的请求。
    let started = TRANSFERRING
        .lock()
        .expect("sync transferring poisoned")
        .insert(item_id.to_owned());
    if !started {
        return Err(command_error(
            app,
            crate::i18n::commands::Key::SyncFileTransferBusy,
        ));
    }

    let total_bytes = files
        .iter()
        .filter_map(|entry| entry.size)
        .fold(0u64, |acc, size| {
            if size > 0 {
                acc.saturating_add(size as u64)
            } else {
                acc
            }
        });
    emit_transfer_progress(app, Some(item_id), 0, total_bytes, false);

    let result = pull_files(
        app,
        remote_content,
        &record.device_id,
        &files,
        Some(item_id),
    )
    .await;

    TRANSFERRING
        .lock()
        .expect("sync transferring poisoned")
        .remove(item_id);

    match result {
        Ok(paths) => {
            if let Ok(json) = serde_json::to_string(&paths) {
                if let Err(err) = file_state::set_local_paths(&pool, item_id, &json).await {
                    log::warn!("sync file transfer: save local paths failed: {err}");
                }
            }
            emit_transfer_progress(app, Some(item_id), total_bytes, total_bytes, true);
            Ok(Some(paths))
        }
        Err(err) => {
            log::warn!("sync file transfer for item {item_id} failed: {err}");
            emit_transfer_progress(app, Some(item_id), 0, total_bytes, true);
            Err(command_error(
                app,
                crate::i18n::commands::Key::SyncFileTransferFailed,
            ))
        }
    }
}

/// 读取并校验缓存的本地路径：全部文件仍存在才算命中。
fn read_local_paths(raw: Option<&str>) -> Option<Vec<String>> {
    let paths: Vec<String> = serde_json::from_str(raw?).ok()?;
    if paths.is_empty() {
        return None;
    }
    if !paths.iter().all(|path| Path::new(path).is_file()) {
        return None;
    }

    Some(paths)
}

/// 上报文件传输进度；`item_id` 缺失时不发事件。
fn emit_transfer_progress(
    app: &AppHandle,
    item_id: Option<&str>,
    transferred_bytes: u64,
    total_bytes: u64,
    done: bool,
) {
    let Some(item_id) = item_id else {
        return;
    };

    let payload = json!({
        "itemId": item_id,
        "transferredBytes": transferred_bytes,
        "totalBytes": total_bytes,
        "done": done,
    });
    if let Err(err) = app.emit(EVENT_FILE_PROGRESS, payload) {
        log::warn!("sync file transfer: emit progress failed: {err}");
    }
}

/// 构造命令层可展示的错误文案（跟随界面语言）。
fn command_error(app: &AppHandle, key: crate::i18n::commands::Key) -> AppError {
    let lang = crate::i18n::current_language(app);
    AppError::Other(anyhow::anyhow!(crate::i18n::commands::label(lang, key)))
}

/// 拉取单个文件的全部块并校验完整性。
async fn pull_one_file(
    app: &AppHandle,
    shared_key: &str,
    address: &str,
    entry: &SyncFileEntryDto,
    size: u64,
    local_path: &Path,
    target: Option<&TransferTarget<'_>>,
) -> Result<()> {
    let mut file = tokio::fs::File::create(local_path)
        .await
        .map_err(|err| AppError::Other(anyhow::anyhow!("create {local_path:?} failed: {err}")))?;

    let mut hasher = blake3::Hasher::new();
    let mut offset: u64 = 0;

    while offset < size {
        let len = (size - offset).min(FILE_CHUNK_SIZE as u64);
        let chunk =
            client::fetch_file_chunk(app, shared_key, address, &entry.path, offset, len).await?;

        // 对端返回的长度与请求不一致说明源文件已变化，立即失败而不是写入错位内容。
        if chunk.len() as u64 != len {
            return Err(AppError::Other(anyhow::anyhow!(
                "file {} chunk {offset}..{} returned {} bytes",
                entry.path,
                offset + len,
                chunk.len()
            )));
        }

        file.write_all(&chunk).await.map_err(|err| {
            AppError::Other(anyhow::anyhow!("write {local_path:?} failed: {err}"))
        })?;
        hasher.update(&chunk);
        offset += len;

        if let Some(target) = target {
            target.report(offset);
        }
    }

    file.flush()
        .await
        .map_err(|err| AppError::Other(anyhow::anyhow!("flush {local_path:?} failed: {err}")))?;

    // 发送端只对需要按需拉取的条目采集整文件哈希，缺失时仅以长度为准。
    if let Some(expected) = entry.hash.as_deref() {
        let actual = hasher.finalize().to_hex().to_string();
        if actual != expected {
            return Err(AppError::Other(anyhow::anyhow!(
                "file {} checksum mismatch",
                entry.path
            )));
        }
    }

    Ok(())
}

/// 丢弃未完成的缓存目录。
async fn discard(root: &Path) {
    if let Err(err) = tokio::fs::remove_dir_all(root).await {
        log::warn!("sync file transfer: discard {root:?} failed: {err}");
    }
}

/// 生成缓存内文件名：保留原名便于识别，过滤跨平台非法字符，同批次重名追加序号。
fn unique_file_name(path: &str, index: usize, used: &mut HashMap<String, usize>) -> String {
    let base = Path::new(path)
        .file_name()
        .and_then(|name| name.to_str())
        .map(sanitize_file_name)
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| format!("file-{index}"));

    let count = used.entry(base.clone()).or_insert(0);
    if *count == 0 {
        *count = 1;
        return base;
    }

    *count += 1;
    let suffix = *count;

    // 重名时序号插在扩展名前，保留可双击打开的关联。
    match base.rsplit_once('.') {
        Some((stem, ext)) if !stem.is_empty() && !ext.is_empty() => {
            format!("{stem} ({suffix}).{ext}")
        }
        _ => format!("{base} ({suffix})"),
    }
}

/// 过滤 Windows 不允许的文件名字符，并避开保留设备名与超长文件名。
fn sanitize_file_name(name: &str) -> String {
    let replaced: String = name
        .chars()
        .map(|ch| match ch {
            '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*' => '_',
            _ => ch,
        })
        .collect();

    let trimmed = replaced.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    // Windows 保留名（CON / NUL / COM1 等）无法直接落盘，统一加前缀规避。
    let stem = trimmed.split('.').next().unwrap_or(trimmed);
    let sanitized = if is_windows_reserved_name(stem) {
        format!("_{trimmed}")
    } else {
        trimmed.to_owned()
    };

    sanitized.chars().take(MAX_FILE_NAME_CHARS).collect()
}

/// 判断是否 Windows 保留设备名（大小写不敏感）。
fn is_windows_reserved_name(stem: &str) -> bool {
    let upper = stem.to_ascii_uppercase();
    matches!(
        upper.as_str(),
        "CON"
            | "PRN"
            | "AUX"
            | "NUL"
            | "COM1"
            | "COM2"
            | "COM3"
            | "COM4"
            | "COM5"
            | "COM6"
            | "COM7"
            | "COM8"
            | "COM9"
            | "LPT1"
            | "LPT2"
            | "LPT3"
            | "LPT4"
            | "LPT5"
            | "LPT6"
            | "LPT7"
            | "LPT8"
            | "LPT9"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_replaces_windows_illegal_chars() {
        assert_eq!(sanitize_file_name("a:b*c?.txt"), "a_b_c_.txt");
        assert_eq!(sanitize_file_name("  spaced  "), "spaced");
    }

    #[test]
    fn sanitize_prefixes_reserved_names() {
        assert_eq!(sanitize_file_name("CON"), "_CON");
        assert_eq!(sanitize_file_name("nul.txt"), "_nul.txt");
        assert_eq!(sanitize_file_name("console"), "console");
    }

    #[test]
    fn unique_file_name_appends_suffix_on_duplicate() {
        let mut used = HashMap::new();

        assert_eq!(unique_file_name("/tmp/a.txt", 0, &mut used), "a.txt");
        assert_eq!(unique_file_name("/other/a.txt", 1, &mut used), "a (2).txt");
        assert_eq!(unique_file_name("/third/a.txt", 2, &mut used), "a (3).txt");
        assert_eq!(unique_file_name("/tmp/noext", 3, &mut used), "noext");
        assert_eq!(unique_file_name("/other/noext", 4, &mut used), "noext (2)");
    }

    #[test]
    fn unique_file_name_falls_back_when_basename_missing() {
        let mut used = HashMap::new();

        assert_eq!(unique_file_name("", 7, &mut used), "file-7");
    }

    #[tokio::test]
    async fn discard_ignores_missing_directory() {
        let root = std::env::temp_dir().join(format!(
            "ecopaste-sync-file-transfer-{}",
            uuid::Uuid::new_v4()
        ));

        discard(&root).await;
    }
}
