//! 文件条目的分块拉取与缓存落盘。
//!
//! 两种触发时机共用本模块：
//! - 条目内文件总大小不超过直接传输阈值 → 接收端在入库前立即拉取；
//! - 超过阈值 → 只同步路径，粘贴时再按需拉取。
//!
//! 缓存目录按「远端路径列表」的 blake3 命名，重复接收同一批路径时复用同一目录。
//! 传输不做断点续传：中途失败或用户取消一律丢弃半成品，下次从头再拉。

use std::collections::HashMap;
use std::path::Path;

use tauri::{AppHandle, Manager};
use tokio::io::AsyncWriteExt;

use crate::core::{paths, AppError, Result};
use crate::sync::client;
use crate::sync::devices;
use crate::sync::protocol::{SyncFileEntryDto, FILE_CHUNK_SIZE};
use crate::sync::SyncManager;

/// 同步文件缓存的子目录名（位于 `<app_data_dir>/cache` 下）。
const CACHE_SUBDIR: &str = "sync-files";
/// 缓存文件名保留的最大字符数，避免中文等多字节字符超出文件系统 255 字节限制。
const MAX_FILE_NAME_CHARS: usize = 120;

/// 把远端条目涉及的文件全部拉到本地缓存目录，返回本地路径列表。
///
/// 调用方负责保证 `files` 中不含目录、每项都带 `size`；这里对不可传输的输入
/// 直接报错，避免产生「有路径没有内容」的误导性缓存。
pub async fn pull_files(
    app: &AppHandle,
    remote_content: &str,
    source_device_id: &str,
    files: &[SyncFileEntryDto],
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
        let pulled = pull_one_file(
            app,
            &device.shared_key,
            &address,
            entry,
            size as u64,
            &local_path,
        )
        .await;

        if let Err(err) = pulled {
            // 任一文件失败即整体放弃：保留半套文件会让粘贴内容不完整。
            discard(&root).await;
            return Err(err);
        }

        local_paths.push(local_path.to_string_lossy().into_owned());
    }

    Ok(local_paths)
}

/// 拉取单个文件的全部块并校验完整性。
async fn pull_one_file(
    app: &AppHandle,
    shared_key: &str,
    address: &str,
    entry: &SyncFileEntryDto,
    size: u64,
    local_path: &Path,
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
