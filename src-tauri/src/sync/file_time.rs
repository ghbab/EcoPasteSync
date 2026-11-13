//! 跨平台的文件时间还原。
//!
//! 用户要求：传输完成后本地文件的「创建时间 + 修改时间」都要与远端一致。
//! 平台能力差异：
//! - Windows：`SetFileTime` 可同时设置创建时间与修改时间；
//! - macOS：`utimensat` 设置修改时间，创建时间（birthtime）需 `setattrlist`
//!   且部分文件系统不支持，失败时只告警、不影响传输结果；
//! - 其它平台：`File::set_times` 只设置修改时间。

use std::path::Path;

/// 把远端时间戳还原到本地文件。
///
/// `mtime_ms` / `ctime_ms` 为 `None` 时保持系统默认值（即不覆盖）。
/// 返回的错误只用于日志告警：时间还原失败不应把传输判定为失败，
/// 因为文件内容已经完整落盘，时间只是元数据。
pub fn apply_times(
    path: &Path,
    mtime_ms: Option<i64>,
    ctime_ms: Option<i64>,
) -> std::io::Result<()> {
    #[cfg(target_os = "windows")]
    {
        windows_impl::apply(path, mtime_ms, ctime_ms)
    }

    #[cfg(target_os = "macos")]
    {
        macos_impl::apply(path, mtime_ms, ctime_ms)
    }

    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    {
        let _ = ctime_ms;
        if let Some(mtime) = mtime_ms {
            return set_modified(path, mtime);
        }
        Ok(())
    }
}

/// 用标准库设置修改时间。
///
/// Windows 与「其它平台」会调用它；macOS 走 `utimensat` 所以不会用到，
/// 这里保留定义并用 `allow(dead_code)` 避免 macOS 上的未使用告警。
#[allow(dead_code)]
fn set_modified(path: &Path, mtime_ms: i64) -> std::io::Result<()> {
    let millis = u64::try_from(mtime_ms).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "mtime is before unix epoch",
        )
    })?;
    let file = std::fs::OpenOptions::new().write(true).open(path)?;
    let times = std::fs::FileTimes::new()
        .set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_millis(millis));

    file.set_times(times)
}

/// Windows：先设置修改时间，再用 `SetFileTime` 单独设置创建时间。
#[cfg(target_os = "windows")]
mod windows_impl {
    use std::os::windows::io::AsRawHandle;
    use std::path::Path;

    use winapi::shared::minwindef::FILETIME;
    use winapi::um::fileapi::SetFileTime;
    use winapi::um::winnt::HANDLE;

    /// 1601-01-01 到 1970-01-01 的 100ns 间隔数。
    const EPOCH_DIFF_100NS: i64 = 116_444_736_000_000_000;

    pub fn apply(path: &Path, mtime_ms: Option<i64>, ctime_ms: Option<i64>) -> std::io::Result<()> {
        if let Some(mtime) = mtime_ms {
            super::set_modified(path, mtime)?;
        }

        let Some(ctime) = ctime_ms else {
            return Ok(());
        };

        // SetFileTime 需要一个可写句柄；打开失败说明文件已被移走或权限不足。
        let file = std::fs::OpenOptions::new().write(true).open(path)?;
        let handle = file.as_raw_handle() as HANDLE;
        let created = unix_ms_to_filetime(ctime);

        // 只改创建时间：访问时间与修改时间传空指针表示保持不变。
        let ok = unsafe { SetFileTime(handle, &created, std::ptr::null(), std::ptr::null()) };
        if ok == 0 {
            return Err(std::io::Error::last_os_error());
        }

        Ok(())
    }

    /// Unix 毫秒 → Windows FILETIME（100ns 间隔，1601 纪元）。
    fn unix_ms_to_filetime(ms: i64) -> FILETIME {
        let intervals = ms.saturating_mul(10_000).saturating_add(EPOCH_DIFF_100NS);
        let value = intervals as u64;

        FILETIME {
            dwLowDateTime: value as u32,
            dwHighDateTime: (value >> 32) as u32,
        }
    }
}

/// macOS：`utimensat` 设置修改时间，`setattrlist` 设置创建时间。
#[cfg(target_os = "macos")]
mod macos_impl {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    use std::path::Path;

    pub fn apply(path: &Path, mtime_ms: Option<i64>, ctime_ms: Option<i64>) -> std::io::Result<()> {
        let c_path = CString::new(path.as_os_str().as_bytes()).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "path contains nul byte")
        })?;

        if let Some(mtime) = mtime_ms {
            // 第一项传 UTIME_OMIT 表示保持不变，第二项是新的修改时间。
            let times = [
                libc::timespec {
                    tv_sec: 0,
                    tv_nsec: libc::UTIME_OMIT as libc::c_long,
                },
                libc::timespec {
                    tv_sec: mtime.div_euclid(1000) as libc::time_t,
                    tv_nsec: (mtime.rem_euclid(1000) * 1_000_000) as libc::c_long,
                },
            ];
            let rc = unsafe { libc::utimensat(libc::AT_FDCWD, c_path.as_ptr(), times.as_ptr(), 0) };
            if rc != 0 {
                return Err(std::io::Error::last_os_error());
            }
        }

        let Some(ctime) = ctime_ms else {
            return Ok(());
        };

        // birthtime 不在 POSIX 标准里，只能走 BSD 的 setattrlist。
        let mut attributes = libc::attrlist {
            bitmapcount: libc::ATTR_BIT_MAP_COUNT,
            reserved: 0,
            commonattr: libc::ATTR_CMN_CRTIME,
            volattr: 0,
            dirattr: 0,
            fileattr: 0,
            forkattr: 0,
        };
        let created = libc::timespec {
            tv_sec: ctime.div_euclid(1000) as libc::time_t,
            tv_nsec: (ctime.rem_euclid(1000) * 1_000_000) as libc::c_long,
        };

        let rc = unsafe {
            libc::setattrlist(
                c_path.as_ptr(),
                &mut attributes as *mut libc::attrlist as *mut libc::c_void,
                &created as *const libc::timespec as *mut libc::c_void,
                std::mem::size_of::<libc::timespec>(),
                0,
            )
        };
        if rc != 0 {
            return Err(std::io::Error::last_os_error());
        }

        Ok(())
    }
}

/// 判断远端时间与本地时间是否在容差内（±2 秒）。
///
/// 不同文件系统的时间精度不同（FAT 只有 2 秒），精确相等会把「未修改」
/// 误判成「已修改」，因此留出容差。
pub fn mtime_close(remote_ms: i64, local_ms: i64) -> bool {
    const TOLERANCE_MS: i64 = 2_000;

    remote_ms.saturating_sub(local_ms).abs() <= TOLERANCE_MS
}

#[cfg(test)]
mod tests {
    /// 时间还原的容差判断：修改时间允许 ±2 秒误差。
    #[test]
    fn mtime_tolerance_is_two_seconds() {
        assert!(crate::sync::file_time::mtime_close(1_000_000, 1_000_999));
        assert!(crate::sync::file_time::mtime_close(1_000_000, 1_002_000));
        assert!(!crate::sync::file_time::mtime_close(1_000_000, 1_002_001));
    }
}
