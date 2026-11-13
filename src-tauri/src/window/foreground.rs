//! 前台窗口探测：判断用户触发粘贴时是否正对着文件资源管理器，并取它当前的目录。
//!
//! 背景：EcoPaste 的剪贴板窗口是「非激活面板」（macOS NSPanel nonactivating /
//! Windows focusable=false），正常情况下弹出时不会把前台焦点抢走。但搜索框聚焦
//! （例如开启「打开时自动聚焦搜索框」，或用户先点搜索框再点条目）会让面板临时
//! 变成前台窗口，此时必须回退到面板聚焦前记住的外部前台窗口，否则会被误判成
//! 「不在资源管理器」并降级为粘贴路径。
//!
//! 平台实现：
//! - Windows：`GetForegroundWindow` 确认进程是 `explorer.exe` 后，用系统自带的
//!   PowerShell 调 Shell.Application COM 取与前台窗口 HWND 匹配的资源管理器路径；
//! - macOS：前台 App 是 Finder 时用 `osascript` 取前台 Finder 窗口的 POSIX 路径；
//! - 任何一步取不到都返回 `None`，调用方据此降级为「只复制路径」。
//!
//! 之所以不在 Rust 里直接操作 Shell COM：`IShellWindows` 的接口绑定与
//! `windows` crate 版本强相关，而这里只需要一次低频调用；用系统脚本换取
//! 更低的维护成本，取路径失败也不影响主流程。

use std::path::PathBuf;

/// 返回前台资源管理器窗口当前打开的目录；前台不是资源管理器时返回 `None`。
pub fn explorer_directory() -> Option<PathBuf> {
    #[cfg(target_os = "windows")]
    {
        windows_impl::explorer_directory()
    }

    #[cfg(target_os = "macos")]
    {
        macos_impl::explorer_directory()
    }

    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    {
        None
    }
}

/// 记录当前前台窗口（仅当它不属于本进程）。
///
/// 在剪贴板窗口显示前、以及搜索框聚焦前调用，供后续粘贴时回退使用。
#[cfg(target_os = "windows")]
pub fn remember_external_foreground() {
    windows_impl::remember_external_foreground();
}

/// Windows：前台进程是 explorer.exe 时，用 Shell.Application 取对应窗口的目录。
#[cfg(target_os = "windows")]
mod windows_impl {
    use std::os::windows::ffi::OsStringExt;
    use std::os::windows::process::CommandExt;
    use std::path::PathBuf;
    use std::process::Command;
    use std::sync::Mutex;

    use winapi::shared::windef::HWND;
    use winapi::um::processthreadsapi::{GetCurrentProcessId, OpenProcess};
    use winapi::um::winbase::QueryFullProcessImageNameW;
    use winapi::um::winnt::PROCESS_QUERY_LIMITED_INFORMATION;
    use winapi::um::winuser::{GetForegroundWindow, GetWindowThreadProcessId, IsWindow};

    /// CREATE_NO_WINDOW：避免每次粘贴都闪出控制台窗口。
    const CREATE_NO_WINDOW: u32 = 0x0800_0000;

    /// 最近一次「非本进程」的前台窗口。
    ///
    /// 剪贴板窗口在搜索框聚焦时会临时变成可聚焦窗口并把焦点抢过来（例如开启了
    /// 「打开时自动聚焦搜索框」，或用户先点搜索框再点条目）。此时直接读
    /// `GetForegroundWindow` 只能拿到 EcoPaste 自己，会被误判成「不在资源管理器」，
    /// 进而降级为粘贴路径。这里记住剪贴板窗口显示 / 聚焦之前的外部前台窗口作为兜底。
    static LAST_EXTERNAL_FOREGROUND: Mutex<Option<isize>> = Mutex::new(None);

    /// 记录当前前台窗口；仅当它不属于本进程时写入。
    pub fn remember_external_foreground() {
        let hwnd = unsafe { GetForegroundWindow() };
        if hwnd.is_null() {
            return;
        }
        if is_own_process_window(hwnd) {
            return;
        }

        let mut guard = match LAST_EXTERNAL_FOREGROUND.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        *guard = Some(hwnd as isize);
    }

    /// 返回前台资源管理器窗口当前打开的目录。
    ///
    /// 依次检查当前前台窗口、剪贴板窗口获得焦点前记住的外部窗口；
    /// 第一个「属于 explorer.exe 且能取到目录」的候选即为粘贴目标。
    pub fn explorer_directory() -> Option<PathBuf> {
        let mut candidates: Vec<isize> = Vec::new();

        let current = unsafe { GetForegroundWindow() };
        if !current.is_null() {
            candidates.push(current as isize);
        }

        let remembered = match LAST_EXTERNAL_FOREGROUND.lock() {
            Ok(guard) => *guard,
            Err(poisoned) => *poisoned.into_inner(),
        };
        if let Some(value) = remembered {
            if !candidates.contains(&value) {
                candidates.push(value);
            }
        }

        for value in candidates.iter().copied() {
            let hwnd = value as HWND;

            if unsafe { IsWindow(hwnd) } == 0 {
                log::info!("foreground: candidate hwnd={value} is no longer valid");
                continue;
            }

            if !is_explorer_process(hwnd) {
                log::info!(
                    "foreground: candidate hwnd={value} exe={:?} is not explorer.exe",
                    process_exe_name(hwnd)
                );
                continue;
            }

            match query_explorer_path(hwnd) {
                Some(path) => {
                    log::info!(
                        "foreground: resolved explorer directory {path:?} from hwnd={value}"
                    );
                    return Some(path);
                }
                None => {
                    log::warn!("foreground: explorer hwnd={value} has no folder path");
                }
            }
        }

        log::warn!("foreground: no explorer directory found, candidates={candidates:?}");
        None
    }

    /// 用 Shell.Application 按 HWND 精确查询资源管理器窗口的当前目录。
    fn query_explorer_path(hwnd: HWND) -> Option<PathBuf> {
        // HWND 统一按 int64 比较：Automation 暴露的 HWND 可能是 32 位有符号数，
        // 直接与 64 位句柄比较在部分系统上会漏匹配。
        let script = format!(
            "[Console]::OutputEncoding=[System.Text.Encoding]::UTF8; \
             $target=[int64]{}; \
             $w=(New-Object -ComObject Shell.Application).Windows() | \
             Where-Object {{ try {{ [int64]$_.HWND -eq $target }} catch {{ $false }} }} | \
             Select-Object -First 1; \
             if($w){{ $w.Document.Folder.Self.Path }}",
            hwnd as isize
        );

        let output = Command::new("powershell")
            .args([
                "-NoProfile",
                "-NonInteractive",
                "-WindowStyle",
                "Hidden",
                "-Command",
                &script,
            ])
            .creation_flags(CREATE_NO_WINDOW)
            .output()
            .ok()?;

        if !output.status.success() {
            log::warn!(
                "foreground: powershell exited with {:?}",
                output.status.code()
            );
            return None;
        }

        // PowerShell 可能带 BOM / 行尾空白。
        let raw = String::from_utf8_lossy(&output.stdout);
        let trimmed = raw.trim_matches(|ch: char| ch.is_whitespace() || ch == '\u{feff}');
        if trimmed.is_empty() {
            return None;
        }

        Some(PathBuf::from(trimmed))
    }

    /// 判断窗口所属进程是否为本进程（EcoPaste 自己）。
    fn is_own_process_window(hwnd: HWND) -> bool {
        let mut pid: u32 = 0;
        unsafe {
            GetWindowThreadProcessId(hwnd, &mut pid);
        }
        if pid == 0 {
            return false;
        }

        pid == unsafe { GetCurrentProcessId() }
    }

    /// 取窗口所属进程的可执行文件名（小写）。
    fn process_exe_name(hwnd: HWND) -> Option<String> {
        let mut pid: u32 = 0;
        unsafe {
            GetWindowThreadProcessId(hwnd, &mut pid);
        }
        if pid == 0 {
            return None;
        }

        let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
        if handle.is_null() {
            return None;
        }

        let mut buffer = [0u16; 512];
        let mut size: u32 = buffer.len() as u32;
        let ok = unsafe { QueryFullProcessImageNameW(handle, 0, buffer.as_mut_ptr(), &mut size) };
        unsafe {
            winapi::um::handleapi::CloseHandle(handle);
        }
        if ok == 0 {
            return None;
        }

        let path = std::ffi::OsString::from_wide(&buffer[..size as usize]);
        let name = path
            .to_string_lossy()
            .rsplit(['\\', '/'])
            .next()
            .unwrap_or_default()
            .to_ascii_lowercase();

        Some(name)
    }

    /// 判断窗口所属进程是否为 explorer.exe。
    fn is_explorer_process(hwnd: HWND) -> bool {
        process_exe_name(hwnd).as_deref() == Some("explorer.exe")
    }
}

/// macOS：前台是 Finder 时，取前台 Finder 窗口指向的目录。
#[cfg(target_os = "macos")]
mod macos_impl {
    use std::path::PathBuf;
    use std::process::Command;

    pub fn explorer_directory() -> Option<PathBuf> {
        // System Events 判断前台 App 名；只有 Finder 才继续取窗口路径。
        let script = r#"
tell application "System Events"
    set frontName to name of first application process whose frontmost is true
end tell
if frontName is "Finder" then
    tell application "Finder"
        try
            return POSIX path of (target of front window as alias)
        end try
    end tell
end if
return ""
"#;

        let output = Command::new("osascript")
            .arg("-e")
            .arg(script)
            .output()
            .ok()?;
        if !output.status.success() {
            log::warn!(
                "foreground: osascript exited with {:?}",
                output.status.code()
            );
            return None;
        }

        let raw = String::from_utf8_lossy(&output.stdout);
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return None;
        }

        Some(PathBuf::from(trimmed))
    }
}
