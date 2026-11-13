//! 前台窗口探测：判断用户触发粘贴时是否正对着文件资源管理器，并取它当前的目录。
//!
//! 背景：EcoPaste 的剪贴板窗口是「非激活面板」（macOS NSPanel nonactivating /
//! Windows focusable=false），弹出时不会把前台焦点抢走，所以这里拿到的
//! 前台窗口仍是用户原本正在操作的那个窗口，而不是 EcoPaste 自己。
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
        return windows_impl::explorer_directory();
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

/// Windows：前台进程是 explorer.exe 时，用 Shell.Application 取对应窗口的目录。
#[cfg(target_os = "windows")]
mod windows_impl {
    use std::os::windows::process::CommandExt;
    use std::path::PathBuf;
    use std::process::Command;

    /// CREATE_NO_WINDOW：避免每次粘贴都闪出控制台窗口。
    const CREATE_NO_WINDOW: u32 = 0x0800_0000;

    pub fn explorer_directory() -> Option<PathBuf> {
        let hwnd = unsafe { winapi::um::winuser::GetForegroundWindow() };
        if hwnd.is_null() {
            return None;
        }

        if !is_explorer_process(hwnd) {
            return None;
        }

        // 通过 HWND 精确定位对应的资源管理器窗口，避免多个窗口取错目录。
        let script = format!(
            "[Console]::OutputEncoding=[System.Text.Encoding]::UTF8; \
             $w=(New-Object -ComObject Shell.Application).Windows() | \
             Where-Object {{$_.HWND -eq {}}} | Select-Object -First 1; \
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

    /// 判断窗口所属进程是否为 explorer.exe。
    fn is_explorer_process(hwnd: winapi::shared::windef::HWND) -> bool {
        use std::os::windows::ffi::OsStringExt;

        use winapi::um::processthreadsapi::OpenProcess;
        use winapi::um::winbase::QueryFullProcessImageNameW;
        use winapi::um::winnt::PROCESS_QUERY_LIMITED_INFORMATION;
        use winapi::um::winuser::GetWindowThreadProcessId;

        let mut pid: u32 = 0;
        unsafe {
            GetWindowThreadProcessId(hwnd, &mut pid);
        }
        if pid == 0 {
            return false;
        }

        let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
        if handle.is_null() {
            return false;
        }

        let mut buffer = [0u16; 512];
        let mut size: u32 = buffer.len() as u32;
        let ok = unsafe { QueryFullProcessImageNameW(handle, 0, buffer.as_mut_ptr(), &mut size) };
        unsafe {
            winapi::um::handleapi::CloseHandle(handle);
        }
        if ok == 0 {
            return false;
        }

        let path = std::ffi::OsString::from_wide(&buffer[..size as usize]);
        let name = path
            .to_string_lossy()
            .rsplit(['\\', '/'])
            .next()
            .unwrap_or_default()
            .to_ascii_lowercase();

        name == "explorer.exe"
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
