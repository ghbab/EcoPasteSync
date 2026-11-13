//! 前台窗口探测：判断用户触发粘贴时是否正对着文件资源管理器，并取它当前的目录。
//!
//! 背景：EcoPaste 的剪贴板窗口是「非激活面板」（macOS NSPanel nonactivating /
//! Windows focusable=false），正常情况下弹出时不会把前台焦点抢走。但搜索框聚焦
//! （例如开启「打开时自动聚焦搜索框」，或用户先点搜索框再点条目）会让面板临时
//! 变成前台窗口，此时必须回退到面板聚焦前记住的外部前台窗口，否则会被误判成
//! 「不在资源管理器」并降级为粘贴路径。
//!
//! 平台实现：
//! - Windows：`GetForegroundWindow` 确认进程是 `explorer.exe` 后，在进程内用
//!   Shell COM（`ShellWindows` + `IWebBrowser2`）取与前台窗口 HWND 匹配的目录；
//! - macOS：前台 App 是 Finder 时用 `osascript` 取前台 Finder 窗口的 POSIX 路径；
//! - 任何一步取不到都返回 `None`，调用方据此降级为「只复制路径」。
//!
//! Windows 侧不调用外部 `powershell.exe`：部分机器上它会被安全软件或组策略
//! 直接拦截（进程以 -1 退出），导致探测必然失败；进程内 COM 只依赖系统组件，
//! 不受这类限制。

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
    use std::mem::ManuallyDrop;
    use std::os::windows::ffi::OsStringExt;
    use std::path::PathBuf;
    use std::sync::Mutex;

    use winapi::shared::windef::HWND;
    use winapi::um::processthreadsapi::{GetCurrentProcessId, OpenProcess};
    use winapi::um::winbase::QueryFullProcessImageNameW;
    use winapi::um::winnt::PROCESS_QUERY_LIMITED_INFORMATION;
    use winapi::um::winuser::{GetForegroundWindow, GetWindowThreadProcessId, IsWindow};
    use windows::core::{ComInterface, BSTR};
    use windows::Win32::Foundation::SHANDLE_PTR;
    use windows::Win32::System::Com::{
        CoCreateInstance, CoInitializeEx, CoUninitialize, IDispatch, CLSCTX_ALL,
        COINIT_APARTMENTTHREADED,
    };
    use windows::Win32::System::Variant::{VARIANT, VARIANT_0, VARIANT_0_0, VARIANT_0_0_0, VT_I4};
    use windows::Win32::UI::Shell::{IShellWindows, IWebBrowser2, ShellWindows};

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

            match com_explorer_path(hwnd) {
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

    /// 用进程内 Shell COM（ShellWindows + IWebBrowser2）按 HWND 取目录。
    fn com_explorer_path(target: HWND) -> Option<PathBuf> {
        unsafe {
            // CoInitializeEx 返回 Ok 时（S_OK / S_FALSE）本次调用需要配对 CoUninitialize；
            // 返回 RPC_E_CHANGED_MODE 表示当前线程已用其它套间初始化过 COM：
            // 此时 COM 依然可以调用，只是本次不需要也不能再调用 CoUninitialize。
            let should_uninitialize: bool = match CoInitializeEx(None, COINIT_APARTMENTTHREADED) {
                Ok(()) => true,
                Err(err) => {
                    log::info!(
                        "foreground: CoInitializeEx failed, continue with existing apartment: {err:?}"
                    );
                    false
                }
            };

            let path: Option<PathBuf> = com_explorer_path_inner(target);

            if should_uninitialize {
                CoUninitialize();
            }

            path
        }
    }

    /// 遍历 ShellWindows，匹配 HWND 后把 LocationURL 还原成本地目录。
    fn com_explorer_path_inner(target: HWND) -> Option<PathBuf> {
        unsafe {
            let shell_windows: IShellWindows =
                match CoCreateInstance(&ShellWindows, None, CLSCTX_ALL) {
                    Ok(value) => value,
                    Err(err) => {
                        log::warn!("foreground: create ShellWindows failed: {err:?}");
                        return None;
                    }
                };

            let count: i32 = match shell_windows.Count() {
                Ok(value) => value,
                Err(err) => {
                    log::warn!("foreground: ShellWindows.Count failed: {err:?}");
                    return None;
                }
            };

            for index in 0..count {
                // ShellWindows.Item 接受 VARIANT 形式的索引，这里直接构造 VT_I4 变体。
                // 不能用 VARIANT::from：该类型没有 From<i32> 实现；也不能对
                // ManuallyDrop 包裹的联合体字段直接赋值（不会自动 DerefMut）。
                let payload: VARIANT_0_0_0 = VARIANT_0_0_0 { lVal: index };
                let value: VARIANT_0_0 = VARIANT_0_0 {
                    vt: VT_I4,
                    wReserved1: 0,
                    wReserved2: 0,
                    wReserved3: 0,
                    Anonymous: payload,
                };
                let variant: VARIANT = VARIANT {
                    Anonymous: VARIANT_0 {
                        Anonymous: ManuallyDrop::new(value),
                    },
                };

                let dispatch: IDispatch = match shell_windows.Item(variant) {
                    Ok(value) => value,
                    Err(_) => continue,
                };
                let browser: IWebBrowser2 = match dispatch.cast() {
                    Ok(value) => value,
                    Err(_) => continue,
                };
                let window_handle: SHANDLE_PTR = match browser.HWND() {
                    Ok(value) => value,
                    Err(_) => continue,
                };
                if window_handle.0 != target as isize {
                    continue;
                }

                let location: BSTR = match browser.LocationURL() {
                    Ok(value) => value,
                    Err(_) => continue,
                };
                let location_text: String = location.to_string();
                if let Some(path) = path_from_location_url(&location_text) {
                    return Some(path);
                }
            }

            None
        }
    }

    /// 把资源管理器的 `file://` 地址还原成本地路径。
    fn path_from_location_url(url: &str) -> Option<PathBuf> {
        if let Some(rest) = url.strip_prefix("file:///") {
            let decoded: String = percent_decode(rest);
            let trimmed: &str = decoded.trim_end_matches('/');
            return Some(PathBuf::from(trimmed.replace('/', "\\")));
        }

        // UNC：file://server/share/path
        if let Some(rest) = url.strip_prefix("file://") {
            let decoded: String = percent_decode(rest);
            let trimmed: &str = decoded.trim_end_matches('/');
            return Some(PathBuf::from(format!("\\\\{}", trimmed.replace('/', "\\"))));
        }

        None
    }

    /// 解码 URL 中的百分号转义（UTF-8）。
    fn percent_decode(input: &str) -> String {
        let bytes: &[u8] = input.as_bytes();
        let mut output: Vec<u8> = Vec::with_capacity(bytes.len());
        let mut index: usize = 0usize;

        while index < bytes.len() {
            if bytes[index] == b'%' && index + 2 < bytes.len() {
                let high: Option<u8> = hex_value(bytes[index + 1]);
                let low: Option<u8> = hex_value(bytes[index + 2]);
                if let (Some(high), Some(low)) = (high, low) {
                    output.push(high * 16 + low);
                    index += 3;
                    continue;
                }
            }

            output.push(bytes[index]);
            index += 1;
        }

        String::from_utf8_lossy(&output).into_owned()
    }

    /// 单个十六进制字符转数值。
    fn hex_value(byte: u8) -> Option<u8> {
        if byte.is_ascii_digit() {
            return Some(byte - b'0');
        }
        if (b'a'..=b'f').contains(&byte) {
            return Some(byte - b'a' + 10);
        }
        if (b'A'..=b'F').contains(&byte) {
            return Some(byte - b'A' + 10);
        }

        None
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
