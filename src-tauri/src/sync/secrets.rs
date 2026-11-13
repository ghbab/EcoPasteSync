//! 配对共享密钥的落地存储。
//!
//! 密钥不写 SQLite：数据库文件可被同用户进程直接读取，明文密钥等于把
//! 所有已配对设备的同步权限交给任何本地程序。这里改用系统钥匙串：
//! macOS Keychain / Windows Credential Manager，按 device_id 区分条目。
//!
//! 单元测试使用进程内内存实现，避免测试环境访问真实钥匙串。

/// 把共享密钥写入系统钥匙串（覆盖同设备旧条目）。
pub fn store(device_id: &str, key_hex: &str) -> crate::core::Result<()> {
    backend::store(device_id, key_hex)
}

/// 读取共享密钥；钥匙串中没有该设备条目时返回 `None`。
pub fn load(device_id: &str) -> crate::core::Result<Option<String>> {
    backend::load(device_id)
}

/// 删除共享密钥条目；条目不存在视为成功。
pub fn remove(device_id: &str) -> crate::core::Result<()> {
    backend::remove(device_id)
}

#[cfg(all(not(test), any(target_os = "macos", target_os = "windows")))]
mod backend {
    use crate::core::{AppError, Result};

    /// 钥匙串服务名（同一服务下按 device_id 区分条目）。
    const KEYRING_SERVICE: &str = "dev.ecopaste.sync";

    /// 打开指定设备的钥匙串条目。
    fn entry(device_id: &str) -> Result<keyring::Entry> {
        keyring::Entry::new(KEYRING_SERVICE, device_id)
            .map_err(|err| AppError::Other(anyhow::anyhow!("open keyring entry failed: {err}")))
    }

    pub fn store(device_id: &str, key_hex: &str) -> Result<()> {
        entry(device_id)?.set_password(key_hex).map_err(|err| {
            AppError::Other(anyhow::anyhow!("store shared key in keyring failed: {err}"))
        })
    }

    pub fn load(device_id: &str) -> Result<Option<String>> {
        match entry(device_id)?.get_password() {
            Ok(value) => Ok(Some(value)),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(err) => Err(AppError::Other(anyhow::anyhow!(
                "load shared key from keyring failed: {err}"
            ))),
        }
    }

    pub fn remove(device_id: &str) -> Result<()> {
        match entry(device_id)?.delete_credential() {
            Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
            Err(err) => Err(AppError::Other(anyhow::anyhow!(
                "delete shared key from keyring failed: {err}"
            ))),
        }
    }
}

/// 单元测试用的内存钥匙串。
///
/// 用线程本地存储按测试线程隔离，避免并行用例共用同一 device_id 时互相覆盖。
#[cfg(test)]
mod backend {
    use std::cell::RefCell;
    use std::collections::HashMap;

    use crate::core::Result;

    thread_local! {
        static MEMORY: RefCell<HashMap<String, String>> = RefCell::new(HashMap::new());
    }

    pub fn store(device_id: &str, key_hex: &str) -> Result<()> {
        MEMORY.with(|memory| {
            memory
                .borrow_mut()
                .insert(device_id.to_owned(), key_hex.to_owned());
        });
        Ok(())
    }

    pub fn load(device_id: &str) -> Result<Option<String>> {
        Ok(MEMORY.with(|memory| memory.borrow().get(device_id).cloned()))
    }

    pub fn remove(device_id: &str) -> Result<()> {
        MEMORY.with(|memory| {
            memory.borrow_mut().remove(device_id);
        });
        Ok(())
    }
}

/// 非 macOS / Windows 的构建（仅用于本地静态检查）不提供密钥存储。
#[cfg(all(not(test), not(any(target_os = "macos", target_os = "windows"))))]
mod backend {
    use crate::core::{AppError, Result};

    pub fn store(_device_id: &str, _key_hex: &str) -> Result<()> {
        Err(AppError::Other(anyhow::anyhow!(
            "system keyring is only supported on macOS and Windows"
        )))
    }

    pub fn load(_device_id: &str) -> Result<Option<String>> {
        Ok(None)
    }

    pub fn remove(_device_id: &str) -> Result<()> {
        Ok(())
    }
}
