//! 应用对外版本号：上游三段版本 + 本项目发布序号。
//!
//! Tauri 打包要求 `version` 是标准三段 semver，因此第四位不写入
//! `tauri.conf.json`，而是在展示层由 `package.json` 的 `ecopasteRelease` 拼接。

use std::sync::OnceLock;

use serde_json::Value;

/// 编译期快照的 package.json：版本与发布序号只随构建变化。
const PACKAGE_JSON: &str = include_str!("../../../package.json");

/// 对外展示版本；读取 package.json 失败时退回 Cargo 包版本。
pub fn display_version() -> &'static str {
    static DISPLAY_VERSION: OnceLock<String> = OnceLock::new();

    DISPLAY_VERSION
        .get_or_init(|| {
            let Ok(package) = serde_json::from_str::<Value>(PACKAGE_JSON) else {
                return env!("CARGO_PKG_VERSION").to_owned();
            };

            let Some(version) = package.get("version").and_then(Value::as_str) else {
                return env!("CARGO_PKG_VERSION").to_owned();
            };

            // nightly 等带后缀的构建版本保持原样，只有上游三段版本才追加发布序号。
            if !is_plain_semver(version) {
                return version.to_owned();
            }

            match package.get("ecopasteRelease").and_then(Value::as_u64) {
                Some(release) => format!("{version}.{release}"),
                None => version.to_owned(),
            }
        })
        .as_str()
}

/// 判断是否为 `major.minor.patch` 形式的纯数字三段版本。
fn is_plain_semver(version: &str) -> bool {
    let parts: Vec<&str> = version.split('.').collect();

    if parts.len() != 3 {
        return false;
    }

    parts
        .iter()
        .all(|part| !part.is_empty() && part.chars().all(|ch| ch.is_ascii_digit()))
}

#[cfg(test)]
mod tests {
    use super::is_plain_semver;

    #[test]
    fn accepts_plain_three_part_version() {
        assert!(is_plain_semver("1.1.0"));
    }

    #[test]
    fn rejects_versions_with_suffix_or_extra_parts() {
        assert!(!is_plain_semver("1.1"));
        assert!(!is_plain_semver("1.1.0.1"));
        assert!(!is_plain_semver("1.1.0-nightly.20261112.1"));
    }
}
