//! 同步链路的密钥派生、请求认证与报文加密。
//!
//! - 共享密钥：配对时由发起方的 32 字节随机 `seed` + 双方 device_id 排序拼接，
//!   经 blake3 派生，两侧独立计算结果一致，密钥本身从不上线。
//! - 请求认证：`X-Device-Id` + `X-Timestamp` + 请求路径做 blake3 keyed hash 签名，
//!   时间戳容差 ±10 分钟（容忍两机时钟偏差），防伪造与重放。
//! - 报文加密：ChaCha20-Poly1305，随机 12 字节 nonce 前置于密文。

use blake3::Hasher;
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use rand::RngCore;

use crate::core::{AppError, Result};

/// 认证时间戳容差（秒）。两台桌面机时钟偏差通常在秒级，10 分钟足够宽松。
pub const AUTH_TIMESTAMP_TOLERANCE_SECS: i64 = 600;

/// 生成 32 字节随机 seed 的 hex（配对发起方调用）。
pub fn generate_pairing_seed() -> String {
    let mut bytes = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// 从 hex 恢复 32 字节 seed。
pub fn seed_from_hex(hex: &str) -> Result<[u8; 32]> {
    let mut out = [0u8; 32];
    if hex.len() != 64 {
        return Err(AppError::Other(anyhow::anyhow!(
            "pairing seed must be 64 hex chars"
        )));
    }

    for (idx, chunk) in hex.as_bytes().chunks(2).enumerate() {
        let high = (chunk[0] as char).to_digit(16);
        let low = (chunk[1] as char).to_digit(16);
        match (high, low) {
            (Some(h), Some(l)) => {
                out[idx] = ((h << 4) | l) as u8;
            }
            _ => {
                return Err(AppError::Other(anyhow::anyhow!(
                    "pairing seed contains invalid hex"
                )));
            }
        }
    }

    Ok(out)
}

/// 由配对 seed 与双方 device_id 派生共享密钥。
/// device_id 排序后参与哈希，保证两侧计算顺序无关、结果一致。
pub fn derive_shared_key(seed: &[u8; 32], id_a: &str, id_b: &str) -> [u8; 32] {
    let (first, second) = if id_a <= id_b {
        (id_a, id_b)
    } else {
        (id_b, id_a)
    };

    let mut hasher = Hasher::new();
    hasher.update(seed);
    hasher.update(first.as_bytes());
    hasher.update(second.as_bytes());
    *hasher.finalize().as_bytes()
}

/// 把字节数组序列化为 hex（存库用）。
pub fn key_to_hex(key: &[u8; 32]) -> String {
    key.iter().map(|b| format!("{b:02x}")).collect()
}

/// 从 hex 恢复共享密钥。
pub fn key_from_hex(hex: &str) -> Result<[u8; 32]> {
    seed_from_hex(hex)
}

/// 计算请求签名：blake3 keyed hash over `device_id|timestamp|path`。
pub fn sign_request(key: &[u8; 32], device_id: &str, timestamp: i64, path: &str) -> String {
    let mut hasher = Hasher::new_keyed(key);
    hasher.update(device_id.as_bytes());
    hasher.update(b"|");
    hasher.update(timestamp.to_string().as_bytes());
    hasher.update(b"|");
    hasher.update(path.as_bytes());
    hasher.finalize().to_hex().to_string()
}

/// 校验时间戳是否落在容差窗口内。
pub fn timestamp_within_tolerance(timestamp: i64, now: i64) -> bool {
    (timestamp - now).abs() <= AUTH_TIMESTAMP_TOLERANCE_SECS
}

/// 加密明文：输出 `nonce(12) || ciphertext`。
pub fn seal(key: &[u8; 32], plaintext: &[u8]) -> Result<Vec<u8>> {
    let cipher = ChaCha20Poly1305::new(Key::from_slice(key));
    let mut nonce_bytes = [0u8; 12];
    rand::rngs::OsRng.fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);

    let ciphertext = cipher
        .encrypt(nonce, Payload::from(plaintext))
        .map_err(|_| AppError::Other(anyhow::anyhow!("encrypt sync payload failed")))?;

    let mut out = Vec::with_capacity(12 + ciphertext.len());
    out.extend_from_slice(&nonce_bytes);
    out.extend_from_slice(&ciphertext);
    Ok(out)
}

/// 解密 [`seal`] 的输出。
pub fn open(key: &[u8; 32], data: &[u8]) -> Result<Vec<u8>> {
    if data.len() < 12 {
        return Err(AppError::Other(anyhow::anyhow!(
            "sync payload too short to contain nonce"
        )));
    }

    let (nonce_bytes, ciphertext) = data.split_at(12);
    let cipher = ChaCha20Poly1305::new(Key::from_slice(key));
    let plaintext = cipher
        .decrypt(Nonce::from_slice(nonce_bytes), Payload::from(ciphertext))
        .map_err(|_| AppError::Other(anyhow::anyhow!("decrypt sync payload failed")))?;

    Ok(plaintext)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seed_roundtrip() {
        let seed = generate_pairing_seed();
        let parsed = seed_from_hex(&seed).unwrap();
        assert_eq!(parsed.len(), 32);
    }

    #[test]
    fn seed_rejects_invalid_input() {
        assert!(seed_from_hex("xyz").is_err());
        assert!(seed_from_hex(&"a".repeat(63)).is_err());
    }

    #[test]
    fn shared_key_is_symmetric() {
        let seed = [7u8; 32];
        let key_ab = derive_shared_key(&seed, "device-a", "device-b");
        let key_ba = derive_shared_key(&seed, "device-b", "device-a");
        assert_eq!(key_ab, key_ba);
    }

    #[test]
    fn seal_open_roundtrip() {
        let key = [42u8; 32];
        let data = b"hello sync".to_vec();
        let sealed = seal(&key, &data).unwrap();
        let opened = open(&key, &sealed).unwrap();
        assert_eq!(opened, data);
    }

    #[test]
    fn open_rejects_wrong_key() {
        let key = [42u8; 32];
        let other = [43u8; 32];
        let sealed = seal(&key, b"secret").unwrap();
        assert!(open(&other, &sealed).is_err());
    }

    #[test]
    fn signature_is_deterministic() {
        let key = [1u8; 32];
        let sig = sign_request(&key, "dev", 1000, "/api/clipboard/push");
        assert_eq!(sig, sign_request(&key, "dev", 1000, "/api/clipboard/push"));
        assert_ne!(sig, sign_request(&key, "dev", 1001, "/api/clipboard/push"));
    }

    #[test]
    fn tolerance_window() {
        assert!(timestamp_within_tolerance(
            1000,
            1000 + AUTH_TIMESTAMP_TOLERANCE_SECS
        ));
        assert!(!timestamp_within_tolerance(
            1000,
            1000 + AUTH_TIMESTAMP_TOLERANCE_SECS + 1
        ));
    }
}
