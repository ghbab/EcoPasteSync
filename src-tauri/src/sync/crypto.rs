//! 同步链路的配对密钥交换、请求认证与报文加密。
//!
//! - 配对密钥：X25519 ECDH。发起方只在明文请求里携带临时公钥，接收方在
//!   202 响应里返回自己的临时公钥，双方各自做 DH 后经 blake3 派生共享密钥；
//!   私钥始终不上线，仅窃听配对流量无法还原共享密钥。
//! - 配对校验：由共享密钥派生 6 位 SAS（Short Authentication String），
//!   两端各自展示给用户人工比对；中间人替换公钥时两端 SAS 必然不同。
//! - 请求认证：`X-Device-Id` + `X-Timestamp` + 请求路径做 blake3 keyed hash 签名，
//!   时间戳容差 ±2 分钟（容忍两机时钟偏差），防伪造与重放。
//! - 报文加密：ChaCha20-Poly1305，随机 12 字节 nonce 前置于密文。

use blake3::Hasher;
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use rand::RngCore;
use x25519_dalek::{PublicKey, StaticSecret};

use crate::core::{AppError, Result};

/// 认证时间戳容差（秒）。两台桌面机时钟偏差通常在秒级，2 分钟足够，
/// 同时把认证头的重放窗口压到最小。
pub const AUTH_TIMESTAMP_TOLERANCE_SECS: i64 = 120;

/// 配对临时密钥对：`secret` 只在进程内存中保留到配对结束，从不上线。
pub struct PairingKeypair {
    pub secret: [u8; 32],
    pub public: [u8; 32],
}

/// 生成 X25519 配对临时密钥对。
pub fn generate_pairing_keypair() -> PairingKeypair {
    let mut secret_bytes = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut secret_bytes);
    let secret = StaticSecret::from(secret_bytes);
    let public = PublicKey::from(&secret);

    PairingKeypair {
        secret: secret_bytes,
        public: public.to_bytes(),
    }
}

/// 由本机临时私钥 + 对端临时公钥做 ECDH，并按双方 device_id 排序派生共享密钥。
///
/// device_id 排序后参与哈希，保证两侧计算顺序无关、结果一致。
pub fn derive_pairing_key(
    secret: &[u8; 32],
    peer_public: &[u8; 32],
    id_a: &str,
    id_b: &str,
) -> Result<[u8; 32]> {
    let local = StaticSecret::from(*secret);
    let peer = PublicKey::from(*peer_public);
    let shared = local.diffie_hellman(&peer);

    // 对端公钥属于低阶点时共享秘密恒为 0，必须拒绝，避免弱密钥。
    if !shared.was_contributory() {
        return Err(AppError::Other(anyhow::anyhow!(
            "peer pairing public key is invalid"
        )));
    }

    let (first, second) = if id_a <= id_b {
        (id_a, id_b)
    } else {
        (id_b, id_a)
    };

    // 域分隔前缀：避免同一 DH 结果被其它协议复用。
    let mut hasher = Hasher::new();
    hasher.update(b"ecopaste-pair-v1");
    hasher.update(shared.as_bytes());
    hasher.update(first.as_bytes());
    hasher.update(second.as_bytes());
    Ok(*hasher.finalize().as_bytes())
}

/// 由共享密钥派生 6 位 SAS 验证码（补零到 6 位）。
///
/// 两端密钥一致时 SAS 必然一致；中间人无法让两端得到相同密钥，
/// 用户比对数字即可发现配对被劫持。
pub fn pairing_sas(key: &[u8; 32]) -> String {
    let mut hasher = Hasher::new_keyed(key);
    hasher.update(b"ecopaste-sas-v1");
    let digest = hasher.finalize();

    // 取摘要前 8 字节拼成 u64，再对 100 万取模，得到 0..=999999。
    let mut value: u64 = 0;
    for byte in digest.as_bytes().iter().take(8) {
        value = (value << 8) | (*byte as u64);
    }

    format!("{:06}", value % 1_000_000)
}

/// 把字节序列编码为小写 hex。
pub fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes.iter() {
        out.push_str(&format!("{byte:02x}"));
    }

    out
}

/// 把 64 位 hex 解析为 32 字节数组；长度或字符非法时报错。
fn hex_to_32(hex: &str) -> Result<[u8; 32]> {
    let raw = hex.as_bytes();
    if raw.len() != 64 {
        return Err(AppError::Other(anyhow::anyhow!("expected 64 hex chars")));
    }

    let mut out = [0u8; 32];
    for idx in 0..32 {
        let high = (raw[idx * 2] as char).to_digit(16);
        let low = (raw[idx * 2 + 1] as char).to_digit(16);
        match (high, low) {
            (Some(h), Some(l)) => {
                out[idx] = ((h << 4) | l) as u8;
            }
            _ => {
                return Err(AppError::Other(anyhow::anyhow!(
                    "hex string contains invalid characters"
                )));
            }
        }
    }

    Ok(out)
}

/// 把 X25519 公钥编码为 hex（配对请求 / 响应使用）。
pub fn public_key_to_hex(public: &[u8; 32]) -> String {
    bytes_to_hex(public)
}

/// 从 hex 恢复 X25519 公钥。
pub fn public_key_from_hex(hex: &str) -> Result<[u8; 32]> {
    hex_to_32(hex)
}

/// 把共享密钥序列化为 hex（存库用）。
pub fn key_to_hex(key: &[u8; 32]) -> String {
    bytes_to_hex(key)
}

/// 从 hex 恢复共享密钥。
pub fn key_from_hex(hex: &str) -> Result<[u8; 32]> {
    hex_to_32(hex)
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
    fn pairing_key_is_symmetric() {
        let alice = generate_pairing_keypair();
        let bob = generate_pairing_keypair();
        let key_ab =
            derive_pairing_key(&alice.secret, &bob.public, "device-a", "device-b").unwrap();
        let key_ba =
            derive_pairing_key(&bob.secret, &alice.public, "device-b", "device-a").unwrap();
        assert_eq!(key_ab, key_ba);
    }

    #[test]
    fn pairing_key_rejects_low_order_public_key() {
        let alice = generate_pairing_keypair();
        let zero_public = [0u8; 32];
        assert!(derive_pairing_key(&alice.secret, &zero_public, "device-a", "device-b").is_err());
    }

    #[test]
    fn sas_is_stable_and_six_digits() {
        let key = [9u8; 32];
        let sas = pairing_sas(&key);
        assert_eq!(sas.len(), 6);
        assert!(sas.chars().all(|c| c.is_ascii_digit()));
        assert_eq!(sas, pairing_sas(&key));
        assert_ne!(sas, pairing_sas(&[10u8; 32]));
    }

    #[test]
    fn public_key_hex_roundtrip() {
        let pair = generate_pairing_keypair();
        let hex = public_key_to_hex(&pair.public);
        assert_eq!(hex.len(), 64);
        assert_eq!(public_key_from_hex(&hex).unwrap(), pair.public);
        assert!(public_key_from_hex("xyz").is_err());
        assert!(public_key_from_hex(&"a".repeat(63)).is_err());
    }

    #[test]
    fn shared_key_hex_roundtrip() {
        let key = [3u8; 32];
        let hex = key_to_hex(&key);
        assert_eq!(key_from_hex(&hex).unwrap(), key);
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
