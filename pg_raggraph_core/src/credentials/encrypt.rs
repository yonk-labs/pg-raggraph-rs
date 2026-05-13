//! AES-256-GCM credential encryption.
//!
//! Storage form: `enc:v1:<base64(nonce)>:<base64(ciphertext|tag)>`.
//! Nonce is 12 bytes (AES-GCM standard); tag is 16 bytes appended by the
//! `aes-gcm` crate to the ciphertext output.
//!
//! SC-003: `provider_create` stores this exact format when the master key is
//! set. SC-015: plaintext never appears in logs, list output, or errors.

use aes_gcm::aead::Aead;
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use base64::Engine;
use base64::engine::general_purpose::STANDARD_NO_PAD;

use crate::error::{CoreError, CoreResult};

const NONCE_LEN: usize = 12;
const ENV_PREFIX: &str = "enc:v1:";

/// Encrypt `plaintext` under a 32-byte AES-256 key. Returns the storage form
/// `enc:v1:<nonce_b64>:<ciphertext_b64>`.
pub fn encrypt_v1(plaintext: &str, key: &[u8; 32]) -> CoreResult<String> {
    let cipher =
        Aes256Gcm::new_from_slice(key).map_err(|e| CoreError::Crypto(format!("key init: {e}")))?;
    let mut nonce_bytes = [0u8; NONCE_LEN];
    getrandom::getrandom(&mut nonce_bytes)
        .map_err(|e| CoreError::Crypto(format!("nonce rng: {e}")))?;
    let nonce = Nonce::from_slice(&nonce_bytes);
    let ct = cipher
        .encrypt(nonce, plaintext.as_bytes())
        .map_err(|e| CoreError::Crypto(format!("encrypt: {e}")))?;
    Ok(format!(
        "{ENV_PREFIX}{}:{}",
        STANDARD_NO_PAD.encode(nonce_bytes),
        STANDARD_NO_PAD.encode(ct)
    ))
}

/// Decrypt a storage-form string. Returns the plaintext.
pub fn decrypt_v1(envelope: &str, key: &[u8; 32]) -> CoreResult<String> {
    let rest = envelope
        .strip_prefix(ENV_PREFIX)
        .ok_or_else(|| CoreError::Crypto("envelope missing enc:v1: prefix".into()))?;
    let (nonce_b64, ct_b64) = rest
        .split_once(':')
        .ok_or_else(|| CoreError::Crypto("envelope missing ciphertext".into()))?;
    let nonce_bytes = STANDARD_NO_PAD
        .decode(nonce_b64)
        .map_err(|e| CoreError::Crypto(format!("nonce b64: {e}")))?;
    if nonce_bytes.len() != NONCE_LEN {
        return Err(CoreError::Crypto(format!(
            "nonce length {} != {NONCE_LEN}",
            nonce_bytes.len()
        )));
    }
    let ct = STANDARD_NO_PAD
        .decode(ct_b64)
        .map_err(|e| CoreError::Crypto(format!("ct b64: {e}")))?;
    let cipher =
        Aes256Gcm::new_from_slice(key).map_err(|e| CoreError::Crypto(format!("key init: {e}")))?;
    let nonce = Nonce::from_slice(&nonce_bytes);
    let pt = cipher
        .decrypt(nonce, ct.as_slice())
        .map_err(|e| CoreError::Crypto(format!("decrypt: {e}")))?;
    String::from_utf8(pt).map_err(|e| CoreError::Crypto(format!("plaintext utf-8: {e}")))
}

/// True if `s` looks like an encrypted envelope (lightweight check;
/// `decrypt_v1` is the authoritative validator).
#[must_use]
pub fn is_encrypted(s: &str) -> bool {
    s.starts_with(ENV_PREFIX) && s.split(':').count() == 4
}
