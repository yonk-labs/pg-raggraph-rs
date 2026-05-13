//! Credential handling: redaction (Plan 1) + AES-GCM encryption (Plan 4).
//!
//! Plan 4 adds `master_key` (file-backed key load + permission check) and
//! `encrypt` (AES-256-GCM with the `enc:v1:<nonce>:<ciphertext>` storage
//! format).

pub mod encrypt;
pub mod master_key;
pub mod redact;

pub use encrypt::{decrypt_v1, encrypt_v1, is_encrypted};
pub use master_key::MasterKey;
pub use redact::redact;
