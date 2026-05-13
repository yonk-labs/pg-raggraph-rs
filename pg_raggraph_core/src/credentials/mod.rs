//! Credential handling: redaction (Plan 1) + AES-GCM encryption (Plan 4).
//!
//! Plan 4 adds `master_key` (file-backed key load + permission check) and
//! `encrypt` (AES-256-GCM with the `enc:v1:<nonce>:<ciphertext>` storage
//! format).

pub mod redact;
// pub mod master_key;  // <-- added in Task 4
// pub mod encrypt;     // <-- added in Task 5

pub use redact::redact;
