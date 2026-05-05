//! `pg_raggraph_core` — provider-agnostic logic for the `pg_raggraph` extension.
//!
//! Has no pgrx dependency; testable with plain `cargo test`. Used by both the
//! extension crate (linked into the .so) and the sidecar binary.

pub mod error;
pub mod retrieval;
pub mod types;

pub use error::{CoreError, CoreResult};
pub use types::*;

pub mod credentials {
    /// Redacted form for display: keeps the first 3 chars, replaces the rest with `***`.
    /// Designed to keep the provider prefix (sk-, key-, ...) visible while hiding the secret.
    #[must_use]
    pub fn redact(credential: &str) -> String {
        if credential.len() <= 3 {
            return "***".to_string();
        }
        let (visible, _) = credential.split_at(3);
        format!("{visible}***")
    }
}
