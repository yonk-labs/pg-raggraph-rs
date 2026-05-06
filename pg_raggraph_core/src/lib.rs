//! `pg_raggraph_core` — provider-agnostic logic for the `pg_raggraph` extension.
//!
//! Has no pgrx dependency; testable with plain `cargo test`. Used by both the
//! extension crate (linked into the .so) and the sidecar binary.

pub mod embedding;
pub mod error;
pub mod retrieval;
pub mod types;

pub use error::{CoreError, CoreResult};
pub use types::*;

pub mod credentials {
    /// Redacted form for display: keeps the first 3 chars, replaces the rest with `***`.
    /// Designed to keep the provider prefix (sk-, key-, ...) visible while hiding the secret.
    ///
    /// Uses character-aware indexing so multi-byte UTF-8 inputs do not panic on a
    /// non-char-boundary byte index.
    #[must_use]
    pub fn redact(credential: &str) -> String {
        if credential.chars().count() <= 3 {
            return "***".to_string();
        }
        // Find the byte index of the 4th character (3-char prefix end).
        let cutoff = credential
            .char_indices()
            .nth(3)
            .map_or(credential.len(), |(i, _)| i);
        format!("{}***", &credential[..cutoff])
    }
}
