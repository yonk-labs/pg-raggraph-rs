//! `pg_raggraph_core` — provider-agnostic logic for the `pg_raggraph` extension.
//!
//! Has no pgrx dependency; testable with plain `cargo test`. Used by both the
//! extension crate (linked into the .so) and the sidecar binary.

pub mod chunking;
pub mod credentials;
pub mod embedding;
pub mod error;
pub mod ingest;
pub mod llm;
pub mod retrieval;
pub mod types;

pub use error::{CoreError, CoreResult};
pub use types::*;

/// Test-only helpers shared between core unit tests and the pgrx extension's
/// `mod tests`. Public so the extension crate can call without re-implementing
/// the hashing.
pub mod test_helpers {
    use sha2::{Digest, Sha256};

    /// Derive a deterministic UUID-v4-shaped string from a namespace + seed byte.
    ///
    /// Used by pgrx fixture loaders to keep `documents.id` (a global PK) unique
    /// across namespaces, so parallel tests do not collide on the same
    /// hardcoded UUID. The output sets the version (4) and variant (RFC 4122)
    /// nibbles so `PostgreSQL`'s `uuid` parser accepts it.
    #[must_use]
    pub fn ns_uuid(ns: &str, seed: u8) -> String {
        let mut h = Sha256::new();
        h.update(ns.as_bytes());
        h.update([seed]);
        let bytes = h.finalize();
        let mut b = [0u8; 16];
        b.copy_from_slice(&bytes[..16]);
        b[6] = (b[6] & 0x0F) | 0x40; // version 4
        b[8] = (b[8] & 0x3F) | 0x80; // variant
        format!(
            "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            b[0],
            b[1],
            b[2],
            b[3],
            b[4],
            b[5],
            b[6],
            b[7],
            b[8],
            b[9],
            b[10],
            b[11],
            b[12],
            b[13],
            b[14],
            b[15],
        )
    }
}
