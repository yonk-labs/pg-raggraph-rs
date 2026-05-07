//! Canonical content-hash computation.
//!
//! Mission brief SC-007: re-ingesting the same source (identical `content_hash`)
//! is a no-op. Hash is SHA-256 over the canonical bytes, lowercase hex.
//! Pure function; called identically by the bg worker (Plan 3) and the
//! sidecar (Plan 5) so hashes are byte-stable across both code paths.

use sha2::{Digest, Sha256};

/// SHA-256 hex (64 lowercase chars) of `bytes`.
#[must_use]
pub fn content_hash(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    let digest = h.finalize();
    // Lowercase hex; one allocation.
    let mut s = String::with_capacity(64);
    for b in digest {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}
