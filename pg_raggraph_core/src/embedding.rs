//! Deterministic test-only embedder.
//!
//! Plan 2 ships this as the embedding contract for `pgrg.embed`. Plan 3
//! introduces the real model loader (chunkshop `hf_cache` for
//! `BAAI/bge-small-en-v1.5`); the SQL surface (`pgrg.embed`) does not
//! change. Until Plan 3, all retrieval tests use this embedder.
//!
//! Mission brief SC-002: byte-identical output for identical input, dim
//! equal to the `pgrg.embed_dim` GUC.
//! Mission brief SC-011: no LLM provider lookup, no network — runs on a
//! fresh PG with no `pgrg.providers` rows.

use sha2::{Digest, Sha256};

/// Hash-derived deterministic embedding.
///
/// Produces an L2-normalized `Vec<f32>` of length `dim`. Pure function
/// (same input → same output across processes and machines). Suitable
/// for tests and parity smoke runs; NOT a semantic embedding — Plan 3
/// replaces this with the real `bge-small-en-v1.5` ONNX model.
///
/// `u as f32 / u32::MAX as f32`: precision loss is intentional and
/// bounded; we only need a stable spread in (-1, 1).
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn deterministic_embed(text: &str, dim: usize) -> Vec<f32> {
    // Expand SHA-256 by repeated hashing until we have enough bytes for `dim` f32s.
    let bytes_needed = dim * 4;
    let mut buf = Vec::<u8>::with_capacity(bytes_needed);
    let mut counter: u32 = 0;
    while buf.len() < bytes_needed {
        let mut hasher = Sha256::new();
        hasher.update(text.as_bytes());
        hasher.update(counter.to_le_bytes());
        buf.extend_from_slice(&hasher.finalize());
        counter = counter.wrapping_add(1);
    }
    buf.truncate(bytes_needed);

    // Each f32 component: convert 4 bytes to a u32, then map to (-1, 1).
    let mut v: Vec<f32> = buf
        .chunks_exact(4)
        .map(|b| {
            let u = u32::from_le_bytes([b[0], b[1], b[2], b[3]]);
            // Map u32 -> [-1, 1]. Avoid 0 norm by ensuring nonzero spread.
            (u as f32 / u32::MAX as f32) * 2.0 - 1.0
        })
        .collect();

    // L2-normalize (avoid divide-by-zero with epsilon).
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 1e-12 {
        for x in &mut v {
            *x /= norm;
        }
    }
    v
}
