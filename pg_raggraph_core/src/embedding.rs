//! Embedding backend abstraction + deterministic test impl.
//!
//! Plan 2 shipped `deterministic_embed` as a free function. Plan 3 introduces
//! the `EmbeddingBackend` trait so a real ONNX-backed impl (`OnnxEmbedder`,
//! Task 5) can replace the deterministic one in production builds while
//! `pg_test` and `cargo test` continue to use the deterministic impl for
//! byte-stable fixture parity.
//!
//! Mission brief SC-002: byte-identical output for identical input, dim
//! equal to the `pgrg.embed_dim` GUC.
//! Mission brief SC-009: production embedder loaded once per worker process
//! at startup; never per-job. The trait is `Send + Sync + 'static` to permit
//! storage in a worker-local `OnceCell`.

use crate::error::CoreResult;
use sha2::{Digest, Sha256};

/// Trait for any embedding backend the worker can load.
///
/// Implementations must be cheap to share by `&self` (typically wrapped in
/// `Arc` internally) and thread-safe (`Send + Sync`) so a single backend
/// instance can serve concurrent jobs within a worker.
pub trait EmbeddingBackend: Send + Sync + 'static {
    /// Vector dimension this backend produces. Must match `pgrg.embed_dim`.
    fn dim(&self) -> usize;

    /// Embed a single text. Returns a `Vec<f32>` of length `self.dim()`.
    ///
    /// # Errors
    /// Returns `CoreError` if the backend fails to produce an embedding
    /// (e.g., ONNX session error, tokenizer failure). The deterministic
    /// impl is infallible.
    fn embed(&self, text: &str) -> CoreResult<Vec<f32>>;

    /// Embed a batch. Default impl loops `embed`; ONNX backend overrides
    /// for batched inference.
    ///
    /// # Errors
    /// Returns the first `CoreError` encountered while embedding any input.
    fn embed_batch(&self, texts: &[&str]) -> CoreResult<Vec<Vec<f32>>> {
        texts.iter().map(|t| self.embed(t)).collect()
    }
}

/// Deterministic SHA-256-derived embedder. Used by `cargo test`,
/// `pgrx::pg_test`, and Plan 2's fixture loaders.
#[derive(Debug, Clone)]
pub struct DeterministicEmbedder {
    dim: usize,
}

impl DeterministicEmbedder {
    #[must_use]
    pub fn new(dim: usize) -> Self {
        Self { dim }
    }
}

impl EmbeddingBackend for DeterministicEmbedder {
    fn dim(&self) -> usize {
        self.dim
    }

    fn embed(&self, text: &str) -> CoreResult<Vec<f32>> {
        Ok(deterministic_embed(text, self.dim))
    }
}

/// Convenience: produce a `Box<dyn EmbeddingBackend>` for callers that
/// need a trait object.
#[must_use]
pub fn deterministic_backend(dim: usize) -> Box<dyn EmbeddingBackend> {
    Box::new(DeterministicEmbedder::new(dim))
}

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
