//! Embedding backend, loaded ONCE at sidecar startup (SC-014) and shared
//! across all worker tasks via `Arc` — same lifecycle as the Plan 3 bg worker.

use std::sync::Arc;

use pg_raggraph_core::embedding::{DeterministicEmbedder, EmbeddingBackend};

/// Build the embedding backend from sidecar config. Mirrors the Plan 3
/// bg-worker selection (`DeterministicEmbedder` unless an ONNX model path is
/// configured and the `onnx` feature is built — ONNX wiring is a documented
/// carry-forward, not Plan 5 scope).
///
/// The `embed_dim` parameter maps directly to `pgrg.embed_dim`. The
/// `_embed_model_path` argument is accepted but unused; it is the hook for
/// future ONNX wiring without a signature change.
///
/// # Errors
/// Currently infallible for the deterministic backend. Returns `anyhow::Error`
/// so callers are forward-compatible with the ONNX path (which can fail on
/// invalid model paths or session initialisation errors).
pub fn build_embedder(
    embed_dim: i32,
    _embed_model_path: Option<&str>,
) -> anyhow::Result<Arc<dyn EmbeddingBackend>> {
    // Mirror the bg-worker dim-defaulting exactly:
    // `try_from` keeps clippy happy without an `as` cast.
    // `embed_dim` is a GUC bounded [64, 4096] so `unwrap_or` is a safety net.
    let dim: usize = usize::try_from(embed_dim).unwrap_or(384);
    Ok(Arc::new(DeterministicEmbedder::new(dim)))
}
