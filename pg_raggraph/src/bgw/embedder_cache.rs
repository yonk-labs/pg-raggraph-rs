//! Per-worker embedder cache — load the embedding backend once at worker
//! startup (SC-009).
//!
//! Plan 3 builds:
//!   - In `cfg(any(test, feature = "pg_test"))`: `DeterministicEmbedder`
//!     (Plan 2 fixture stability — byte-identical embeddings across runs).
//!   - Otherwise: `DeterministicEmbedder` for now. Plan 4/5 will add an
//!     `onnx` feature on the `pg_raggraph` crate that forwards to
//!     `pg_raggraph_core/onnx` and threads `pgrg.embed_model_path` /
//!     chunkshop's `hf_cache` resolver into a real `OnnxEmbedder`.
//!
//! The function is invoked once per worker process before the poll loop,
//! and the resulting `Arc<dyn EmbeddingBackend>` is shared by reference
//! across every job iteration.

use std::sync::Arc;

use pg_raggraph_core::embedding::{DeterministicEmbedder, EmbeddingBackend};

/// Build the worker's embedding backend at startup.
///
/// Returns `Arc<dyn EmbeddingBackend>` so the bg worker can pass `&*backend`
/// into `run_job` cheaply across many job iterations.
pub(crate) fn build_backend() -> Arc<dyn EmbeddingBackend> {
    // `EMBED_DIM` is an i32 GUC bounded `[64, 4096]` (see `gucs.rs`); always
    // non-negative. `try_from` keeps clippy happy without an `as` cast.
    let dim_i32 = crate::gucs::EMBED_DIM.get();
    let dim: usize = usize::try_from(dim_i32).unwrap_or(384);

    // Test builds (cargo test, pgrx pg_test) use the deterministic embedder
    // so Plan 2 fixtures stay byte-stable. Non-test builds also use it
    // for now; Plan 4/5 will add a real ONNX path behind a `pg_raggraph/onnx`
    // feature flag.
    Arc::new(DeterministicEmbedder::new(dim))
}
