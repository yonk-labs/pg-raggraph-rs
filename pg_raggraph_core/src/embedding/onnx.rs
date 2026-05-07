//! Real ONNX-backed embedder for `BAAI/bge-small-en-v1.5`.
//!
//! Loaded once per worker process at startup (SC-009). Resolution of a
//! cache path lives in Task 6 (chunkshop integration); this module always
//! receives an explicit `model_path` from the GUC `pgrg.embed_model_path`
//! or, in tests, the env var `PGRG_TEST_ONNX_MODEL_PATH`.
//!
//! `model_path` is a directory containing `model.onnx` + `tokenizer.json`,
//! matching the layout `HuggingFace`'s optimum exporter and chunkshop
//! both produce.

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use ndarray::Array2;
use ort::session::{Session, builder::SessionBuilder};
use ort::value::{TensorRef, Value};
use tokenizers::Tokenizer;

use crate::embedding::EmbeddingBackend;
use crate::error::{CoreError, CoreResult};

/// Configuration for `OnnxEmbedder::load`.
#[derive(Debug, Clone)]
pub struct OnnxEmbedderConfig {
    /// Path to a directory containing `model.onnx` and `tokenizer.json`.
    pub model_path: PathBuf,
    /// Vector dimension expected by the GUC `pgrg.embed_dim`. Mismatch
    /// against the model's actual output dim is a fatal startup error
    /// (SC-010).
    pub expected_dim: usize,
}

/// ONNX embedder for sentence-transformers-style models (CLS-pooled,
/// L2-normalized).
///
/// The session is wrapped in a `Mutex` because `Session::run` requires
/// `&mut self`. Worker-local sharing is via `Arc<OnnxEmbedder>`; per-call
/// contention inside a single worker is not on the critical path
/// (embedding is the expensive step regardless).
pub struct OnnxEmbedder {
    session: Mutex<Session>,
    tokenizer: Tokenizer,
    dim: usize,
}

impl std::fmt::Debug for OnnxEmbedder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OnnxEmbedder")
            .field("dim", &self.dim)
            .finish_non_exhaustive()
    }
}

impl OnnxEmbedder {
    /// Load the model from disk. Verifies the model's output dim matches
    /// `cfg.expected_dim`. Failure to load (missing files, ONNX session
    /// error, dim mismatch, tokenizer parse error) returns
    /// `CoreError::InvalidConfig`.
    ///
    /// This call is expensive (allocates the ONNX session) and intended
    /// to be called exactly once per worker process at startup.
    pub fn load(cfg: &OnnxEmbedderConfig) -> CoreResult<Self> {
        let dir = &cfg.model_path;
        let model_file = resolve_model_file(dir)?;
        let tokenizer_file = resolve_tokenizer_file(dir)?;

        let tokenizer = Tokenizer::from_file(&tokenizer_file).map_err(|e| {
            CoreError::InvalidConfig(format!(
                "tokenizer load failed for {}: {e}",
                tokenizer_file.display()
            ))
        })?;

        let mut builder = SessionBuilder::new().map_err(|e| {
            CoreError::InvalidConfig(format!("ONNX session builder init failed: {e}"))
        })?;
        let session = builder
            .commit_from_file(&model_file)
            .map_err(|e| CoreError::InvalidConfig(format!("ONNX model load failed: {e}")))?;
        let session_mutex = Mutex::new(session);

        // Probe dim by running on a short input. Cheap and deterministic.
        // We pass `expected_dim` only as a sanity hint; the actual hidden
        // dim is taken from the output tensor shape and validated below.
        let probe_vec = embed_with(&session_mutex, &tokenizer, "probe", cfg.expected_dim)?;
        let actual_dim = probe_vec.len();
        if actual_dim != cfg.expected_dim {
            return Err(CoreError::InvalidConfig(format!(
                "ONNX model output dimension {actual_dim} does not match expected_dim {} \
                 (pgrg.embed_dim). Set pgrg.embed_dim to {actual_dim} or use a different model.",
                cfg.expected_dim
            )));
        }
        Ok(Self {
            session: session_mutex,
            tokenizer,
            dim: actual_dim,
        })
    }
}

/// Internal: tokenize, run inference, CLS-pool, L2-normalize.
///
/// `hidden_hint` is informational only; the true hidden dim comes from
/// the output tensor shape. Free function (not a method) so it can be
/// called during `OnnxEmbedder::load`'s probe step before the struct
/// is constructed.
fn embed_with(
    session: &Mutex<Session>,
    tokenizer: &Tokenizer,
    text: &str,
    _hidden_hint: usize,
) -> CoreResult<Vec<f32>> {
    let enc = tokenizer
        .encode(text, true)
        .map_err(|e| CoreError::InvalidConfig(format!("tokenizer encode failed: {e}")))?;
    let ids: Vec<i64> = enc.get_ids().iter().map(|&u| i64::from(u)).collect();
    let mask: Vec<i64> = enc
        .get_attention_mask()
        .iter()
        .map(|&u| i64::from(u))
        .collect();
    let type_ids: Vec<i64> = enc.get_type_ids().iter().map(|&u| i64::from(u)).collect();

    let seq_len = ids.len();
    let shape = [1_usize, seq_len];
    let ids_arr = Array2::<i64>::from_shape_vec(shape, ids).map_err(|e| {
        CoreError::InvalidConfig(format!("input_ids shape construction failed: {e}"))
    })?;
    let mask_arr = Array2::<i64>::from_shape_vec(shape, mask).map_err(|e| {
        CoreError::InvalidConfig(format!("attention_mask shape construction failed: {e}"))
    })?;
    let type_arr = Array2::<i64>::from_shape_vec(shape, type_ids).map_err(|e| {
        CoreError::InvalidConfig(format!("token_type_ids shape construction failed: {e}"))
    })?;

    let ids_tensor = TensorRef::from_array_view(&ids_arr).map_err(|e| {
        CoreError::InvalidConfig(format!("input_ids tensor construction failed: {e}"))
    })?;
    let mask_tensor = TensorRef::from_array_view(&mask_arr).map_err(|e| {
        CoreError::InvalidConfig(format!("attention_mask tensor construction failed: {e}"))
    })?;
    let type_tensor = TensorRef::from_array_view(&type_arr).map_err(|e| {
        CoreError::InvalidConfig(format!("token_type_ids tensor construction failed: {e}"))
    })?;

    let inputs = ort::inputs![
        "input_ids" => ids_tensor,
        "attention_mask" => mask_tensor,
        "token_type_ids" => type_tensor,
    ];

    let mut session_guard = session
        .lock()
        .map_err(|_| CoreError::InvalidConfig("ONNX session mutex poisoned".to_string()))?;
    let outputs = session_guard
        .run(inputs)
        .map_err(|e| CoreError::InvalidConfig(format!("ONNX inference failed: {e}")))?;

    // bge-small-en exports last_hidden_state at output[0].
    let value: &Value = &outputs[0];
    let (shape_out, data) = value
        .try_extract_tensor::<f32>()
        .map_err(|e| CoreError::InvalidConfig(format!("output tensor extract failed: {e}")))?;
    let dims: Vec<i64> = shape_out.iter().copied().collect();
    if dims.len() != 3 {
        return Err(CoreError::InvalidConfig(format!(
            "expected 3-D output [batch, seq, hidden], got shape {dims:?}"
        )));
    }
    let batch = usize::try_from(dims[0]).unwrap_or(0);
    let seq = usize::try_from(dims[1]).unwrap_or(0);
    let hidden = usize::try_from(dims[2]).unwrap_or(0);
    if batch != 1 || seq != seq_len || hidden == 0 {
        return Err(CoreError::InvalidConfig(format!(
            "unexpected output shape: batch={batch}, seq={seq}, hidden={hidden}, expected_seq={seq_len}"
        )));
    }

    // bge-small-en-v1.5 uses CLS-pooling: take the embedding at token index 0
    // from the last hidden state, then L2-normalize.
    let cls_row = &data[0..hidden];
    let mut pooled: Vec<f32> = cls_row.to_vec();
    let norm: f32 = pooled.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 1e-12 {
        for v in &mut pooled {
            *v /= norm;
        }
    }
    Ok(pooled)
}

impl EmbeddingBackend for OnnxEmbedder {
    fn dim(&self) -> usize {
        self.dim
    }

    fn embed(&self, text: &str) -> CoreResult<Vec<f32>> {
        embed_with(&self.session, &self.tokenizer, text, self.dim)
    }

    // embed_batch falls through to the trait default (serial loop). Real
    // batched ONNX inference would acquire the Mutex once and shape inputs
    // as `[batch, seq_len]` — bench in Plan 6 if hot.
}

/// Resolve the ONNX model file inside `dir`. Accepts a few common names.
fn resolve_model_file(dir: &Path) -> CoreResult<PathBuf> {
    if dir.is_file() {
        return Ok(dir.to_path_buf());
    }
    for candidate in ["model.onnx", "onnx/model.onnx", "model_optimized.onnx"] {
        let p = dir.join(candidate);
        if p.exists() {
            return Ok(p);
        }
    }
    Err(CoreError::InvalidConfig(format!(
        "model.onnx not found under {}",
        dir.display()
    )))
}

/// Resolve the tokenizer file inside `dir`.
fn resolve_tokenizer_file(dir: &Path) -> CoreResult<PathBuf> {
    if dir.is_file() {
        // If the user passed a model file directly, look for tokenizer alongside.
        if let Some(parent) = dir.parent() {
            let p = parent.join("tokenizer.json");
            if p.exists() {
                return Ok(p);
            }
        }
    }
    let p = dir.join("tokenizer.json");
    if p.exists() {
        return Ok(p);
    }
    Err(CoreError::InvalidConfig(format!(
        "tokenizer.json not found under {}",
        dir.display()
    )))
}
