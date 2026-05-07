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

/// ONNX embedder for sentence-transformers-style models (mean-pooled,
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

        // Probe dim by running on a 1-token input. Cheap and deterministic.
        let mut probe = Self {
            session: Mutex::new(session),
            tokenizer,
            // Filled after probe.
            dim: 0,
        };
        let probe_vec = probe.embed_with(&probe.session, "x")?;
        let actual_dim = probe_vec.len();
        if actual_dim != cfg.expected_dim {
            return Err(CoreError::InvalidConfig(format!(
                "ONNX model output dimension {actual_dim} does not match expected_dim {} \
                 (pgrg.embed_dim). Set pgrg.embed_dim to {actual_dim} or use a different model.",
                cfg.expected_dim
            )));
        }
        probe.dim = actual_dim;
        Ok(probe)
    }

    /// Internal: tokenize, run inference, mean-pool, L2-normalize.
    /// Works even before `self.dim` is set, since output dim is taken
    /// from the result tensor shape.
    fn embed_with(&self, session: &Mutex<Session>, text: &str) -> CoreResult<Vec<f32>> {
        let enc = self
            .tokenizer
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
        let mask_arr = Array2::<i64>::from_shape_vec(shape, mask.clone()).map_err(|e| {
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

        // Mean-pool with attention mask, then L2-normalize.
        let mut pooled = vec![0.0_f32; hidden];
        let mut total_weight = 0.0_f32;
        for (token_idx, &m) in mask.iter().enumerate() {
            if m == 0 {
                continue;
            }
            let row_start = token_idx * hidden;
            for h in 0..hidden {
                pooled[h] += data[row_start + h];
            }
            total_weight += 1.0;
        }
        if total_weight > 0.0 {
            for v in &mut pooled {
                *v /= total_weight;
            }
        }
        let norm: f32 = pooled.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 1e-12 {
            for v in &mut pooled {
                *v /= norm;
            }
        }
        Ok(pooled)
    }
}

impl EmbeddingBackend for OnnxEmbedder {
    fn dim(&self) -> usize {
        self.dim
    }

    fn embed(&self, text: &str) -> CoreResult<Vec<f32>> {
        self.embed_with(&self.session, text)
    }
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
