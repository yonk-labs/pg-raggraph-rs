//! ONNX embedder smoke tests. Skipped when the `onnx` feature is off.

#![cfg(feature = "onnx")]

use pg_raggraph_core::embedding::{EmbeddingBackend, OnnxEmbedder, OnnxEmbedderConfig};

/// Path to the `bge-small-en-v1.5` ONNX model. Tests look in the standard
/// chunkshop `hf_cache` path or skip if the model is absent.
fn model_path() -> Option<std::path::PathBuf> {
    let p = std::env::var("PGRG_TEST_ONNX_MODEL_PATH")
        .ok()
        .map(std::path::PathBuf::from);
    p.filter(|p| p.exists())
}

#[test]
fn onnx_loads_and_embeds_when_model_present() {
    // SC-004 / SC-009: real model produces 384-dim vectors.
    let Some(path) = model_path() else {
        eprintln!("skip: PGRG_TEST_ONNX_MODEL_PATH not set or model missing");
        return;
    };
    let cfg = OnnxEmbedderConfig {
        model_path: path,
        expected_dim: 384,
    };
    let e = OnnxEmbedder::load(&cfg).expect("ONNX load must succeed");
    assert_eq!(e.dim(), 384);
    let v = e.embed("hello world").expect("inference must succeed");
    assert_eq!(v.len(), 384);
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    assert!((norm - 1.0).abs() < 1e-2, "expected ~unit norm, got {norm}");
}

#[test]
fn onnx_dim_mismatch_returns_error() {
    // SC-010: mismatched dimension between override model and pgrg.embed_dim
    // causes a startup error.
    let Some(path) = model_path() else {
        eprintln!("skip: PGRG_TEST_ONNX_MODEL_PATH not set or model missing");
        return;
    };
    let cfg = OnnxEmbedderConfig {
        model_path: path,
        expected_dim: 768,
    };
    let result = OnnxEmbedder::load(&cfg);
    assert!(result.is_err(), "dim mismatch must error at load time");
    let msg = format!("{:?}", result.unwrap_err());
    assert!(
        msg.contains("dim") || msg.contains("dimension"),
        "error message must mention dimension, got: {msg}"
    );
}
