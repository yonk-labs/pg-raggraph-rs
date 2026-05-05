use pg_raggraph_core::embedding::deterministic_embed;

#[test]
fn deterministic_embed_returns_correct_dim() {
    let v = deterministic_embed("hello world", 384);
    assert_eq!(v.len(), 384);
}

#[test]
fn deterministic_embed_is_byte_stable() {
    // SC-002: two consecutive calls on the same input return byte-identical vectors.
    let a = deterministic_embed("hello world", 384);
    let b = deterministic_embed("hello world", 384);
    assert_eq!(a, b);
}

#[test]
fn deterministic_embed_different_inputs_differ() {
    let a = deterministic_embed("hello", 384);
    let b = deterministic_embed("world", 384);
    assert_ne!(a, b);
}

#[test]
fn deterministic_embed_l2_normalized() {
    // Cosine similarity assumes unit norm; produces well-defined comparisons.
    let v = deterministic_embed("hello world", 384);
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    assert!(
        (norm - 1.0).abs() < 1e-5,
        "expected L2 norm ~1.0, got {norm}"
    );
}

#[test]
fn deterministic_embed_respects_dim_parameter() {
    let v_128 = deterministic_embed("test", 128);
    let v_768 = deterministic_embed("test", 768);
    assert_eq!(v_128.len(), 128);
    assert_eq!(v_768.len(), 768);
}
