use pg_raggraph_core::embedding::{DeterministicEmbedder, EmbeddingBackend};

#[test]
fn deterministic_backend_dim_matches_request() {
    let e = DeterministicEmbedder::new(384);
    let v = e.embed("hello").expect("embed must succeed");
    assert_eq!(v.len(), 384);
}

#[test]
fn deterministic_backend_byte_stable() {
    let e = DeterministicEmbedder::new(384);
    let a = e.embed("hello").unwrap();
    let b = e.embed("hello").unwrap();
    assert_eq!(a, b);
}

#[test]
fn deterministic_backend_dim_query_matches_constructor() {
    let e = DeterministicEmbedder::new(768);
    assert_eq!(e.dim(), 768);
}

#[test]
fn deterministic_backend_batch_returns_one_per_input() {
    let e = DeterministicEmbedder::new(384);
    let batch = e.embed_batch(&["a", "b", "c"]).unwrap();
    assert_eq!(batch.len(), 3);
    for v in &batch {
        assert_eq!(v.len(), 384);
    }
}
