use pg_raggraph_sidecar::embedder::build_embedder;

#[test]
fn embedder_builds_with_configured_dim() {
    let e = build_embedder(384, None).expect("embedder");
    // `embed_batch` is the EmbeddingBackend trait method (default impl loops
    // `embed`). One call with one input → one vector of length 384.
    let v = e.embed_batch(&["hello"]).expect("embed");
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].len(), 384);
}
