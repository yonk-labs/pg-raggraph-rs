use pg_raggraph_core::llm::{Extraction, LlmProvider, MockProvider};

#[test]
fn mock_provider_returns_empty_extraction() {
    let p = MockProvider::new();
    let result: Extraction = p
        .extract("any chunk text", "any namespace")
        .expect("mock must succeed");
    assert!(
        result.entities.is_empty(),
        "MockProvider entities must be empty"
    );
    assert!(
        result.relationships.is_empty(),
        "MockProvider relationships must be empty"
    );
}

#[test]
fn mock_provider_is_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<MockProvider>();
}

#[test]
fn provider_trait_object_safe() {
    let _p: Box<dyn LlmProvider> = Box::new(MockProvider::new());
}
