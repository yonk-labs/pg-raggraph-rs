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

#[test]
fn mock_provider_complete_returns_stub_answer() {
    use pg_raggraph_core::llm::{LlmProvider, MockProvider};

    let p = MockProvider::default().with_stub_answer("hello world");
    let r = p.complete("anything").unwrap();
    assert_eq!(r.text, "hello world");
    assert_eq!(r.prompt_tokens, 0);
    assert_eq!(r.completion_tokens, 0);
}

#[test]
fn mock_provider_complete_default_is_empty() {
    use pg_raggraph_core::llm::{LlmProvider, MockProvider};

    let p = MockProvider::new();
    let r = p.complete("anything").unwrap();
    assert_eq!(r.text, "");
}

#[test]
fn mock_provider_extract_still_empty_after_stub_answer() {
    use pg_raggraph_core::llm::{LlmProvider, MockProvider};

    let p = MockProvider::default().with_stub_answer("ignore me for extract");
    let r = p.extract("alice works at acme", "default").unwrap();
    assert!(r.entities.is_empty());
    assert!(r.relationships.is_empty());
}

#[test]
fn mock_provider_extract_returns_stub_extraction() {
    use pg_raggraph_core::llm::{
        ExtractedEntity, ExtractedRelationship, Extraction, LlmProvider, MockProvider,
    };

    let stub = Extraction {
        entities: vec![ExtractedEntity {
            name: "Alice".into(),
            kind: Some("person".into()),
            description: None,
            confidence: 0.9,
        }],
        relationships: vec![ExtractedRelationship {
            src_name: "Alice".into(),
            dst_name: "Acme".into(),
            kind: "works_at".into(),
            weight: 1.0,
            confidence: 0.9,
        }],
    };
    let p = MockProvider::default().with_stub_extraction(stub);
    let r = p.extract("anything", "default").unwrap();
    assert_eq!(r.entities.len(), 1);
    assert_eq!(r.entities[0].name, "Alice");
    assert_eq!(r.relationships.len(), 1);
    assert_eq!(r.relationships[0].kind, "works_at");
}
