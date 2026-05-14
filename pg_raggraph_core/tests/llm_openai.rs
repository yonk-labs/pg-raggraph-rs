//! SC-001 (OpenAI): cassette test — mockito-served chat-completions JSON
//! produces a parsed Extraction.

use pg_raggraph_core::llm::LlmProvider;
use pg_raggraph_core::llm::openai::OpenAiProvider;

#[test]
fn extracts_entities_and_relationships_from_chat_completion() {
    let mut srv = mockito::Server::new();
    let body = serde_json::json!({
        "id": "chatcmpl-test",
        "choices": [{
            "message": {
                "role": "assistant",
                "content": serde_json::json!({
                    "entities": [
                        {"name": "Alice", "kind": "person", "confidence": 0.95},
                        {"name": "Acme Corp", "kind": "organization", "confidence": 0.92}
                    ],
                    "relationships": [
                        {"src_name": "Alice", "dst_name": "Acme Corp",
                         "kind": "works_at", "weight": 1.0, "confidence": 0.9}
                    ]
                }).to_string()
            }
        }],
        "usage": {"prompt_tokens": 100, "completion_tokens": 50, "total_tokens": 150}
    });

    let m = srv
        .mock("POST", "/v1/chat/completions")
        .match_header("authorization", "Bearer sk-test")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(body.to_string())
        .create();

    let provider = OpenAiProvider::new("sk-test", "gpt-4o-mini", srv.url());
    let result = provider
        .extract("Alice works at Acme Corp.", "default")
        .unwrap();
    m.assert();

    assert_eq!(result.entities.len(), 2);
    assert_eq!(result.entities[0].name, "Alice");
    assert_eq!(result.relationships.len(), 1);
    assert_eq!(result.relationships[0].kind, "works_at");
}

#[test]
fn maps_429_to_retryable_http_error() {
    let mut srv = mockito::Server::new();
    let _m = srv
        .mock("POST", "/v1/chat/completions")
        .with_status(429)
        .with_body("rate limit")
        .create();

    let provider = OpenAiProvider::new("sk-test", "gpt-4o-mini", srv.url());
    let err = provider.extract("x", "default").expect_err("429");
    let msg = format!("{err}");
    // Must classify as CoreError::Http so RetryingProvider treats it as retryable.
    assert!(msg.starts_with("http error"), "got: {msg}");
}

#[test]
fn maps_400_to_permanent_llm_error() {
    let mut srv = mockito::Server::new();
    let _m = srv
        .mock("POST", "/v1/chat/completions")
        .with_status(400)
        .with_body(serde_json::json!({"error": {"message": "bad request"}}).to_string())
        .create();

    let provider = OpenAiProvider::new("sk-test", "gpt-4o-mini", srv.url());
    let err = provider.extract("x", "default").expect_err("400");
    let msg = format!("{err}");
    assert!(msg.starts_with("llm error"), "got: {msg}");
}

#[test]
fn complete_returns_text_and_usage() {
    let mut srv = mockito::Server::new();
    let body = serde_json::json!({
        "id": "chatcmpl-test-complete",
        "choices": [{
            "message": {
                "role": "assistant",
                "content": "The answer is [1]."
            }
        }],
        "usage": {"prompt_tokens": 42, "completion_tokens": 7, "total_tokens": 49}
    });
    let _m = srv
        .mock("POST", "/v1/chat/completions")
        .match_header("authorization", "Bearer sk-test")
        .with_status(200)
        .with_body(body.to_string())
        .create();

    let provider = OpenAiProvider::new("sk-test", "gpt-4o-mini", srv.url());
    let r = provider.complete("Question?").unwrap();
    assert_eq!(r.text, "The answer is [1].");
    assert_eq!(r.prompt_tokens, 42);
    assert_eq!(r.completion_tokens, 7);
}
