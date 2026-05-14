//! SC-001 (Ollama): `/api/generate` with `format: "json"` returns a JSON-string
//! response that parses into Extraction.

use pg_raggraph_core::llm::LlmProvider;
use pg_raggraph_core::llm::ollama::OllamaProvider;

#[test]
fn parses_generate_response_into_extraction() {
    let mut srv = mockito::Server::new();
    let extraction_json = serde_json::json!({
        "entities": [
            {"name": "Carol", "kind": "person", "confidence": 0.8}
        ],
        "relationships": []
    });
    let body = serde_json::json!({
        "model": "llama3:8b",
        "response": extraction_json.to_string(),
        "done": true,
        "context": [1, 2, 3],
        "total_duration": 5_000_000,
        "eval_count": 30
    });
    let _m = srv
        .mock("POST", "/api/generate")
        .with_status(200)
        .with_body(body.to_string())
        .create();

    let provider = OllamaProvider::new("llama3:8b", srv.url());
    let result = provider.extract("Carol writes poetry.", "default").unwrap();
    assert_eq!(result.entities.len(), 1);
    assert_eq!(result.entities[0].name, "Carol");
    assert_eq!(result.relationships.len(), 0);
}

#[test]
fn maps_500_to_retryable_http_error() {
    let mut srv = mockito::Server::new();
    let _m = srv
        .mock("POST", "/api/generate")
        .with_status(500)
        .with_body("internal error")
        .create();
    let provider = OllamaProvider::new("llama3:8b", srv.url());
    let err = provider.extract("x", "default").expect_err("500");
    assert!(format!("{err}").starts_with("http error"));
}
