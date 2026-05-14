//! SC-001 (Anthropic): Messages API with forced tool use returns
//! `content[0].input` as the structured Extraction.

use pg_raggraph_core::llm::LlmProvider;
use pg_raggraph_core::llm::anthropic::AnthropicProvider;

#[test]
fn extracts_via_forced_tool_use() {
    let mut srv = mockito::Server::new();
    let body = serde_json::json!({
        "id": "msg_test",
        "type": "message",
        "role": "assistant",
        "content": [{
            "type": "tool_use",
            "id": "tu_1",
            "name": "extract_graph",
            "input": {
                "entities": [
                    {"name": "Bob", "kind": "person", "confidence": 0.9},
                    {"name": "BetaCo", "kind": "organization", "confidence": 0.85}
                ],
                "relationships": [
                    {"src_name": "Bob", "dst_name": "BetaCo", "kind": "founded",
                     "weight": 1.0, "confidence": 0.88}
                ]
            }
        }],
        "model": "claude-3-5-haiku",
        "stop_reason": "tool_use",
        "usage": {"input_tokens": 80, "output_tokens": 40}
    });

    let _m = srv
        .mock("POST", "/v1/messages")
        .match_header("x-api-key", "sk-ant-test")
        .with_status(200)
        .with_body(body.to_string())
        .create();

    let provider = AnthropicProvider::new("sk-ant-test", "claude-3-5-haiku", srv.url());
    let result = provider.extract("Bob founded BetaCo.", "default").unwrap();
    assert_eq!(result.entities.len(), 2);
    assert_eq!(result.relationships[0].kind, "founded");
}

#[test]
fn maps_529_overloaded_to_retryable_http_error() {
    let mut srv = mockito::Server::new();
    let _m = srv
        .mock("POST", "/v1/messages")
        .with_status(529)
        .with_body("overloaded")
        .create();
    let provider = AnthropicProvider::new("sk-ant-test", "claude-3-5-haiku", srv.url());
    let err = provider.extract("x", "default").expect_err("529");
    assert!(format!("{err}").starts_with("http error"), "got {err:?}");
}

#[test]
fn complete_returns_text_and_usage() {
    let mut srv = mockito::Server::new();
    let body = serde_json::json!({
        "id": "msg_test_complete",
        "type": "message",
        "role": "assistant",
        "content": [
            {"type": "text", "text": "The answer is [1]."}
        ],
        "model": "claude-3-5-haiku",
        "stop_reason": "end_turn",
        "usage": {"input_tokens": 35, "output_tokens": 11}
    });
    let _m = srv
        .mock("POST", "/v1/messages")
        .match_header("x-api-key", "sk-ant-test")
        .with_status(200)
        .with_body(body.to_string())
        .create();

    let provider = AnthropicProvider::new("sk-ant-test", "claude-3-5-haiku", srv.url());
    let r = provider.complete("Question?").unwrap();
    assert_eq!(r.text, "The answer is [1].");
    assert_eq!(r.prompt_tokens, 35);
    assert_eq!(r.completion_tokens, 11);
}
