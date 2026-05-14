//! SC-009/010/016/018 integration: `ask()` orchestrator behavior.
//!
//! Uses `MockProvider` (no real LLM) configured via `with_stub_answer` to
//! produce deterministic responses with known `[N]` citation markers.

use pg_raggraph_core::llm::MockProvider;
use pg_raggraph_core::llm::ask::{AskRequest, ask};
use pg_raggraph_core::llm::prompt::PromptChunk;
use uuid::Uuid;

fn pc(uuid_str: &str, ord: i32, text: &str, tokens: i32) -> PromptChunk {
    PromptChunk {
        chunk_id: Uuid::parse_str(uuid_str).unwrap(),
        document_id: Uuid::nil(),
        ord,
        text: text.into(),
        token_count: tokens,
    }
}

#[test]
fn ask_produces_answer_and_citations_subset() {
    // SC-009: non-empty answer + citations.
    // SC-010: every citation references a chunk that was in the prompt.
    // SC-018: signals.llm carries provider/model/latency/token attribution.

    let chunks = vec![
        pc(
            "11111111-1111-4111-8111-111111111111",
            0,
            "the auth module verifies user credentials",
            10,
        ),
        pc(
            "22222222-2222-4222-8222-222222222222",
            1,
            "session tokens expire after 24 hours",
            8,
        ),
    ];
    let provider = MockProvider::default()
        .with_stub_answer("Auth verifies credentials [1]. Tokens expire after 24h [2].");
    let req = AskRequest {
        question: "what does auth do?".into(),
        chunks,
        provider: "mock".into(),
        model: "mock-v1".into(),
        token_budget: 4000,
    };
    let out = ask(&req, &provider).unwrap();
    assert!(!out.answer.is_empty(), "answer must be non-empty");
    assert_eq!(out.citations.len(), 2, "expected 2 citations");
    assert_eq!(
        out.citations[0].chunk_id.to_string(),
        "11111111-1111-4111-8111-111111111111"
    );
    assert_eq!(
        out.citations[1].chunk_id.to_string(),
        "22222222-2222-4222-8222-222222222222"
    );
    assert_eq!(out.mode_used, "hybrid");

    // SC-018: signals shape
    let llm = out.signals.get("llm").expect("signals.llm");
    assert_eq!(llm["provider"], "mock");
    assert_eq!(llm["model"], "mock-v1");
    assert!(
        llm["latency_ms"].as_u64().is_some(),
        "latency_ms must be u64"
    );
    assert!(llm["prompt_tokens"].as_u64().is_some());
    assert!(llm["completion_tokens"].as_u64().is_some());

    let retrieval = out.signals.get("retrieval").expect("signals.retrieval");
    assert_eq!(retrieval["chunks_in_prompt"], 2);
    assert_eq!(retrieval["dropped_for_budget"], 0);
}

#[test]
fn ask_drops_forged_citations() {
    // SC-010 forgery guarantee: if the LLM emits [99] (or any N outside the
    // prompt's id_map), it must NOT resolve to a real chunk_id.

    let chunks = vec![pc(
        "11111111-1111-4111-8111-111111111111",
        0,
        "real context",
        5,
    )];
    let provider = MockProvider::default().with_stub_answer("fake claim [99].");
    let req = AskRequest {
        question: "q?".into(),
        chunks,
        provider: "mock".into(),
        model: "mock-v1".into(),
        token_budget: 4000,
    };
    let out = ask(&req, &provider).unwrap();
    assert_eq!(
        out.citations.len(),
        0,
        "forged [99] must not produce a citation"
    );
    // Answer text itself is preserved verbatim (the LLM's text is returned
    // as-is — only the citations JSON is filtered).
    assert_eq!(out.answer, "fake claim [99].");
}

#[test]
fn ask_records_dropped_chunks_in_signals() {
    // SC-012 (verified via prompt builder; ask propagates the dropped count).
    let chunks = vec![
        pc("11111111-1111-4111-8111-111111111111", 0, "a", 1000),
        pc("22222222-2222-4222-8222-222222222222", 1, "b", 1500),
        pc("33333333-3333-4333-8333-333333333333", 2, "c", 2000),
    ];
    // budget 2400 -> only chunk 0 fits (1000 <= 2400, 1000+1500=2500 > 2400)
    let provider = MockProvider::default().with_stub_answer("ok [1].");
    let req = AskRequest {
        question: "q?".into(),
        chunks,
        provider: "mock".into(),
        model: "mock-v1".into(),
        token_budget: 2400,
    };
    let out = ask(&req, &provider).unwrap();
    assert_eq!(out.signals["retrieval"]["chunks_in_prompt"], 1);
    assert_eq!(out.signals["retrieval"]["dropped_for_budget"], 2);
    assert_eq!(out.citations.len(), 1);
}
