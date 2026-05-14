//! SC-010: numbered citations [N] in prompt, post-mapped to `chunk_ids` in code.
//! SC-012: token budget enforcement — drop chunks past the budget.

use pg_raggraph_core::llm::prompt::{PromptChunk, build_ask_prompt};

fn pc(id: &str, ord: i32, text: &str, tokens: i32) -> PromptChunk {
    PromptChunk {
        chunk_id: uuid::Uuid::parse_str(id).unwrap(),
        document_id: uuid::Uuid::nil(),
        ord,
        text: text.into(),
        token_count: tokens,
    }
}

#[test]
fn prompt_contains_numbered_blocks_and_no_raw_chunk_ids() {
    let chunks = vec![
        pc(
            "11111111-1111-4111-8111-111111111111",
            0,
            "first chunk text",
            10,
        ),
        pc(
            "22222222-2222-4222-8222-222222222222",
            1,
            "second chunk text",
            12,
        ),
    ];
    let built = build_ask_prompt("question?", &chunks, 4000).expect("budget ok");

    assert!(built.prompt_text.contains("[1]"));
    assert!(built.prompt_text.contains("[2]"));
    assert!(built.prompt_text.contains("first chunk text"));
    assert!(built.prompt_text.contains("second chunk text"));
    // SC-010: LLM must not see raw chunk_ids.
    assert!(
        !built
            .prompt_text
            .contains("11111111-1111-4111-8111-111111111111")
    );
    assert!(
        !built
            .prompt_text
            .contains("22222222-2222-4222-8222-222222222222")
    );

    // The id_map lets the post-mapper resolve [N] -> chunk_id.
    assert_eq!(built.id_map.len(), 2);
    assert_eq!(
        built.id_map[0].to_string(),
        "11111111-1111-4111-8111-111111111111"
    );
}

#[test]
fn drops_chunks_past_budget_in_order() {
    let chunks = vec![
        pc("11111111-1111-4111-8111-111111111111", 0, "a", 1000),
        pc("22222222-2222-4222-8222-222222222222", 1, "b", 1500),
        pc("33333333-3333-4333-8333-333333333333", 2, "c", 2000),
    ];
    // budget = 2400 -> first fits (1000 <= 2400); second would push total to 2500
    // which is > 2400; so only chunk 0 fits.
    let built = build_ask_prompt("q?", &chunks, 2400).unwrap();
    assert_eq!(built.id_map.len(), 1);
    assert!(built.prompt_text.contains("[1]"));
    assert!(!built.prompt_text.contains("[2]"));
    assert!(built.dropped_count == 2);
}

#[test]
fn returns_error_when_first_chunk_exceeds_budget() {
    let chunks = vec![pc("11111111-1111-4111-8111-111111111111", 0, "x", 5000)];
    let err = build_ask_prompt("q?", &chunks, 4000).expect_err("over-budget single chunk");
    assert!(format!("{err}").contains("budget"));
}
