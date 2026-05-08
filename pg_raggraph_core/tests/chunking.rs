//! Integration tests for the `chunking` module — the chunkshop shim.
//!
//! Plan 3 / Task 6 (SC-008): verify the `ChunkStrategy` enum and the `Chunker`
//! shim that delegates to `chunkshop-rs`. The public API exercised here is the
//! contract Tasks 7, 10, and 11 will consume verbatim.

use pg_raggraph_core::chunking::{ChunkStrategy, Chunker};

#[test]
fn strategy_parses_documented_values() {
    assert_eq!(ChunkStrategy::parse("auto"), Some(ChunkStrategy::Auto));
    assert_eq!(
        ChunkStrategy::parse("hierarchy"),
        Some(ChunkStrategy::Hierarchy)
    );
    assert_eq!(
        ChunkStrategy::parse("semantic"),
        Some(ChunkStrategy::Semantic)
    );
    assert_eq!(
        ChunkStrategy::parse("sentence_aware"),
        Some(ChunkStrategy::SentenceAware)
    );
    assert_eq!(
        ChunkStrategy::parse("fixed_overlap"),
        Some(ChunkStrategy::FixedOverlap)
    );
    assert_eq!(
        ChunkStrategy::parse("neighbor_expand"),
        Some(ChunkStrategy::NeighborExpand)
    );
}

#[test]
fn strategy_unknown_returns_none() {
    assert_eq!(ChunkStrategy::parse("rolling"), None);
    assert_eq!(ChunkStrategy::parse(""), None);
    assert_eq!(ChunkStrategy::parse("AUTO"), None);
}

#[test]
fn strategy_default_is_auto() {
    assert_eq!(ChunkStrategy::default(), ChunkStrategy::Auto);
}

#[test]
fn chunker_yields_at_least_one_chunk_for_nonempty_input() {
    let c = Chunker::new(ChunkStrategy::Auto);
    let chunks = c.chunk("hello world").expect("must chunk");
    assert!(!chunks.is_empty());
    let total: String = chunks.iter().map(|c| c.text.as_str()).collect();
    assert!(total.contains("hello"));
}

#[test]
fn chunker_preserves_token_count_field() {
    let c = Chunker::new(ChunkStrategy::Auto);
    let chunks = c
        .chunk("the quick brown fox jumps over the lazy dog")
        .unwrap();
    for chunk in &chunks {
        assert!(chunk.token_count > 0, "token_count must be positive");
    }
}
