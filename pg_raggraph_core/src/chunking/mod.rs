//! Chunking shim — pg-raggraph's only entry point to `chunkshop-rs`.
//!
//! Plan 3 / Task 6 (SC-008). The project constitution forbids hand-rolling a
//! chunker; every code path that produces chunks goes through this module.
//!
//! # `chunkshop-rs` 0.3.x API surface used here
//!
//! `chunkshop-rs` exposes per-strategy chunker types (no single
//! `chunkshop::chunk(text, Config)` free function). Each chunker is constructed
//! from its own `*ChunkerConfig` struct and implements
//! `chunk(&Document) -> Vec<chunkshop::Chunk>`. We translate our
//! `ChunkStrategy` to the matching chunker, run it, and convert the resulting
//! `chunkshop::Chunk { original_content, embedded_content, seq_num, .. }` to
//! our slimmer [`Chunk`] DTO.
//!
//! # Strategy mapping
//!
//! | `ChunkStrategy`   | `chunkshop-rs` chunker                       |
//! |-------------------|----------------------------------------------|
//! | `Auto`            | `SentenceAwareChunker` (default, model-free) |
//! | `SentenceAware`   | `SentenceAwareChunker`                       |
//! | `Hierarchy`       | `HierarchyChunker`                           |
//! | `FixedOverlap`    | `FixedOverlapChunker`                        |
//! | `NeighborExpand`  | `NeighborExpandChunker` over `SentenceAware` |
//! | `Semantic`        | not yet wired (loads a fastembed boundary    |
//! |                   | model — out of scope for the queued worker)  |
//!
//! # Token-count derivation
//!
//! `chunkshop::Chunk` carries no token count; we derive one with whitespace
//! word count (`text.split_whitespace().count()`) — a stable, model-free
//! proxy that's positive for any non-empty chunk and matches how
//! `fixed_overlap` already thinks about chunk size.

mod strategy;

pub use strategy::ChunkStrategy;

use chunkshop::chunker::{
    ChunkerImpl, FixedOverlapChunker, HierarchyChunker, NeighborExpandChunker, SentenceAwareChunker,
};
use chunkshop::config::{
    FixedOverlapChunkerConfig, HierarchyChunkerConfig, SentenceAwareChunkerConfig,
};
use chunkshop::source::Document;
use serde_json::json;

use crate::error::{CoreError, CoreResult};

/// One chunk produced by the [`Chunker`].
///
/// Slimmer than `chunkshop::Chunk` — pg-raggraph stores `(ord, text,
/// token_count)` per row, and the embedded vs. original distinction is a
/// chunkshop-internal trace we collapse to a single `text` field (we use
/// chunkshop's `embedded_content`, which is what gets vectorized).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunk {
    /// 0-based ordinal within the source document.
    pub ord: i32,
    /// Chunk text (chunkshop's `embedded_content`).
    pub text: String,
    /// Whitespace-word count of `text`. Always positive for non-empty chunks.
    pub token_count: i32,
}

/// Thin facade over the `chunkshop-rs` chunker family.
///
/// One [`Chunker`] is bound to one [`ChunkStrategy`]. Construction is cheap
/// for every strategy except `Semantic` (which would load a fastembed
/// boundary model — currently rejected at `chunk()` time with
/// [`CoreError::InvalidConfig`]).
pub struct Chunker {
    strategy: ChunkStrategy,
}

impl Chunker {
    /// Build a chunker for `strategy`. No I/O, no model loads.
    #[must_use]
    pub fn new(strategy: ChunkStrategy) -> Self {
        Self { strategy }
    }

    /// Chunk `text`. The shim produces an in-memory `chunkshop::Document` so
    /// every chunkshop chunker (which requires a `Document`, not a raw `&str`)
    /// can consume it uniformly.
    pub fn chunk(&self, text: &str) -> CoreResult<Vec<Chunk>> {
        let doc = Document {
            id: String::new(),
            content: text.to_string(),
            title: None,
            metadata: json!({}),
        };
        let chunker: Box<dyn ChunkerImpl + Send + Sync> = match self.strategy {
            ChunkStrategy::Auto | ChunkStrategy::SentenceAware => {
                Box::new(SentenceAwareChunker::new(SentenceAwareChunkerConfig {
                    doc_type: "prose".to_string(),
                    max_chars: 2000,
                    min_chars: 200,
                    if_oversize: None,
                }))
            }
            ChunkStrategy::Hierarchy => Box::new(HierarchyChunker::new(HierarchyChunkerConfig {
                prefix_heading: true,
                min_section_chars: 100,
                max_chars: 2000,
                if_oversize: None,
            })),
            ChunkStrategy::FixedOverlap => Box::new(
                FixedOverlapChunker::new(FixedOverlapChunkerConfig {
                    window_words: 300,
                    step_words: 150,
                    max_chars: None,
                    if_oversize: None,
                })
                .map_err(|e| {
                    CoreError::InvalidConfig(format!("fixed_overlap construction failed: {e}"))
                })?,
            ),
            ChunkStrategy::NeighborExpand => {
                // NeighborExpandChunker wraps a base chunker — we wrap
                // SentenceAware (the same backing chunker as `auto`) and
                // pass the window directly to the constructor. The matching
                // `NeighborExpandChunkerConfig` exists in chunkshop but is
                // only consumed by chunkshop's own YAML runner; the
                // chunker constructor takes the resolved values directly.
                let base: Box<dyn ChunkerImpl + Send + Sync> =
                    Box::new(SentenceAwareChunker::new(SentenceAwareChunkerConfig {
                        doc_type: "prose".to_string(),
                        max_chars: 2000,
                        min_chars: 200,
                        if_oversize: None,
                    }));
                Box::new(NeighborExpandChunker::new(1, base, Some(2000), None))
            }
            ChunkStrategy::Semantic => {
                return Err(CoreError::InvalidConfig(
                    "strategy `semantic` requires a fastembed boundary-model load and \
                     is not yet wired in pg_raggraph_core; use `auto` or \
                     `sentence_aware` for the queued worker"
                        .to_string(),
                ));
            }
        };

        let raw = chunker.chunk(&doc);
        Ok(raw
            .into_iter()
            .map(|c| {
                let token_count = c.embedded_content.split_whitespace().count();
                Chunk {
                    // chunkshop emits seq_num: usize; cap to i32 for our row schema.
                    ord: i32::try_from(c.seq_num).unwrap_or(i32::MAX),
                    text: c.embedded_content,
                    token_count: i32::try_from(token_count).unwrap_or(i32::MAX),
                }
            })
            .collect())
    }
}
