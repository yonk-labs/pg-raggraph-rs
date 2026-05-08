//! `ChunkStrategy` — six documented chunking strategies that map to the
//! `chunkshop-rs` chunker family.
//!
//! Parses from the same lower-snake-case strings the Python `chunkshop`
//! package uses (`auto`, `hierarchy`, `semantic`, `sentence_aware`,
//! `fixed_overlap`, `neighbor_expand`) so a strategy chosen on the Python
//! side stays valid when echoed at the Rust extension.

/// One of the six chunking strategies pg-raggraph supports.
///
/// `Auto` is the project default — it currently maps to the
/// `sentence_aware` chunker (the most reliable model-free chunker in
/// `chunkshop-rs`). The mapping may evolve, but the variant name is
/// stable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ChunkStrategy {
    /// Project default. Currently mapped to `sentence_aware`.
    #[default]
    Auto,
    /// Markdown-heading aware: one chunk per `#`-heading section.
    Hierarchy,
    /// Boundary detection via sentence-embedding similarity drops.
    Semantic,
    /// Pack paragraphs / sentences up to a `max_chars` budget.
    SentenceAware,
    /// Word-level sliding window with overlap.
    FixedOverlap,
    /// Wraps a base chunker; expands each chunk with its `±N` neighbors.
    NeighborExpand,
}

impl ChunkStrategy {
    /// Stable snake-case label for the strategy. Round-trips through
    /// `ChunkStrategy::parse`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            ChunkStrategy::Auto => "auto",
            ChunkStrategy::Hierarchy => "hierarchy",
            ChunkStrategy::Semantic => "semantic",
            ChunkStrategy::SentenceAware => "sentence_aware",
            ChunkStrategy::FixedOverlap => "fixed_overlap",
            ChunkStrategy::NeighborExpand => "neighbor_expand",
        }
    }

    /// Parse a snake-case strategy label. Case-sensitive: `"AUTO"` is rejected.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "auto" => Some(ChunkStrategy::Auto),
            "hierarchy" => Some(ChunkStrategy::Hierarchy),
            "semantic" => Some(ChunkStrategy::Semantic),
            "sentence_aware" => Some(ChunkStrategy::SentenceAware),
            "fixed_overlap" => Some(ChunkStrategy::FixedOverlap),
            "neighbor_expand" => Some(ChunkStrategy::NeighborExpand),
            _ => None,
        }
    }
}
