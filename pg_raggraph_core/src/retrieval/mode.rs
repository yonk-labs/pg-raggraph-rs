//! Retrieval mode parameter (ablation knobs).
//!
//! Per spec §4 and mission brief SC-004:
//! - `Hybrid` (default) fuses all four lanes (vector, bm25, graph, metadata predicate).
//! - `Vector`, `Bm25`, `Graph` force a single-lane query for benchmarking.
//!
//! Per mission brief Constraints "Never": no smart-mode, no `naive_boost`,
//! no local/global modes. Those are spec §11 out of scope for v1.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Mode {
    /// Default: fuse vector + BM25 + graph under RRF (k=60) with metadata predicate inside each lane.
    #[default]
    Hybrid,
    /// Vector lane only (pgvector cosine).
    Vector,
    /// BM25 lane only (`ts_rank_cd`).
    Bm25,
    /// Graph lane only (recursive-CTE walk from entity seeds).
    Graph,
}

impl Mode {
    /// Stable string identifier for SQL parameter passing.
    /// Matches the `mode text` SQL parameter values in `pgrg.query`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Mode::Hybrid => "hybrid",
            Mode::Vector => "vector",
            Mode::Bm25 => "bm25",
            Mode::Graph => "graph",
        }
    }

    /// Parse a mode from its SQL string identifier. Case-sensitive
    /// (matches the documented SQL surface). Unknown -> None.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "hybrid" => Some(Mode::Hybrid),
            "vector" => Some(Mode::Vector),
            "bm25" => Some(Mode::Bm25),
            "graph" => Some(Mode::Graph),
            _ => None,
        }
    }

    /// Returns true iff this mode includes the vector lane in fusion.
    #[must_use]
    pub const fn uses_vector(self) -> bool {
        matches!(self, Mode::Hybrid | Mode::Vector)
    }

    /// Returns true iff this mode includes the BM25 lane in fusion.
    #[must_use]
    pub const fn uses_bm25(self) -> bool {
        matches!(self, Mode::Hybrid | Mode::Bm25)
    }

    /// Returns true iff this mode includes the graph lane in fusion.
    #[must_use]
    pub const fn uses_graph(self) -> bool {
        matches!(self, Mode::Hybrid | Mode::Graph)
    }
}
