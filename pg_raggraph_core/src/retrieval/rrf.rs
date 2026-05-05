//! Reciprocal Rank Fusion (RRF) — spec §4 fusion contract.
//!
//! `score = SUM(weight_lane * 1.0 / (k + rk))` summed over each lane
//! appearance of the chunk. `k=60` and equal weights `{vec:1, bm25:1,
//! graph:1}` are the parity-pinned defaults (mission brief SC-005).

use serde::{Deserialize, Serialize};

/// RRF k constant — pinned to 60 for parity with the Python implementation
/// (spec §10, mission brief Constraint "Always" — byte-for-byte semantics).
pub const RRF_K: f64 = 60.0;

/// One lane hit for one chunk.
#[derive(Debug, Clone, Copy)]
pub struct LaneHit<'a> {
    pub id: i64,
    pub lane: &'a str, // "vec" | "bm25" | "graph"
    pub rk: i64,       // 1-indexed rank within the lane
}

/// Per-lane RRF weights. Default = equal weights (1.0 each).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct RrfWeights {
    pub vec: f64,
    pub bm25: f64,
    pub graph: f64,
}

impl Default for RrfWeights {
    fn default() -> Self {
        Self {
            vec: 1.0,
            bm25: 1.0,
            graph: 1.0,
        }
    }
}

impl RrfWeights {
    /// Look up the weight for a given lane name. Unknown lanes return 0.0
    /// (silently ignored — defensive against future-added lanes in JSONB).
    #[must_use]
    pub fn weight_for(&self, lane: &str) -> f64 {
        match lane {
            "vec" => self.vec,
            "bm25" => self.bm25,
            "graph" => self.graph,
            _ => 0.0,
        }
    }
}

/// One scored chunk after fusion.
#[derive(Debug, Clone)]
pub struct ScoredChunk {
    pub id: i64,
    pub score: f64,
}

/// Fuse lane hits into per-chunk RRF scores. Returns descending by score.
///
/// `hit.rk as f64` is allowed to lose precision: ranks are bounded by top-k
/// (typically <= 1000), comfortably within the f64 mantissa (2^53).
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn fuse(hits: &[LaneHit<'_>], weights: &RrfWeights) -> Vec<ScoredChunk> {
    use std::collections::HashMap;
    let mut acc: HashMap<i64, f64> = HashMap::new();
    for hit in hits {
        let w = weights.weight_for(hit.lane);
        let contribution = w / (RRF_K + hit.rk as f64);
        *acc.entry(hit.id).or_insert(0.0) += contribution;
    }
    let mut scored: Vec<ScoredChunk> = acc
        .into_iter()
        .map(|(id, score)| ScoredChunk { id, score })
        .collect();
    scored.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    scored
}
