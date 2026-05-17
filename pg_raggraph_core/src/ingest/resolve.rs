//! Entity resolution at ingestion: `pg_trgm` (fuzzy name) + cosine on `name_emb`.
//!
//! SC-014. The resolver decides MERGE vs INSERT for an extracted entity. A
//! merge requires that the candidate clear BOTH thresholds (trigram name
//! similarity AND cosine on the name embedding). Either one alone is
//! insufficient — this guards against name collisions across senses (e.g.,
//! "Mercury" the planet vs the element).
//!
//! Thresholds (`TRGM_MERGE`/`COSINE_MERGE`) are sourced at compile time from
//! `bench/parity/resolution_constants.yaml` via `build.rs` (see
//! `crate::parity_constants`). Changing them requires updating the YAML and
//! the canonical literals in `build.rs` in the same commit (drift = build error).

use std::collections::HashMap;

use uuid::Uuid;

use crate::error::CoreResult;
use crate::ingest::pg_client::{EntityRow, PgClient};
use crate::parity_constants::{COSINE_MERGE, TRGM_MERGE};

const TRGM_CANDIDATE_LIMIT: usize = 8;

/// Resolve an extracted entity to an existing row or insert a new one.
///
/// Decision flow:
///   1. `fuzzy_match_entity(name)` -> up to `TRGM_CANDIDATE_LIMIT` candidates
///      sorted by trigram similarity desc.
///   2. For each candidate with `trgm_similarity >= TRGM_MERGE`, compute
///      cosine on `name_emb` vs `incoming_emb`. If cosine >= `COSINE_MERGE`,
///      MERGE — return the candidate's id.
///   3. If no candidate clears both thresholds, INSERT a new row and return
///      the new id.
///
/// # Errors
/// Returns `CoreError` from the underlying client calls (`fuzzy_match_entity`
/// or `insert_entity`).
pub fn resolve_or_insert_entity(
    client: &mut dyn PgClient,
    namespace: &str,
    name: &str,
    kind: Option<&str>,
    incoming_emb: Vec<f32>,
    description: Option<String>,
) -> CoreResult<Uuid> {
    let cands = client.fuzzy_match_entity(namespace, name, TRGM_CANDIDATE_LIMIT)?;
    for c in &cands {
        if c.trgm_similarity < TRGM_MERGE {
            continue;
        }
        if let Some(cand_emb) = &c.name_emb {
            let cs = cosine(&incoming_emb, cand_emb);
            if cs >= COSINE_MERGE {
                return Ok(c.id);
            }
        }
    }
    let new_id = Uuid::new_v4();
    client.insert_entity(&EntityRow {
        id: new_id,
        namespace: namespace.to_string(),
        name: name.to_string(),
        kind: kind.map(str::to_string),
        name_emb: Some(incoming_emb),
        description,
    })?;
    Ok(new_id)
}

/// Cosine similarity between two equal-length vectors. Returns 0.0 for
/// mismatched lengths or zero-norm inputs (caller treats as "below threshold").
fn cosine(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
    let na: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let nb: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if na == 0.0 || nb == 0.0 {
        0.0
    } else {
        dot / (na * nb)
    }
}

/// Deterministic name embedding for cross-impl resolution parity: a fixed,
/// implementation-independent function (NOT a semantic embedding). Parity
/// here is about the threshold/tie-break LOGIC being identical; both impls
/// compute this same documented function. 8-dim suffices to separate groups.
fn parity_name_vec(name: &str) -> Vec<f32> {
    let norm = name.to_lowercase();
    let mut v = vec![0.0_f32; 8];
    for (i, b) in norm.bytes().enumerate() {
        v[i % 8] += f32::from(b);
    }
    let n: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if n > 0.0 {
        for x in &mut v {
            *x /= n;
        }
    }
    v
}

/// Group entity-name variants into canonical ids using the SAME two-step as
/// the production resolver (trgm >= `TRGM_MERGE` AND cosine >= `COSINE_MERGE`,
/// candidates ordered by trigram similarity desc). Returns name -> canonical.
///
/// This is the cross-implementation parity entry point (SC-005): the Python
/// sibling repo runs the identical YAML thresholds + identical fixture
/// through its resolver and must produce the same partition.
#[must_use]
pub fn resolve_canonical_ids(names: &[&str]) -> HashMap<String, usize> {
    let mut canon: Vec<(String, Vec<f32>)> = Vec::new();
    let mut out: HashMap<String, usize> = HashMap::new();
    for &name in names {
        let emb = parity_name_vec(name);
        let mut cands: Vec<(usize, f32)> = canon
            .iter()
            .enumerate()
            .map(|(idx, (cn, _))| (idx, crate::ingest::pg_client::trgm_sim(name, cn)))
            .collect();
        cands.sort_by(|a, b| b.1.total_cmp(&a.1));
        let mut hit = None;
        for (idx, ts) in cands {
            if ts < TRGM_MERGE {
                break;
            }
            if cosine(&emb, &canon[idx].1) >= COSINE_MERGE {
                hit = Some(idx);
                break;
            }
        }
        let id = if let Some(idx) = hit {
            idx
        } else {
            canon.push((name.to_string(), emb));
            canon.len() - 1
        };
        out.insert(name.to_string(), id);
    }
    out
}

#[cfg(test)]
mod parity_tests {
    use super::*;

    #[test]
    // Exact-equality is intentional: build.rs emits these as exact `f32` literals
    // from resolution_constants.yaml. An epsilon comparison would weaken the
    // SC-005 drift lock — any accumulated round-trip error is a real bug here.
    // The allow is function-scoped (not crate-wide) because clippy::float_cmp
    // fires inside the assert_eq! macro expansion and is not suppressed by a
    // statement attribute.
    #[allow(clippy::float_cmp)]
    fn resolution_constants_sourced_from_shared_yaml() {
        // SC-005: the canonical thresholds must equal the shared parity YAML.
        // The build FAILS before this test can even run if the YAML drifts from
        // the canonical literals (see build.rs DRIFT panic). These asserts are the
        // in-source behavior lock: the resolver still uses exactly 0.85 / 0.90.
        assert_eq!(TRGM_MERGE, 0.85_f32);
        assert_eq!(COSINE_MERGE, 0.90_f32);
    }
}
