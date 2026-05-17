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

#[cfg(test)]
mod parity_tests {
    use super::*;

    #[test]
    fn resolution_constants_sourced_from_shared_yaml() {
        // SC-005: the canonical thresholds must equal the shared parity YAML.
        // The build FAILS before this test can even run if the YAML drifts from
        // the canonical literals (see build.rs DRIFT panic). These asserts are the
        // in-source behavior lock: the resolver still uses exactly 0.85 / 0.90.
        assert_eq!(TRGM_MERGE, 0.85_f32);
        assert_eq!(COSINE_MERGE, 0.90_f32);
    }
}
