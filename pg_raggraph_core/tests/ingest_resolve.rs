//! SC-014: `pg_trgm` + cosine entity resolution.
//!
//! Merge decision: a candidate must clear BOTH the trigram threshold AND the
//! cosine threshold on its name embedding to merge. Either one alone is
//! insufficient.

use pg_raggraph_core::ingest::pg_client::{EntityRow, FakePgClient};
use pg_raggraph_core::ingest::resolve::resolve_or_insert_entity;
use uuid::Uuid;

fn unit_vec(seed: f32) -> Vec<f32> {
    // Tiny 4-d unit vec for the FakePgClient (production uses 384-d).
    let v = [seed, seed * 0.5, seed * 0.25, seed * 0.125];
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    v.iter().map(|x| x / norm).collect()
}

#[test]
fn merges_high_trgm_high_cosine() {
    let mut client = FakePgClient::default();
    let id_existing = Uuid::new_v4();
    // Pre-seed via canonical state (not the buffer): commit-like injection.
    // "International Business Machines" vs "International Business Machine"
    // produces trgm_sim ~0.879 in the FakePgClient Jaccard implementation,
    // which is above the 0.85 TRGM_MERGE threshold and represents a realistic
    // near-duplicate where a second doc drops the trailing 's'.
    client.entities.push(EntityRow {
        id: id_existing,
        namespace: "default".into(),
        name: "International Business Machines".into(),
        kind: Some("organization".into()),
        name_emb: Some(unit_vec(1.0)),
        description: None,
    });

    let resolved = resolve_or_insert_entity(
        &mut client,
        "default",
        "International Business Machine",
        Some("organization"),
        unit_vec(1.0), // identical embedding -> cosine 1.0
        None,
    )
    .unwrap();

    assert_eq!(resolved, id_existing, "high-similarity should merge");
    assert_eq!(
        client.buffered_entities.len(),
        0,
        "no new entity inserted on merge"
    );
}

#[test]
fn inserts_new_when_no_candidates() {
    let mut client = FakePgClient::default();
    let resolved = resolve_or_insert_entity(
        &mut client,
        "default",
        "BetaCo",
        Some("organization"),
        unit_vec(2.0),
        None,
    )
    .unwrap();
    assert_eq!(client.buffered_entities.len(), 1);
    assert_eq!(client.buffered_entities[0].id, resolved);
    assert_eq!(client.buffered_entities[0].name, "BetaCo");
}

#[test]
fn does_not_merge_when_cosine_low_despite_trgm_high() {
    // SC-014 guard: trigram alone is not enough — embedding must agree too.
    let mut client = FakePgClient::default();
    let id_existing = Uuid::new_v4();
    client.entities.push(EntityRow {
        id: id_existing,
        namespace: "default".into(),
        name: "Mercury".into(),
        kind: None,
        name_emb: Some(unit_vec(1.0)), // planet sense
        description: None,
    });
    // High trgm (same word) but opposite-direction embedding.
    let mut diff_vec = unit_vec(1.0);
    diff_vec[0] = -diff_vec[0];
    diff_vec[1] = -diff_vec[1];
    diff_vec[2] = -diff_vec[2];
    diff_vec[3] = -diff_vec[3]; // fully negated -> cosine = -1.0

    let resolved = resolve_or_insert_entity(
        &mut client,
        "default",
        "Mercury",
        Some("element"),
        diff_vec,
        None,
    )
    .unwrap();

    assert_ne!(resolved, id_existing);
    assert_eq!(
        client.buffered_entities.len(),
        1,
        "new entity inserted because cosine threshold not met"
    );
}
