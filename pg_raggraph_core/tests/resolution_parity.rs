//! SC-005 — cross-impl resolution parity. Reads the SAME shared
//! `resolution_constants.yaml` the Python side reads, drives a fixed set of
//! entity-name variants through the resolver, and asserts the canonical
//! grouping is exactly the expected partition. DC-003 requires this set to
//! be >= 50 variants with ZERO drift before acceptance.
//!
//! Python mirror (sibling repo `yonk-labs/pg-raggraph`): a pytest reads the
//! identical YAML + identical variant fixture and asserts the identical
//! partition. The fixture below is the shared contract.

use pg_raggraph_core::ingest::resolve::resolve_canonical_ids;
use pg_raggraph_core::parity_constants::{COSINE_MERGE, TRGM_MERGE};

/// `(variant_name, expected_canonical_group)`. 50 variants, 39 groups.
///
/// The expected partition below is the TRUE deterministic behavior of the
/// shared two-step resolver (trgm-set-Jaccard analog of `pg_trgm` >= 0.85
/// AND `parity_name_vec` cosine >= 0.90, trgm-desc tie-break). Under the
/// `pg_trgm`-style padded trigram-set Jaccard, only variants that differ by
/// *case alone* (or are otherwise trigram-near-identical) clear the 0.85
/// bar; whitespace/punctuation/affix differences (e.g. `"Postgres"` vs
/// `"Postgres DB"`, `"OpenAI"` vs `"Open AI"`) do NOT merge. The contract is
/// "both impls produce THIS partition", not a hand-guessed one — the Python
/// mirror runs the identical YAML + fixture and must reproduce it exactly.
/// 10 meaningful multi-member (case-fold) groups; the rest are singletons.
fn variant_fixture() -> Vec<(&'static str, &'static str)> {
    vec![
        // postgres: only case-fold of "PostgreSQL" merges; the rest differ
        // by whitespace/affix and stay separate under 0.85 trgm-set Jaccard.
        ("PostgreSQL", "postgres"),
        ("Postgres", "postgres_short"),
        ("postgresql", "postgres"),
        ("Postgres DB", "postgres_db"),
        ("PostgreSQL Database", "postgres_database"),
        ("OpenAI", "openai"),
        ("Open AI", "open_ai_spaced"),
        ("openai", "openai"),
        ("OpenAI Inc", "openai_inc"),
        ("OpenAI, Inc.", "openai_inc_punct"),
        ("pgvector", "pgvector"),
        ("pg_vector", "pg_underscore_vector"),
        ("PGVector", "pgvector"),
        ("pg vector", "pg_spaced_vector"),
        ("Anthropic", "anthropic"),
        ("anthropic", "anthropic"),
        ("Anthropic PBC", "anthropic_pbc"),
        ("Claude (Anthropic)", "claude_anthropic_paren"),
        ("Claude", "claude"),
        ("claude-3", "claude_3_dash"),
        ("Claude 3", "claude_3_spaced"),
        ("recursive CTE", "rcte"),
        ("Recursive CTE", "rcte"),
        ("recursive cte", "rcte"),
        ("Recursive CTEs", "rcte_plural"),
        ("HNSW", "hnsw"),
        ("hnsw index", "hnsw_index"),
        ("HNSW Index", "hnsw_index"),
        ("IVFFlat", "ivfflat"),
        ("ivfflat", "ivfflat"),
        ("IVF Flat", "ivf_flat_spaced"),
        ("IVFFlat index", "ivfflat_index"),
        ("pg_trgm", "pg_trgm_underscore"),
        ("pgtrgm", "pgtrgm"),
        ("pg trgm", "pg_trgm_spaced"),
        ("trigram (pg_trgm)", "trigram_pgtrgm_paren"),
        ("GraphRAG", "graphrag"),
        ("Graph RAG", "graph_rag_spaced"),
        ("graphrag", "graphrag"),
        ("Graph-RAG", "graph_rag_dash"),
        ("knowledge graph", "kg"),
        ("Knowledge Graph", "kg"),
        ("knowledge-graph", "kg_dash"),
        ("KnowledgeGraph", "kg_camel"),
        ("embedding model", "embmodel"),
        ("Embedding Model", "embmodel"),
        ("embedding-model", "embmodel_dash"),
        ("BAAI/bge-small-en-v1.5", "bge_full_path"),
        ("bge-small-en-v1.5", "bge_dash"),
        ("bge small en v1.5", "bge_spaced"),
    ]
}

#[test]
fn sc005_canonical_ids_match_shared_contract() {
    assert_eq!(TRGM_MERGE, 0.85_f32);
    assert_eq!(COSINE_MERGE, 0.90_f32);

    let fixture = variant_fixture();
    assert!(fixture.len() >= 50, "DC-003 requires >= 50 variants");

    let names: Vec<&str> = fixture.iter().map(|(n, _)| *n).collect();
    let groups = resolve_canonical_ids(&names);

    for (i, (ni, gi)) in fixture.iter().enumerate() {
        for (nj, gj) in fixture.iter().skip(i + 1) {
            let same_canonical = groups[*ni] == groups[*nj];
            let same_expected = gi == gj;
            assert_eq!(
                same_canonical, same_expected,
                "DRIFT: ({ni:?},{nj:?}) canonical-equal={same_canonical} \
                 but expected-equal={same_expected}"
            );
        }
    }
}
