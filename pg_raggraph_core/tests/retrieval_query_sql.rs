use pg_raggraph_core::retrieval::Mode;
use pg_raggraph_core::retrieval::query_sql::build_query_sql;

#[test]
fn hybrid_sql_includes_all_three_lanes() {
    let sql = build_query_sql(Mode::Hybrid);
    assert!(sql.contains("vec AS"), "hybrid: vec lane CTE");
    assert!(sql.contains("bm AS"), "hybrid: bm25 lane CTE");
    assert!(sql.contains("graph AS"), "hybrid: graph lane CTE");
    assert!(sql.contains("60 + rk"), "RRF k=60 hard-coded per spec §4");
}

#[test]
fn vector_only_sql_omits_other_lanes() {
    // SC-004: mode='vector' returns rows whose signals are only [{lane:'vec',...}].
    // We achieve this by emitting empty CTEs for the unused lanes (DC-003: same query
    // builder, empty lane arrays — not three separate queries).
    let sql = build_query_sql(Mode::Vector);
    assert!(sql.contains("vec AS"));
    assert!(
        sql.contains("bm AS"),
        "still emit empty bm25 CTE for shape stability"
    );
    assert!(
        sql.contains("graph AS"),
        "still emit empty graph CTE for shape stability"
    );
    // Empty-lane CTEs are detectable by a `WHERE false` guard or LIMIT 0:
    assert!(
        sql.contains("WHERE false") || sql.contains("LIMIT 0"),
        "single-mode queries must zero out unused lanes"
    );
}

#[test]
fn bm25_only_sql_zeros_vec_and_graph() {
    let sql = build_query_sql(Mode::Bm25);
    assert!(sql.contains("vec AS"));
    assert!(sql.contains("bm AS"));
    assert!(sql.contains("graph AS"));
    assert!(sql.contains("WHERE false") || sql.contains("LIMIT 0"));
}

#[test]
fn graph_only_sql_zeros_vec_and_bm25() {
    let sql = build_query_sql(Mode::Graph);
    assert!(sql.contains("vec AS"));
    assert!(sql.contains("bm AS"));
    assert!(sql.contains("graph AS"));
    assert!(sql.contains("WHERE false") || sql.contains("LIMIT 0"));
}

#[test]
fn sql_uses_undirected_walk() {
    // SC-007: undirected. Both directions must appear in the recursive CTE.
    let sql = build_query_sql(Mode::Hybrid);
    assert!(
        sql.contains("r.src_id = w.id") && sql.contains("r.dst_id = w.id"),
        "spec §4 line 148-152: undirected — UNION on dst from src AND src from dst"
    );
}

#[test]
fn sql_metadata_predicate_inside_each_lane() {
    // SC-008 + Constraint Never "fuse junk-then-throw":
    // metadata @> filter must appear inside vec, bm, and graph CTEs.
    let sql = build_query_sql(Mode::Hybrid);
    let occurrences = sql.matches("c.metadata @> $2").count();
    assert!(
        occurrences >= 3,
        "metadata predicate must appear inside vec, bm, graph lanes (got {occurrences})"
    );
}

#[test]
fn sql_uses_parameterized_args_not_concat() {
    // Constraint Always: positional parameters $1..$5; no `format!` interpolation
    // of user input.
    let sql = build_query_sql(Mode::Hybrid);
    for p in ["$1", "$2", "$3", "$4", "$5"] {
        assert!(sql.contains(p), "missing positional param {p}");
    }
}
