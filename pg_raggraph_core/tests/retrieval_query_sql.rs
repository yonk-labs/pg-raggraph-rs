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
    // Constraint Always: positional parameters $1..$8; no `format!` interpolation
    // of user input. T11 added $6/$7/$8 for per-lane RRF weights.
    let sql = build_query_sql(Mode::Hybrid);
    for p in ["$1", "$2", "$3", "$4", "$5", "$6", "$7", "$8"] {
        assert!(sql.contains(p), "missing positional param {p}");
    }
}

#[test]
fn sql_includes_weight_binds() {
    // SC-010: per-lane RRF weights flow through positional binds $6/$7/$8.
    let sql = build_query_sql(Mode::Hybrid);
    for p in ["$6", "$7", "$8"] {
        assert!(sql.contains(p), "missing weight positional param {p}");
    }
}

#[test]
fn sql_uses_with_recursive_for_graph_walk() {
    // The graph CTE self-references via the `walked` recursive walker.
    // Without WITH RECURSIVE, PG errors at execution time with
    // `relation "walked" does not exist`. T6's substring-only tests
    // missed this; T7 caught it. Pin the contract here.
    let sql = build_query_sql(Mode::Hybrid);
    assert!(
        sql.contains("WITH RECURSIVE"),
        "fused query SQL must use WITH RECURSIVE for graph self-reference"
    );
}

#[test]
fn sql_score_is_cast_to_float8() {
    // PG infers `SUM(1.0 / (60 + rk))` as numeric. Without an explicit cast
    // to float8, pgrx-side `r.get::<f64>` errors with
    // `IncompatibleTypes { rust_type: "f64", datum_type: "numeric" }`.
    // Regression guard for T7's discovery.
    let sql = build_query_sql(Mode::Hybrid);
    assert!(
        sql.contains("f.score::float8"),
        "score column must be cast to float8 for pgrx f64 binding compatibility"
    );
}

#[test]
fn sql_graph_lane_gated_when_hops_zero() {
    // SC-006: hops=0 must exclude the graph lane entirely. The recursive
    // walker's base case emits seeds at d=0 regardless of $5, so without an
    // extra runtime gate the `graph` CTE would still surface seed-attached
    // chunks via `chunk_entities` joins. A `$5 >= 1` predicate inside the
    // graph CTE forces zero rows when hops=0.
    //
    // Pin the contract at the SQL-string level so any future refactor of the
    // walker has to keep the lane hops-aware (the integration test
    // `hops_zero_excludes_graph_lane` is the end-to-end gate).
    let sql = build_query_sql(Mode::Graph);
    assert!(
        sql.contains("$5 >= 1"),
        "graph lane must gate on hops >= 1 to satisfy SC-006; got SQL: {sql}"
    );
}

#[test]
fn sql_recursive_cte_has_single_recursive_term() {
    // PG requires a recursive CTE be expressible as `non-recursive UNION ALL recursive`.
    // T6 originally emitted three branches (one base + two recursive), which PG rejects
    // with `recursive reference to query "walked" must not appear within its non-recursive
    // term`. Encode undirected traversal via `OR` in the JOIN instead.
    let sql = build_query_sql(Mode::Hybrid);
    let union_all_count = sql.matches("UNION ALL").count();
    // walked has exactly one UNION ALL between its base and recursive term;
    // fused has two more (joining bm and graph onto vec). Total expected: 3.
    assert_eq!(
        union_all_count, 3,
        "expected 3 UNION ALLs total (1 in walked, 2 in fused), got {union_all_count}"
    );
    assert!(
        sql.contains("r.src_id = w.id OR r.dst_id = w.id"),
        "recursive walker must use OR-join to keep a single recursive term"
    );
}
