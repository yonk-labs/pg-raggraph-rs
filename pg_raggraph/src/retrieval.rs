//! `pgrg.query` SQL function — pgrx wrapper over `pg_raggraph_core::retrieval`.
//!
//! Constraint Always: parameterized SQL with positional arguments. The SQL
//! template comes from `build_query_sql(Mode)` (no user-text interpolation).
//!
//! Bind contract (matches `pg_raggraph_core::retrieval::query_sql`):
//!   $1 = q text
//!   $2 = filter jsonb (or NULL)
//!   $3 = top_k int
//!   $4 = namespace text
//!   $5 = hops int
//!   $6 = vec_weight float8
//!   $7 = bm25_weight float8
//!   $8 = graph_weight float8
//!
//! Result columns: (chunk_id uuid, document_id uuid, text text, score float, signals jsonb).
//! Schema reference: `pg_raggraph/sql/001_tables.sql` — `chunks.id`, `chunks.document_id`,
//! `documents.id` are all `uuid`. T6's SQL selects `c.id, c.document_id, c.text, f.score,
//! f.sigs` directly from `pgrg.chunks`, so the columns are uuid-typed natively.

use pg_raggraph_core::retrieval::Mode;
use pg_raggraph_core::retrieval::query_sql::build_query_sql;
use pgrx::prelude::*;

/// `pgrg.query(q, filter, top_k, namespace, hops, weights, mode)`
///
/// Returns one row per fused chunk: (chunk_id, document_id, text, score, signals).
///
/// `weights` is a JSONB object with optional keys `vec` / `bm25` / `graph`
/// (float8); missing keys default to 1.0. NULL means "use defaults".
#[pg_extern]
fn query(
    q: &str,
    filter: default!(Option<pgrx::JsonB>, "NULL"),
    top_k: default!(i32, "10"),
    namespace: default!(&str, "'default'"),
    hops: default!(i32, "1"),
    weights: default!(Option<pgrx::JsonB>, "NULL"),
    mode: default!(&str, "'hybrid'"),
) -> TableIterator<
    'static,
    (
        name!(chunk_id, pgrx::Uuid),
        name!(document_id, pgrx::Uuid),
        name!(text, String),
        name!(score, f64),
        name!(signals, pgrx::JsonB),
    ),
> {
    use pg_raggraph_core::retrieval::rrf::RrfWeights;
    let weights = match weights {
        Some(jsonb) => {
            let v = &jsonb.0;
            RrfWeights {
                vec: v
                    .get("vec")
                    .and_then(serde_json::Value::as_f64)
                    .unwrap_or(1.0),
                bm25: v
                    .get("bm25")
                    .and_then(serde_json::Value::as_f64)
                    .unwrap_or(1.0),
                graph: v
                    .get("graph")
                    .and_then(serde_json::Value::as_f64)
                    .unwrap_or(1.0),
            }
        }
        None => RrfWeights::default(),
    };

    let parsed_mode = match Mode::parse(mode) {
        Some(m) => m,
        None => {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
                format!(
                    "pgrg.query: unknown mode `{mode}`; expected hybrid | vector | bm25 | graph"
                )
            );
        }
    };

    let sql = build_query_sql(parsed_mode);

    let rows: Vec<(pgrx::Uuid, pgrx::Uuid, String, f64, pgrx::JsonB)> = Spi::connect(|client| {
        client
            .select(
                &sql,
                Some(i64::from(top_k)),
                &[
                    q.into(),
                    filter.into(),
                    top_k.into(),
                    namespace.into(),
                    hops.into(),
                    weights.vec.into(),
                    weights.bm25.into(),
                    weights.graph.into(),
                ],
            )
            .expect("pgrg.query: select failed")
            .map(|r| {
                (
                    r.get::<pgrx::Uuid>(1)
                        .expect("chunk_id col")
                        .expect("chunk_id NOT NULL"),
                    r.get::<pgrx::Uuid>(2)
                        .expect("document_id col")
                        .expect("document_id NOT NULL"),
                    r.get::<String>(3).expect("text col").unwrap_or_default(),
                    r.get::<f64>(4).expect("score col").unwrap_or(0.0),
                    r.get::<pgrx::JsonB>(5)
                        .expect("signals col")
                        .unwrap_or_else(|| pgrx::JsonB(serde_json::json!([]))),
                )
            })
            .collect()
    });

    TableIterator::new(rows)
}
