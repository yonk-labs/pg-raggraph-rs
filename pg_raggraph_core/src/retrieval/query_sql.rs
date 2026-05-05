//! Builds the fused recursive-CTE SQL for `pgrg.query`.
//!
//! Spec §4 lines 121-176 is the source of truth. This builder produces the
//! exact statement, with mode-conditional lane gating: when a lane is
//! disabled, its CTE keeps the same shape but adds a `WHERE false` guard
//! so the UNION ALL in `fused` sees zero rows from that lane (DC-003: same
//! builder, empty lanes — not three separate queries).
//!
//! Bind contract:
//!   $1 = q text
//!   $2 = filter jsonb (or NULL)
//!   $3 = `top_k` int
//!   $4 = namespace text
//!   $5 = hops int
//!
//! RRF k=60 is hard-coded per spec §4 line 164 and Constraint Always
//! ("single SQL statement matching spec §4 byte-for-byte semantically").

use crate::retrieval::Mode;

/// Build the fused query SQL for a given retrieval mode.
///
/// Single-mode variants (`Mode::Vector`, `Mode::Bm25`, `Mode::Graph`) emit
/// the same four CTEs (`vec`, `bm`, `walked`/`graph`) as `Mode::Hybrid`,
/// but inactive lanes are zeroed out with a `WHERE false` guard so the
/// fused `UNION ALL` sees zero rows from them. This satisfies DC-003
/// ("same query builder, empty lane arrays — not three separate queries")
/// without changing the result-row shape across modes.
#[must_use]
pub fn build_query_sql(mode: Mode) -> String {
    // Per-lane gate: when a mode disables a lane, inject `WHERE false AND`
    // ahead of the lane's existing predicates so the CTE produces zero rows.
    // This keeps the CTE's column shape stable across modes (DC-003) while
    // making the single-mode behaviour assertable from the SQL string alone.
    let vec_gate = if mode.uses_vector() { "" } else { "false AND " };
    let bm_gate = if mode.uses_bm25() { "" } else { "false AND " };
    let graph_gate = if mode.uses_graph() { "" } else { "false AND " };

    format!(
        r"
WITH
  q_emb AS (SELECT pgrg.embed($1) AS v),
  vec AS (
    SELECT c.id, ROW_NUMBER() OVER (ORDER BY c.embedding <=> (SELECT v FROM q_emb)) AS rk
    FROM pgrg.chunks c
    WHERE {vec_gate}c.namespace = $4
      AND ($2::jsonb IS NULL OR c.metadata @> $2)
    ORDER BY c.embedding <=> (SELECT v FROM q_emb) LIMIT 50
  ),
  bm AS (
    SELECT c.id, ROW_NUMBER() OVER (ORDER BY ts_rank_cd(c.text_search, q) DESC) AS rk
    FROM pgrg.chunks c, plainto_tsquery('english', $1) q
    WHERE {bm_gate}c.text_search @@ q
      AND c.namespace = $4
      AND ($2::jsonb IS NULL OR c.metadata @> $2)
    ORDER BY ts_rank_cd(c.text_search, q) DESC LIMIT 50
  ),
  seeds AS (
    SELECT e.id FROM pgrg.entities e
    WHERE e.namespace = $4
      AND e.name_emb <=> (SELECT v FROM q_emb) < 0.35
    ORDER BY e.name_emb <=> (SELECT v FROM q_emb) LIMIT 8
  ),
  walked AS (
    SELECT id, 0 AS d FROM seeds
    UNION ALL
    SELECT r.dst_id, w.d + 1 FROM pgrg.relationships r JOIN walked w ON r.src_id = w.id
    WHERE w.d < $5
    UNION ALL
    SELECT r.src_id, w.d + 1 FROM pgrg.relationships r JOIN walked w ON r.dst_id = w.id
    WHERE w.d < $5
  ),
  graph AS (
    SELECT m.chunk_id AS id, ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rk
    FROM pgrg.chunk_entities m
    JOIN walked w ON m.entity_id = w.id
    JOIN pgrg.chunks c ON c.id = m.chunk_id
    WHERE {graph_gate}c.namespace = $4
      AND ($2::jsonb IS NULL OR c.metadata @> $2)
    GROUP BY m.chunk_id LIMIT 50
  ),
  fused AS (
    SELECT id, SUM(1.0 / (60 + rk)) AS score,
           jsonb_agg(jsonb_build_object('lane', lane, 'rk', rk)) AS sigs
    FROM (
      SELECT id, rk, 'vec'   AS lane FROM vec
      UNION ALL SELECT id, rk, 'bm25'  FROM bm
      UNION ALL SELECT id, rk, 'graph' FROM graph
    ) u
    GROUP BY id
  )
SELECT c.id, c.document_id, c.text, f.score, f.sigs
FROM fused f JOIN pgrg.chunks c ON c.id = f.id
ORDER BY f.score DESC LIMIT $3
"
    )
}
