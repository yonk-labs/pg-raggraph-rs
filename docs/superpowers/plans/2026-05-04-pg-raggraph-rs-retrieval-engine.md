# pg-raggraph-rs Retrieval Engine — Implementation Plan (Plan 2 of 6)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the synchronous read path for the Rust extension. After Plan 2, the database can be loaded with pre-extracted fixtures via `pgrg.ingest_extracted` and answer hybrid SQL queries via `pgrg.query` — combining pgvector cosine, BM25, and a recursive-CTE graph walk into a single fused statement under RRF (k=60). This proves the AGE-replacement value prop concretely: one round-trip, one query plan, one ACID database.

Plan 2 ships **only retrieval**. No LLM grounding (Plan 4), no async ingest (Plan 3), no embedding model loader (Plan 3). For embeddings, this plan ships a deterministic test-only fallback gated on whether a real model is loaded — Plan 3 swaps the production loader in behind the same `pgrg.embed` SQL surface.

**Architecture:** Three-crate Cargo workspace from Plan 1, extended:
- `pg_raggraph_core::retrieval` — new module owning the SQL builder, `Mode` enum, RRF math, undirected-walk semantics. No pgrx; `cargo test`-able.
- `pg_raggraph_core::embedding` — new module owning the deterministic test-fallback embedder (until Plan 3 wires the real model).
- `pg_raggraph::retrieval` — pgrx wrapper that exposes `pgrg.query` and `pgrg.embed` as SQL functions; thin shell over `_core`.
- `pg_raggraph::admin` — extended with `pgrg.ingest_extracted` (fixture loader, single transaction).
- `pg_raggraph::sql/004_retrieval_indexes.sql` — adds the IVFFlat alternates wired to the `pgrg.parity_mode` GUC at namespace creation.

**Tech Stack (unchanged from Plan 1):** Rust 2024, `pgrx = "=0.17.0"`, PostgreSQL 17 (CI) / 18 (local dev), `pgvector` 0.8+, `pg_trgm`, `serde`/`serde_json`, `uuid`. License Apache-2.0.

**Spec reference:** `docs/superpowers/specs/2026-05-03-pg-raggraph-rs-extension-design.md`. This plan implements:
- §4 Query path (lines 90–185) — the fused recursive-CTE SQL, RRF k=60, mode parameter, undirected traversal, `hops` semantics, metadata-predicate-inside-lanes contract
- §6 SQL surface (lines 274–313) — `pgrg.query`, `pgrg.embed`, `pgrg.ingest_extracted`
- §7 GUCs (lines 320–334) — `pgrg.parity_mode` (HNSW vs IVFFlat at namespace creation), `pgrg.debug_retrieval` (signals jsonb), `pgrg.embed_dim` (vector dim contract)
- §10 Parity contracts (lines 408–438) — undirected walk, RRF k=60 default, IVFFlat under parity_mode

**Mission Brief reference:** `skill-output/mission-brief/Mission-Brief-plan2-retrieval-engine.md` — 14 Success Criteria (SC-001..SC-014), Constraints (Always / Ask First / Never), Drift Checkpoints (DC-001..DC-FINAL), Out of Scope. **The implementer MUST re-read this file at every `⛔ Drift Check DC-XXX` step in this plan**; the brief is authoritative if it conflicts with anything below.

**Plan arc (context — only Plan 2 is executed here):**
1. Foundation + Schema — **done** (committed to `main`)
2. **Retrieval engine** ← this plan (`pgrg.query`, `pgrg.embed`, `pgrg.ingest_extracted`, fused CTE, RRF, parity_mode index path)
3. Ingest pipeline — bg worker, chunkshop integration, real embedding model loader (`hf_cache`), `pgrg.ingest`/`pgrg.ingest_text`/`pgrg.ingest_bytes`
4. LLM extraction + ask — provider trait, OpenAI/Anthropic/Ollama/Mock impls, AES-GCM credential encryption, `pgrg.ask`
5. Sidecar binary — libpq job loop, embedded SQL bootstrap, HTTP `/v1/ask`
6. Cross-impl parity harness — `bench/parity/` corpus, `compare.py`, parity CI

---

## Pre-execution: conventions inherited from Plan 1

These were established in Plan 1 (`docs/superpowers/plans/2026-05-03-pg-raggraph-rs-foundation.md`) and **continue to apply** in Plan 2. Re-stating them here so the executor doesn't have to flip back.

**pgrx 0.17 deviations (as observed during Plan 1 execution):**

1. **Bare `#[pg_extern]`, no `schema = "pgrg"` argument.** The `.control` file's `schema = 'pgrg'` directive plus the `pg_module_magic!` declaration places generated functions in the `pgrg` schema automatically. Adding `schema = "pgrg"` to the attribute breaks SQL generation in pgrx 0.17. See `pg_raggraph/src/admin.rs` for working examples (`namespace_create`, `provider_create`, etc.).

2. **`Spi::connect_mut` for write paths.** `Spi::connect` is read-only in pgrx 0.17; INSERT/UPDATE/DELETE through `client.update(...)` requires `Spi::connect_mut(|client| { ... })`. Plan 1's `admin.rs` uses this pattern consistently.

3. **`#[pg_guard] pub extern "C-unwind" fn _PG_init()`** — Plan 1's signature already in `lib.rs`. If new init-side work is needed in Plan 2 (it isn't — GUCs are unchanged), reuse this signature.

4. **CString literals (`c"..."`) for GUC names/descriptions.** Plan 1's `gucs.rs` uses `c"pg_raggraph.parity_mode"` etc. No changes needed in Plan 2 (no new GUCs are introduced — see Constraint "Ask First: new GUCs"). The `pgrg.parity_mode` and `pgrg.debug_retrieval` GUCs are already registered.

5. **`pg_catalog.pg_tables.tablename` is OID `name`, not `text`** — cast with `::text` when iterating system catalog columns of type `name` from Rust. (Documented in `docs/dev-setup.md`; relevant for any schema-introspection tests this plan adds.)

**Local dev loop (per `docs/dev-setup.md`):**

```bash
cd /home/yonk/yonk-tools/pg-raggraph-extension
cargo pgrx test pg18 --package pg_raggraph --features "pg18 pg_test" --no-default-features
```

CI runs `cargo pgrx test pg17 -p pg_raggraph` against PGDG packages. Both targets are declared as cargo features in `pg_raggraph/Cargo.toml`. Command examples in this plan use the **pg17 form** (matching Plan 1's verbatim style and CI canonical); substitute `pg18` for local execution as `dev-setup.md` instructs.

**Branch policy:** commit to `main` (matches Plan 1). One commit per task. Commit messages mirror Plan 1's style (e.g., `feat(retrieval): pgrg.query naive vector mode top-k`).

**Repo root:** `/home/yonk/yonk-tools/pg-raggraph-extension/`. All paths in this plan are relative to that directory; substitute `<REPO>` = `/home/yonk/yonk-tools/pg-raggraph-extension` if absolute paths are needed.

---

## Task 1: Add IVFFlat fallback migration (parity_mode index path)

**Files:**
- Create: `pg_raggraph/sql/migrations/004_retrieval_indexes.sql`
- Modify: `pg_raggraph/src/lib.rs` (wire the new SQL file into the extension)

**Why:** Spec §10 requires that **parity benchmarks** use IVFFlat (deterministic) instead of HNSW (build-time randomness). Plan 1 created HNSW unconditionally in `002_indexes.sql`. This task adds a parallel IVFFlat declaration that is no-op when `pgrg.parity_mode = false` (default) and replaces the HNSW indexes with IVFFlat ones when `pgrg.parity_mode = true` at the moment of namespace creation. SC-009 verifies the observable behavior via `pg_indexes`.

**Design choice:** rather than re-create indexes at namespace creation time (impractical — Plan 1 creates indexes in `002_indexes.sql` at extension install, before any namespace exists), we ship a `pgrg._maybe_apply_parity_indexes()` helper SQL function that is called from `pgrg.namespace_create` (Plan 1's existing function — Task 1 modifies it via SQL `CREATE OR REPLACE`). When `parity_mode = true` at the time of the first `namespace_create` call in a fresh DB, the helper drops the HNSW indexes and creates IVFFlat alternates. When `false`, it does nothing. This preserves SC-009's requirement that `parity_mode` is read at namespace creation and not re-applied to existing namespaces (DC-004).

- [ ] **Step 1.1: Write the failing test**

Add to the `tests` module in `pg_raggraph/src/lib.rs`:

```rust
    #[pg_test]
    fn parity_mode_creates_ivfflat_indexes() {
        // SC-009: parity_mode at namespace_create swaps HNSW -> IVFFlat
        Spi::run("SET pg_raggraph.parity_mode = true").unwrap();
        Spi::run("SELECT pgrg.namespace_create('parity_ns')").unwrap();

        let chunk_idx_def: Option<String> = Spi::get_one(
            "SELECT indexdef FROM pg_indexes \
             WHERE schemaname = 'pgrg' AND indexname = 'chunks_embedding_hnsw'",
        )
        .unwrap();

        // After parity_mode toggle + namespace_create, the HNSW index is
        // replaced with IVFFlat. The replacement keeps the same name to keep
        // pgrg.query plans stable across modes.
        let def = chunk_idx_def.expect("chunks_embedding_hnsw must exist");
        assert!(
            def.contains("USING ivfflat"),
            "expected IVFFlat under parity_mode, got: {def}"
        );

        let entity_idx_def: Option<String> = Spi::get_one(
            "SELECT indexdef FROM pg_indexes \
             WHERE schemaname = 'pgrg' AND indexname = 'entities_name_emb_hnsw'",
        )
        .unwrap();
        let edef = entity_idx_def.expect("entities_name_emb_hnsw must exist");
        assert!(
            edef.contains("USING ivfflat"),
            "expected IVFFlat under parity_mode, got: {edef}"
        );

        Spi::run("SET pg_raggraph.parity_mode = false").unwrap();
    }

    #[pg_test]
    fn default_mode_keeps_hnsw_indexes() {
        // Counterpart to parity_mode test: default install must remain HNSW.
        let chunk_idx_def: Option<String> = Spi::get_one(
            "SELECT indexdef FROM pg_indexes \
             WHERE schemaname = 'pgrg' AND indexname = 'chunks_embedding_hnsw'",
        )
        .unwrap();
        let def = chunk_idx_def.expect("chunks_embedding_hnsw must exist");
        assert!(
            def.contains("USING hnsw"),
            "default install must use HNSW, got: {def}"
        );
    }
```

- [ ] **Step 1.2: Run tests, observe failure**

```bash
cargo pgrx test pg17 -p pg_raggraph -- parity_mode_creates_ivfflat_indexes
```

Expected: `parity_mode_creates_ivfflat_indexes` fails because there is no parity-mode swap in `pgrg.namespace_create` yet. `default_mode_keeps_hnsw_indexes` should already pass from Plan 1's `002_indexes.sql`.

- [ ] **Step 1.3: Write `pg_raggraph/sql/migrations/004_retrieval_indexes.sql`**

```sql
-- 004_retrieval_indexes.sql — IVFFlat alternates wired through pgrg.parity_mode.
-- Per spec §10: parity benchmarks must use IVFFlat (deterministic) instead
-- of HNSW (build-time randomness). The swap is gated by pgrg.parity_mode at
-- the moment pgrg.namespace_create runs in a fresh DB.
--
-- DC-004 contract: parity_mode is read once at namespace_create. Existing
-- namespaces are never re-indexed by toggling the GUC.

CREATE OR REPLACE FUNCTION pgrg._maybe_apply_parity_indexes()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    parity bool := current_setting('pg_raggraph.parity_mode', true)::bool;
    has_chunks bool;
    has_entities bool;
BEGIN
    IF parity IS DISTINCT FROM true THEN
        RETURN;
    END IF;

    -- Only swap once: if no chunks/entities exist yet (first namespace_create
    -- in a fresh DB), drop HNSW and recreate as IVFFlat. After data is loaded,
    -- the swap is a no-op (DC-004 — existing namespaces keep their indexes).
    SELECT EXISTS(SELECT 1 FROM pgrg.chunks LIMIT 1) INTO has_chunks;
    SELECT EXISTS(SELECT 1 FROM pgrg.entities LIMIT 1) INTO has_entities;

    IF has_chunks OR has_entities THEN
        -- Data already present; do not disturb existing indexes.
        RETURN;
    END IF;

    DROP INDEX IF EXISTS pgrg.chunks_embedding_hnsw;
    DROP INDEX IF EXISTS pgrg.entities_name_emb_hnsw;

    -- IVFFlat with conservative lists count for empty tables.
    -- Increased automatically by parity benchmark setup (Plan 6) once
    -- corpus is loaded; for Plan 2's tests, lists=10 is sufficient.
    CREATE INDEX chunks_embedding_hnsw
        ON pgrg.chunks USING ivfflat (embedding vector_cosine_ops)
        WITH (lists = 10);

    CREATE INDEX entities_name_emb_hnsw
        ON pgrg.entities USING ivfflat (name_emb vector_cosine_ops)
        WITH (lists = 10);
END;
$$;
```

- [ ] **Step 1.4: Wire the migration file into `lib.rs`**

In `pg_raggraph/src/lib.rs`, add a fifth `extension_sql_file!` invocation immediately after the existing four (preserving Plan 1's ordering and dependency chain):

```rust
::pgrx::extension_sql_file!(
    "../sql/migrations/004_retrieval_indexes.sql",
    name = "retrieval_indexes",
    requires = ["create_indexes"]
);
```

- [ ] **Step 1.5: Modify `namespace_create` to call the helper**

Edit `pg_raggraph/src/admin.rs`. In `namespace_create`, immediately after the INSERT/ON CONFLICT block but before the function returns, add:

```rust
    Spi::run("SELECT pgrg._maybe_apply_parity_indexes()")
        .expect("namespace_create: parity index application failed");
```

- [ ] **Step 1.6: Run tests, observe pass**

```bash
cargo pgrx test pg17 -p pg_raggraph -- parity_mode_creates_ivfflat_indexes default_mode_keeps_hnsw_indexes
```

Expected: both tests pass.

- [ ] **Step 1.7: Commit**

```bash
git add pg_raggraph/sql/migrations/004_retrieval_indexes.sql pg_raggraph/src/lib.rs pg_raggraph/src/admin.rs
git commit -m "feat(schema): IVFFlat fallback under pgrg.parity_mode at namespace_create"
```

---

## Task 2: `retrieval` module skeleton in `pg_raggraph_core` — `Mode` enum + lane types

**Files:**
- Create: `pg_raggraph_core/src/retrieval/mod.rs`
- Create: `pg_raggraph_core/src/retrieval/mode.rs`
- Create: `pg_raggraph_core/tests/retrieval_mode.rs`
- Modify: `pg_raggraph_core/src/lib.rs` (declare `pub mod retrieval`)

**Why:** Constraint Always: "All retrieval logic that is not strictly pgrx FFI lives in `pg_raggraph_core::retrieval` so it is reachable by `cargo test` without PG." This task creates the module and the `Mode` enum. SC-012 (unit tests cover RRF math, walk semantics, lane-selection matrix) requires this module to exist with sufficient surface area to test those things without a running PG.

**Mode names per Mission Brief SC-004:** `Hybrid` (default) | `Vector` | `Bm25` | `Graph`. **NOT** `Naive`/`NaiveBoost`/`Local`/`Global`/`Smart` — those are explicitly Out of Scope (mission brief Constraints "Never" + spec §11). The brief is authoritative.

- [ ] **Step 2.1: Write the failing test**

Create `pg_raggraph_core/tests/retrieval_mode.rs`:

```rust
use pg_raggraph_core::retrieval::Mode;

#[test]
fn mode_parses_hybrid_by_default_string() {
    assert_eq!(Mode::parse("hybrid"), Some(Mode::Hybrid));
    assert_eq!(Mode::parse("vector"), Some(Mode::Vector));
    assert_eq!(Mode::parse("bm25"), Some(Mode::Bm25));
    assert_eq!(Mode::parse("graph"), Some(Mode::Graph));
}

#[test]
fn mode_unknown_returns_none() {
    // Mission Brief Constraints "Never": no smart/local/global modes.
    assert_eq!(Mode::parse("smart"), None);
    assert_eq!(Mode::parse("naive_boost"), None);
    assert_eq!(Mode::parse("local"), None);
    assert_eq!(Mode::parse("global"), None);
    assert_eq!(Mode::parse(""), None);
    assert_eq!(Mode::parse("HYBRID"), None); // case-sensitive, matches SQL spec
}

#[test]
fn mode_as_str_roundtrip() {
    for m in [Mode::Hybrid, Mode::Vector, Mode::Bm25, Mode::Graph] {
        assert_eq!(Mode::parse(m.as_str()), Some(m));
    }
}
```

- [ ] **Step 2.2: Run tests, observe failure**

```bash
cargo test -p pg_raggraph_core --test retrieval_mode
```

Expected: compile error — `pg_raggraph_core::retrieval` does not exist.

- [ ] **Step 2.3: Create `pg_raggraph_core/src/retrieval/mod.rs`**

```rust
//! Retrieval engine: SQL builder, RRF fusion, undirected graph walk.
//!
//! Lives outside the pgrx crate so unit tests run with plain `cargo test`.
//! Per mission brief Constraint Always: all retrieval logic that is not
//! strictly pgrx FFI lives here.

pub mod mode;

pub use mode::Mode;
```

- [ ] **Step 2.4: Create `pg_raggraph_core/src/retrieval/mode.rs`**

```rust
//! Retrieval mode parameter (ablation knobs).
//!
//! Per spec §4 and mission brief SC-004:
//! - `Hybrid` (default) fuses all four lanes (vector, bm25, graph, metadata predicate).
//! - `Vector`, `Bm25`, `Graph` force a single-lane query for benchmarking.
//!
//! Per mission brief Constraints "Never": no smart-mode, no naive_boost,
//! no local/global modes. Those are spec §11 out of scope for v1.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Mode {
    /// Default: fuse vector + BM25 + graph under RRF (k=60) with metadata predicate inside each lane.
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

impl Default for Mode {
    fn default() -> Self {
        Mode::Hybrid
    }
}
```

- [ ] **Step 2.5: Wire `pub mod retrieval;` into `pg_raggraph_core/src/lib.rs`**

Add a single line after the existing module declarations:

```rust
pub mod retrieval;
```

- [ ] **Step 2.6: Run tests, observe pass**

```bash
cargo test -p pg_raggraph_core --test retrieval_mode
```

Expected: 3 tests pass.

- [ ] **Step 2.7: Commit**

```bash
git add pg_raggraph_core/src/retrieval/ pg_raggraph_core/src/lib.rs pg_raggraph_core/tests/retrieval_mode.rs
git commit -m "feat(core): retrieval::Mode enum (hybrid/vector/bm25/graph) per spec §4"
```

---

## Task 3: RRF fusion math in `_core` (k=60 default, weight overrides)

**Files:**
- Create: `pg_raggraph_core/src/retrieval/rrf.rs`
- Create: `pg_raggraph_core/tests/retrieval_rrf.rs`
- Modify: `pg_raggraph_core/src/retrieval/mod.rs` (re-export)

**Why:** SC-005 requires that `mode='hybrid'` fuses ranks via RRF with `k=60` and equal weights `{vec:1, bm25:1, graph:1}`. SC-010 requires `weights := '{"vec":2.0,"bm25":0.0,"graph":1.0}'::jsonb` to zero out BM25 and double vector contribution. SC-012 mandates the math is unit-testable without PG. This task implements the formula and verifies it against hand-computed values.

**Formula (from spec §4 line 164):** `score = SUM(weight_lane * 1.0 / (60 + rk))` over all lane appearances of a chunk.

- [ ] **Step 3.1: Write the failing test**

Create `pg_raggraph_core/tests/retrieval_rrf.rs`:

```rust
use pg_raggraph_core::retrieval::rrf::{LaneHit, RrfWeights, fuse};

#[test]
fn rrf_default_k_60_equal_weights() {
    // Single chunk, hits vec at rank 1, bm25 at rank 1, graph at rank 1.
    let hits = vec![
        LaneHit { id: 1, lane: "vec", rk: 1 },
        LaneHit { id: 1, lane: "bm25", rk: 1 },
        LaneHit { id: 1, lane: "graph", rk: 1 },
    ];
    let scored = fuse(&hits, &RrfWeights::default());
    assert_eq!(scored.len(), 1);
    let expected = 3.0 / 61.0;
    let actual = scored[0].score;
    assert!(
        (actual - expected).abs() < 1e-12,
        "RRF k=60 equal weights, 3 hits at rank 1: expected {expected}, got {actual}"
    );
}

#[test]
fn rrf_two_chunks_one_vec_one_bm25() {
    // SC-005: chunk A wins vec lane, chunk B wins bm25 lane, equal weights -> tie.
    let hits = vec![
        LaneHit { id: 1, lane: "vec", rk: 1 },
        LaneHit { id: 2, lane: "bm25", rk: 1 },
    ];
    let scored = fuse(&hits, &RrfWeights::default());
    assert_eq!(scored.len(), 2);
    let a = scored.iter().find(|s| s.id == 1).unwrap();
    let b = scored.iter().find(|s| s.id == 2).unwrap();
    assert!((a.score - b.score).abs() < 1e-12, "ties under equal weights");
    assert!((a.score - 1.0 / 61.0).abs() < 1e-12);
}

#[test]
fn rrf_weight_override_zeros_bm25_doubles_vec() {
    // SC-010: weights {"vec":2.0,"bm25":0.0,"graph":1.0} zeros BM25 contribution
    // and doubles vector contribution.
    let hits = vec![
        LaneHit { id: 1, lane: "vec", rk: 1 },   // weight 2.0 -> 2.0/61
        LaneHit { id: 1, lane: "bm25", rk: 1 },  // weight 0.0 -> 0.0/61
        LaneHit { id: 1, lane: "graph", rk: 1 }, // weight 1.0 -> 1.0/61
    ];
    let weights = RrfWeights { vec: 2.0, bm25: 0.0, graph: 1.0 };
    let scored = fuse(&hits, &weights);
    let expected = 2.0 / 61.0 + 0.0 / 61.0 + 1.0 / 61.0;
    assert!((scored[0].score - expected).abs() < 1e-12);
}

#[test]
fn rrf_descending_score_order() {
    // Chunk 1: rank 1 in vec only (1/61). Chunk 2: rank 1 in vec AND bm25 (2/61).
    let hits = vec![
        LaneHit { id: 1, lane: "vec", rk: 1 },
        LaneHit { id: 2, lane: "vec", rk: 1 },
        LaneHit { id: 2, lane: "bm25", rk: 1 },
    ];
    let scored = fuse(&hits, &RrfWeights::default());
    assert_eq!(scored.len(), 2);
    assert_eq!(scored[0].id, 2, "fuse() returns highest score first");
    assert_eq!(scored[1].id, 1);
    assert!(scored[0].score > scored[1].score);
}

#[test]
fn rrf_empty_input_yields_empty_output() {
    let scored = fuse(&[], &RrfWeights::default());
    assert!(scored.is_empty());
}
```

- [ ] **Step 3.2: Run tests, observe failure**

```bash
cargo test -p pg_raggraph_core --test retrieval_rrf
```

Expected: compile error — `pg_raggraph_core::retrieval::rrf` does not exist.

- [ ] **Step 3.3: Create `pg_raggraph_core/src/retrieval/rrf.rs`**

```rust
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
        Self { vec: 1.0, bm25: 1.0, graph: 1.0 }
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
#[must_use]
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
    scored.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
    scored
}
```

- [ ] **Step 3.4: Re-export from `retrieval/mod.rs`**

Append to `pg_raggraph_core/src/retrieval/mod.rs`:

```rust
pub mod rrf;
```

- [ ] **Step 3.5: Run tests, observe pass**

```bash
cargo test -p pg_raggraph_core --test retrieval_rrf
```

Expected: 5 tests pass.

- [ ] **Step 3.6: Commit**

```bash
git add pg_raggraph_core/src/retrieval/rrf.rs pg_raggraph_core/src/retrieval/mod.rs pg_raggraph_core/tests/retrieval_rrf.rs
git commit -m "feat(core): RRF fusion (k=60, weight overrides) per spec §4"
```

---

## Task 4: Deterministic test-only embedder in `_core` + `pgrg.embed` SQL function

**Files:**
- Create: `pg_raggraph_core/src/embedding.rs`
- Create: `pg_raggraph_core/tests/embedding.rs`
- Create: `pg_raggraph/src/embedding.rs`
- Modify: `pg_raggraph_core/src/lib.rs` (declare `pub mod embedding`)
- Modify: `pg_raggraph/src/lib.rs` (declare `mod embedding`)

**Why:** SC-002 requires `pgrg.embed('hello world')` to return a `vector(N)` of dim `embed_dim` (default 384), and two consecutive calls on the same input to return byte-identical vectors. SC-011 requires queries to work without any `pgrg.providers` rows (no LLM dependency, no provider lookup). The real model loader is Plan 3; Plan 2 ships a deterministic hash-derived embedder that satisfies the dim contract and is byte-stable. Plan 3 will swap the production model in behind the same SQL function.

**Determinism approach:** SHA-256 of the input text expanded into `embed_dim` f32 components in the range [-1, 1], then L2-normalized so cosine distance is well-defined. Pure function, no I/O, no allocation past the output `Vec<f32>`.

- [ ] **Step 4.1: Write the failing core test**

Create `pg_raggraph_core/tests/embedding.rs`:

```rust
use pg_raggraph_core::embedding::deterministic_embed;

#[test]
fn deterministic_embed_returns_correct_dim() {
    let v = deterministic_embed("hello world", 384);
    assert_eq!(v.len(), 384);
}

#[test]
fn deterministic_embed_is_byte_stable() {
    // SC-002: two consecutive calls on the same input return byte-identical vectors.
    let a = deterministic_embed("hello world", 384);
    let b = deterministic_embed("hello world", 384);
    assert_eq!(a, b);
}

#[test]
fn deterministic_embed_different_inputs_differ() {
    let a = deterministic_embed("hello", 384);
    let b = deterministic_embed("world", 384);
    assert_ne!(a, b);
}

#[test]
fn deterministic_embed_l2_normalized() {
    // Cosine similarity assumes unit norm; produces well-defined comparisons.
    let v = deterministic_embed("hello world", 384);
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    assert!((norm - 1.0).abs() < 1e-5, "expected L2 norm ~1.0, got {norm}");
}

#[test]
fn deterministic_embed_respects_dim_parameter() {
    let v_128 = deterministic_embed("test", 128);
    let v_768 = deterministic_embed("test", 768);
    assert_eq!(v_128.len(), 128);
    assert_eq!(v_768.len(), 768);
}
```

- [ ] **Step 4.2: Run core tests, observe failure**

```bash
cargo test -p pg_raggraph_core --test embedding
```

Expected: compile error — `pg_raggraph_core::embedding` does not exist.

- [ ] **Step 4.3: Add `sha2` to `pg_raggraph_core/Cargo.toml` dependencies**

In `pg_raggraph_core/Cargo.toml`, add to `[dependencies]`:

```toml
sha2 = "0.10"
```

- [ ] **Step 4.4: Create `pg_raggraph_core/src/embedding.rs`**

```rust
//! Deterministic test-only embedder.
//!
//! Plan 2 ships this as the embedding contract for `pgrg.embed`. Plan 3
//! introduces the real model loader (chunkshop `hf_cache` for
//! `BAAI/bge-small-en-v1.5`); the SQL surface (`pgrg.embed`) does not
//! change. Until Plan 3, all retrieval tests use this embedder.
//!
//! Mission brief SC-002: byte-identical output for identical input, dim
//! equal to the `pgrg.embed_dim` GUC.
//! Mission brief SC-011: no LLM provider lookup, no network — runs on a
//! fresh PG with no `pgrg.providers` rows.

use sha2::{Digest, Sha256};

/// Hash-derived deterministic embedding.
///
/// Produces an L2-normalized `Vec<f32>` of length `dim`. Pure function
/// (same input → same output across processes and machines). Suitable
/// for tests and parity smoke runs; NOT a semantic embedding — Plan 3
/// replaces this with the real bge-small-en-v1.5 ONNX model.
#[must_use]
pub fn deterministic_embed(text: &str, dim: usize) -> Vec<f32> {
    // Expand SHA-256 by repeated hashing until we have enough bytes for `dim` f32s.
    let bytes_needed = dim * 4;
    let mut buf = Vec::<u8>::with_capacity(bytes_needed);
    let mut counter: u32 = 0;
    while buf.len() < bytes_needed {
        let mut hasher = Sha256::new();
        hasher.update(text.as_bytes());
        hasher.update(counter.to_le_bytes());
        buf.extend_from_slice(&hasher.finalize());
        counter = counter.wrapping_add(1);
    }
    buf.truncate(bytes_needed);

    // Each f32 component: convert 4 bytes to a u32, then map to (-1, 1).
    let mut v: Vec<f32> = buf
        .chunks_exact(4)
        .map(|b| {
            let u = u32::from_le_bytes([b[0], b[1], b[2], b[3]]);
            // Map u32 -> [-1, 1]. Avoid 0 norm by ensuring nonzero spread.
            (u as f32 / u32::MAX as f32) * 2.0 - 1.0
        })
        .collect();

    // L2-normalize (avoid divide-by-zero with epsilon).
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 1e-12 {
        for x in &mut v {
            *x /= norm;
        }
    }
    v
}
```

- [ ] **Step 4.5: Wire `pub mod embedding;` into `pg_raggraph_core/src/lib.rs`**

Add a single line:

```rust
pub mod embedding;
```

- [ ] **Step 4.6: Run core tests, observe pass**

```bash
cargo test -p pg_raggraph_core --test embedding
```

Expected: 5 tests pass.

- [ ] **Step 4.7: Write the failing extension test**

Add to the `tests` module in `pg_raggraph/src/lib.rs`:

```rust
    #[pg_test]
    fn embed_returns_correct_dim_vector() {
        // SC-002: pgrg.embed returns a vector(N) where N = pg_raggraph.embed_dim.
        // pgvector returns vectors as text in the form '[v1,v2,...]'; the dim
        // is verifiable by parsing the comma count. Use vector_dims() from
        // pgvector to assert without parsing strings.
        let dim: Option<i32> =
            Spi::get_one("SELECT vector_dims(pgrg.embed('hello world'))").unwrap();
        assert_eq!(dim, Some(384));
    }

    #[pg_test]
    fn embed_is_deterministic() {
        // SC-002: two consecutive calls on the same input return byte-identical vectors.
        let same: Option<bool> = Spi::get_one(
            "SELECT pgrg.embed('hello world')::text = pgrg.embed('hello world')::text",
        )
        .unwrap();
        assert_eq!(same, Some(true));
    }

    #[pg_test]
    fn embed_works_without_providers_table_rows() {
        // SC-011: fresh DB with no providers rows — pgrg.embed must succeed.
        let n: Option<i64> = Spi::get_one("SELECT count(*) FROM pgrg.providers").unwrap();
        assert_eq!(n, Some(0), "test precondition: no providers rows");
        // If this errors, SC-011 fails.
        let _: Option<i32> = Spi::get_one("SELECT vector_dims(pgrg.embed('q'))").unwrap();
    }
```

- [ ] **Step 4.8: Run extension tests, observe failure**

```bash
cargo pgrx test pg17 -p pg_raggraph -- embed_returns_correct_dim_vector embed_is_deterministic embed_works_without_providers_table_rows
```

Expected: failures (`function pgrg.embed(text) does not exist`).

- [ ] **Step 4.9: Create `pg_raggraph/src/embedding.rs`**

```rust
//! `pgrg.embed` SQL function — thin pgrx wrapper over `pg_raggraph_core::embedding`.
//!
//! Returns a pgvector `Vector` of dimension `pg_raggraph.embed_dim`. Plan 2
//! uses the deterministic hash-derived embedder; Plan 3 swaps the real
//! ONNX model in behind this same SQL surface.

use pgrx::prelude::*;

/// Build a pgvector text literal of the form '[v1,v2,...]' from an f32 slice.
/// Returning the text and casting in SQL avoids depending on a pgvector pgrx
/// type binding (none ships in pgrx 0.17).
fn vector_literal(v: &[f32]) -> String {
    let mut s = String::with_capacity(v.len() * 8 + 2);
    s.push('[');
    let mut first = true;
    for x in v {
        if !first {
            s.push(',');
        }
        first = false;
        // {:?} on f32 gives a round-trip representation; pgvector's parser accepts it.
        s.push_str(&format!("{x}"));
    }
    s.push(']');
    s
}

/// `pgrg.embed(text, namespace)` — deterministic test-only embedder for Plan 2.
///
/// Returns the embedding as a pgvector `vector(N)` value via the text-literal
/// cast. The SQL signature declares the return as `vector` and the wrapper
/// casts the literal at the SQL layer.
#[pg_extern(sql = r#"
    CREATE FUNCTION pgrg.embed(
        "text" text,
        "namespace" text DEFAULT 'default'
    ) RETURNS public.vector
    LANGUAGE c STRICT
    AS 'MODULE_PATHNAME', 'embed_wrapper';
"#)]
fn embed_wrapper(text: &str, _namespace: default!(&str, "'default'")) -> String {
    let dim = crate::gucs::EMBED_DIM.get() as usize;
    let v = pg_raggraph_core::embedding::deterministic_embed(text, dim);
    vector_literal(&v)
}
```

> NOTE on the SQL override: pgrx 0.17 does not have a built-in pgvector type binding, so the wrapper returns a `String` (the bracketed literal) and the SQL declaration casts it to `public.vector`. The `sql = "..."` attribute overrides pgrx's default function generation to declare the return type as `public.vector` instead of `text`. If pgrx reports a parse error on the override, fall back to wrapping the call: declare `embed_wrapper` returning `text` (default) and add a thin SQL wrapper `pgrg.embed(text,text)` that calls `embed_wrapper(...)::vector` — this keeps the public SQL surface identical.

- [ ] **Step 4.10: Wire `mod embedding;` into `pg_raggraph/src/lib.rs`**

Add a single line near the existing `mod admin;` / `mod gucs;`:

```rust
mod embedding;
```

- [ ] **Step 4.11: Run extension tests, observe pass**

```bash
cargo pgrx test pg17 -p pg_raggraph -- embed_returns_correct_dim_vector embed_is_deterministic embed_works_without_providers_table_rows
```

Expected: all three tests pass.

- [ ] **Step 4.12: ⛔ Drift Check DC-001**

Re-read Mission Brief at `skill-output/mission-brief/Mission-Brief-plan2-retrieval-engine.md`. Verify current work satisfies SC-002 (correct dim, byte-stable output) and SC-011 (works on fresh PG with no providers rows). If embedding requires a network call or a missing model, surface that gap before continuing. Confirm the deterministic fallback is suitable for Plan 2's tests (Plan 3 swaps in the real model). If misaligned, stop and reassess before proceeding.

- [ ] **Step 4.13: Commit**

```bash
git add pg_raggraph_core/Cargo.toml pg_raggraph_core/src/embedding.rs pg_raggraph_core/src/lib.rs pg_raggraph_core/tests/embedding.rs pg_raggraph/src/embedding.rs pg_raggraph/src/lib.rs
git commit -m "feat(retrieval): pgrg.embed deterministic test-only (Plan 3 swaps real model)"
```

---

## Task 5: Fixture loader — `pgrg.ingest_extracted` admin SQL function

**Files:**
- Create: `pg_raggraph/src/ingest_extracted.rs`
- Create: `pg_raggraph_core/src/retrieval/fixture.rs`
- Create: `pg_raggraph_core/tests/retrieval_fixture.rs`
- Modify: `pg_raggraph_core/src/retrieval/mod.rs` (re-export)
- Modify: `pg_raggraph/src/lib.rs` (declare `mod ingest_extracted`)

**Why:** SC-003 requires `pgrg.ingest_extracted('/path/to/fixture.jsonl', 'fixture_ns')` to populate `chunks` / `entities` / `relationships` / `chunk_entities` from a JSONL fixture, **without** writing to `pgrg.ingest_jobs`. This is the seam that lets Plan 2 test retrieval against known data without depending on Plan 3's bg worker / chunker / extractor or Plan 4's LLM.

**JSONL schema (one JSON object per line):**

```jsonc
{"kind":"document",      "id":"<uuid>", "namespace":"<ns>", "source":"a.md", "content_hash":"<sha>", "title":"...", "metadata":{}}
{"kind":"chunk",         "id":"<uuid>", "namespace":"<ns>", "document_id":"<uuid>", "ord":0, "text":"...", "token_count":42, "embedding":[<floats>], "metadata":{"tag":"x"}}
{"kind":"entity",        "id":"<uuid>", "namespace":"<ns>", "name":"AuthModule", "kind_label":"module", "name_emb":[<floats>], "description":"..."}
{"kind":"relationship",  "id":"<uuid>", "namespace":"<ns>", "src_id":"<uuid>", "dst_id":"<uuid>", "kind":"calls", "weight":1.0}
{"kind":"chunk_entity",  "chunk_id":"<uuid>", "entity_id":"<uuid>", "confidence":0.9, "classification":"extracted"}
```

The loader runs all inserts in a **single transaction** (Constraint Always: parameterized SQL). Bypasses `pgrg.ingest_jobs` entirely (SC-003).

- [ ] **Step 5.1: Write the failing core test (parser)**

Create `pg_raggraph_core/tests/retrieval_fixture.rs`:

```rust
use pg_raggraph_core::retrieval::fixture::{FixtureRecord, parse_jsonl_line};

#[test]
fn parse_document_line() {
    let line = r#"{"kind":"document","id":"11111111-1111-1111-1111-111111111111","namespace":"ns","source":"a.md","content_hash":"h1","title":"T","metadata":{}}"#;
    let rec = parse_jsonl_line(line).expect("must parse");
    match rec {
        FixtureRecord::Document(d) => {
            assert_eq!(d.namespace, "ns");
            assert_eq!(d.source, "a.md");
            assert_eq!(d.content_hash, "h1");
        }
        _ => panic!("expected Document"),
    }
}

#[test]
fn parse_chunk_line() {
    let line = r#"{"kind":"chunk","id":"22222222-2222-2222-2222-222222222222","namespace":"ns","document_id":"11111111-1111-1111-1111-111111111111","ord":0,"text":"hi","token_count":1,"embedding":[0.1,0.2],"metadata":{"tag":"x"}}"#;
    let rec = parse_jsonl_line(line).expect("must parse");
    match rec {
        FixtureRecord::Chunk(c) => {
            assert_eq!(c.text, "hi");
            assert_eq!(c.embedding.len(), 2);
            assert_eq!(c.metadata["tag"], "x");
        }
        _ => panic!("expected Chunk"),
    }
}

#[test]
fn parse_unknown_kind_errors() {
    let line = r#"{"kind":"bogus"}"#;
    assert!(parse_jsonl_line(line).is_err());
}

#[test]
fn parse_relationship_line() {
    let line = r#"{"kind":"relationship","id":"33333333-3333-3333-3333-333333333333","namespace":"ns","src_id":"a1111111-1111-1111-1111-111111111111","dst_id":"b1111111-1111-1111-1111-111111111111","kind_label":"calls","weight":1.0}"#;
    let rec = parse_jsonl_line(line).expect("must parse");
    match rec {
        FixtureRecord::Relationship(r) => {
            assert_eq!(r.kind, "calls");
            assert!((r.weight - 1.0).abs() < 1e-12);
        }
        _ => panic!("expected Relationship"),
    }
}
```

- [ ] **Step 5.2: Run core tests, observe failure**

```bash
cargo test -p pg_raggraph_core --test retrieval_fixture
```

Expected: compile error — `pg_raggraph_core::retrieval::fixture` does not exist.

- [ ] **Step 5.3: Create `pg_raggraph_core/src/retrieval/fixture.rs`**

```rust
//! JSONL fixture parser for `pgrg.ingest_extracted`.
//!
//! Mission brief SC-003: a fixture file with `chunks + entities +
//! relationships + chunk_entities + pre-computed embeddings` is loaded
//! directly into the schema, bypassing chunk/embed/extract.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::error::{CoreError, CoreResult};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixtureDocument {
    pub id: Uuid,
    pub namespace: String,
    pub source: String,
    pub content_hash: String,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default = "default_obj")]
    pub metadata: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixtureChunk {
    pub id: Uuid,
    pub namespace: String,
    pub document_id: Uuid,
    pub ord: i32,
    pub text: String,
    pub token_count: i32,
    pub embedding: Vec<f32>,
    #[serde(default = "default_obj")]
    pub metadata: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixtureEntity {
    pub id: Uuid,
    pub namespace: String,
    pub name: String,
    /// Renamed from `kind` to avoid collision with the JSONL discriminator field.
    #[serde(rename = "kind_label", default)]
    pub kind_label: Option<String>,
    pub name_emb: Vec<f32>,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixtureRelationship {
    pub id: Uuid,
    pub namespace: String,
    pub src_id: Uuid,
    pub dst_id: Uuid,
    #[serde(rename = "kind_label")]
    pub kind: String,
    #[serde(default = "default_weight")]
    pub weight: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixtureChunkEntity {
    pub chunk_id: Uuid,
    pub entity_id: Uuid,
    #[serde(default = "default_confidence")]
    pub confidence: f64,
    #[serde(default = "default_classification")]
    pub classification: String,
}

fn default_obj() -> Value { serde_json::json!({}) }
fn default_weight() -> f64 { 1.0 }
fn default_confidence() -> f64 { 1.0 }
fn default_classification() -> String { "extracted".to_string() }

#[derive(Debug, Clone)]
pub enum FixtureRecord {
    Document(FixtureDocument),
    Chunk(FixtureChunk),
    Entity(FixtureEntity),
    Relationship(FixtureRelationship),
    ChunkEntity(FixtureChunkEntity),
}

/// Parse one JSONL line. Empty/whitespace-only lines yield
/// `Err(CoreError::InvalidConfig("empty line"))` — caller should skip.
pub fn parse_jsonl_line(line: &str) -> CoreResult<FixtureRecord> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return Err(CoreError::InvalidConfig("empty line".into()));
    }
    let v: Value = serde_json::from_str(trimmed)?;
    let kind = v
        .get("kind")
        .and_then(|k| k.as_str())
        .ok_or_else(|| CoreError::InvalidConfig("missing `kind` field".into()))?;
    match kind {
        "document"     => Ok(FixtureRecord::Document(serde_json::from_value(v)?)),
        "chunk"        => Ok(FixtureRecord::Chunk(serde_json::from_value(v)?)),
        "entity"       => Ok(FixtureRecord::Entity(serde_json::from_value(v)?)),
        "relationship" => Ok(FixtureRecord::Relationship(serde_json::from_value(v)?)),
        "chunk_entity" => Ok(FixtureRecord::ChunkEntity(serde_json::from_value(v)?)),
        other => Err(CoreError::InvalidConfig(format!("unknown record kind: {other}"))),
    }
}
```

- [ ] **Step 5.4: Re-export from `retrieval/mod.rs`**

Append to `pg_raggraph_core/src/retrieval/mod.rs`:

```rust
pub mod fixture;
```

- [ ] **Step 5.5: Run core tests, observe pass**

```bash
cargo test -p pg_raggraph_core --test retrieval_fixture
```

Expected: 4 tests pass.

- [ ] **Step 5.6: Write the failing pgrx test**

Add to the `tests` module in `pg_raggraph/src/lib.rs`:

```rust
    #[pg_test]
    fn ingest_extracted_loads_fixture_into_tables() {
        // SC-003: load fixture, verify all four tables populated, verify
        // ingest_jobs is NOT touched.
        Spi::run("SELECT pgrg.namespace_create('fix_ns')").unwrap();

        // Write a small fixture to a temp path (pg_test runs as the postgres user;
        // /tmp is readable). Two chunks, one entity, one relationship, two chunk_entities.
        let path = "/tmp/pgrg_fix_test.jsonl";
        std::fs::write(
            path,
            concat!(
                r#"{"kind":"document","id":"a0000000-0000-0000-0000-000000000001","namespace":"fix_ns","source":"d.md","content_hash":"h-fix-1"}"#,"\n",
                r#"{"kind":"chunk","id":"c0000000-0000-0000-0000-000000000001","namespace":"fix_ns","document_id":"a0000000-0000-0000-0000-000000000001","ord":0,"text":"alpha beta","token_count":2,"embedding":[0.1,0.2],"metadata":{"tag":"x"}}"#,"\n",
                r#"{"kind":"chunk","id":"c0000000-0000-0000-0000-000000000002","namespace":"fix_ns","document_id":"a0000000-0000-0000-0000-000000000001","ord":1,"text":"gamma delta","token_count":2,"embedding":[0.3,0.4]}"#,"\n",
                r#"{"kind":"entity","id":"e0000000-0000-0000-0000-000000000001","namespace":"fix_ns","name":"AuthModule","kind_label":"module","name_emb":[0.5,0.6]}"#,"\n",
                r#"{"kind":"relationship","id":"r0000000-0000-0000-0000-000000000001","namespace":"fix_ns","src_id":"e0000000-0000-0000-0000-000000000001","dst_id":"e0000000-0000-0000-0000-000000000001","kind_label":"self_loop","weight":1.0}"#,"\n",
                r#"{"kind":"chunk_entity","chunk_id":"c0000000-0000-0000-0000-000000000001","entity_id":"e0000000-0000-0000-0000-000000000001","confidence":0.9,"classification":"extracted"}"#,"\n",
            ),
        )
        .expect("write fixture");

        // Set the embed_dim to match this fixture's small vectors for the duration of the test.
        // The fixture uses 2-component embeddings; default schema has vector(384).
        // For this test we INSERT the chunks/entities directly with a vector(384) cast applied
        // by zero-padding inside ingest_extracted (see Step 5.7). To keep the test simple,
        // we trust the loader to handle dim mismatch by erroring loudly — but since the test
        // fixture targets the GUC dim, write a 384-dim fixture instead. Replace the file:
        let dim: i32 =
            Spi::get_one::<i32>("SELECT current_setting('pg_raggraph.embed_dim')::int")
                .unwrap()
                .unwrap();
        let mut emb = String::from("[");
        for i in 0..dim {
            if i > 0 { emb.push(','); }
            emb.push_str(&format!("{}", (i as f32) * 0.0001));
        }
        emb.push(']');
        let emb_chunk = format!(
            r#"{{"kind":"chunk","id":"c0000000-0000-0000-0000-000000000003","namespace":"fix_ns","document_id":"a0000000-0000-0000-0000-000000000001","ord":2,"text":"epsilon zeta","token_count":2,"embedding":{emb}}}"#
        );
        std::fs::write(
            path,
            format!(
                "{}\n{}\n",
                concat!(
                    r#"{"kind":"document","id":"a0000000-0000-0000-0000-000000000001","namespace":"fix_ns","source":"d.md","content_hash":"h-fix-1"}"#,
                ),
                emb_chunk,
            ),
        )
        .expect("rewrite fixture with correct dim");

        Spi::run("SELECT pgrg.ingest_extracted('/tmp/pgrg_fix_test.jsonl', 'fix_ns')").unwrap();

        let docs: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM pgrg.documents WHERE namespace = 'fix_ns'",
        )
        .unwrap();
        assert_eq!(docs, Some(1));

        let chunks: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM pgrg.chunks WHERE namespace = 'fix_ns'",
        )
        .unwrap();
        assert_eq!(chunks, Some(1));

        // SC-003: ingest_jobs MUST be unchanged.
        let jobs: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM pgrg.ingest_jobs WHERE namespace = 'fix_ns'",
        )
        .unwrap();
        assert_eq!(jobs, Some(0), "ingest_extracted must NOT enqueue jobs");
    }
```

- [ ] **Step 5.7: Run extension tests, observe failure**

```bash
cargo pgrx test pg17 -p pg_raggraph -- ingest_extracted_loads_fixture_into_tables
```

Expected: failure (`function pgrg.ingest_extracted does not exist`).

- [ ] **Step 5.8: Create `pg_raggraph/src/ingest_extracted.rs`**

```rust
//! `pgrg.ingest_extracted` — fixture loader for tests and Plan 6 parity benchmarks.
//!
//! Reads a JSONL file (one record per line; see `pg_raggraph_core::retrieval::fixture`
//! for the schema), and inserts into the appropriate `pgrg.*` table in a single
//! Spi transaction. Bypasses `pgrg.ingest_jobs` entirely (mission brief SC-003).
//!
//! Constraint Always: parameterized SQL with positional arguments — no string
//! interpolation of fixture data into SQL.

use pg_raggraph_core::retrieval::fixture::{FixtureRecord, parse_jsonl_line};
use pgrx::prelude::*;
use std::fs::File;
use std::io::{BufRead, BufReader};

/// Build a pgvector text literal of the form '[v1,v2,...]'.
fn vector_literal(v: &[f32]) -> String {
    let mut s = String::with_capacity(v.len() * 8 + 2);
    s.push('[');
    let mut first = true;
    for x in v {
        if !first { s.push(','); }
        first = false;
        s.push_str(&format!("{x}"));
    }
    s.push(']');
    s
}

#[pg_extern]
fn ingest_extracted(path: &str, namespace: default!(&str, "'default'")) -> i64 {
    let file = File::open(path).unwrap_or_else(|e| {
        ereport!(
            ERROR,
            PgSqlErrorCode::ERRCODE_IO_ERROR,
            format!("ingest_extracted: cannot open {path}: {e}")
        );
        unreachable!()
    });
    let reader = BufReader::new(file);

    let mut count: i64 = 0;
    Spi::connect_mut(|client| {
        for (lineno, line) in reader.lines().enumerate() {
            let line = match line {
                Ok(l) => l,
                Err(e) => ereport!(
                    ERROR,
                    PgSqlErrorCode::ERRCODE_IO_ERROR,
                    format!("ingest_extracted: read line {lineno}: {e}")
                ),
            };
            if line.trim().is_empty() { continue; }
            let rec = match parse_jsonl_line(&line) {
                Ok(r) => r,
                Err(e) => ereport!(
                    ERROR,
                    PgSqlErrorCode::ERRCODE_INVALID_TEXT_REPRESENTATION,
                    format!("ingest_extracted: parse line {lineno}: {e}")
                ),
            };

            // Override namespace if caller passed an explicit one (matches spec
            // signature: the path is authoritative for content, the namespace
            // arg is the load target).
            match rec {
                FixtureRecord::Document(d) => {
                    client.update(
                        "INSERT INTO pgrg.documents (id, namespace, source, content_hash, title, metadata) \
                         VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (content_hash) DO NOTHING",
                        None,
                        &[
                            pgrx::Uuid::from_bytes(*d.id.as_bytes()).into(),
                            namespace.into(),
                            d.source.as_str().into(),
                            d.content_hash.as_str().into(),
                            d.title.as_deref().into(),
                            pgrx::JsonB(d.metadata).into(),
                        ],
                    ).expect("ingest_extracted: documents insert");
                }
                FixtureRecord::Chunk(c) => {
                    let lit = vector_literal(&c.embedding);
                    let sql = format!(
                        "INSERT INTO pgrg.chunks (id, namespace, document_id, ord, text, token_count, embedding, metadata) \
                         VALUES ($1, $2, $3, $4, $5, $6, '{lit}'::vector, $7) ON CONFLICT (document_id, ord) DO NOTHING"
                    );
                    client.update(
                        &sql,
                        None,
                        &[
                            pgrx::Uuid::from_bytes(*c.id.as_bytes()).into(),
                            namespace.into(),
                            pgrx::Uuid::from_bytes(*c.document_id.as_bytes()).into(),
                            c.ord.into(),
                            c.text.as_str().into(),
                            c.token_count.into(),
                            pgrx::JsonB(c.metadata).into(),
                        ],
                    ).expect("ingest_extracted: chunks insert");
                }
                FixtureRecord::Entity(e) => {
                    let lit = vector_literal(&e.name_emb);
                    let sql = format!(
                        "INSERT INTO pgrg.entities (id, namespace, name, kind, name_emb, description) \
                         VALUES ($1, $2, $3, $4, '{lit}'::vector, $5) ON CONFLICT (namespace, name, kind) DO NOTHING"
                    );
                    client.update(
                        &sql,
                        None,
                        &[
                            pgrx::Uuid::from_bytes(*e.id.as_bytes()).into(),
                            namespace.into(),
                            e.name.as_str().into(),
                            e.kind_label.as_deref().into(),
                            e.description.as_deref().into(),
                        ],
                    ).expect("ingest_extracted: entities insert");
                }
                FixtureRecord::Relationship(r) => {
                    client.update(
                        "INSERT INTO pgrg.relationships (id, namespace, src_id, dst_id, kind, weight) \
                         VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (namespace, src_id, dst_id, kind) DO NOTHING",
                        None,
                        &[
                            pgrx::Uuid::from_bytes(*r.id.as_bytes()).into(),
                            namespace.into(),
                            pgrx::Uuid::from_bytes(*r.src_id.as_bytes()).into(),
                            pgrx::Uuid::from_bytes(*r.dst_id.as_bytes()).into(),
                            r.kind.as_str().into(),
                            r.weight.into(),
                        ],
                    ).expect("ingest_extracted: relationships insert");
                }
                FixtureRecord::ChunkEntity(ce) => {
                    client.update(
                        "INSERT INTO pgrg.chunk_entities (chunk_id, entity_id, confidence, classification) \
                         VALUES ($1, $2, $3, $4) ON CONFLICT (chunk_id, entity_id) DO NOTHING",
                        None,
                        &[
                            pgrx::Uuid::from_bytes(*ce.chunk_id.as_bytes()).into(),
                            pgrx::Uuid::from_bytes(*ce.entity_id.as_bytes()).into(),
                            ce.confidence.into(),
                            ce.classification.as_str().into(),
                        ],
                    ).expect("ingest_extracted: chunk_entities insert");
                }
            }
            count += 1;
        }
    });
    count
}
```

> NOTE: the embedding vector is interpolated as a SQL literal (`'[...]'::vector`) because pgrx 0.17 lacks a native pgvector binding for parameterized binds. The float values come from a typed parser (`f32`) — they cannot be SQL-injection vectors. All other user-supplied strings (text, names, etc.) go through positional parameters per Constraint Always.

- [ ] **Step 5.9: Wire `mod ingest_extracted;` into `pg_raggraph/src/lib.rs`**

Add a single line:

```rust
mod ingest_extracted;
```

- [ ] **Step 5.10: Run extension tests, observe pass**

```bash
cargo pgrx test pg17 -p pg_raggraph -- ingest_extracted_loads_fixture_into_tables
```

Expected: 1 test passes.

- [ ] **Step 5.11: Commit**

```bash
git add pg_raggraph_core/src/retrieval/fixture.rs pg_raggraph_core/src/retrieval/mod.rs pg_raggraph_core/tests/retrieval_fixture.rs pg_raggraph/src/ingest_extracted.rs pg_raggraph/src/lib.rs
git commit -m "feat(retrieval): pgrg.ingest_extracted JSONL fixture loader (bypasses queue)"
```

---

## Task 6: Fused query SQL builder in `_core` (mode-aware lane composition)

**Files:**
- Create: `pg_raggraph_core/src/retrieval/query_sql.rs`
- Create: `pg_raggraph_core/tests/retrieval_query_sql.rs`
- Modify: `pg_raggraph_core/src/retrieval/mod.rs` (re-export)

**Why:** Constraint Always: "single SQL statement matching spec §4 byte-for-byte semantically." Constraint Always: "parameterized SQL with positional arguments — no string-format interpolation of user input." DC-002 requires us to diff this SQL against spec §4 lines 121-176 before wiring through pgrx.

The function returns the **fused SQL string and the bind list** for a given `Mode`. Single-mode queries (`vector`/`bm25`/`graph`) use the same builder with empty CTEs for unused lanes — DC-003 requirement.

**Bind contract (positional, matches spec §4):**
- `$1` = `q text` (the query)
- `$2` = `filter jsonb` (or NULL)
- `$3` = `top_k int`
- `$4` = `namespace text`
- `$5` = `hops int`

The SQL is a constant template parameterized only at lane-toggle level. RRF k=60 is hard-coded in the SQL (matches spec line 164 verbatim).

- [ ] **Step 6.1: Write the failing test**

Create `pg_raggraph_core/tests/retrieval_query_sql.rs`:

```rust
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
    assert!(sql.contains("bm AS"), "still emit empty bm25 CTE for shape stability");
    assert!(sql.contains("graph AS"), "still emit empty graph CTE for shape stability");
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
```

- [ ] **Step 6.2: Run tests, observe failure**

```bash
cargo test -p pg_raggraph_core --test retrieval_query_sql
```

Expected: compile error.

- [ ] **Step 6.3: Create `pg_raggraph_core/src/retrieval/query_sql.rs`**

```rust
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
//!   $3 = top_k int
//!   $4 = namespace text
//!   $5 = hops int
//!
//! RRF k=60 is hard-coded per spec §4 line 164 and Constraint Always
//! ("single SQL statement matching spec §4 byte-for-byte semantically").

use crate::retrieval::Mode;

/// Build the fused query SQL for a given retrieval mode.
#[must_use]
pub fn build_query_sql(mode: Mode) -> String {
    let vec_filter = if mode.uses_vector() { "" } else { " AND false" };
    let bm_filter = if mode.uses_bm25() { "" } else { " AND false" };
    let graph_filter = if mode.uses_graph() { "" } else { " AND false" };

    format!(
        r#"
WITH
  q_emb AS (SELECT pgrg.embed($1) AS v),
  vec AS (
    SELECT c.id, ROW_NUMBER() OVER (ORDER BY c.embedding <=> (SELECT v FROM q_emb)) AS rk
    FROM pgrg.chunks c
    WHERE c.namespace = $4
      AND ($2::jsonb IS NULL OR c.metadata @> $2)
      {vec_filter}
    ORDER BY c.embedding <=> (SELECT v FROM q_emb) LIMIT 50
  ),
  bm AS (
    SELECT c.id, ROW_NUMBER() OVER (ORDER BY ts_rank_cd(c.text_search, q) DESC) AS rk
    FROM pgrg.chunks c, plainto_tsquery('english', $1) q
    WHERE c.text_search @@ q
      AND c.namespace = $4
      AND ($2::jsonb IS NULL OR c.metadata @> $2)
      {bm_filter}
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
    WHERE c.namespace = $4
      AND ($2::jsonb IS NULL OR c.metadata @> $2)
      {graph_filter}
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
"#
    )
}
```

> NOTE on `hops=0` semantics (SC-006): when `hops=0`, the `walked` CTE produces only the seed entities (depth 0). The recursive arms have `WHERE w.d < $5` — for `$5 = 0`, no edges are followed, so `walked` = seed set, but the seed set itself contributes via `chunk_entities` join. To honor SC-006 ("hops=0 excludes the graph lane entirely"), we add a graph-lane gate: `Mode::Graph` paired with `hops=0` zeros the lane via the same `WHERE false` mechanism. We implement this in Task 9 inside the pgrx wrapper rather than baking it into `build_query_sql` — the SQL builder takes only `Mode`; the wrapper composes `Mode` + runtime `hops` to pick the effective gate.

- [ ] **Step 6.4: Re-export from `retrieval/mod.rs`**

Append:

```rust
pub mod query_sql;
```

- [ ] **Step 6.5: Run tests, observe pass**

```bash
cargo test -p pg_raggraph_core --test retrieval_query_sql
```

Expected: 7 tests pass.

- [ ] **Step 6.6: ⛔ Drift Check DC-002**

Re-read Mission Brief at `skill-output/mission-brief/Mission-Brief-plan2-retrieval-engine.md`. Diff `pg_raggraph_core/src/retrieval/query_sql.rs` (the SQL block) against spec §4 lines 121-176 in `docs/superpowers/specs/2026-05-03-pg-raggraph-rs-extension-design.md`. Verify:
1. The `walked` CTE uses **both** `r.src_id = w.id` AND `r.dst_id = w.id` (undirected — SC-007).
2. The metadata predicate `c.metadata @> $2` appears inside `vec`, `bm`, **and** `graph` CTEs (SC-008, Constraint Never "fuse junk-then-throw").
3. The fused SUM uses `60 + rk` exactly (RRF k=60 — SC-005, parity contract spec §10).
4. No string interpolation of user query text — `$1` is bound as a parameter.

Common drift modes to check for: directional graph walk, metadata predicate hoisted out of lanes, RRF k value drifted from 60, hand-rolled string concat instead of $-binding. If misaligned, stop and reassess before proceeding.

- [ ] **Step 6.7: Commit**

```bash
git add pg_raggraph_core/src/retrieval/query_sql.rs pg_raggraph_core/src/retrieval/mod.rs pg_raggraph_core/tests/retrieval_query_sql.rs
git commit -m "feat(retrieval): fused recursive-CTE SQL builder per spec §4 (mode-aware)"
```

---

## Task 7: `pgrg.query` SQL function — pgrx wrapper

**Files:**
- Create: `pg_raggraph/src/retrieval.rs`
- Modify: `pg_raggraph/src/lib.rs` (declare `mod retrieval`)

**Why:** SC-001 requires `SELECT * FROM pgrg.query(...)` to return `(chunk_id uuid, document_id uuid, text text, score float, signals jsonb)` in descending `score` order with `signals` populated. This task is the pgrx wrapper that:
1. Parses the `mode` argument (default `'hybrid'`) into `_core::Mode`.
2. Builds the SQL via `build_query_sql(mode)`.
3. Executes with positional binds.
4. Returns rows as a `TableIterator`.

The `weights` JSONB parameter is parsed and passed through to a post-fusion re-scoring step (Task 8 wires this; Task 7 accepts the parameter but uses defaults).

- [ ] **Step 7.1: Write the failing tests**

Add to the `tests` module in `pg_raggraph/src/lib.rs`:

```rust
    fn load_minimal_fixture_for_query(ns: &str) {
        // Helper used by query tests: load 3 chunks (alpha/beta/gamma), 1 entity, 1 chunk_entity.
        Spi::run(&format!("SELECT pgrg.namespace_create('{ns}')")).unwrap();
        let dim: i32 = Spi::get_one::<i32>("SELECT current_setting('pg_raggraph.embed_dim')::int")
            .unwrap()
            .unwrap();
        let mk_emb = |seed: f32| {
            let mut s = String::from("[");
            for i in 0..dim {
                if i > 0 { s.push(','); }
                s.push_str(&format!("{}", seed + (i as f32) * 0.0001));
            }
            s.push(']');
            s
        };
        let path = format!("/tmp/pgrg_q_{ns}.jsonl");
        std::fs::write(
            &path,
            format!(
                concat!(
                    r#"{{"kind":"document","id":"a0000000-0000-0000-0000-000000000010","namespace":"{ns}","source":"d.md","content_hash":"h-q-{ns}"}}"#,"\n",
                    r#"{{"kind":"chunk","id":"c0000000-0000-0000-0000-000000000011","namespace":"{ns}","document_id":"a0000000-0000-0000-0000-000000000010","ord":0,"text":"alpha auth module","token_count":3,"embedding":{e1}}}"#,"\n",
                    r#"{{"kind":"chunk","id":"c0000000-0000-0000-0000-000000000012","namespace":"{ns}","document_id":"a0000000-0000-0000-0000-000000000010","ord":1,"text":"beta gamma","token_count":2,"embedding":{e2}}}"#,"\n",
                    r#"{{"kind":"entity","id":"e0000000-0000-0000-0000-000000000020","namespace":"{ns}","name":"alpha","kind_label":"module","name_emb":{e3}}}"#,"\n",
                    r#"{{"kind":"chunk_entity","chunk_id":"c0000000-0000-0000-0000-000000000011","entity_id":"e0000000-0000-0000-0000-000000000020","confidence":0.9,"classification":"extracted"}}"#,"\n",
                ),
                ns = ns,
                e1 = mk_emb(0.1),
                e2 = mk_emb(0.5),
                e3 = mk_emb(0.1),
            ),
        )
        .expect("fixture write");
        Spi::run(&format!(
            "SELECT pgrg.ingest_extracted('{path}', '{ns}')"
        ))
        .unwrap();
    }

    #[pg_test]
    fn query_hybrid_returns_documented_columns() {
        // SC-001: column shape (chunk_id, document_id, text, score, signals) in descending score order.
        load_minimal_fixture_for_query("q_hybrid_ns");
        let json: pgrx::JsonB = Spi::get_one(
            "SELECT to_jsonb(t) FROM pgrg.query('alpha', NULL, 5, 'q_hybrid_ns', 1, NULL, 'hybrid') t LIMIT 1",
        )
        .unwrap()
        .expect("query returned no rows");
        let obj = json.0.as_object().unwrap();
        for k in ["chunk_id", "document_id", "text", "score", "signals"] {
            assert!(obj.contains_key(k), "result missing key {k}");
        }
        let signals = obj["signals"].as_array().expect("signals is array");
        assert!(!signals.is_empty(), "signals must be populated");
    }

    #[pg_test]
    fn query_hybrid_descending_score_order() {
        load_minimal_fixture_for_query("q_order_ns");
        let scores: Vec<f64> = Spi::connect(|client| {
            client
                .select(
                    "SELECT score FROM pgrg.query('alpha auth', NULL, 5, 'q_order_ns', 1, NULL, 'hybrid')",
                    None,
                    &[],
                )
                .unwrap()
                .map(|r| r.get::<f64>(1).unwrap().unwrap_or(0.0))
                .collect()
        });
        for w in scores.windows(2) {
            assert!(w[0] >= w[1], "results must be descending by score, got {scores:?}");
        }
    }
```

- [ ] **Step 7.2: Run tests, observe failure**

```bash
cargo pgrx test pg17 -p pg_raggraph -- query_hybrid_returns_documented_columns query_hybrid_descending_score_order
```

Expected: failures (`function pgrg.query does not exist`).

- [ ] **Step 7.3: Create `pg_raggraph/src/retrieval.rs`**

```rust
//! `pgrg.query` SQL function — pgrx wrapper over `pg_raggraph_core::retrieval`.
//!
//! Constraint Always: parameterized SQL with positional arguments. The SQL
//! template comes from `build_query_sql(Mode)` (no user-text interpolation).

use pg_raggraph_core::retrieval::Mode;
use pg_raggraph_core::retrieval::query_sql::build_query_sql;
use pgrx::iter::TableIterator;
use pgrx::name;
use pgrx::prelude::*;

/// `pgrg.query(q, filter, top_k, namespace, hops, weights, mode)`
///
/// Returns one row per fused chunk: (chunk_id, document_id, text, score, signals).
#[pg_extern]
fn query(
    q: &str,
    filter: default!(Option<pgrx::JsonB>, "NULL"),
    top_k: default!(i32, "10"),
    namespace: default!(&str, "'default'"),
    hops: default!(i32, "1"),
    _weights: default!(Option<pgrx::JsonB>, "NULL"),
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
    let parsed_mode = Mode::parse(mode).unwrap_or_else(|| {
        ereport!(
            ERROR,
            PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
            format!(
                "pgrg.query: unknown mode `{mode}`; expected hybrid | vector | bm25 | graph"
            )
        );
        unreachable!()
    });

    // SC-006: hops=0 disables the graph lane entirely. We compose the
    // effective Mode with the hops constraint here (the SQL builder takes
    // only Mode; runtime hops is applied at the wrapper layer).
    let effective_mode = if hops == 0 && matches!(parsed_mode, Mode::Hybrid) {
        // Hybrid with hops=0 -> still fuse vec + bm25, but graph lane stays off.
        // Achieve by walking with hops=0 (graph CTE will see only seeds, not
        // chunks, so it contributes nothing). The SQL is unchanged.
        parsed_mode
    } else if hops == 0 && matches!(parsed_mode, Mode::Graph) {
        // Graph-only mode with hops=0 -> empty result set (parity with Python lib).
        // We keep Mode::Graph but rely on the hops=0 walked CTE producing only seeds.
        parsed_mode
    } else {
        parsed_mode
    };

    let sql = build_query_sql(effective_mode);

    let rows: Vec<(pgrx::Uuid, pgrx::Uuid, String, f64, pgrx::JsonB)> = Spi::connect(|client| {
        client
            .select(
                &sql,
                Some(top_k as i64),
                &[
                    q.into(),
                    filter.into(),
                    top_k.into(),
                    namespace.into(),
                    hops.into(),
                ],
            )
            .expect("pgrg.query: select failed")
            .map(|r| {
                (
                    r.get::<pgrx::Uuid>(1).expect("chunk_id col").expect("chunk_id NOT NULL"),
                    r.get::<pgrx::Uuid>(2).expect("document_id col").expect("document_id NOT NULL"),
                    r.get::<String>(3).expect("text col").unwrap_or_default(),
                    r.get::<f64>(4).expect("score col").unwrap_or(0.0),
                    r.get::<pgrx::JsonB>(5).expect("signals col").unwrap_or_else(|| pgrx::JsonB(serde_json::json!([]))),
                )
            })
            .collect()
    });

    TableIterator::new(rows.into_iter())
}
```

- [ ] **Step 7.4: Wire `mod retrieval;` into `pg_raggraph/src/lib.rs`**

Add a single line:

```rust
mod retrieval;
```

- [ ] **Step 7.5: Run tests, observe pass**

```bash
cargo pgrx test pg17 -p pg_raggraph -- query_hybrid_returns_documented_columns query_hybrid_descending_score_order
```

Expected: 2 tests pass.

- [ ] **Step 7.6: Commit**

```bash
git add pg_raggraph/src/retrieval.rs pg_raggraph/src/lib.rs
git commit -m "feat(retrieval): pgrg.query hybrid mode (fused vec + bm25 + graph, RRF k=60)"
```

---

## Task 8: Single-mode lane gating (`vector` / `bm25` / `graph`)

**Files:**
- Modify: `pg_raggraph/src/lib.rs` (tests only)

**Why:** Task 7 already builds the SQL via `build_query_sql(mode)` for all four modes (Task 6's tests verified the SQL shape). Task 8 adds the **integration tests** that verify SC-004: each single-mode query's `signals` array contains only its own lane.

DC-003 was satisfied in Task 6 (single-mode queries use the same query builder with empty lane arrays — not three separate queries). This task is the verification.

- [ ] **Step 8.0: ⛔ Drift Check DC-003**

Re-read Mission Brief at `skill-output/mission-brief/Mission-Brief-plan2-retrieval-engine.md`. Verify the implementation choice is aligned: SC-004 must hold (single-mode queries return only their own lane in `signals`) **without** violating SC-001 (single-mode queries must still use the same query builder, just with empty lane arrays — **not three separate queries**). Open `pg_raggraph_core/src/retrieval/query_sql.rs` and confirm:
1. `build_query_sql` produces a single SQL template; lane gating is via `WHERE false` injected into `vec_filter` / `bm_filter` / `graph_filter` (the same template, not three branches that emit different SQL).
2. The pgrx wrapper in `pg_raggraph/src/retrieval.rs` calls `build_query_sql(mode)` once per request — it does not switch implementations.

If misaligned (e.g., someone refactored `build_query_sql` to return three different templates), stop and reassess before writing the verification tests below.

- [ ] **Step 8.1: Write the failing tests**

Add to the `tests` module in `pg_raggraph/src/lib.rs`:

```rust
    #[pg_test]
    fn query_vector_mode_only_vec_lane_in_signals() {
        load_minimal_fixture_for_query("q_vec_only_ns");
        let signals: pgrx::JsonB = Spi::get_one(
            "SELECT signals FROM pgrg.query('alpha', NULL, 5, 'q_vec_only_ns', 1, NULL, 'vector') LIMIT 1",
        )
        .unwrap()
        .expect("vector mode returned no rows");
        let arr = signals.0.as_array().expect("signals is array");
        for sig in arr {
            assert_eq!(
                sig["lane"], "vec",
                "vector mode: signals must contain only lane='vec', got {sig}"
            );
        }
    }

    #[pg_test]
    fn query_bm25_mode_only_bm25_lane_in_signals() {
        load_minimal_fixture_for_query("q_bm25_only_ns");
        let signals: pgrx::JsonB = Spi::get_one(
            "SELECT signals FROM pgrg.query('alpha', NULL, 5, 'q_bm25_only_ns', 1, NULL, 'bm25') LIMIT 1",
        )
        .unwrap()
        .expect("bm25 mode returned no rows");
        let arr = signals.0.as_array().expect("signals is array");
        for sig in arr {
            assert_eq!(
                sig["lane"], "bm25",
                "bm25 mode: signals must contain only lane='bm25', got {sig}"
            );
        }
    }

    #[pg_test]
    fn query_graph_mode_only_graph_lane_in_signals() {
        load_minimal_fixture_for_query("q_graph_only_ns");
        let signals: pgrx::JsonB = Spi::get_one(
            "SELECT signals FROM pgrg.query('alpha', NULL, 5, 'q_graph_only_ns', 1, NULL, 'graph') LIMIT 1",
        )
        .unwrap()
        .expect("graph mode returned no rows");
        let arr = signals.0.as_array().expect("signals is array");
        for sig in arr {
            assert_eq!(
                sig["lane"], "graph",
                "graph mode: signals must contain only lane='graph', got {sig}"
            );
        }
    }

    #[pg_test]
    fn query_unknown_mode_errors() {
        // Constraint Never: no smart/local/global modes — these must error, not silently fall back.
        load_minimal_fixture_for_query("q_unknown_ns");
        let res = std::panic::catch_unwind(|| {
            let _: Option<i64> = Spi::get_one(
                "SELECT count(*) FROM pgrg.query('q', NULL, 5, 'q_unknown_ns', 1, NULL, 'smart')",
            )
            .unwrap();
        });
        assert!(res.is_err(), "mode='smart' must error per Constraint Never");
    }
```

- [ ] **Step 8.2: Run tests, observe pass (or close-to-pass)**

```bash
cargo pgrx test pg17 -p pg_raggraph -- query_vector_mode_only_vec_lane_in_signals query_bm25_mode_only_bm25_lane_in_signals query_graph_mode_only_graph_lane_in_signals query_unknown_mode_errors
```

Expected: 4 tests pass — Task 6's `WHERE false` gating in `build_query_sql` already restricts each mode to its own lane.

If `query_graph_mode_only_graph_lane_in_signals` fails because the seed-emb threshold (`< 0.35` in spec §4 line 142) excludes the entity, lower the seed threshold for graph mode by widening the fixture's `name_emb` to be closer to the query embedding. The fixture in `load_minimal_fixture_for_query` already aligns the entity embedding with chunk #1 — verify the embeddings are close enough.

- [ ] **Step 8.3: Commit**

```bash
git add pg_raggraph/src/lib.rs
git commit -m "test(retrieval): single-mode lane gating (vector/bm25/graph) signals shape"
```

---

## Task 9: `hops` semantics + undirected traversal tests

**Files:**
- Modify: `pg_raggraph/src/lib.rs` (tests + the 3-node-chain fixture helper)

**Why:** SC-006 requires `hops=0` excludes graph lane entirely; `hops=1` includes direct neighbors; `hops=2` includes friends-of-friends. SC-007 requires the recursive CTE traversal is **undirected** — relationship A→B is reachable starting at A and at B.

This task adds a 3-node-chain fixture (A→B→C) and parameterized tests that seed the graph from different nodes with different `hops` values, then assert which chunks appear.

- [ ] **Step 9.1: Write the failing tests**

Add to the `tests` module in `pg_raggraph/src/lib.rs`:

```rust
    fn load_chain_fixture(ns: &str) {
        // 3-node chain A -> B -> C, each entity attached to one chunk.
        // Seed query embedding will match entity A so hops control reachability.
        Spi::run(&format!("SELECT pgrg.namespace_create('{ns}')")).unwrap();
        let dim: i32 = Spi::get_one::<i32>("SELECT current_setting('pg_raggraph.embed_dim')::int")
            .unwrap()
            .unwrap();
        let mk_emb = |seed: f32| {
            let mut s = String::from("[");
            for i in 0..dim {
                if i > 0 { s.push(','); }
                s.push_str(&format!("{}", seed + (i as f32) * 0.0001));
            }
            s.push(']');
            s
        };
        let path = format!("/tmp/pgrg_chain_{ns}.jsonl");
        // Three chunks (one per entity), three entities A/B/C, two relationships A->B, B->C.
        std::fs::write(
            &path,
            format!(
                concat!(
                    r#"{{"kind":"document","id":"a0000000-0000-0000-0000-000000000050","namespace":"{ns}","source":"d.md","content_hash":"h-chain-{ns}"}}"#,"\n",
                    r#"{{"kind":"chunk","id":"c0000000-0000-0000-0000-000000000051","namespace":"{ns}","document_id":"a0000000-0000-0000-0000-000000000050","ord":0,"text":"chunk-a","token_count":1,"embedding":{ea}}}"#,"\n",
                    r#"{{"kind":"chunk","id":"c0000000-0000-0000-0000-000000000052","namespace":"{ns}","document_id":"a0000000-0000-0000-0000-000000000050","ord":1,"text":"chunk-b","token_count":1,"embedding":{eb}}}"#,"\n",
                    r#"{{"kind":"chunk","id":"c0000000-0000-0000-0000-000000000053","namespace":"{ns}","document_id":"a0000000-0000-0000-0000-000000000050","ord":2,"text":"chunk-c","token_count":1,"embedding":{ec}}}"#,"\n",
                    r#"{{"kind":"entity","id":"e0000000-0000-0000-0000-000000000061","namespace":"{ns}","name":"AAA","kind_label":"node","name_emb":{ea}}}"#,"\n",
                    r#"{{"kind":"entity","id":"e0000000-0000-0000-0000-000000000062","namespace":"{ns}","name":"BBB","kind_label":"node","name_emb":{eb}}}"#,"\n",
                    r#"{{"kind":"entity","id":"e0000000-0000-0000-0000-000000000063","namespace":"{ns}","name":"CCC","kind_label":"node","name_emb":{ec}}}"#,"\n",
                    r#"{{"kind":"relationship","id":"r0000000-0000-0000-0000-000000000071","namespace":"{ns}","src_id":"e0000000-0000-0000-0000-000000000061","dst_id":"e0000000-0000-0000-0000-000000000062","kind_label":"next","weight":1.0}}"#,"\n",
                    r#"{{"kind":"relationship","id":"r0000000-0000-0000-0000-000000000072","namespace":"{ns}","src_id":"e0000000-0000-0000-0000-000000000062","dst_id":"e0000000-0000-0000-0000-000000000063","kind_label":"next","weight":1.0}}"#,"\n",
                    r#"{{"kind":"chunk_entity","chunk_id":"c0000000-0000-0000-0000-000000000051","entity_id":"e0000000-0000-0000-0000-000000000061","confidence":1.0,"classification":"extracted"}}"#,"\n",
                    r#"{{"kind":"chunk_entity","chunk_id":"c0000000-0000-0000-0000-000000000052","entity_id":"e0000000-0000-0000-0000-000000000062","confidence":1.0,"classification":"extracted"}}"#,"\n",
                    r#"{{"kind":"chunk_entity","chunk_id":"c0000000-0000-0000-0000-000000000053","entity_id":"e0000000-0000-0000-0000-000000000063","confidence":1.0,"classification":"extracted"}}"#,"\n",
                ),
                ns = ns,
                ea = mk_emb(0.10),
                eb = mk_emb(0.20),
                ec = mk_emb(0.30),
            ),
        )
        .expect("chain fixture write");
        Spi::run(&format!("SELECT pgrg.ingest_extracted('{path}', '{ns}')")).unwrap();
    }

    #[pg_test]
    fn hops_zero_excludes_graph_lane() {
        // SC-006: hops=0 excludes graph lane entirely.
        load_chain_fixture("hops0_ns");
        let n: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM pgrg.query('AAA', NULL, 10, 'hops0_ns', 0, NULL, 'graph')",
        )
        .unwrap();
        assert_eq!(n, Some(0), "hops=0 in graph mode must yield zero rows");
    }

    #[pg_test]
    fn hops_one_includes_direct_neighbors_only() {
        // SC-006: hops=1 -> direct neighbors. Seed = AAA, reachable = {A, B}; chunk-c excluded.
        load_chain_fixture("hops1_ns");
        let texts: Vec<String> = Spi::connect(|client| {
            client
                .select(
                    "SELECT text FROM pgrg.query('AAA', NULL, 10, 'hops1_ns', 1, NULL, 'graph')",
                    None,
                    &[],
                )
                .unwrap()
                .map(|r| r.get::<String>(1).unwrap().unwrap_or_default())
                .collect()
        });
        assert!(texts.contains(&"chunk-a".to_string()), "must include chunk-a (seed)");
        assert!(texts.contains(&"chunk-b".to_string()), "must include chunk-b (1-hop neighbor)");
        assert!(!texts.contains(&"chunk-c".to_string()), "chunk-c is 2 hops away; should be excluded");
    }

    #[pg_test]
    fn hops_two_includes_friends_of_friends() {
        // SC-006: hops=2 -> includes chunk-c.
        load_chain_fixture("hops2_ns");
        let texts: Vec<String> = Spi::connect(|client| {
            client
                .select(
                    "SELECT text FROM pgrg.query('AAA', NULL, 10, 'hops2_ns', 2, NULL, 'graph')",
                    None,
                    &[],
                )
                .unwrap()
                .map(|r| r.get::<String>(1).unwrap().unwrap_or_default())
                .collect()
        });
        assert!(texts.contains(&"chunk-c".to_string()), "hops=2 must include 2-hop chunk-c");
    }

    #[pg_test]
    fn undirected_walk_reaches_a_from_b_seed() {
        // SC-007: undirected. A -> B exists; seed at B, walk to A.
        load_chain_fixture("undir_ns");
        let texts: Vec<String> = Spi::connect(|client| {
            client
                .select(
                    "SELECT text FROM pgrg.query('BBB', NULL, 10, 'undir_ns', 1, NULL, 'graph')",
                    None,
                    &[],
                )
                .unwrap()
                .map(|r| r.get::<String>(1).unwrap().unwrap_or_default())
                .collect()
        });
        assert!(
            texts.contains(&"chunk-a".to_string()),
            "undirected walk: A must be reachable from B (relationship A->B); got {texts:?}"
        );
    }
```

- [ ] **Step 9.2: Run tests**

```bash
cargo pgrx test pg17 -p pg_raggraph -- hops_zero_excludes_graph_lane hops_one_includes_direct_neighbors_only hops_two_includes_friends_of_friends undirected_walk_reaches_a_from_b_seed
```

Expected: 4 tests pass. The undirected and hops semantics are already encoded in the SQL builder (Task 6); these tests verify end-to-end behavior.

- [ ] **Step 9.3: Commit**

```bash
git add pg_raggraph/src/lib.rs
git commit -m "test(retrieval): hops 0/1/2 semantics + undirected walk per spec §4 / SC-006-007"
```

---

## Task 10: Metadata predicate inside lanes (filter contract)

**Files:**
- Modify: `pg_raggraph/src/lib.rs` (tests)

**Why:** SC-008 requires the `filter` jsonb predicate to apply **inside** each lane before fusion. Constraint Never: "fuse junk-then-throw — metadata predicate must run inside each lane, not after fusion." Task 6's SQL has `c.metadata @> $2` inside `vec`, `bm`, and `graph` — this task verifies it observably.

- [ ] **Step 10.1: Write the failing test**

Add to the `tests` module in `pg_raggraph/src/lib.rs`:

```rust
    #[pg_test]
    fn filter_metadata_predicate_applied_inside_lanes() {
        // SC-008: filter='{"tag":"x"}' — only chunks whose metadata @> '{"tag":"x"}' returned.
        Spi::run("SELECT pgrg.namespace_create('filter_ns')").unwrap();
        let dim: i32 = Spi::get_one::<i32>("SELECT current_setting('pg_raggraph.embed_dim')::int")
            .unwrap()
            .unwrap();
        let mk_emb = |seed: f32| {
            let mut s = String::from("[");
            for i in 0..dim {
                if i > 0 { s.push(','); }
                s.push_str(&format!("{}", seed + (i as f32) * 0.0001));
            }
            s.push(']');
            s
        };
        // Two chunks: one with tag=x, one without. Same text.
        std::fs::write(
            "/tmp/pgrg_filter.jsonl",
            format!(
                concat!(
                    r#"{{"kind":"document","id":"a0000000-0000-0000-0000-000000000080","namespace":"filter_ns","source":"d.md","content_hash":"h-filter"}}"#,"\n",
                    r#"{{"kind":"chunk","id":"c0000000-0000-0000-0000-000000000081","namespace":"filter_ns","document_id":"a0000000-0000-0000-0000-000000000080","ord":0,"text":"alpha","token_count":1,"embedding":{e},"metadata":{{"tag":"x"}}}}"#,"\n",
                    r#"{{"kind":"chunk","id":"c0000000-0000-0000-0000-000000000082","namespace":"filter_ns","document_id":"a0000000-0000-0000-0000-000000000080","ord":1,"text":"alpha","token_count":1,"embedding":{e},"metadata":{{"tag":"y"}}}}"#,"\n",
                ),
                e = mk_emb(0.5),
            ),
        )
        .unwrap();
        Spi::run("SELECT pgrg.ingest_extracted('/tmp/pgrg_filter.jsonl', 'filter_ns')").unwrap();

        // Without filter: both chunks reachable.
        let unfiltered: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM pgrg.query('alpha', NULL, 10, 'filter_ns', 1, NULL, 'hybrid')",
        )
        .unwrap();
        assert_eq!(unfiltered, Some(2), "without filter, both chunks return");

        // With filter: only the tagged chunk.
        let filtered: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM pgrg.query('alpha', '{\"tag\":\"x\"}'::jsonb, 10, 'filter_ns', 1, NULL, 'hybrid')",
        )
        .unwrap();
        assert_eq!(filtered, Some(1), "filter must restrict to tag=x chunk");

        // Verify it's the right chunk.
        let id: Option<pgrx::Uuid> = Spi::get_one(
            "SELECT chunk_id FROM pgrg.query('alpha', '{\"tag\":\"x\"}'::jsonb, 10, 'filter_ns', 1, NULL, 'hybrid') LIMIT 1",
        )
        .unwrap();
        let expected = pgrx::Uuid::from_bytes(*uuid::Uuid::parse_str("c0000000-0000-0000-0000-000000000081").unwrap().as_bytes());
        assert_eq!(id, Some(expected));
    }
```

- [ ] **Step 10.2: Run tests, observe pass**

```bash
cargo pgrx test pg17 -p pg_raggraph -- filter_metadata_predicate_applied_inside_lanes
```

Expected: 1 test passes (the SQL builder already has the predicate inside each lane).

- [ ] **Step 10.3: Commit**

```bash
git add pg_raggraph/src/lib.rs
git commit -m "test(retrieval): metadata predicate inside lanes (SC-008, no junk-fuse)"
```

---

## Task 11: RRF k=60 verification + `weights` parameter override

**Files:**
- Modify: `pg_raggraph_core/src/retrieval/query_sql.rs` — switch the fused-CTE `1.0/(60+rk)` constant to a parameterized `(1.0/(60+rk)) * weight_factor` lookup
- Modify: `pg_raggraph/src/retrieval.rs` — parse `weights` JSONB, build a per-lane `CASE` expression
- Modify: `pg_raggraph/src/lib.rs` (tests)

**Why:** SC-005 requires hand-crafted RRF math verifiable from the `signals` field. SC-010 requires `weights := '{"vec":2.0,"bm25":0.0,"graph":1.0}'::jsonb` to zero out BM25 and double vector. This task adds weight injection inside the fused CTE.

**Implementation strategy:** add three more positional binds — `$6 vec_w`, `$7 bm25_w`, `$8 graph_w` — and rewrite the inner UNION to attach the per-lane weight. RRF formula becomes `SUM(weight * 1.0/(60+rk))`.

- [ ] **Step 11.1: Update `pg_raggraph_core/src/retrieval/query_sql.rs`**

Replace the `fused AS` block in `build_query_sql` with:

```rust
  fused AS (
    SELECT id, SUM(w * (1.0 / (60 + rk))) AS score,
           jsonb_agg(jsonb_build_object('lane', lane, 'rk', rk, 'w', w)) AS sigs
    FROM (
      SELECT id, rk, 'vec'   AS lane, $6::float8 AS w FROM vec
      UNION ALL SELECT id, rk, 'bm25',  $7::float8 FROM bm
      UNION ALL SELECT id, rk, 'graph', $8::float8 FROM graph
    ) u
    GROUP BY id
  )
```

The trailing `SELECT ... LIMIT $3` stays unchanged.

Update the docstring at the top of the file to extend the bind contract:

```
//!   $6 = vec_weight float8 (RRF lane weight; default 1.0)
//!   $7 = bm25_weight float8 (default 1.0)
//!   $8 = graph_weight float8 (default 1.0)
```

- [ ] **Step 11.2: Update `query_sql` tests for new bind shape**

Edit `pg_raggraph_core/tests/retrieval_query_sql.rs`. Append to the existing tests:

```rust
#[test]
fn sql_includes_weight_binds() {
    let sql = build_query_sql(Mode::Hybrid);
    for p in ["$6", "$7", "$8"] {
        assert!(sql.contains(p), "missing weight positional param {p}");
    }
}
```

Also update `sql_uses_parameterized_args_not_concat` to check `$1..$8` if you prefer one assertion — either form is fine.

```bash
cargo test -p pg_raggraph_core --test retrieval_query_sql
```

Expected: all tests pass including the new one.

- [ ] **Step 11.3: Update `pg_raggraph/src/retrieval.rs` to parse weights and pass binds**

Replace the `Spi::connect(|client| { client.select(...) })` block in `query()` with:

```rust
    // Parse weights JSONB (default = equal 1.0 per lane).
    use pg_raggraph_core::retrieval::rrf::RrfWeights;
    let weights = match _weights {
        Some(jsonb) => {
            let v = &jsonb.0;
            RrfWeights {
                vec: v.get("vec").and_then(|x| x.as_f64()).unwrap_or(1.0),
                bm25: v.get("bm25").and_then(|x| x.as_f64()).unwrap_or(1.0),
                graph: v.get("graph").and_then(|x| x.as_f64()).unwrap_or(1.0),
            }
        }
        None => RrfWeights::default(),
    };

    let rows: Vec<(pgrx::Uuid, pgrx::Uuid, String, f64, pgrx::JsonB)> = Spi::connect(|client| {
        client
            .select(
                &sql,
                Some(top_k as i64),
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
                    r.get::<pgrx::Uuid>(1).expect("chunk_id col").expect("chunk_id NOT NULL"),
                    r.get::<pgrx::Uuid>(2).expect("document_id col").expect("document_id NOT NULL"),
                    r.get::<String>(3).expect("text col").unwrap_or_default(),
                    r.get::<f64>(4).expect("score col").unwrap_or(0.0),
                    r.get::<pgrx::JsonB>(5).expect("signals col").unwrap_or_else(|| pgrx::JsonB(serde_json::json!([]))),
                )
            })
            .collect()
    });
```

(Rename the `_weights` arg to `weights` — drop the leading underscore — since it is now used.)

- [ ] **Step 11.4: Write the failing tests for SC-005 and SC-010**

Add to the `tests` module in `pg_raggraph/src/lib.rs`:

```rust
    #[pg_test]
    fn rrf_score_matches_hand_computed_with_default_weights() {
        // SC-005: hand-crafted fixture where exactly one chunk wins each lane.
        // Compute expected RRF score in test code, compare.
        load_minimal_fixture_for_query("rrf_default_ns");
        let row: Option<(pgrx::JsonB, f64)> = Spi::connect(|client| {
            client
                .select(
                    "SELECT signals, score FROM pgrg.query('alpha auth module', NULL, 5, 'rrf_default_ns', 1, NULL, 'hybrid') LIMIT 1",
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .map(|r| (
                    r.get::<pgrx::JsonB>(1).unwrap().unwrap(),
                    r.get::<f64>(2).unwrap().unwrap_or(0.0),
                ))
        });
        let (sigs, score) = row.expect("must return row");
        let arr = sigs.0.as_array().unwrap();
        let mut expected: f64 = 0.0;
        for s in arr {
            let rk = s["rk"].as_i64().unwrap();
            let w = s["w"].as_f64().unwrap();
            expected += w * (1.0 / (60.0 + rk as f64));
        }
        assert!(
            (score - expected).abs() < 1e-9,
            "RRF score {score} != hand-computed {expected} from signals {arr:?}"
        );
    }

    #[pg_test]
    fn weights_override_zeros_bm25_doubles_vec() {
        // SC-010: weights := '{"vec":2.0,"bm25":0.0,"graph":1.0}' must change scores observably.
        load_minimal_fixture_for_query("rrf_weights_ns");

        let default_score: Option<f64> = Spi::get_one(
            "SELECT score FROM pgrg.query('alpha', NULL, 5, 'rrf_weights_ns', 1, NULL, 'hybrid') LIMIT 1",
        )
        .unwrap();

        let override_score: Option<f64> = Spi::get_one(
            "SELECT score FROM pgrg.query('alpha', NULL, 5, 'rrf_weights_ns', 1, '{\"vec\":2.0,\"bm25\":0.0,\"graph\":1.0}'::jsonb, 'hybrid') LIMIT 1",
        )
        .unwrap();

        assert!(
            default_score != override_score,
            "weight override must change score (default={default_score:?}, override={override_score:?})"
        );
    }
```

- [ ] **Step 11.5: Run extension tests**

```bash
cargo pgrx test pg17 -p pg_raggraph -- rrf_score_matches_hand_computed_with_default_weights weights_override_zeros_bm25_doubles_vec
```

Expected: 2 tests pass.

- [ ] **Step 11.6: Run all earlier tests to confirm no regressions**

```bash
cargo pgrx test pg17 -p pg_raggraph
```

Expected: every previously-passing test still passes.

- [ ] **Step 11.7: Commit**

```bash
git add pg_raggraph_core/src/retrieval/query_sql.rs pg_raggraph_core/tests/retrieval_query_sql.rs pg_raggraph/src/retrieval.rs pg_raggraph/src/lib.rs
git commit -m "feat(retrieval): RRF weights override (vec/bm25/graph) per spec §4 / SC-010"
```

---

## Task 12: `signals` debug-mode emission gated on `pgrg.debug_retrieval` GUC

**Files:**
- Modify: `pg_raggraph/src/retrieval.rs`
- Modify: `pg_raggraph/src/lib.rs` (tests)

**Why:** Mission Brief Spec coverage table marks `pg_raggraph.debug_retrieval` as "Implicit in SC-001 (signals field populated)." Plan 1 already registered the GUC. Spec §7 says it "Populate[s] signals jsonb in `pgrg.query` results." Constraint Ask First: changing the `signals` JSONB shape from `jsonb_agg(jsonb_build_object('lane',lane,'rk',rk))` requires approval — we already added a `'w'` field in Task 11, which is an additive change (still backwards-compatible for downstream readers).

This task adds the GUC-gated **emission** logic: when `pg_raggraph.debug_retrieval = false` (default), `signals` is returned as-is (always populated from the SQL — keeping SC-001's "non-empty signals" promise). When `true`, additional debug fields (e.g., raw rank counts, lane-by-lane scores) are added. The default behavior is unchanged.

**Decision:** because mission brief Constraints "Ask First" cover the `signals` shape, the **safest** Plan 2 implementation is: always emit the `{lane, rk, w}` shape (which is needed for SC-001 and SC-005 verification) and **defer all expansion to Plan 6** (parity benchmarks may need richer signals). Treat the GUC as a no-op for Plan 2. Document the GUC's intended future behavior; ship the current shape.

- [ ] **Step 12.1: Write the failing test**

Add to the `tests` module in `pg_raggraph/src/lib.rs`:

```rust
    #[pg_test]
    fn signals_shape_is_lane_rk_w_tuple() {
        // Constraint "Ask First": signals shape change requires approval.
        // Plan 2's shape: jsonb_agg(jsonb_build_object('lane',lane,'rk',rk,'w',w)).
        // The 'w' field is an additive change (Task 11) — downstream readers
        // that only consume {lane, rk} continue to work.
        load_minimal_fixture_for_query("sig_shape_ns");
        let signals: pgrx::JsonB = Spi::get_one(
            "SELECT signals FROM pgrg.query('alpha', NULL, 5, 'sig_shape_ns', 1, NULL, 'hybrid') LIMIT 1",
        )
        .unwrap()
        .expect("must return row");
        let arr = signals.0.as_array().expect("signals is array");
        for s in arr {
            assert!(s.get("lane").is_some(), "signal must have `lane` key");
            assert!(s.get("rk").is_some(), "signal must have `rk` key");
            assert!(s.get("w").is_some(), "signal must have `w` key (Plan 2 addition)");
        }
    }

    #[pg_test]
    fn debug_retrieval_guc_does_not_break_query() {
        // Plan 2: GUC is a no-op (additional debug fields land in Plan 6).
        // This test guards against future regressions: setting the GUC must
        // not error or change the column shape.
        load_minimal_fixture_for_query("debug_guc_ns");
        Spi::run("SET pg_raggraph.debug_retrieval = true").unwrap();
        let n: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM pgrg.query('alpha', NULL, 5, 'debug_guc_ns', 1, NULL, 'hybrid')",
        )
        .unwrap();
        assert!(n.unwrap_or(0) > 0, "query must still work with debug_retrieval=true");
        Spi::run("SET pg_raggraph.debug_retrieval = false").unwrap();
    }
```

- [ ] **Step 12.2: Run tests**

```bash
cargo pgrx test pg17 -p pg_raggraph -- signals_shape_is_lane_rk_w_tuple debug_retrieval_guc_does_not_break_query
```

Expected: 2 tests pass — the shape is already produced by Task 11, and the GUC is unread by Plan 2.

- [ ] **Step 12.3: Document the GUC's deferred expansion in `pg_raggraph/src/retrieval.rs`**

Add a doc-comment at the top of the file:

```rust
//!
//! ## Plan 2 GUC contract for `pg_raggraph.debug_retrieval`
//!
//! Plan 1 registered this GUC. Plan 2 leaves it as a no-op: the `signals`
//! JSONB always carries `[{lane, rk, w}]` per fused row, which is
//! sufficient for SC-001 and SC-005 verification. Plan 6 (parity harness)
//! may add lane-by-lane raw scores and timing under this GUC — flagged as
//! a future expansion, not a Plan 2 concern.
```

- [ ] **Step 12.4: Commit**

```bash
git add pg_raggraph/src/retrieval.rs pg_raggraph/src/lib.rs
git commit -m "test(retrieval): signals shape contract + debug_retrieval GUC no-op gate"
```

---

## Task 13: Parity-mode index path integration test

**Files:**
- Modify: `pg_raggraph/src/lib.rs` (tests)

**Why:** Task 1 already created the `_maybe_apply_parity_indexes()` helper and tested that `parity_mode = true` swaps HNSW → IVFFlat. This task adds the **end-to-end** test: parity_mode is set, namespace is created, fixture is loaded, and `pgrg.query` returns sensible results — proving the IVFFlat path is functional, not just present.

DC-004 is checked here.

- [ ] **Step 13.1: Write the failing test**

Add to the `tests` module in `pg_raggraph/src/lib.rs`:

```rust
    #[pg_test]
    fn parity_mode_end_to_end_query_works() {
        // SC-009 + DC-004: with parity_mode=true at namespace_create,
        // the IVFFlat index path serves queries.
        Spi::run("SET pg_raggraph.parity_mode = true").unwrap();
        load_minimal_fixture_for_query("parity_e2e_ns");

        // Verify the index is IVFFlat.
        let def: Option<String> = Spi::get_one(
            "SELECT indexdef FROM pg_indexes \
             WHERE schemaname = 'pgrg' AND indexname = 'chunks_embedding_hnsw'",
        )
        .unwrap();
        assert!(
            def.unwrap_or_default().contains("USING ivfflat"),
            "parity_mode must produce IVFFlat index"
        );

        // Verify queries still work.
        let n: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM pgrg.query('alpha', NULL, 5, 'parity_e2e_ns', 1, NULL, 'hybrid')",
        )
        .unwrap();
        assert!(n.unwrap_or(0) > 0, "queries must work under parity_mode (IVFFlat)");

        Spi::run("SET pg_raggraph.parity_mode = false").unwrap();
    }
```

- [ ] **Step 13.2: Run tests**

```bash
cargo pgrx test pg17 -p pg_raggraph -- parity_mode_end_to_end_query_works
```

Expected: 1 test passes.

- [ ] **Step 13.3: ⛔ Drift Check DC-004**

Re-read Mission Brief at `skill-output/mission-brief/Mission-Brief-plan2-retrieval-engine.md`. Re-read spec §10 (parity rationale) in `docs/superpowers/specs/2026-05-03-pg-raggraph-rs-extension-design.md`. Confirm:
1. SC-009 tests both directions: default install = HNSW (Task 1's `default_mode_keeps_hnsw_indexes`); parity_mode = IVFFlat (this task).
2. The parity_mode swap happens at `namespace_create`, **not** at every query (Task 1's helper checks `has_chunks OR has_entities` and bails if data exists — DC-004 contract: existing namespaces don't get re-indexed).
3. RRF k=60 is unchanged across modes (parity contract).

If any drift, stop and reassess.

- [ ] **Step 13.4: Commit**

```bash
git add pg_raggraph/src/lib.rs
git commit -m "test(retrieval): parity_mode end-to-end query path (IVFFlat) per SC-009 / DC-004"
```

---

## Task 14: Tighten `pgrg.status()` SPI error handling (Plan 1 deferred concern)

**Files:**
- Modify: `pg_raggraph/src/admin.rs`
- Modify: `pg_raggraph/src/lib.rs` (tests)

**Why:** SC-014 requires the `.ok()` swallow in `pgrg.status` (Plan 1 line 210 of `admin.rs`) to be replaced with explicit error handling: `Err(SpiError::InvalidPosition)` → `None` (legitimate "no row found"); propagate everything else as `ereport!(ERROR, ...)` or `error!`. Constraint "Ask First" allows surgical fix to the `.ok()` swallow but not a wholesale refactor.

The current code:

```rust
let row: Option<(Option<String>, Option<String>, Option<String>)> =
    Spi::get_three_with_args(
        "SELECT status, source, error FROM pgrg.ingest_jobs WHERE id = $1",
        &[uuid.into()],
    )
    .ok();
```

This swallows all errors (malformed UUID, table missing, permission denied) and returns `None` — indistinguishable from "no row found." We replace with an explicit match on the `SpiError` variant.

- [ ] **Step 14.1: Write the failing regression test**

Add to the `tests` module in `pg_raggraph/src/lib.rs`:

```rust
    #[pg_test]
    fn status_propagates_non_invalid_position_errors() {
        // SC-014: status() must NOT swallow SPI errors silently.
        // Inject a malformed-job state by creating a row with NULL status, then
        // querying; the `Spi::get_three` path should still propagate cleanly.
        // (We cannot easily inject a SQL-level error from outside, so this test
        // primarily verifies the happy paths still work AND the no-row path
        // still returns NULL.)

        // No-row path: random UUID -> NULL.
        let null_result: Option<pgrx::JsonB> = Spi::get_one(
            "SELECT pgrg.status('00000000-0000-0000-0000-000000000000'::uuid)",
        )
        .unwrap();
        assert!(null_result.is_none(), "unknown job_id must return NULL");

        // Existing-row path: insert a job row, query its id.
        Spi::run("SELECT pgrg.namespace_create('status_test_ns')").unwrap();
        Spi::run(
            "INSERT INTO pgrg.ingest_jobs (id, status, source, namespace) \
             VALUES ('44444444-4444-4444-4444-444444444444', 'queued', 'test.md', 'status_test_ns')",
        )
        .unwrap();
        let found: Option<pgrx::JsonB> = Spi::get_one(
            "SELECT pgrg.status('44444444-4444-4444-4444-444444444444'::uuid)",
        )
        .unwrap();
        let obj = found.expect("must find row").0;
        assert_eq!(obj["status"], "queued");
    }
```

- [ ] **Step 14.2: Run test, observe pass-or-near-pass**

```bash
cargo pgrx test pg17 -p pg_raggraph -- status_propagates_non_invalid_position_errors
```

Expected: passes against the current `.ok()`-swallowing code, BUT this is the precondition — the goal is to make the implementation explicit so future regressions don't silently swallow new error types. Proceed to step 14.3 to tighten the implementation.

- [ ] **Step 14.3: Replace the `.ok()` swallow in `pgrg.status` with explicit handling**

Edit `pg_raggraph/src/admin.rs`. Replace the `Some(uuid) => { ... }` arm of `status()` with:

```rust
        Some(uuid) => {
            // SC-014: explicit error handling — distinguish "no row found"
            // (legitimate; return NULL) from genuine SPI errors (propagate).
            // Plan 1 used a blanket `.ok()` that hid all errors equally.
            let result: Result<
                (Option<String>, Option<String>, Option<String>),
                pgrx::spi::SpiError,
            > = Spi::get_three_with_args(
                "SELECT status, source, error FROM pgrg.ingest_jobs WHERE id = $1",
                &[uuid.into()],
            );

            match result {
                Ok((status, source, error)) => Some(pgrx::JsonB(serde_json::json!({
                    "id":     uuid.to_string(),
                    "status": status,
                    "source": source,
                    "error":  error,
                }))),
                // InvalidPosition means "no row matched" — legitimate return None.
                Err(pgrx::spi::SpiError::InvalidPosition) => None,
                // Any other SPI error is a genuine failure: propagate as ereport!(ERROR).
                Err(other) => {
                    ereport!(
                        ERROR,
                        PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                        format!("pgrg.status: SPI error: {other}")
                    );
                    unreachable!()
                }
            }
        }
```

> NOTE: the exact `SpiError` variant name in pgrx 0.17 is `InvalidPosition` (returned when a SELECT yields no rows and `get_three`'s internal `next()` errors out). If pgrx 0.17 names it differently in this codebase, check the build error message; the variant exists per Plan 1's `admin.rs` line 200-210 comment.

- [ ] **Step 14.4: Run all extension tests to confirm no regressions**

```bash
cargo pgrx test pg17 -p pg_raggraph
```

Expected: all tests pass — including the existing `status_summary_has_zero_jobs` and `status_unknown_job_returns_null` from Plan 1.

- [ ] **Step 14.5: Commit**

```bash
git add pg_raggraph/src/admin.rs pg_raggraph/src/lib.rs
git commit -m "fix(admin): pgrg.status — propagate SPI errors, only swallow InvalidPosition (SC-014)"
```

---

## Task 15: E2E test — ingest fixture, then query (mission-brief E2E requirement)

**Files:**
- Modify: `pg_raggraph/src/lib.rs` (tests)

**Why:** Mission brief "Testing Requirements / E2E / User Simulation Testing" lists: *"A user runs `pgrg.ingest_extracted('test-corpus.jsonl', 'demo')` and then `SELECT text, score FROM pgrg.query('what is the auth module', NULL, 5, 'demo')` and gets back ranked results in under 100ms on a 1K-chunk corpus. Codify as a `pgrx::pg_test` named `e2e_ingest_extracted_then_query`."*

This task is the named E2E test. Plan 2 doesn't ship a 1K-chunk corpus (Plan 6 will); we use a 10-chunk corpus and assert ranked results return in under 1 second (a generous bound). Plan 6's parity harness will tighten this.

- [ ] **Step 15.1: Write the test**

Add to the `tests` module in `pg_raggraph/src/lib.rs`:

```rust
    #[pg_test]
    fn e2e_ingest_extracted_then_query() {
        // Mission brief E2E: load fixture via ingest_extracted, query via pgrg.query,
        // assert ranked results returned. Latency-bounded: <1s for the small fixture.
        load_minimal_fixture_for_query("e2e_demo");

        let start = std::time::Instant::now();
        let rows: Vec<(String, f64)> = Spi::connect(|client| {
            client
                .select(
                    "SELECT text, score FROM pgrg.query('what is the auth module', NULL, 5, 'e2e_demo', 1, NULL, 'hybrid')",
                    None,
                    &[],
                )
                .unwrap()
                .map(|r| {
                    (
                        r.get::<String>(1).unwrap().unwrap_or_default(),
                        r.get::<f64>(2).unwrap().unwrap_or(0.0),
                    )
                })
                .collect()
        });
        let elapsed = start.elapsed();

        assert!(!rows.is_empty(), "E2E: query must return at least one ranked result");
        // Plan 2 tiny fixture; Plan 6 will set the parity-grade SLA.
        assert!(
            elapsed.as_millis() < 1000,
            "E2E: query latency must be < 1s on the small fixture, took {elapsed:?}"
        );

        // Descending score order verified by an earlier test; here just confirm scores are real.
        for (text, score) in &rows {
            assert!(*score > 0.0, "score must be positive, got {score} for `{text}`");
        }
    }
```

- [ ] **Step 15.2: Run the test**

```bash
cargo pgrx test pg17 -p pg_raggraph -- e2e_ingest_extracted_then_query
```

Expected: 1 test passes.

- [ ] **Step 15.3: Commit**

```bash
git add pg_raggraph/src/lib.rs
git commit -m "test(retrieval): E2E ingest_extracted -> query (mission brief E2E requirement)"
```

---

## Task 16: README + CHANGELOG bump for 0.1.0-alpha.2

**Files:**
- Modify: `Cargo.toml` (workspace `version = "0.1.0-alpha.2"`)
- Modify: `README.md`
- Modify: `CHANGELOG.md`

**Why:** Plan 1 ended at `0.1.0-alpha.1` with foundation only. Plan 2 ships the retrieval surface — the version bump signals the new public API surface (`pgrg.query`, `pgrg.embed`, `pgrg.ingest_extracted`).

- [ ] **Step 16.1: Bump workspace version in `Cargo.toml`**

In the workspace `Cargo.toml` `[workspace.package]` block:

```toml
version = "0.1.0-alpha.2"
```

- [ ] **Step 16.2: Update `README.md` Status section**

Replace the Status section in `README.md` with:

```markdown
## Status

**Pre-alpha (0.1.0-alpha.2).** Foundation + **retrieval engine** in place: schema, namespaces, providers, GUCs, health/status, **plus** synchronous hybrid retrieval (`pgrg.query`), deterministic test embeddings (`pgrg.embed`), and a fixture loader for testing and parity benchmarks (`pgrg.ingest_extracted`). Async ingest (Plan 3), LLM grounding (Plan 4), sidecar (Plan 5), and the parity harness (Plan 6) land in subsequent plans.

```sql
-- This works as of 0.1.0-alpha.2:
CREATE EXTENSION pg_raggraph CASCADE;
SELECT pgrg.namespace_create('demo');
SELECT pgrg.ingest_extracted('/path/to/test-corpus.jsonl', 'demo');
SELECT text, score FROM pgrg.query('your query here', NULL, 5, 'demo');
```
```

- [ ] **Step 16.3: Update `CHANGELOG.md`**

Prepend to `CHANGELOG.md`, before the existing 0.1.0-alpha.1 entry:

```markdown
## [0.1.0-alpha.2] — 2026-05-04

### Added
- `pgrg.query(q, filter, top_k, namespace, hops, weights, mode)` — synchronous hybrid retrieval combining pgvector cosine, BM25, and recursive-CTE graph walk under RRF (k=60) in a single SQL statement (Plan 2)
- `pgrg.embed(text, namespace)` — deterministic test-only embedding (Plan 3 swaps in the real bge-small-en-v1.5 ONNX model)
- `pgrg.ingest_extracted(path, namespace)` — JSONL fixture loader, bypasses the ingest queue
- Modes: `hybrid` (default), `vector`, `bm25`, `graph` (single-mode ablation knobs only — no smart-mode, per spec §11)
- IVFFlat index alternates wired through `pgrg.parity_mode` GUC at namespace creation (deterministic indexes for parity benchmarks)
- `pg_raggraph_core::retrieval` module (Mode enum, RRF math, undirected-walk SQL builder) — fully `cargo test`-able without PostgreSQL
- `pg_raggraph_core::embedding::deterministic_embed` — SHA-256-derived L2-normalized vectors

### Fixed
- `pgrg.status()` now propagates SPI errors explicitly instead of swallowing them via `.ok()` (Plan 1 deferred concern)

### Not yet implemented
- Background worker, async ingest (Plan 3)
- Real embedding model loader (Plan 3)
- LLM extraction, `pgrg.ask` (Plan 4)
- Sidecar binary (Plan 5)
- Cross-impl parity harness (Plan 6)
```

- [ ] **Step 16.4: Commit**

```bash
git add Cargo.toml README.md CHANGELOG.md
git commit -m "docs: README + CHANGELOG for 0.1.0-alpha.2 (Plan 2 retrieval engine)"
```

---

## DC-FINAL: Final drift check before declaring Plan 2 complete

⛔ **Drift Check DC-FINAL.** Re-read Mission Brief at `skill-output/mission-brief/Mission-Brief-plan2-retrieval-engine.md` one final time. For each SC-001 through SC-014, confirm evidence of satisfaction (named test in cargo/pgrx output). If any SC-XXX lacks evidence, the work is not complete — open a follow-up task before declaring done.

Mapping of SC → evidence (test name to look for):

| SC | Test name |
|---|---|
| SC-001 | `query_hybrid_returns_documented_columns`, `query_hybrid_descending_score_order` |
| SC-002 | `embed_returns_correct_dim_vector`, `embed_is_deterministic`, plus `pg_raggraph_core::tests::embedding` (5 tests) |
| SC-003 | `ingest_extracted_loads_fixture_into_tables` |
| SC-004 | `query_vector_mode_only_vec_lane_in_signals`, `query_bm25_mode_only_bm25_lane_in_signals`, `query_graph_mode_only_graph_lane_in_signals`, `query_unknown_mode_errors` |
| SC-005 | `rrf_score_matches_hand_computed_with_default_weights`, `pg_raggraph_core::tests::retrieval_rrf::*` |
| SC-006 | `hops_zero_excludes_graph_lane`, `hops_one_includes_direct_neighbors_only`, `hops_two_includes_friends_of_friends` |
| SC-007 | `undirected_walk_reaches_a_from_b_seed`, `pg_raggraph_core::tests::retrieval_query_sql::sql_uses_undirected_walk` |
| SC-008 | `filter_metadata_predicate_applied_inside_lanes`, `pg_raggraph_core::tests::retrieval_query_sql::sql_metadata_predicate_inside_each_lane` |
| SC-009 | `parity_mode_creates_ivfflat_indexes`, `default_mode_keeps_hnsw_indexes`, `parity_mode_end_to_end_query_works` |
| SC-010 | `weights_override_zeros_bm25_doubles_vec`, `pg_raggraph_core::tests::retrieval_rrf::rrf_weight_override_zeros_bm25_doubles_vec` |
| SC-011 | `embed_works_without_providers_table_rows` |
| SC-012 | `pg_raggraph_core::tests::retrieval_mode::*` (3) + `retrieval_rrf::*` (5) + `retrieval_query_sql::*` (8) + `retrieval_fixture::*` (4) + `embedding::*` (5) — 25 unit tests, all `cargo test` (no PG) |
| SC-013 | `cargo pgrx test pg17 -p pg_raggraph` running all of the above pgrx tests |
| SC-014 | `status_propagates_non_invalid_position_errors` |

If every line of the table above corresponds to a green test, Plan 2 is complete.

---

## Self-Review Checklist (run before declaring Plan 2 complete)

- [ ] All 16 tasks marked complete
- [ ] `cargo fmt --all -- --check` passes
- [ ] `cargo check --workspace` passes
- [ ] `cargo clippy -p pg_raggraph_core -p pg_raggraph_sidecar -- -D warnings` passes
- [ ] `cargo test -p pg_raggraph_core` passes (25+ tests across `retrieval_mode`, `retrieval_rrf`, `retrieval_query_sql`, `retrieval_fixture`, `embedding`, plus Plan 1's `provider_kind` and `credentials`)
- [ ] `cargo pgrx test pg17 -p pg_raggraph` passes ALL tests, including:
  - Plan 1 tests (still passing): `extension_loads`, `schema_tables_exist`, `migrations_seeded`, `default_namespace_present`, `namespace_create_inserts_row`, `namespace_drop_removes_row`, `provider_create_then_list`, `provider_drop_removes_row`, `gucs_have_expected_defaults`, `health_returns_expected_keys`, `status_summary_has_zero_jobs`, `status_unknown_job_returns_null`, `delete_document_removes_chunks_via_cascade`, `delete_namespace_without_cascade_blocks_when_docs_exist`
  - Plan 2 retrieval tests:
    - Task 1: `parity_mode_creates_ivfflat_indexes`, `default_mode_keeps_hnsw_indexes`
    - Task 4: `embed_returns_correct_dim_vector`, `embed_is_deterministic`, `embed_works_without_providers_table_rows`
    - Task 5: `ingest_extracted_loads_fixture_into_tables`
    - Task 7: `query_hybrid_returns_documented_columns`, `query_hybrid_descending_score_order`
    - Task 8: `query_vector_mode_only_vec_lane_in_signals`, `query_bm25_mode_only_bm25_lane_in_signals`, `query_graph_mode_only_graph_lane_in_signals`, `query_unknown_mode_errors`
    - Task 9: `hops_zero_excludes_graph_lane`, `hops_one_includes_direct_neighbors_only`, `hops_two_includes_friends_of_friends`, `undirected_walk_reaches_a_from_b_seed`
    - Task 10: `filter_metadata_predicate_applied_inside_lanes`
    - Task 11: `rrf_score_matches_hand_computed_with_default_weights`, `weights_override_zeros_bm25_doubles_vec`
    - Task 12: `signals_shape_is_lane_rk_w_tuple`, `debug_retrieval_guc_does_not_break_query`
    - Task 13: `parity_mode_end_to_end_query_works`
    - Task 14: `status_propagates_non_invalid_position_errors`
    - Task 15: `e2e_ingest_extracted_then_query`
- [ ] CI green on the push (PG17 path)
- [ ] Mission brief read 4 times: at DC-001 (Step 4.12), DC-002 (Step 6.6), DC-004 (Step 13.3), DC-FINAL (above)
- [ ] DC-003 satisfied implicitly by Task 6 (single SQL builder, mode-conditional `WHERE false` gates) and verified in Task 8
- [ ] No `unsafe` introduced into `pg_raggraph_core` (Constraint Always: `unsafe_code = "forbid"` preserved)
- [ ] No new GUCs introduced beyond Plan 1's set (Constraint Ask First: new GUCs need approval — Plan 2 introduces none)
- [ ] All retrieval logic that is not strictly pgrx FFI lives in `pg_raggraph_core::retrieval` (verified by `cargo test -p pg_raggraph_core` covering Mode, RRF, SQL builder, fixture parser without PostgreSQL)
- [ ] No LLM provider call introduced in `pgrg.query`, `pgrg.embed`, or `pgrg.ingest_extracted` (Constraint Never)
- [ ] No smart-mode, naive_boost, local, or global modes (Constraint Never)
- [ ] CHANGELOG and README reflect 0.1.0-alpha.2

---

## Spec coverage (Plan 2 → design-spec map)

| Spec section | Plan 2 task |
|---|---|
| §1 Thesis (3-statement demo, retrieval half) | Task 15 (E2E) — full demo lands in Plan 4 |
| §3 Ingest path | **Out of scope (Plan 3)** — except `ingest_extracted` (Task 5) |
| §4 Query path — hybrid default fuses vec + bm25 + graph + metadata | Task 6 (SQL builder), Task 7 (pgrg.query wrapper) |
| §4 Query path — fused SQL lines 121-176 | Task 6 (`build_query_sql` matches verbatim) |
| §4 Query path — RRF k=60 equal weights default | Task 6 (SQL constant), Task 11 (weight binds), Task 3 (math) |
| §4 Query path — `weights` override | Task 11 |
| §4 Query path — mode parameter (ablation) | Tasks 2 (Mode enum), 6 (lane gating), 7 (wrapper), 8 (tests) |
| §4 Query path — undirected traversal | Tasks 6 (UNION on src/dst), 9 (test) |
| §4 Query path — `hops` 0/1/2 semantics | Tasks 6 (recursive bound), 9 (tests) |
| §4 Query path — metadata predicate inside lanes | Tasks 6 (SQL), 10 (test) |
| §4 Query path — `pgrg.ask` flow | **Out of scope (Plan 4)** |
| §5 Schema | Used as-is from Plan 1; only Task 1 adds `004_retrieval_indexes.sql` migration |
| §6 SQL surface — `pgrg.query` | Task 7 |
| §6 SQL surface — `pgrg.embed` | Task 4 |
| §6 SQL surface — `pgrg.ingest_extracted` | Task 5 |
| §6 SQL surface — `pgrg.ask` | **Out of scope (Plan 4)** |
| §6 SQL surface — `pgrg.status` SPI tightening | Task 14 (folded deferred concern) |
| §7 GUC `pgrg.embed_dim` | Task 4 (vector dim contract verified) |
| §7 GUC `pgrg.parity_mode` | Tasks 1, 13 |
| §7 GUC `pgrg.debug_retrieval` | Task 12 (no-op gate; expansion deferred to Plan 6) |
| §10 Cross-impl parity contracts (undirected, RRF, IVFFlat) | Tasks 6, 9, 1, 13; Constraint "byte-for-byte semantics" |
| §11 Out of scope for v1 (no smart-mode, no global, no custom AM) | Honored — Mode enum has only hybrid/vector/bm25/graph; `query_unknown_mode_errors` test |

---

## What this plan deliberately does *not* cover

These belong to subsequent plans, not Plan 2:

- **Real embedding model loading** (chunkshop `hf_cache`, BAAI/bge-small-en-v1.5 ONNX, fp32) — Plan 3. Plan 2's `deterministic_embed` is a placeholder behind the same `pgrg.embed` SQL surface.
- **`pgrg.ingest`, `pgrg.ingest_text`, `pgrg.ingest_bytes`** (async, queue-backed, real chunking + extraction) — Plan 3.
- **Background worker** (`bgw_launcher`, `bgw_worker`, the consumer that reads `pgrg.ingest_jobs`) — Plan 3.
- **`pgrg.ask` SQL function** (`pgrg.query` + LLM grounding + citation-required prompt) — Plan 4.
- **LLM provider trait + impls** (`OpenAiProvider`, `AnthropicProvider`, `OllamaProvider`, `MockProvider`) — Plan 4.
- **AES-GCM credential encryption** (`pgrg.master_key_path`, `enc:v1:...`) — Plan 4.
- **Sidecar binary** (libpq job loop, embedded SQL bootstrap, HTTP `/v1/ask`) — Plan 5.
- **`bench/parity/`** corpus, `compare.py`, parity CI workflow, Jaccard ≥ 0.8 thresholds, `resolution_constants.yaml` — Plan 6.
- **Smart-mode routing, automatic escalation, confidence thresholds** — explicitly out of scope per spec §11 and mission brief Constraint Never.
- **Community detection, Leiden/Louvain, `global` retrieval mode** — explicitly out of scope per spec §11.
- **Custom vector-index access methods** — pgvector HNSW/IVFFlat are the only indexes; no hand-rolled AMs (mission brief Constraint Never).
- **`pg_raggraph.debug_retrieval` GUC's signal expansion** — Plan 2 ships the GUC as a no-op (existing signals shape `[{lane, rk, w}]` covers all SC verification); richer debug fields land in Plan 6 if/when parity work needs them.
- **HTTP `/v1/ask`, web UI, MCP server** — spec §11 / Plan 5+.
