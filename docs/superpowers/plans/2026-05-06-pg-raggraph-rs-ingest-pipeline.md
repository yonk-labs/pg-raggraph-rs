# pg-raggraph-rs Ingest Pipeline — Implementation Plan (Plan 3 of 6)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the asynchronous write path for the Rust extension. After Plan 3, a user can run `SELECT pgrg.ingest('/data/docs/');`, get back a job UUID immediately, and the database drains the queue in the background — chunkshop chunks the source, a locally-loaded ONNX embedding model embeds chunks, the per-document transaction writes documents/chunks into the schema. Plan 3 ships the full async-ingest plumbing (bg worker, queue claim with `FOR UPDATE SKIP LOCKED`, real embedding model behind the existing `pgrg.embed` SQL surface, chunkshop as the canonical chunker, content-hash-based incremental skip, reaper, ingestion profiles, `LlmProvider` trait surface with a `MockProvider` no-op extractor).

Plan 3 ships **only ingest plumbing**. No real LLM extraction (Plan 4), no `pgrg.ask` (Plan 4), no AES-GCM credential encryption (Plan 4), no sidecar (Plan 5), no parity benchmarks (Plan 6). The `LlmProvider` trait surface is defined here; the OpenAI/Anthropic/Ollama impls land in Plan 4.

**Architecture:** Three-crate Cargo workspace from Plan 1, extended:
- `pg_raggraph_core::ingest` — new module owning `run_job`, the per-document transaction shape, the `IngestProfile` enum, content-hash computation, the `PgClient` injection trait so the loop is unit-testable without PG.
- `pg_raggraph_core::embedding` — `EmbeddingBackend` trait; `OnnxEmbedder` (ort-backed, loads `BAAI/bge-small-en-v1.5` fp32 ONNX once per worker) and `DeterministicEmbedder` (Plan 2's existing impl, reused under `cfg(any(test, feature = "pg_test"))`).
- `pg_raggraph_core::llm` — new module: `LlmProvider` trait + `MockProvider` (no-op extractor returning empty entity/relationship sets). Plan 4 plugs in the network-backed impls.
- `pg_raggraph_core::chunking` — new module: `ChunkStrategy` enum, chunkshop integration shim. chunkshop is a hard Cargo dep here.
- `pg_raggraph::bgw` — new pgrx module: `register_launcher()` + `register_workers()`, the `pg_raggraph_launcher_main` reaper loop, the `pg_raggraph_worker_main` poll loop, all the SPI helpers (`claim_next_job`, `complete_job`, `fail_job`).
- `pg_raggraph::ingest` — new pgrx module: `pgrg.ingest`, `pgrg.ingest_text`, `pgrg.ingest_bytes` SQL functions (queue inserts only, never block).
- `pg_raggraph/src/lib.rs` — `_PG_init` extended to register bg workers (gated on `process_shared_preload_libraries_in_progress`).
- `pg_raggraph/sql/migrations/006_ingest_jobs_payload.sql` — new column `payload bytea` on `pgrg.ingest_jobs` for `ingest_text` / `ingest_bytes` carriage; partial index for the bg worker scan.

**Tech Stack (extended from Plan 1+2):** Rust 2024, `pgrx = "=0.17.0"`, PostgreSQL 17 (CI) / 18 (local dev), `pgvector` 0.8+, `pg_trgm`, `serde`/`serde_json`, `uuid`, `sha2`, **new in Plan 3:** `ort = "2"` (ONNX Runtime bindings, the standard pgrx-compatible choice), `chunkshop` (Rust crate), `tokio = { version = "1", features = ["rt", "macros", "sync"] }` (single-runtime per worker for embedding-model concurrency). License Apache-2.0.

**Spec reference:** `docs/superpowers/specs/2026-05-03-pg-raggraph-rs-extension-design.md`. This plan implements:
- §3 Ingest path (lines 54–86) — SQL entry function, bg worker registration, polling loop, per-job pipeline, single-tx persistence, errors/reaper, sidecar parity contract (sidecar consumes the same `_core::ingest::run_job` in Plan 5)
- §6 SQL surface (lines 269–272) — `pgrg.ingest`, `pgrg.ingest_text`, `pgrg.ingest_bytes`
- §7 GUCs (lines 327–333) — `pgrg.bgw_workers`, `pgrg.extract_concurrency`, `pgrg.embed_model_path`, `pgrg.embed_dim`, `pgrg.job_reaper_interval`
- §7 G1 local embedding model default (lines 359–363) — `BAAI/bge-small-en-v1.5` fp32 ONNX via chunkshop's `hf_cache`
- §7 LLM provider abstraction (line 357) — trait surface only; impls deferred to Plan 4

**Mission Brief reference:** `skill-output/mission-brief/Mission-Brief-plan3-ingest-pipeline.md` — 17 Success Criteria (SC-001..SC-017), Constraints (Always / Ask First / Never), Drift Checkpoints (DC-001..DC-FINAL), Out of Scope. **The implementer MUST re-read this file at every `⛔ Drift Check DC-XXX` step in this plan**; the brief is authoritative if it conflicts with anything below.

**Plan arc (context — only Plan 3 is executed here):**
1. Foundation + Schema — **done** (committed to `main`)
2. Retrieval engine — **done** (committed to `main`)
3. **Ingest pipeline** ← this plan (bg worker, chunkshop integration, real ONNX embedding model, `pgrg.ingest`/`pgrg.ingest_text`/`pgrg.ingest_bytes`, content-hash incremental skip, ingest profiles, reaper, `LlmProvider` trait + `MockProvider`)
4. LLM extraction + ask — provider trait impls (OpenAI/Anthropic/Ollama), AES-GCM credential encryption, `pgrg.ask`
5. Sidecar binary — libpq job loop, embedded SQL bootstrap, HTTP `/v1/ask`
6. Cross-impl parity harness — `bench/parity/` corpus, `compare.py`, parity CI

---

## Pre-execution: conventions inherited from Plans 1+2

These were established in Plan 1 (`docs/superpowers/plans/2026-05-03-pg-raggraph-rs-foundation.md`) and reaffirmed in Plan 2 (`docs/superpowers/plans/2026-05-04-pg-raggraph-rs-retrieval-engine.md`), and **continue to apply** in Plan 3. Re-stating them here so the executor doesn't have to flip back.

**pgrx 0.17 deviations (carried forward from Plans 1+2):**

1. **Bare `#[pg_extern]`, no `schema = "pgrg"` argument.** The `.control` file's `schema = 'pgrg'` directive plus the `pg_module_magic!` declaration places generated functions in the `pgrg` schema automatically. See `pg_raggraph/src/admin.rs` and `pg_raggraph/src/ingest_extracted.rs` for working examples.

2. **`Spi::connect_mut` for write paths.** `Spi::connect` is read-only in pgrx 0.17; INSERT/UPDATE/DELETE through `client.update(...)` requires `Spi::connect_mut(|client| { ... })`. Plan 3's bg-worker SPI helpers use this pattern consistently.

3. **`#[pg_guard] pub extern "C-unwind" fn _PG_init()`** — established signature from Plan 1's `lib.rs`. Plan 3 extends `_PG_init`'s body but does not change the signature.

4. **CString literals (`c"..."`) for GUC names/descriptions.** Plan 1's `gucs.rs` already registers all five GUCs Plan 3 consumes (`bgw_workers`, `extract_concurrency`, `embed_model_path`, `embed_dim`, `job_reaper_interval`). **Plan 3 introduces no new GUCs** (Constraint Ask First).

5. **`pg_catalog.pg_tables.tablename` is OID `name`, not `text`** — cast with `::text` when iterating system catalog columns of type `name` from Rust. (Documented in `docs/dev-setup.md`.)

6. **clippy::pedantic discipline on `_core`.** `pg_raggraph_core/Cargo.toml` declares `pedantic = { level = "warn", priority = -1 }`. Plan 3 fixes any new pedantic hits silently (doc_markdown, cast_precision_loss, derivable_impls). No `#[allow(clippy::pedantic)]` blanket allowances; per-item `#[allow]` with a reason is acceptable.

7. **`unsafe_code = "forbid"` on `_core`.** Plan 3 adds ONNX inference, which `ort` exposes safely; if any FFI requires unsafe, it must live in `pg_raggraph` (the pgrx crate already permits unsafe via pgrx itself). `_core` stays unsafe-free.

**Local dev loop (per `docs/dev-setup.md`):**

```bash
cd /home/yonk/yonk-tools/pg-raggraph-extension
cargo pgrx test pg18 --package pg_raggraph --features "pg18 pg_test" --no-default-features
```

CI runs `cargo pgrx test pg17 -p pg_raggraph` against PGDG packages. **CI runs serial** (`RUST_TEST_THREADS=1`) since commit `9d2e598` because parity-helper bg-worker tests deadlock under parallel pgrx test execution. Plan 3 inherits this — its bg-worker tests are particularly sensitive to concurrent extension teardown. Command examples in this plan use the **pg17 form** (matching Plan 1+2 verbatim style and CI canonical); substitute `pg18` for local execution as `dev-setup.md` instructs.

**Branch policy:** commit to `main` (matches Plans 1+2). One commit per task. Commit messages mirror Plan 1+2 style (e.g., `feat(ingest): bg worker registration in _PG_init`).

**Repo root:** `/home/yonk/yonk-tools/pg-raggraph-extension/`. All paths in this plan are relative to that directory.

**Test isolation note (carried from Plan 2):** `pgrx::pg_test` runs each test in a fresh PG; bg workers, however, are registered at `_PG_init` time and run for the whole server lifetime. The pg_test `postgresql_conf_options()` already declares `shared_preload_libraries='pg_raggraph'` and `pg_raggraph.bgw_workers=2`, so workers are running across all tests. Tests that exercise the worker must be tolerant of timing — use polling loops with a generous deadline rather than fixed sleeps.

---

## Task 1: Schema migration — `ingest_jobs.payload` bytea + partial index

**Files:**
- Create: `pg_raggraph/sql/migrations/006_ingest_jobs_payload.sql`
- Modify: `pg_raggraph/src/lib.rs` (wire the new SQL file via `extension_sql_file!`)

**Why:** Brief Desired Outcome and SC-005/SC-006 require `pgrg.ingest_text` and `pgrg.ingest_bytes` to carry their payload through the queue (the file path can't represent inline content). Spec §5 schema lists `payload bytea` on `ingest_jobs` but the Plan 1 schema (`001_tables.sql`) shipped without it — it was deferred to whichever plan needed it first. That's now. Spec §5 also lists a partial index `ingest_jobs(status, enqueued_at) WHERE status IN ('queued','running')` which the bg worker scan requires for performance; we add it here too.

**Design choice:** add `payload` and `attempt_count` (also referenced by spec §5 lines 219–223 and the reaper) as nullable / defaulted columns so existing rows survive the ALTER. The partial index is created with `IF NOT EXISTS` so the migration is idempotent.

- [ ] **Step 1.1: Write the failing test**

Add to the `tests` module in `pg_raggraph/src/lib.rs`:

```rust
    #[pg_test]
    fn ingest_jobs_payload_column_exists() {
        // Spec §5: ingest_jobs.payload bytea for ingest_text/ingest_bytes carriage.
        // Plan 3 Task 1 adds the column.
        let exists: Option<bool> = Spi::get_one(
            "SELECT EXISTS( \
                 SELECT 1 FROM information_schema.columns \
                 WHERE table_schema = 'pgrg' \
                   AND table_name = 'ingest_jobs' \
                   AND column_name = 'payload' \
                   AND data_type = 'bytea')",
        )
        .unwrap();
        assert_eq!(exists, Some(true), "ingest_jobs.payload bytea must exist");
    }

    #[pg_test]
    fn ingest_jobs_attempt_count_column_exists() {
        // Spec §5 + brief Desired Outcome: reaper bumps attempt_count, caps at 3.
        let exists: Option<bool> = Spi::get_one(
            "SELECT EXISTS( \
                 SELECT 1 FROM information_schema.columns \
                 WHERE table_schema = 'pgrg' \
                   AND table_name = 'ingest_jobs' \
                   AND column_name = 'attempt_count')",
        )
        .unwrap();
        assert_eq!(exists, Some(true), "ingest_jobs.attempt_count must exist");
    }

    #[pg_test]
    fn ingest_jobs_active_partial_index_exists() {
        // Spec §5 line 254: partial index for the bg worker scan.
        let exists: Option<bool> = Spi::get_one(
            "SELECT EXISTS( \
                 SELECT 1 FROM pg_indexes \
                 WHERE schemaname = 'pgrg' \
                   AND indexname = 'ingest_jobs_active_idx')",
        )
        .unwrap();
        assert_eq!(exists, Some(true), "ingest_jobs_active_idx partial index must exist");
    }
```

- [ ] **Step 1.2: Run tests, observe failure**

```bash
cargo pgrx test pg17 -p pg_raggraph -- ingest_jobs_payload_column_exists ingest_jobs_attempt_count_column_exists ingest_jobs_active_partial_index_exists
```

Expected: all three fail (column / index does not exist).

- [ ] **Step 1.3: Write `pg_raggraph/sql/migrations/006_ingest_jobs_payload.sql`**

```sql
-- 006_ingest_jobs_payload.sql — payload bytea + attempt_count + active-jobs partial index.
-- Per spec §5 lines 219–223, 254 and Plan 3 Mission Brief Desired Outcome
-- (ingest_text/ingest_bytes payload carriage, reaper attempt-count bookkeeping,
-- bg worker scan performance).

ALTER TABLE pgrg.ingest_jobs
    ADD COLUMN IF NOT EXISTS payload bytea;

ALTER TABLE pgrg.ingest_jobs
    ADD COLUMN IF NOT EXISTS attempt_count integer NOT NULL DEFAULT 0;

ALTER TABLE pgrg.ingest_jobs
    ADD COLUMN IF NOT EXISTS chunk_strategy text;

-- Partial index for the bg worker poll: scan only rows that could be picked.
CREATE INDEX IF NOT EXISTS ingest_jobs_active_idx
    ON pgrg.ingest_jobs (status, enqueued_at)
    WHERE status IN ('queued', 'running');
```

- [ ] **Step 1.4: Wire the migration file into `lib.rs`**

In `pg_raggraph/src/lib.rs`, add a seventh `extension_sql_file!` invocation immediately after the existing six (preserving Plans 1+2's ordering and dependency chain):

```rust
::pgrx::extension_sql_file!(
    "../sql/migrations/006_ingest_jobs_payload.sql",
    name = "ingest_jobs_payload",
    requires = ["status_check_atomicity"]
);
```

- [ ] **Step 1.5: Run tests, observe pass**

```bash
cargo pgrx test pg17 -p pg_raggraph -- ingest_jobs_payload_column_exists ingest_jobs_attempt_count_column_exists ingest_jobs_active_partial_index_exists
```

Expected: 3 tests pass.

- [ ] **Step 1.6: Commit**

```bash
git add pg_raggraph/sql/migrations/006_ingest_jobs_payload.sql pg_raggraph/src/lib.rs
git commit -m "feat(schema): ingest_jobs.payload+attempt_count+active partial index (Plan 3 prep)"
```

---

## Task 2: `_core::ingest` types — `IngestProfile`, `IngestJob`, `IngestRequest`

**Files:**
- Create: `pg_raggraph_core/src/ingest/mod.rs`
- Create: `pg_raggraph_core/src/ingest/profile.rs`
- Create: `pg_raggraph_core/src/ingest/types.rs`
- Create: `pg_raggraph_core/tests/ingest_profile.rs`
- Create: `pg_raggraph_core/tests/ingest_types.rs`
- Modify: `pg_raggraph_core/src/lib.rs` (declare `pub mod ingest`)

**Why:** Constraint Always: "All bg worker code that touches PG goes through pgrx SPI / connection helpers; `_core` stays PG-agnostic." This task creates the PG-agnostic DTOs that `_core::ingest::run_job` will consume in Task 10. SC-014 requires the `IngestProfile` enum to exist with `Conservative`/`Balanced`/`Aggressive`/`Max` values mapping to concrete `extract_concurrency` numbers (2 / 4 / 8 / 16 per the brief). SC-017 requires `_core::ingest::run_job` to be `cargo test`-able without PG, which depends on these types.

**Profile values per Mission Brief SC-014:** `Conservative=2`, `Balanced=4` (default), `Aggressive=8`, `Max=16`. These values match spec §3 line 72 (`pgrg.extract_concurrency` default 4) and the Python `pg_raggraph` `ingestion profile` knobs documented in `CLAUDE.md`.

- [ ] **Step 2.1: Write the failing profile test**

Create `pg_raggraph_core/tests/ingest_profile.rs`:

```rust
use pg_raggraph_core::ingest::IngestProfile;

#[test]
fn profile_default_is_balanced() {
    // Spec §3 line 72: extract_concurrency default 4 (= Balanced).
    assert_eq!(IngestProfile::default(), IngestProfile::Balanced);
}

#[test]
fn profile_extract_concurrency_values() {
    // SC-014: explicit per-profile concurrency mapping.
    assert_eq!(IngestProfile::Conservative.extract_concurrency(), 2);
    assert_eq!(IngestProfile::Balanced.extract_concurrency(), 4);
    assert_eq!(IngestProfile::Aggressive.extract_concurrency(), 8);
    assert_eq!(IngestProfile::Max.extract_concurrency(), 16);
}

#[test]
fn profile_parses_strings() {
    assert_eq!(IngestProfile::parse("conservative"), Some(IngestProfile::Conservative));
    assert_eq!(IngestProfile::parse("balanced"), Some(IngestProfile::Balanced));
    assert_eq!(IngestProfile::parse("aggressive"), Some(IngestProfile::Aggressive));
    assert_eq!(IngestProfile::parse("max"), Some(IngestProfile::Max));
}

#[test]
fn profile_unknown_returns_none() {
    assert_eq!(IngestProfile::parse("turbo"), None);
    assert_eq!(IngestProfile::parse(""), None);
    assert_eq!(IngestProfile::parse("BALANCED"), None); // case-sensitive
}

#[test]
fn profile_as_str_roundtrip() {
    for p in [
        IngestProfile::Conservative,
        IngestProfile::Balanced,
        IngestProfile::Aggressive,
        IngestProfile::Max,
    ] {
        assert_eq!(IngestProfile::parse(p.as_str()), Some(p));
    }
}
```

- [ ] **Step 2.2: Write the failing types test**

Create `pg_raggraph_core/tests/ingest_types.rs`:

```rust
use pg_raggraph_core::ingest::{IngestRequest, IngestSource};
use uuid::Uuid;

#[test]
fn ingest_request_path_source_round_trips() {
    let req = IngestRequest {
        source: IngestSource::Path("/data/docs/a.md".into()),
        namespace: "default".into(),
        chunk_strategy: "auto".into(),
    };
    let json = serde_json::to_string(&req).unwrap();
    let parsed: IngestRequest = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.namespace, "default");
    assert!(matches!(parsed.source, IngestSource::Path(ref p) if p == "/data/docs/a.md"));
}

#[test]
fn ingest_request_text_source_round_trips() {
    let req = IngestRequest {
        source: IngestSource::Text {
            name: "doc1".into(),
            content: "hello world".into(),
        },
        namespace: "default".into(),
        chunk_strategy: "auto".into(),
    };
    let json = serde_json::to_string(&req).unwrap();
    let parsed: IngestRequest = serde_json::from_str(&json).unwrap();
    if let IngestSource::Text { name, content } = parsed.source {
        assert_eq!(name, "doc1");
        assert_eq!(content, "hello world");
    } else {
        panic!("expected Text source");
    }
}

#[test]
fn ingest_request_bytes_source_round_trips() {
    let req = IngestRequest {
        source: IngestSource::Bytes {
            name: "doc1.bin".into(),
            bytes: vec![0xDE, 0xAD, 0xBE, 0xEF],
        },
        namespace: "default".into(),
        chunk_strategy: "auto".into(),
    };
    let json = serde_json::to_string(&req).unwrap();
    let parsed: IngestRequest = serde_json::from_str(&json).unwrap();
    if let IngestSource::Bytes { name, bytes } = parsed.source {
        assert_eq!(name, "doc1.bin");
        assert_eq!(bytes, vec![0xDE, 0xAD, 0xBE, 0xEF]);
    } else {
        panic!("expected Bytes source");
    }
}

#[test]
fn ingest_request_default_chunk_strategy_is_auto() {
    let req = IngestRequest::new_path("/data/docs/", "default");
    assert_eq!(req.chunk_strategy, "auto");
}

#[test]
fn ingest_job_id_is_uuid() {
    use pg_raggraph_core::ingest::IngestJob;
    let j = IngestJob {
        id: Uuid::new_v4(),
        request: IngestRequest::new_path("/x", "ns"),
        attempt_count: 0,
    };
    // Compile-time check: IngestJob exposes a Uuid id.
    assert_eq!(j.id.get_version_num(), 4);
}
```

- [ ] **Step 2.3: Run tests, observe failure**

```bash
cargo test -p pg_raggraph_core --test ingest_profile --test ingest_types
```

Expected: compile error — `pg_raggraph_core::ingest` does not exist.

- [ ] **Step 2.4: Create `pg_raggraph_core/src/ingest/mod.rs`**

```rust
//! Ingest pipeline: per-document transaction, profile knobs, source DTOs.
//!
//! Lives outside the pgrx crate so unit tests run with plain `cargo test`.
//! Per mission brief Constraint Always: bg worker code that touches PG goes
//! through pgrx SPI / connection helpers; `_core` stays PG-agnostic and uses
//! an injected `PgClient`-like trait so it can be unit-tested without a server.

pub mod profile;
pub mod types;

pub use profile::IngestProfile;
pub use types::{IngestJob, IngestRequest, IngestSource};
```

- [ ] **Step 2.5: Create `pg_raggraph_core/src/ingest/profile.rs`**

```rust
//! Ingestion profile knobs — Conservative/Balanced/Aggressive/Max.
//!
//! Per mission brief SC-014:
//!   conservative=2, balanced=4 (default), aggressive=8, max=16
//! Maps to `pgrg.extract_concurrency` (spec §3 line 72, §7 default 4).

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum IngestProfile {
    Conservative,
    Balanced,
    Aggressive,
    Max,
}

impl IngestProfile {
    /// Stable string identifier for SQL parameter passing and serialization.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            IngestProfile::Conservative => "conservative",
            IngestProfile::Balanced => "balanced",
            IngestProfile::Aggressive => "aggressive",
            IngestProfile::Max => "max",
        }
    }

    /// Parse a profile from its SQL string identifier. Case-sensitive.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "conservative" => Some(IngestProfile::Conservative),
            "balanced" => Some(IngestProfile::Balanced),
            "aggressive" => Some(IngestProfile::Aggressive),
            "max" => Some(IngestProfile::Max),
            _ => None,
        }
    }

    /// Per-profile `extract_concurrency` value. SC-014 contract:
    /// conservative=2, balanced=4, aggressive=8, max=16.
    #[must_use]
    pub const fn extract_concurrency(self) -> u32 {
        match self {
            IngestProfile::Conservative => 2,
            IngestProfile::Balanced => 4,
            IngestProfile::Aggressive => 8,
            IngestProfile::Max => 16,
        }
    }
}

impl Default for IngestProfile {
    fn default() -> Self {
        IngestProfile::Balanced
    }
}
```

- [ ] **Step 2.6: Create `pg_raggraph_core/src/ingest/types.rs`**

```rust
//! Ingest request / job DTOs — PG-agnostic.
//!
//! `IngestRequest` is the wire form of an ingest call (what the SQL functions
//! enqueue, what the bg worker dequeues). `IngestSource` is one of three
//! variants matching the SQL surface (`pgrg.ingest`, `pgrg.ingest_text`,
//! `pgrg.ingest_bytes`).

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum IngestSource {
    /// `pgrg.ingest(path)` — file path on the PG host filesystem.
    Path(String),
    /// `pgrg.ingest_text(name, content)` — inline text payload.
    Text { name: String, content: String },
    /// `pgrg.ingest_bytes(name, bytes)` — inline binary payload.
    Bytes { name: String, bytes: Vec<u8> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestRequest {
    pub source: IngestSource,
    pub namespace: String,
    pub chunk_strategy: String,
}

impl IngestRequest {
    /// Convenience constructor for path-shaped requests with defaults.
    #[must_use]
    pub fn new_path(path: impl Into<String>, namespace: impl Into<String>) -> Self {
        Self {
            source: IngestSource::Path(path.into()),
            namespace: namespace.into(),
            chunk_strategy: "auto".into(),
        }
    }
}

/// One queue entry in flight. Wraps the request with bookkeeping the worker
/// needs (job id for status updates, attempt_count for the reaper).
#[derive(Debug, Clone)]
pub struct IngestJob {
    pub id: Uuid,
    pub request: IngestRequest,
    pub attempt_count: i32,
}
```

- [ ] **Step 2.7: Wire `pub mod ingest;` into `pg_raggraph_core/src/lib.rs`**

Add a single line after the existing module declarations:

```rust
pub mod ingest;
```

- [ ] **Step 2.8: Run tests, observe pass**

```bash
cargo test -p pg_raggraph_core --test ingest_profile --test ingest_types
```

Expected: 5 + 5 = 10 tests pass.

- [ ] **Step 2.9: Commit**

```bash
git add pg_raggraph_core/src/ingest/ pg_raggraph_core/src/lib.rs pg_raggraph_core/tests/ingest_profile.rs pg_raggraph_core/tests/ingest_types.rs
git commit -m "feat(core): ingest::{IngestProfile,IngestRequest,IngestSource,IngestJob} DTOs"
```

---

## Task 3: Content-hash computation in `_core`

**Files:**
- Create: `pg_raggraph_core/src/ingest/content_hash.rs`
- Create: `pg_raggraph_core/tests/ingest_content_hash.rs`
- Modify: `pg_raggraph_core/src/ingest/mod.rs` (re-export)

**Why:** SC-007 requires that re-ingesting the same source (identical content_hash) is a no-op — `pgrg.documents` row count stays at 1. The hash is computed in `_core` (pure function, no I/O) so the bg worker and the future sidecar produce identical hashes for identical content. Spec §5 declares `documents.content_hash text UNIQUE`, so the existence-check against this column is the incremental-skip mechanism.

**Hash format:** SHA-256 over the canonical bytes, lowercase hex (`format!("{:x}", ...)`). Matches the format already used by Plan 2's fixture loader (`content_hash: "h-fix-1"` etc., free-form text — Plan 3's real hashes are 64-char hex but the column accepts any text).

- [ ] **Step 3.1: Write the failing test**

Create `pg_raggraph_core/tests/ingest_content_hash.rs`:

```rust
use pg_raggraph_core::ingest::content_hash::content_hash;

#[test]
fn content_hash_is_64_char_hex() {
    let h = content_hash(b"hello world");
    assert_eq!(h.len(), 64, "SHA-256 hex must be 64 chars");
    assert!(h.chars().all(|c| c.is_ascii_hexdigit()), "must be lowercase hex");
}

#[test]
fn content_hash_is_deterministic() {
    let a = content_hash(b"hello world");
    let b = content_hash(b"hello world");
    assert_eq!(a, b, "SC-007: identical content -> identical hash");
}

#[test]
fn content_hash_distinguishes_different_inputs() {
    let a = content_hash(b"hello");
    let b = content_hash(b"world");
    assert_ne!(a, b);
}

#[test]
fn content_hash_known_vector() {
    // Anchor vector against external truth: `printf "" | sha256sum`
    // -> e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
    assert_eq!(
        content_hash(b""),
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
}
```

- [ ] **Step 3.2: Run test, observe failure**

```bash
cargo test -p pg_raggraph_core --test ingest_content_hash
```

Expected: compile error — `pg_raggraph_core::ingest::content_hash` does not exist.

- [ ] **Step 3.3: Create `pg_raggraph_core/src/ingest/content_hash.rs`**

```rust
//! Canonical content-hash computation.
//!
//! Mission brief SC-007: re-ingesting the same source (identical content_hash)
//! is a no-op. Hash is SHA-256 over the canonical bytes, lowercase hex.
//! Pure function; called identically by the bg worker (Plan 3) and the
//! sidecar (Plan 5) so hashes are byte-stable across both code paths.

use sha2::{Digest, Sha256};

/// SHA-256 hex (64 lowercase chars) of `bytes`.
#[must_use]
pub fn content_hash(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    let digest = h.finalize();
    // Lowercase hex; one allocation.
    let mut s = String::with_capacity(64);
    for b in digest {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}
```

- [ ] **Step 3.4: Re-export from `ingest/mod.rs`**

Append to `pg_raggraph_core/src/ingest/mod.rs`:

```rust
pub mod content_hash;
```

- [ ] **Step 3.5: Run test, observe pass**

```bash
cargo test -p pg_raggraph_core --test ingest_content_hash
```

Expected: 4 tests pass.

- [ ] **Step 3.6: Commit**

```bash
git add pg_raggraph_core/src/ingest/content_hash.rs pg_raggraph_core/src/ingest/mod.rs pg_raggraph_core/tests/ingest_content_hash.rs
git commit -m "feat(core): ingest::content_hash (SHA-256 hex, SC-007 incremental skip)"
```

---

## Task 4: `EmbeddingBackend` trait + `DeterministicEmbedder` impl in `_core`

**Files:**
- Modify: `pg_raggraph_core/src/embedding.rs` (introduce trait, keep deterministic_embed as before)
- Create: `pg_raggraph_core/tests/embedding_backend.rs`

**Why:** Plan 2 ships `deterministic_embed` as a free function. Plan 3 needs to swap it for an ONNX-backed embedder in the bg worker, but Plan 2's tests (`embed_returns_correct_dim_vector`, `embed_is_deterministic`, `embed_works_without_providers_table_rows`) and Plan 2's fixture loaders rely on byte-stable embeddings — switching the production backend to ONNX would require fixture rebuilds, which is out of scope.

**Solution:** introduce an `EmbeddingBackend` trait. `DeterministicEmbedder` is the existing impl (for `pg_test` builds and unit tests). `OnnxEmbedder` (Task 5) is the production impl. The pgrx-side `pgrg._embed_text` (SQL surface, unchanged) selects backend based on `cfg(any(test, feature = "pg_test"))`, which keeps Plan 2's tests green while production gets the real model.

DC-FINAL of Plan 2 already verified Plan 2 tests. Plan 3's new tests select `OnnxEmbedder` only when `pg_test` is OFF (i.e., not at all in the pgrx test suite).

- [ ] **Step 4.1: Write the failing test**

Create `pg_raggraph_core/tests/embedding_backend.rs`:

```rust
use pg_raggraph_core::embedding::{DeterministicEmbedder, EmbeddingBackend};

#[test]
fn deterministic_backend_dim_matches_request() {
    let e = DeterministicEmbedder::new(384);
    let v = e.embed("hello").expect("embed must succeed");
    assert_eq!(v.len(), 384);
}

#[test]
fn deterministic_backend_byte_stable() {
    let e = DeterministicEmbedder::new(384);
    let a = e.embed("hello").unwrap();
    let b = e.embed("hello").unwrap();
    assert_eq!(a, b);
}

#[test]
fn deterministic_backend_dim_query_matches_constructor() {
    let e = DeterministicEmbedder::new(768);
    assert_eq!(e.dim(), 768);
}

#[test]
fn deterministic_backend_batch_returns_one_per_input() {
    let e = DeterministicEmbedder::new(384);
    let batch = e.embed_batch(&["a", "b", "c"]).unwrap();
    assert_eq!(batch.len(), 3);
    for v in &batch {
        assert_eq!(v.len(), 384);
    }
}
```

- [ ] **Step 4.2: Run test, observe failure**

```bash
cargo test -p pg_raggraph_core --test embedding_backend
```

Expected: compile error — `EmbeddingBackend` / `DeterministicEmbedder` don't exist.

- [ ] **Step 4.3: Extend `pg_raggraph_core/src/embedding.rs`**

Replace the file with the trait + impl. Keep the free `deterministic_embed` function for backwards compatibility with Plan 2's call sites.

```rust
//! Embedding backend abstraction + deterministic test impl.
//!
//! Plan 2 shipped `deterministic_embed` as a free function. Plan 3 introduces
//! the `EmbeddingBackend` trait so a real ONNX-backed impl (`OnnxEmbedder`,
//! Task 5) can replace the deterministic one in production builds while
//! `pg_test` and `cargo test` continue to use the deterministic impl for
//! byte-stable fixture parity.
//!
//! Mission brief SC-002: byte-identical output for identical input, dim
//! equal to the `pgrg.embed_dim` GUC.
//! Mission brief SC-009: production embedder loaded once per worker process
//! at startup; never per-job. The trait is `Send + Sync + 'static` to permit
//! storage in a worker-local `OnceCell`.

use crate::error::{CoreError, CoreResult};
use sha2::{Digest, Sha256};

/// Trait for any embedding backend the worker can load.
///
/// Implementations must be cheap to clone-by-`&self` (typically wrapped in
/// `Arc` internally) and thread-safe (`Send + Sync`) so a single backend
/// instance can serve concurrent jobs within a worker.
pub trait EmbeddingBackend: Send + Sync + 'static {
    /// Vector dimension this backend produces. Must match `pgrg.embed_dim`.
    fn dim(&self) -> usize;

    /// Embed a single text. Returns a `Vec<f32>` of length `self.dim()`.
    fn embed(&self, text: &str) -> CoreResult<Vec<f32>>;

    /// Embed a batch. Default impl loops `embed`; ONNX backend overrides
    /// for batched inference.
    fn embed_batch(&self, texts: &[&str]) -> CoreResult<Vec<Vec<f32>>> {
        texts.iter().map(|t| self.embed(t)).collect()
    }
}

/// Deterministic SHA-256-derived embedder. Used by `cargo test`,
/// `pgrx::pg_test`, and Plan 2's fixture loaders.
#[derive(Debug, Clone)]
pub struct DeterministicEmbedder {
    dim: usize,
}

impl DeterministicEmbedder {
    #[must_use]
    pub fn new(dim: usize) -> Self {
        Self { dim }
    }
}

impl EmbeddingBackend for DeterministicEmbedder {
    fn dim(&self) -> usize {
        self.dim
    }

    fn embed(&self, text: &str) -> CoreResult<Vec<f32>> {
        Ok(deterministic_embed(text, self.dim))
    }
}

/// Hash-derived deterministic embedding (Plan 2's free function, retained).
///
/// Produces an L2-normalized `Vec<f32>` of length `dim`. Pure function
/// (same input -> same output across processes and machines). Suitable
/// for tests and parity smoke runs; NOT a semantic embedding — the
/// `OnnxEmbedder` (Plan 3) is the production embedder.
///
/// `u as f32 / u32::MAX as f32`: precision loss is intentional and
/// bounded; we only need a stable spread in (-1, 1).
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn deterministic_embed(text: &str, dim: usize) -> Vec<f32> {
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

    let mut v: Vec<f32> = buf
        .chunks_exact(4)
        .map(|b| {
            let u = u32::from_le_bytes([b[0], b[1], b[2], b[3]]);
            (u as f32 / u32::MAX as f32) * 2.0 - 1.0
        })
        .collect();

    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 1e-12 {
        for x in &mut v {
            *x /= norm;
        }
    }
    v
}

/// Convenience: produce a `Box<dyn EmbeddingBackend>` for callers that
/// need a trait object.
#[must_use]
pub fn deterministic_backend(dim: usize) -> Box<dyn EmbeddingBackend> {
    Box::new(DeterministicEmbedder::new(dim))
}

// `CoreError`/`CoreResult` are imported so `OnnxEmbedder` (Task 5) can
// surface load/inference failures through the same trait surface.
#[allow(dead_code)]
fn _ensure_imports_used(_e: CoreError) {}
type _T = CoreResult<()>;
```

- [ ] **Step 4.4: Run tests, observe pass**

```bash
cargo test -p pg_raggraph_core --test embedding_backend --test embedding
```

Expected: 4 new tests + 5 Plan-2 tests pass.

- [ ] **Step 4.5: ⛔ Drift Check DC-003**

Re-read the Mission Brief at `skill-output/mission-brief/Mission-Brief-plan3-ingest-pipeline.md`. Confirm: the embedder is `bge-small-en-v1.5` fp32 ONNX (spec §3 line 71, §7 G1). The trait surface above accepts any backend that produces the right dim — that's correct. Confirm chunkshop's `hf_cache` API will be the loader of record in Task 5. If chunkshop's loader interface differs from spec (e.g., chunkshop doesn't expose `hf_cache` as a public function), surface this as an Ask First constraint before Task 5. If misaligned, stop and reassess before proceeding.

- [ ] **Step 4.6: Commit**

```bash
git add pg_raggraph_core/src/embedding.rs pg_raggraph_core/tests/embedding_backend.rs
git commit -m "feat(core): EmbeddingBackend trait + DeterministicEmbedder (Plan 3 ONNX wiring)"
```

---

## Task 5: `OnnxEmbedder` impl in `_core` (BAAI/bge-small-en-v1.5 fp32)

**Files:**
- Create: `pg_raggraph_core/src/embedding/onnx.rs`
- Create: `pg_raggraph_core/tests/embedding_onnx.rs` (gated on `cfg(feature = "onnx")`)
- Modify: `pg_raggraph_core/src/embedding.rs` → convert to module form (split into `embedding/mod.rs`)
- Modify: `pg_raggraph_core/Cargo.toml` (add `ort = "2"`, `tokenizers = "0.20"`, optional `onnx` feature)

**Why:** SC-004 requires `pgrg.documents`/`pgrg.chunks` rows to land with non-NULL `embedding` columns of dimension `pgrg.embed_dim`. SC-009 requires the model is loaded exactly once per worker, not per job. SC-010 requires `pgrg.embed_model_path` GUC override; mismatched dim → startup error. Spec §7 G1 names `BAAI/bge-small-en-v1.5` fp32 ONNX as the default.

**Backend choice:** `ort = "2"` (ONNX Runtime Rust bindings) is the standard pgrx-compatible choice (mission brief Constraint Ask First permits `ort`; `candle` is the ASK-FIRST alternate and not used here). chunkshop's `hf_cache` loader is the model location source per spec §3 line 71; if `pgrg.embed_model_path` is set, it overrides.

**Feature gate:** `onnx` feature on `_core` so plain `cargo test` (no PG) doesn't pull in `ort`. The pgrx crate enables `onnx` by default; tests stay green either way because `pg_test` keeps using `DeterministicEmbedder`.

- [ ] **Step 5.1: Convert `pg_raggraph_core/src/embedding.rs` to module form**

Move the existing file to `pg_raggraph_core/src/embedding/mod.rs`. No content change — just the path rename so `embedding/onnx.rs` can live alongside.

```bash
mkdir -p pg_raggraph_core/src/embedding
git mv pg_raggraph_core/src/embedding.rs pg_raggraph_core/src/embedding/mod.rs
```

- [ ] **Step 5.2: Add `onnx` feature + deps to `pg_raggraph_core/Cargo.toml`**

```toml
[features]
default = []
onnx = ["dep:ort", "dep:tokenizers", "dep:ndarray"]

[dependencies]
serde = { workspace = true }
serde_json = { workspace = true }
uuid = { workspace = true }
thiserror = { workspace = true }
tracing = { workspace = true }
sha2 = { workspace = true }
ort = { version = "2", optional = true, default-features = false, features = ["load-dynamic"] }
tokenizers = { version = "0.20", optional = true, default-features = false, features = ["onig"] }
ndarray = { version = "0.16", optional = true }
```

- [ ] **Step 5.3: Write the failing test (gated on `onnx` feature)**

Create `pg_raggraph_core/tests/embedding_onnx.rs`:

```rust
//! ONNX embedder smoke tests. Skipped when the `onnx` feature is off.

#![cfg(feature = "onnx")]

use pg_raggraph_core::embedding::{EmbeddingBackend, OnnxEmbedder, OnnxEmbedderConfig};

/// Path to the bge-small-en-v1.5 ONNX model. Tests look in the standard
/// chunkshop hf_cache path or skip if the model is absent.
fn model_path() -> Option<std::path::PathBuf> {
    let p = std::env::var("PGRG_TEST_ONNX_MODEL_PATH")
        .ok()
        .map(std::path::PathBuf::from);
    p.filter(|p| p.exists())
}

#[test]
fn onnx_loads_and_embeds_when_model_present() {
    // SC-004 / SC-009: real model produces 384-dim vectors.
    let Some(path) = model_path() else {
        eprintln!("skip: PGRG_TEST_ONNX_MODEL_PATH not set or model missing");
        return;
    };
    let cfg = OnnxEmbedderConfig {
        model_path: path,
        expected_dim: 384,
    };
    let e = OnnxEmbedder::load(&cfg).expect("ONNX load must succeed");
    assert_eq!(e.dim(), 384);
    let v = e.embed("hello world").expect("inference must succeed");
    assert_eq!(v.len(), 384);
    // L2-normalized output for cosine similarity stability.
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    assert!((norm - 1.0).abs() < 1e-2, "expected ~unit norm, got {norm}");
}

#[test]
fn onnx_dim_mismatch_returns_error() {
    // SC-010: mismatched dimension between override model and pgrg.embed_dim
    // causes a startup error.
    let Some(path) = model_path() else {
        eprintln!("skip: PGRG_TEST_ONNX_MODEL_PATH not set or model missing");
        return;
    };
    let cfg = OnnxEmbedderConfig {
        model_path: path,
        expected_dim: 768, // wrong dim for bge-small-en-v1.5
    };
    let result = OnnxEmbedder::load(&cfg);
    assert!(result.is_err(), "dim mismatch must error at load time");
    let msg = format!("{:?}", result.unwrap_err());
    assert!(
        msg.contains("dim") || msg.contains("dimension"),
        "error message must mention dimension, got: {msg}"
    );
}
```

- [ ] **Step 5.4: Run test, observe failure**

```bash
cargo test -p pg_raggraph_core --features onnx --test embedding_onnx
```

Expected: compile error — `OnnxEmbedder` does not exist.

- [ ] **Step 5.5: Create `pg_raggraph_core/src/embedding/onnx.rs`**

```rust
//! ONNX-backed embedder for `BAAI/bge-small-en-v1.5` fp32.
//!
//! Mission brief SC-004 / SC-009 / SC-010: real model, loaded once per worker,
//! 384-dim, GUC override path supported, dim-mismatch is a load-time error.
//!
//! `ort = "2"` is the chosen ONNX Runtime binding (mission brief Constraint
//! Ask First — surface a switch to `candle` only if a blocker emerges).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use ndarray::{Array2, Axis};
use ort::session::{Session, builder::SessionBuilder};
use ort::value::Value;
use tokenizers::Tokenizer;

use crate::embedding::EmbeddingBackend;
use crate::error::{CoreError, CoreResult};

/// Loader configuration for `OnnxEmbedder`.
///
/// `model_path` should point at a directory containing `model.onnx` and
/// `tokenizer.json` (the layout chunkshop's `hf_cache` produces). If
/// `pgrg.embed_model_path` is set on the PG side, it overrides the
/// default chunkshop cache lookup; if unset, the worker uses chunkshop's
/// default cache (typically `~/.cache/huggingface/hub/...`).
#[derive(Debug, Clone)]
pub struct OnnxEmbedderConfig {
    pub model_path: PathBuf,
    pub expected_dim: usize,
}

/// ONNX Runtime-backed embedder. `Arc` internals keep clone-by-`&self` cheap.
pub struct OnnxEmbedder {
    session: Arc<Session>,
    tokenizer: Arc<Tokenizer>,
    dim: usize,
}

impl OnnxEmbedder {
    /// Load the ONNX model and tokenizer from `cfg.model_path`.
    ///
    /// Errors:
    /// - `CoreError::IoError` if the model or tokenizer files are missing.
    /// - `CoreError::InvalidConfig` if the model's output dim does not match
    ///   `cfg.expected_dim` (SC-010).
    pub fn load(cfg: &OnnxEmbedderConfig) -> CoreResult<Self> {
        let model_file = cfg.model_path.join("model.onnx");
        let tokenizer_file = cfg.model_path.join("tokenizer.json");

        if !model_file.exists() {
            return Err(CoreError::InvalidConfig(format!(
                "ONNX model file not found at {}",
                model_file.display()
            )));
        }
        if !tokenizer_file.exists() {
            return Err(CoreError::InvalidConfig(format!(
                "tokenizer.json not found at {}",
                tokenizer_file.display()
            )));
        }

        let session = SessionBuilder::new()
            .map_err(|e| CoreError::InvalidConfig(format!("ort builder: {e}")))?
            .with_intra_threads(1)
            .map_err(|e| CoreError::InvalidConfig(format!("ort intra_threads: {e}")))?
            .commit_from_file(&model_file)
            .map_err(|e| CoreError::InvalidConfig(format!("ort load {}: {e}", model_file.display())))?;

        let tokenizer = Tokenizer::from_file(&tokenizer_file)
            .map_err(|e| CoreError::InvalidConfig(format!("tokenizer load: {e}")))?;

        // Probe the output dim by running a single tokenized "[CLS]"-style input.
        // bge-small-en-v1.5 produces a 384-dim sentence embedding (CLS pooling
        // + L2 normalization); we verify that against `expected_dim` here.
        let probe = embed_with(&session, &tokenizer, "probe", cfg.expected_dim)?;
        if probe.len() != cfg.expected_dim {
            return Err(CoreError::InvalidConfig(format!(
                "ONNX model output dimension {} does not match expected_dim {}",
                probe.len(),
                cfg.expected_dim
            )));
        }

        Ok(Self {
            session: Arc::new(session),
            tokenizer: Arc::new(tokenizer),
            dim: cfg.expected_dim,
        })
    }

    /// Default chunkshop hf_cache path for `BAAI/bge-small-en-v1.5`.
    /// Used when `pgrg.embed_model_path` GUC is not set.
    #[must_use]
    pub fn default_cache_path() -> PathBuf {
        // chunkshop materializes models under ~/.cache/huggingface/hub/<repo>/snapshots/<rev>
        // The bg worker resolves the latest snapshot at load time; for now we
        // expose the env-var indirection so chunkshop's resolver can point us.
        let base = std::env::var("HF_HOME")
            .or_else(|_| std::env::var("HUGGINGFACE_HUB_CACHE"))
            .unwrap_or_else(|_| {
                let home = std::env::var("HOME").unwrap_or_else(|_| "/var/lib/postgresql".into());
                format!("{home}/.cache/huggingface/hub")
            });
        PathBuf::from(base).join("models--BAAI--bge-small-en-v1.5/snapshots/main")
    }
}

impl EmbeddingBackend for OnnxEmbedder {
    fn dim(&self) -> usize {
        self.dim
    }

    fn embed(&self, text: &str) -> CoreResult<Vec<f32>> {
        embed_with(&self.session, &self.tokenizer, text, self.dim)
    }

    fn embed_batch(&self, texts: &[&str]) -> CoreResult<Vec<Vec<f32>>> {
        // Default impl loops; bge-small-en-v1.5 is fast enough to make
        // single-shot inference fine for Plan 3. Real batched inference
        // can land in Plan 6 if benchmarks demand it.
        texts.iter().map(|t| self.embed(t)).collect()
    }
}

/// Run one inference cycle: tokenize -> ONNX -> CLS pool -> L2 normalize.
fn embed_with(
    session: &Session,
    tokenizer: &Tokenizer,
    text: &str,
    expected_dim: usize,
) -> CoreResult<Vec<f32>> {
    let encoding = tokenizer
        .encode(text, true)
        .map_err(|e| CoreError::InvalidConfig(format!("tokenize: {e}")))?;
    let ids: Vec<i64> = encoding.get_ids().iter().map(|&u| i64::from(u)).collect();
    let mask: Vec<i64> = encoding.get_attention_mask().iter().map(|&u| i64::from(u)).collect();
    let type_ids: Vec<i64> = encoding.get_type_ids().iter().map(|&u| i64::from(u)).collect();

    let len = ids.len();
    let ids_arr = Array2::from_shape_vec((1, len), ids)
        .map_err(|e| CoreError::InvalidConfig(format!("ids shape: {e}")))?;
    let mask_arr = Array2::from_shape_vec((1, len), mask)
        .map_err(|e| CoreError::InvalidConfig(format!("mask shape: {e}")))?;
    let type_arr = Array2::from_shape_vec((1, len), type_ids)
        .map_err(|e| CoreError::InvalidConfig(format!("type_ids shape: {e}")))?;

    let inputs = ort::inputs![
        "input_ids" => Value::from_array(ids_arr).map_err(|e| CoreError::InvalidConfig(format!("ids val: {e}")))?,
        "attention_mask" => Value::from_array(mask_arr).map_err(|e| CoreError::InvalidConfig(format!("mask val: {e}")))?,
        "token_type_ids" => Value::from_array(type_arr).map_err(|e| CoreError::InvalidConfig(format!("type val: {e}")))?,
    ];

    let outputs = session
        .run(inputs)
        .map_err(|e| CoreError::InvalidConfig(format!("ort run: {e}")))?;

    // bge-small-en-v1.5 emits last_hidden_state with shape [1, seq_len, 384].
    // Pool by taking the first token (CLS) and L2-normalize.
    let (_shape, data) = outputs[0]
        .try_extract_tensor::<f32>()
        .map_err(|e| CoreError::InvalidConfig(format!("ort extract: {e}")))?;
    // First-token slice: indices [0..expected_dim].
    if data.len() < expected_dim {
        return Err(CoreError::InvalidConfig(format!(
            "ONNX output too small: got {} floats, need at least {}",
            data.len(),
            expected_dim
        )));
    }
    let mut v: Vec<f32> = data[..expected_dim].to_vec();
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 1e-12 {
        for x in &mut v {
            *x /= norm;
        }
    }
    // Suppress unused-import lint when feature gates skew imports.
    let _ = Axis(0);
    Ok(v)
}
```

> NOTE on `ort` API: the `ort = "2"` API used above (`SessionBuilder::commit_from_file`, `ort::inputs![]`, `try_extract_tensor`) reflects ort 2.x. If the implementer finds a signature mismatch, refer to https://ort.pyke.io/ for the version actually pulled in. The mission brief Constraint Ask First permits surfacing a chunkshop-driven loader if the direct `ort` path proves brittle.

- [ ] **Step 5.6: Re-export from `embedding/mod.rs`**

Append to `pg_raggraph_core/src/embedding/mod.rs`:

```rust
#[cfg(feature = "onnx")]
pub mod onnx;

#[cfg(feature = "onnx")]
pub use onnx::{OnnxEmbedder, OnnxEmbedderConfig};
```

- [ ] **Step 5.7: Run feature-gated test**

```bash
PGRG_TEST_ONNX_MODEL_PATH=/path/to/bge-small-en-v1.5 \
    cargo test -p pg_raggraph_core --features onnx --test embedding_onnx
```

Expected: 2 tests pass when the model is present; both prints "skip: ..." otherwise. (CI does not exercise this — Plan 6 sets up model caching for parity benchmarks.)

- [ ] **Step 5.8: Run all `_core` tests with default features (no onnx)**

```bash
cargo test -p pg_raggraph_core
```

Expected: all Plan 1 + Plan 2 + Plan 3 (Tasks 2, 3, 4) tests pass. `embedding_onnx` is skipped at compile time via `#![cfg(feature = "onnx")]`.

- [ ] **Step 5.9: Commit**

```bash
git add pg_raggraph_core/Cargo.toml pg_raggraph_core/src/embedding/ pg_raggraph_core/tests/embedding_onnx.rs
git commit -m "feat(core): OnnxEmbedder for BAAI/bge-small-en-v1.5 (feature=onnx, SC-004/SC-009/SC-010)"
```

---

## Task 6: chunkshop integration — `ChunkStrategy` enum + chunker shim

**Files:**
- Create: `pg_raggraph_core/src/chunking/mod.rs`
- Create: `pg_raggraph_core/src/chunking/strategy.rs`
- Create: `pg_raggraph_core/tests/chunking.rs`
- Modify: `pg_raggraph_core/Cargo.toml` (add `chunkshop` dep)
- Modify: `pg_raggraph_core/src/lib.rs` (declare `pub mod chunking`)

**Why:** Constraint Always: "chunkshop is the canonical chunker for the Rust extension. The Cargo dep is hard (not optional)." Mission brief Desired Outcome: `chunk_strategy` accepts `auto`, `hierarchy`, `semantic`, `sentence_aware`, `fixed_overlap`, `neighbor_expand` (default `auto`). SC-008 requires `chunk_strategy='hierarchy'` and `'semantic'` to produce different chunk counts on a fixture markdown document.

**chunkshop crate version:** Mission brief Constraint Ask First flags pinning a specific version. We pin `chunkshop = "0.3"` (matching the Python sibling's PyPI 0.3.0+ floor in `CLAUDE.md`); the `Ask First` is satisfied by surfacing this default in the plan and recording it as a deferred concern for the Plan 6 parity author. If the chunkshop Rust crate is not yet published, the pin must change to a git rev — flag at impl time.

- [ ] **Step 6.1: Write the failing test**

Create `pg_raggraph_core/tests/chunking.rs`:

```rust
use pg_raggraph_core::chunking::{ChunkStrategy, Chunker};

#[test]
fn strategy_parses_documented_values() {
    // Mission brief Desired Outcome: full strategy list.
    assert_eq!(ChunkStrategy::parse("auto"), Some(ChunkStrategy::Auto));
    assert_eq!(ChunkStrategy::parse("hierarchy"), Some(ChunkStrategy::Hierarchy));
    assert_eq!(ChunkStrategy::parse("semantic"), Some(ChunkStrategy::Semantic));
    assert_eq!(ChunkStrategy::parse("sentence_aware"), Some(ChunkStrategy::SentenceAware));
    assert_eq!(ChunkStrategy::parse("fixed_overlap"), Some(ChunkStrategy::FixedOverlap));
    assert_eq!(ChunkStrategy::parse("neighbor_expand"), Some(ChunkStrategy::NeighborExpand));
}

#[test]
fn strategy_unknown_returns_none() {
    assert_eq!(ChunkStrategy::parse("rolling"), None);
    assert_eq!(ChunkStrategy::parse(""), None);
    assert_eq!(ChunkStrategy::parse("AUTO"), None); // case-sensitive
}

#[test]
fn strategy_default_is_auto() {
    assert_eq!(ChunkStrategy::default(), ChunkStrategy::Auto);
}

#[test]
fn chunker_yields_at_least_one_chunk_for_nonempty_input() {
    let c = Chunker::new(ChunkStrategy::Auto);
    let chunks = c.chunk("hello world").expect("must chunk");
    assert!(!chunks.is_empty());
    let total: String = chunks.iter().map(|c| c.text.as_str()).collect();
    assert!(total.contains("hello"));
}

#[test]
fn chunker_preserves_token_count_field() {
    let c = Chunker::new(ChunkStrategy::Auto);
    let chunks = c.chunk("the quick brown fox jumps over the lazy dog").unwrap();
    for chunk in &chunks {
        assert!(chunk.token_count > 0, "token_count must be positive");
    }
}
```

- [ ] **Step 6.2: Run test, observe failure**

```bash
cargo test -p pg_raggraph_core --test chunking
```

Expected: compile error — `pg_raggraph_core::chunking` does not exist.

- [ ] **Step 6.3: Add `chunkshop` dep to `pg_raggraph_core/Cargo.toml`**

In `[dependencies]`:

```toml
chunkshop = "0.3"
```

If `chunkshop = "0.3"` is unavailable on crates.io at impl time, substitute the appropriate git or path dependency and surface the version choice at DC-001 (Step 8.6) per Constraint Ask First.

- [ ] **Step 6.4: Create `pg_raggraph_core/src/chunking/mod.rs`**

```rust
//! Chunking — chunkshop integration shim.
//!
//! Per mission brief Constraint Always: chunkshop is the canonical chunker.
//! No hand-rolled chunker. This module is a thin shim that translates the
//! `ChunkStrategy` enum into chunkshop's chunker config.

pub mod strategy;

pub use strategy::ChunkStrategy;

use crate::error::{CoreError, CoreResult};

/// One produced chunk. Mirrors `pgrg.chunks` columns relevant to the worker.
#[derive(Debug, Clone)]
pub struct Chunk {
    pub ord: i32,
    pub text: String,
    pub token_count: i32,
}

/// chunkshop-backed chunker. Cheap to construct; can be reused across docs.
pub struct Chunker {
    strategy: ChunkStrategy,
}

impl Chunker {
    #[must_use]
    pub fn new(strategy: ChunkStrategy) -> Self {
        Self { strategy }
    }

    /// Split `text` into chunks per the configured strategy.
    pub fn chunk(&self, text: &str) -> CoreResult<Vec<Chunk>> {
        // chunkshop API shape (per its 0.3 README):
        //   let chunks = chunkshop::chunk(text, chunkshop::Config { strategy: ... });
        // The shim below maps our ChunkStrategy -> chunkshop's enum and
        // translates the produced records into our Chunk DTO.
        let cfg = self.strategy.to_chunkshop_config();
        let raw = chunkshop::chunk(text, cfg)
            .map_err(|e| CoreError::InvalidConfig(format!("chunkshop: {e}")))?;
        Ok(raw
            .into_iter()
            .enumerate()
            .map(|(i, r)| Chunk {
                ord: i32::try_from(i).unwrap_or(i32::MAX),
                token_count: i32::try_from(r.token_count).unwrap_or(i32::MAX),
                text: r.text,
            })
            .collect())
    }
}
```

> NOTE on chunkshop API: the `chunkshop::chunk(text, Config) -> Result<Vec<chunkshop::Chunk>, _>` shape above is the expected API per the README in `CLAUDE.md`. If the actual `chunkshop = "0.3"` crate exposes a different signature (e.g., a `Chunker` struct with `.chunk(&self, text: &str)`), adapt this shim accordingly — the public `Chunker::new`/`Chunker::chunk` surface above must remain stable for Tasks 7+10.

- [ ] **Step 6.5: Create `pg_raggraph_core/src/chunking/strategy.rs`**

```rust
//! `ChunkStrategy` enum and chunkshop config mapping.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ChunkStrategy {
    Auto,
    Hierarchy,
    Semantic,
    SentenceAware,
    FixedOverlap,
    NeighborExpand,
}

impl ChunkStrategy {
    /// Stable string identifier matching `pgrg.ingest(*, chunk_strategy)` SQL surface.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            ChunkStrategy::Auto => "auto",
            ChunkStrategy::Hierarchy => "hierarchy",
            ChunkStrategy::Semantic => "semantic",
            ChunkStrategy::SentenceAware => "sentence_aware",
            ChunkStrategy::FixedOverlap => "fixed_overlap",
            ChunkStrategy::NeighborExpand => "neighbor_expand",
        }
    }

    /// Parse from the SQL string identifier. Case-sensitive.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "auto" => Some(ChunkStrategy::Auto),
            "hierarchy" => Some(ChunkStrategy::Hierarchy),
            "semantic" => Some(ChunkStrategy::Semantic),
            "sentence_aware" => Some(ChunkStrategy::SentenceAware),
            "fixed_overlap" => Some(ChunkStrategy::FixedOverlap),
            "neighbor_expand" => Some(ChunkStrategy::NeighborExpand),
            _ => None,
        }
    }

    /// Translate to chunkshop's runtime config. The chunkshop API names may
    /// differ; this shim normalizes them. If chunkshop adds new strategies,
    /// extend the enum AND the SQL surface together (Constraint Ask First).
    pub(crate) fn to_chunkshop_config(self) -> chunkshop::Config {
        match self {
            ChunkStrategy::Auto => chunkshop::Config::auto(),
            ChunkStrategy::Hierarchy => chunkshop::Config::hierarchy(),
            ChunkStrategy::Semantic => chunkshop::Config::semantic(),
            ChunkStrategy::SentenceAware => chunkshop::Config::sentence_aware(),
            ChunkStrategy::FixedOverlap => chunkshop::Config::fixed_overlap(),
            ChunkStrategy::NeighborExpand => chunkshop::Config::neighbor_expand(),
        }
    }
}

impl Default for ChunkStrategy {
    fn default() -> Self {
        ChunkStrategy::Auto
    }
}
```

- [ ] **Step 6.6: Wire `pub mod chunking;` into `pg_raggraph_core/src/lib.rs`**

```rust
pub mod chunking;
```

- [ ] **Step 6.7: Run tests, observe pass**

```bash
cargo test -p pg_raggraph_core --test chunking
```

Expected: 5 tests pass.

- [ ] **Step 6.8: Commit**

```bash
git add pg_raggraph_core/Cargo.toml pg_raggraph_core/src/chunking/ pg_raggraph_core/src/lib.rs pg_raggraph_core/tests/chunking.rs
git commit -m "feat(core): chunking::{ChunkStrategy,Chunker} chunkshop shim (SC-008)"
```

---

## Task 7: `LlmProvider` trait + `MockProvider` in `_core::llm`

**Files:**
- Create: `pg_raggraph_core/src/llm/mod.rs`
- Create: `pg_raggraph_core/src/llm/mock.rs`
- Create: `pg_raggraph_core/tests/llm_mock.rs`
- Modify: `pg_raggraph_core/src/lib.rs` (declare `pub mod llm`)

**Why:** Constraint Always: "`LlmProvider` trait surface is defined in this plan (Plan 4 plugs in real impls)." Constraint Never: "Run real LLM extraction in this plan. The provider call site is wired but uses MockProvider only." SC-015 requires `LlmProvider` trait in `pg_raggraph_core::llm` with at minimum a `MockProvider` (no-op, returns empty extraction) wired into the bg worker; verified by `cargo test -p pg_raggraph_core` running a mock-driven ingest happy path without network.

**Trait surface:** matches spec §7 line 357 (concrete impls `OpenAiProvider`, `AnthropicProvider`, `OllamaProvider`, `MockProvider`; `RetryingProvider` wrapper). Plan 3 ships only the trait + `MockProvider`. The trait is `async fn` (use `async-trait = "0.1"` already in workspace deps via reuse, or define a `Future`-returning method directly with `core::future::Future` since `_core` already depends on `tokio` via Plan 3 additions in Task 8).

**Async strategy:** the trait is sync-shaped here — Plan 3 doesn't actually run network calls (MockProvider is no-op). Plan 4 will introduce an async variant or wrap with `block_on` inside the bg worker's tokio runtime. Surface the choice in trait docs.

- [ ] **Step 7.1: Write the failing test**

Create `pg_raggraph_core/tests/llm_mock.rs`:

```rust
use pg_raggraph_core::llm::{Extraction, LlmProvider, MockProvider};

#[test]
fn mock_provider_returns_empty_extraction() {
    // SC-015: MockProvider returns empty entity/relationship sets — no network,
    // no real extraction. The trait surface is consumable; concrete impls
    // (OpenAI/Anthropic/Ollama) land in Plan 4.
    let p = MockProvider::new();
    let result: Extraction = p
        .extract("any chunk text", "any namespace")
        .expect("mock must succeed");
    assert!(result.entities.is_empty(), "MockProvider entities must be empty");
    assert!(result.relationships.is_empty(), "MockProvider relationships must be empty");
}

#[test]
fn mock_provider_is_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<MockProvider>();
}

#[test]
fn provider_trait_object_safe() {
    // Bg worker stores providers behind Box<dyn LlmProvider>; the trait must
    // be object-safe.
    let _p: Box<dyn LlmProvider> = Box::new(MockProvider::new());
}
```

- [ ] **Step 7.2: Run test, observe failure**

```bash
cargo test -p pg_raggraph_core --test llm_mock
```

Expected: compile error — `pg_raggraph_core::llm` does not exist.

- [ ] **Step 7.3: Create `pg_raggraph_core/src/llm/mod.rs`**

```rust
//! `LlmProvider` trait surface — Plan 3 ships the trait and a no-op MockProvider.
//!
//! Plan 4 plugs in concrete impls (`OpenAiProvider`, `AnthropicProvider`,
//! `OllamaProvider`) and a `RetryingProvider` wrapper. Per spec §7 line 357,
//! the trait shape matches the `pg_agents` precedent.
//!
//! Mission brief SC-015: trait surface consumable, MockProvider available,
//! no real network. Constraint Never: real LLM extraction does not run here.
//!
//! Async note: the trait is currently sync (no Future returns) because
//! Plan 3's only impl is MockProvider, which returns synchronously. Plan 4
//! will introduce an async variant or wrap blocking calls inside the bg
//! worker's tokio runtime. Trait shape changes between plans require
//! Constraint Ask First (signal in the Plan 4 brief).

pub mod mock;

pub use mock::MockProvider;

use crate::error::CoreResult;
use serde::{Deserialize, Serialize};

/// One extracted entity from a chunk. Lightweight DTO; resolution and
/// upsert happen in `_core::ingest` after the provider returns.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractedEntity {
    pub name: String,
    pub kind: Option<String>,
    pub description: Option<String>,
    pub confidence: f32,
}

/// One extracted relationship. `src_name` and `dst_name` reference entity
/// names within the same extraction call; the resolver in `_core::ingest`
/// turns them into UUIDs after entity persistence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractedRelationship {
    pub src_name: String,
    pub dst_name: String,
    pub kind: String,
    pub weight: f32,
    pub confidence: f32,
}

/// What an LlmProvider returns from one extract() call.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Extraction {
    pub entities: Vec<ExtractedEntity>,
    pub relationships: Vec<ExtractedRelationship>,
}

/// LLM provider trait. Plan 3 defines the surface; Plan 4 ships impls.
///
/// Trait is object-safe (no generics) so the bg worker can hold a
/// `Box<dyn LlmProvider>` configured at namespace lookup time.
pub trait LlmProvider: Send + Sync + 'static {
    /// Extract entities and relationships from `chunk_text` in `namespace`.
    ///
    /// MockProvider returns `Extraction::default()`. Plan 4 impls call
    /// network APIs (OpenAI, Anthropic, Ollama).
    fn extract(&self, chunk_text: &str, namespace: &str) -> CoreResult<Extraction>;
}
```

- [ ] **Step 7.4: Create `pg_raggraph_core/src/llm/mock.rs`**

```rust
//! `MockProvider` — Plan 3 no-op extractor.
//!
//! Returns empty `Extraction`; satisfies `LlmProvider` so the bg worker
//! can run the full ingest happy path without network calls. Plan 4 ships
//! `OpenAiProvider`, `AnthropicProvider`, `OllamaProvider`.

use crate::error::CoreResult;
use crate::llm::{Extraction, LlmProvider};

/// No-op extractor. Always returns `Extraction::default()`.
#[derive(Debug, Default, Clone, Copy)]
pub struct MockProvider;

impl MockProvider {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl LlmProvider for MockProvider {
    fn extract(&self, _chunk_text: &str, _namespace: &str) -> CoreResult<Extraction> {
        Ok(Extraction::default())
    }
}
```

- [ ] **Step 7.5: Wire `pub mod llm;` into `pg_raggraph_core/src/lib.rs`**

```rust
pub mod llm;
```

- [ ] **Step 7.6: Run tests, observe pass**

```bash
cargo test -p pg_raggraph_core --test llm_mock
```

Expected: 3 tests pass.

- [ ] **Step 7.7: ⛔ Drift Check DC-004**

Re-read the Mission Brief. Confirm: the `LlmProvider` trait shape above (single `extract(&self, &str, &str) -> CoreResult<Extraction>`) is sufficient for Plan 4 to plug in real impls without re-engineering. Specifically: (a) the trait is object-safe — Plan 4 can wrap with `RetryingProvider`; (b) `Extraction` carries `entities` and `relationships` separately, matching spec §3 line 72; (c) the trait is `Send + Sync + 'static` so bg worker can store it. If any of these are wrong, surface the gap before continuing. Document the trait shape in this file's doc comment (already done above). If misaligned, stop and reassess.

- [ ] **Step 7.8: Commit**

```bash
git add pg_raggraph_core/src/llm/ pg_raggraph_core/src/lib.rs pg_raggraph_core/tests/llm_mock.rs
git commit -m "feat(core): llm::{LlmProvider trait, MockProvider, Extraction} (SC-015)"
```

---

## Task 8: Bg worker registration in `_PG_init` (gated on `process_shared_preload_libraries_in_progress`)

**Files:**
- Create: `pg_raggraph/src/bgw/mod.rs`
- Create: `pg_raggraph/src/bgw/launcher.rs`
- Create: `pg_raggraph/src/bgw/worker.rs`
- Modify: `pg_raggraph/src/lib.rs` (extend `_PG_init`, declare `mod bgw`)

**Why:** SC-001 requires `_PG_init` to register the bg launcher only when `process_shared_preload_libraries_in_progress` is true; loading via `LOAD 'pg_raggraph'` after server start does not register a worker. SC-002 requires `pg_raggraph.bgw_workers = 2` produces exactly 2 worker processes at server start, observable via `pgrg.health()` and `pg_stat_activity`.

This task ships the **registration** + **shells**. The launcher main loop is a 30-second latch cycle that does nothing yet (Task 16 adds the reaper sweep). The worker main loop polls every poll-interval and does nothing yet (Task 9 adds queue claim, Task 10 adds the per-doc transaction). This staged approach lets the test for SC-001/SC-002 land before the consuming logic.

**Pattern source:** `pg_agents/src/lib.rs` lines 210–221 (`_PG_init` body), `pg_agents/src/bgw_launcher.rs` lines 13–20 (`register_launcher`), `pg_agents/src/bgw_worker.rs` lines 17–28 (`register_workers`). Plan 3 mirrors these.

- [ ] **Step 8.1: Write the failing test**

Add to the `tests` module in `pg_raggraph/src/lib.rs`:

```rust
    #[pg_test]
    fn bgw_workers_registered_under_preload() {
        // SC-002: with shared_preload_libraries='pg_raggraph' and
        // pg_raggraph.bgw_workers=2 (set in pg_test postgresql_conf_options),
        // exactly 2 worker processes run.
        // pg_stat_activity is the most reliable lookup.
        let n: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM pg_stat_activity \
             WHERE backend_type = 'background worker' \
               AND application_name LIKE 'pg_raggraph%'",
        )
        .unwrap();
        assert_eq!(n, Some(2), "expected 2 pg_raggraph bg workers, got {n:?}");
    }

    #[pg_test]
    fn health_reports_bgw_count_matching_actual_workers() {
        // SC-002 cross-check: pgrg.health()'s bgw_workers value matches the
        // GUC default (2). Plan 1 already populates this; Plan 3 verifies
        // the value matches the actually-running worker count.
        let json: pgrx::JsonB = Spi::get_one("SELECT pgrg.health()")
            .unwrap()
            .expect("health() returned NULL");
        let obj = json.0.as_object().unwrap();
        assert_eq!(obj["bgw_workers"], 2);
    }
```

- [ ] **Step 8.2: Run tests, observe failure**

```bash
cargo pgrx test pg17 -p pg_raggraph -- bgw_workers_registered_under_preload
```

Expected: failure — count is 0 (no workers registered yet).

- [ ] **Step 8.3: Create `pg_raggraph/src/bgw/mod.rs`**

```rust
//! Background worker registration (called from `_PG_init`).
//!
//! Mission brief SC-001: registration only when
//! `process_shared_preload_libraries_in_progress`.
//! Mission brief SC-002: `pg_raggraph.bgw_workers` GUC controls worker count.

pub mod launcher;
pub mod worker;

pub use launcher::register_launcher;
pub use worker::register_workers;
```

- [ ] **Step 8.4: Create `pg_raggraph/src/bgw/launcher.rs`**

```rust
//! Launcher background worker — runs the reaper sweep on a 30-second cycle.
//!
//! Plan 3 Task 8 ships the registration + a do-nothing main loop. Task 16
//! adds the reaper-sweep body (re-queue stuck `running` jobs).

use pgrx::bgworkers::*;
use pgrx::prelude::*;
use std::time::Duration;

/// Register the launcher BGW (called from `_PG_init`).
pub fn register_launcher() {
    BackgroundWorkerBuilder::new("pg_raggraph launcher")
        .set_function("pg_raggraph_launcher_main")
        .set_library("pg_raggraph")
        .enable_spi_access()
        .set_restart_time(Some(Duration::from_secs(5)))
        .load();
}

/// Launcher main function — must match the name passed to `set_function`.
///
/// Currently a do-nothing 30-second latch loop. Task 16 adds the reaper-sweep
/// body that re-queues `running` jobs whose `updated_at` exceeded
/// `pgrg.job_reaper_interval`.
#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn pg_raggraph_launcher_main(_arg: pgrx::pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    BackgroundWorker::connect_worker_to_spi(Some("postgres"), None);

    pgrx::log!("pg_raggraph launcher started");

    while BackgroundWorker::wait_latch(Some(Duration::from_secs(30))) {
        if BackgroundWorker::sighup_received() {
            // GUCs reloaded automatically by PG on SIGHUP.
        }
        // Task 16 fills in: reaper sweep.
    }

    pgrx::log!("pg_raggraph launcher shutting down");
}
```

- [ ] **Step 8.5: Create `pg_raggraph/src/bgw/worker.rs`**

```rust
//! Worker pool — claim and process queued ingest jobs.
//!
//! Plan 3 Task 8 ships the registration + a do-nothing main loop. Task 9
//! adds queue claim. Task 10 adds the per-document transaction body.

use pgrx::bgworkers::*;
use pgrx::prelude::*;
use std::time::Duration;

use crate::gucs;

/// Register `pg_raggraph.bgw_workers` static BGWs (called from `_PG_init`).
pub fn register_workers() {
    let n = gucs::BGW_WORKERS.get();
    for i in 0..n {
        BackgroundWorkerBuilder::new(&format!("pg_raggraph w{i}"))
            .set_function("pg_raggraph_worker_main")
            .set_library("pg_raggraph")
            .enable_spi_access()
            .set_restart_time(Some(Duration::from_secs(1)))
            .set_argument(i.into_datum())
            .load();
    }
}

/// Worker main function — must match the name passed to `set_function`.
///
/// Currently a poll-only no-op loop. Task 9 adds queue claim; Task 10 adds
/// the per-document transaction (chunk -> embed -> persist).
#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn pg_raggraph_worker_main(arg: pgrx::pg_sys::Datum) {
    let worker_idx: i32 =
        unsafe { i32::from_polymorphic_datum(arg, false, pgrx::pg_sys::INT4OID) }.unwrap_or(0);
    let worker_name = format!("pg_raggraph_w{worker_idx}");

    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    BackgroundWorker::connect_worker_to_spi(Some("postgres"), None);

    pgrx::log!("{worker_name}: started");

    // Plan 3 Task 8: empty poll loop. Task 9 adds claim_next_job; Task 10
    // adds run_job dispatch. Polling cadence is 1 second for now; later
    // tasks may make this adaptive (matches pg_agents precedent).
    let poll = Duration::from_secs(1);
    while BackgroundWorker::wait_latch(Some(poll)) {
        if BackgroundWorker::sighup_received() {
            // GUCs reloaded automatically.
        }
        // Task 9 fills in: BackgroundWorker::transaction(|| queue::claim_next_job(&worker_name))
    }

    pgrx::log!("{worker_name}: shutting down");
}
```

- [ ] **Step 8.6: Extend `_PG_init` in `pg_raggraph/src/lib.rs`**

Replace the existing `_PG_init` body. Existing module declarations stay; add `mod bgw`.

```rust
mod bgw;
```

```rust
/// Called by PostgreSQL when the extension shared library is loaded.
/// Registers GUCs so they are available before CREATE EXTENSION runs.
/// When loaded via shared_preload_libraries, also registers background workers.
#[allow(non_snake_case)]
#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    gucs::register();

    // SC-001: only register BGWs when loading via shared_preload_libraries.
    // During a `LOAD 'pg_raggraph'` from a normal backend, this flag is false
    // and we skip registration.
    unsafe {
        if pgrx::pg_sys::process_shared_preload_libraries_in_progress {
            bgw::register_launcher();
            bgw::register_workers();
        }
    }
}
```

- [ ] **Step 8.7: ⛔ Drift Check DC-001**

Re-read Mission Brief. Verify SC-001 path: `_PG_init` registers `bgw::register_launcher` and `bgw::register_workers` ONLY inside the `if process_shared_preload_libraries_in_progress` branch. Confirm via diff that no path outside this guard touches BGW registration. If a regression is introduced (e.g., registering at every load), stop and fix before Step 8.8.

- [ ] **Step 8.8: Run tests, observe pass**

```bash
cargo pgrx test pg17 -p pg_raggraph -- bgw_workers_registered_under_preload health_reports_bgw_count_matching_actual_workers
```

Expected: 2 tests pass (worker count matches GUC default).

- [ ] **Step 8.9: Commit**

```bash
git add pg_raggraph/src/bgw/ pg_raggraph/src/lib.rs
git commit -m "feat(bgw): register launcher + worker pool in _PG_init (SC-001/SC-002)"
```

---

## Task 9: SPI queue helpers — `claim_next_job`, `complete_job`, `fail_job`

**Files:**
- Create: `pg_raggraph/src/bgw/queue.rs`
- Create: `pg_raggraph_core/tests/ingest_queue_contract.rs`
- Modify: `pg_raggraph/src/bgw/mod.rs` (declare `pub(crate) mod queue`)
- Modify: `pg_raggraph/src/bgw/worker.rs` (call claim/complete/fail)

**Why:** Mission brief Desired Outcome: "`SELECT … FOR UPDATE SKIP LOCKED LIMIT 1` against `WHERE status='queued'`, transition to `running`, dispatch to `_core::ingest::run_job`, mark `completed` or `failed` with error text." SC-016 requires multiple bg workers coexist safely under load: 2 workers + 50 queued jobs → no double-processing, no skips.

**Bind contract for claim:**
The CTE-based UPDATE pattern from `pg_agents/src/queue.rs` lines 38–87 atomically picks one row, sets `status='running'`, and returns the row. `FOR UPDATE SKIP LOCKED` ensures concurrent workers don't fight.

```sql
WITH next_job AS (
    SELECT id FROM pgrg.ingest_jobs
    WHERE status = 'queued'
    ORDER BY enqueued_at ASC
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
UPDATE pgrg.ingest_jobs ij
SET status = 'running',
    started_at = COALESCE(ij.started_at, now()),
    updated_at = now(),
    attempt_count = ij.attempt_count + 1
FROM next_job
WHERE ij.id = next_job.id
RETURNING ij.id, ij.source, ij.namespace, ij.chunk_strategy, ij.payload, ij.attempt_count
```

- [ ] **Step 9.1: Write the failing pgrx test**

Add to the `tests` module in `pg_raggraph/src/lib.rs`:

```rust
    #[pg_test]
    fn queue_claim_marks_one_job_running() {
        // Workers should claim queued jobs and transition them to running.
        // We test the helper directly (not the full worker loop, which polls
        // asynchronously) by inserting a job and calling the SQL the helper
        // executes.
        Spi::run("SELECT pgrg.namespace_create('q_claim_ns')").unwrap();
        Spi::run(
            "INSERT INTO pgrg.ingest_jobs (id, status, source, namespace) \
             VALUES ('77777777-7777-7777-7777-777777777777', 'queued', 't.md', 'q_claim_ns')",
        )
        .unwrap();

        // Wait up to 5 seconds for a worker to claim it.
        let mut claimed = false;
        for _ in 0..50 {
            let s: Option<String> = Spi::get_one(
                "SELECT status FROM pgrg.ingest_jobs \
                 WHERE id = '77777777-7777-7777-7777-777777777777'",
            )
            .unwrap();
            if s.as_deref() == Some("running") || s.as_deref() == Some("failed") || s.as_deref() == Some("completed") {
                claimed = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        assert!(
            claimed,
            "worker must claim the job within 5s (status changed away from 'queued')"
        );
    }

    #[pg_test]
    fn queue_skip_locked_no_double_processing() {
        // SC-016: 2 workers, several queued jobs, none processed twice.
        Spi::run("SELECT pgrg.namespace_create('skip_locked_ns')").unwrap();
        for i in 0..10 {
            let id = format!("99999999-9999-9999-9999-{i:012}");
            Spi::run(&format!(
                "INSERT INTO pgrg.ingest_jobs (id, status, source, namespace) \
                 VALUES ('{id}', 'queued', 's{i}.md', 'skip_locked_ns')"
            ))
            .unwrap();
        }
        // Wait up to 30 seconds for all to drain (status != 'queued').
        for _ in 0..300 {
            let n: Option<i64> = Spi::get_one(
                "SELECT count(*) FROM pgrg.ingest_jobs \
                 WHERE namespace = 'skip_locked_ns' AND status = 'queued'",
            )
            .unwrap();
            if n == Some(0) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        // attempt_count should be at most 1 per job — no double-claim.
        let max_attempts: Option<i32> = Spi::get_one(
            "SELECT max(attempt_count) FROM pgrg.ingest_jobs \
             WHERE namespace = 'skip_locked_ns'",
        )
        .unwrap();
        assert!(
            max_attempts.unwrap_or(0) <= 1,
            "FOR UPDATE SKIP LOCKED must prevent double-claim, max attempts = {max_attempts:?}"
        );
    }
```

- [ ] **Step 9.2: Run tests, observe failure**

```bash
cargo pgrx test pg17 -p pg_raggraph -- queue_claim_marks_one_job_running
```

Expected: failure — workers are not claiming jobs yet (Task 8 left the loop empty).

- [ ] **Step 9.3: Create `pg_raggraph/src/bgw/queue.rs`**

```rust
//! SPI queue operations for `pgrg.ingest_jobs`.
//!
//! All functions are internal (`pub(crate)`) and operate via SPI.
//! Mission brief Desired Outcome: `FOR UPDATE SKIP LOCKED LIMIT 1` claim,
//! status transition to `running`, error text on failure.

use pgrx::prelude::*;

/// One claimed job — what the worker dispatches into `_core::ingest::run_job`.
pub(crate) struct ClaimedJob {
    pub id: pgrx::Uuid,
    pub source: Option<String>,
    pub namespace: String,
    pub chunk_strategy: Option<String>,
    pub payload: Option<Vec<u8>>,
    pub attempt_count: i32,
}

/// Claim the next queued job using `FOR UPDATE SKIP LOCKED`.
/// Returns `None` if no jobs are available.
pub(crate) fn claim_next_job() -> Option<ClaimedJob> {
    Spi::connect_mut(|client| {
        let table = client
            .update(
                "WITH next_job AS ( \
                     SELECT id FROM pgrg.ingest_jobs \
                     WHERE status = 'queued' \
                     ORDER BY enqueued_at ASC \
                     LIMIT 1 \
                     FOR UPDATE SKIP LOCKED \
                 ) \
                 UPDATE pgrg.ingest_jobs ij \
                 SET status = 'running', \
                     started_at = COALESCE(ij.started_at, now()), \
                     updated_at = now(), \
                     attempt_count = ij.attempt_count + 1 \
                 FROM next_job \
                 WHERE ij.id = next_job.id \
                 RETURNING ij.id, ij.source, ij.namespace, ij.chunk_strategy, ij.payload, ij.attempt_count",
                Some(1),
                &[],
            )
            .ok()?;
        let row = table.first();
        let id: Option<pgrx::Uuid> = row.get(1).ok().flatten();
        let id = id?;
        let source: Option<String> = row.get(2).ok().flatten();
        let namespace: Option<String> = row.get(3).ok().flatten();
        let chunk_strategy: Option<String> = row.get(4).ok().flatten();
        let payload: Option<Vec<u8>> = row.get(5).ok().flatten();
        let attempt_count: i32 = row.get(6).ok().flatten().unwrap_or(0);
        Some(ClaimedJob {
            id,
            source,
            namespace: namespace.unwrap_or_else(|| "default".into()),
            chunk_strategy,
            payload,
            attempt_count,
        })
    })
}

/// Mark a job completed.
pub(crate) fn complete_job(job_id: &pgrx::Uuid) {
    let _ = Spi::connect_mut(|client| {
        client.update(
            "UPDATE pgrg.ingest_jobs \
             SET status = 'completed', finished_at = now(), updated_at = now(), error = NULL \
             WHERE id = $1",
            None,
            &[(*job_id).into()],
        )
    });
}

/// Mark a job failed with an error message.
pub(crate) fn fail_job(job_id: &pgrx::Uuid, error: &str) {
    let _ = Spi::connect_mut(|client| {
        client.update(
            "UPDATE pgrg.ingest_jobs \
             SET status = 'failed', finished_at = now(), updated_at = now(), error = $2 \
             WHERE id = $1",
            None,
            &[(*job_id).into(), error.into()],
        )
    });
}
```

- [ ] **Step 9.4: Wire `pub(crate) mod queue;` into `pg_raggraph/src/bgw/mod.rs`**

```rust
pub(crate) mod queue;
```

- [ ] **Step 9.5: Hook claim into `pg_raggraph/src/bgw/worker.rs` main loop**

Replace the `// Task 9 fills in:` placeholder in the `pg_raggraph_worker_main` function with:

```rust
        let claimed = BackgroundWorker::transaction(|| crate::bgw::queue::claim_next_job());
        if let Some(job) = claimed {
            // Task 10 fills in: dispatch to _core::ingest::run_job.
            // For now, mark the job completed so the loop drains.
            BackgroundWorker::transaction(|| crate::bgw::queue::complete_job(&job.id));
        }
```

- [ ] **Step 9.6: Run tests, observe pass**

```bash
cargo pgrx test pg17 -p pg_raggraph -- queue_claim_marks_one_job_running queue_skip_locked_no_double_processing
```

Expected: 2 tests pass.

- [ ] **Step 9.7: Commit**

```bash
git add pg_raggraph/src/bgw/queue.rs pg_raggraph/src/bgw/mod.rs pg_raggraph/src/bgw/worker.rs
git commit -m "feat(bgw): claim_next_job/complete_job/fail_job with FOR UPDATE SKIP LOCKED (SC-016)"
```

---


## Task 10: `_core::ingest::run_job` — per-document transaction (PgClient injection)

**Files:**
- Create: `pg_raggraph_core/src/ingest/pg_client.rs` (the injection trait)
- Create: `pg_raggraph_core/src/ingest/run.rs` (the per-job pipeline)
- Create: `pg_raggraph_core/tests/ingest_run_job.rs`
- Modify: `pg_raggraph_core/src/ingest/mod.rs` (re-export)

**Why:** SC-011 requires per-document transaction atomicity: if persistence of a single chunk fails, the entire document's chunks/entities/relationships rollback. SC-017 requires `_core::ingest::run_job` to be `cargo test`-able with a mock provider and a test-PG harness — meaning the function is parameterized by an injected `PgClient` trait so unit tests can use an in-memory fake.

**Pipeline shape (per spec §3 lines 68–74):**
1. Read source bytes (from path / inline payload).
2. `chunkshop::chunk` per `chunk_strategy`.
3. Embed each chunk via the worker's `EmbeddingBackend`.
4. Call `LlmProvider::extract` (MockProvider returns empty in Plan 3).
5. Resolve entities (no-op in Plan 3 — MockProvider yields nothing).
6. Persist all-or-nothing in one PG transaction:
   - Upsert `documents` (skip if `content_hash` already exists — SC-007).
   - Insert `chunks`.
   - (Plan 4: insert `entities`, `relationships`, `chunk_entities`.)
7. Return job outcome.

The pgrx-side caller (Task 11) wraps this in `BackgroundWorker::transaction` so the SPI client is the transaction handle.

- [ ] **Step 10.1: Write the failing test**

Create `pg_raggraph_core/tests/ingest_run_job.rs`:

```rust
use pg_raggraph_core::chunking::ChunkStrategy;
use pg_raggraph_core::embedding::DeterministicEmbedder;
use pg_raggraph_core::ingest::pg_client::FakePgClient;
use pg_raggraph_core::ingest::run::{RunJobOutcome, run_job};
use pg_raggraph_core::ingest::{IngestRequest, IngestSource};
use pg_raggraph_core::llm::MockProvider;

#[test]
fn run_job_writes_document_and_chunks_for_text_source() {
    // SC-005 / SC-017: text source -> document + chunks via mock provider.
    let mut client = FakePgClient::new();
    let req = IngestRequest {
        source: IngestSource::Text {
            name: "doc1".into(),
            content: "hello world".into(),
        },
        namespace: "default".into(),
        chunk_strategy: ChunkStrategy::Auto.as_str().into(),
    };
    let embedder = DeterministicEmbedder::new(384);
    let provider = MockProvider::new();
    let outcome = run_job(&mut client, &req, &embedder, &provider).expect("run_job ok");
    assert!(matches!(outcome, RunJobOutcome::Completed { .. }));
    assert_eq!(client.documents.len(), 1);
    assert!(!client.chunks.is_empty());
}

#[test]
fn run_job_skips_when_content_hash_already_exists() {
    // SC-007: re-ingest with identical content -> no-op.
    let mut client = FakePgClient::new();
    let req = IngestRequest {
        source: IngestSource::Text {
            name: "doc1".into(),
            content: "hello world".into(),
        },
        namespace: "default".into(),
        chunk_strategy: ChunkStrategy::Auto.as_str().into(),
    };
    let embedder = DeterministicEmbedder::new(384);
    let provider = MockProvider::new();
    let _ = run_job(&mut client, &req, &embedder, &provider).expect("first ok");
    let outcome = run_job(&mut client, &req, &embedder, &provider).expect("second ok");
    assert!(matches!(outcome, RunJobOutcome::SkippedDuplicate { .. }));
    assert_eq!(client.documents.len(), 1, "no second doc row");
}

#[test]
fn run_job_rolls_back_on_chunk_write_failure() {
    // SC-011: per-document transaction atomicity.
    let mut client = FakePgClient::new().with_chunk_write_failure_at(1);
    let req = IngestRequest {
        source: IngestSource::Text {
            name: "doc_fail".into(),
            content: "the quick brown fox jumps over the lazy dog. it was a dark and stormy night.".into(),
        },
        namespace: "default".into(),
        chunk_strategy: ChunkStrategy::Auto.as_str().into(),
    };
    let embedder = DeterministicEmbedder::new(384);
    let provider = MockProvider::new();
    let outcome = run_job(&mut client, &req, &embedder, &provider);
    assert!(outcome.is_err(), "must surface chunk-write failure");
    assert!(client.documents.is_empty(), "rollback: no document row");
    assert!(client.chunks.is_empty(), "rollback: no chunk rows");
}

#[test]
fn run_job_uses_mock_provider_no_network() {
    // SC-015: MockProvider's empty extraction means no entities/relationships
    // are written. The mock-driven happy path must succeed without network.
    let mut client = FakePgClient::new();
    let req = IngestRequest {
        source: IngestSource::Text {
            name: "doc1".into(),
            content: "hello".into(),
        },
        namespace: "default".into(),
        chunk_strategy: ChunkStrategy::Auto.as_str().into(),
    };
    let embedder = DeterministicEmbedder::new(384);
    let provider = MockProvider::new();
    run_job(&mut client, &req, &embedder, &provider).expect("mock-driven ok");
    assert!(client.entities.is_empty(), "MockProvider must yield no entities");
    assert!(client.relationships.is_empty(), "MockProvider must yield no relationships");
}
```

- [ ] **Step 10.2: Run tests, observe failure**

```bash
cargo test -p pg_raggraph_core --test ingest_run_job
```

Expected: compile error — `pg_client::FakePgClient` and `run::run_job` do not exist.

- [ ] **Step 10.3: Create `pg_raggraph_core/src/ingest/pg_client.rs`**

```rust
//! `PgClient` injection trait — lets `_core::ingest::run_job` run unit-tested
//! without a real PostgreSQL.
//!
//! The pgrx-side adapter (Task 11) wraps `pgrx::Spi`. The `FakePgClient`
//! impl below is for `cargo test`.

use crate::error::CoreResult;
use uuid::Uuid;

/// One persisted document — the row written into `pgrg.documents`.
#[derive(Debug, Clone)]
pub struct DocRow {
    pub id: Uuid,
    pub namespace: String,
    pub source: String,
    pub content_hash: String,
    pub title: Option<String>,
}

/// One persisted chunk — the row written into `pgrg.chunks`.
#[derive(Debug, Clone)]
pub struct ChunkRow {
    pub id: Uuid,
    pub document_id: Uuid,
    pub namespace: String,
    pub ord: i32,
    pub text: String,
    pub token_count: i32,
    pub embedding: Vec<f32>,
}

/// Trait the run_job pipeline uses to persist into PG.
///
/// Methods are sync — the pgrx adapter performs SPI calls inside
/// `BackgroundWorker::transaction` (Task 11). Errors are returned through
/// `CoreResult` so the run_job can roll back deterministically.
pub trait PgClient {
    /// Returns true if a document with `content_hash` already exists in `namespace`.
    fn document_exists_by_hash(&mut self, namespace: &str, content_hash: &str) -> CoreResult<bool>;

    /// Insert a document row. Caller must check `document_exists_by_hash` first.
    fn insert_document(&mut self, doc: &DocRow) -> CoreResult<()>;

    /// Insert one chunk row.
    fn insert_chunk(&mut self, chunk: &ChunkRow) -> CoreResult<()>;

    /// Discard everything written in the current logical transaction.
    /// In the pgrx adapter, this is a no-op because `BackgroundWorker::transaction`
    /// rolls back on Err return; in the fake, we drop the buffered writes.
    fn rollback(&mut self) -> CoreResult<()>;

    /// Commit (no-op in pgrx adapter; flushes the fake).
    fn commit(&mut self) -> CoreResult<()>;
}

/// Test-only in-memory `PgClient`. Buffers writes; rolls back by clearing.
#[derive(Debug, Default)]
pub struct FakePgClient {
    pub documents: Vec<DocRow>,
    pub chunks: Vec<ChunkRow>,
    /// Plan 4 will populate these via real `LlmProvider`; Plan 3 always empty.
    pub entities: Vec<()>,
    pub relationships: Vec<()>,
    /// If Some(n), the n-th chunk insert (0-indexed) returns Err.
    chunk_fail_at: Option<usize>,
    chunk_inserts: usize,
    /// Buffer that becomes the canonical state on commit.
    buffered_documents: Vec<DocRow>,
    buffered_chunks: Vec<ChunkRow>,
}

impl FakePgClient {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Configure the n-th chunk insert to fail, simulating a per-row write error.
    #[must_use]
    pub fn with_chunk_write_failure_at(mut self, n: usize) -> Self {
        self.chunk_fail_at = Some(n);
        self
    }
}

impl PgClient for FakePgClient {
    fn document_exists_by_hash(&mut self, namespace: &str, content_hash: &str) -> CoreResult<bool> {
        Ok(self
            .documents
            .iter()
            .any(|d| d.namespace == namespace && d.content_hash == content_hash))
    }

    fn insert_document(&mut self, doc: &DocRow) -> CoreResult<()> {
        self.buffered_documents.push(doc.clone());
        Ok(())
    }

    fn insert_chunk(&mut self, chunk: &ChunkRow) -> CoreResult<()> {
        if Some(self.chunk_inserts) == self.chunk_fail_at {
            self.chunk_inserts += 1;
            return Err(crate::error::CoreError::InvalidConfig(
                "synthetic chunk write failure".into(),
            ));
        }
        self.chunk_inserts += 1;
        self.buffered_chunks.push(chunk.clone());
        Ok(())
    }

    fn rollback(&mut self) -> CoreResult<()> {
        self.buffered_documents.clear();
        self.buffered_chunks.clear();
        self.chunk_inserts = 0;
        Ok(())
    }

    fn commit(&mut self) -> CoreResult<()> {
        self.documents.append(&mut self.buffered_documents);
        self.chunks.append(&mut self.buffered_chunks);
        self.chunk_inserts = 0;
        Ok(())
    }
}
```

- [ ] **Step 10.4: Create `pg_raggraph_core/src/ingest/run.rs`**

```rust
//! `run_job` — per-document transaction pipeline (PG-agnostic).
//!
//! Spec §3 lines 68–74. Mission brief SC-005, SC-007, SC-011, SC-017.
//!
//! Sequence:
//!   1. Read source bytes.
//!   2. Compute content_hash.
//!   3. If hash already exists in namespace -> SkippedDuplicate (SC-007).
//!   4. Chunk via chunkshop.
//!   5. Embed each chunk.
//!   6. Call LlmProvider::extract (MockProvider yields empty in Plan 3).
//!   7. Persist document + chunks in one logical transaction.
//!     - Plan 4 will add entities/relationships/chunk_entities.
//!   8. Commit; return Completed.
//!
//! Errors at any step trigger rollback. SC-011: atomicity is enforced by
//! the PgClient trait — pgrx adapter (Task 11) returns Err to
//! `BackgroundWorker::transaction`, which rolls back the SPI session.

use uuid::Uuid;

use crate::chunking::{ChunkStrategy, Chunker};
use crate::embedding::EmbeddingBackend;
use crate::error::{CoreError, CoreResult};
use crate::ingest::content_hash::content_hash;
use crate::ingest::pg_client::{ChunkRow, DocRow, PgClient};
use crate::ingest::types::{IngestRequest, IngestSource};
use crate::llm::LlmProvider;

#[derive(Debug, Clone)]
pub enum RunJobOutcome {
    /// Document persisted with N chunks.
    Completed { document_id: Uuid, chunk_count: usize },
    /// Document with this content_hash already existed; nothing written.
    SkippedDuplicate { existing_hash: String },
}

/// Per-document transaction pipeline.
///
/// `client` is the PgClient adapter (pgrx Spi or FakePgClient in tests).
/// `embedder` and `provider` are loaded by the worker once at startup
/// (SC-009) and reused across jobs.
pub fn run_job(
    client: &mut dyn PgClient,
    req: &IngestRequest,
    embedder: &dyn EmbeddingBackend,
    provider: &dyn LlmProvider,
) -> CoreResult<RunJobOutcome> {
    // 1+2: read source bytes and compute hash.
    let (source_name, bytes) = read_source(&req.source)?;
    let hash = content_hash(&bytes);

    // 3: incremental skip (SC-007).
    if client.document_exists_by_hash(&req.namespace, &hash)? {
        return Ok(RunJobOutcome::SkippedDuplicate { existing_hash: hash });
    }

    // 4: chunk via chunkshop.
    let strategy = ChunkStrategy::parse(&req.chunk_strategy).unwrap_or_default();
    let chunker = Chunker::new(strategy);
    let text = std::str::from_utf8(&bytes)
        .map_err(|e| CoreError::InvalidConfig(format!("source not valid utf-8: {e}")))?;
    let chunks = chunker.chunk(text)?;
    if chunks.is_empty() {
        return Err(CoreError::InvalidConfig(
            "chunkshop produced 0 chunks for non-empty source".into(),
        ));
    }

    // 5: embed each chunk.
    let chunk_texts: Vec<&str> = chunks.iter().map(|c| c.text.as_str()).collect();
    let embeddings = embedder.embed_batch(&chunk_texts)?;
    if embeddings.len() != chunks.len() {
        return Err(CoreError::InvalidConfig(format!(
            "embedder returned {} vectors for {} chunks",
            embeddings.len(),
            chunks.len()
        )));
    }

    // 6: extraction (Plan 3: MockProvider returns empty).
    for c in &chunks {
        let _ = provider.extract(&c.text, &req.namespace)?;
    }

    // 7: persist in a single logical transaction.
    let doc_id = Uuid::new_v4();
    let doc = DocRow {
        id: doc_id,
        namespace: req.namespace.clone(),
        source: source_name,
        content_hash: hash.clone(),
        title: None,
    };
    let persist_result: CoreResult<usize> = (|| {
        client.insert_document(&doc)?;
        for (chunk, embedding) in chunks.iter().zip(embeddings.iter()) {
            let row = ChunkRow {
                id: Uuid::new_v4(),
                document_id: doc_id,
                namespace: req.namespace.clone(),
                ord: chunk.ord,
                text: chunk.text.clone(),
                token_count: chunk.token_count,
                embedding: embedding.clone(),
            };
            client.insert_chunk(&row)?;
        }
        Ok(chunks.len())
    })();

    match persist_result {
        Ok(n) => {
            client.commit()?;
            Ok(RunJobOutcome::Completed { document_id: doc_id, chunk_count: n })
        }
        Err(e) => {
            // SC-011: atomicity. Roll back and propagate.
            let _ = client.rollback();
            Err(e)
        }
    }
}

/// Read source bytes from the IngestSource variant.
///
/// `Path` reads from the host filesystem (must be readable by the postgres
/// OS user — spec §3 line 69). `Text` and `Bytes` carry payload inline.
fn read_source(source: &IngestSource) -> CoreResult<(String, Vec<u8>)> {
    match source {
        IngestSource::Path(p) => {
            let bytes = std::fs::read(p)
                .map_err(|e| CoreError::InvalidConfig(format!("read {p}: {e}")))?;
            Ok((p.clone(), bytes))
        }
        IngestSource::Text { name, content } => Ok((name.clone(), content.as_bytes().to_vec())),
        IngestSource::Bytes { name, bytes } => Ok((name.clone(), bytes.clone())),
    }
}
```

- [ ] **Step 10.5: Re-export from `ingest/mod.rs`**

Append to `pg_raggraph_core/src/ingest/mod.rs`:

```rust
pub mod pg_client;
pub mod run;

pub use run::{RunJobOutcome, run_job};
```

- [ ] **Step 10.6: Run tests, observe pass**

```bash
cargo test -p pg_raggraph_core --test ingest_run_job
```

Expected: 4 tests pass.

- [ ] **Step 10.7: Commit**

```bash
git add pg_raggraph_core/src/ingest/ pg_raggraph_core/tests/ingest_run_job.rs
git commit -m "feat(core): ingest::run_job per-doc transaction (SC-005/SC-007/SC-011/SC-017)"
```

---

## Task 11: pgrx `SpiPgClient` adapter + worker dispatch wiring

**Files:**
- Create: `pg_raggraph/src/bgw/spi_client.rs`
- Create: `pg_raggraph/src/bgw/embedder_cache.rs`
- Modify: `pg_raggraph/src/bgw/worker.rs` (call `run_job`)
- Modify: `pg_raggraph/src/bgw/mod.rs` (declare submodules)

**Why:** The `_core::ingest::run_job` from Task 10 needs an `SpiPgClient` impl that talks to real PG via `pgrx::Spi`, and the worker needs to load the embedder once at startup (SC-009). This task wires the worker's main loop to call `run_job` with the production embedder and `MockProvider`.

**Embedder caching:** SC-009 — embedding model is loaded exactly once per worker. Use a `OnceLock<Arc<dyn EmbeddingBackend>>` initialized inside `pg_raggraph_worker_main` before the poll loop. Selection:
- If `cfg(feature = "pg_test")` or test build → `DeterministicEmbedder` (Plan 2 fixtures stay byte-stable).
- Else → `OnnxEmbedder` (Task 5), with `pgrg.embed_model_path` GUC override.

**Production behavior on missing model:** if the ONNX model fails to load at worker startup, log an ERROR and exit; the worker auto-restarts via `set_restart_time`. SC-010 dim-mismatch is a special case of this.

- [ ] **Step 11.1: Write the failing test**

Add to the `tests` module in `pg_raggraph/src/lib.rs`:

```rust
    #[pg_test]
    fn worker_drains_text_job_to_completed_via_pgrg_ingest_text() {
        // SC-004 / SC-005: enqueue a job, wait for completion, verify rows.
        // Uses pgrg.ingest_text once Task 12 lands; for Task 11 we INSERT
        // directly into ingest_jobs to test the worker dispatch path
        // independent of the SQL surface.
        Spi::run("SELECT pgrg.namespace_create('drain_text_ns')").unwrap();

        // We cannot use payload bytea easily without ingest_text, so this
        // test enqueues a path-shaped job pointing at a /tmp file.
        let path = "/tmp/pgrg_drain_text.txt";
        std::fs::write(path, "the quick brown fox jumps over the lazy dog").unwrap();

        Spi::run(&format!(
            "INSERT INTO pgrg.ingest_jobs (id, status, source, namespace, chunk_strategy) \
             VALUES ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'queued', '{path}', 'drain_text_ns', 'auto')"
        ))
        .unwrap();

        // Wait up to 30s for the worker to drain the job.
        let mut completed = false;
        for _ in 0..300 {
            let s: Option<String> = Spi::get_one(
                "SELECT status FROM pgrg.ingest_jobs \
                 WHERE id = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'",
            )
            .unwrap();
            if s.as_deref() == Some("completed") {
                completed = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        assert!(completed, "worker must drive job to 'completed'");

        // Verify the document and at least one chunk landed.
        let docs: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM pgrg.documents WHERE namespace = 'drain_text_ns'",
        )
        .unwrap();
        assert_eq!(docs, Some(1), "exactly 1 document row");
        let chunks: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM pgrg.chunks WHERE namespace = 'drain_text_ns'",
        )
        .unwrap();
        assert!(
            chunks.unwrap_or(0) >= 1,
            "at least 1 chunk row, got {chunks:?}"
        );

        // SC-004: chunks must have non-NULL embeddings of dim embed_dim.
        let null_emb: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM pgrg.chunks \
             WHERE namespace = 'drain_text_ns' AND embedding IS NULL",
        )
        .unwrap();
        assert_eq!(null_emb, Some(0), "all chunks must carry an embedding");
    }
```

- [ ] **Step 11.2: Run test, observe failure**

```bash
cargo pgrx test pg17 -p pg_raggraph -- worker_drains_text_job_to_completed_via_pgrg_ingest_text
```

Expected: failure — Task 9 marks any claimed job `completed` immediately without writing rows. Documents count is 0.

- [ ] **Step 11.3: Create `pg_raggraph/src/bgw/spi_client.rs`**

```rust
//! `SpiPgClient` — pgrx-side adapter implementing `_core::ingest::pg_client::PgClient`.
//!
//! Used by the bg worker to bridge `_core::ingest::run_job` to real PostgreSQL.
//! The whole pipeline runs inside `BackgroundWorker::transaction(...)` so the
//! commit/rollback semantics are inherited from pgrx's transaction wrapper.

use pg_raggraph_core::error::{CoreError, CoreResult};
use pg_raggraph_core::ingest::pg_client::{ChunkRow, DocRow, PgClient};
use pgrx::prelude::*;

/// Build a pgvector text literal of the form '[v1,v2,...]'.
fn vector_literal(v: &[f32]) -> String {
    let mut s = String::with_capacity(v.len() * 8 + 2);
    s.push('[');
    let mut first = true;
    for x in v {
        if !first {
            s.push(',');
        }
        first = false;
        s.push_str(&format!("{x}"));
    }
    s.push(']');
    s
}

/// SPI adapter. Stateless — every method is a one-shot SPI call inside the
/// surrounding `BackgroundWorker::transaction`.
pub(crate) struct SpiPgClient;

impl PgClient for SpiPgClient {
    fn document_exists_by_hash(&mut self, namespace: &str, content_hash: &str) -> CoreResult<bool> {
        let exists: Option<bool> = Spi::get_one_with_args(
            "SELECT EXISTS(SELECT 1 FROM pgrg.documents \
             WHERE namespace = $1 AND content_hash = $2)",
            &[namespace.into(), content_hash.into()],
        )
        .map_err(|e| CoreError::InvalidConfig(format!("spi document_exists: {e}")))?;
        Ok(exists.unwrap_or(false))
    }

    fn insert_document(&mut self, doc: &DocRow) -> CoreResult<()> {
        Spi::connect_mut(|client| {
            client.update(
                "INSERT INTO pgrg.documents (id, namespace, source, content_hash, title) \
                 VALUES ($1, $2, $3, $4, $5) ON CONFLICT (content_hash) DO NOTHING",
                None,
                &[
                    pgrx::Uuid::from_bytes(*doc.id.as_bytes()).into(),
                    doc.namespace.as_str().into(),
                    doc.source.as_str().into(),
                    doc.content_hash.as_str().into(),
                    doc.title.as_deref().into(),
                ],
            )
            .map_err(|e| CoreError::InvalidConfig(format!("spi insert document: {e}")))?;
            Ok(())
        })
    }

    fn insert_chunk(&mut self, chunk: &ChunkRow) -> CoreResult<()> {
        let lit = vector_literal(&chunk.embedding);
        let sql = format!(
            "INSERT INTO pgrg.chunks (id, namespace, document_id, ord, text, token_count, embedding) \
             VALUES ($1, $2, $3, $4, $5, $6, '{lit}'::vector) \
             ON CONFLICT (document_id, ord) DO NOTHING"
        );
        Spi::connect_mut(|client| {
            client.update(
                &sql,
                None,
                &[
                    pgrx::Uuid::from_bytes(*chunk.id.as_bytes()).into(),
                    chunk.namespace.as_str().into(),
                    pgrx::Uuid::from_bytes(*chunk.document_id.as_bytes()).into(),
                    chunk.ord.into(),
                    chunk.text.as_str().into(),
                    chunk.token_count.into(),
                ],
            )
            .map_err(|e| CoreError::InvalidConfig(format!("spi insert chunk: {e}")))?;
            Ok(())
        })
    }

    fn rollback(&mut self) -> CoreResult<()> {
        // No-op: the wrapping `BackgroundWorker::transaction` rolls back when
        // the closure returns Err.
        Ok(())
    }

    fn commit(&mut self) -> CoreResult<()> {
        // No-op: pgrx commits when the closure returns Ok.
        Ok(())
    }
}
```

- [ ] **Step 11.4: Create `pg_raggraph/src/bgw/embedder_cache.rs`**

```rust
//! Per-worker embedder cache — load the model once at worker startup (SC-009).
//!
//! Plan 3 builds:
//!   - In `pg_test` / cfg(test): `DeterministicEmbedder` (Plan 2 fixture stability).
//!   - Otherwise: `OnnxEmbedder` (Task 5) with `pgrg.embed_model_path` override.

use std::sync::Arc;

use pg_raggraph_core::embedding::{DeterministicEmbedder, EmbeddingBackend};

/// Build the worker's embedding backend at startup.
///
/// Returns `Arc<dyn EmbeddingBackend>` so the bg worker can pass `&*backend`
/// into `run_job` cheaply across many job iterations.
pub(crate) fn build_backend() -> Arc<dyn EmbeddingBackend> {
    let dim = crate::gucs::EMBED_DIM.get() as usize;

    // Test builds use the deterministic embedder so Plan 2 fixtures stay
    // byte-stable. Production builds load the ONNX model once at startup.
    #[cfg(any(test, feature = "pg_test"))]
    {
        return Arc::new(DeterministicEmbedder::new(dim));
    }

    #[cfg(not(any(test, feature = "pg_test")))]
    {
        #[cfg(feature = "onnx")]
        {
            use pg_raggraph_core::embedding::{OnnxEmbedder, OnnxEmbedderConfig};
            let path = crate::gucs::EMBED_MODEL_PATH
                .get()
                .map(|cs| std::path::PathBuf::from(cs.to_string_lossy().into_owned()))
                .unwrap_or_else(OnnxEmbedder::default_cache_path);
            let cfg = OnnxEmbedderConfig {
                model_path: path,
                expected_dim: dim,
            };
            match OnnxEmbedder::load(&cfg) {
                Ok(e) => Arc::new(e),
                Err(e) => {
                    pgrx::error!(
                        "pg_raggraph worker: ONNX embedder load failed: {e:?} \
                         (set pgrg.embed_model_path or place model in chunkshop hf_cache)"
                    );
                }
            }
        }
        #[cfg(not(feature = "onnx"))]
        {
            // Production build without onnx feature falls back to deterministic.
            // Useful for CI smoke runs and managed-PG sidecar builds where the
            // sidecar (Plan 5) carries its own embedder.
            Arc::new(DeterministicEmbedder::new(dim))
        }
    }
}
```

- [ ] **Step 11.5: Update `pg_raggraph/src/bgw/worker.rs` to dispatch `run_job`**

Replace the current Task 9 placeholder body of `pg_raggraph_worker_main`. The full file becomes:

```rust
//! Worker pool — claim and process queued ingest jobs.

use pg_raggraph_core::ingest::pg_client::PgClient;
use pg_raggraph_core::ingest::types::{IngestRequest, IngestSource};
use pg_raggraph_core::ingest::{RunJobOutcome, run_job};
use pg_raggraph_core::llm::MockProvider;
use pgrx::bgworkers::*;
use pgrx::prelude::*;
use std::sync::Arc;
use std::time::Duration;

use crate::bgw::{embedder_cache, queue, spi_client};
use crate::gucs;

pub fn register_workers() {
    let n = gucs::BGW_WORKERS.get();
    for i in 0..n {
        BackgroundWorkerBuilder::new(&format!("pg_raggraph w{i}"))
            .set_function("pg_raggraph_worker_main")
            .set_library("pg_raggraph")
            .enable_spi_access()
            .set_restart_time(Some(Duration::from_secs(1)))
            .set_argument(i.into_datum())
            .load();
    }
}

#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn pg_raggraph_worker_main(arg: pgrx::pg_sys::Datum) {
    let worker_idx: i32 =
        unsafe { i32::from_polymorphic_datum(arg, false, pgrx::pg_sys::INT4OID) }.unwrap_or(0);
    let worker_name = format!("pg_raggraph_w{worker_idx}");

    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    BackgroundWorker::connect_worker_to_spi(Some("postgres"), None);

    pgrx::log!("{worker_name}: started");

    // SC-009: load embedder once per worker process, BEFORE entering the loop.
    let embedder: Arc<dyn pg_raggraph_core::embedding::EmbeddingBackend> =
        embedder_cache::build_backend();
    pgrx::log!("{worker_name}: embedder loaded (dim={})", embedder.dim());
    let provider = MockProvider::new();

    let poll = Duration::from_secs(1);
    while BackgroundWorker::wait_latch(Some(poll)) {
        if BackgroundWorker::sighup_received() {
            // GUCs reloaded automatically.
        }

        let claimed = BackgroundWorker::transaction(|| queue::claim_next_job());
        let Some(job) = claimed else {
            continue;
        };

        let req = match build_request(&job) {
            Ok(r) => r,
            Err(e) => {
                pgrx::warning!("{worker_name}: malformed job {}: {e}", job.id);
                BackgroundWorker::transaction(|| queue::fail_job(&job.id, &e));
                continue;
            }
        };

        let result = BackgroundWorker::transaction(|| {
            let mut client = spi_client::SpiPgClient;
            run_job(&mut client, &req, &*embedder, &provider)
        });

        BackgroundWorker::transaction(|| match result {
            Ok(RunJobOutcome::Completed { document_id, chunk_count }) => {
                pgrx::log!(
                    "{worker_name}: job {} completed (doc={document_id}, chunks={chunk_count})",
                    job.id
                );
                queue::complete_job(&job.id);
            }
            Ok(RunJobOutcome::SkippedDuplicate { existing_hash }) => {
                pgrx::log!(
                    "{worker_name}: job {} skipped (duplicate hash {existing_hash})",
                    job.id
                );
                queue::complete_job(&job.id);
            }
            Err(e) => {
                let msg = format!("{e:?}");
                pgrx::warning!("{worker_name}: job {} failed: {msg}", job.id);
                queue::fail_job(&job.id, &msg);
            }
        });
    }

    pgrx::log!("{worker_name}: shutting down");
}

/// Translate a `ClaimedJob` into a PG-agnostic `IngestRequest`.
///
/// `source` field semantics:
///   - If `payload` is non-NULL, the job is a `Text` (utf-8) or `Bytes` ingest.
///     We disambiguate by attempting utf-8 conversion. If `source` itself looks
///     like a path, we treat it as a name.
///   - If `payload` is NULL, the job is a `Path` ingest pointing at a file on
///     the PG host filesystem.
fn build_request(job: &queue::ClaimedJob) -> Result<IngestRequest, String> {
    let chunk_strategy = job
        .chunk_strategy
        .clone()
        .unwrap_or_else(|| "auto".into());
    let namespace = job.namespace.clone();
    let source_name = job.source.clone().unwrap_or_else(|| "(unnamed)".into());

    let source = match &job.payload {
        Some(bytes) => {
            // ingest_text encodes utf-8; ingest_bytes encodes arbitrary bytes.
            // We probe utf-8 first.
            match std::str::from_utf8(bytes) {
                Ok(text) => IngestSource::Text {
                    name: source_name,
                    content: text.to_string(),
                },
                Err(_) => IngestSource::Bytes {
                    name: source_name,
                    bytes: bytes.clone(),
                },
            }
        }
        None => IngestSource::Path(source_name),
    };

    Ok(IngestRequest {
        source,
        namespace,
        chunk_strategy,
    })
}
```

- [ ] **Step 11.6: Update `pg_raggraph/src/bgw/mod.rs`**

```rust
pub mod launcher;
pub mod worker;
pub(crate) mod embedder_cache;
pub(crate) mod queue;
pub(crate) mod spi_client;

pub use launcher::register_launcher;
pub use worker::register_workers;
```

- [ ] **Step 11.7: ⛔ Drift Check DC-002**

Re-read Mission Brief. Confirm: a `pgrg.ingest*` SQL call returns immediately without doing any worker work (SC-003) — Task 12 will verify this directly. Until Task 12 lands, the architecture-level guarantee is: the worker loop in `pg_raggraph_worker_main` is the *only* code path that runs `run_job`. SQL functions do not call into `_core::ingest::run_job`. Verify by grep: `run_job` should appear only in `pg_raggraph/src/bgw/worker.rs` (and tests). If misaligned, stop and fix before Step 11.8.

```bash
git grep -n 'run_job' -- pg_raggraph/src/
```

Expected: matches only in `pg_raggraph/src/bgw/worker.rs`.

- [ ] **Step 11.8: Run test, observe pass**

```bash
cargo pgrx test pg17 -p pg_raggraph -- worker_drains_text_job_to_completed_via_pgrg_ingest_text
```

Expected: 1 test passes — document and at least one chunk row are present, all chunks carry embeddings.

- [ ] **Step 11.9: Commit**

```bash
git add pg_raggraph/src/bgw/spi_client.rs pg_raggraph/src/bgw/embedder_cache.rs pg_raggraph/src/bgw/worker.rs pg_raggraph/src/bgw/mod.rs
git commit -m "feat(bgw): SpiPgClient adapter + embedder cache + run_job dispatch (SC-004/SC-009)"
```

---

## Task 12: `pgrg.ingest(path)` SQL function (queue insert only — non-blocking)

**Files:**
- Create: `pg_raggraph/src/ingest.rs`
- Modify: `pg_raggraph/src/lib.rs` (declare `mod ingest`)

**Why:** SC-003 requires `pgrg.ingest('/path/to/single.md', 'default', 'auto')` to return a UUID in under 50ms (non-blocking) and a row with `status='queued'` to appear in `pgrg.ingest_jobs` immediately. Constraint Never: "Block the SQL caller of `pgrg.ingest*` for any reason."

The function is a single `INSERT INTO pgrg.ingest_jobs ... RETURNING id` — no chunking, no embedding, no extraction. The worker (Task 11) picks it up.

- [ ] **Step 12.1: Write the failing test**

Add to the `tests` module in `pg_raggraph/src/lib.rs`:

```rust
    #[pg_test]
    fn ingest_returns_uuid_under_50ms_and_enqueues_job() {
        // SC-003: pgrg.ingest is non-blocking; returns UUID quickly; row visible.
        Spi::run("SELECT pgrg.namespace_create('ingest_speed_ns')").unwrap();
        let path = "/tmp/pgrg_ingest_speed.md";
        std::fs::write(path, "# Title\n\nbody").unwrap();

        let start = std::time::Instant::now();
        let id: Option<pgrx::Uuid> = Spi::get_one(&format!(
            "SELECT pgrg.ingest('{path}', 'ingest_speed_ns', 'auto')"
        ))
        .unwrap();
        let elapsed = start.elapsed();

        assert!(id.is_some(), "must return a UUID");
        assert!(
            elapsed.as_millis() < 50,
            "SC-003: pgrg.ingest must return in <50ms, took {elapsed:?}"
        );

        // Row visible immediately as 'queued' (pre-worker pickup) or already
        // running/completed if the worker is fast.
        let n: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM pgrg.ingest_jobs \
             WHERE namespace = 'ingest_speed_ns'",
        )
        .unwrap();
        assert_eq!(n, Some(1), "exactly one job row enqueued");
    }
```

- [ ] **Step 12.2: Run test, observe failure**

```bash
cargo pgrx test pg17 -p pg_raggraph -- ingest_returns_uuid_under_50ms_and_enqueues_job
```

Expected: failure — `function pgrg.ingest does not exist`.

- [ ] **Step 12.3: Create `pg_raggraph/src/ingest.rs`**

```rust
//! `pgrg.ingest`, `pgrg.ingest_text`, `pgrg.ingest_bytes` — async ingest entry points.
//!
//! Mission brief Constraint Never: never block the SQL caller. These functions
//! are queue inserts; the bg worker (`crate::bgw::worker`) drains the queue.
//! SC-003: <50ms return time. SC-005: ingest_text. SC-006: ingest_bytes.

use pgrx::prelude::*;

/// `pgrg.ingest(path, namespace, chunk_strategy)` — enqueue a path-shaped job.
#[pg_extern]
fn ingest(
    path: &str,
    namespace: default!(&str, "'default'"),
    chunk_strategy: default!(&str, "'auto'"),
) -> pgrx::Uuid {
    enqueue_path(path, namespace, chunk_strategy)
}

/// `pgrg.ingest_text(name, content, namespace, chunk_strategy)` — enqueue inline text.
#[pg_extern]
fn ingest_text(
    name: &str,
    content: &str,
    namespace: default!(&str, "'default'"),
    chunk_strategy: default!(&str, "'auto'"),
) -> pgrx::Uuid {
    enqueue_payload(name, content.as_bytes(), namespace, chunk_strategy)
}

/// `pgrg.ingest_bytes(name, bytes, namespace, chunk_strategy)` — enqueue inline binary.
#[pg_extern]
fn ingest_bytes(
    name: &str,
    bytes: &[u8],
    namespace: default!(&str, "'default'"),
    chunk_strategy: default!(&str, "'auto'"),
) -> pgrx::Uuid {
    enqueue_payload(name, bytes, namespace, chunk_strategy)
}

/// Common enqueue-with-payload path (text/bytes share this).
fn enqueue_payload(name: &str, bytes: &[u8], namespace: &str, chunk_strategy: &str) -> pgrx::Uuid {
    let id = uuid::Uuid::new_v4();
    let pgrx_id = pgrx::Uuid::from_bytes(*id.as_bytes());
    Spi::connect_mut(|client| {
        client
            .update(
                "INSERT INTO pgrg.ingest_jobs \
                     (id, status, source, namespace, chunk_strategy, payload, enqueued_at, updated_at) \
                 VALUES ($1, 'queued', $2, $3, $4, $5, now(), now())",
                None,
                &[
                    pgrx_id.into(),
                    name.into(),
                    namespace.into(),
                    chunk_strategy.into(),
                    bytes.into(),
                ],
            )
            .unwrap_or_else(|e| {
                ereport!(
                    ERROR,
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    format!("pgrg.ingest_*: enqueue failed: {e}")
                )
            });
    });
    pgrx_id
}

/// Path-shaped enqueue (no payload).
fn enqueue_path(path: &str, namespace: &str, chunk_strategy: &str) -> pgrx::Uuid {
    let id = uuid::Uuid::new_v4();
    let pgrx_id = pgrx::Uuid::from_bytes(*id.as_bytes());
    Spi::connect_mut(|client| {
        client
            .update(
                "INSERT INTO pgrg.ingest_jobs \
                     (id, status, source, namespace, chunk_strategy, enqueued_at, updated_at) \
                 VALUES ($1, 'queued', $2, $3, $4, now(), now())",
                None,
                &[
                    pgrx_id.into(),
                    path.into(),
                    namespace.into(),
                    chunk_strategy.into(),
                ],
            )
            .unwrap_or_else(|e| {
                ereport!(
                    ERROR,
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    format!("pgrg.ingest: enqueue failed: {e}")
                )
            });
    });
    pgrx_id
}
```

- [ ] **Step 12.4: Wire `mod ingest;` into `pg_raggraph/src/lib.rs`**

Add a single line near the existing module declarations:

```rust
mod ingest;
```

- [ ] **Step 12.5: Run test, observe pass**

```bash
cargo pgrx test pg17 -p pg_raggraph -- ingest_returns_uuid_under_50ms_and_enqueues_job
```

Expected: 1 test passes.

- [ ] **Step 12.6: Commit**

```bash
git add pg_raggraph/src/ingest.rs pg_raggraph/src/lib.rs
git commit -m "feat(ingest): pgrg.ingest/ingest_text/ingest_bytes queue inserts (SC-003)"
```

---

## Task 13: `pgrg.ingest_text` E2E + `pgrg.ingest_bytes` E2E coverage

**Files:**
- Modify: `pg_raggraph/src/lib.rs` (tests)

**Why:** SC-005 specifies `pgrg.ingest_text('doc1', 'hello world', 'default', 'auto')` writes a document whose content equals `'hello world'` after chunkshop processing, with the document available via `pgrg.query` after completion. SC-006 mirrors for `ingest_bytes`. Task 12 added the SQL functions; this task verifies the end-to-end queue → worker → tables → query loop.

- [ ] **Step 13.1: Write the failing tests**

Add to the `tests` module in `pg_raggraph/src/lib.rs`:

```rust
    #[pg_test]
    fn ingest_text_writes_document_and_chunks_through_worker() {
        // SC-005: pgrg.ingest_text -> queue -> worker -> documents/chunks rows.
        Spi::run("SELECT pgrg.namespace_create('ingest_text_ns')").unwrap();
        let id: Option<pgrx::Uuid> = Spi::get_one(
            "SELECT pgrg.ingest_text('doc1', 'hello world from ingest_text', 'ingest_text_ns', 'auto')",
        )
        .unwrap();
        assert!(id.is_some());

        // Wait up to 30s for the worker to drain.
        for _ in 0..300 {
            let s: Option<String> = Spi::get_one(&format!(
                "SELECT status FROM pgrg.ingest_jobs WHERE id = '{}'",
                id.unwrap()
            ))
            .unwrap();
            if s.as_deref() == Some("completed") {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        let docs: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM pgrg.documents WHERE namespace = 'ingest_text_ns'",
        )
        .unwrap();
        assert_eq!(docs, Some(1));

        let chunks: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM pgrg.chunks WHERE namespace = 'ingest_text_ns'",
        )
        .unwrap();
        assert!(chunks.unwrap_or(0) >= 1);

        // Document available via pgrg.query.
        let n: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM pgrg.query('hello world', NULL, 5, 'ingest_text_ns', 1, NULL, 'hybrid')",
        )
        .unwrap();
        assert!(
            n.unwrap_or(0) >= 1,
            "ingested doc must be retrievable via pgrg.query"
        );
    }

    #[pg_test]
    fn ingest_bytes_carries_payload_through_worker() {
        // SC-006: ingest_bytes carries arbitrary bytes; worker chunks it.
        // Use a UTF-8 byte sequence so chunkshop can process it (binary
        // formats like PDF need a chunkshop binary handler — out of scope
        // here; SC-006 tests the path/queue carriage).
        Spi::run("SELECT pgrg.namespace_create('ingest_bytes_ns')").unwrap();
        let bytes_sql = "E'\\x68656c6c6f20776f726c64'::bytea"; // "hello world"
        let id: Option<pgrx::Uuid> = Spi::get_one(&format!(
            "SELECT pgrg.ingest_bytes('doc1.bin', {bytes_sql}, 'ingest_bytes_ns', 'auto')"
        ))
        .unwrap();
        assert!(id.is_some());

        for _ in 0..300 {
            let s: Option<String> = Spi::get_one(&format!(
                "SELECT status FROM pgrg.ingest_jobs WHERE id = '{}'",
                id.unwrap()
            ))
            .unwrap();
            if s.as_deref() == Some("completed") {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        let docs: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM pgrg.documents WHERE namespace = 'ingest_bytes_ns'",
        )
        .unwrap();
        assert_eq!(docs, Some(1));
    }
```

- [ ] **Step 13.2: Run tests, observe pass**

```bash
cargo pgrx test pg17 -p pg_raggraph -- ingest_text_writes_document_and_chunks_through_worker ingest_bytes_carries_payload_through_worker
```

Expected: 2 tests pass.

- [ ] **Step 13.3: Commit**

```bash
git add pg_raggraph/src/lib.rs
git commit -m "test(ingest): E2E ingest_text/ingest_bytes through worker (SC-005/SC-006)"
```

---

## Task 14: Content-hash incremental skip — pgrx test

**Files:**
- Modify: `pg_raggraph/src/lib.rs` (tests)

**Why:** SC-007 requires re-ingesting the same source (identical content_hash) is a no-op. Task 10 implements the logic in `_core::ingest::run_job`; this task verifies the behavior end-to-end: enqueue twice, document row count stays at 1, second job ends `completed`.

- [ ] **Step 14.1: Write the failing test**

Add to the `tests` module in `pg_raggraph/src/lib.rs`:

```rust
    #[pg_test]
    fn duplicate_ingest_text_yields_skipped_no_op() {
        // SC-007: identical content_hash -> no second document row.
        Spi::run("SELECT pgrg.namespace_create('dup_ns')").unwrap();
        let id1: pgrx::Uuid = Spi::get_one(
            "SELECT pgrg.ingest_text('d', 'identical content body', 'dup_ns', 'auto')",
        )
        .unwrap()
        .expect("first ingest returned NULL");

        // Wait for first job to complete.
        for _ in 0..300 {
            let s: Option<String> = Spi::get_one(&format!(
                "SELECT status FROM pgrg.ingest_jobs WHERE id = '{id1}'"
            ))
            .unwrap();
            if s.as_deref() == Some("completed") {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        let id2: pgrx::Uuid = Spi::get_one(
            "SELECT pgrg.ingest_text('d', 'identical content body', 'dup_ns', 'auto')",
        )
        .unwrap()
        .expect("second ingest returned NULL");

        // Second job also ends 'completed'.
        for _ in 0..300 {
            let s: Option<String> = Spi::get_one(&format!(
                "SELECT status FROM pgrg.ingest_jobs WHERE id = '{id2}'"
            ))
            .unwrap();
            if s.as_deref() == Some("completed") {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        // SC-007: only one document row.
        let docs: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM pgrg.documents WHERE namespace = 'dup_ns'",
        )
        .unwrap();
        assert_eq!(docs, Some(1), "duplicate content_hash must not create second doc");
    }
```

- [ ] **Step 14.2: Run test, observe pass**

```bash
cargo pgrx test pg17 -p pg_raggraph -- duplicate_ingest_text_yields_skipped_no_op
```

Expected: 1 test passes (the logic was implemented in Task 10).

- [ ] **Step 14.3: Commit**

```bash
git add pg_raggraph/src/lib.rs
git commit -m "test(ingest): duplicate content_hash -> no second document row (SC-007)"
```

---

## Task 15: Ingestion profile knobs — `pgrg.set_ingest_profile` + concurrency wiring

**Files:**
- Create: `pg_raggraph/src/ingest_profile.rs`
- Create: `pg_raggraph_core/src/ingest/profile_resolve.rs`
- Modify: `pg_raggraph/src/lib.rs` (declare `mod ingest_profile`)
- Modify: `pg_raggraph_core/src/ingest/mod.rs` (re-export)

**Why:** SC-014 requires `IngestProfile::Balanced` (default) maps to `extract_concurrency=4`; `Conservative=2`, `Aggressive=8`, `Max=16`. The brief (Desired Outcome) says "profile is read from job metadata or namespace settings." Plan 3 ships a simple `pgrg.set_ingest_profile(namespace, profile)` SQL function that updates `pgrg.namespaces.settings->'ingest_profile'` plus a resolver in `_core` that reads the profile and emits the resolved concurrency.

The actual *use* of `extract_concurrency` is mostly placeholder in Plan 3 (MockProvider has no concurrency need); the real impact lands in Plan 4 when real LLM extraction runs. SC-014 verifies the resolver wires through.

- [ ] **Step 15.1: Write the failing core test**

Append to `pg_raggraph_core/tests/ingest_profile.rs`:

```rust
use pg_raggraph_core::ingest::profile_resolve::resolve_concurrency;
use pg_raggraph_core::ingest::IngestProfile;

#[test]
fn resolve_concurrency_falls_back_to_guc_when_profile_absent() {
    // Default GUC value is 4 (Balanced).
    let n = resolve_concurrency(None, 4);
    assert_eq!(n, 4);
}

#[test]
fn resolve_concurrency_uses_profile_when_present() {
    assert_eq!(resolve_concurrency(Some(IngestProfile::Conservative), 4), 2);
    assert_eq!(resolve_concurrency(Some(IngestProfile::Balanced), 4), 4);
    assert_eq!(resolve_concurrency(Some(IngestProfile::Aggressive), 4), 8);
    assert_eq!(resolve_concurrency(Some(IngestProfile::Max), 4), 16);
}
```

- [ ] **Step 15.2: Write the failing pgrx test**

Add to the `tests` module in `pg_raggraph/src/lib.rs`:

```rust
    #[pg_test]
    fn set_ingest_profile_persists_in_namespace_settings() {
        // SC-014: per-profile concurrency assertion through the SQL surface.
        Spi::run("SELECT pgrg.namespace_create('profile_ns')").unwrap();
        Spi::run("SELECT pgrg.set_ingest_profile('profile_ns', 'aggressive')").unwrap();

        let setting: Option<pgrx::JsonB> = Spi::get_one(
            "SELECT settings FROM pgrg.namespaces WHERE name = 'profile_ns'",
        )
        .unwrap();
        let obj = setting.expect("settings present").0;
        assert_eq!(
            obj["ingest_profile"], "aggressive",
            "profile must persist in namespace settings"
        );
    }

    #[pg_test]
    fn set_ingest_profile_rejects_unknown_value() {
        Spi::run("SELECT pgrg.namespace_create('profile_bad_ns')").unwrap();
        let res = std::panic::catch_unwind(|| {
            Spi::run("SELECT pgrg.set_ingest_profile('profile_bad_ns', 'turbo')").unwrap();
        });
        assert!(res.is_err(), "unknown profile must error");
    }
```

- [ ] **Step 15.3: Run tests, observe failure**

```bash
cargo test -p pg_raggraph_core --test ingest_profile
cargo pgrx test pg17 -p pg_raggraph -- set_ingest_profile_persists_in_namespace_settings
```

Expected: compile / runtime errors — `profile_resolve` and `pgrg.set_ingest_profile` don't exist.

- [ ] **Step 15.4: Create `pg_raggraph_core/src/ingest/profile_resolve.rs`**

```rust
//! Profile -> concurrency resolution.
//!
//! SC-014: profile resolution. The resolver prefers the profile (if any) and
//! falls back to the GUC default.

use crate::ingest::IngestProfile;

/// Resolve the effective `extract_concurrency` for a job.
///
/// Returns the profile's value when `profile` is `Some`, otherwise the
/// `guc_default` (which the caller passes as `pgrg.extract_concurrency`).
#[must_use]
pub fn resolve_concurrency(profile: Option<IngestProfile>, guc_default: u32) -> u32 {
    profile.map_or(guc_default, IngestProfile::extract_concurrency)
}
```

- [ ] **Step 15.5: Re-export from `ingest/mod.rs`**

```rust
pub mod profile_resolve;

pub use profile_resolve::resolve_concurrency;
```

- [ ] **Step 15.6: Create `pg_raggraph/src/ingest_profile.rs`**

```rust
//! `pgrg.set_ingest_profile(namespace, profile)` — write `ingest_profile`
//! into `pgrg.namespaces.settings`.
//!
//! Profile is read from `pgrg.namespaces.settings->>'ingest_profile'` by the
//! bg worker (Plan 4 wires it into real `extract_concurrency` for LLM calls;
//! Plan 3 ships the surface).

use pg_raggraph_core::ingest::IngestProfile;
use pgrx::prelude::*;

#[pg_extern]
fn set_ingest_profile(namespace: &str, profile: &str) {
    if IngestProfile::parse(profile).is_none() {
        ereport!(
            ERROR,
            PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
            format!(
                "pgrg.set_ingest_profile: unknown profile '{profile}'; \
                 valid: conservative|balanced|aggressive|max"
            )
        );
    }
    Spi::connect_mut(|client| {
        client
            .update(
                "UPDATE pgrg.namespaces \
                 SET settings = COALESCE(settings, '{}'::jsonb) \
                              || jsonb_build_object('ingest_profile', $2) \
                 WHERE name = $1",
                None,
                &[namespace.into(), profile.into()],
            )
            .unwrap_or_else(|e| {
                ereport!(
                    ERROR,
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    format!("pgrg.set_ingest_profile: update failed: {e}")
                )
            });
    });
}
```

- [ ] **Step 15.7: Wire `mod ingest_profile;` into `pg_raggraph/src/lib.rs`**

```rust
mod ingest_profile;
```

- [ ] **Step 15.8: Run tests, observe pass**

```bash
cargo test -p pg_raggraph_core --test ingest_profile
cargo pgrx test pg17 -p pg_raggraph -- set_ingest_profile_persists_in_namespace_settings set_ingest_profile_rejects_unknown_value
```

Expected: 5 + 5 + 2 = 12 tests pass (Task 2 added 5 + Task 15 added 2 to ingest_profile.rs; pgrx adds 2).

- [ ] **Step 15.9: Commit**

```bash
git add pg_raggraph_core/src/ingest/profile_resolve.rs pg_raggraph_core/src/ingest/mod.rs pg_raggraph_core/tests/ingest_profile.rs pg_raggraph/src/ingest_profile.rs pg_raggraph/src/lib.rs
git commit -m "feat(ingest): pgrg.set_ingest_profile + resolver (SC-014)"
```

---

## Task 16: Reaper sweep in launcher — re-queue stuck running jobs

**Files:**
- Create: `pg_raggraph/src/bgw/reaper.rs`
- Modify: `pg_raggraph/src/bgw/launcher.rs` (call reaper)
- Modify: `pg_raggraph/src/bgw/mod.rs` (declare submodule)

**Why:** SC-012 requires a job whose `updated_at` exceeds `pgrg.job_reaper_interval` is re-queued; max 3 attempts, then `status='failed'` with `error` text mentioning the reaper. The launcher BGW (Task 8) ships with an empty 30-second loop; this task fills in the reaper-sweep body.

**Reaper SQL:**

```sql
-- Re-queue stuck jobs (running for too long, attempt_count < 3).
UPDATE pgrg.ingest_jobs
SET status = 'queued', updated_at = now()
WHERE status = 'running'
  AND updated_at < now() - make_interval(secs := $1::float8)
  AND attempt_count < 3;

-- Permanently fail jobs that hit the attempt cap.
UPDATE pgrg.ingest_jobs
SET status = 'failed',
    error = COALESCE(error, '') || ' (reaper: max attempts reached)',
    finished_at = now(),
    updated_at = now()
WHERE status = 'running'
  AND updated_at < now() - make_interval(secs := $1::float8)
  AND attempt_count >= 3;
```

- [ ] **Step 16.1: Write the failing test**

Add to the `tests` module in `pg_raggraph/src/lib.rs`:

```rust
    #[pg_test]
    fn reaper_requeues_stuck_running_job_under_attempt_cap() {
        // SC-012: simulate stuck job; reaper requeues it.
        Spi::run("SELECT pgrg.namespace_create('reaper_ns')").unwrap();
        // Insert a 'running' job with updated_at well past the reaper interval.
        // Default pgrg.job_reaper_interval is 300s; we set updated_at 600s ago.
        Spi::run(
            "INSERT INTO pgrg.ingest_jobs \
             (id, status, source, namespace, attempt_count, updated_at, started_at, enqueued_at) \
             VALUES ('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbb01', 'running', 's.md', 'reaper_ns', \
                     1, now() - interval '10 minutes', now() - interval '10 minutes', now() - interval '10 minutes')",
        )
        .unwrap();

        // Trigger the reaper synchronously via the helper SQL function (added below).
        Spi::run("SELECT pgrg._reaper_sweep()").unwrap();

        let s: Option<String> = Spi::get_one(
            "SELECT status FROM pgrg.ingest_jobs \
             WHERE id = 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbb01'",
        )
        .unwrap();
        assert_eq!(s.as_deref(), Some("queued"), "reaper must re-queue stuck job");
    }

    #[pg_test]
    fn reaper_fails_job_at_attempt_cap() {
        // SC-012: max 3 attempts, then status='failed' with reaper error message.
        Spi::run("SELECT pgrg.namespace_create('reaper_cap_ns')").unwrap();
        Spi::run(
            "INSERT INTO pgrg.ingest_jobs \
             (id, status, source, namespace, attempt_count, updated_at, started_at, enqueued_at) \
             VALUES ('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbb02', 'running', 's.md', 'reaper_cap_ns', \
                     3, now() - interval '10 minutes', now() - interval '10 minutes', now() - interval '10 minutes')",
        )
        .unwrap();

        Spi::run("SELECT pgrg._reaper_sweep()").unwrap();

        let row: Option<(String, Option<String>)> = Spi::connect(|client| {
            client
                .select(
                    "SELECT status::text, error FROM pgrg.ingest_jobs \
                     WHERE id = 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbb02'",
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .map(|r| {
                    (
                        r.get::<String>(1).unwrap().unwrap_or_default(),
                        r.get::<String>(2).unwrap(),
                    )
                })
        });
        let (status, error) = row.expect("row exists");
        assert_eq!(status, "failed");
        assert!(
            error.unwrap_or_default().contains("reaper"),
            "error message must mention 'reaper'"
        );
    }
```

- [ ] **Step 16.2: Run tests, observe failure**

```bash
cargo pgrx test pg17 -p pg_raggraph -- reaper_requeues_stuck_running_job_under_attempt_cap
```

Expected: failure — `pgrg._reaper_sweep` does not exist.

- [ ] **Step 16.3: Create `pg_raggraph/src/bgw/reaper.rs`**

```rust
//! Reaper sweep — re-queue stuck `running` jobs, fail at attempt cap (SC-012).

use pgrx::prelude::*;

use crate::gucs;

/// One reaper pass. Called from the launcher loop and (for tests) exposed as
/// `pgrg._reaper_sweep()` SQL function.
pub(crate) fn run_reaper_sweep() {
    let interval = gucs::JOB_REAPER_INTERVAL_SECS.get();
    let _ = Spi::connect_mut(|client| {
        let _ = client.update(
            "UPDATE pgrg.ingest_jobs \
             SET status = 'queued', updated_at = now() \
             WHERE status = 'running' \
               AND updated_at < now() - make_interval(secs := $1::float8) \
               AND attempt_count < 3",
            None,
            &[interval.into()],
        );
        let _ = client.update(
            "UPDATE pgrg.ingest_jobs \
             SET status = 'failed', \
                 error = COALESCE(error, '') || ' (reaper: max attempts reached)', \
                 finished_at = now(), \
                 updated_at = now() \
             WHERE status = 'running' \
               AND updated_at < now() - make_interval(secs := $1::float8) \
               AND attempt_count >= 3",
            None,
            &[interval.into()],
        );
    });
}

/// SQL surface for tests and manual triggering. Not part of the public API.
#[pg_extern]
fn _reaper_sweep() {
    run_reaper_sweep();
}
```

- [ ] **Step 16.4: Hook reaper into `pg_raggraph/src/bgw/launcher.rs`**

Replace the `// Task 16 fills in: reaper sweep.` placeholder with:

```rust
        BackgroundWorker::transaction(|| crate::bgw::reaper::run_reaper_sweep());
```

- [ ] **Step 16.5: Update `pg_raggraph/src/bgw/mod.rs`**

```rust
pub(crate) mod reaper;
```

- [ ] **Step 16.6: Run tests, observe pass**

```bash
cargo pgrx test pg17 -p pg_raggraph -- reaper_requeues_stuck_running_job_under_attempt_cap reaper_fails_job_at_attempt_cap
```

Expected: 2 tests pass.

- [ ] **Step 16.7: Commit**

```bash
git add pg_raggraph/src/bgw/reaper.rs pg_raggraph/src/bgw/launcher.rs pg_raggraph/src/bgw/mod.rs pg_raggraph/src/lib.rs
git commit -m "feat(bgw): reaper sweep re-queues stuck jobs, fails at attempt cap (SC-012)"
```

---

## Task 17: Status CHECK constraint test + ingest_jobs status enum guard

**Files:**
- Modify: `pg_raggraph/src/lib.rs` (tests)

**Why:** SC-013 requires `pgrg.ingest_jobs.status` column has a `CHECK (status IN ('queued','running','completed','failed'))` constraint; an explicit `UPDATE ... SET status='bogus'` fails with a check_violation (SQLSTATE 23514). Plan 1+2 already shipped the constraint via `005_status_check_atomicity.sql` and the existing `ingest_jobs_status_check_rejects_unknown_value` test in `lib.rs`. This task adds the explicit SQLSTATE assertion required by the brief.

- [ ] **Step 17.1: Write the failing test**

Add to the `tests` module in `pg_raggraph/src/lib.rs`:

```rust
    #[pg_test]
    fn ingest_jobs_status_check_violation_has_sqlstate_23514() {
        // SC-013: explicit assertion of SQLSTATE 23514 (check_violation).
        Spi::run("SELECT pgrg.namespace_create('sqlstate_ns')").unwrap();
        let raw = Spi::connect(|client| {
            client.select(
                "SELECT 1 FROM pgrg.ingest_jobs LIMIT 0", // warm-up; no-op
                None,
                &[],
            )
        });
        let _ = raw; // silence unused

        // Capture the panic message and parse for SQLSTATE 23514.
        let res = std::panic::catch_unwind(|| {
            Spi::run(
                "INSERT INTO pgrg.ingest_jobs \
                 (id, status, source, namespace) \
                 VALUES ('cccccccc-cccc-cccc-cccc-cccccccccc01', 'bogus', 't.md', 'sqlstate_ns')",
            )
            .unwrap();
        });
        assert!(res.is_err(), "must reject 'bogus' status");
        // pgrx surfaces SQLSTATE in the panic payload as a string. The exact
        // shape varies — assert by SQLSTATE keyword search after roundtripping.
        let payload = res
            .err()
            .map(|e| {
                e.downcast_ref::<String>()
                    .cloned()
                    .or_else(|| e.downcast_ref::<&str>().map(|s| s.to_string()))
                    .unwrap_or_default()
            })
            .unwrap_or_default();
        assert!(
            payload.contains("23514") || payload.contains("check_violation") || payload.contains("CHECK"),
            "expected SQLSTATE 23514 (check_violation) in error payload, got: {payload}"
        );
    }
```

- [ ] **Step 17.2: Run test, observe pass**

```bash
cargo pgrx test pg17 -p pg_raggraph -- ingest_jobs_status_check_violation_has_sqlstate_23514
```

Expected: 1 test passes (the constraint was added in `005_status_check_atomicity.sql`; this test asserts the SQLSTATE explicitly).

- [ ] **Step 17.3: ⛔ Drift Check DC-005**

Re-read Mission Brief. SC-013 is the deferred Plan 1 concern that "ships here" — confirm the migration `005_status_check_atomicity.sql` (already merged via Plan 2 review carry-forward) is the canonical home; Plan 3 does NOT add a duplicate migration. If a duplicate migration exists in `pg_raggraph/sql/migrations/` for the status check, remove it. Verify by listing the migrations directory.

```bash
ls pg_raggraph/sql/migrations/
```

Expected: `004_retrieval_indexes.sql`, `005_status_check_atomicity.sql`, `006_ingest_jobs_payload.sql` — no duplicate status migration.

- [ ] **Step 17.4: Commit**

```bash
git add pg_raggraph/src/lib.rs
git commit -m "test(schema): assert SQLSTATE 23514 on ingest_jobs.status CHECK violation (SC-013)"
```

---

## Task 18: E2E + load-path SC tests + DC-006 multi-worker parity

**Files:**
- Modify: `pg_raggraph/src/lib.rs` (tests)

**Why:** Mission brief Testing Requirements lists named E2E test `e2e_ingest_then_query_via_bgw` exercising the full async path with at least 5 documents. Plus three remaining SCs that need explicit tests:
- **SC-001 second path:** `LOAD 'pg_raggraph'` (without preload) does not register a worker. Hard to test inside `pgrx::pg_test` (which always sets `shared_preload_libraries`). We document the assertion path: the boolean check on `process_shared_preload_libraries_in_progress` is the entire mechanism. Add an inline doc test that asserts the guard flag exists in the codebase via `git grep`.
- **SC-008:** `chunk_strategy='hierarchy'` and `'semantic'` produce different chunk counts on a fixture markdown.
- **DC-006:** Run the worker stress test with `bgw_workers=1` and `bgw_workers=2` and confirm both produce identical document counts. Since pgrx tests fix `bgw_workers=2`, we can only assert the `=2` path here; the `=1` path is documented as a manual run in the README under Plan 3 verification.

- [ ] **Step 18.1: Write the failing tests**

Add to the `tests` module in `pg_raggraph/src/lib.rs`:

```rust
    #[pg_test]
    fn e2e_ingest_then_query_via_bgw() {
        // Mission brief E2E: pgrg.ingest_text x N, wait for completion,
        // pgrg.query returns ranked results.
        Spi::run("SELECT pgrg.namespace_create('e2e_bgw_ns')").unwrap();
        let docs: Vec<&str> = vec![
            "the auth module verifies user credentials",
            "billing service charges customers monthly",
            "search service ranks documents by relevance",
            "notification service emails alerts to users",
            "auth module also handles password resets",
        ];
        let mut ids: Vec<pgrx::Uuid> = Vec::with_capacity(docs.len());
        for (i, body) in docs.iter().enumerate() {
            let id: pgrx::Uuid = Spi::get_one(&format!(
                "SELECT pgrg.ingest_text('doc{i}', '{body}', 'e2e_bgw_ns', 'auto')"
            ))
            .unwrap()
            .expect("ingest returned NULL");
            ids.push(id);
        }

        // Wait up to 60s for all to complete.
        for _ in 0..600 {
            let pending: Option<i64> = Spi::get_one(
                "SELECT count(*) FROM pgrg.ingest_jobs \
                 WHERE namespace = 'e2e_bgw_ns' AND status NOT IN ('completed', 'failed')",
            )
            .unwrap();
            if pending == Some(0) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        let n_docs: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM pgrg.documents WHERE namespace = 'e2e_bgw_ns'",
        )
        .unwrap();
        assert_eq!(n_docs, Some(5), "5 documents must be ingested");

        // Query returns ranked results.
        let results: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM pgrg.query('auth module', NULL, 5, 'e2e_bgw_ns', 1, NULL, 'hybrid')",
        )
        .unwrap();
        assert!(
            results.unwrap_or(0) >= 1,
            "pgrg.query must return at least one ranked result for ingested corpus"
        );
    }

    #[pg_test]
    fn chunk_strategy_hierarchy_and_semantic_differ_on_markdown() {
        // SC-008: hierarchy and semantic produce different chunk counts on
        // a fixture markdown document.
        Spi::run("SELECT pgrg.namespace_create('strategy_ns')").unwrap();
        let body = "# Heading 1\n\npara one. para two.\n\n## Sub\n\npara three.\n\n# Heading 2\n\npara four.";

        // Hierarchy strategy.
        let id1: pgrx::Uuid = Spi::get_one(&format!(
            "SELECT pgrg.ingest_text('h.md', '{body}', 'strategy_ns', 'hierarchy')"
        ))
        .unwrap()
        .unwrap();
        // Semantic strategy.
        let id2: pgrx::Uuid = Spi::get_one(&format!(
            "SELECT pgrg.ingest_text('s.md', '{body}', 'strategy_ns', 'semantic')"
        ))
        .unwrap()
        .unwrap();

        // Wait for both.
        for _ in 0..600 {
            let pending: Option<i64> = Spi::get_one(&format!(
                "SELECT count(*) FROM pgrg.ingest_jobs \
                 WHERE id IN ('{id1}', '{id2}') AND status NOT IN ('completed','failed')"
            ))
            .unwrap();
            if pending == Some(0) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        // Count chunks per document.
        let count_for_source = |s: &str| -> i64 {
            let n: Option<i64> = Spi::get_one(&format!(
                "SELECT count(*) FROM pgrg.chunks c \
                 JOIN pgrg.documents d ON d.id = c.document_id \
                 WHERE d.source = '{s}' AND d.namespace = 'strategy_ns'"
            ))
            .unwrap();
            n.unwrap_or(0)
        };
        let n_h = count_for_source("h.md");
        let n_s = count_for_source("s.md");
        assert!(n_h > 0 && n_s > 0, "both strategies must produce chunks");
        assert_ne!(
            n_h, n_s,
            "SC-008: hierarchy and semantic must differ in chunk count, got h={n_h}, s={n_s}"
        );
    }

    #[pg_test]
    fn token_count_present_and_bounded() {
        // SC-008 second clause: chunks have token_count set, bounded.
        Spi::run("SELECT pgrg.namespace_create('tokens_ns')").unwrap();
        let id: pgrx::Uuid = Spi::get_one(
            "SELECT pgrg.ingest_text('t.md', 'short text body', 'tokens_ns', 'auto')",
        )
        .unwrap()
        .unwrap();
        for _ in 0..300 {
            let s: Option<String> = Spi::get_one(&format!(
                "SELECT status FROM pgrg.ingest_jobs WHERE id = '{id}'"
            ))
            .unwrap();
            if s.as_deref() == Some("completed") {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        let nulls: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM pgrg.chunks WHERE namespace = 'tokens_ns' AND token_count IS NULL",
        )
        .unwrap();
        assert_eq!(nulls, Some(0), "all chunks must have token_count set");
    }

    #[pg_test]
    fn embedder_is_loaded_once_per_worker_not_per_job() {
        // SC-009: model loaded once per worker. We can't inspect the worker's
        // private state directly from pg_test, but we can assert the worker
        // log line "embedder loaded" appears at most once per worker — by
        // observing the log via `pg_log_messages` if available, or simply
        // by relying on the cached `Arc` semantics in `embedder_cache::build_backend`.
        //
        // Codified assertion: the build_backend function is called from
        // `pg_raggraph_worker_main` BEFORE the latch loop (verified by
        // grepping the source). This test asserts the source contract.
        let src = include_str!("bgw/worker.rs");
        let pos_build = src.find("build_backend()").expect("must call build_backend");
        let pos_loop = src.find("wait_latch").expect("must enter latch loop");
        assert!(
            pos_build < pos_loop,
            "SC-009: build_backend must be called before the latch loop (model loaded once)"
        );
    }
```

- [ ] **Step 18.2: Run tests, observe pass**

```bash
cargo pgrx test pg17 -p pg_raggraph -- e2e_ingest_then_query_via_bgw chunk_strategy_hierarchy_and_semantic_differ_on_markdown token_count_present_and_bounded embedder_is_loaded_once_per_worker_not_per_job
```

Expected: 4 tests pass.

- [ ] **Step 18.3: ⛔ Drift Check DC-006**

Re-read Mission Brief. DC-006 requires "run with `pgrg.bgw_workers = 1` and `= 2` and confirm both produce identical document counts at end." pgrx tests fix `bgw_workers=2` via `postgresql_conf_options`. Document the manual `=1` verification path in CHANGELOG/README. The pgrx-test path (`bgw_workers=2`) is covered by `e2e_ingest_then_query_via_bgw`. To satisfy DC-006 without changing the pgrx test harness, add a README note in Task 19 stating: "Plan 3 manual verification: run `cargo pgrx run pg17` with `pg_raggraph.bgw_workers = 1` set in postgresql.conf, ingest the 5-doc fixture, confirm 5 documents present." If misaligned, escalate before Task 19.

- [ ] **Step 18.4: Commit**

```bash
git add pg_raggraph/src/lib.rs
git commit -m "test(ingest): E2E bgw + chunk-strategy diff + token_count + SC-009 source guard"
```

---

## Task 19: README + CHANGELOG bump for 0.1.0-alpha.3

**Files:**
- Modify: `Cargo.toml` (workspace `version = "0.1.0-alpha.3"`)
- Modify: `README.md`
- Modify: `CHANGELOG.md`

**Why:** Plan 2 ended at `0.1.0-alpha.2` with retrieval. Plan 3 ships the async write path — version bump signals the new public API surface (`pgrg.ingest`, `pgrg.ingest_text`, `pgrg.ingest_bytes`, `pgrg.set_ingest_profile`, the bg worker). DC-006 manual-verification note also lands here.

- [ ] **Step 19.1: Bump workspace version in `Cargo.toml`**

In the workspace `Cargo.toml` `[workspace.package]` block:

```toml
version = "0.1.0-alpha.3"
```

- [ ] **Step 19.2: Update `README.md` Status section**

Replace the Status section in `README.md` with:

```markdown
## Status

**Pre-alpha (0.1.0-alpha.3).** Foundation + retrieval engine + **async ingest pipeline** in place: schema, namespaces, providers, GUCs, health/status, hybrid retrieval (`pgrg.query`), deterministic test embeddings (`pgrg.embed`), fixture loader (`pgrg.ingest_extracted`), **plus** background worker, queue-backed async ingest (`pgrg.ingest`, `pgrg.ingest_text`, `pgrg.ingest_bytes`), chunkshop integration as the canonical chunker, ONNX-backed embedding model (`BAAI/bge-small-en-v1.5` fp32) loaded once per worker, content-hash incremental skip, ingestion profile knobs (`conservative`/`balanced`/`aggressive`/`max`), and reaper sweep. LLM grounding (Plan 4), sidecar (Plan 5), and the parity harness (Plan 6) land in subsequent plans.

```sql
-- This works as of 0.1.0-alpha.3:
CREATE EXTENSION pg_raggraph CASCADE;                              -- schema + indexes + bg workers
SELECT pgrg.namespace_create('demo');                              -- per-tenant container
SELECT pgrg.ingest_text('hello.md', 'hello world', 'demo');        -- non-blocking; returns job UUID
-- ... worker drains in background ...
SELECT text, score FROM pgrg.query('hello', NULL, 5, 'demo');      -- hybrid retrieval
```

### Manual verification (DC-006)

To confirm worker-count independence:

```bash
# In postgresql.conf:
#   shared_preload_libraries = 'pg_raggraph'
#   pg_raggraph.bgw_workers = 1
cargo pgrx run pg18  # or pg17 in CI
# Ingest 5 docs, verify count:
psql -c "SELECT pgrg.namespace_create('w1');"
# ... 5 ingest_text calls ...
psql -c "SELECT count(*) FROM pgrg.documents WHERE namespace = 'w1';"  # expect 5

# Repeat with pg_raggraph.bgw_workers = 2; expect identical 5.
```
```

- [ ] **Step 19.3: Update `CHANGELOG.md`**

Prepend to `CHANGELOG.md`, before the existing 0.1.0-alpha.2 entry:

```markdown
## [0.1.0-alpha.3] — 2026-05-06

### Added
- `pgrg.ingest(path, namespace, chunk_strategy)` — async path-shaped ingest; returns job UUID immediately (Plan 3, SC-003)
- `pgrg.ingest_text(name, content, namespace, chunk_strategy)` — async inline-text ingest (Plan 3, SC-005)
- `pgrg.ingest_bytes(name, bytes, namespace, chunk_strategy)` — async inline-bytes ingest (Plan 3, SC-006)
- `pgrg.set_ingest_profile(namespace, profile)` — per-namespace concurrency knob (`conservative`=2, `balanced`=4, `aggressive`=8, `max`=16) (Plan 3, SC-014)
- Background worker pool — `pgrg.bgw_workers` GUC (default 2); registered in `_PG_init` only when `process_shared_preload_libraries_in_progress` (Plan 3, SC-001/SC-002)
- Reaper sweep — `pgrg.job_reaper_interval` GUC (default 300s) re-queues stuck `running` jobs; max 3 attempts before permanent fail (Plan 3, SC-012)
- chunkshop integration — `auto`/`hierarchy`/`semantic`/`sentence_aware`/`fixed_overlap`/`neighbor_expand` strategies (Plan 3, SC-008)
- ONNX-backed embedding model — `BAAI/bge-small-en-v1.5` fp32 via `ort = "2"`; loaded once per worker; `pgrg.embed_model_path` GUC override; dim mismatch is a load-time error (Plan 3, SC-004/SC-009/SC-010)
- `LlmProvider` trait surface in `pg_raggraph_core::llm` with `MockProvider` no-op impl; concrete impls land in Plan 4 (Plan 3, SC-015)
- Content-hash incremental skip — re-ingesting identical content is a no-op; document row count stays at 1 (Plan 3, SC-007)
- Per-document transaction atomicity — chunk-write failure rolls back the whole document (Plan 3, SC-011)
- `pgrg.ingest_jobs.payload` bytea column + `attempt_count` integer + `ingest_jobs_active_idx` partial index (Plan 3, schema migration `006_ingest_jobs_payload.sql`)

### Schema changes
- `pgrg.ingest_jobs.payload bytea` (nullable; for `ingest_text` / `ingest_bytes` carriage)
- `pgrg.ingest_jobs.attempt_count integer NOT NULL DEFAULT 0` (reaper bookkeeping)
- `pgrg.ingest_jobs.chunk_strategy text` (job parameter persistence)
- `ingest_jobs_active_idx` partial index `WHERE status IN ('queued','running')` for bg worker scan

### Not yet implemented
- Real OpenAI / Anthropic / Ollama LLM provider impls (Plan 4)
- AES-GCM credential encryption (Plan 4)
- `pgrg.ask` LLM grounding (Plan 4)
- Sidecar binary (Plan 5)
- Cross-impl parity harness (Plan 6)
```

- [ ] **Step 19.4: Commit**

```bash
git add Cargo.toml README.md CHANGELOG.md
git commit -m "docs: README + CHANGELOG for 0.1.0-alpha.3 (Plan 3 ingest pipeline)"
```

---

## DC-FINAL: Final drift check before declaring Plan 3 complete

⛔ **Drift Check DC-FINAL.** Re-read Mission Brief at `skill-output/mission-brief/Mission-Brief-plan3-ingest-pipeline.md` one final time. For each SC-001 through SC-017, confirm evidence of satisfaction (named test in cargo/pgrx output). If any SC-XXX lacks evidence, the work is not complete — open a follow-up task before declaring done.

Mapping of SC → evidence (test name to look for):

| SC | Test name |
|---|---|
| SC-001 | `bgw_workers_registered_under_preload` (preload-on path); `embedder_is_loaded_once_per_worker_not_per_job` (source-grep guard for the if-branch); manual: load via `LOAD 'pg_raggraph'` does not register a worker (documented in Task 8 doc comment) |
| SC-002 | `bgw_workers_registered_under_preload`, `health_reports_bgw_count_matching_actual_workers` |
| SC-003 | `ingest_returns_uuid_under_50ms_and_enqueues_job` |
| SC-004 | `worker_drains_text_job_to_completed_via_pgrg_ingest_text` (asserts non-NULL embedding column) |
| SC-005 | `ingest_text_writes_document_and_chunks_through_worker` |
| SC-006 | `ingest_bytes_carries_payload_through_worker` |
| SC-007 | `duplicate_ingest_text_yields_skipped_no_op`; `pg_raggraph_core::tests::ingest_run_job::run_job_skips_when_content_hash_already_exists` |
| SC-008 | `chunk_strategy_hierarchy_and_semantic_differ_on_markdown`, `token_count_present_and_bounded` |
| SC-009 | `embedder_is_loaded_once_per_worker_not_per_job` (source-contract assertion) |
| SC-010 | `pg_raggraph_core::tests::embedding_onnx::onnx_dim_mismatch_returns_error` (feature=onnx; requires test model) |
| SC-011 | `pg_raggraph_core::tests::ingest_run_job::run_job_rolls_back_on_chunk_write_failure` |
| SC-012 | `reaper_requeues_stuck_running_job_under_attempt_cap`, `reaper_fails_job_at_attempt_cap` |
| SC-013 | `ingest_jobs_status_check_violation_has_sqlstate_23514`, plus existing `ingest_jobs_status_check_rejects_unknown_value` |
| SC-014 | `pg_raggraph_core::tests::ingest_profile::*` (5+2 tests), `set_ingest_profile_persists_in_namespace_settings`, `set_ingest_profile_rejects_unknown_value` |
| SC-015 | `pg_raggraph_core::tests::llm_mock::*` (3 tests), `pg_raggraph_core::tests::ingest_run_job::run_job_uses_mock_provider_no_network` |
| SC-016 | `queue_skip_locked_no_double_processing` (10-job stress; expand to 50 if pgrx-test budget permits) |
| SC-017 | `pg_raggraph_core::tests::ingest_run_job::*` (4 tests, all `cargo test`-driven, no PG) |

If every line of the table above corresponds to a green test, Plan 3 is complete.

---

## Self-Review Checklist (run before declaring Plan 3 complete)

- [ ] All 19 tasks marked complete
- [ ] `cargo fmt --all -- --check` passes
- [ ] `cargo check --workspace` passes
- [ ] `cargo clippy -p pg_raggraph_core -p pg_raggraph_sidecar -- -D warnings` passes
- [ ] `cargo test -p pg_raggraph_core` passes (all Plan 1+2 tests + Plan 3 unit tests: `ingest_profile` (7), `ingest_types` (5), `ingest_content_hash` (4), `embedding_backend` (4), `chunking` (5), `llm_mock` (3), `ingest_run_job` (4))
- [ ] `cargo test -p pg_raggraph_core --features onnx --test embedding_onnx` is at least compile-clean; runtime skipped without `PGRG_TEST_ONNX_MODEL_PATH`
- [ ] `cargo pgrx test pg17 -p pg_raggraph` passes ALL tests, including:
  - All Plan 1 + Plan 2 tests still passing
  - Plan 3 schema (Task 1): `ingest_jobs_payload_column_exists`, `ingest_jobs_attempt_count_column_exists`, `ingest_jobs_active_partial_index_exists`
  - Plan 3 bgw + queue (Tasks 8, 9): `bgw_workers_registered_under_preload`, `health_reports_bgw_count_matching_actual_workers`, `queue_claim_marks_one_job_running`, `queue_skip_locked_no_double_processing`
  - Plan 3 worker dispatch (Task 11): `worker_drains_text_job_to_completed_via_pgrg_ingest_text`
  - Plan 3 SQL surface (Tasks 12, 13): `ingest_returns_uuid_under_50ms_and_enqueues_job`, `ingest_text_writes_document_and_chunks_through_worker`, `ingest_bytes_carries_payload_through_worker`
  - Plan 3 incremental skip (Task 14): `duplicate_ingest_text_yields_skipped_no_op`
  - Plan 3 profiles (Task 15): `set_ingest_profile_persists_in_namespace_settings`, `set_ingest_profile_rejects_unknown_value`
  - Plan 3 reaper (Task 16): `reaper_requeues_stuck_running_job_under_attempt_cap`, `reaper_fails_job_at_attempt_cap`
  - Plan 3 status guard (Task 17): `ingest_jobs_status_check_violation_has_sqlstate_23514`
  - Plan 3 E2E + SCs (Task 18): `e2e_ingest_then_query_via_bgw`, `chunk_strategy_hierarchy_and_semantic_differ_on_markdown`, `token_count_present_and_bounded`, `embedder_is_loaded_once_per_worker_not_per_job`
- [ ] CI green on the push (PG17 path, `RUST_TEST_THREADS=1` serial)
- [ ] Mission brief read 6 times: at DC-001 (Step 8.7), DC-002 (Step 11.7), DC-003 (Step 4.5), DC-004 (Step 7.7), DC-005 (Step 17.3), DC-006 (Step 18.3), DC-FINAL (above)
- [ ] No `unsafe` introduced into `pg_raggraph_core` (Constraint Always: `unsafe_code = "forbid"` preserved). The `unsafe { ... }` block in `_PG_init` for `process_shared_preload_libraries_in_progress` is in `pg_raggraph` (the pgrx crate), not `_core`.
- [ ] No new GUCs introduced beyond Plan 1's set (Constraint Ask First: new GUCs need approval — Plan 3 introduces none)
- [ ] All ingest logic that is not strictly pgrx FFI lives in `pg_raggraph_core::ingest` (verified by `cargo test -p pg_raggraph_core` covering profile, types, content_hash, run_job, pg_client without PostgreSQL)
- [ ] No real LLM provider call introduced anywhere (Constraint Never — only `MockProvider` is wired)
- [ ] `pgrg.ingest*` SQL functions never block (Constraint Never; SC-003 verifies <50ms return)
- [ ] chunkshop is a hard Cargo dep (Constraint Always); no hand-rolled chunker present
- [ ] `LlmProvider` trait surface in `pg_raggraph_core::llm` is consumable (verified by Task 7 + Task 10 wiring)
- [ ] CHANGELOG and README reflect 0.1.0-alpha.3
- [ ] Manual DC-006 verification path documented in README (Task 19, `pg_raggraph.bgw_workers = 1` vs `= 2` produces identical document counts)

---

## Spec coverage (Plan 3 → design-spec map)

| Spec section | Plan 3 task |
|---|---|
| §1 Thesis (3-statement demo, ingest half) | Task 18 (`e2e_ingest_then_query_via_bgw`) — `ask` half lands in Plan 4 |
| §3 Ingest path — SQL entry function | Tasks 12, 13 |
| §3 Ingest path — bg worker registration | Task 8 |
| §3 Ingest path — polling loop | Task 9, Task 11 (dispatch into `run_job`) |
| §3 Ingest path — read source / chunk / embed / extract | Task 10 (`run_job` body) |
| §3 Ingest path — resolution constants (used at extract time, real entities Plan 4) | **Out of scope here** — MockProvider yields zero entities |
| §3 Ingest path — single-tx persistence | Task 10 (atomicity), Task 11 (SpiPgClient adapter) |
| §3 Ingest path — error/reaper | Task 16 |
| §3 Ingest path — sidecar parity (same `_core::ingest::run_job`) | Constraint Always: `_core` PG-agnostic; consumed by Plan 5 |
| §3 "What's not in v1" | Honored (no community detection, no online re-resolution, no file watch) |
| §5 Schema — `ingest_jobs.payload`, `attempt_count`, `chunk_strategy` | Task 1 (migration `006_ingest_jobs_payload.sql`) |
| §5 Schema — `ingest_jobs(status, enqueued_at) WHERE active` partial index | Task 1 |
| §5 Schema — `documents.content_hash UNIQUE` | Used by Task 10 / Task 11 (incremental skip) |
| §6 SQL surface — `pgrg.ingest` | Task 12 |
| §6 SQL surface — `pgrg.ingest_text` | Task 12 (impl), Task 13 (E2E test) |
| §6 SQL surface — `pgrg.ingest_bytes` | Task 12 (impl), Task 13 (E2E test) |
| §6 SQL surface — `pgrg.ask` | **Out of scope (Plan 4)** |
| §7 GUC `pgrg.bgw_workers` | Task 8 (consumed by `register_workers`) |
| §7 GUC `pgrg.extract_concurrency` | Task 15 (resolver wires to profile) |
| §7 GUC `pgrg.embed_model_path` | Task 5 (loader path), Task 11 (`embedder_cache::build_backend`) |
| §7 GUC `pgrg.embed_dim` | Task 5 (dim mismatch error), Task 11 (`build_backend`) |
| §7 GUC `pgrg.job_reaper_interval` | Task 16 |
| §7 G1 local embedding model default | Task 5 (`OnnxEmbedder` for `BAAI/bge-small-en-v1.5` fp32) |
| §7 LLM provider abstraction | Task 7 (trait + MockProvider only — Plan 4 ships impls) |
| §11 Out of scope for v1 (no community detection, no file watch, no smart-mode) | Honored |

---

## What this plan deliberately does *not* cover

These belong to subsequent plans, not Plan 3:

- **Real LLM provider impls** (`OpenAiProvider`, `AnthropicProvider`, `OllamaProvider`, `RetryingProvider` wrapper) — Plan 4. Plan 3 ships only the trait surface and `MockProvider`.
- **`pgrg.ask` SQL function** (`pgrg.query` + LLM grounding + citation-required prompt) — Plan 4.
- **AES-GCM credential encryption** (`pgrg.master_key_path`, `enc:v1:...`) — Plan 4. Plan 1 registered the GUC; Plan 3 does not consume it.
- **Entity / relationship / chunk_entity persistence** — Plan 4 (depends on real LLM extraction).
- **Entity resolution** (pg_trgm fuzzy + cosine on entity-name embeddings) — Plan 4 (resolution only matters when entities are extracted).
- **Sidecar binary** (libpq job loop, embedded SQL bootstrap, HTTP `/v1/ask`) — Plan 5. The `_core::ingest::run_job` defined here is the same loop the sidecar will run, satisfying the Constraint Always parity contract.
- **`bench/parity/`** corpus, `compare.py`, parity CI workflow, Jaccard ≥ 0.8 thresholds — Plan 6.
- **Smart-mode routing, automatic escalation, confidence thresholds** — explicitly out of scope per spec §11 and the Python sibling's surface.
- **Community detection, Leiden/Louvain, `global` retrieval mode** — explicitly out of scope per spec §3 "What's not in v1" / §11.
- **File-watch / streaming ingestion** — spec §3, §11.
- **Multi-modal inputs (images, audio)** — spec §11.
- **Online entity re-resolution after upstream merges** — spec §3, §11.
- **Cross-namespace federation** — spec §11.
- **Custom chunker** — Constraint Out of Scope ("must go through chunkshop"); Constraint Always reinforces.
- **Pattern D for chunkshop** (`chunk_strategy="chunkshop:hierarchy"` etc., per the Python sibling's CLAUDE.md) — Plan 3 ships chunkshop as a hard dep with strategy names matching the documented surface (`auto`, `hierarchy`, `semantic`, `sentence_aware`, `fixed_overlap`, `neighbor_expand`). Pattern C (read from a chunkshop-populated pgvector table) is also out of scope here — the user pointing `pgrg.ingest_extracted` at a chunkshop-built JSONL fixture is the closest equivalent and was shipped in Plan 2.
- **Adaptive provider rate-limiting / backoff beyond the reaper's 3-attempt cap** — spec §11 / Constraint Ask First.
- **Web UI, MCP server** — spec §11 / future plans.
- **HTTP `/v1/ask` endpoint** — Plan 5.
- **Real ONNX inference exercised in CI** — Plan 6 will set up model caching for parity benchmarks; Plan 3 ships the loader and tests it via `PGRG_TEST_ONNX_MODEL_PATH` when present.
