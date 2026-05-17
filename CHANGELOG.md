# Changelog

## [0.1.0-rc.1] — 2026-05-17

Plan 6: Cross-implementation parity harness — closing gate before v1.

### Added
- `bench/parity/` harness: frozen-graph corpus (small tier committed; medium/large generated), extracted JSONL fixtures, per-tier query sets (5×4 categories), `compare.py` driver (machine verdict via exit code), `metrics.md`, `precheck.py` (ONNX artifact-identity guard), `undirected_fixture.jsonl`, `ci/parity.yml` + `.github/workflows/parity.yml` (SC-001..017)
- `bench/parity/resolution_constants.yaml` — shared TRGM/cosine constants; Rust sources at build time via dependency-free `pg_raggraph_core/build.rs`; drift vs. `resolve.rs` = build error (SC-005)
- `pg_raggraph_core/tests/resolution_parity.rs` — 50-variant cross-impl canonical-id test; 0 drift (DC-003)
- `#[pg_test] sc012_parity_mode_guc_is_suset` — regression locks `pg_raggraph.parity_mode` as `Suset` (folded deferred Plan 1 concern; DC-005 re-verifies Plan 2 SC-009 green)
- `#[pg_test] sc011_graph_traversal_is_undirected` — proves UNION ALL both directions in 1-hop traversal (SC-011)
- `#[pg_test] sc002_parity_small_fixture_loads_via_real_loader` — fixture schema validated against real Rust loader

### Pending (GATE-A)
- `bench/parity/requirements.txt` (SC-015) — requires user to supply pinned `pg-raggraph` version from sibling repo `yonk-labs/pg-raggraph` before executing T9

### Changed
- `pg_raggraph_core/src/ingest/resolve.rs`: `TRGM_MERGE`/`COSINE_MERGE` now sourced from shared YAML (behavior unchanged; build-time drift guard added)

## [0.1.0-alpha.5] — 2026-05-16

Plan 5: Sidecar binary for managed PostgreSQL.

### Added
- Standalone `pg_raggraph_sidecar` binary — runs GraphRAG against managed PostgreSQL (RDS, Cloud SQL, Supabase, Neon) with no `shared_preload_libraries` access (Plan 5, SC-001)
- First-connect idempotent SQL bootstrap — creates all `pgrg.*` tables; embedded migrations are byte-identical to the in-extension migration files (Plan 5, SC-002/SC-003, DC-003)
- libpq job loop — polls `pgrg.ingest_jobs` with `FOR UPDATE SKIP LOCKED`, processes each job exactly once across multiple sidecar instances (Plan 5, SC-004)
- `POST /v1/ask` (axum) — JSON `{q, namespace, top_k}` → `{answer, citations, signals, mode_used}`; sanitized `400`/`404`/`500` error envelopes (no stack traces, no credentials) (Plan 5, SC-005/SC-013)
- `pg_raggraph_sidecar/sql/client.sql` — PL/pgSQL `pgrg.ask` shim that POSTs to the sidecar via `pg_net`, signature-parity with the in-extension `pgrg.ask` (Plan 5, SC-006 — see Known limitations)
- `TokioPgClient` — libpq mirror of `SpiPgClient` running verbatim SQL, zero pgrx (Plan 5, SC-008/SC-016, DC-005)
- Crashed-sidecar reaper — re-queues jobs whose `updated_at` exceeds the reaper interval, max 3 attempts then `failed`; same semantics as the Plan 3 bg-worker reaper (Plan 5, SC-009)
- Bounded empty-queue backoff — 1s → grow → cap → reset; does not hammer the DB when idle (Plan 5, SC-010)
- AES-GCM credential interop — sidecar reads/writes `pgrg.providers` with the shared `_core` encryption module; in-extension path reads the same rows (Plan 5, SC-011)
- `provider_factory` — provider resolution parity with the pgrx factory (Plan 5, SC-016)
- Managed-PG E2E — full ingest → grounded ask against a stock `pgvector/pgvector:pg17` container with no preload config (Plan 5, SC-012 — the SC-008 thesis proof)
- `docker/Dockerfile.sidecar` and `.github/workflows/sidecar.yml` — CI runs `cargo test -p pg_raggraph_sidecar` against a Docker PG on every push/PR (Plan 5, SC-015)

### Known limitations / carry-forward
- SC-006 ⚠️ partial: `client.sql` is code-complete, signature-parity-exact, installs on `supabase/postgres` pg_net 0.14.0, uses the non-deprecated `net._await_response` path — but the pg_net background-worker outbound HTTP round-trip is unvalidated in this sandbox (egress blocked). Validate on a real non-sandboxed managed-PG/Supabase instance.
- SC-007 ⚠️ partial: `ingest_text`/`ingest_bytes`/`query`/`ask` have proven sidecar counterparts, but `pgrg.status`/`pgrg.health` are not surfaced in the v1 sidecar (no `GET /v1/status|/v1/health` route).
- `docker/Dockerfile.sidecar` has no `.dockerignore` (`COPY . .` ships `target/` — works but slow/large build context).
- Sidecar build needs `pkg-config libssl-dev g++` (builder) + `libssl3 libstdc++6 ca-certificates` (runtime) — `_core` pulls chunkshop/ort/onnxruntime/openssl.
- Bg-worker-queue dispatch is not exercisable under pgrx test MVCC; the in-process `process_one` path is the tested one; queue/launcher is covered by Plan 3.

### Constraint notes
- Sync `PgClient` bridged to async `tokio-postgres` (`Handle::block_on` inside `spawn_blocking`).
- Transaction boundary owned by the job loop — `TokioPgClient` commit/rollback are no-ops (DC-006).
- Load-bearing `pgrg.embed($1)` → inline-vector substitution (managed PG lacks the pgrx fn) is regression-locked by the `parity_matrix` SQL-drift guard.
- No HTTP auth and no in-sidecar TLS in v1 — private-network deployment assumption; reverse proxy required for TLS/auth.

## [0.1.0-alpha.4] — 2026-05-15

Plan 4: LLM extraction + `pgrg.ask`.

### Added
- AES-256-GCM credential encryption (`enc:v1:<nonce>:<ciphertext>`) when `pg_raggraph.master_key_path` is set; transparent decrypt at use site (Plan 4, SC-003)
- Master key file 0600 permission check — rejects group/world-readable key files (Plan 4, SC-006)
- Startup WARNING when `master_key_path` is unset (plaintext-fallback honesty) (Plan 4, SC-005)
- Real `OpenAiProvider`, `AnthropicProvider`, `OllamaProvider` — HTTP-cassette tested, no live network in CI (Plan 4, SC-001)
- `RetryingProvider` — 3 retries, 1s/2s/4s backoff, 10s wall-clock cap; retries 429/5xx, fails fast on 4xx (Plan 4, SC-002)
- `pgrg.ask(q, filter, top_k, namespace, hops, llm_provider)` — grounded answer with `[N]` citations resolved to chunk_ids (Plan 4, SC-009)
- Numbered-citation prompt builder — LLM never sees raw chunk_ids; citation forgery impossible by construction (Plan 4, SC-010)
- Token budget per namespace (`namespaces.settings.ask_token_budget`, default 4000) (Plan 4, SC-012)
- Provider resolution chain: explicit → namespace default → first LLM provider → error (Plan 4, SC-011)
- Entity resolution at ingestion (pg_trgm + cosine on name embeddings) wired into `run_job` (Plan 4, SC-014 — see Known limitations)
- Real LLM extraction in the bg worker (replaces Plan 3 MockProvider; resolved per-job) (Plan 4, SC-013)
- `signals.llm` cost/latency attribution on `pgrg.ask` (Plan 4, SC-018)

### Fixed (folded Plan 1 deferred concerns)
- `redact()` UTF-8 panic-safety — regression-locked (Plan 1 deferred, SC-008)
- `pg_raggraph.master_key_path` GUC context confirmed `Suset` — regression-locked (Plan 1 deferred, SC-007)

### Known limitations / carry-forward
- SC-014 entity-resolution is validated for the decision logic (unit) and the real-pg_trgm + cross-document merge pipeline (E2E), but the cosine-with-real-semantic-embeddings leg is deferred: the deterministic test embedder (SHA-256, non-semantic) cannot exercise it. Full punctuation-variant validation requires the Plan 3 ONNX-embedder carry-forward.
- Bg-worker queue dispatch is not exercised by pgrx tests (transaction/MVCC isolation); the in-process pipeline is tested via direct `run_job` dispatch, and the queue/launcher is covered by Plan 3 tests.
- `mock` / `mock-extractor` provider kinds are reachable from production SQL (same risk profile as before); production hardening could feature-gate them.

### Constraint notes
- `LlmProvider` trait extended with `complete()` (default errors; real providers + MockProvider override) — pre-approved trait-shape change.

## [0.1.0-alpha.3] — 2026-05-11

### Added
- `pgrg.ingest(path, namespace, chunk_strategy)` — async path-shaped ingest; returns job UUID immediately (Plan 3, SC-003)
- `pgrg.ingest_text(name, content, namespace, chunk_strategy)` — async inline-text ingest (Plan 3, SC-005)
- `pgrg.ingest_bytes(name, bytes, namespace, chunk_strategy)` — async inline-bytes ingest (Plan 3, SC-006)
- `pgrg.set_ingest_profile(namespace, profile)` — per-namespace concurrency knob (`conservative`=2, `balanced`=4, `aggressive`=8, `max`=16) (Plan 3, SC-014)
- Background worker pool — `pgrg.bgw_workers` GUC (default 2); registered in `_PG_init` only when `process_shared_preload_libraries_in_progress` (Plan 3, SC-001/SC-002)
- Reaper sweep — `pgrg.job_reaper_interval` GUC (default 300s) re-queues stuck `running` jobs; max 3 attempts before permanent fail (Plan 3, SC-012)
- chunkshop integration — `auto` (= `sentence_aware`), `hierarchy`, `sentence_aware`, `fixed_overlap`, `neighbor_expand` strategies; `semantic` is exposed but defers to Plan 4 (requires fastembed boundary-model load) (Plan 3, SC-008)
- ONNX-backed embedding model — `BAAI/bge-small-en-v1.5` fp32 via `ort = "2"`, gated on `pg_raggraph_core/onnx` feature; CLS-pooled + L2-normalized; dim mismatch is a load-time error (Plan 3, SC-004/SC-009/SC-010 unit; integration in Plan 4)
- `LlmProvider` trait surface in `pg_raggraph_core::llm` with `MockProvider` no-op impl; concrete impls land in Plan 4 (Plan 3, SC-015)
- Content-hash incremental skip — re-ingesting identical content is a no-op; document row count stays at 1 (Plan 3, SC-007)
- Per-document transaction atomicity — chunk-write failure rolls back the whole document (Plan 3, SC-011)
- Schema-invariant tests for `ingest_jobs.payload`/`attempt_count`/`ingest_jobs_active_idx` (columns/index already shipped in Plan 1's `001_tables.sql`/`002_indexes.sql`; Plan 3 locks them via tests)

### Carry-forward to later plans
- Real OpenAI / Anthropic / Ollama LLM provider impls (Plan 4)
- AES-GCM credential encryption (Plan 4)
- `pgrg.ask` LLM grounding (Plan 4)
- ONNX embedder wired into bg worker production builds via a `pg_raggraph/onnx` feature (Plan 4)
- chunkshop `semantic` strategy with fastembed boundary-model (Plan 4)
- Cross-backend bg-worker dispatch tests (deferred from Plan 3 due to pgrx test transaction model; manual `cargo pgrx run` verification documented in README)
- Sidecar binary (Plan 5)
- Cross-impl parity harness (Plan 6)

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

## [0.1.0-alpha.1] — 2026-05-03

### Added
- Cargo workspace skeleton (`pg_raggraph`, `pg_raggraph_core`, `pg_raggraph_sidecar`)
- pgrx 0.17 extension setup, PostgreSQL 17 target
- Initial schema: namespaces, documents, chunks, entities, relationships, chunk_entities, ingest_jobs, providers, migrations
- Indexes: HNSW on chunk + entity embeddings; GIN on text_search, metadata, entity name trigrams; partial index on active jobs
- Admin SQL functions:
  - `pgrg.namespace_create`, `pgrg.namespace_drop`
  - `pgrg.provider_create`, `pgrg.provider_drop`, `pgrg.provider_list` (redacted)
  - `pgrg.delete_document`, `pgrg.delete_namespace`
  - `pgrg.health`, `pgrg.status`
- GUCs: `bgw_workers` (default 2), `extract_concurrency`, `embed_dim` (default 384), `debug_retrieval`, `job_reaper_interval`, `parity_mode`, `master_key_path`, `embed_model_path`
- pgrx test suite covering schema bootstrap, admin functions, GUC defaults
- GitHub Actions CI: fmt, clippy, workspace check, pgrx tests on PG17

### Not yet implemented (per Plan 1 scope)
- Background worker, ingest pipeline (Plan 3)
- Retrieval (`pgrg.query`), embedding model loading (Plan 2/3)
- LLM extraction, `pgrg.ask` (Plan 4)
- Sidecar binary (Plan 5)
- Credential encryption (Plan 4)
- Cross-impl parity harness (Plan 6)
