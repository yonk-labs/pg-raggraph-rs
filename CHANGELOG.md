# Changelog

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
