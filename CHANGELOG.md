# Changelog

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
