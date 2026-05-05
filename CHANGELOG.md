# Changelog

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
