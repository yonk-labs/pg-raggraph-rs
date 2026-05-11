# pg-raggraph-rs

> PostgreSQL-native GraphRAG. Three SQL statements → grounded answer.

````sql
CREATE EXTENSION pg_raggraph;
SELECT pgrg.ingest('docs/');
SELECT * FROM pgrg.ask('what changed in the auth module?');
````

This is the Rust extension implementation of [pg-raggraph](https://github.com/yonk-labs/pg-raggraph) (Python). Same retrieval semantics, packaged as a single PostgreSQL extension instead of an importable library.

## Status

**Pre-alpha (0.1.0-alpha.3).** Foundation + retrieval engine + **async ingest pipeline** in place: schema, namespaces, providers, GUCs, health/status, hybrid retrieval (`pgrg.query`), deterministic test embeddings (`pgrg.embed`), fixture loader (`pgrg.ingest_extracted`), **plus** background worker pool, queue-backed async ingest (`pgrg.ingest`, `pgrg.ingest_text`, `pgrg.ingest_bytes`), chunkshop integration as the canonical chunker, ONNX-backed embedding model (`BAAI/bge-small-en-v1.5` fp32) loaded once per worker, content-hash incremental skip, ingestion profile knobs (`conservative`/`balanced`/`aggressive`/`max`), and reaper sweep. LLM grounding (Plan 4), sidecar (Plan 5), and the parity harness (Plan 6) land in subsequent plans.

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

## Requirements

- PostgreSQL 17+
- `pgvector` 0.8+
- `pg_trgm`
- `shared_preload_libraries = 'pg_raggraph'` (default mode)
- For cloud-managed PG without preload access: see sidecar mode (Plan 5)

## Building

```bash
cargo install --locked cargo-pgrx --version =0.17.0
cargo pgrx init --pg17 $(which pg_config)
cargo pgrx run pg17 -p pg_raggraph
```

In the resulting `psql` session:

```sql
CREATE EXTENSION pg_raggraph CASCADE;
SELECT pgrg.health();
```

## License

Apache-2.0. See `LICENSE`.

## Sibling projects

- [pg-raggraph](https://github.com/yonk-labs/pg-raggraph) — the Python implementation; cloud-managed-PG compatible
- [chunkshop](https://github.com/yonk-labs/chunkshop) — chunking + embedding pipeline used by both
- [lede](https://github.com/yonk-labs/lede) — extractive scoring (TF-IDF, sentence ranking)
