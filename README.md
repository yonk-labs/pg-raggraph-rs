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

## Sidecar (managed PostgreSQL)

Cloud-managed PostgreSQL (RDS, Cloud SQL, Supabase, Neon) forbids
`shared_preload_libraries`, so the extension can't load. The
`pg_raggraph_sidecar` binary runs the same `_core` GraphRAG engine as an
external process that talks to the database over plain libpq + HTTP — no
pgrx, no SPI, no preload.

```sql
-- 1. On the managed DB (one-time, the only privileges you need):
CREATE EXTENSION vector;
CREATE EXTENSION pg_trgm;
```

```bash
# 2. Run the sidecar — it bootstraps all pgrg.* tables on first connect:
pg_raggraph_sidecar --database-url "$PGRG_DATABASE_URL" --http-bind 0.0.0.0:8080
```

```sql
-- 3. Enqueue an ingest with plain SQL (no pgrx pgrg.ingest() in this mode):
INSERT INTO pgrg.ingest_jobs (namespace, payload) VALUES ('default', 'hello world');
-- ... or, for an in-SQL pgrg.ask() over pg_net, install the shim:
--   \i pg_raggraph_sidecar/sql/client.sql   (requires pg_net)
```

```sql
-- 4. Ask — over the pg_net SQL shim, or directly over HTTP:
SELECT * FROM pgrg.ask('what changed in auth?');          -- via client.sql shim
-- curl -s localhost:8080/v1/ask -d '{"q":"what changed in auth?"}'
```

Caveats (v1): **no HTTP auth** and **no in-sidecar TLS** — deploy on a
private network behind a reverse proxy that terminates TLS. The `pg_net`
shim needs a Supabase-flavored PostgreSQL (`pg_net` is not on stock
images); validate the live `pg_net` egress path on your real managed-PG
instance. The HTTP `/v1/ask` path has no such dependency.

## License

Apache-2.0. See `LICENSE`.

## Sibling projects

- [pg-raggraph](https://github.com/yonk-labs/pg-raggraph) — the Python implementation; cloud-managed-PG compatible
- [chunkshop](https://github.com/yonk-labs/chunkshop) — chunking + embedding pipeline used by both
- [lede](https://github.com/yonk-labs/lede) — extractive scoring (TF-IDF, sentence ranking)
