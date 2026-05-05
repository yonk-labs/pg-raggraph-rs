# pg-raggraph-rs

> PostgreSQL-native GraphRAG. Three SQL statements → grounded answer.

````sql
CREATE EXTENSION pg_raggraph;
SELECT pgrg.ingest('docs/');
SELECT * FROM pgrg.ask('what changed in the auth module?');
````

This is the Rust extension implementation of [pg-raggraph](https://github.com/yonk-labs/pg-raggraph) (Python). Same retrieval semantics, packaged as a single PostgreSQL extension instead of an importable library.

## Status

**Pre-alpha.** Foundation in place: schema, namespaces, providers, GUCs, health/status. Ingest, retrieval, and `ask` land in subsequent plans (see `docs/superpowers/plans/`).

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
