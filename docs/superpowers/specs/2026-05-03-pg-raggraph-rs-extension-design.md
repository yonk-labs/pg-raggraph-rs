# pg-raggraph-rs — PostgreSQL Extension (Rust)

**Status:** Design (approved through brainstorming, awaiting implementation plan)
**Date:** 2026-05-03
**Target repo:** `yonk-labs/pg-raggraph-rs` (NEW — does not yet exist).
This spec was drafted in the Python `pg-raggraph` repo's `docs/superpowers/specs/` for convenience and travels to the new repo at first commit.

---

## 1. Thesis

> **GraphRAG with no Python app server, no orchestration framework — just SQL.**

```sql
CREATE EXTENSION pg_raggraph;
SELECT pgrg.ingest('docs/');
SELECT * FROM pgrg.ask('what changed in the auth module?');
```

Three statements, grounded answer with citations. That's the v1 demo.

The Python `pg-raggraph` library keeps the cloud-managed-PG audience (no `shared_preload_libraries` requirement, importable from any app). `pg-raggraph-rs` takes the self-hosted (and Azure) audience: one extension, single binary, SQL-native end to end. Cloud-managed-PG users (RDS, Cloud SQL, Supabase, Neon) get the **sidecar mode** (Section 7) — same SQL surface, plus one external process.

Both implementations target the same retrieval semantics and run the same parity benchmarks (Section 10).

---

## 2. Repo, crates, license, versions

**Repo:** `yonk-labs/pg-raggraph-rs` (new public GitHub repo).

**Cargo workspace, three members:**

| Crate | Purpose |
|---|---|
| `pg_raggraph` | The pgrx extension `.so`. Thin pgrx adapter: SQL function bindings, schema migrations, background worker registration, GUCs. |
| `pg_raggraph_core` | No-pgrx logic: graph store APIs, retrieval (recursive-CTE driver), smart-mode (deferred), provider abstractions, scoring/RRF, resolution. Testable with plain `cargo test`. |
| `pg_raggraph_sidecar` | Standalone binary that wraps `_core` for cloud-managed PG hosts where the extension can't be installed. |

**Why three crates:** mirrors `pg_agents` precedent (which we have working locally). `_core` is testable in isolation; the same code drives both extension and sidecar; pgrx-specific code stays small and auditable.

**License:** Apache-2.0 (matches `pg_agents`; compatible with `chunkshop` MIT and `lede` Apache-2.0 dependencies).

**Postgres:** 17+. Required extensions: `pgvector` 0.8+, `pg_trgm`. Optional: `pgsodium` or `pgcrypto` for stronger credential encryption (Section 6).

**Naming:**
- Cargo package + extension control file: `pg_raggraph` (`CREATE EXTENSION pg_raggraph;`).
- SQL schema: `pgrg` — all functions and tables. Short brand `pgrg` for everything user-facing.

**Toolchain:** `pgrx` (already cloned at `../pgrx/`), Rust edition 2024, `unsafe_code = "forbid"` in `_core` (extension crate has unavoidable `unsafe` for pgrx FFI; isolated and reviewed).

---

## 3. Architecture: ingestion path

`SELECT pgrg.ingest('docs/')` returns a job UUID immediately. Real work happens in the background worker.

### Flow

1. **SQL entry function** (`pg_raggraph::sql::ingest`): validates inputs, computes content hash, INSERTs `(status='queued', source, namespace, chunk_strategy, payload?)` into `pgrg.ingest_jobs`, returns job id. Non-blocking; user's connection is free immediately.

2. **Background worker** (`pg_raggraph::bgw`): registered in `_PG_init` when `shared_preload_libraries = 'pg_raggraph'`. Default 2 workers (configurable via `pgrg.bgw_workers` GUC). Each worker owns a single Tokio runtime and its own embedding-model in-memory copy.

   Polling loop:
   - `SELECT … FOR UPDATE SKIP LOCKED LIMIT 1` against `pgrg.ingest_jobs WHERE status='queued'`.
   - Mark `running`, dispatch to `_core::ingest::run_job`, on completion mark `completed` (or `failed` with error text).

3. **Per-job pipeline** (`pg_raggraph_core::ingest`):
   - **Read source bytes.** `pgrg.ingest(path)` resolves on the PG host filesystem (`postgres` OS user must be able to read it). `ingest_text` / `ingest_bytes` carry the payload in the job row.
   - **Chunk** via `chunkshop::chunker`. `chunk_strategy` job param controls mode (`auto`, `hierarchy`, `semantic`, `sentence_aware`, `fixed_overlap`, `neighbor_expand`); default `auto`.
   - **Embed** via local model loaded at worker startup (chunkshop's `hf_cache` + ONNX Runtime; default `BAAI/bge-small-en-v1.5` fp32).
   - **Extract** entities + relationships via the namespace's configured `LlmProvider`. Concurrency capped by `pgrg.extract_concurrency` (default 4).
   - **Resolve** entities: pg_trgm fuzzy + cosine on entity-name embeddings. Constants in shared parity fixture (Section 10).
   - **Persist** in a single PG transaction per document: `documents` → `chunks` → `entities` (upsert via resolution) → `relationships` → `chunk_entities`. Triggers maintain `entities.degree`. Set `ingest_jobs.status='completed'`.

4. **Errors:** any per-document failure → `status='failed'` + `error` column. Bg worker continues. `pgrg.status(job_id)` surfaces state. Crashed workers → reaper sweep (`pgrg.job_reaper_interval`, default 300s) re-queues `running` jobs whose `updated_at` is older than the interval; max 3 attempts.

5. **Sidecar parity:** `pg_raggraph_sidecar` runs the *same* `_core::ingest::run_job` loop against the same `pgrg.ingest_jobs` table over a libpq connection. Multiple sidecar instances coexist safely (`FOR UPDATE SKIP LOCKED`). SQL surface identical.

### What's *not* in v1 ingestion

- Community detection (Leiden/Louvain) — deferred to v2.
- Online entity re-resolution after upstream merges.
- File-watch / streaming ingestion.

Smart-mode at *query* time still works without communities (we just don't have a `global` retrieval mode in v1).

---

## 4. Architecture: query path

Two synchronous SQL entry points (both run on the caller's PG connection — no bg worker; user wants results, not a job):

```sql
pgrg.query (q text, filter jsonb DEFAULT NULL, top_k int DEFAULT 10,
            namespace text DEFAULT 'default', hops int DEFAULT 1,
            weights jsonb DEFAULT NULL, mode text DEFAULT 'hybrid')
  RETURNS TABLE (chunk_id uuid, document_id uuid, text text, score float, signals jsonb)

pgrg.ask   (q text, filter jsonb DEFAULT NULL, top_k int DEFAULT 10,
            namespace text DEFAULT 'default', hops int DEFAULT 1,
            llm_provider text DEFAULT NULL)
  RETURNS TABLE (answer text, citations jsonb, signals jsonb, mode_used text)
```

### Default mode is `hybrid` — every query fuses four signals

1. **Vector** — pgvector cosine on `pgrg.chunks.embedding` (HNSW or IVFFlat).
2. **BM25** — PG `tsvector` + `ts_rank_cd`, `plainto_tsquery('english', q)`.
3. **Graph** — entity-anchored, recursive-CTE neighbor expansion (depth `hops`, default 1). **Undirected** traversal (UNION on `dst_id` from `src` and `src_id` from `dst`) — pinned for parity with Python.
4. **Metadata predicate** — `filter jsonb` applied via `metadata @> filter` inside *each* lane (so we don't fuse junk we'd then have to throw away). Backed by GIN index on `chunks.metadata jsonb_path_ops`.

Fusion: **RRF (reciprocal rank fusion)**, default `k=60`, equal weights. `weights` parameter overrides for ablation experiments.

### Mode parameter (ablation knobs only)

`mode='hybrid'` (default) | `'vector'` | `'bm25'` | `'graph'`. Forces a single-lane query for benchmarking. **No automatic escalation** (smart-mode is explicitly out of scope; it's the Python lib's surface, not ours).

### The fused query (one SQL statement)

```sql
WITH
  q_emb AS (SELECT pgrg.embed($1) AS v),
  vec AS (
    SELECT c.id, ROW_NUMBER() OVER (ORDER BY c.embedding <=> (SELECT v FROM q_emb)) AS rk
    FROM pgrg.chunks c
    WHERE c.namespace = $4
      AND ($2::jsonb IS NULL OR c.metadata @> $2)
    ORDER BY c.embedding <=> (SELECT v FROM q_emb) LIMIT 50
  ),
  bm AS (
    SELECT c.id, ROW_NUMBER() OVER (ORDER BY ts_rank_cd(c.text_search, q) DESC) AS rk
    FROM pgrg.chunks c, plainto_tsquery('english', $1) q
    WHERE c.text_search @@ q
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
    WHERE c.namespace = $4
      AND ($2::jsonb IS NULL OR c.metadata @> $2)
    GROUP BY m.chunk_id LIMIT 50
  ),
  fused AS (
    SELECT id, SUM(1.0 / (60 + rk)) AS score,
           jsonb_agg(jsonb_build_object('lane',lane,'rk',rk)) AS sigs
    FROM (
      SELECT id, rk, 'vec'   AS lane FROM vec
      UNION ALL SELECT id, rk, 'bm25'  FROM bm
      UNION ALL SELECT id, rk, 'graph' FROM graph
    ) u
    GROUP BY id
  )
SELECT c.id, c.document_id, c.text, f.score, f.sigs
FROM fused f JOIN pgrg.chunks c ON c.id = f.id
ORDER BY f.score DESC LIMIT $3;
```

This is the AGE-replacement value prop made concrete: pgvector + recursive CTE + JOIN, one query, one index plan. AGE can't compose Cypher and pgvector in a single round-trip; we can.

### `pgrg.ask` flow

`query` → assemble token-budgeted context from top-k chunks → call configured `LlmProvider` synchronously with citation-required prompt → return `(answer, citations, signals, mode_used)`. Citations carry chunk ids so callers can render anchored links. Token budget configurable per namespace.

---

## 5. Schema

`CREATE EXTENSION pg_raggraph` creates everything in the `pgrg` schema.

```sql
pgrg.namespaces       (name text PK, embedding_model text, llm_provider text, settings jsonb,
                       created_at timestamptz)

pgrg.documents        (id uuid PK, namespace text, source text, content_hash text UNIQUE,
                       title text, metadata jsonb, ingested_at timestamptz)

pgrg.chunks           (id uuid PK, namespace text, document_id uuid REFERENCES documents,
                       ord int, text text, token_count int,
                       embedding vector(N),                    -- N set DB-wide via pgrg.embed_dim GUC
                       text_search tsvector GENERATED ALWAYS AS (...) STORED,
                       metadata jsonb,
                       UNIQUE(document_id, ord))

pgrg.entities         (id uuid PK, namespace text, name text, kind text,
                       name_emb vector(N),                     -- same N as chunks.embedding
                       description text, properties jsonb,
                       degree int DEFAULT 0,                   -- maintained by trigger
                       UNIQUE(namespace, name, kind))

pgrg.relationships    (id uuid PK, namespace text, src_id uuid, dst_id uuid,
                       kind text, description text, weight float DEFAULT 1.0,
                       provenance jsonb,                       -- {source_chunk_id, confidence, classification}
                       UNIQUE(namespace, src_id, dst_id, kind))

pgrg.chunk_entities   (chunk_id uuid, entity_id uuid,
                       confidence float, classification text,  -- 'extracted'|'inferred'|'ambiguous'
                       PRIMARY KEY(chunk_id, entity_id))

pgrg.ingest_jobs      (id uuid PK, status text, source text, namespace text,
                       chunk_strategy text, error text,
                       attempt_count int DEFAULT 0,
                       payload bytea,                          -- for ingest_text/ingest_bytes
                       enqueued_at, started_at, finished_at, updated_at)

pgrg.providers        (name text PK, kind text,                -- 'llm' | 'embedding'
                       provider text,                          -- 'openai'|'anthropic'|'ollama'|'local'
                       base_url text, model text,
                       credential text,                        -- AES-GCM if master key set
                       config jsonb)

pgrg.migrations       (version int PK, applied_at timestamptz)
```

### Design choices

- **Embeddings inline on `chunks`** (not a side table) — one less join in the hot path. **Vector dimension is DB-wide**, set at extension install via the `pgrg.embed_dim` GUC; all namespaces in a database share the same dimension. Mixing embedding dimensions across namespaces is a v2 concern (likely solved by per-namespace partitioned tables).
- **`text_search` is `GENERATED ALWAYS … STORED`** — auto-updates on INSERT/UPDATE, GIN-indexable, no triggers.
- **`degree` materialized on `entities`** — maintained by trigger on `relationships`. Saves `COUNT(*)` per entity at retrieval.
- **Provenance lives in `relationships.provenance` and `chunk_entities`** — every fact traces to source chunk + extraction confidence + classification.
- **Multi-tenancy via `namespace text` column** indexed first. Cheaper than schema-per-tenant. Hard isolation: one PG schema per tenant.
- **`metadata jsonb` on documents and chunks**, GIN-indexed (`jsonb_path_ops`). Predicate filtering in retrieval.

### Indexes (created at extension install)

- `chunks(namespace, document_id)`
- `chunks USING hnsw(embedding vector_cosine_ops)` — IVFFlat alternative when `pgrg.parity_mode = true`
- `chunks USING gin(text_search)` — BM25
- `chunks USING gin(metadata jsonb_path_ops)` — predicate filtering
- `entities(namespace, name)`
- `entities USING gin(name gin_trgm_ops)` — fuzzy resolution
- `entities USING hnsw(name_emb vector_cosine_ops)` — entity seed match
- `relationships(src_id)`, `relationships(dst_id)`, `relationships(namespace, kind)`
- `chunk_entities(entity_id)` (chunk_id is PK-prefix)
- `ingest_jobs(status, enqueued_at) WHERE status IN ('queued','running')` — partial index for the bg worker scan

### Migrations

SQL files in `pg_raggraph/sql/migrations/NNN_<slug>.sql`. `_PG_init` runs pending migrations idempotently against `pgrg.migrations`. Schema changes ship with the extension; no separate `pgrg.migrate()` call.

---

## 6. Public SQL surface

Everything under `pgrg.` schema.

### Ingestion (async, return `uuid` job id)

```sql
pgrg.ingest        (path text,    namespace text DEFAULT 'default', chunk_strategy text DEFAULT 'auto')
pgrg.ingest_text   (name text, content text, namespace text DEFAULT 'default', chunk_strategy text DEFAULT 'auto')
pgrg.ingest_bytes  (name text, bytes bytea,  namespace text DEFAULT 'default', chunk_strategy text DEFAULT 'auto')
```

### Retrieval (synchronous on caller's connection)

```sql
pgrg.query  (q text, filter jsonb DEFAULT NULL, top_k int DEFAULT 10, namespace text DEFAULT 'default',
             hops int DEFAULT 1, weights jsonb DEFAULT NULL, mode text DEFAULT 'hybrid')
pgrg.ask    (q text, filter jsonb DEFAULT NULL, top_k int DEFAULT 10, namespace text DEFAULT 'default',
             hops int DEFAULT 1, llm_provider text DEFAULT NULL)
pgrg.embed  (text text, namespace text DEFAULT 'default')
```

### Operational

```sql
pgrg.status            (job_id uuid DEFAULT NULL)              -- one job, or queue summary if NULL
pgrg.health            ()                                       -- jsonb: bgw pid, queue depth, last error, model loaded
pgrg.delete_document   (document_id uuid)
pgrg.delete_namespace  (name text, cascade boolean DEFAULT false)
```

### Namespace + provider admin

```sql
pgrg.namespace_create (name text, embedding_model text DEFAULT 'bge-small-en-v1.5',
                       llm_provider text DEFAULT NULL, settings jsonb DEFAULT '{}')
pgrg.namespace_drop   (name text, cascade boolean DEFAULT false)

pgrg.provider_create  (name text, kind text,                   -- 'llm' | 'embedding'
                       provider text, base_url text, model text,
                       credential text, config jsonb DEFAULT '{}')
pgrg.provider_drop    (name text)
pgrg.provider_list    ()                                       -- credentials redacted
```

### Parity

```sql
pgrg.ingest_extracted (path text, namespace text DEFAULT 'default')
                                                               -- loads pre-extracted JSONL fixture
                                                               -- skips chunk/embed/extract; writes directly
```

### Permissions

`REVOKE ALL ON pgrg.providers FROM PUBLIC` at install time. `provider_*` functions are `SECURITY DEFINER`. Reading `pgrg.providers` directly requires a granted role. `provider_list()` redacts credentials (`'sk-...***'`).

---

## 7. Configuration & credentials

### GUCs (operator-level tunables)

| GUC | Default | Purpose |
|---|---|---|
| `pgrg.bgw_workers` | `2` | Number of bg worker processes |
| `pgrg.extract_concurrency` | `4` | Concurrent LLM extraction calls per worker |
| `pgrg.embed_model_path` | (HF cache) | Override embedding model location (offline installs) |
| `pgrg.embed_dim` | `384` | DB-wide vector dimension. Must be set before `CREATE EXTENSION` runs migrations; must match the embedding model. Default 384 matches `bge-small-en-v1.5`. |
| `pgrg.master_key_path` | (none) | File path to AES-GCM master key for credential encryption |
| `pgrg.debug_retrieval` | `false` | Populate `signals` jsonb in `pgrg.query` results |
| `pgrg.job_reaper_interval` | `300s` | How often to re-queue stuck `running` jobs |
| `pgrg.parity_mode` | `false` | At namespace creation: use IVFFlat instead of HNSW for deterministic parity benchmarks |

### Per-tenant config — `pgrg.providers` table

LLM endpoints, embedding endpoints, API keys. Managed only via `provider_*` SQL functions (never raw INSERTs).

### Credential storage

- **If `pgrg.master_key_path` is set:** `provider_create` encrypts `credential` with AES-GCM. Master key file must be readable only by the postgres OS user; we error at startup if perms are too open. Stored as `enc:v1:<nonce>:<ciphertext>` in `pgrg.providers.credential`.
- **If unset:** credential stored plaintext. Extension logs a `WARNING` at startup: *"pgrg.master_key_path not set — provider credentials stored plaintext. They will appear in `pg_dump`. Set a master key for production."*

We are not a secret manager. Operators needing stronger guarantees integrate `pgsodium` or an init-job pulling secrets from a vault — both compatible with this schema.

### Provider resolution at runtime

Every operation that needs a provider picks in this order:
1. Explicit param (`pgrg.ask(..., llm_provider := 'gpt4-prod')`)
2. Namespace's `llm_provider` / `embedding_model` setting
3. First matching provider in `pgrg.providers` with the right `kind`
4. Error.

### LLM provider abstraction (`pg_raggraph_core::llm`)

`LlmProvider` trait; concrete impls `OpenAiProvider`, `AnthropicProvider`, `OllamaProvider`, `MockProvider`. `RetryingProvider` wrapper for backoff. Pattern matches `pg_agents` precedent.

### Embedding provider — local default (G1)

- Bg worker loads `BAAI/bge-small-en-v1.5` (fp32 ONNX) via chunkshop's `hf_cache` at startup.
- Inference in-process; no network, no credential.
- HTTP embedding providers (OpenAI text-embedding-3, Voyage, Cohere) added as opt-in via `pgrg.providers` + `kind='embedding'`. v1 supports the API surface; default is local.

---

## 8. Sidecar mode (cloud-managed PG)

For RDS, Cloud SQL, Supabase, Neon — anywhere `shared_preload_libraries` modification is unavailable.

### Mechanics

- **Same `pgrg.ingest_jobs` contract.** `pg_raggraph_sidecar` polls with `FOR UPDATE SKIP LOCKED` over libpq, processes jobs, writes results back. Multiple sidecar instances coexist (queue is the coordination point).
- **Bootstrap via embedded SQL.** Managed PG can't `CREATE EXTENSION pg_raggraph`, so the sidecar embeds the schema DDL and runs it idempotently against `pgrg.migrations` on first connect. The pgrx-generated SQL for native functions is *not* available — those functions are sidecar-side only.
- **`query` works without the sidecar.** The fused recursive-CTE query (Section 4) is plain SQL — no native functions needed. We ship it as a SQL view-builder so users can run search directly. Sidecar only matters for `ingest` (async LLM extraction) and `ask` (LLM grounding).
- **`ask` exposed as HTTP/JSON.** Sidecar listens on a configurable port, accepts `POST /v1/ask` with `{q, filter, ...}`. We ship a thin SQL wrapper in `pg_raggraph_sidecar/sql/client.sql` that uses `pg_net` (available on Supabase/Neon) to call the sidecar from PL/pgSQL. SQL call shape stays the same: `SELECT * FROM pgrg.ask('q')`.
- **Configuration:** GUCs become CLI flags / env vars (`PGRG_EXTRACT_CONCURRENCY`, `PGRG_MASTER_KEY_PATH`, etc.). Provider table managed via the same SQL functions (sidecar exposes them via the embedded SQL wrapper, or directly via REST).
- **Embedding model loaded identically** via chunkshop's `hf_cache` in the sidecar process.

### Demo flow comparison

| Mode | Setup | Demo query |
|---|---|---|
| Self-hosted / Azure | `CREATE EXTENSION pg_raggraph;` | 3 SQL statements |
| Managed PG (sidecar) | `CREATE EXTENSION pgvector; \i pgrg-bootstrap.sql;` + run sidecar | 4 SQL statements + 1 process |

Honest trade-off, same SQL surface.

---

## 9. Testing strategy

| Layer | Tooling | Scope |
|---|---|---|
| `pg_raggraph_core` | `cargo test` | Unit tests, mock `LlmProvider`, golden-corpus retrieval scoring. No PG. |
| `pg_raggraph` (extension) | `pgrx::pg_test` | Per-test fresh PG with extension installed. SQL surface, schema migrations, bg worker happy path, recursive-CTE correctness. |
| `pg_raggraph_sidecar` | Docker PG (`docker-compose.test.yml`) | Integration; same fixtures as extension tests where possible (byte-identical results from both code paths on same input). |
| Cross-impl parity | `bench/parity/` (Section 10) | Both Python and Rust against same corpus + queries; Jaccard ≥ 0.8 on top-k. |
| Bench | `criterion` | Ingest throughput, retrieval p50/p95/p99 across corpus tiers (1K / 100K / 1M chunks). |
| CI | GitHub Actions | PG 17 (and 18 once stable), Linux x86_64 + arm64. macOS for dev convenience, not gated. |

---

## 10. Cross-implementation parity

The goal: any benchmark that runs on Python `pg-raggraph` runs on `pg-raggraph-rs` and produces comparable, defensible top-k results. Anything that breaks that goal is a parity bug, not a feature decision.

### Parity contracts (both impls MUST honor)

**Chunking — chunkshop is the canonical chunker.**
Rust uses chunkshop natively. Python uses chunkshop via Pattern D (`chunk_strategy="chunkshop:hierarchy"`) for any benchmark run. The parity suite forces chunkshop on both sides.

**Embedding artifact — identical ONNX file, identical precision.**
- Canonical: `BAAI/bge-small-en-v1.5`, ONNX export, fp32 (no quantization for parity runs; quantized variants are a v2 perf knob).
- Both Python `FastEmbedProvider` and chunkshop's `hf_cache` resolve to the same `model.onnx` blob. Verified by SHA256 in a parity precheck.
- If chunkshop's loader uses a different runtime/precision today, that's a chunkshop upstream contribution before benchmarks land.

**Tokenizer — chunkshop authoritative.**
The HF tokenizer that ships with `bge-small-en-v1.5` is canonical. Both impls use it for chunk-size accounting.

**Resolution constants — shared fixture.**
- `bench/parity/resolution_constants.yaml` holds pg_trgm threshold, cosine threshold, tie-break rules, name-canonicalization regex.
- Both impls read this file at startup (Python) or compile-time-include (Rust, via `include_str!`).
- A `parity::resolution_constants` cross-impl test asserts both stacks produce identical canonical entity ids on a fixed input set. Drift fails CI.

**Retrieval mode — pinned for benchmarks.**
- All parity benchmarks force `mode='hybrid'`.
- Smart-mode (Python) is out of scope for parity — production heuristic, not comparable surface.
- RRF defaults pinned: `k=60`, equal weights `{vec: 1, bm25: 1, graph: 1}`. Override via `weights` only for ablation.

**Graph traversal direction — undirected.**
- Recursive CTE in both impls treats relationships as undirected (UNION ALL on `dst_id` from `src` and `src_id` from `dst`).
- `hops` semantics identical: 0 disables graph lane, 1 = direct neighbors, 2 = friends-of-friends.

**Vector index for parity runs — IVFFlat, not HNSW.**
- Production deployments use HNSW.
- Parity benchmarks set `pgrg.parity_mode = true` at namespace creation, which swaps to IVFFlat (deterministic; eliminates index-build randomness).

### Frozen-graph corpus (workaround for LLM non-determinism)

LLM-based extraction can't be byte-deterministic. Benchmarking it side-by-side punishes both impls for the LLM's variance.

- `bench/parity/corpus/` holds curated documents (small / medium / large tiers).
- `bench/parity/extracted/` holds **pre-extracted** entities + relationships + chunk-entity mentions as JSONL, produced once by a designated LLM run (model + version + temperature + seed all recorded).
- Both impls expose `pgrg.ingest_extracted()` (Rust) / `ingest_extracted()` (Python) admin functions that take fixture path and skip chunking/embedding/extraction, loading directly into `chunks`/`entities`/`relationships`. Embeddings pre-computed and bundled, so the embedding model isn't in the loop for parity runs either.
- Result: parity benchmark exercises *resolution + storage + retrieval + ranking + fusion*. LLM extraction quality has its own qualitative eval with a golden set.

### Parity test harness

`bench/parity/` ships:

- `compare.py` — drives both stacks against the same query set; fixed top-k, fixed mode, fixed seed where seedable. Outputs per-query Jaccard, p50/p95 latency, regression summary.
- `query_sets/` — small curated sets per corpus tier with rationale per query.
- `metrics.md` — the parity bar:
  - **Top-k Jaccard ≥ 0.8** (Rust vs Python) on the frozen-graph corpus, `mode='hybrid'`, `top_k=10`, IVFFlat indexes.
  - **Strict equality** on resolution canonical-id assignment.
  - **Latency:** Rust ≤ 1.0× Python on retrieval (no regressions). v2 target: Rust ≤ 0.5× Python on ingest plumbing. v1 only requires "not worse."
- `ci/parity.yml` — runs small tier on every PR, medium on `main`, large on tags.

### What parity does *not* cover

- LLM extraction quality (separate qualitative eval with golden entities/relationships per chunk).
- `ask` answer text — compare retrieved chunk set instead.
- Performance under load (separate bench, not in this spec).
- Behavioral parity of admin operations (`provider_create`, namespace lifecycle, etc.) — they're spec-conformant, not output-comparable.

---

## 11. Explicitly out of scope for v1

Each is plausible v2 work. None belongs in the first ship.

- **Smart-mode / confidence-triggered routing.** Replaced by hybrid-by-default.
- **Community detection** (Leiden / Louvain) and the `global` retrieval mode that depends on it.
- **Custom index access method** for fused vector+graph queries.
- **New SQL operators / Cypher-ish syntax.**
- **Streaming / file-watch ingestion**, multi-modal inputs (images, audio).
- **Online entity re-resolution** after upstream merges. Resolution at ingest only.
- **Distributed / sharded mode.**
- **Web UI** (Python lib has one; not part of the extension).
- **MCP server** (Python lib has one; could be added later).
- **Cross-namespace federation.**
- **Adaptive provider rate-limiting / backoff.** Basic retries only.

---

## 12. Decisions log

| # | Decision | Rationale |
|---|---|---|
| 1 | v1-A demo: 3 SQL statements → grounded answer | The hook that justifies a `CREATE EXTENSION` |
| 2 | Hybrid mode + sidecar option (E4) | Default = bg worker for self-hosted; sidecar for managed PG |
| 3 | F3: chunkshop/lede as Cargo deps via thin pgrx adapter | Maintainable, testable in isolation, contribute upstream rather than reach in |
| 4 | Three-crate workspace (`pg_raggraph` / `_core` / `_sidecar`) | Mirrors `pg_agents`; `_core` is testable without pgrx; same code drives extension + sidecar |
| 5 | License Apache-2.0 | Matches `pg_agents`, compatible with chunkshop MIT and lede Apache-2.0 |
| 6 | PostgreSQL 17+ | Matches `pg_agents`, modern pgrx targets |
| 7 | G1: local embedding model default | One external dep (LLM only), cleanest first-run experience, reuses chunkshop's `hf_cache` |
| 8 | Hybrid-by-default retrieval (vector + BM25 + graph + metadata predicate, RRF fusion) | User explicit requirement; simpler than smart-mode escalation; consistent latency |
| 9 | Smart-mode dropped from v1 | Replaced by always-hybrid; preserved as Python-side surface; not needed for parity |
| 10 | Default `pgrg.bgw_workers = 2`, configurable | User explicit |
| 11 | Provider config in `pgrg.providers` table, not GUCs | Matches `pg_agents` precedent; supports multi-tenancy; security via REVOKE + SECURITY DEFINER |
| 12 | Credentials: AES-GCM with optional master key, plaintext fallback with WARNING | Honest about scope (not a secret manager); compatible with pgsodium upgrade path |
| 13 | Multi-tenancy via `namespace text` column everywhere | Cheaper than schema-per-tenant; users wanting hard isolation use one PG schema |
| 14 | Graph traversal undirected (UNION on src and dst) | Required for parity with Python lib |
| 15 | Parity bar: top-k Jaccard ≥ 0.8 on frozen-graph corpus, IVFFlat indexes | Accommodates HNSW non-determinism; LLM bypass via pre-extracted fixtures |
| 16 | `ingest_extracted()` admin function for benchmarks | The mechanism that makes cross-impl parity actually testable |

---

## 13. What this spec does *not* decide (deferred to implementation plan)

- Concrete migration step ordering.
- Retry / backoff timing constants for `LlmProvider`.
- Token budget defaults for `pgrg.ask` context assembly.
- Specific GIN index operator class fine-tuning (`jsonb_path_ops` vs default).
- BM25 language config selection beyond the default `'english'`.
- Sidecar's HTTP API exact shape (paths, error envelopes) — drafted at implementation time.
- Whether the sidecar's HTTP endpoint speaks JSON-RPC or REST.
- Concrete benchmark corpus selection (we'll likely seed with a public set + an internal one).

These are implementation-plan concerns, not design-spec concerns.
