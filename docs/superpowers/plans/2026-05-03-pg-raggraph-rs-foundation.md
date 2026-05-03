# pg-raggraph-rs Foundation + Schema — Implementation Plan (Plan 1 of 6)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the new `pg-raggraph-rs` repo with a working pgrx extension that installs into PostgreSQL 17, creates the full schema from the design spec, and exposes admin SQL functions (namespace + provider CRUD, health, status, deletes).

**Architecture:** Three-crate Cargo workspace mirroring `pg_agents`:
- `pg_raggraph` — the pgrx `cdylib` extension; thin SQL surface + schema migrations
- `pg_raggraph_core` — no-pgrx logic (types, errors, future provider traits); plain `cargo test`-able
- `pg_raggraph_sidecar` — placeholder binary crate (real work in Plan 5)

This plan ends *before* the bg worker, retrieval engine, ingest pipeline, LLM provider impls, and credential encryption. Each of those gets its own plan (2–6 in the arc).

**Tech Stack:** Rust 2024, `pgrx = "=0.17.0"`, PostgreSQL 17, `pgvector` 0.8+, `pg_trgm`, `serde`/`serde_json`, `uuid`. License Apache-2.0.

**Spec reference:** `docs/superpowers/specs/2026-05-03-pg-raggraph-rs-extension-design.md`. Section numbers below cite that doc.

**Plan arc (context — not executed here):**
1. **Foundation + Schema** ← this plan
2. Retrieval engine (fused recursive CTE, `pgrg.query`, `ingest_extracted` for fixtures)
3. Ingest pipeline (bg worker, chunkshop integration, `pgrg.ingest`, embedding model)
4. LLM extraction + ask (provider trait + impls, credential encryption, `pgrg.ask`)
5. Sidecar binary
6. Cross-impl parity harness

---

## Pre-execution: directory naming

The existing path `/home/yonk/yonk-tools/pg-raggraph-rs/` currently contains a duplicate of the Python `pg-raggraph` project. **Do NOT clobber it.** This plan creates the new Rust repo at:

```
/home/yonk/yonk-tools/pg-raggraph-extension/
```

Resolving whether to repurpose the `pg-raggraph-rs` directory name is a user-driven decision outside this plan. Substitute `<REPO>` = `/home/yonk/yonk-tools/pg-raggraph-extension` throughout. The GitHub repo at `yonk-labs/pg-raggraph-rs` does not need to match the local directory name.

When the new repo is initialized, **copy** (don't move) the spec from the source repo:
```
src:  /home/yonk/yonk-tools/pg-raggraph-rs/docs/superpowers/specs/2026-05-03-pg-raggraph-rs-extension-design.md
dst:  <REPO>/docs/superpowers/specs/2026-05-03-pg-raggraph-rs-extension-design.md
```
And copy this plan:
```
src:  /home/yonk/yonk-tools/pg-raggraph-rs/docs/superpowers/plans/2026-05-03-pg-raggraph-rs-foundation.md
dst:  <REPO>/docs/superpowers/plans/2026-05-03-pg-raggraph-rs-foundation.md
```

---

## Task 1: Bootstrap repo + Cargo workspace

**Files:**
- Create: `<REPO>/Cargo.toml`
- Create: `<REPO>/LICENSE`
- Create: `<REPO>/.gitignore`
- Create: `<REPO>/README.md`
- Create: `<REPO>/CHANGELOG.md`
- Create: `<REPO>/rust-toolchain.toml`

- [ ] **Step 1.1: Create the directory and copy spec/plan**

```bash
mkdir -p <REPO>/docs/superpowers/specs <REPO>/docs/superpowers/plans
cp /home/yonk/yonk-tools/pg-raggraph-rs/docs/superpowers/specs/2026-05-03-pg-raggraph-rs-extension-design.md \
   <REPO>/docs/superpowers/specs/
cp /home/yonk/yonk-tools/pg-raggraph-rs/docs/superpowers/plans/2026-05-03-pg-raggraph-rs-foundation.md \
   <REPO>/docs/superpowers/plans/
cd <REPO>
git init -b main
```

- [ ] **Step 1.2: Write `Cargo.toml` (workspace root)**

```toml
[workspace]
resolver = "2"
members = ["pg_raggraph", "pg_raggraph_core", "pg_raggraph_sidecar"]

[workspace.package]
version = "0.1.0-alpha.1"
edition = "2024"
license = "Apache-2.0"
repository = "https://github.com/yonk-labs/pg-raggraph-rs"
homepage = "https://github.com/yonk-labs/pg-raggraph-rs"
authors = ["The Yonk <matt@theyonk.com>"]
rust-version = "1.85"

[workspace.dependencies]
pgrx = "=0.17.0"
pgrx-tests = "=0.17.0"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
uuid = { version = "1", features = ["v4", "serde"] }
thiserror = "2"
tracing = "0.1"
```

- [ ] **Step 1.3: Write `LICENSE` (Apache-2.0)**

Use the standard text from https://www.apache.org/licenses/LICENSE-2.0.txt. Replace placeholders with `2026 Yonk Labs`.

- [ ] **Step 1.4: Write `.gitignore`**

```
target/
**/*.rs.bk
Cargo.lock.bak
.DS_Store
*.swp
.idea/
.vscode/
.pgrx/
skill-output/
```

> NOTE on `Cargo.lock`: it IS checked in for binary/extension crates. Don't ignore it.

- [ ] **Step 1.5: Write `rust-toolchain.toml`**

```toml
[toolchain]
channel = "1.85"
components = ["rustfmt", "clippy"]
```

- [ ] **Step 1.6: Write minimal `README.md`**

```markdown
# pg-raggraph-rs

PostgreSQL extension implementing GraphRAG natively in PostgreSQL.
Three SQL statements → grounded answer.

**Status:** Pre-alpha (foundation + schema). See `docs/superpowers/specs/`.

## License
Apache-2.0
```

- [ ] **Step 1.7: Write minimal `CHANGELOG.md`**

```markdown
# Changelog

## [Unreleased]

### Added
- Initial Cargo workspace skeleton (Plan 1)
```

- [ ] **Step 1.8: Initial commit**

```bash
git add Cargo.toml LICENSE .gitignore README.md CHANGELOG.md rust-toolchain.toml docs/
git commit -m "chore: initial workspace skeleton (Plan 1, Task 1)"
```

---

## Task 2: pg_raggraph extension crate skeleton

**Files:**
- Create: `<REPO>/pg_raggraph/Cargo.toml`
- Create: `<REPO>/pg_raggraph/pg_raggraph.control`
- Create: `<REPO>/pg_raggraph/src/lib.rs`
- Create: `<REPO>/pg_raggraph/src/bin/pgrx_embed.rs`

- [ ] **Step 2.1: Write `pg_raggraph/Cargo.toml`**

```toml
[package]
name = "pg_raggraph"
version.workspace = true
edition.workspace = true
license.workspace = true
repository.workspace = true

[lib]
crate-type = ["cdylib", "lib"]

[[bin]]
name = "pgrx_embed_pg_raggraph"
path = "./src/bin/pgrx_embed.rs"

[features]
default = ["pg17"]
pg17 = ["pgrx/pg17", "pgrx-tests/pg17"]
pg18 = ["pgrx/pg18", "pgrx-tests/pg18"]
pg_test = []

[dependencies]
pgrx = { workspace = true }
pg_raggraph_core = { path = "../pg_raggraph_core" }
serde = { workspace = true }
serde_json = { workspace = true }
uuid = { workspace = true }
thiserror = { workspace = true }
tracing = { workspace = true }

[dev-dependencies]
pgrx-tests = { workspace = true }
```

- [ ] **Step 2.2: Write `pg_raggraph/pg_raggraph.control`**

```
comment = 'PostgreSQL-native GraphRAG: hybrid retrieval over a single Postgres'
default_version = '@CARGO_VERSION@'
module_pathname = '$libdir/pg_raggraph'
relocatable = false
superuser = true
schema = 'pgrg'
requires = 'vector, pg_trgm'
```

- [ ] **Step 2.3: Write `pg_raggraph/src/bin/pgrx_embed.rs`**

```rust
::pgrx::pgrx_embed!();
```

- [ ] **Step 2.4: Write `pg_raggraph/src/lib.rs` (minimal entry)**

```rust
//! pg_raggraph — PostgreSQL-native GraphRAG extension.
//!
//! See `docs/superpowers/specs/2026-05-03-pg-raggraph-rs-extension-design.md`.

use pgrx::prelude::*;

::pgrx::pg_module_magic!(name, version);

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn extension_loads() {
        // Smoke test: extension is installable and returns a known SQL true.
        assert_eq!(Spi::get_one::<bool>("SELECT true").unwrap(), Some(true));
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![
            "shared_preload_libraries='pg_raggraph'",
            "pg_raggraph.bgw_workers=2",
        ]
    }
}
```

- [ ] **Step 2.5: Verify it builds (`cargo check`)**

```bash
cd <REPO>
cargo check -p pg_raggraph --features pg17
```

Expected: clean compile (warnings about unused imports OK; no errors).

- [ ] **Step 2.6: Commit**

```bash
git add pg_raggraph/
git commit -m "feat(extension): pg_raggraph crate skeleton (pgrx 0.17, pg17 default)"
```

---

## Task 3: pg_raggraph_core crate skeleton

**Files:**
- Create: `<REPO>/pg_raggraph_core/Cargo.toml`
- Create: `<REPO>/pg_raggraph_core/src/lib.rs`
- Create: `<REPO>/pg_raggraph_core/src/error.rs`
- Create: `<REPO>/pg_raggraph_core/src/types.rs`

- [ ] **Step 3.1: Write `pg_raggraph_core/Cargo.toml`**

```toml
[package]
name = "pg_raggraph_core"
version.workspace = true
edition.workspace = true
license.workspace = true
repository.workspace = true

[dependencies]
serde = { workspace = true }
serde_json = { workspace = true }
uuid = { workspace = true }
thiserror = { workspace = true }
tracing = { workspace = true }

[lints.rust]
unsafe_code = "forbid"

[lints.clippy]
pedantic = { level = "warn", priority = -1 }
missing_errors_doc = "allow"
missing_panics_doc = "allow"
module_name_repetitions = "allow"
```

- [ ] **Step 3.2: Write `pg_raggraph_core/src/error.rs`**

```rust
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("namespace `{0}` not found")]
    NamespaceNotFound(String),

    #[error("provider `{0}` not found")]
    ProviderNotFound(String),

    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

pub type CoreResult<T> = Result<T, CoreError>;
```

- [ ] **Step 3.3: Write `pg_raggraph_core/src/types.rs`**

```rust
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NamespaceName(pub String);

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ProviderKind {
    Llm,
    Embedding,
}

impl ProviderKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ProviderKind::Llm => "llm",
            ProviderKind::Embedding => "embedding",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "llm" => Some(ProviderKind::Llm),
            "embedding" => Some(ProviderKind::Embedding),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentId(pub Uuid);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkId(pub Uuid);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityId(pub Uuid);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobId(pub Uuid);
```

- [ ] **Step 3.4: Write `pg_raggraph_core/src/lib.rs`**

```rust
//! pg_raggraph_core — provider-agnostic logic for the pg_raggraph extension.
//!
//! Has no pgrx dependency; testable with plain `cargo test`. Used by both the
//! extension crate (linked into the .so) and the sidecar binary.

pub mod error;
pub mod types;

pub use error::{CoreError, CoreResult};
pub use types::*;
```

- [ ] **Step 3.5: Failing test for `ProviderKind::from_str` round-trip**

Create `<REPO>/pg_raggraph_core/tests/provider_kind.rs`:

```rust
use pg_raggraph_core::ProviderKind;

#[test]
fn provider_kind_roundtrip() {
    for kind in [ProviderKind::Llm, ProviderKind::Embedding] {
        assert_eq!(ProviderKind::from_str(kind.as_str()), Some(kind));
    }
}

#[test]
fn provider_kind_unknown_returns_none() {
    assert_eq!(ProviderKind::from_str("garbage"), None);
}
```

- [ ] **Step 3.6: Run tests**

```bash
cargo test -p pg_raggraph_core
```

Expected: 2 tests pass.

- [ ] **Step 3.7: Commit**

```bash
git add pg_raggraph_core/
git commit -m "feat(core): pg_raggraph_core crate with error + ID types"
```

---

## Task 4: pg_raggraph_sidecar binary placeholder

**Files:**
- Create: `<REPO>/pg_raggraph_sidecar/Cargo.toml`
- Create: `<REPO>/pg_raggraph_sidecar/src/main.rs`

- [ ] **Step 4.1: Write `pg_raggraph_sidecar/Cargo.toml`**

```toml
[package]
name = "pg_raggraph_sidecar"
version.workspace = true
edition.workspace = true
license.workspace = true
repository.workspace = true

[[bin]]
name = "pg-raggraph-sidecar"
path = "src/main.rs"

[dependencies]
pg_raggraph_core = { path = "../pg_raggraph_core" }
tracing = { workspace = true }
```

- [ ] **Step 4.2: Write `pg_raggraph_sidecar/src/main.rs` placeholder**

```rust
//! pg-raggraph-sidecar — standalone binary for cloud-managed PostgreSQL.
//!
//! Real implementation lands in Plan 5. This crate exists in Plan 1 only so
//! the workspace builds end-to-end.

fn main() {
    eprintln!("pg-raggraph-sidecar v{}: not yet implemented (Plan 5)", env!("CARGO_PKG_VERSION"));
    std::process::exit(64); // EX_USAGE
}
```

- [ ] **Step 4.3: Verify workspace builds**

```bash
cd <REPO>
cargo check
cargo build --bin pg-raggraph-sidecar
```

Expected: workspace builds; binary runs and prints the not-yet-implemented message:

```bash
cargo run --bin pg-raggraph-sidecar
# stderr: pg-raggraph-sidecar v0.1.0-alpha.1: not yet implemented (Plan 5)
# exit:   64
```

- [ ] **Step 4.4: Commit**

```bash
git add pg_raggraph_sidecar/
git commit -m "chore(sidecar): placeholder binary (real impl in Plan 5)"
```

---

## Task 5: GitHub Actions CI

**Files:**
- Create: `<REPO>/.github/workflows/ci.yml`

- [ ] **Step 5.1: Write `.github/workflows/ci.yml`**

```yaml
name: ci

on:
  push:
    branches: [main]
  pull_request:

env:
  CARGO_TERM_COLOR: always

jobs:
  fmt-clippy-check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@1.85
        with:
          components: rustfmt, clippy
      - uses: Swatinem/rust-cache@v2

      - name: cargo fmt
        run: cargo fmt --all -- --check

      - name: cargo check (workspace, default features)
        run: cargo check --workspace

      - name: cargo clippy (core + sidecar — non-pgrx crates)
        run: cargo clippy -p pg_raggraph_core -p pg_raggraph_sidecar -- -D warnings

      - name: cargo test (core)
        run: cargo test -p pg_raggraph_core

  pgrx-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@1.85
      - uses: Swatinem/rust-cache@v2

      - name: install postgres + dev headers
        run: |
          sudo apt-get update
          sudo apt-get install -y postgresql-17 postgresql-server-dev-17 \
                                   postgresql-17-pgvector postgresql-contrib-17

      - name: install cargo-pgrx
        run: cargo install --locked cargo-pgrx --version =0.17.0

      - name: pgrx init
        run: cargo pgrx init --pg17 $(which pg_config)

      - name: pgrx test
        run: cargo pgrx test pg17 -p pg_raggraph
```

- [ ] **Step 5.2: Commit**

```bash
git add .github/
git commit -m "ci: workspace fmt/check/clippy + pgrx test on PG17"
```

> NOTE: This CI won't actually run until the repo is pushed to GitHub. Don't push yet — wait until Task 14.

---

## Task 6: Schema — write the initial migration SQL

**Files:**
- Create: `<REPO>/pg_raggraph/sql/000_schema.sql`
- Create: `<REPO>/pg_raggraph/sql/001_tables.sql`
- Create: `<REPO>/pg_raggraph/sql/002_indexes.sql`
- Create: `<REPO>/pg_raggraph/sql/003_migrations_table.sql`

These mirror the schema in design-spec Section 5. Split by phase per `pg_agents` convention.

- [ ] **Step 6.1: Write `pg_raggraph/sql/000_schema.sql`**

```sql
-- 000_schema.sql — bootstrap schema before pgrx-generated functions.
CREATE SCHEMA IF NOT EXISTS pgrg;

-- Required extensions (declared via .control `requires`, but harmless to assert):
DO $$
BEGIN
    PERFORM 1 FROM pg_extension WHERE extname = 'vector';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'pg_raggraph requires the vector extension; CREATE EXTENSION vector first';
    END IF;
    PERFORM 1 FROM pg_extension WHERE extname = 'pg_trgm';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'pg_raggraph requires the pg_trgm extension; CREATE EXTENSION pg_trgm first';
    END IF;
END;
$$;
```

- [ ] **Step 6.2: Write `pg_raggraph/sql/001_tables.sql`**

```sql
-- 001_tables.sql — full schema per design-spec Section 5.
-- Vector dimension is read from the GUC pg_raggraph.embed_dim (default 384).
-- We use a SQL-level GUC lookup to template the dimension.

DO $$
DECLARE
    embed_dim int := current_setting('pg_raggraph.embed_dim', true)::int;
BEGIN
    IF embed_dim IS NULL THEN
        embed_dim := 384;
    END IF;

    EXECUTE format($f$
        CREATE TABLE pgrg.namespaces (
            name text PRIMARY KEY,
            embedding_model text NOT NULL DEFAULT 'bge-small-en-v1.5',
            llm_provider text,
            settings jsonb NOT NULL DEFAULT '{}'::jsonb,
            created_at timestamptz NOT NULL DEFAULT now()
        );

        CREATE TABLE pgrg.documents (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            namespace text NOT NULL REFERENCES pgrg.namespaces(name) ON DELETE CASCADE,
            source text NOT NULL,
            content_hash text NOT NULL UNIQUE,
            title text,
            metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
            ingested_at timestamptz NOT NULL DEFAULT now()
        );

        CREATE TABLE pgrg.chunks (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            namespace text NOT NULL,
            document_id uuid NOT NULL REFERENCES pgrg.documents(id) ON DELETE CASCADE,
            ord int NOT NULL,
            text text NOT NULL,
            token_count int NOT NULL,
            embedding vector(%1$s),
            text_search tsvector GENERATED ALWAYS AS
                (to_tsvector('english', coalesce(text, ''))) STORED,
            metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
            UNIQUE(document_id, ord)
        );

        CREATE TABLE pgrg.entities (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            namespace text NOT NULL,
            name text NOT NULL,
            kind text,
            name_emb vector(%1$s),
            description text,
            properties jsonb NOT NULL DEFAULT '{}'::jsonb,
            degree int NOT NULL DEFAULT 0,
            UNIQUE(namespace, name, kind)
        );

        CREATE TABLE pgrg.relationships (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            namespace text NOT NULL,
            src_id uuid NOT NULL REFERENCES pgrg.entities(id) ON DELETE CASCADE,
            dst_id uuid NOT NULL REFERENCES pgrg.entities(id) ON DELETE CASCADE,
            kind text NOT NULL,
            description text,
            weight float NOT NULL DEFAULT 1.0,
            provenance jsonb NOT NULL DEFAULT '{}'::jsonb,
            UNIQUE(namespace, src_id, dst_id, kind)
        );

        CREATE TABLE pgrg.chunk_entities (
            chunk_id uuid NOT NULL REFERENCES pgrg.chunks(id) ON DELETE CASCADE,
            entity_id uuid NOT NULL REFERENCES pgrg.entities(id) ON DELETE CASCADE,
            confidence float NOT NULL DEFAULT 1.0,
            classification text NOT NULL DEFAULT 'extracted',
            PRIMARY KEY(chunk_id, entity_id)
        );

        CREATE TABLE pgrg.ingest_jobs (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            status text NOT NULL DEFAULT 'queued',
            source text NOT NULL,
            namespace text NOT NULL,
            chunk_strategy text NOT NULL DEFAULT 'auto',
            error text,
            attempt_count int NOT NULL DEFAULT 0,
            payload bytea,
            enqueued_at timestamptz NOT NULL DEFAULT now(),
            started_at timestamptz,
            finished_at timestamptz,
            updated_at timestamptz NOT NULL DEFAULT now()
        );

        CREATE TABLE pgrg.providers (
            name text PRIMARY KEY,
            kind text NOT NULL CHECK (kind IN ('llm', 'embedding')),
            provider text NOT NULL,
            base_url text,
            model text,
            credential text,
            config jsonb NOT NULL DEFAULT '{}'::jsonb,
            created_at timestamptz NOT NULL DEFAULT now()
        );
    $f$, embed_dim);
END;
$$;

-- Default namespace (referenced by ingest defaults).
INSERT INTO pgrg.namespaces (name) VALUES ('default') ON CONFLICT DO NOTHING;
```

- [ ] **Step 6.3: Write `pg_raggraph/sql/002_indexes.sql`**

```sql
-- 002_indexes.sql — indexes per design-spec Section 5.

CREATE INDEX chunks_ns_doc_idx        ON pgrg.chunks (namespace, document_id);
CREATE INDEX chunks_text_search_idx   ON pgrg.chunks USING gin(text_search);
CREATE INDEX chunks_metadata_idx      ON pgrg.chunks USING gin(metadata jsonb_path_ops);
CREATE INDEX chunks_embedding_hnsw    ON pgrg.chunks USING hnsw(embedding vector_cosine_ops);

CREATE INDEX entities_ns_name_idx     ON pgrg.entities (namespace, name);
CREATE INDEX entities_name_trgm_idx   ON pgrg.entities USING gin(name gin_trgm_ops);
CREATE INDEX entities_name_emb_hnsw   ON pgrg.entities USING hnsw(name_emb vector_cosine_ops);

CREATE INDEX relationships_src_idx    ON pgrg.relationships (src_id);
CREATE INDEX relationships_dst_idx    ON pgrg.relationships (dst_id);
CREATE INDEX relationships_ns_kind    ON pgrg.relationships (namespace, kind);

CREATE INDEX chunk_entities_eid_idx   ON pgrg.chunk_entities (entity_id);

CREATE INDEX ingest_jobs_active_idx
    ON pgrg.ingest_jobs (status, enqueued_at)
    WHERE status IN ('queued', 'running');
```

- [ ] **Step 6.4: Write `pg_raggraph/sql/003_migrations_table.sql`**

```sql
-- 003_migrations_table.sql — track schema migrations applied so far.

CREATE TABLE pgrg.migrations (
    version int PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO pgrg.migrations (version) VALUES (1);
```

- [ ] **Step 6.5: Wire SQL files into `pg_raggraph/src/lib.rs`**

Replace the `pg_module_magic!` block with:

```rust
::pgrx::pg_module_magic!(name, version);

::pgrx::extension_sql_file!(
    "../sql/000_schema.sql",
    name = "bootstrap_schema",
    bootstrap
);
::pgrx::extension_sql_file!(
    "../sql/001_tables.sql",
    name = "create_tables",
    requires = ["bootstrap_schema"]
);
::pgrx::extension_sql_file!(
    "../sql/002_indexes.sql",
    name = "create_indexes",
    requires = ["create_tables"]
);
::pgrx::extension_sql_file!(
    "../sql/003_migrations_table.sql",
    name = "migrations_table",
    requires = ["create_tables"]
);
```

- [ ] **Step 6.6: Build to confirm syntax**

```bash
cargo check -p pg_raggraph --features pg17
```

Expected: clean compile.

- [ ] **Step 6.7: Commit**

```bash
git add pg_raggraph/sql/ pg_raggraph/src/lib.rs
git commit -m "feat(schema): initial schema (namespaces, documents, chunks, entities, relationships, jobs, providers)"
```

---

## Task 7: Test that CREATE EXTENSION builds the schema

**Files:**
- Modify: `<REPO>/pg_raggraph/src/lib.rs` (extend the `tests` module)

- [ ] **Step 7.1: Write the failing test**

Replace the `tests` module in `pg_raggraph/src/lib.rs` with:

```rust
#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn extension_loads() {
        assert_eq!(Spi::get_one::<bool>("SELECT true").unwrap(), Some(true));
    }

    #[pg_test]
    fn schema_tables_exist() {
        let tables: Vec<String> = Spi::connect(|client| {
            let rows = client
                .select(
                    "SELECT tablename FROM pg_tables WHERE schemaname = 'pgrg' ORDER BY tablename",
                    None,
                    &[],
                )
                .unwrap();
            rows.map(|r| r.get::<String>(1).unwrap().unwrap()).collect()
        });

        let expected: Vec<&str> = vec![
            "chunk_entities",
            "chunks",
            "documents",
            "entities",
            "ingest_jobs",
            "migrations",
            "namespaces",
            "providers",
            "relationships",
        ];
        let actual: Vec<&str> = tables.iter().map(String::as_str).collect();
        assert_eq!(actual, expected, "expected pgrg.* tables present");
    }

    #[pg_test]
    fn migrations_seeded() {
        let v: Option<i32> =
            Spi::get_one("SELECT max(version) FROM pgrg.migrations").unwrap();
        assert_eq!(v, Some(1));
    }

    #[pg_test]
    fn default_namespace_present() {
        let n: Option<i64> =
            Spi::get_one("SELECT count(*) FROM pgrg.namespaces WHERE name = 'default'").unwrap();
        assert_eq!(n, Some(1));
    }
}
```

- [ ] **Step 7.2: Run pgrx tests**

```bash
cd <REPO>
cargo pgrx test pg17 -p pg_raggraph
```

Expected: all four tests pass. (`extension_loads`, `schema_tables_exist`, `migrations_seeded`, `default_namespace_present`)

- [ ] **Step 7.3: Commit**

```bash
git add pg_raggraph/src/lib.rs
git commit -m "test(schema): assert tables, migrations, default namespace post-CREATE EXTENSION"
```

---

## Task 8: Namespace SQL functions

**Files:**
- Create: `<REPO>/pg_raggraph/src/admin.rs`
- Modify: `<REPO>/pg_raggraph/src/lib.rs` (declare module)

- [ ] **Step 8.1: Failing test for `pgrg.namespace_create`**

Add to the `tests` module in `pg_raggraph/src/lib.rs`:

```rust
    #[pg_test]
    fn namespace_create_inserts_row() {
        Spi::run("SELECT pgrg.namespace_create('test_ns')").unwrap();
        let n: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM pgrg.namespaces WHERE name = 'test_ns'",
        )
        .unwrap();
        assert_eq!(n, Some(1));
    }

    #[pg_test]
    fn namespace_drop_removes_row() {
        Spi::run("SELECT pgrg.namespace_create('drop_me')").unwrap();
        Spi::run("SELECT pgrg.namespace_drop('drop_me', false)").unwrap();
        let n: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM pgrg.namespaces WHERE name = 'drop_me'",
        )
        .unwrap();
        assert_eq!(n, Some(0));
    }
```

- [ ] **Step 8.2: Run tests, observe failure**

```bash
cargo pgrx test pg17 -p pg_raggraph
```

Expected: the two new tests fail (`function pgrg.namespace_create(...) does not exist`).

- [ ] **Step 8.3: Create `pg_raggraph/src/admin.rs`**

```rust
//! Admin SQL functions: namespaces, providers, operational endpoints.

use pgrx::prelude::*;

#[pg_extern(schema = "pgrg")]
fn namespace_create(
    name: &str,
    embedding_model: default!(&str, "'bge-small-en-v1.5'"),
    llm_provider: default!(Option<&str>, "NULL"),
    settings: default!(pgrx::JsonB, "'{}'::jsonb"),
) {
    Spi::connect(|mut client| {
        client
            .update(
                "INSERT INTO pgrg.namespaces (name, embedding_model, llm_provider, settings) \
                 VALUES ($1, $2, $3, $4) \
                 ON CONFLICT (name) DO UPDATE SET \
                     embedding_model = EXCLUDED.embedding_model, \
                     llm_provider    = EXCLUDED.llm_provider, \
                     settings        = EXCLUDED.settings",
                None,
                &[
                    name.into(),
                    embedding_model.into(),
                    llm_provider.into(),
                    settings.into(),
                ],
            )
            .expect("namespace_create insert failed");
    });
}

#[pg_extern(schema = "pgrg")]
fn namespace_drop(name: &str, cascade: default!(bool, "false")) {
    if cascade {
        Spi::connect(|mut client| {
            client
                .update(
                    "DELETE FROM pgrg.namespaces WHERE name = $1",
                    None,
                    &[name.into()],
                )
                .expect("namespace_drop cascade failed");
        });
        return;
    }

    let has_docs: Option<bool> = Spi::get_one_with_args(
        "SELECT EXISTS(SELECT 1 FROM pgrg.documents WHERE namespace = $1)",
        &[name.into()],
    )
    .expect("namespace_drop: existence check failed");

    if has_docs.unwrap_or(false) {
        ereport!(
            ERROR,
            PgSqlErrorCode::ERRCODE_FOREIGN_KEY_VIOLATION,
            format!("namespace `{name}` has documents; pass cascade := true to delete")
        );
    }

    Spi::connect(|mut client| {
        client
            .update(
                "DELETE FROM pgrg.namespaces WHERE name = $1",
                None,
                &[name.into()],
            )
            .expect("namespace_drop failed");
    });
}
```

- [ ] **Step 8.4: Wire `mod admin;` into lib.rs**

In `pg_raggraph/src/lib.rs`, add immediately after `pg_module_magic!`:

```rust
mod admin;
```

- [ ] **Step 8.5: Run tests**

```bash
cargo pgrx test pg17 -p pg_raggraph
```

Expected: all tests pass including `namespace_create_inserts_row` and `namespace_drop_removes_row`.

- [ ] **Step 8.6: Commit**

```bash
git add pg_raggraph/src/admin.rs pg_raggraph/src/lib.rs
git commit -m "feat(admin): pgrg.namespace_create + pgrg.namespace_drop"
```

---

## Task 9: Provider SQL functions (plaintext credentials, redacted list)

**Files:**
- Modify: `<REPO>/pg_raggraph/src/admin.rs`

> NOTE: Credential encryption is intentionally deferred to Plan 4 (where it's exercised by real LLM provider implementations). v1 of provider_create stores credentials plaintext but `provider_list` redacts in the *display* layer. This is documented behavior, not a security oversight.

- [ ] **Step 9.1: Failing tests**

Add to the `tests` module in `pg_raggraph/src/lib.rs`:

```rust
    #[pg_test]
    fn provider_create_then_list() {
        Spi::run(
            "SELECT pgrg.provider_create('p1', 'llm', 'openai', \
                                          'https://api.openai.com', 'gpt-4o-mini', \
                                          'sk-test-secret-1234567890', '{}')",
        )
        .unwrap();

        let json: pgrx::JsonB = Spi::get_one("SELECT pgrg.provider_list()")
            .unwrap()
            .expect("provider_list returned NULL");
        let arr = json.0.as_array().expect("provider_list returns array");
        assert_eq!(arr.len(), 1);
        let obj = &arr[0];
        assert_eq!(obj["name"], "p1");
        assert_eq!(obj["kind"], "llm");
        assert_eq!(obj["provider"], "openai");
        let cred = obj["credential"].as_str().unwrap();
        assert!(cred.starts_with("sk-"), "credential should still show prefix");
        assert!(cred.contains("***"),    "credential should be redacted");
        assert!(!cred.contains("1234567890"), "credential should not include the secret");
    }

    #[pg_test]
    fn provider_drop_removes_row() {
        Spi::run(
            "SELECT pgrg.provider_create('p2', 'embedding', 'openai', \
                                          'https://api.openai.com', 'text-embedding-3-small', \
                                          'sk-also-secret', '{}')",
        )
        .unwrap();
        Spi::run("SELECT pgrg.provider_drop('p2')").unwrap();
        let n: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM pgrg.providers WHERE name = 'p2'",
        )
        .unwrap();
        assert_eq!(n, Some(0));
    }
```

- [ ] **Step 9.2: Run tests, observe failure**

```bash
cargo pgrx test pg17 -p pg_raggraph
```

Expected: failures for `provider_create_then_list` and `provider_drop_removes_row` (functions don't exist).

- [ ] **Step 9.3: Add `redact_credential` helper to `pg_raggraph_core`**

Append to `pg_raggraph_core/src/lib.rs`:

```rust
pub mod credentials {
    /// Redacted form for display: keeps the first 3 chars, replaces the rest with `***`.
    /// Designed to keep the provider prefix (sk-, key-, ...) visible while hiding the secret.
    #[must_use]
    pub fn redact(credential: &str) -> String {
        if credential.len() <= 3 {
            return "***".to_string();
        }
        let (visible, _) = credential.split_at(3);
        format!("{visible}***")
    }
}
```

Add a unit test in `pg_raggraph_core/tests/credentials.rs`:

```rust
use pg_raggraph_core::credentials::redact;

#[test]
fn redact_keeps_prefix() {
    assert_eq!(redact("sk-secret-1234567890"), "sk-***");
}

#[test]
fn redact_short_credential_fully_masked() {
    assert_eq!(redact("ab"), "***");
}
```

Run: `cargo test -p pg_raggraph_core` — expect 4 tests pass (two new + two from Task 3).

- [ ] **Step 9.4: Implement `provider_create`, `provider_drop`, `provider_list`**

Append to `pg_raggraph/src/admin.rs`:

```rust
#[pg_extern(schema = "pgrg")]
fn provider_create(
    name: &str,
    kind: &str,
    provider: &str,
    base_url: Option<&str>,
    model: Option<&str>,
    credential: Option<&str>,
    config: default!(pgrx::JsonB, "'{}'::jsonb"),
) {
    if !matches!(kind, "llm" | "embedding") {
        ereport!(
            ERROR,
            PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
            format!("provider kind must be 'llm' or 'embedding', got `{kind}`")
        );
    }
    Spi::connect(|mut client| {
        client
            .update(
                "INSERT INTO pgrg.providers \
                   (name, kind, provider, base_url, model, credential, config) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7) \
                 ON CONFLICT (name) DO UPDATE SET \
                   kind       = EXCLUDED.kind, \
                   provider   = EXCLUDED.provider, \
                   base_url   = EXCLUDED.base_url, \
                   model      = EXCLUDED.model, \
                   credential = EXCLUDED.credential, \
                   config     = EXCLUDED.config",
                None,
                &[
                    name.into(),
                    kind.into(),
                    provider.into(),
                    base_url.into(),
                    model.into(),
                    credential.into(),
                    config.into(),
                ],
            )
            .expect("provider_create insert failed");
    });
}

#[pg_extern(schema = "pgrg")]
fn provider_drop(name: &str) {
    Spi::connect(|mut client| {
        client
            .update(
                "DELETE FROM pgrg.providers WHERE name = $1",
                None,
                &[name.into()],
            )
            .expect("provider_drop failed");
    });
}

#[pg_extern(schema = "pgrg")]
fn provider_list() -> pgrx::JsonB {
    let rows: Vec<serde_json::Value> = Spi::connect(|client| {
        client
            .select(
                "SELECT name, kind, provider, base_url, model, credential, config \
                 FROM pgrg.providers ORDER BY name",
                None,
                &[],
            )
            .expect("provider_list select")
            .map(|r| {
                let credential_redacted = r
                    .get::<String>(6)
                    .ok()
                    .flatten()
                    .map(|c| pg_raggraph_core::credentials::redact(&c));
                serde_json::json!({
                    "name":       r.get::<String>(1).ok().flatten(),
                    "kind":       r.get::<String>(2).ok().flatten(),
                    "provider":   r.get::<String>(3).ok().flatten(),
                    "base_url":   r.get::<String>(4).ok().flatten(),
                    "model":      r.get::<String>(5).ok().flatten(),
                    "credential": credential_redacted,
                    "config":     r
                        .get::<pgrx::JsonB>(7)
                        .ok()
                        .flatten()
                        .map(|j| j.0)
                        .unwrap_or_else(|| serde_json::json!({})),
                })
            })
            .collect()
    });
    pgrx::JsonB(serde_json::Value::Array(rows))
}
```

- [ ] **Step 9.5: Run tests**

```bash
cargo pgrx test pg17 -p pg_raggraph
```

Expected: `provider_create_then_list` and `provider_drop_removes_row` pass.

- [ ] **Step 9.6: Commit**

```bash
git add pg_raggraph_core/src/lib.rs pg_raggraph_core/tests/credentials.rs pg_raggraph/src/admin.rs pg_raggraph/src/lib.rs
git commit -m "feat(admin): pgrg.provider_create / provider_drop / provider_list with redacted output"
```

---

## Task 10: GUC registration

**Files:**
- Create: `<REPO>/pg_raggraph/src/gucs.rs`
- Modify: `<REPO>/pg_raggraph/src/lib.rs`

- [ ] **Step 10.1: Failing test**

Add to the `tests` module in `pg_raggraph/src/lib.rs`:

```rust
    #[pg_test]
    fn gucs_have_expected_defaults() {
        let workers: Option<i32> =
            Spi::get_one("SELECT current_setting('pg_raggraph.bgw_workers')::int").unwrap();
        assert_eq!(workers, Some(2));

        let dim: Option<i32> =
            Spi::get_one("SELECT current_setting('pg_raggraph.embed_dim')::int").unwrap();
        assert_eq!(dim, Some(384));

        let extract_conc: Option<i32> = Spi::get_one(
            "SELECT current_setting('pg_raggraph.extract_concurrency')::int",
        )
        .unwrap();
        assert_eq!(extract_conc, Some(4));
    }
```

- [ ] **Step 10.2: Run tests, observe failure**

```bash
cargo pgrx test pg17 -p pg_raggraph
```

Expected: failures (`unrecognized configuration parameter`).

- [ ] **Step 10.3: Create `pg_raggraph/src/gucs.rs`**

```rust
//! Operator-level GUCs registered at extension startup.
//!
//! See design-spec Section 7. Per-tenant settings live in `pgrg.providers` /
//! `pgrg.namespaces`, NOT in GUCs.

use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};

pub static BGW_WORKERS: GucSetting<i32> = GucSetting::<i32>::new(2);
pub static EXTRACT_CONCURRENCY: GucSetting<i32> = GucSetting::<i32>::new(4);
pub static EMBED_DIM: GucSetting<i32> = GucSetting::<i32>::new(384);
pub static DEBUG_RETRIEVAL: GucSetting<bool> = GucSetting::<bool>::new(false);
pub static JOB_REAPER_INTERVAL_SECS: GucSetting<i32> = GucSetting::<i32>::new(300);
pub static PARITY_MODE: GucSetting<bool> = GucSetting::<bool>::new(false);
pub static MASTER_KEY_PATH: GucSetting<Option<&'static std::ffi::CStr>> =
    GucSetting::<Option<&'static std::ffi::CStr>>::new(None);
pub static EMBED_MODEL_PATH: GucSetting<Option<&'static std::ffi::CStr>> =
    GucSetting::<Option<&'static std::ffi::CStr>>::new(None);

pub fn register() {
    GucRegistry::define_int_guc(
        "pg_raggraph.bgw_workers",
        "Number of pg_raggraph background worker processes",
        "Set in postgresql.conf and restart. Per design-spec §7.",
        &BGW_WORKERS,
        1,
        16,
        GucContext::Postmaster,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        "pg_raggraph.extract_concurrency",
        "Concurrent LLM extraction calls per worker",
        "",
        &EXTRACT_CONCURRENCY,
        1,
        64,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        "pg_raggraph.embed_dim",
        "DB-wide vector dimension; must be set before CREATE EXTENSION",
        "Default 384 matches BAAI/bge-small-en-v1.5.",
        &EMBED_DIM,
        64,
        4096,
        GucContext::Postmaster,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        "pg_raggraph.debug_retrieval",
        "Populate signals jsonb in pgrg.query results",
        "",
        &DEBUG_RETRIEVAL,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        "pg_raggraph.job_reaper_interval",
        "Seconds between reaper sweeps for stuck running jobs",
        "",
        &JOB_REAPER_INTERVAL_SECS,
        10,
        86_400,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        "pg_raggraph.parity_mode",
        "Use IVFFlat instead of HNSW for deterministic parity benchmarks",
        "",
        &PARITY_MODE,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        "pg_raggraph.master_key_path",
        "File path to AES-GCM master key for credential encryption",
        "",
        &MASTER_KEY_PATH,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        "pg_raggraph.embed_model_path",
        "Override embedding model location for offline installs",
        "",
        &EMBED_MODEL_PATH,
        GucContext::Sighup,
        GucFlags::default(),
    );
}
```

- [ ] **Step 10.4: Wire `_PG_init` to register GUCs**

In `pg_raggraph/src/lib.rs`, add the module declaration and `_PG_init`:

```rust
mod admin;
mod gucs;

#[pg_guard]
pub extern "C" fn _PG_init() {
    gucs::register();
}
```

- [ ] **Step 10.5: Run tests**

```bash
cargo pgrx test pg17 -p pg_raggraph
```

Expected: `gucs_have_expected_defaults` passes.

- [ ] **Step 10.6: Commit**

```bash
git add pg_raggraph/src/gucs.rs pg_raggraph/src/lib.rs
git commit -m "feat(gucs): register pg_raggraph.* configuration parameters with sensible bounds"
```

---

## Task 11: `pgrg.health()` function

**Files:**
- Modify: `<REPO>/pg_raggraph/src/admin.rs`

- [ ] **Step 11.1: Failing test**

Add to the `tests` module in `pg_raggraph/src/lib.rs`:

```rust
    #[pg_test]
    fn health_returns_expected_keys() {
        let json: pgrx::JsonB = Spi::get_one("SELECT pgrg.health()")
            .unwrap()
            .expect("health() returned NULL");
        let obj = json.0.as_object().expect("health() returns object");
        for k in ["version", "schema_version", "queue_depth", "bgw_workers"] {
            assert!(obj.contains_key(k), "health() missing key `{k}`");
        }
        assert_eq!(obj["bgw_workers"], 2);
        assert_eq!(obj["queue_depth"], 0);
        let v = obj["version"].as_str().unwrap();
        assert!(v.starts_with("0.1.0"), "version should start with 0.1.0, got {v}");
    }
```

- [ ] **Step 11.2: Run tests, observe failure**

Expected: `function pgrg.health() does not exist`.

- [ ] **Step 11.3: Implement `health()`**

Append to `pg_raggraph/src/admin.rs`:

```rust
#[pg_extern(schema = "pgrg")]
fn health() -> pgrx::JsonB {
    let queue_depth: Option<i64> = Spi::get_one(
        "SELECT count(*) FROM pgrg.ingest_jobs WHERE status IN ('queued', 'running')",
    )
    .unwrap_or(Some(0));

    let schema_version: Option<i32> =
        Spi::get_one("SELECT max(version) FROM pgrg.migrations").unwrap_or(Some(0));

    let bgw_workers = crate::gucs::BGW_WORKERS.get();

    let body = serde_json::json!({
        "version":        env!("CARGO_PKG_VERSION"),
        "schema_version": schema_version.unwrap_or(0),
        "queue_depth":    queue_depth.unwrap_or(0),
        "bgw_workers":    bgw_workers,
        "model_loaded":   serde_json::Value::Null, // populated in Plan 3
        "last_error":     serde_json::Value::Null, // populated in Plan 3
    });
    pgrx::JsonB(body)
}
```

- [ ] **Step 11.4: Run tests**

```bash
cargo pgrx test pg17 -p pg_raggraph
```

Expected: `health_returns_expected_keys` passes.

- [ ] **Step 11.5: Commit**

```bash
git add pg_raggraph/src/admin.rs pg_raggraph/src/lib.rs
git commit -m "feat(admin): pgrg.health() returns version + queue depth + bgw config"
```

---

## Task 12: `pgrg.status()` function

**Files:**
- Modify: `<REPO>/pg_raggraph/src/admin.rs`

- [ ] **Step 12.1: Failing tests**

Add to the `tests` module in `pg_raggraph/src/lib.rs`:

```rust
    #[pg_test]
    fn status_summary_has_zero_jobs() {
        let json: pgrx::JsonB = Spi::get_one("SELECT pgrg.status()")
            .unwrap()
            .expect("status() returned NULL");
        let obj = json.0.as_object().unwrap();
        assert_eq!(obj["queued"],     0);
        assert_eq!(obj["running"],    0);
        assert_eq!(obj["completed"],  0);
        assert_eq!(obj["failed"],     0);
    }

    #[pg_test]
    fn status_unknown_job_returns_null() {
        let json: Option<pgrx::JsonB> = Spi::get_one(
            "SELECT pgrg.status('00000000-0000-0000-0000-000000000000'::uuid)",
        )
        .unwrap();
        assert!(json.is_none(), "unknown job_id should return NULL");
    }
```

- [ ] **Step 12.2: Run tests, observe failure**

Expected: function does not exist.

- [ ] **Step 12.3: Implement `status()` (overload via `Option<Uuid>`)**

Append to `pg_raggraph/src/admin.rs`:

```rust
#[pg_extern(schema = "pgrg")]
fn status(job_id: default!(Option<pgrx::Uuid>, "NULL")) -> Option<pgrx::JsonB> {
    match job_id {
        None => {
            let counts: Vec<(String, i64)> = Spi::connect(|client| {
                client
                    .select(
                        "SELECT status, count(*)::bigint FROM pgrg.ingest_jobs GROUP BY status",
                        None,
                        &[],
                    )
                    .unwrap()
                    .map(|r| {
                        (
                            r.get::<String>(1).unwrap().unwrap_or_default(),
                            r.get::<i64>(2).unwrap().unwrap_or(0),
                        )
                    })
                    .collect()
            });

            let mut summary = serde_json::json!({
                "queued":     0,
                "running":    0,
                "completed":  0,
                "failed":     0,
            });
            for (k, v) in counts {
                summary[k] = serde_json::Value::from(v);
            }
            Some(pgrx::JsonB(summary))
        }
        Some(uuid) => {
            let row: Option<(String, String, Option<String>)> = Spi::get_three_with_args(
                "SELECT status, source, error FROM pgrg.ingest_jobs WHERE id = $1",
                &[uuid.into()],
            )
            .ok()
            .flatten();

            row.map(|(status, source, error)| {
                pgrx::JsonB(serde_json::json!({
                    "id":     uuid.to_string(),
                    "status": status,
                    "source": source,
                    "error":  error,
                }))
            })
        }
    }
}
```

> NOTE: `Spi::get_three_with_args` returns `Result<Option<(...)>, _>` — the closure above flattens both. If the pgrx 0.17 signature differs in your environment, fall back to `Spi::connect` with an explicit `select` and `next()`.

- [ ] **Step 12.4: Run tests**

```bash
cargo pgrx test pg17 -p pg_raggraph
```

Expected: both new tests pass.

- [ ] **Step 12.5: Commit**

```bash
git add pg_raggraph/src/admin.rs pg_raggraph/src/lib.rs
git commit -m "feat(admin): pgrg.status() — queue summary or single-job detail"
```

---

## Task 13: Delete functions

**Files:**
- Modify: `<REPO>/pg_raggraph/src/admin.rs`

- [ ] **Step 13.1: Failing tests**

Add to the `tests` module in `pg_raggraph/src/lib.rs`:

```rust
    #[pg_test]
    fn delete_document_removes_chunks_via_cascade() {
        Spi::run("SELECT pgrg.namespace_create('del_doc_ns')").unwrap();
        Spi::run(
            "INSERT INTO pgrg.documents (id, namespace, source, content_hash) \
             VALUES ('11111111-1111-1111-1111-111111111111', 'del_doc_ns', 'a.md', 'hash1')",
        )
        .unwrap();
        Spi::run(
            "INSERT INTO pgrg.chunks (namespace, document_id, ord, text, token_count) \
             VALUES ('del_doc_ns', '11111111-1111-1111-1111-111111111111', 0, 'hi', 1)",
        )
        .unwrap();

        Spi::run(
            "SELECT pgrg.delete_document('11111111-1111-1111-1111-111111111111'::uuid)",
        )
        .unwrap();

        let docs: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM pgrg.documents \
             WHERE id = '11111111-1111-1111-1111-111111111111'",
        )
        .unwrap();
        assert_eq!(docs, Some(0));

        let chunks: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM pgrg.chunks WHERE namespace = 'del_doc_ns'",
        )
        .unwrap();
        assert_eq!(chunks, Some(0), "chunks must cascade");
    }

    #[pg_test]
    fn delete_namespace_without_cascade_blocks_when_docs_exist() {
        Spi::run("SELECT pgrg.namespace_create('blocked_ns')").unwrap();
        Spi::run(
            "INSERT INTO pgrg.documents (namespace, source, content_hash) \
             VALUES ('blocked_ns', 'b.md', 'hashB')",
        )
        .unwrap();

        let res = std::panic::catch_unwind(|| {
            Spi::run("SELECT pgrg.namespace_drop('blocked_ns', false)").unwrap();
        });
        assert!(res.is_err(), "namespace_drop without cascade must error");
    }
```

- [ ] **Step 13.2: Run tests, observe failure**

`pgrg.delete_document` does not exist; the cascade test will fail. The `namespace_drop` cascade-blocking test should already pass from Task 8.

- [ ] **Step 13.3: Implement `delete_document`**

Append to `pg_raggraph/src/admin.rs`:

```rust
#[pg_extern(schema = "pgrg")]
fn delete_document(document_id: pgrx::Uuid) -> bool {
    let rows: Option<i64> = Spi::connect(|mut client| {
        let n = client
            .update(
                "DELETE FROM pgrg.documents WHERE id = $1",
                None,
                &[document_id.into()],
            )
            .map(|r| r.len() as i64)
            .ok();
        n
    });
    rows.unwrap_or(0) > 0
}

#[pg_extern(schema = "pgrg")]
fn delete_namespace(name: &str, cascade: default!(bool, "false")) {
    namespace_drop(name, cascade);
}
```

> The `delete_namespace` function is sugar — `namespace_drop` already does the right thing. Keeping a separate name keeps the SQL surface aligned with the spec (Section 6).

- [ ] **Step 13.4: Run tests**

```bash
cargo pgrx test pg17 -p pg_raggraph
```

Expected: both new tests pass.

- [ ] **Step 13.5: Commit**

```bash
git add pg_raggraph/src/admin.rs pg_raggraph/src/lib.rs
git commit -m "feat(admin): pgrg.delete_document + pgrg.delete_namespace (cascade-aware)"
```

---

## Task 14: README + CHANGELOG polish + push to GitHub

**Files:**
- Modify: `<REPO>/README.md`
- Modify: `<REPO>/CHANGELOG.md`

- [ ] **Step 14.1: Expand `README.md`**

```markdown
# pg-raggraph-rs

> PostgreSQL-native GraphRAG. Three SQL statements → grounded answer.

```sql
CREATE EXTENSION pg_raggraph;
SELECT pgrg.ingest('docs/');
SELECT * FROM pgrg.ask('what changed in the auth module?');
```

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
```

- [ ] **Step 14.2: Update `CHANGELOG.md`**

```markdown
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
```

- [ ] **Step 14.3: Final commit**

```bash
git add README.md CHANGELOG.md
git commit -m "docs: README + CHANGELOG for 0.1.0-alpha.1 (Plan 1 complete)"
```

- [ ] **Step 14.4: Create the GitHub repo + push**

> THIS STEP HAS USER-VISIBLE SIDE EFFECTS. Confirm with the user before pushing — they may want to review the local commits first or change the GitHub org / visibility.

```bash
# Once user confirms:
gh repo create yonk-labs/pg-raggraph-rs --public \
    --description "PostgreSQL-native GraphRAG: hybrid retrieval as a Postgres extension" \
    --source <REPO> --remote origin
git push -u origin main
```

CI should run on first push and complete green. If pgrx tests fail in Actions but pass locally, the most common cause is missing `postgresql-17-pgvector` / `postgresql-contrib-17` packages — the workflow at `.github/workflows/ci.yml` installs them, but Ubuntu image versions matter; bump if needed.

---

## Self-Review Checklist (run before declaring Plan 1 complete)

- [ ] All 14 tasks marked complete
- [ ] `cargo fmt --all -- --check` passes
- [ ] `cargo check --workspace` passes
- [ ] `cargo clippy -p pg_raggraph_core -p pg_raggraph_sidecar -- -D warnings` passes
- [ ] `cargo test -p pg_raggraph_core` passes (4 tests: provider kind roundtrip, unknown→None, redact prefix, redact short)
- [ ] `cargo pgrx test pg17 -p pg_raggraph` passes all tests:
  - `extension_loads`
  - `schema_tables_exist`
  - `migrations_seeded`
  - `default_namespace_present`
  - `namespace_create_inserts_row`
  - `namespace_drop_removes_row`
  - `provider_create_then_list`
  - `provider_drop_removes_row`
  - `gucs_have_expected_defaults`
  - `health_returns_expected_keys`
  - `status_summary_has_zero_jobs`
  - `status_unknown_job_returns_null`
  - `delete_document_removes_chunks_via_cascade`
  - `delete_namespace_without_cascade_blocks_when_docs_exist`
- [ ] CI green on first push (Task 14.4)
- [ ] Spec carried forward to `<REPO>/docs/superpowers/specs/`
- [ ] This plan carried forward to `<REPO>/docs/superpowers/plans/`

---

## Spec coverage (Plan 1 → design-spec map)

| Spec section | Plan 1 task |
|---|---|
| §1 Thesis | Repo README (Task 14) |
| §2 Repo + crates | Task 1 (workspace), Task 2 (extension), Task 3 (core), Task 4 (sidecar placeholder) |
| §3 Ingest path | **Out of scope (Plan 3)** |
| §4 Query path | **Out of scope (Plan 2)** |
| §5 Schema | Task 6 (SQL files), Task 7 (test) |
| §6 SQL surface — admin | Tasks 8 (namespace), 9 (provider), 11 (health), 12 (status), 13 (delete) |
| §6 SQL surface — ingest/retrieval | **Out of scope (Plans 2–4)** |
| §7 Configuration — GUCs | Task 10 |
| §7 Configuration — credential encryption | **Deferred to Plan 4** |
| §8 Sidecar mode | Task 4 placeholder; **real impl Plan 5** |
| §9 Testing strategy | Per-task tests; CI in Task 5; full suite in self-review |
| §10 Cross-impl parity | **Out of scope (Plan 6)** |
| §11 Out of scope for v1 | Honored |

## What this plan deliberately does *not* cover

These belong to subsequent plans, not Plan 1:

- The bg worker (`bgw_launcher`/`bgw_worker` modules) — Plan 3
- chunkshop / lede integration — Plans 2 and 3
- Embedding model loading via `hf_cache` — Plan 3
- `pgrg.ingest`, `pgrg.ingest_text`, `pgrg.ingest_bytes` SQL functions — Plan 3
- `pgrg.query`, `pgrg.embed`, `pgrg.ingest_extracted` SQL functions — Plan 2
- `pgrg.ask` SQL function + LLM provider impls — Plan 4
- Credential encryption (AES-GCM with master key) — Plan 4
- Sidecar libpq job loop, embedded SQL bootstrap, HTTP `ask` endpoint — Plan 5
- `bench/parity/` harness — Plan 6
