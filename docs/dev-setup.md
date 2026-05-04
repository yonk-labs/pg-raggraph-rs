# Dev setup

Local environment for `pg-raggraph-rs` development. Captures load-bearing deviations from `docs/superpowers/plans/2026-05-03-pg-raggraph-rs-foundation.md` discovered during Plan 1 execution.

## TL;DR — the working dev loop

```bash
cd /home/yonk/yonk-tools/pg-raggraph-extension
cargo pgrx test pg18 --package pg_raggraph --features "pg18 pg_test" --no-default-features
```

No sudo. Uses pgrx-private PG 18 under `~/.pgrx/18.X/`. Output ends with `test result: ok. N passed`.

## Toolchain

- **Rust 1.89+** (pinned in `rust-toolchain.toml`). pgrx 0.17.0 uses `NonNull::from_mut`, stabilized in 1.89.
- **`cargo-pgrx 0.17.0`**, exact-pin: `cargo install --locked cargo-pgrx --version =0.17.0`.

## Apt prereqs (one-time)

PG 18 is built from source by `cargo pgrx init`. The configure step needs:

```bash
sudo apt-get install -y libicu-dev bison flex libreadline-dev
```

(`gcc`, `make`, `perl`, `zlib1g-dev` were already present on the dev box. If a future contributor hits a missing-build-dep error, those four are the most likely culprits, but `libssl-dev` and `libxml2-dev` are remote possibilities.)

## pgrx setup (one-time, ~12 min)

We use **Model Y'**: pgrx downloads + builds its own PG 18 into `~/.pgrx/18.X/`, fully isolated from any system PostgreSQL. Avoids the sudo/path pollution that comes with pointing pgrx at system PG.

```bash
cargo pgrx init --pg18 download
```

Verify after:

```bash
cat ~/.pgrx/config.toml          # pg18 line should point under ~/.pgrx/, NOT /usr/bin/
cargo pgrx info pg-config pg18   # should print ~/.pgrx/18.X/pgrx-install/bin/pg_config
```

## pgvector + pg_trgm

`pg_trgm` ships with PG contrib — no extra step.

`pgvector` must be built against the pgrx-private PG. **Use v0.8.1, not v0.8.0** — v0.8.0 fails on PG 18 (`vacuum_delay_point()` API changed; v0.8.1 has the compat shim).

```bash
PGRX_PG_CONFIG=$(cargo pgrx info pg-config pg18)
git clone --branch v0.8.1 --depth 1 https://github.com/pgvector/pgvector.git /tmp/pgvector
cd /tmp/pgvector
PG_CONFIG="$PGRX_PG_CONFIG" make
PG_CONFIG="$PGRX_PG_CONFIG" make install
```

No sudo — install dir is under `~/.pgrx/`.

## CI vs local target

- **CI** (`.github/workflows/ci.yml`): runs `cargo pgrx test pg17 -p pg_raggraph` on Ubuntu Linux with PGDG packages. CI is the canonical pg17 validation.
- **Local dev** (this machine): pg18 because that's what's available. Both targets are declared as cargo features (`pg17`, `pg18`); the codebase supports either.

If you're a contributor on a system with pg17 available, `cargo pgrx test pg17 -p pg_raggraph` should also work — substitute `pg17` for `pg18` everywhere above.

## Quirks worth knowing

**`pg_catalog.pg_tables.tablename` is OID `name` (19), not `text` (25).** pgrx 0.17's `String` extractor only accepts `text`. Cast in SQL when iterating system catalog columns of type `name`:

```rust
"SELECT tablename::text FROM pg_tables WHERE schemaname = 'pgrg'"
```

Without the cast: `DatumError(IncompatibleTypes { rust_type: "alloc::string::String", rust_oid: 25, datum_type: "name", datum_oid: 19 })`.

**Don't manually `CREATE SCHEMA pgrg` in bootstrap SQL.** The `.control` file's `schema = 'pgrg'` directive tells PostgreSQL to create the schema **and register it as a member of the extension** automatically. Manual `CREATE SCHEMA IF NOT EXISTS pgrg` produces a non-extension-member schema, which PG 18 rejects with `schema pgrg is not a member of extension "pg_raggraph"`. PG 17 was apparently more permissive; PG 18 enforces strictly. See `pg_raggraph/sql/000_schema.sql` for the working pattern (extension assertions only, no `CREATE SCHEMA`).

**`pg_test::postgresql_conf_options()` lists `pg_raggraph.bgw_workers=2`.** That GUC isn't registered until Task 10. PG 18 starts cleanly with the unknown GUC line in `postgresql.conf` (treats it as a custom placeholder). Task 10 will register it properly. If you somehow trip a startup failure on this line, drop it from the vec — Task 10 re-adds.

## Reference precedents

- **`pg_agents`** at `/home/yonk/yonk-tools/pg-agent/pg_agents/` — same pgrx 0.17 patterns we're mirroring (workspace, control file, bootstrap SQL, `extension_sql_file!` macro, GUC registration). Useful when you're unsure how an idiom should look.
- **Plan 1** at `docs/superpowers/plans/2026-05-03-pg-raggraph-rs-foundation.md` — task-by-task implementation guide. Each task has full code, test code, commit messages.
- **Spec** at `docs/superpowers/specs/2026-05-03-pg-raggraph-rs-extension-design.md` — design document. Section numbers (e.g. §5 schema, §6 SQL surface) are referenced from commit messages and plan tasks.
