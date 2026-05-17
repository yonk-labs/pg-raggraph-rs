# Parity bar & methodology (spec §10)

## The bar (machine-decidable; compare.py exit code)
- **Top-k Jaccard ≥ 0.8** — Rust extension vs Python `pg-raggraph`,
  `mode='hybrid'`, `top_k=10`, IVFFlat (`pg_raggraph.parity_mode=true`).
- **Strict equality** on resolution canonical-id assignment (SC-005;
  `cargo test -p pg_raggraph_core --test resolution_parity`, Python mirror).
- **Latency:** Rust p95 ≤ Python p95 on retrieval. v1 = "not worse".

## Why these mitigations

| Non-determinism source | Mitigation |
|---|---|
| LLM extraction variance | Frozen-graph corpus via `pgrg.ingest_extracted` |
| HNSW index-build randomness | `pg_raggraph.parity_mode=true` → IVFFlat |
| Resolver drift | Shared `resolution_constants.yaml` (build-time error on drift) |
| Embedding model drift | SHA256 artifact-identity precheck (`precheck.py`) |
| Chunker drift | chunkshop canonical both sides (frozen corpus side-steps) |

## Pinned (not user-configurable from the harness)

mode=hybrid · top\_k=10 · RRF k=60 · weights {vec:1,bm25:1,graph:1} ·
undirected traversal · IVFFlat.

## What parity does NOT cover

LLM extraction quality · `ask` answer text · load perf · admin-op behavior ·
smart-mode (Python-only). See brief Out of Scope.

## Reading a report

`results/<ts>.json`: `verdict` PASS/FAIL, `aggregate_jaccard`, per-query
`jaccard` + `drift` (SC-017) on misses, `p50/p95_latency_ms`, `ablation`
(SC-016, when `--mode-ablation`).
