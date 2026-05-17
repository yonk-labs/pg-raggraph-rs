# Fixture generation (honest scope)

**Committed in-repo:** small tier only (`corpus/small/docs.jsonl`,
`extracted/small.jsonl`). This is the PR-gating tier (SC-007).

**Generated, NOT committed (too large for the repo):** medium (1k docs),
large (10k docs). Regenerate deterministically (run from `bench/parity/`):

```bash
for T in medium large; do
  python3 corpus/gen_corpus.py --tier $T --out corpus/$T/docs.jsonl
  python3 extracted/gen_extracted.py --tier $T \
    --corpus corpus/$T/docs.jsonl --out extracted/$T.jsonl
done
```

CI regenerates medium on `main` push and large on tags (see ci/parity.yml).
Seeds are fixed (`pgrg-parity-<tier>-v1`) so regeneration is byte-stable —
the medium/large fixtures are reproducible, just not stored.

**Provenance (SC-002 / DC-001):** every `extracted/*.jsonl` LINE 1 is a
`{"kind":"_header",...}` JSON recording extraction_model + model_version +
temperature + seed. Frozen-graph corpus => no LLM in the parity loop
(spec §10 lines 439-446).

**⚠️ Strip-before-load contract:** `pgrg.ingest_extracted` hard-errors on
any non-record line, so the header line MUST be stripped (`tail -n +2`)
before the file is passed to the loader. `compare.py` does this; the
SC-002 load test (`sc002_parity_small_fixture_loads_via_real_loader`)
verifies a header-stripped copy of the real shipped fixture loads cleanly.
