import json, subprocess, sys, pathlib, os

ROOT = pathlib.Path(__file__).parent


def test_sc014_exit_code_and_report_shape(tmp_path):
    # Self-parity: --python self runs the Rust side twice. Jaccard must be 1.0.
    # Requires a pg_raggraph-enabled PostgreSQL. If not available, the test is
    # skipped with a clear message (honest partial per Plan 6 T10 honesty note).
    import pytest
    dsn = os.environ.get(
        "PGRG_RUST_DSN",
        "postgresql://postgres:postgres@localhost:5443/pgrg_sidecar_test",
    )
    # Quick availability check — skip if pg_raggraph extension is not present.
    r = subprocess.run(
        ["psql", dsn, "-tAX", "-c",
         "SELECT 1 FROM pg_extension WHERE extname='pg_raggraph'"],
        capture_output=True, text=True,
    )
    if r.returncode != 0 or "1" not in r.stdout:
        pytest.skip("pg_raggraph extension not available at PGRG_RUST_DSN — honest partial; runs in CI with pgrx-provisioned DB")

    out = tmp_path / "r.json"
    r = subprocess.run(
        [sys.executable, str(ROOT / "compare.py"),
         "--tier", "small",
         "--queries", str(ROOT / "query_sets/small.yaml"),
         "--top-k", "10", "--mode", "hybrid", "--python", "self",
         "--report", str(out)],
        capture_output=True, text=True, env={**os.environ},
    )
    assert r.returncode == 0, r.stderr
    rep = json.loads(out.read_text())
    for k in ("config", "per_query", "aggregate_jaccard",
              "p50_latency_ms", "p95_latency_ms", "verdict"):
        assert k in rep, f"missing key: {k}"
    assert rep["config"]["mode"] == "hybrid"
    assert rep["config"]["rrf_k"] == 60
    assert rep["aggregate_jaccard"] == 1.0
    assert rep["verdict"] == "PASS"
