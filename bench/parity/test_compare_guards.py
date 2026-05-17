"""Test suite for compare.py guards: SC-009, SC-016, SC-017.

SC-009: Argument validation guards
  - smart-mode rejected (exit 3)
  - non-hybrid mode rejected unless --mode-ablation (exit 3)

SC-016: Ablation report shape and lanes
  - --mode-ablation produces report["ablation"] with {vector, bm25, graph} keys

SC-017: Drift report on low Jaccard
  - --inject-regression forces Python side to empty, triggers drift detection
"""
import json, os, subprocess, sys, pathlib

ROOT = pathlib.Path(__file__).parent


def _pg_raggraph_available():
    """Return True if pg_raggraph extension is reachable and installed."""
    dsn = os.environ.get(
        "PGRG_RUST_DSN",
        "postgresql://postgres:postgres@localhost:5443/pgrg_sidecar_test",
    )
    r = subprocess.run(
        ["psql", dsn, "-tAX", "-c",
         "SELECT 1 FROM pg_extension WHERE extname='pg_raggraph'"],
        capture_output=True, text=True,
    )
    return r.returncode == 0 and "1" in r.stdout


# ---- SC-009: argument-rejection guards (no DB needed) ----


def test_sc009_smart_mode_rejected():
    """Smart-mode must be rejected with exit 3 (spec §10 pinned contract)."""
    r = subprocess.run(
        [sys.executable, str(ROOT / "compare.py"),
         "--tier", "small",
         "--queries", str(ROOT / "query_sets/small.yaml"),
         "--mode", "smart"],
        capture_output=True, text=True,
    )
    assert r.returncode == 3, f"expected exit 3, got {r.returncode}"
    assert "smart-mode is not part of parity surface (spec §10)" in r.stderr


def test_sc009_nonhybrid_mode_rejected():
    """Non-hybrid modes (vector, bm25, graph) rejected unless --mode-ablation."""
    for mode in ("vector", "bm25", "graph"):
        r = subprocess.run(
            [sys.executable, str(ROOT / "compare.py"),
             "--tier", "small",
             "--queries", str(ROOT / "query_sets/small.yaml"),
             "--mode", mode],
            capture_output=True, text=True,
        )
        assert r.returncode == 3, (
            f"mode={mode!r} without --mode-ablation should exit 3, got {r.returncode}"
        )
        assert "pins mode='hybrid'" in r.stderr, (
            f"mode={mode!r}: expected 'pins mode' error in stderr"
        )


def test_sc009_nonhybrid_allowed_with_ablation():
    """Non-hybrid modes allowed WITH --mode-ablation (no exit 3)."""
    import pytest
    if not _pg_raggraph_available():
        pytest.skip("pg_raggraph extension not available — runs in CI with pgrx-provisioned DB")

    out = pathlib.Path("/tmp") / "test_ablation_nonhybrid.json"
    r = subprocess.run(
        [sys.executable, str(ROOT / "compare.py"),
         "--tier", "small",
         "--queries", str(ROOT / "query_sets/small.yaml"),
         "--mode-ablation", "--mode", "vector",
         "--report", str(out)],
        capture_output=True, text=True,
    )
    # --mode-ablation takes precedence; specific --mode is ignored in ablation logic
    # (compare.py ablation loop iterates all three lanes).
    # The command should succeed (exit 0 via the ablation branch).
    assert r.returncode == 0, f"exit {r.returncode}: {r.stderr}"


# ---- SC-016: ablation report shape (DB-gated) ----


def test_sc016_mode_ablation_report_shape(tmp_path):
    """Ablation mode produces report with ablation key containing vector/bm25/graph lanes."""
    import pytest
    if not _pg_raggraph_available():
        pytest.skip("pg_raggraph extension not available — runs in CI with pgrx-provisioned DB")

    out = tmp_path / "abl.json"
    r = subprocess.run(
        [sys.executable, str(ROOT / "compare.py"),
         "--tier", "small",
         "--queries", str(ROOT / "query_sets/small.yaml"),
         "--mode-ablation",
         "--report", str(out)],
        capture_output=True, text=True,
    )
    assert r.returncode == 0, f"ablation run failed: {r.stderr}"

    rep = json.loads(out.read_text())

    # Check top-level structure
    assert "ablation" in rep, "report missing 'ablation' key"
    assert "config" in rep, "report missing 'config' key"
    assert "verdict" in rep, "report missing 'verdict' key"

    # Check ablation lanes
    assert set(rep["ablation"].keys()) == {"vector", "bm25", "graph"}, (
        f"ablation lanes should be {{vector, bm25, graph}}, got {set(rep['ablation'].keys())}"
    )

    # Each lane should have per-query results
    for lane in ("vector", "bm25", "graph"):
        lane_results = rep["ablation"][lane]
        assert isinstance(lane_results, list), f"ablation[{lane!r}] should be a list"
        assert len(lane_results) > 0, f"ablation[{lane!r}] is empty"

        # Each query result should have jaccard, rust_ms, python_ms
        for qres in lane_results:
            assert "q" in qres, f"ablation[{lane!r}]: missing 'q' in query result"
            assert "jaccard" in qres, f"ablation[{lane!r}]: missing 'jaccard'"
            assert "rust_ms" in qres, f"ablation[{lane!r}]: missing 'rust_ms'"
            assert "python_ms" in qres, f"ablation[{lane!r}]: missing 'python_ms'"
            assert isinstance(qres["jaccard"], (int, float)), (
                f"ablation[{lane!r}][...]['jaccard'] should be numeric"
            )

    # Config should show ablation mode
    assert rep["config"]["mode"] == "ablation", (
        f"config['mode'] should be 'ablation', got {rep['config'].get('mode')}"
    )
    assert rep["verdict"] == "ABLATION", (
        f"verdict should be 'ABLATION' in ablation mode, got {rep['verdict']}"
    )


def test_sc016_ablation_latency_recorded(tmp_path):
    """Ablation report includes per-query latency for all lanes."""
    import pytest
    if not _pg_raggraph_available():
        pytest.skip("pg_raggraph extension not available — runs in CI with pgrx-provisioned DB")

    out = tmp_path / "abl.json"
    r = subprocess.run(
        [sys.executable, str(ROOT / "compare.py"),
         "--tier", "small",
         "--queries", str(ROOT / "query_sets/small.yaml"),
         "--mode-ablation",
         "--report", str(out)],
        capture_output=True, text=True,
    )
    assert r.returncode == 0

    rep = json.loads(out.read_text())

    # Spot-check: first query of each lane should have latency > 0
    for lane in ("vector", "bm25", "graph"):
        first_q = rep["ablation"][lane][0]
        assert first_q["rust_ms"] >= 0, f"ablation[{lane!r}][0] rust_ms not recorded"
        assert first_q["python_ms"] >= 0, f"ablation[{lane!r}][0] python_ms not recorded"


# ---- SC-017: drift report on low Jaccard (DB-gated) ----


def test_sc017_drift_report_on_inject_regression(tmp_path):
    """--inject-regression forces Python to return empty, triggering drift detection."""
    import pytest
    if not _pg_raggraph_available():
        pytest.skip("pg_raggraph extension not available — runs in CI with pgrx-provisioned DB")

    out = tmp_path / "drift.json"
    r = subprocess.run(
        [sys.executable, str(ROOT / "compare.py"),
         "--tier", "small",
         "--queries", str(ROOT / "query_sets/small.yaml"),
         "--inject-regression",
         "--report", str(out)],
        capture_output=True, text=True,
    )
    # With inject-regression, Jaccard < 0.8 overall, so exit 1 (FAIL)
    assert r.returncode == 1, f"expected exit 1 (FAIL) with --inject-regression, got {r.returncode}"

    rep = json.loads(out.read_text())

    # Top-level verdict should be FAIL
    assert rep["verdict"] == "FAIL", (
        f"verdict should be FAIL with --inject-regression, got {rep['verdict']}"
    )

    # Should have per-query results
    assert "per_query" in rep, "missing 'per_query' key"
    assert len(rep["per_query"]) > 0, "per_query is empty"

    # At least some queries should have drift detected (all, actually, since Python is empty)
    has_drift = any("drift" in qr for qr in rep["per_query"])
    assert has_drift, "no queries have drift detected despite inject-regression"


def test_sc017_drift_detail_structure(tmp_path):
    """Drift detail has only_rust and only_python lists."""
    import pytest
    if not _pg_raggraph_available():
        pytest.skip("pg_raggraph extension not available — runs in CI with pgrx-provisioned DB")

    out = tmp_path / "drift.json"
    r = subprocess.run(
        [sys.executable, str(ROOT / "compare.py"),
         "--tier", "small",
         "--queries", str(ROOT / "query_sets/small.yaml"),
         "--inject-regression",
         "--report", str(out)],
        capture_output=True, text=True,
    )
    assert r.returncode == 1

    rep = json.loads(out.read_text())

    # All queries should have drift (since Python returns empty)
    for qr in rep["per_query"]:
        assert "drift" in qr, f"query {qr['q']!r} should have drift with inject-regression"
        drift = qr["drift"]
        assert "only_rust" in drift, "drift missing 'only_rust'"
        assert "only_python" in drift, "drift missing 'only_python'"
        assert isinstance(drift["only_rust"], list), "only_rust should be a list"
        assert isinstance(drift["only_python"], list), "only_python should be a list"

        # With inject-regression, Python returns [], so all Rust results are "only_rust"
        assert len(drift["only_rust"]) > 0, (
            f"query {qr['q']!r}: only_rust should not be empty with inject-regression"
        )
        assert len(drift["only_python"]) == 0, (
            f"query {qr['q']!r}: only_python should be empty (Python forced to [])"
        )


def test_sc017_jaccard_below_bar(tmp_path):
    """--inject-regression causes aggregate_jaccard < 0.8 (JACCARD_BAR)."""
    import pytest
    if not _pg_raggraph_available():
        pytest.skip("pg_raggraph extension not available — runs in CI with pgrx-provisioned DB")

    out = tmp_path / "drift.json"
    r = subprocess.run(
        [sys.executable, str(ROOT / "compare.py"),
         "--tier", "small",
         "--queries", str(ROOT / "query_sets/small.yaml"),
         "--inject-regression",
         "--report", str(out)],
        capture_output=True, text=True,
    )

    rep = json.loads(out.read_text())

    # aggregate_jaccard should be present and < 0.8 (JACCARD_BAR)
    assert "aggregate_jaccard" in rep, "missing aggregate_jaccard"
    agg = rep["aggregate_jaccard"]
    assert isinstance(agg, (int, float)), "aggregate_jaccard should be numeric"
    assert agg < 0.8, (
        f"aggregate_jaccard should be < 0.8 with inject-regression, got {agg}"
    )


# ---- Smoke test: baseline pass case (DB-gated) ----


def test_baseline_hybrid_mode_passes(tmp_path):
    """Baseline: normal hybrid run (--python self) should pass."""
    import pytest
    if not _pg_raggraph_available():
        pytest.skip("pg_raggraph extension not available — runs in CI with pgrx-provisioned DB")

    out = tmp_path / "baseline.json"
    r = subprocess.run(
        [sys.executable, str(ROOT / "compare.py"),
         "--tier", "small",
         "--queries", str(ROOT / "query_sets/small.yaml"),
         "--mode", "hybrid",
         "--python", "self",
         "--report", str(out)],
        capture_output=True, text=True,
    )
    assert r.returncode == 0, f"baseline hybrid run failed: {r.stderr}"

    rep = json.loads(out.read_text())
    assert rep["verdict"] == "PASS", f"baseline verdict should be PASS, got {rep['verdict']}"
    assert rep["aggregate_jaccard"] == 1.0, (
        f"self-parity should give Jaccard=1.0, got {rep['aggregate_jaccard']}"
    )
