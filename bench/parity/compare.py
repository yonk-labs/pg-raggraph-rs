#!/usr/bin/env python3
"""Cross-impl parity driver (spec §10, Plan 6).

Loads the frozen-graph extracted fixture into BOTH stacks via ingest_extracted,
runs the query set through each at PINNED params (mode=hybrid, top_k=10,
RRF k=60, equal weights, undirected), and emits a machine-decidable verdict.

Rust side: psql against a Postgres with pg_raggraph + parity_mode=true.
Python side: pinned `pg-raggraph` library (sibling repo) with same fixture.
  --python self: runs the Rust side twice (CI smoke; Jaccard==1.0).

Exit 0 IFF aggregate Jaccard >= 0.8 AND Rust p95 <= Python p95 (SC-014).
Non-zero otherwise. No human in the loop.

Strip-before-load contract: extracted/*.jsonl line 1 is a _header record;
this script strips it before calling pgrg.ingest_extracted.
"""
import argparse, datetime, json, os, pathlib, statistics, subprocess
import sys, tempfile, time

RRF_K = 60
EQUAL_WEIGHTS = {"vec": 1, "bm25": 1, "graph": 1}
JACCARD_BAR = 0.8  # GATE-C: contract, never lowered here
ROOT = pathlib.Path(__file__).parent


# ---- spec §10 pinned-contract guard -----------------------------------------
def _assert_pinned(mode, ablation):
    if mode == "smart":
        sys.stderr.write("smart-mode is not part of parity surface (spec §10)\n")
        raise SystemExit(3)
    if mode != "hybrid" and not ablation:
        sys.stderr.write(
            f"mode={mode!r} not allowed; parity pins mode='hybrid' "
            "(use --mode-ablation for per-lane diagnostics)\n"
        )
        raise SystemExit(3)


# ---- Rust stack via psql -----------------------------------------------------
def _psql(dsn, sql):
    cmd = ["psql", dsn, "-tAX", "-v", "ON_ERROR_STOP=1", "-c", sql]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"psql failed: {r.stderr.strip()}")
    return [x for x in r.stdout.splitlines() if x]


def rust_setup(dsn, fixture):
    """Load fixture into Rust stack, stripping the _header line first."""
    raw = pathlib.Path(fixture).read_text(encoding="utf-8").splitlines()
    stripped = "\n".join(raw[1:]) + "\n"
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".jsonl", delete=False, encoding="utf-8", newline="\n"
    ) as tmp:
        tmp.write(stripped)
        stripped_path = tmp.name
    try:
        _psql(dsn, "SET pg_raggraph.parity_mode = true")
        _psql(dsn, "SELECT pgrg.namespace_create('parity')")
        _psql(dsn, f"SELECT pgrg.ingest_extracted('{stripped_path}', 'parity')")
    finally:
        os.unlink(stripped_path)


def rust_query(dsn, q, top_k, mode="hybrid"):
    safe = q.replace("'", "''")
    sql = (
        f"SELECT chunk_id FROM pgrg.query('{safe}', NULL, {top_k}, "
        f"'parity', 1, NULL, '{mode}')"
    )
    t0 = time.perf_counter()
    ids = _psql(dsn, sql)
    return ids[:top_k], (time.perf_counter() - t0) * 1000.0


# ---- Python stack ------------------------------------------------------------
def python_query(adapter, dsn, q, top_k, fixture, mode="hybrid"):
    if adapter == "self":
        return rust_query(dsn, q, top_k, mode)
    # Real adapter: pinned pg-raggraph library. Imported lazily so --python self
    # (smoke path) needs no sibling install.
    from pg_raggraph_parity import run_query  # thin shim in sibling lib
    t0 = time.perf_counter()
    ids = run_query(
        dsn=dsn, q=q, top_k=top_k, mode="hybrid",
        rrf_k=RRF_K, weights=EQUAL_WEIGHTS, namespace="parity",
    )
    return list(map(str, ids))[:top_k], (time.perf_counter() - t0) * 1000.0


# ---- metrics -----------------------------------------------------------------
def jaccard(a, b):
    sa, sb = set(a), set(b)
    return 1.0 if not sa and not sb else len(sa & sb) / len(sa | sb)


def _pct(values, p):
    if not values:
        return 0.0
    v = sorted(values)
    return v[min(len(v) - 1, int(len(v) * p))]


# ---- main --------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tier", required=True)
    ap.add_argument("--queries", required=True)
    ap.add_argument("--top-k", type=int, default=10)
    ap.add_argument("--mode", default="hybrid")
    ap.add_argument(
        "--python", default="self",
        help="'self' (smoke; Rust side x2) or 'lib' (pinned pg-raggraph)",
    )
    ap.add_argument(
        "--rust-dsn",
        default=os.environ.get(
            "PGRG_RUST_DSN",
            "postgresql://postgres:postgres@localhost:5443/pgrg_sidecar_test",
        ),
    )
    ap.add_argument(
        "--python-dsn",
        default=os.environ.get(
            "PGRG_PY_DSN",
            "postgresql://postgres:postgres@localhost:5443/pgrg_sidecar_test",
        ),
    )
    ap.add_argument("--report", default=None)
    ap.add_argument("--mode-ablation", action="store_true")
    ap.add_argument(
        "--inject-regression", action="store_true",
        help="(testing only) force Python side to return empty results for SC-017",
    )
    args = ap.parse_args()

    _assert_pinned(args.mode, args.mode_ablation)

    import yaml
    with open(args.queries, encoding="utf-8") as f:
        qset = yaml.safe_load(f)["queries"]

    fixture = str(ROOT / f"extracted/{args.tier}.jsonl")
    rust_setup(args.rust_dsn, fixture)
    if args.python not in ("self",):
        from pg_raggraph_parity import setup as py_setup
        py_setup(
            dsn=args.python_dsn, fixture=fixture, namespace="parity",
            parity_mode=True,
        )

    # ---- ablation mode -------------------------------------------------------
    if args.mode_ablation:
        ablation = {}
        for lane in ("vector", "bm25", "graph"):
            lane_res = []
            for qr in qset:
                r_ids, r_ms = rust_query(args.rust_dsn, qr["q"], args.top_k, lane)
                p_ids, p_ms = python_query(
                    args.python, args.python_dsn, qr["q"], args.top_k, fixture, lane
                )
                lane_res.append({
                    "q": qr["q"],
                    "jaccard": jaccard(r_ids, p_ids),
                    "rust_ms": r_ms,
                    "python_ms": p_ms,
                })
            ablation[lane] = lane_res
        report = {
            "config": {"tier": args.tier, "mode": "ablation", "top_k": args.top_k,
                       "rrf_k": RRF_K, "weights": EQUAL_WEIGHTS, "parity_mode": True,
                       "python_adapter": args.python},
            "ablation": ablation,
            "verdict": "ABLATION",
        }
        ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        out = args.report or str(ROOT / f"results/{ts}-ablation.json")
        pathlib.Path(out).parent.mkdir(parents=True, exist_ok=True)
        json.dump(report, open(out, "w", encoding="utf-8"), indent=2)
        print(f"ablation report={out}")
        raise SystemExit(0)

    # ---- main parity run -----------------------------------------------------
    per_query, r_lat, p_lat = [], [], []
    for qr in qset:
        q = qr["q"]
        r_ids, r_ms = rust_query(args.rust_dsn, q, args.top_k)
        if args.inject_regression:
            p_ids, p_ms = [], 0.0  # SC-017 test hook
        else:
            p_ids, p_ms = python_query(
                args.python, args.python_dsn, q, args.top_k, fixture
            )
        j = jaccard(r_ids, p_ids)
        r_lat.append(r_ms)
        p_lat.append(p_ms)
        rec = {
            "q": q, "category": qr["category"],
            "jaccard": j, "rust_ms": r_ms, "python_ms": p_ms,
        }
        if j < JACCARD_BAR:
            rec["drift"] = {
                "only_rust": sorted(set(r_ids) - set(p_ids)),
                "only_python": sorted(set(p_ids) - set(r_ids)),
            }
        per_query.append(rec)

    agg = statistics.mean(qr["jaccard"] for qr in per_query)
    r_p95, p_p95 = _pct(r_lat, 0.95), _pct(p_lat, 0.95)
    latency_ok = (r_p95 <= p_p95) if args.python != "self" else True
    passed = agg >= JACCARD_BAR and latency_ok

    report = {
        "config": {
            "tier": args.tier, "mode": "hybrid", "top_k": args.top_k,
            "rrf_k": RRF_K, "weights": EQUAL_WEIGHTS, "parity_mode": True,
            "python_adapter": args.python,
        },
        "per_query": per_query,
        "aggregate_jaccard": round(agg, 6),
        "p50_latency_ms": {
            "rust": round(_pct(r_lat, 0.5), 3),
            "python": round(_pct(p_lat, 0.5), 3),
        },
        "p95_latency_ms": {
            "rust": round(r_p95, 3),
            "python": round(p_p95, 3),
        },
        "jaccard_bar": JACCARD_BAR,
        "verdict": "PASS" if passed else "FAIL",
    }
    ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    out = args.report or str(ROOT / f"results/{ts}.json")
    pathlib.Path(out).parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2)
    print(f"verdict={report['verdict']} jaccard={agg:.4f} report={out}")
    raise SystemExit(0 if passed else 1)


if __name__ == "__main__":
    main()
