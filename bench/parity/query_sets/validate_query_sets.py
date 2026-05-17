#!/usr/bin/env python3
"""SC-013: assert each query_sets/<tier>.yaml has >=5 of each category and a
non-empty rationale per query. Exit non-zero on violation (CI consumes this).
"""
import sys, glob
try:
    import yaml
except ImportError:
    print("PyYAML required for query-set validation", file=sys.stderr)
    sys.exit(2)

CATS = {"entity_anchored", "keyword", "semantic", "hybrid_favored"}
bad = False
for path in sorted(glob.glob("bench/parity/query_sets/*.yaml")):
    doc = yaml.safe_load(open(path))
    counts = {c: 0 for c in CATS}
    for qr in doc["queries"]:
        assert qr["category"] in CATS, f"{path}: bad category {qr['category']}"
        assert qr.get("rationale", "").strip(), f"{path}: missing rationale: {qr['q']}"
        counts[qr["category"]] += 1
    for c, n in counts.items():
        if n < 5:
            print(f"FAIL {path}: category {c} has {n} (<5)", file=sys.stderr)
            bad = True
    print(f"OK {path}: {counts}")
sys.exit(1 if bad else 0)
