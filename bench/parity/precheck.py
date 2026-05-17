#!/usr/bin/env python3
"""SC-008 artifact-identity guard. Asserts Python FastEmbedProvider's
model.onnx and chunkshop's hf_cache model.onnx are byte-identical (SHA256).
Does NOT run inference — frozen embeddings are already in the fixtures
(GATE-C / spec §10). Guards against silent model drift. Exit 0 if identical,
non-zero with 'SHA256 mismatch' message on stderr otherwise.
"""
import argparse, hashlib, sys


def sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for blk in iter(lambda: f.read(1 << 20), b""):
            h.update(blk)
    return h.hexdigest()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fastembed", required=True, help="FastEmbedProvider model.onnx")
    ap.add_argument("--chunkshop", required=True, help="chunkshop hf_cache model.onnx")
    a = ap.parse_args()
    fe, cs = sha256(a.fastembed), sha256(a.chunkshop)
    if fe != cs:
        print(f"SHA256 mismatch: fastembed={fe[:12]}… chunkshop={cs[:12]}…",
              file=sys.stderr)
        return 1
    print(f"OK identical ONNX artifact sha256={fe[:12]}…")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
