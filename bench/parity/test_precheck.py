import subprocess, sys, pathlib

PRECHECK = pathlib.Path(__file__).parent / "precheck.py"


def _run(a, b):
    return subprocess.run(
        [sys.executable, str(PRECHECK), "--fastembed", a, "--chunkshop", b],
        capture_output=True,
        text=True,
    )


def test_sc008_matching_blobs_pass(tmp_path):
    p = tmp_path / "model.onnx"
    p.write_bytes(b"IDENTICAL-ONNX-BYTES")
    q = tmp_path / "hf.onnx"
    q.write_bytes(b"IDENTICAL-ONNX-BYTES")
    r = _run(str(p), str(q))
    assert r.returncode == 0, r.stderr


def test_sc008_divergent_blobs_fail(tmp_path):
    p = tmp_path / "model.onnx"
    p.write_bytes(b"FASTEMBED-ONNX")
    q = tmp_path / "hf.onnx"
    q.write_bytes(b"CHUNKSHOP-ONNX-DIFFERENT")
    r = _run(str(p), str(q))
    assert r.returncode != 0
    assert "SHA256 mismatch" in r.stderr
