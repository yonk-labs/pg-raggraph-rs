#!/usr/bin/env python3
"""Frozen-graph extracted-fixture generator. NO LLM is called (GATE-C:
frozen-graph corpus, spec §10 lines 439-446). Entities/relationships are
derived deterministically from the corpus topic structure; embeddings are a
fixed 384-dim function of text (spec §10: embeddings ship pre-computed; the
embedding model is NOT in the parity loop).

LINE 1 is a `_header` JSON object recording the provenance SC-002/DC-001
require. NOTE: `pgrg.ingest_extracted` HARD-ERRORS on any non-record line,
so the parity harness / loader MUST strip line 1 (`tail -n +2`) before
ingesting. See GENERATION.md. Record schema EXACTLY matches
pg_raggraph_core/src/retrieval/fixture.rs (discriminator `kind`; an
entity/relationship TYPE field is `kind_label`).
Usage: python gen_extracted.py --tier small --corpus ../corpus/small/docs.jsonl --out small.jsonl
"""
import argparse, hashlib, json, uuid

EMB_DIM = 384  # matches pgrg.embed_dim default / bge-small-en-v1.5

def emb(text: str):
    # Deterministic, ALWAYS-FINITE unit vector. Each component is a hash
    # byte mapped to [-1, 1) then L2-normalized — no float-bit
    # reinterpretation (that can yield NaN/Inf, which serde_json rejects
    # as non-standard JSON and the real loader hard-errors on).
    h = hashlib.sha256(text.encode()).digest()
    pool = h * (EMB_DIM // len(h) + 1)
    raw = [(pool[i] - 128) / 128.0 for i in range(EMB_DIM)]
    n = sum(x * x for x in raw) ** 0.5 or 1.0
    return [x / n for x in raw]

def duid(*parts):
    return str(uuid.UUID(hashlib.sha256("|".join(parts).encode()).hexdigest()[:32]))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tier", required=True)
    ap.add_argument("--corpus", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    ns = "parity"

    docs = [json.loads(l) for l in open(a.corpus, encoding="utf-8")]
    with open(a.out, "w", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps({
            "kind": "_header",
            "extraction_model": "frozen-graph-synthetic",
            "model_version": "v1",
            "temperature": 0.0,
            "seed": f"pgrg-parity-{a.tier}-v1",
            "note": "No LLM in the parity loop (spec §10 lines 439-446). "
                    "Strip this line before pgrg.ingest_extracted.",
        }) + "\n")
        for d in docs:
            # The chunk FK (chunks.document_id -> documents.id, NOT NULL, no
            # deferral) requires the parent document row to exist first, so
            # the document record is emitted ahead of its chunk. The corpus
            # line is already a valid `document` record (schema matches
            # fixture.rs::FixtureDocument); pass it through verbatim.
            f.write(json.dumps({
                "kind": "document", "id": d["id"], "namespace": ns,
                "source": d["source"], "content_hash": d["content_hash"],
                "title": d["title"], "metadata": {},
            }) + "\n")
            text = d["title"] + " :: " + d["source"]
            cid = duid("chunk", d["id"])
            f.write(json.dumps({
                "kind": "chunk", "id": cid, "namespace": ns,
                "document_id": d["id"], "ord": 0, "text": text,
                "token_count": max(1, len(text.split())),
                "embedding": emb(text), "metadata": {},
            }) + "\n")
            topic = d["title"].split()[0]
            eid = duid("entity", topic)
            f.write(json.dumps({
                "kind": "entity", "id": eid, "namespace": ns,
                "name": topic, "kind_label": "concept",
                "name_emb": emb(topic), "description": None,
            }) + "\n")
            f.write(json.dumps({
                "kind": "chunk_entity", "chunk_id": cid, "entity_id": eid,
                "confidence": 1.0, "classification": "extracted",
            }) + "\n")
        topics = sorted({json.loads(l)["title"].split()[0]
                         for l in open(a.corpus, encoding="utf-8")})
        for i in range(len(topics) - 1):
            f.write(json.dumps({
                "kind": "relationship",
                "id": duid("rel", topics[i], topics[i + 1]),
                "namespace": ns,
                "src_id": duid("entity", topics[i]),
                "dst_id": duid("entity", topics[i + 1]),
                "kind_label": "related_to", "weight": 1.0,
            }) + "\n")

if __name__ == "__main__":
    main()
