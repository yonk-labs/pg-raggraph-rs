#!/usr/bin/env python3
"""Deterministic frozen-corpus generator. Seeded; stable content_hash.
Small tier is committed; medium/large are regenerated via this script
(see extracted/GENERATION.md). Source = synthetic, public-domain technical
prose about this project's own domain (Postgres/GraphRAG) so queries have
real lexical+semantic structure without external licensing.
Usage: python gen_corpus.py --tier small --out corpus/small/docs.jsonl
"""
import argparse, hashlib, json, random, uuid

TIERS = {"small": 120, "medium": 1000, "large": 10000}
TOPICS = [
    ("postgres", "PostgreSQL stores {x} using {y} with ACID guarantees."),
    ("pgvector", "pgvector indexes {x} embeddings; {y} powers similarity search."),
    ("graphrag", "GraphRAG fuses {x} traversal with {y} retrieval over a knowledge graph."),
    ("resolution", "Entity resolution merges {x} variants using {y} thresholds."),
    ("rrf", "Reciprocal Rank Fusion blends {x} and {y} lanes at k=60."),
]
FILL = ["recursive CTEs", "HNSW", "IVFFlat", "BM25", "cosine similarity",
        "adjacency tables", "trigram matching", "hybrid mode", "chunk embeddings",
        "the bge-small-en-v1.5 model"]

def gen(tier: str):
    n = TIERS[tier]
    rng = random.Random(f"pgrg-parity-{tier}-v1")  # fixed seed -> stable corpus
    ns = "parity"
    for i in range(n):
        topic, tpl = TOPICS[i % len(TOPICS)]
        body = " ".join(
            tpl.format(x=rng.choice(FILL), y=rng.choice(FILL)) for _ in range(rng.randint(4, 9))
        )
        text = f"# {topic.title()} note {i}\n\n{body}"
        ch = hashlib.sha256(text.encode()).hexdigest()
        did = str(uuid.UUID(ch[:32]))
        yield {
            "kind": "document", "id": did, "namespace": ns,
            "source": f"synthetic://{tier}/{topic}/{i}",
            "content_hash": ch, "title": f"{topic} note {i}", "metadata": {},
        }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tier", required=True, choices=list(TIERS))
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    with open(a.out, "w", encoding="utf-8", newline="\n") as f:
        for rec in gen(a.tier):
            f.write(json.dumps(rec) + "\n")

if __name__ == "__main__":
    main()
