//! `pgrg.ingest_extracted` — fixture loader for tests and Plan 6 parity benchmarks.
//!
//! Reads a JSONL file (one record per line; see `pg_raggraph_core::retrieval::fixture`
//! for the schema), and inserts into the appropriate `pgrg.*` table in a single
//! Spi transaction. Bypasses `pgrg.ingest_jobs` entirely (mission brief SC-003).
//!
//! Constraint Always: parameterized SQL with positional arguments — no string
//! interpolation of fixture data into SQL, except for the `vector(N)` literal
//! (typed `f32` values cannot be SQL-injection vectors and pgrx 0.17 has no
//! native pgvector binding).

use pg_raggraph_core::retrieval::fixture::{FixtureRecord, parse_jsonl_line};
use pgrx::prelude::*;
use std::fs::File;
use std::io::{BufRead, BufReader};

/// Build a pgvector text literal of the form `[v1,v2,...]`.
fn vector_literal(v: &[f32]) -> String {
    let mut s = String::with_capacity(v.len() * 8 + 2);
    s.push('[');
    let mut first = true;
    for x in v {
        if !first {
            s.push(',');
        }
        first = false;
        s.push_str(&format!("{x}"));
    }
    s.push(']');
    s
}

#[pg_extern]
fn ingest_extracted(path: &str, namespace: default!(&str, "'default'")) -> i64 {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) => {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_IO_ERROR,
                format!("ingest_extracted: cannot open {path}: {e}")
            );
        }
    };
    let reader = BufReader::new(file);

    let mut count: i64 = 0;
    Spi::connect_mut(|client| {
        for (lineno, line) in reader.lines().enumerate() {
            let line = match line {
                Ok(l) => l,
                Err(e) => {
                    ereport!(
                        ERROR,
                        PgSqlErrorCode::ERRCODE_IO_ERROR,
                        format!("ingest_extracted: read line {lineno}: {e}")
                    );
                }
            };
            if line.trim().is_empty() {
                continue;
            }
            let rec = match parse_jsonl_line(&line) {
                Ok(r) => r,
                Err(e) => {
                    ereport!(
                        ERROR,
                        PgSqlErrorCode::ERRCODE_INVALID_TEXT_REPRESENTATION,
                        format!("ingest_extracted: parse line {lineno}: {e}")
                    );
                }
            };

            // Override namespace from the SQL arg (the path is authoritative
            // for content; the namespace arg is the load target).
            match rec {
                FixtureRecord::Document(d) => {
                    client
                        .update(
                            "INSERT INTO pgrg.documents (id, namespace, source, content_hash, title, metadata) \
                             VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (content_hash) DO NOTHING",
                            None,
                            &[
                                pgrx::Uuid::from_bytes(*d.id.as_bytes()).into(),
                                namespace.into(),
                                d.source.as_str().into(),
                                d.content_hash.as_str().into(),
                                d.title.as_deref().into(),
                                pgrx::JsonB(d.metadata).into(),
                            ],
                        )
                        .expect("ingest_extracted: documents insert");
                }
                FixtureRecord::Chunk(c) => {
                    let lit = vector_literal(&c.embedding);
                    let sql = format!(
                        "INSERT INTO pgrg.chunks (id, namespace, document_id, ord, text, token_count, embedding, metadata) \
                         VALUES ($1, $2, $3, $4, $5, $6, '{lit}'::vector, $7) ON CONFLICT (document_id, ord) DO NOTHING"
                    );
                    client
                        .update(
                            &sql,
                            None,
                            &[
                                pgrx::Uuid::from_bytes(*c.id.as_bytes()).into(),
                                namespace.into(),
                                pgrx::Uuid::from_bytes(*c.document_id.as_bytes()).into(),
                                c.ord.into(),
                                c.text.as_str().into(),
                                c.token_count.into(),
                                pgrx::JsonB(c.metadata).into(),
                            ],
                        )
                        .expect("ingest_extracted: chunks insert");
                }
                FixtureRecord::Entity(e) => {
                    let lit = vector_literal(&e.name_emb);
                    let sql = format!(
                        "INSERT INTO pgrg.entities (id, namespace, name, kind, name_emb, description) \
                         VALUES ($1, $2, $3, $4, '{lit}'::vector, $5) ON CONFLICT (namespace, name, kind) DO NOTHING"
                    );
                    client
                        .update(
                            &sql,
                            None,
                            &[
                                pgrx::Uuid::from_bytes(*e.id.as_bytes()).into(),
                                namespace.into(),
                                e.name.as_str().into(),
                                e.kind_label.as_deref().into(),
                                e.description.as_deref().into(),
                            ],
                        )
                        .expect("ingest_extracted: entities insert");
                }
                FixtureRecord::Relationship(r) => {
                    client
                        .update(
                            "INSERT INTO pgrg.relationships (id, namespace, src_id, dst_id, kind, weight) \
                             VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (namespace, src_id, dst_id, kind) DO NOTHING",
                            None,
                            &[
                                pgrx::Uuid::from_bytes(*r.id.as_bytes()).into(),
                                namespace.into(),
                                pgrx::Uuid::from_bytes(*r.src_id.as_bytes()).into(),
                                pgrx::Uuid::from_bytes(*r.dst_id.as_bytes()).into(),
                                r.kind.as_str().into(),
                                r.weight.into(),
                            ],
                        )
                        .expect("ingest_extracted: relationships insert");
                }
                FixtureRecord::ChunkEntity(ce) => {
                    client
                        .update(
                            "INSERT INTO pgrg.chunk_entities (chunk_id, entity_id, confidence, classification) \
                             VALUES ($1, $2, $3, $4) ON CONFLICT (chunk_id, entity_id) DO NOTHING",
                            None,
                            &[
                                pgrx::Uuid::from_bytes(*ce.chunk_id.as_bytes()).into(),
                                pgrx::Uuid::from_bytes(*ce.entity_id.as_bytes()).into(),
                                ce.confidence.into(),
                                ce.classification.as_str().into(),
                            ],
                        )
                        .expect("ingest_extracted: chunk_entities insert");
                }
            }
            count += 1;
        }
    });
    count
}
