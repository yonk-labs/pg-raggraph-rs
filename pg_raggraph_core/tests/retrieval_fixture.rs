use pg_raggraph_core::retrieval::fixture::{FixtureRecord, parse_jsonl_line};

#[test]
fn parse_document_line() {
    let line = r#"{"kind":"document","id":"11111111-1111-1111-1111-111111111111","namespace":"ns","source":"a.md","content_hash":"h1","title":"T","metadata":{}}"#;
    let rec = parse_jsonl_line(line).expect("must parse");
    match rec {
        FixtureRecord::Document(d) => {
            assert_eq!(d.namespace, "ns");
            assert_eq!(d.source, "a.md");
            assert_eq!(d.content_hash, "h1");
        }
        _ => panic!("expected Document"),
    }
}

#[test]
fn parse_chunk_line() {
    let line = r#"{"kind":"chunk","id":"22222222-2222-2222-2222-222222222222","namespace":"ns","document_id":"11111111-1111-1111-1111-111111111111","ord":0,"text":"hi","token_count":1,"embedding":[0.1,0.2],"metadata":{"tag":"x"}}"#;
    let rec = parse_jsonl_line(line).expect("must parse");
    match rec {
        FixtureRecord::Chunk(c) => {
            assert_eq!(c.text, "hi");
            assert_eq!(c.embedding.len(), 2);
            assert_eq!(c.metadata["tag"], "x");
        }
        _ => panic!("expected Chunk"),
    }
}

#[test]
fn parse_unknown_kind_errors() {
    let line = r#"{"kind":"bogus"}"#;
    assert!(parse_jsonl_line(line).is_err());
}

#[test]
fn parse_relationship_line() {
    let line = r#"{"kind":"relationship","id":"33333333-3333-3333-3333-333333333333","namespace":"ns","src_id":"a1111111-1111-1111-1111-111111111111","dst_id":"b1111111-1111-1111-1111-111111111111","kind_label":"calls","weight":1.0}"#;
    let rec = parse_jsonl_line(line).expect("must parse");
    match rec {
        FixtureRecord::Relationship(r) => {
            assert_eq!(r.kind, "calls");
            assert!((r.weight - 1.0).abs() < 1e-12);
        }
        _ => panic!("expected Relationship"),
    }
}
