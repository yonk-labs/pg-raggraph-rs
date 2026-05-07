use pg_raggraph_core::ingest::{IngestRequest, IngestSource};
use uuid::Uuid;

#[test]
fn ingest_request_path_source_round_trips() {
    let req = IngestRequest {
        source: IngestSource::Path("/data/docs/a.md".into()),
        namespace: "default".into(),
        chunk_strategy: "auto".into(),
    };
    let json = serde_json::to_string(&req).unwrap();
    let parsed: IngestRequest = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.namespace, "default");
    assert!(matches!(parsed.source, IngestSource::Path(ref p) if p == "/data/docs/a.md"));
}

#[test]
fn ingest_request_text_source_round_trips() {
    let req = IngestRequest {
        source: IngestSource::Text {
            name: "doc1".into(),
            content: "hello world".into(),
        },
        namespace: "default".into(),
        chunk_strategy: "auto".into(),
    };
    let json = serde_json::to_string(&req).unwrap();
    let parsed: IngestRequest = serde_json::from_str(&json).unwrap();
    if let IngestSource::Text { name, content } = parsed.source {
        assert_eq!(name, "doc1");
        assert_eq!(content, "hello world");
    } else {
        panic!("expected Text source");
    }
}

#[test]
fn ingest_request_bytes_source_round_trips() {
    let req = IngestRequest {
        source: IngestSource::Bytes {
            name: "doc1.bin".into(),
            bytes: vec![0xDE, 0xAD, 0xBE, 0xEF],
        },
        namespace: "default".into(),
        chunk_strategy: "auto".into(),
    };
    let json = serde_json::to_string(&req).unwrap();
    let parsed: IngestRequest = serde_json::from_str(&json).unwrap();
    if let IngestSource::Bytes { name, bytes } = parsed.source {
        assert_eq!(name, "doc1.bin");
        assert_eq!(bytes, vec![0xDE, 0xAD, 0xBE, 0xEF]);
    } else {
        panic!("expected Bytes source");
    }
}

#[test]
fn ingest_request_default_chunk_strategy_is_auto() {
    let req = IngestRequest::new_path("/data/docs/", "default");
    assert_eq!(req.chunk_strategy, "auto");
}

#[test]
fn ingest_job_id_is_uuid() {
    use pg_raggraph_core::ingest::IngestJob;
    let j = IngestJob {
        id: Uuid::new_v4(),
        request: IngestRequest::new_path("/x", "ns"),
        attempt_count: 0,
    };
    // Compile-time check: IngestJob exposes a Uuid id.
    assert_eq!(j.id.get_version_num(), 4);
}
