//! DC-001 (resolved): `ingest_jobs` is column-based (`source`,
//! `chunk_strategy`, `payload` bytea NULL-for-path, `namespace`) — NOT a serde
//! blob. row→`IngestSource` mirrors pgrx `worker::build_request` EXACTLY:
//! `payload` NULL ⇒ `Path`; `payload` present ⇒ `Text` if valid UTF-8 else
//! `Bytes`.

use pg_raggraph_core::ingest::types::IngestSource;
use pg_raggraph_sidecar::jobloop::row_to_ingest_source;

#[test]
fn payload_null_is_path() {
    assert_eq!(
        row_to_ingest_source("/data/a.md", None),
        IngestSource::Path("/data/a.md".into())
    );
}
#[test]
fn payload_valid_utf8_is_text() {
    assert_eq!(
        row_to_ingest_source("doc-1", Some(b"hello world".to_vec())),
        IngestSource::Text {
            name: "doc-1".into(),
            content: "hello world".into()
        }
    );
}
#[test]
fn payload_invalid_utf8_is_bytes() {
    let raw = vec![0xff, 0xfe, 0x00, 0x01];
    assert_eq!(
        row_to_ingest_source("blob-1", Some(raw.clone())),
        IngestSource::Bytes {
            name: "blob-1".into(),
            bytes: raw
        }
    );
}
