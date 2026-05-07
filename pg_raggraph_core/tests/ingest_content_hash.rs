use pg_raggraph_core::ingest::content_hash::content_hash;

#[test]
fn content_hash_is_64_char_hex() {
    let h = content_hash(b"hello world");
    assert_eq!(h.len(), 64, "SHA-256 hex must be 64 chars");
    assert!(
        h.chars().all(|c| c.is_ascii_hexdigit()),
        "must be lowercase hex"
    );
}

#[test]
fn content_hash_is_deterministic() {
    let a = content_hash(b"hello world");
    let b = content_hash(b"hello world");
    assert_eq!(a, b, "SC-007: identical content -> identical hash");
}

#[test]
fn content_hash_distinguishes_different_inputs() {
    let a = content_hash(b"hello");
    let b = content_hash(b"world");
    assert_ne!(a, b);
}

#[test]
fn content_hash_known_vector() {
    // Anchor vector against external truth: `printf "" | sha256sum`
    // -> e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
    assert_eq!(
        content_hash(b""),
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
}
