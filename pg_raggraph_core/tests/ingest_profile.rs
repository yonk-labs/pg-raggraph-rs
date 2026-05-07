use pg_raggraph_core::ingest::IngestProfile;

#[test]
fn profile_default_is_balanced() {
    // Spec §3 line 72: extract_concurrency default 4 (= Balanced).
    assert_eq!(IngestProfile::default(), IngestProfile::Balanced);
}

#[test]
fn profile_extract_concurrency_values() {
    // SC-014: explicit per-profile concurrency mapping.
    assert_eq!(IngestProfile::Conservative.extract_concurrency(), 2);
    assert_eq!(IngestProfile::Balanced.extract_concurrency(), 4);
    assert_eq!(IngestProfile::Aggressive.extract_concurrency(), 8);
    assert_eq!(IngestProfile::Max.extract_concurrency(), 16);
}

#[test]
fn profile_parses_strings() {
    assert_eq!(
        IngestProfile::parse("conservative"),
        Some(IngestProfile::Conservative)
    );
    assert_eq!(
        IngestProfile::parse("balanced"),
        Some(IngestProfile::Balanced)
    );
    assert_eq!(
        IngestProfile::parse("aggressive"),
        Some(IngestProfile::Aggressive)
    );
    assert_eq!(IngestProfile::parse("max"), Some(IngestProfile::Max));
}

#[test]
fn profile_unknown_returns_none() {
    assert_eq!(IngestProfile::parse("turbo"), None);
    assert_eq!(IngestProfile::parse(""), None);
    assert_eq!(IngestProfile::parse("BALANCED"), None); // case-sensitive
}

#[test]
fn profile_as_str_roundtrip() {
    for p in [
        IngestProfile::Conservative,
        IngestProfile::Balanced,
        IngestProfile::Aggressive,
        IngestProfile::Max,
    ] {
        assert_eq!(IngestProfile::parse(p.as_str()), Some(p));
    }
}
