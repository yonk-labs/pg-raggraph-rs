use pg_raggraph_core::retrieval::Mode;

#[test]
fn mode_parses_hybrid_by_default_string() {
    assert_eq!(Mode::parse("hybrid"), Some(Mode::Hybrid));
    assert_eq!(Mode::parse("vector"), Some(Mode::Vector));
    assert_eq!(Mode::parse("bm25"), Some(Mode::Bm25));
    assert_eq!(Mode::parse("graph"), Some(Mode::Graph));
}

#[test]
fn mode_unknown_returns_none() {
    // Mission Brief Constraints "Never": no smart/local/global modes.
    assert_eq!(Mode::parse("smart"), None);
    assert_eq!(Mode::parse("naive_boost"), None);
    assert_eq!(Mode::parse("local"), None);
    assert_eq!(Mode::parse("global"), None);
    assert_eq!(Mode::parse(""), None);
    assert_eq!(Mode::parse("HYBRID"), None); // case-sensitive, matches SQL spec
}

#[test]
fn mode_as_str_roundtrip() {
    for m in [Mode::Hybrid, Mode::Vector, Mode::Bm25, Mode::Graph] {
        assert_eq!(Mode::parse(m.as_str()), Some(m));
    }
}
