use pg_raggraph_core::ProviderKind;

#[test]
fn provider_kind_roundtrip() {
    for kind in [ProviderKind::Llm, ProviderKind::Embedding] {
        assert_eq!(ProviderKind::parse(kind.as_str()), Some(kind));
    }
}

#[test]
fn provider_kind_unknown_returns_none() {
    assert_eq!(ProviderKind::parse("garbage"), None);
}
