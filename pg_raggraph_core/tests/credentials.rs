use pg_raggraph_core::credentials::redact;

#[test]
fn redact_keeps_prefix() {
    assert_eq!(redact("sk-secret-1234567890"), "sk-***");
}

#[test]
fn redact_short_credential_fully_masked() {
    assert_eq!(redact("ab"), "***");
}
