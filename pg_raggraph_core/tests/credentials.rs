use pg_raggraph_core::credentials::redact;

#[test]
fn redact_keeps_prefix() {
    assert_eq!(redact("sk-secret-1234567890"), "sk-***");
}

#[test]
fn redact_short_credential_fully_masked() {
    assert_eq!(redact("ab"), "***");
}

#[test]
fn redact_preserves_multibyte_prefix() {
    // €abc-secret has € as a 3-byte UTF-8 char. Old split_at(3) would panic.
    let s = "€abc-secret";
    let r = redact(s);
    assert!(r.ends_with("***"));
    assert!(
        !r.contains("secret"),
        "secret bytes must not leak; got `{r}`"
    );
}
