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

#[test]
fn redact_handles_multibyte_utf8_without_panic() {
    // 4-character strings where the 4th char is multi-byte.
    // Old `split_at(3)` byte-index code would panic trying to split mid-character.
    // Correct behavior: keep first 3 CHARACTERS + redact the rest.
    let cases: &[(&str, &str)] = &[
        ("sk-Ω🔑abc", "sk-***"),    // first 3 chars are 's','k','-'; 4th is 'Ω'
        ("sk-é🔑x", "sk-***"),      // first 3 chars are 's','k','-'; 4th is 'é'
        ("🔑🔑🔑🔑x", "🔑🔑🔑***"), // first 3 chars are all emoji; 4th is emoji
        ("аэр1xyz", "аэр***"),      // first 3 chars are Cyrillic; 4th is '1'
    ];
    for (input, expected) in cases {
        assert_eq!(redact(input), *expected, "input: {input:?}");
    }
}

#[test]
fn redact_handles_short_inputs() {
    assert_eq!(redact(""), "***");
    assert_eq!(redact("a"), "***");
    assert_eq!(redact("ab"), "***");
    assert_eq!(redact("abc"), "***");
    assert_eq!(redact("abcd"), "abc***");
}

#[test]
fn redact_keeps_ascii_prefix() {
    assert_eq!(redact("sk-1234567890"), "sk-***");
    assert_eq!(redact("key-aaaabbbb"), "key***");
}
