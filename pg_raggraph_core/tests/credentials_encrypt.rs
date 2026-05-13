//! SC-003: AES-GCM encryption produces `enc:v1:<nonce>:<ciphertext>` form
//! and decrypt round-trips faithfully.

use pg_raggraph_core::credentials::encrypt::{decrypt_v1, encrypt_v1, is_encrypted};

fn fixed_key() -> [u8; 32] {
    *b"abcdefghijklmnopqrstuvwxyz012345"
}

#[test]
fn encrypt_v1_produces_enc_v1_prefix() {
    let key = fixed_key();
    let plaintext = "sk-test-real-secret-9999";
    let ct = encrypt_v1(plaintext, &key).expect("encrypt");
    assert!(
        ct.starts_with("enc:v1:"),
        "expected enc:v1: prefix, got {ct:?}"
    );
    let parts: Vec<&str> = ct.split(':').collect();
    assert_eq!(parts.len(), 4, "expected enc:v1:<nonce>:<ct>, got {ct:?}");
    assert_eq!(parts[0], "enc");
    assert_eq!(parts[1], "v1");
    assert!(!parts[2].is_empty());
    assert!(!parts[3].is_empty());
}

#[test]
fn encrypt_decrypt_round_trip() {
    let key = fixed_key();
    let plaintext = "sk-test-real-secret-9999";
    let ct = encrypt_v1(plaintext, &key).unwrap();
    let pt = decrypt_v1(&ct, &key).unwrap();
    assert_eq!(pt, plaintext);
}

#[test]
fn ciphertext_does_not_contain_plaintext_substring() {
    let key = fixed_key();
    let plaintext = "sk-rotated-secret-AAAA-BBBB-CCCC";
    let ct = encrypt_v1(plaintext, &key).unwrap();
    assert!(
        !ct.contains("rotated"),
        "plaintext substring leaked into ct"
    );
    assert!(!ct.contains("BBBB"));
}

#[test]
fn each_encrypt_produces_distinct_nonce() {
    let key = fixed_key();
    let pt = "same plaintext";
    let a = encrypt_v1(pt, &key).unwrap();
    let b = encrypt_v1(pt, &key).unwrap();
    assert_ne!(a, b, "fresh nonce per encrypt expected (no determinism)");
}

#[test]
fn decrypt_rejects_wrong_key() {
    let key = fixed_key();
    let mut wrong = key;
    wrong[0] ^= 0x01;
    let ct = encrypt_v1("plaintext", &key).unwrap();
    let err = decrypt_v1(&ct, &wrong).expect_err("wrong key must fail");
    let msg = format!("{err}");
    assert!(msg.contains("decrypt") || msg.contains("auth"), "got {msg}");
}

#[test]
fn decrypt_rejects_malformed_envelope() {
    let key = fixed_key();
    assert!(decrypt_v1("not-encrypted", &key).is_err());
    assert!(decrypt_v1("enc:v0:foo:bar", &key).is_err()); // wrong version
    assert!(decrypt_v1("enc:v1:nope", &key).is_err()); // missing ciphertext
}

#[test]
fn is_encrypted_detects_envelope() {
    assert!(is_encrypted("enc:v1:nonceb64:ciphertextb64"));
    assert!(!is_encrypted("sk-plaintext"));
    assert!(!is_encrypted(""));
    assert!(!is_encrypted("enc:v0:foo:bar"));
}
