//! SC-011: AES-GCM credential interop — sidecar write path ↔ in-extension read path.
//!
//! Both paths use the SAME `pg_raggraph_core::credentials` module, so the
//! `enc:v1:` on-disk format is inherently compatible. This test LOCKS the
//! four invariants so a future regression is caught:
//!   1. Stored credential starts with `enc:v1:` (encrypted at rest).
//!   2. Stored credential does NOT contain the plaintext (`no-plaintext` leak check).
//!   3. `decrypt_v1` round-trips back to the original plaintext.
//!   4. Two independent encryptions of the same plaintext produce DIFFERENT
//!      ciphertexts (nonce randomization) yet both decrypt to the same plaintext.
//!
//! Gated by `PGRG_TEST_DATABASE_URL` (CI sets it from docker-compose.test.yml,
//! host port 5443). If unset the test is skipped with an informational message.

use std::os::unix::fs::PermissionsExt;

use pg_raggraph_core::credentials::{MasterKey, decrypt_v1, encrypt_v1, is_encrypted};
use pg_raggraph_sidecar::{bootstrap, db};

fn test_db_url() -> Option<String> {
    std::env::var("PGRG_TEST_DATABASE_URL").ok()
}

#[tokio::test]
async fn credential_interop_aes_gcm() {
    let Some(url) = test_db_url() else {
        eprintln!("SKIP: PGRG_TEST_DATABASE_URL unset — set to run credential interop assertions");
        return;
    };

    // ── 1. Write a 32-byte key to a 0600 temp file and load it via MasterKey ──
    let key_file = tempfile::NamedTempFile::new().expect("tempfile");
    let key_bytes: [u8; 32] = [
        0xAB, 0xAC, 0xAD, 0xAE, 0xAF, 0xB0, 0xB1, 0xB2, 0xB3, 0xB4, 0xB5, 0xB6, 0xB7, 0xB8, 0xB9,
        0xBA, 0xBB, 0xBC, 0xBD, 0xBE, 0xBF, 0xC0, 0xC1, 0xC2, 0xC3, 0xC4, 0xC5, 0xC6, 0xC7, 0xC8,
        0xC9, 0xCA,
    ];
    std::fs::write(key_file.path(), key_bytes).expect("write key");
    std::fs::set_permissions(key_file.path(), std::fs::Permissions::from_mode(0o600))
        .expect("chmod 0600");

    let master_key = MasterKey::load_from_path(key_file.path()).expect("load MasterKey");

    // ── 2. Connect and bootstrap a fresh schema ────────────────────────────────
    let mut c = db::connect(&url).await.expect("connect");
    c.batch_execute("DROP SCHEMA IF EXISTS pgrg CASCADE;")
        .await
        .expect("reset schema");
    bootstrap::run_migrations(&mut c)
        .await
        .expect("bootstrap migrations");

    // ── 3. Sidecar WRITE path ─────────────────────────────────────────────────
    // Simulate what `provider_factory` does: encrypt → INSERT into pgrg.providers.
    let plaintext = "sk-interop-SECRET-9988";
    let enc_a = encrypt_v1(plaintext, master_key.as_bytes()).expect("encrypt_v1 (sidecar write)");

    c.execute(
        "INSERT INTO pgrg.providers \
         (name, kind, provider, base_url, model, credential, config) \
         VALUES ('interop-p', 'llm', 'openai', NULL, 'gpt-4o', $1, '{}'::jsonb)",
        &[&enc_a],
    )
    .await
    .expect("INSERT provider");

    // ── 4. Independent READ path (simulates in-extension reader) ──────────────
    let row = c
        .query_one(
            "SELECT credential FROM pgrg.providers WHERE name = 'interop-p'",
            &[],
        )
        .await
        .expect("SELECT credential");
    let stored: String = row.get(0);

    // Assertion 1: stored form starts with `enc:v1:` and is_encrypted returns true.
    assert!(
        stored.starts_with("enc:v1:"),
        "credential must start with enc:v1:; got: {stored:?}"
    );
    assert!(
        is_encrypted(&stored),
        "is_encrypted must be true; got: {stored:?}"
    );

    // Assertion 2: plaintext is NOT present in the stored ciphertext.
    assert!(
        !stored.contains("SECRET"),
        "plaintext 'SECRET' must not appear in stored credential; got: {stored:?}"
    );
    assert!(
        !stored.contains(plaintext),
        "full plaintext must not appear in stored credential; got: {stored:?}"
    );

    // Assertion 3: decrypt_v1 round-trips back to the original plaintext.
    let decrypted =
        decrypt_v1(&stored, master_key.as_bytes()).expect("decrypt_v1 round-trip failed");
    assert_eq!(
        decrypted, plaintext,
        "round-trip: decrypted must equal original plaintext"
    );

    // ── 5. Reverse direction parity (in-extension WRITE → sidecar READ) ───────
    // Encrypt the same plaintext a SECOND time independently.
    let enc_b = encrypt_v1(plaintext, master_key.as_bytes())
        .expect("encrypt_v1 (in-extension write simulation)");

    // Assertion 4a: two encryptions of the same plaintext produce DIFFERENT
    //               ciphertexts (fresh nonce per call — AES-GCM randomization).
    assert_ne!(
        enc_a, enc_b,
        "two encryptions of the same plaintext must differ (nonce randomization)"
    );

    // Assertion 4b: yet BOTH ciphertexts decrypt to the same plaintext,
    //               proving format stability / interoperability.
    let decrypted_b = decrypt_v1(&enc_b, master_key.as_bytes()).expect("decrypt_v1 enc_b failed");
    assert_eq!(
        decrypted_b, plaintext,
        "enc_b must also decrypt to the original plaintext"
    );

    // Belt-and-suspenders: both also satisfy is_encrypted.
    assert!(is_encrypted(&enc_b), "enc_b must also satisfy is_encrypted");

    eprintln!("PASS: all 4 credential interop assertions satisfied");
    eprintln!("  enc_a (sidecar write)  : {enc_a}");
    eprintln!("  enc_b (in-ext sim)     : {enc_b}");
    eprintln!("  nonce randomization    : enc_a != enc_b ✓");
    eprintln!("  round-trip both decrypt: ✓");
}
