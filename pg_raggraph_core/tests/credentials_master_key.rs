//! SC-006: master key file permission check.
//!
//! Mode 0600 (owner read/write only) must succeed. Modes that grant group or
//! world read/write (0644, 0640, 0666) must error out.

use pg_raggraph_core::credentials::master_key::MasterKey;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;

fn write_key_file(mode: u32) -> tempfile::NamedTempFile {
    let mut f = tempfile::NamedTempFile::new().expect("temp file");
    // 32 random-looking bytes — content is not validated here, only length.
    let key = [0xABu8; 32];
    f.write_all(&key).unwrap();
    std::fs::set_permissions(f.path(), std::fs::Permissions::from_mode(mode)).unwrap();
    f
}

#[test]
fn load_succeeds_with_mode_0600() {
    let f = write_key_file(0o600);
    let key = MasterKey::load_from_path(f.path()).expect("0600 should load");
    assert_eq!(key.as_bytes().len(), 32);
}

#[test]
fn load_rejects_group_readable_0640() {
    let f = write_key_file(0o640);
    let err = MasterKey::load_from_path(f.path()).expect_err("0640 must be rejected");
    let msg = format!("{err}");
    assert!(
        msg.contains("permission"),
        "expected permission error, got {msg}"
    );
}

#[test]
fn load_rejects_world_readable_0644() {
    let f = write_key_file(0o644);
    let err = MasterKey::load_from_path(f.path()).expect_err("0644 must be rejected");
    let msg = format!("{err}");
    assert!(
        msg.contains("permission"),
        "expected permission error, got {msg}"
    );
}

#[test]
fn load_rejects_wrong_length_key_file() {
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(b"short").unwrap();
    std::fs::set_permissions(f.path(), std::fs::Permissions::from_mode(0o600)).unwrap();
    let err = MasterKey::load_from_path(f.path()).expect_err("non-32-byte must fail");
    let msg = format!("{err}");
    assert!(
        msg.contains("32"),
        "expected length error mentioning 32, got {msg}"
    );
}
