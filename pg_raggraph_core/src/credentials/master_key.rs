//! Master key file loader for AES-GCM credential encryption.
//!
//! SC-006: rejects files whose permissions allow group or world read.
//! Caller (`pg_raggraph` crate) wires this into `_PG_init` so an OS-level
//! mis-permissioned key file errors out before queries can run.

#![allow(clippy::unnecessary_debug_formatting)]

use std::fs;
use std::io::Read;
use std::path::Path;

use crate::error::{CoreError, CoreResult};

const MASTER_KEY_LEN: usize = 32; // AES-256

/// A loaded master key. Constructed via [`MasterKey::load_from_path`]; the
/// raw bytes are not exposed except through [`MasterKey::as_bytes`] so call
/// sites cannot accidentally print or log the key.
#[derive(Clone)]
pub struct MasterKey {
    bytes: [u8; MASTER_KEY_LEN],
}

impl std::fmt::Debug for MasterKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MasterKey")
            .field("bytes", &"<redacted>")
            .finish()
    }
}

impl MasterKey {
    /// Load a master key from a file path. The file must:
    /// * be exactly 32 bytes (raw key material; not base64-encoded)
    /// * have permission bits 0600 (owner read/write only)
    ///
    /// Returns `CoreError::Crypto` if the file is missing, mis-sized, or
    /// has too-permissive bits.
    pub fn load_from_path(path: impl AsRef<Path>) -> CoreResult<Self> {
        let path = path.as_ref();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let meta =
                fs::metadata(path).map_err(|e| CoreError::Crypto(format!("stat {path:?}: {e}")))?;
            let mode = meta.permissions().mode() & 0o777;
            // Reject any group or world read/write/execute bit.
            if mode & 0o077 != 0 {
                return Err(CoreError::Crypto(format!(
                    "master key file {path:?} has permission {mode:#o} \
                     (group or world readable); set mode 0600"
                )));
            }
        }

        let mut f =
            fs::File::open(path).map_err(|e| CoreError::Crypto(format!("open {path:?}: {e}")))?;
        let mut buf = Vec::with_capacity(MASTER_KEY_LEN + 1);
        f.read_to_end(&mut buf)
            .map_err(|e| CoreError::Crypto(format!("read {path:?}: {e}")))?;
        if buf.len() != MASTER_KEY_LEN {
            return Err(CoreError::Crypto(format!(
                "master key file must be exactly {MASTER_KEY_LEN} bytes, got {}",
                buf.len()
            )));
        }
        let mut bytes = [0u8; MASTER_KEY_LEN];
        bytes.copy_from_slice(&buf);
        Ok(Self { bytes })
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8; MASTER_KEY_LEN] {
        &self.bytes
    }
}
