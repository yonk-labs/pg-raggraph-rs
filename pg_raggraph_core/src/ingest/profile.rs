//! Ingestion profile knobs — Conservative/Balanced/Aggressive/Max.
//!
//! Per mission brief SC-014:
//!   conservative=2, balanced=4 (default), aggressive=8, max=16
//! Maps to `pgrg.extract_concurrency` (spec §3 line 72, §7 default 4).

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum IngestProfile {
    Conservative,
    #[default]
    Balanced,
    Aggressive,
    Max,
}

impl IngestProfile {
    /// Stable string identifier for SQL parameter passing and serialization.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            IngestProfile::Conservative => "conservative",
            IngestProfile::Balanced => "balanced",
            IngestProfile::Aggressive => "aggressive",
            IngestProfile::Max => "max",
        }
    }

    /// Parse a profile from its SQL string identifier. Case-sensitive.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "conservative" => Some(IngestProfile::Conservative),
            "balanced" => Some(IngestProfile::Balanced),
            "aggressive" => Some(IngestProfile::Aggressive),
            "max" => Some(IngestProfile::Max),
            _ => None,
        }
    }

    /// Per-profile `extract_concurrency` value. SC-014 contract:
    /// conservative=2, balanced=4, aggressive=8, max=16.
    #[must_use]
    pub const fn extract_concurrency(self) -> u32 {
        match self {
            IngestProfile::Conservative => 2,
            IngestProfile::Balanced => 4,
            IngestProfile::Aggressive => 8,
            IngestProfile::Max => 16,
        }
    }
}
