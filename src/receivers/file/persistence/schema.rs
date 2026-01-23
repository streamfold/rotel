// SPDX-License-Identifier: Apache-2.0

//! Persistence schema definitions for file receiver state.
//!
//! This module defines the versioned schemas for persisting file tracking state.
//! Currently supports:
//! - v0: Legacy format with base64-encoded entries (no version field)
//! - v1: JSON format with human-readable metadata and version field

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Key used to store known files state in the persister
pub const KNOWN_FILES_KEY: &str = "knownFiles";

/// Current schema version for persisted state
pub const PERSISTED_STATE_VERSION: u8 = 1;

/// Legacy persisted state for all known files (v0, no version field)
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct PersistedStateV0 {
    pub files: Vec<PersistedFileStateV0>,
}

/// Legacy persisted state for a single file (v0)
#[derive(Debug, Serialize, Deserialize)]
pub struct PersistedFileStateV0 {
    /// Device ID (Unix) or volume serial (Windows)
    pub dev: u64,
    /// Inode number (Unix) or file index (Windows)
    pub ino: u64,
    /// Current read offset
    pub offset: u64,
}

/// Persisted state for all known files (v1)
/// Key is "dev:ino" for efficient lookup
#[derive(Debug, Serialize, Deserialize)]
pub struct PersistedStateV1 {
    /// Schema version (always 1 for this format)
    pub version: u8,
    /// Map from file key (dev:ino) to file entry
    pub files: HashMap<String, PersistedFileEntryV1>,
}

impl Default for PersistedStateV1 {
    fn default() -> Self {
        Self {
            version: PERSISTED_STATE_VERSION,
            files: HashMap::new(),
        }
    }
}

/// Persisted state for a single file (v1)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedFileEntryV1 {
    // Human-readable metadata (for user visibility)
    /// Last known path to the file
    pub path: String,
    /// Last known filename
    pub filename: String,

    // Identity (for matching across restarts)
    /// Device ID (Unix) or volume serial (Windows)
    pub dev: u64,
    /// Inode number (Unix) or file index (Windows)
    pub ino: u64,

    // Position tracking
    /// Persisted offset (byte position in file)
    pub offset: u64,
}

impl PersistedFileEntryV1 {
    /// Generate the map key for this entry ("dev:ino" format)
    pub fn key(&self) -> String {
        file_id_to_key(self.dev, self.ino)
    }
}

/// Generate a map key from dev and ino ("dev:ino" format)
pub fn file_id_to_key(dev: u64, ino: u64) -> String {
    format!("{}:{}", dev, ino)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_id_to_key() {
        assert_eq!(file_id_to_key(1, 100), "1:100");
        assert_eq!(file_id_to_key(0, 0), "0:0");
        assert_eq!(
            file_id_to_key(u64::MAX, u64::MAX),
            "18446744073709551615:18446744073709551615"
        );
    }

    #[test]
    fn test_persisted_state_v1_default() {
        let state = PersistedStateV1::default();
        assert_eq!(state.version, PERSISTED_STATE_VERSION);
        assert!(state.files.is_empty());
    }

    #[test]
    fn test_persisted_file_entry_v1_key() {
        let entry = PersistedFileEntryV1 {
            path: "/var/log/test.log".to_string(),
            filename: "test.log".to_string(),
            dev: 1,
            ino: 100,
            offset: 500,
        };
        assert_eq!(entry.key(), "1:100");
    }

    #[test]
    fn test_v0_serialization_roundtrip() {
        let state = PersistedStateV0 {
            files: vec![
                PersistedFileStateV0 {
                    dev: 1,
                    ino: 100,
                    offset: 500,
                },
                PersistedFileStateV0 {
                    dev: 2,
                    ino: 200,
                    offset: 1000,
                },
            ],
        };

        let json = serde_json::to_string(&state).unwrap();
        let restored: PersistedStateV0 = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.files.len(), 2);
        assert_eq!(restored.files[0].dev, 1);
        assert_eq!(restored.files[0].ino, 100);
        assert_eq!(restored.files[0].offset, 500);
    }

    #[test]
    fn test_v1_serialization_roundtrip() {
        let mut files = HashMap::new();
        files.insert(
            "1:100".to_string(),
            PersistedFileEntryV1 {
                path: "/var/log/test.log".to_string(),
                filename: "test.log".to_string(),
                dev: 1,
                ino: 100,
                offset: 500,
            },
        );

        let state = PersistedStateV1 { version: 1, files };

        let json = serde_json::to_string(&state).unwrap();
        let restored: PersistedStateV1 = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.version, 1);
        assert_eq!(restored.files.len(), 1);

        let entry = restored.files.get("1:100").unwrap();
        assert_eq!(entry.path, "/var/log/test.log");
        assert_eq!(entry.offset, 500);
    }
}
