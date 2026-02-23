// SPDX-License-Identifier: Apache-2.0

//! Persistence for kmsg receiver offset tracking.
//!
//! Saves the last-read kernel message sequence number to disk so the receiver
//! can resume from where it left off after a rotel restart. Uses the Linux
//! `boot_id` to detect device reboots and invalidate stale state.

use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
use std::path::Path;
use tempfile::NamedTempFile;
use tracing::{debug, info, warn};

/// Current schema version
const SCHEMA_VERSION: u32 = 1;

/// Persisted state for the kmsg receiver
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PersistedKmsgState {
    /// Schema version for forward compatibility
    pub version: u32,
    /// Linux boot_id from the boot when state was saved
    pub boot_id: String,
    /// Highest kernel message sequence number that was processed
    pub sequence: u64,
}

impl PersistedKmsgState {
    pub fn new(boot_id: String, sequence: u64) -> Self {
        Self {
            version: SCHEMA_VERSION,
            boot_id,
            sequence,
        }
    }
}

/// Load persisted state from a JSON file.
///
/// Returns `None` if the file doesn't exist, is empty, or cannot be parsed.
/// Logs a warning on parse errors (corrupt file).
pub fn load_state(path: &Path) -> Option<PersistedKmsgState> {
    let data = match fs::read_to_string(path) {
        Ok(data) => data,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            debug!("No kmsg offset file found at {}", path.display());
            return None;
        }
        Err(e) => {
            warn!(
                "Failed to read kmsg offset file {}: {}",
                path.display(),
                e
            );
            return None;
        }
    };

    if data.trim().is_empty() {
        return None;
    }

    match serde_json::from_str::<PersistedKmsgState>(&data) {
        Ok(state) => {
            if state.version > SCHEMA_VERSION {
                warn!(
                    "Kmsg offset file has unsupported version {} (expected {}), ignoring",
                    state.version, SCHEMA_VERSION
                );
                return None;
            }
            Some(state)
        }
        Err(e) => {
            warn!(
                "Failed to parse kmsg offset file {}: {}",
                path.display(),
                e
            );
            None
        }
    }
}

/// Save persisted state to a JSON file using atomic write.
///
/// Writes to a temporary file in the same directory first, then renames to the
/// target path. This prevents corruption from partial writes if rotel crashes
/// mid-write.
///
/// When `sync_dir` is true, fsyncs the parent directory to ensure rename
/// durability. Set to false for periodic checkpoints to reduce flash wear on
/// embedded devices; use true for the final shutdown checkpoint.
pub fn save_state(path: &Path, state: &PersistedKmsgState, sync_dir: bool) -> Result<(), std::io::Error> {
    // Ensure parent directory exists
    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "path has no parent directory")
    })?;
    if !parent.as_os_str().is_empty() && !parent.exists() {
        info!("Creating directory for kmsg offset persistence: {}", parent.display());
        fs::create_dir_all(parent)?;
    }

    // Using compact JSON (not pretty-printed) to minimize file size. The structure
    // is simple enough to remain human-readable even without formatting.
    let json = serde_json::to_string(state).map_err(std::io::Error::other)?;

    // Create a temp file in the same directory to ensure same-filesystem rename
    let mut tmp_file = NamedTempFile::new_in(parent)?;
    tmp_file.write_all(json.as_bytes())?;
    tmp_file.write_all(b"\n")?;
    tmp_file.as_file().sync_all()?;

    // persist() atomically renames and consumes the NamedTempFile,
    // preventing cleanup races
    tmp_file.persist(path).map_err(|e| e.error)?;

    // Fsync the parent directory to ensure the rename is durable on Linux.
    // Skipped during periodic checkpoints to reduce flash wear on embedded devices.
    if sync_dir {
        match fs::File::open(parent).and_then(|dir| dir.sync_all()) {
            Ok(()) => {}
            Err(e) => warn!("Failed to fsync parent directory {}: {}", parent.display(), e),
        }
    }

    Ok(())
}

/// Determine the sequence number to resume from, if any.
///
/// Loads persisted state and compares the stored boot ID to the given current boot ID.
/// Returns `Some(sequence)` if the state is valid for the current boot,
/// or `None` if there is no valid state to resume from.
pub fn determine_start_sequence(path: &Path, current_boot_id: &str) -> Option<u64> {
    let state = load_state(path)?;

    if state.boot_id == current_boot_id {
        debug!(
            "Persisted kmsg state matches current boot (sequence={})",
            state.sequence
        );
        Some(state.sequence)
    } else {
        info!(
            "Persisted kmsg state is from a different boot (persisted={}, current={}), ignoring",
            state.boot_id, current_boot_id
        );
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_save_and_load_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("kmsg_offsets.json");

        let state = PersistedKmsgState::new("test-boot-id-1234".to_string(), 42);
        save_state(&path, &state, true).unwrap();

        let loaded = load_state(&path).unwrap();
        assert_eq!(loaded, state);
    }

    #[test]
    fn test_load_missing_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("nonexistent.json");

        assert!(load_state(&path).is_none());
    }

    #[test]
    fn test_load_empty_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("empty.json");
        fs::write(&path, "").unwrap();

        assert!(load_state(&path).is_none());
    }

    #[test]
    fn test_load_corrupt_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("corrupt.json");
        fs::write(&path, "not valid json{{{").unwrap();

        assert!(load_state(&path).is_none());
    }

    #[test]
    fn test_load_unsupported_version() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("future.json");
        let json = r#"{"version": 99, "boot_id": "abc", "sequence": 100}"#;
        fs::write(&path, json).unwrap();

        assert!(load_state(&path).is_none());
    }

    #[test]
    fn test_save_atomic_no_leftover_tmp() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("kmsg_offsets.json");

        let state = PersistedKmsgState::new("boot-123".to_string(), 500);
        save_state(&path, &state, true).unwrap();

        // The final file should exist and no temp files should remain
        assert!(path.exists());
        let tmp_files: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path() != path)
            .collect();
        assert!(tmp_files.is_empty(), "Unexpected temp files: {:?}", tmp_files);
    }

    #[test]
    fn test_save_overwrites_existing() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("kmsg_offsets.json");

        let state1 = PersistedKmsgState::new("boot-1".to_string(), 100);
        save_state(&path, &state1, true).unwrap();

        let state2 = PersistedKmsgState::new("boot-1".to_string(), 200);
        save_state(&path, &state2, true).unwrap();

        let loaded = load_state(&path).unwrap();
        assert_eq!(loaded.sequence, 200);
    }

    #[test]
    fn test_determine_start_sequence_no_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("nonexistent.json");

        // Should return None when file doesn't exist
        assert!(determine_start_sequence(&path, "any-boot-id").is_none());
    }

    #[test]
    fn test_persisted_state_serialization() {
        let state = PersistedKmsgState::new(
            "a1b2c3d4-e5f6-7890-abcd-ef1234567890".to_string(),
            123456,
        );

        let json = serde_json::to_string(&state).unwrap();
        let deserialized: PersistedKmsgState = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.version, 1);
        assert_eq!(
            deserialized.boot_id,
            "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
        );
        assert_eq!(deserialized.sequence, 123456);
    }

    #[test]
    fn test_determine_start_sequence_matching_boot_id() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("kmsg_offsets.json");

        let state = PersistedKmsgState::new("test-boot-id".to_string(), 999);
        save_state(&path, &state, true).unwrap();

        assert_eq!(
            determine_start_sequence(&path, "test-boot-id"),
            Some(999)
        );
    }

    #[test]
    fn test_determine_start_sequence_mismatched_boot_id() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("kmsg_offsets.json");

        let state = PersistedKmsgState::new("old-boot-id".to_string(), 999);
        save_state(&path, &state, true).unwrap();

        assert!(determine_start_sequence(&path, "different-boot-id").is_none());
    }
}
