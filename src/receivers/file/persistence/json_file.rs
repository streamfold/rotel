//! JSON file-based persistence with atomic writes.
//!
//! This module provides a simple file-based persister that stores state as JSON.
//! Writes are atomic using a write-to-temp-then-rename strategy.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

use crate::receivers::file::error::{Error, Result};

/// State stored in the JSON file
/// Values can be either base64-encoded strings (legacy) or raw JSON objects (v1+)
#[derive(Debug, Default, Serialize, Deserialize)]
struct DatabaseState {
    scopes: HashMap<String, HashMap<String, serde_json::Value>>,
}

/// A shared JSON file database handle
#[derive(Clone)]
pub struct JsonFileDatabase {
    path: PathBuf,
    state: Arc<RwLock<DatabaseState>>,
}

impl JsonFileDatabase {
    /// Open or create a database at the given path
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        // Load existing state or create new
        let state = if path.exists() {
            let file = File::open(&path)
                .map_err(|e| Error::Persistence(format!("failed to open database: {}", e)))?;
            let reader = BufReader::new(file);
            serde_json::from_reader(reader)
                .map_err(|e| Error::Persistence(format!("failed to parse database: {}", e)))?
        } else {
            // Ensure parent directory exists
            if let Some(parent) = path.parent() {
                if !parent.exists() {
                    fs::create_dir_all(parent).map_err(|e| {
                        Error::Persistence(format!("failed to create database directory: {}", e))
                    })?;
                }
            }
            DatabaseState::default()
        };

        Ok(Self {
            path,
            state: Arc::new(RwLock::new(state)),
        })
    }

    /// Create an in-memory database (useful for testing)
    pub fn open_memory() -> Self {
        Self {
            path: PathBuf::new(),
            state: Arc::new(RwLock::new(DatabaseState::default())),
        }
    }

    /// Create a scoped persister for the given operator
    pub fn persister(&self, scope: impl Into<String>) -> JsonFilePersister {
        JsonFilePersister::new(self.path.clone(), self.state.clone(), scope.into())
    }

    /// Flush all pending writes to disk
    pub fn flush(&self) -> Result<()> {
        if self.path.as_os_str().is_empty() {
            return Ok(()); // In-memory mode, nothing to flush
        }

        let state = self
            .state
            .read()
            .map_err(|e| Error::Persistence(e.to_string()))?;

        atomic_write(&self.path, &*state)
    }
}

/// A persister backed by a JSON file, scoped to a particular operator.
pub struct JsonFilePersister {
    path: PathBuf,
    state: Arc<RwLock<DatabaseState>>,
    scope: String,
    cache: HashMap<String, serde_json::Value>,
}

impl JsonFilePersister {
    /// Create a new JsonFilePersister with the given scope
    fn new(path: PathBuf, state: Arc<RwLock<DatabaseState>>, scope: String) -> Self {
        Self {
            path,
            state,
            scope,
            cache: HashMap::new(),
        }
    }

    /// Set a raw JSON value (stored as-is, not base64 encoded)
    /// This makes the value human-readable and editable in the JSON file
    pub fn set_raw_json<T: serde::Serialize>(&mut self, key: &str, value: &T) -> Result<()> {
        let json_value = serde_json::to_value(value)
            .map_err(|e| Error::Persistence(format!("failed to serialize to JSON: {}", e)))?;
        self.cache.insert(key.to_string(), json_value);
        Ok(())
    }

    /// Get a raw JSON value
    /// Returns None if the key doesn't exist
    pub fn get_raw_json<T: serde::de::DeserializeOwned>(&self, key: &str) -> Option<T> {
        self.cache
            .get(key)
            .and_then(|v| serde_json::from_value(v.clone()).ok())
    }

    /// Try to get a raw JSON value with explicit error handling
    /// Returns Ok(None) if key doesn't exist
    /// Returns Err if key exists but deserialization fails
    pub fn try_get_raw_json<T: serde::de::DeserializeOwned>(
        &self,
        key: &str,
    ) -> std::result::Result<Option<T>, serde_json::Error> {
        match self.cache.get(key) {
            None => Ok(None),
            Some(v) => serde_json::from_value(v.clone()).map(Some),
        }
    }

    /// Try to get a legacy v0 format value (base64 encoded in file, stored as String).
    /// This reads directly from the underlying state without caching.
    /// Used only for migration from v0 to v1 format.
    /// Returns Ok(None) if key doesn't exist or is not a String (v0 format).
    /// Returns Ok(Some(String)) with the decoded UTF-8 string if found.
    /// Returns Err if base64 decoding or UTF-8 conversion fails.
    pub fn try_get_legacy_v0(&self, key: &str) -> std::result::Result<Option<String>, String> {
        let state = self
            .state
            .read()
            .map_err(|e| format!("failed to read state: {}", e))?;

        let scope_data = match state.scopes.get(&self.scope) {
            Some(data) => data,
            None => return Ok(None),
        };

        match scope_data.get(key) {
            None => Ok(None),
            Some(serde_json::Value::String(s)) => {
                // V0 format: base64 encoded string
                let decoded = base64_decode(s).map_err(|_| "invalid base64 encoding")?;
                String::from_utf8(decoded)
                    .map(Some)
                    .map_err(|e| e.to_string())
            }
            Some(_) => {
                // Not a string - this is v1 format, not v0
                Ok(None)
            }
        }
    }

    /// Delete a key from the cache
    pub fn delete(&mut self, key: &str) {
        self.cache.remove(key);
    }

    /// Load data from the underlying storage into the cache.
    pub fn load(&mut self) -> Result<()> {
        let state = self
            .state
            .read()
            .map_err(|e| Error::Persistence(e.to_string()))?;

        self.cache.clear();

        if let Some(scope_data) = state.scopes.get(&self.scope) {
            for (key, value) in scope_data {
                match value {
                    // String value = base64 encoded (legacy v0 format)
                    // Don't load into cache - read on-demand via try_get_legacy_v0()
                    serde_json::Value::String(_) => {}
                    // Object/Array value = raw JSON (v1+ format)
                    other => {
                        self.cache.insert(key.clone(), other.clone());
                    }
                }
            }
        }

        Ok(())
    }

    /// Sync the cache to the underlying storage.
    pub fn sync(&self) -> Result<()> {
        // Update shared state
        {
            let mut state = self
                .state
                .write()
                .map_err(|e| Error::Persistence(e.to_string()))?;

            let scope_data = state.scopes.entry(self.scope.clone()).or_default();
            scope_data.clear();

            // Write raw JSON values only (v1 format)
            for (key, value) in &self.cache {
                scope_data.insert(key.clone(), value.clone());
            }
        }

        // Write to disk if not in-memory mode
        if self.path.as_os_str().is_empty() {
            return Ok(());
        }

        let state = self
            .state
            .read()
            .map_err(|e| Error::Persistence(e.to_string()))?;

        atomic_write(&self.path, &*state)
    }
}

/// Write state to file atomically (write to temp, then rename)
fn atomic_write(path: &Path, state: &DatabaseState) -> Result<()> {
    use portable_atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    // Ensure parent directory exists
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            fs::create_dir_all(parent).map_err(|e| {
                Error::Persistence(format!("failed to create parent directory: {}", e))
            })?;
        }
    }

    // Use a unique temp file name to avoid race conditions between concurrent writes
    // Combine process ID with a counter to handle multi-threaded writes
    let unique_id = COUNTER.fetch_add(1, Ordering::SeqCst);
    let temp_path = path.with_extension(format!("tmp.{}.{}", std::process::id(), unique_id));

    // Write to temp file
    let file = File::create(&temp_path)
        .map_err(|e| Error::Persistence(format!("failed to create temp file: {}", e)))?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, state)
        .map_err(|e| Error::Persistence(format!("failed to write database: {}", e)))?;

    // Ensure all data is flushed to disk before rename
    use std::io::Write;
    writer
        .flush()
        .map_err(|e| Error::Persistence(format!("failed to flush database: {}", e)))?;

    // Drop the writer to close the file handle before rename
    drop(writer);

    // Rename temp to final (atomic on most filesystems)
    fs::rename(&temp_path, path)
        .map_err(|e| Error::Persistence(format!("failed to rename database file: {}", e)))?;

    Ok(())
}

/// Base64 decode string to bytes (used for reading legacy v0 format)
fn base64_decode(s: &str) -> std::result::Result<Vec<u8>, base64::DecodeError> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.decode(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_raw_json_basic() {
        use serde::{Deserialize, Serialize};

        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct TestData {
            offset: u64,
            path: String,
        }

        let db = JsonFileDatabase::open_memory();
        let mut persister = db.persister("test_operator");

        // Initially empty
        assert!(persister.get_raw_json::<TestData>("key1").is_none());

        // Set and get
        let data = TestData {
            offset: 100,
            path: "/var/log/test.log".to_string(),
        };
        persister.set_raw_json("key1", &data).unwrap();
        assert_eq!(persister.get_raw_json::<TestData>("key1"), Some(data));

        // Delete
        persister.delete("key1");
        assert!(persister.get_raw_json::<TestData>("key1").is_none());
    }

    #[test]
    fn test_raw_json_sync_and_load() {
        use serde::{Deserialize, Serialize};

        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct TestData {
            value: String,
        }

        let db = JsonFileDatabase::open_memory();

        // Set and sync
        {
            let mut persister = db.persister("test_operator");
            persister
                .set_raw_json(
                    "key1",
                    &TestData {
                        value: "value1".to_string(),
                    },
                )
                .unwrap();
            persister
                .set_raw_json(
                    "key2",
                    &TestData {
                        value: "value2".to_string(),
                    },
                )
                .unwrap();
            persister.sync().unwrap();
        }

        // Load in a new persister
        {
            let mut persister = db.persister("test_operator");
            persister.load().unwrap();
            assert_eq!(
                persister.get_raw_json::<TestData>("key1"),
                Some(TestData {
                    value: "value1".to_string()
                })
            );
            assert_eq!(
                persister.get_raw_json::<TestData>("key2"),
                Some(TestData {
                    value: "value2".to_string()
                })
            );
        }
    }

    #[test]
    fn test_scopes_are_isolated() {
        let db = JsonFileDatabase::open_memory();

        let mut persister1 = db.persister("scope1");
        let mut persister2 = db.persister("scope2");

        persister1.set_raw_json("key", &"value1").unwrap();
        persister2.set_raw_json("key", &"value2").unwrap();

        assert_eq!(
            persister1.get_raw_json::<String>("key"),
            Some("value1".to_string())
        );
        assert_eq!(
            persister2.get_raw_json::<String>("key"),
            Some("value2".to_string())
        );
    }

    #[test]
    fn test_try_get_raw_json_error_handling() {
        use serde::{Deserialize, Serialize};

        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct TestData {
            field: u64,
        }

        let db = JsonFileDatabase::open_memory();
        let mut persister = db.persister("test");

        // Store a string value
        persister.set_raw_json("key", &"not a struct").unwrap();

        // try_get_raw_json should return Err when deserialization fails
        let result = persister.try_get_raw_json::<TestData>("key");
        assert!(result.is_err());

        // Non-existent key should return Ok(None)
        let result = persister.try_get_raw_json::<TestData>("nonexistent");
        assert!(matches!(result, Ok(None)));
    }

    #[test]
    fn test_raw_json_with_file() {
        use serde::{Deserialize, Serialize};

        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct TestData {
            offset: u64,
        }

        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("test.json");

        // Create and populate
        {
            let db = JsonFileDatabase::open(&db_path).unwrap();
            let mut persister = db.persister("operator1");
            persister
                .set_raw_json("data", &TestData { offset: 12345 })
                .unwrap();
            persister.sync().unwrap();
        }

        // Reopen and verify
        {
            let db = JsonFileDatabase::open(&db_path).unwrap();
            let mut persister = db.persister("operator1");
            persister.load().unwrap();
            assert_eq!(
                persister.get_raw_json::<TestData>("data"),
                Some(TestData { offset: 12345 })
            );
        }
    }

    #[test]
    fn test_legacy_v0_read() {
        use base64::Engine;

        // Write v0 format directly to file (base64 encoded JSON string)
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("test.json");

        // V0 format: JSON content base64 encoded
        let v0_json_content = r#"{"files":[{"dev":1,"ino":100,"offset":500}]}"#;
        let v0_base64 = base64::engine::general_purpose::STANDARD.encode(v0_json_content);

        // Write v0 format directly to the database file
        let v0_db_content = serde_json::json!({
            "scopes": {
                "test_scope": {
                    "known_files": v0_base64
                }
            }
        });
        std::fs::write(
            &db_path,
            serde_json::to_string_pretty(&v0_db_content).unwrap(),
        )
        .unwrap();

        // Open and verify we can read v0 format
        let db = JsonFileDatabase::open(&db_path).unwrap();
        let mut persister = db.persister("test_scope");
        persister.load().unwrap();

        // V0 data should be readable via try_get_legacy_v0
        let v0_data = persister.try_get_legacy_v0("known_files").unwrap();
        assert!(v0_data.is_some());
        let v0_data = v0_data.unwrap();
        assert!(v0_data.contains(r#""dev":1"#));
        assert!(v0_data.contains(r#""ino":100"#));
        assert!(v0_data.contains(r#""offset":500"#));

        // V0 data should NOT be in raw_json_cache (get_raw_json returns None)
        assert!(
            persister
                .get_raw_json::<serde_json::Value>("known_files")
                .is_none()
        );
    }

    #[test]
    fn test_legacy_v0_not_found_for_v1_data() {
        let db = JsonFileDatabase::open_memory();
        let mut persister = db.persister("test");

        // Store v1 format data (object, not string)
        persister
            .set_raw_json("data", &serde_json::json!({"field": 123}))
            .unwrap();
        persister.sync().unwrap();

        // Reload
        let mut persister2 = db.persister("test");
        persister2.load().unwrap();

        // try_get_legacy_v0 should return None for v1 data (it's not a base64 string)
        let result = persister2.try_get_legacy_v0("data").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_base64_decode() {
        use base64::Engine;

        // Test that base64 decode works correctly
        let test_cases = vec![
            b"".to_vec(),
            b"f".to_vec(),
            b"fo".to_vec(),
            b"foo".to_vec(),
            b"foob".to_vec(),
            b"fooba".to_vec(),
            b"foobar".to_vec(),
            vec![0, 1, 2, 255, 254, 253],
        ];

        for original in test_cases {
            let encoded = base64::engine::general_purpose::STANDARD.encode(&original);
            let decoded = base64_decode(&encoded).unwrap();
            assert_eq!(original, decoded, "Failed for {:?}", original);
        }
    }
}
