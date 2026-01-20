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

use super::store::Persister;
use crate::receivers::file::error::{Error, Result};

/// State stored in the JSON file
#[derive(Debug, Default, Serialize, Deserialize)]
struct DatabaseState {
    /// Map of scope -> (key -> base64-encoded value)
    scopes: HashMap<String, HashMap<String, String>>,
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
    cache: HashMap<String, Vec<u8>>,
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
}

impl Persister for JsonFilePersister {
    fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.cache.get(key).cloned()
    }

    fn set(&mut self, key: &str, value: Vec<u8>) {
        self.cache.insert(key.to_string(), value);
    }

    fn delete(&mut self, key: &str) {
        self.cache.remove(key);
    }

    fn load(&mut self) -> Result<()> {
        let state = self
            .state
            .read()
            .map_err(|e| Error::Persistence(e.to_string()))?;

        self.cache.clear();

        if let Some(scope_data) = state.scopes.get(&self.scope) {
            for (key, value_b64) in scope_data {
                if let Ok(value) = base64_decode(value_b64) {
                    self.cache.insert(key.clone(), value);
                }
            }
        }

        Ok(())
    }

    fn sync(&self) -> Result<()> {
        // Update shared state
        {
            let mut state = self
                .state
                .write()
                .map_err(|e| Error::Persistence(e.to_string()))?;

            let scope_data = state.scopes.entry(self.scope.clone()).or_default();
            scope_data.clear();

            for (key, value) in &self.cache {
                scope_data.insert(key.clone(), base64_encode(value));
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
    use std::sync::atomic::{AtomicU64, Ordering};
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

/// Base64 encode bytes for JSON storage
fn base64_encode(data: &[u8]) -> String {
    use std::io::Write;
    let mut buf = Vec::new();
    let mut encoder = Base64Encoder::new(&mut buf);
    encoder.write_all(data).unwrap();
    encoder.finish();
    String::from_utf8(buf).unwrap()
}

/// Base64 decode string to bytes
fn base64_decode(s: &str) -> std::result::Result<Vec<u8>, ()> {
    base64_decode_impl(s.as_bytes())
}

// Simple base64 implementation to avoid adding another dependency

const BASE64_CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

struct Base64Encoder<'a> {
    output: &'a mut Vec<u8>,
    buffer: u32,
    bits: u8,
}

impl<'a> Base64Encoder<'a> {
    fn new(output: &'a mut Vec<u8>) -> Self {
        Self {
            output,
            buffer: 0,
            bits: 0,
        }
    }

    fn finish(mut self) {
        if self.bits > 0 {
            self.buffer <<= 6 - self.bits;
            self.output
                .push(BASE64_CHARS[(self.buffer & 0x3F) as usize]);
            let padding = (3 - (self.bits / 2) % 3) % 3;
            for _ in 0..padding {
                self.output.push(b'=');
            }
        }
    }
}

impl std::io::Write for Base64Encoder<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        for &byte in buf {
            self.buffer = (self.buffer << 8) | byte as u32;
            self.bits += 8;
            while self.bits >= 6 {
                self.bits -= 6;
                self.output
                    .push(BASE64_CHARS[((self.buffer >> self.bits) & 0x3F) as usize]);
            }
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn base64_decode_impl(input: &[u8]) -> std::result::Result<Vec<u8>, ()> {
    let mut output = Vec::new();
    let mut buffer: u32 = 0;
    let mut bits: u8 = 0;

    for &byte in input {
        if byte == b'=' {
            break;
        }

        let value = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            b' ' | b'\n' | b'\r' | b'\t' => continue,
            _ => return Err(()),
        };

        buffer = (buffer << 6) | value as u32;
        bits += 6;

        if bits >= 8 {
            bits -= 8;
            output.push((buffer >> bits) as u8);
        }
    }

    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::super::PersisterExt;
    use super::*;

    #[test]
    fn test_json_file_persister_basic() {
        let db = JsonFileDatabase::open_memory();
        let mut persister = db.persister("test_operator");

        // Initially empty
        assert!(persister.get("key1").is_none());

        // Set and get
        persister.set("key1", b"value1".to_vec());
        assert_eq!(persister.get("key1"), Some(b"value1".to_vec()));

        // Delete
        persister.delete("key1");
        assert!(persister.get("key1").is_none());
    }

    #[test]
    fn test_json_file_persister_sync_and_load() {
        let db = JsonFileDatabase::open_memory();

        // Set and sync
        {
            let mut persister = db.persister("test_operator");
            persister.set("key1", b"value1".to_vec());
            persister.set("key2", b"value2".to_vec());
            persister.sync().unwrap();
        }

        // Load in a new persister
        {
            let mut persister = db.persister("test_operator");
            persister.load().unwrap();
            assert_eq!(persister.get("key1"), Some(b"value1".to_vec()));
            assert_eq!(persister.get("key2"), Some(b"value2".to_vec()));
        }
    }

    #[test]
    fn test_json_file_persister_scopes_are_isolated() {
        let db = JsonFileDatabase::open_memory();

        let mut persister1 = db.persister("scope1");
        let mut persister2 = db.persister("scope2");

        persister1.set("key", b"value1".to_vec());
        persister2.set("key", b"value2".to_vec());

        assert_eq!(persister1.get("key"), Some(b"value1".to_vec()));
        assert_eq!(persister2.get("key"), Some(b"value2".to_vec()));
    }

    #[test]
    fn test_json_file_persister_ext_u64() {
        let db = JsonFileDatabase::open_memory();
        let mut persister = db.persister("test");

        persister.set_u64("offset", 12345);
        assert_eq!(persister.get_u64("offset"), Some(12345));
    }

    #[test]
    fn test_json_file_persister_ext_string() {
        let db = JsonFileDatabase::open_memory();
        let mut persister = db.persister("test");

        persister.set_string("cursor", "abc123");
        assert_eq!(persister.get_string("cursor"), Some("abc123".to_string()));
    }

    #[test]
    fn test_json_file_persister_ext_json() {
        use serde::{Deserialize, Serialize};

        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct TestData {
            offset: u64,
            path: String,
        }

        let db = JsonFileDatabase::open_memory();
        let mut persister = db.persister("test");

        let data = TestData {
            offset: 100,
            path: "/var/log/test.log".to_string(),
        };

        persister.set_json("data", &data).unwrap();
        let loaded: TestData = persister.get_json("data").unwrap();
        assert_eq!(loaded, data);
    }

    #[test]
    fn test_json_file_persister_with_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("test.json");

        // Create and populate
        {
            let db = JsonFileDatabase::open(&db_path).unwrap();
            let mut persister = db.persister("operator1");
            persister.set("offset", b"12345".to_vec());
            persister.sync().unwrap();
        }

        // Reopen and verify
        {
            let db = JsonFileDatabase::open(&db_path).unwrap();
            let mut persister = db.persister("operator1");
            persister.load().unwrap();
            assert_eq!(persister.get("offset"), Some(b"12345".to_vec()));
        }
    }

    #[test]
    fn test_base64_roundtrip() {
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
            let encoded = base64_encode(&original);
            let decoded = base64_decode(&encoded).unwrap();
            assert_eq!(original, decoded, "Failed for {:?}", original);
        }
    }
}
