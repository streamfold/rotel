use crate::receivers::file::error::Result;

/// Persister is a trait for storing and retrieving persistent data.
/// Each persister is scoped to a particular operator, allowing multiple
/// operators to store data without conflicts.
pub trait Persister: Send + Sync {
    /// Get a value from the cache by key
    fn get(&self, key: &str) -> Option<Vec<u8>>;

    /// Set a value in the cache
    fn set(&mut self, key: &str, value: Vec<u8>);

    /// Delete a key from the cache
    fn delete(&mut self, key: &str);

    /// Load data from the underlying storage into the cache
    fn load(&mut self) -> Result<()>;

    /// Sync the cache to the underlying storage
    fn sync(&self) -> Result<()>;
}

/// Extension trait for typed get/set operations
pub trait PersisterExt: Persister {
    /// Get a value and deserialize it from JSON.
    /// Returns None if key doesn't exist OR if deserialization fails.
    /// Use `try_get_json` if you need to distinguish these cases.
    fn get_json<T: serde::de::DeserializeOwned>(&self, key: &str) -> Option<T> {
        let bytes = self.get(key)?;
        serde_json::from_slice(&bytes).ok()
    }

    /// Get a value and deserialize it from JSON, with explicit error handling.
    /// Returns Ok(None) if key doesn't exist.
    /// Returns Err if key exists but deserialization fails (corrupted data).
    fn try_get_json<T: serde::de::DeserializeOwned>(
        &self,
        key: &str,
    ) -> std::result::Result<Option<T>, serde_json::Error> {
        match self.get(key) {
            None => Ok(None),
            Some(bytes) => serde_json::from_slice(&bytes).map(Some),
        }
    }

    /// Set a value by serializing it to JSON
    fn set_json<T: serde::Serialize>(&mut self, key: &str, value: &T) -> Result<()> {
        let bytes = serde_json::to_vec(value)?;
        self.set(key, bytes);
        Ok(())
    }

    /// Get a u64 value (stored as little-endian bytes)
    fn get_u64(&self, key: &str) -> Option<u64> {
        let bytes = self.get(key)?;
        if bytes.len() == 8 {
            Some(u64::from_le_bytes(bytes.try_into().ok()?))
        } else {
            None
        }
    }

    /// Set a u64 value (stored as little-endian bytes)
    fn set_u64(&mut self, key: &str, value: u64) {
        self.set(key, value.to_le_bytes().to_vec());
    }

    /// Get a string value
    fn get_string(&self, key: &str) -> Option<String> {
        let bytes = self.get(key)?;
        String::from_utf8(bytes).ok()
    }

    /// Set a string value
    fn set_string(&mut self, key: &str, value: &str) {
        self.set(key, value.as_bytes().to_vec());
    }
}

// Implement PersisterExt for all Persisters
impl<T: Persister + ?Sized> PersisterExt for T {}

/// Mock persister for testing
#[cfg(test)]
pub struct MockPersister {
    data: std::collections::HashMap<String, Vec<u8>>,
}

#[cfg(test)]
impl MockPersister {
    /// Create an empty mock persister
    pub fn new() -> Self {
        Self {
            data: std::collections::HashMap::new(),
        }
    }

    /// Create a mock persister with corrupted data for a key
    pub fn with_corrupted_data(key: &str) -> Self {
        let mut data = std::collections::HashMap::new();
        // Invalid JSON that will fail to deserialize
        data.insert(key.to_string(), b"not valid json {{{".to_vec());
        Self { data }
    }
}

#[cfg(test)]
impl Persister for MockPersister {
    fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.data.get(key).cloned()
    }

    fn set(&mut self, key: &str, value: Vec<u8>) {
        self.data.insert(key.to_string(), value);
    }

    fn delete(&mut self, key: &str) {
        self.data.remove(key);
    }

    fn load(&mut self) -> Result<()> {
        Ok(())
    }

    fn sync(&self) -> Result<()> {
        Ok(())
    }
}
