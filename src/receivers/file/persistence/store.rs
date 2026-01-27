#[cfg(test)]
use crate::receivers::file::error::Result;

/// Mock persister for testing
#[cfg(test)]
pub struct MockPersister {
    data: std::collections::HashMap<String, serde_json::Value>,
}

#[cfg(test)]
impl MockPersister {
    /// Create an empty mock persister
    pub fn new() -> Self {
        Self {
            data: std::collections::HashMap::new(),
        }
    }

    /// Set a raw JSON value
    pub fn set_raw_json<T: serde::Serialize>(&mut self, key: &str, value: &T) -> Result<()> {
        let json_value = serde_json::to_value(value)?;
        self.data.insert(key.to_string(), json_value);
        Ok(())
    }

    /// Get a raw JSON value
    pub fn get_raw_json<T: serde::de::DeserializeOwned>(&self, key: &str) -> Option<T> {
        self.data
            .get(key)
            .and_then(|v| serde_json::from_value(v.clone()).ok())
    }

    /// Try to get a raw JSON value with explicit error handling
    pub fn try_get_raw_json<T: serde::de::DeserializeOwned>(
        &self,
        key: &str,
    ) -> std::result::Result<Option<T>, serde_json::Error> {
        match self.data.get(key) {
            None => Ok(None),
            Some(v) => serde_json::from_value(v.clone()).map(Some),
        }
    }

    /// Load data (no-op for mock)
    pub fn load(&mut self) -> Result<()> {
        Ok(())
    }

    /// Sync data (no-op for mock)
    pub fn sync(&self) -> Result<()> {
        Ok(())
    }
}
