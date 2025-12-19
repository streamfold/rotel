use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};

/// A fingerprint identifies a file by the first N bytes of its content.
/// This allows tracking files even if they are renamed or moved.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Fingerprint {
    /// The first N bytes of the file
    first_bytes: Vec<u8>,
}

impl Fingerprint {
    /// Create a new fingerprint from a file
    pub fn new(file: &mut File, size: usize) -> io::Result<Self> {
        let mut buf = vec![0u8; size];

        // Seek to the beginning of the file
        file.seek(SeekFrom::Start(0))?;

        // Read up to `size` bytes
        let n = match file.read(&mut buf) {
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => 0,
            Err(e) => return Err(e),
        };

        buf.truncate(n);

        Ok(Self { first_bytes: buf })
    }

    /// Create a fingerprint from raw bytes (for deserialization)
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self { first_bytes: bytes }
    }

    /// Get the fingerprint bytes
    pub fn bytes(&self) -> &[u8] {
        &self.first_bytes
    }

    /// Get the length of the fingerprint
    pub fn len(&self) -> usize {
        self.first_bytes.len()
    }

    /// Check if the fingerprint is empty
    pub fn is_empty(&self) -> bool {
        self.first_bytes.is_empty()
    }

    /// Check if this fingerprint starts with another fingerprint.
    /// This is used to track files as they grow - a new fingerprint
    /// that starts with an old one likely represents the same file
    /// after it has grown.
    pub fn starts_with(&self, other: &Fingerprint) -> bool {
        let other_len = other.first_bytes.len();

        // Empty fingerprints never match
        if other_len == 0 {
            return false;
        }

        // If the other fingerprint is longer, it can't be a prefix
        if other_len > self.first_bytes.len() {
            return false;
        }

        // Check if the first `other_len` bytes match
        self.first_bytes[..other_len] == other.first_bytes[..other_len]
    }

    /// Extend the fingerprint with additional bytes read from the file.
    /// This is called as more content is read from a file that started small.
    pub fn extend(&mut self, file: &mut File, max_size: usize) -> io::Result<()> {
        if self.first_bytes.len() >= max_size {
            return Ok(());
        }

        let current_len = self.first_bytes.len();
        let remaining = max_size - current_len;

        // Seek to where we left off
        file.seek(SeekFrom::Start(current_len as u64))?;

        // Read more bytes
        let mut buf = vec![0u8; remaining];
        let n = file.read(&mut buf)?;

        // Extend the fingerprint
        self.first_bytes.extend_from_slice(&buf[..n]);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_fingerprint_new() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"hello world").unwrap();
        file.flush().unwrap();

        let mut f = file.reopen().unwrap();
        let fp = Fingerprint::new(&mut f, 1000).unwrap();

        assert_eq!(fp.bytes(), b"hello world");
        assert_eq!(fp.len(), 11);
    }

    #[test]
    fn test_fingerprint_truncates_to_size() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"hello world this is a longer message")
            .unwrap();
        file.flush().unwrap();

        let mut f = file.reopen().unwrap();
        let fp = Fingerprint::new(&mut f, 5).unwrap();

        assert_eq!(fp.bytes(), b"hello");
        assert_eq!(fp.len(), 5);
    }

    #[test]
    fn test_fingerprint_starts_with() {
        let fp1 = Fingerprint::from_bytes(b"hello world".to_vec());
        let fp2 = Fingerprint::from_bytes(b"hello".to_vec());
        let fp3 = Fingerprint::from_bytes(b"world".to_vec());
        let fp_empty = Fingerprint::from_bytes(vec![]);

        // fp1 starts with fp2 (fp1 is longer and has the same prefix)
        assert!(fp1.starts_with(&fp2));

        // fp2 does not start with fp1 (fp2 is shorter)
        assert!(!fp2.starts_with(&fp1));

        // fp1 does not start with fp3 (different content)
        assert!(!fp1.starts_with(&fp3));

        // Empty fingerprints never match
        assert!(!fp1.starts_with(&fp_empty));
        assert!(!fp_empty.starts_with(&fp1));
    }

    #[test]
    fn test_fingerprint_extend() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"hello world").unwrap();
        file.flush().unwrap();

        let mut f = file.reopen().unwrap();

        // Create a small fingerprint
        let mut fp = Fingerprint::new(&mut f, 5).unwrap();
        assert_eq!(fp.bytes(), b"hello");

        // Extend it
        fp.extend(&mut f, 11).unwrap();
        assert_eq!(fp.bytes(), b"hello world");
    }

    #[test]
    fn test_fingerprint_serde() {
        let fp = Fingerprint::from_bytes(b"hello world".to_vec());
        let json = serde_json::to_string(&fp).unwrap();
        let fp2: Fingerprint = serde_json::from_str(&json).unwrap();
        assert_eq!(fp, fp2);
    }
}
