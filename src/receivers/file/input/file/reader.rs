use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{self, BufRead, BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use super::fingerprint::Fingerprint;

/// State for a single file being read
#[derive(Debug, Serialize, Deserialize)]
pub struct FileReaderState {
    /// Fingerprint for identifying this file
    pub fingerprint: Fingerprint,
    /// Current read offset in bytes
    pub offset: u64,
}

/// FileReader manages reading from a single file
pub struct FileReader {
    /// Path to the file
    path: PathBuf,
    /// Current fingerprint
    fingerprint: Fingerprint,
    /// Current offset in bytes
    offset: u64,
    /// The open file handle (kept open for reuse)
    file: Option<File>,
    /// Maximum size of the fingerprint
    fingerprint_size: usize,
    /// Maximum size of a single log line
    max_log_size: usize,
    /// Generation counter for tracking file age
    generation: u64,
    /// Whether we've reached EOF
    eof: bool,
    /// Reusable line buffer to avoid allocations
    line_buffer: String,
}

impl FileReader {
    /// Create a new FileReader for a file
    pub fn new(
        path: impl AsRef<Path>,
        fingerprint: Fingerprint,
        offset: u64,
        fingerprint_size: usize,
        max_log_size: usize,
    ) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::open(&path)?;

        Ok(Self {
            path,
            fingerprint,
            offset,
            file: Some(file),
            fingerprint_size,
            max_log_size,
            generation: 0,
            eof: false,
            line_buffer: String::with_capacity(1024),
        })
    }

    /// Create a FileReader from a saved state
    pub fn from_state(
        state: FileReaderState,
        path: impl AsRef<Path>,
        fingerprint_size: usize,
        max_log_size: usize,
    ) -> io::Result<Self> {
        Self::new(
            path,
            state.fingerprint,
            state.offset,
            fingerprint_size,
            max_log_size,
        )
    }

    /// Get the file path
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get the file name
    pub fn file_name(&self) -> Option<&str> {
        self.path.file_name().and_then(|n| n.to_str())
    }

    /// Get the current fingerprint
    pub fn fingerprint(&self) -> &Fingerprint {
        &self.fingerprint
    }

    /// Get the current offset
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Get the generation (for tracking file age)
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Increment the generation
    pub fn increment_generation(&mut self) {
        self.generation += 1;
    }

    /// Check if we're at EOF
    pub fn is_eof(&self) -> bool {
        self.eof
    }

    /// Get the saved state for persistence
    pub fn state(&self) -> FileReaderState {
        FileReaderState {
            fingerprint: self.fingerprint.clone(),
            offset: self.offset,
        }
    }

    /// Initialize the offset based on start_at configuration
    pub fn initialize_offset(&mut self, start_at_beginning: bool) -> io::Result<()> {
        if !start_at_beginning {
            if let Some(ref file) = self.file {
                let metadata = file.metadata()?;
                self.offset = metadata.len();
            }
        }
        Ok(())
    }

    /// Read all new lines from the file
    pub fn read_lines(&mut self) -> io::Result<Vec<String>> {
        self.eof = false;
        let mut lines = Vec::new();

        // Take the file handle temporarily - we'll put it back after
        let mut file = match self.file.take() {
            Some(f) => f,
            None => return Ok(lines),
        };

        // Seek to the current offset
        file.seek(SeekFrom::Start(self.offset))?;

        // Create a buffered reader
        let mut reader = BufReader::new(&mut file);

        loop {
            self.line_buffer.clear();
            match reader.read_line(&mut self.line_buffer) {
                Ok(0) => {
                    // EOF reached
                    break;
                }
                Ok(bytes_read) => {
                    self.offset += bytes_read as u64;

                    // Remove trailing newline
                    let line = self
                        .line_buffer
                        .trim_end_matches('\n')
                        .trim_end_matches('\r');

                    if line.is_empty() {
                        continue;
                    }

                    // Check max log size
                    if line.len() > self.max_log_size {
                        // Truncate the line
                        let truncated = line.chars().take(self.max_log_size).collect::<String>();
                        lines.push(truncated);
                    } else {
                        lines.push(line.to_string());
                    }
                }
                Err(e) => {
                    if e.kind() == io::ErrorKind::InvalidData {
                        // Skip invalid UTF-8 lines - advance past this byte
                        self.offset += 1;
                        continue;
                    }
                    // Put file back before returning error
                    drop(reader);
                    self.file = Some(file);
                    return Err(e);
                }
            }
        }

        // Put the file handle back (it stays open for reuse)
        drop(reader);
        self.file = Some(file);

        self.eof = true;
        Ok(lines)
    }

    /// Refresh the fingerprint from the current file content
    pub fn refresh_fingerprint(&mut self) -> io::Result<()> {
        if let Some(ref mut file) = self.file {
            if self.fingerprint.len() < self.fingerprint_size {
                self.fingerprint.extend(file, self.fingerprint_size)?;
            }
        }
        Ok(())
    }

    /// Close the file handle
    pub fn close(&mut self) {
        self.file = None;
    }
}

impl Drop for FileReader {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_reader_new() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "line 1").unwrap();
        writeln!(file, "line 2").unwrap();
        file.flush().unwrap();

        let mut f = File::open(file.path()).unwrap();
        let fp = Fingerprint::new(&mut f, 1000).unwrap();

        let reader = FileReader::new(file.path(), fp, 0, 1000, 1024 * 1024).unwrap();

        assert_eq!(reader.offset(), 0);
        assert!(!reader.is_eof());
    }

    #[test]
    fn test_reader_read_lines() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "line 1").unwrap();
        writeln!(file, "line 2").unwrap();
        writeln!(file, "line 3").unwrap();
        file.flush().unwrap();

        let mut f = File::open(file.path()).unwrap();
        let fp = Fingerprint::new(&mut f, 1000).unwrap();

        let mut reader = FileReader::new(file.path(), fp, 0, 1000, 1024 * 1024).unwrap();

        let lines = reader.read_lines().unwrap();
        assert_eq!(lines, vec!["line 1", "line 2", "line 3"]);
        assert!(reader.is_eof());
    }

    #[test]
    fn test_reader_incremental_read() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "line 1").unwrap();
        file.flush().unwrap();

        let mut f = File::open(file.path()).unwrap();
        let fp = Fingerprint::new(&mut f, 1000).unwrap();

        let mut reader = FileReader::new(file.path(), fp, 0, 1000, 1024 * 1024).unwrap();

        // First read
        let lines = reader.read_lines().unwrap();
        assert_eq!(lines, vec!["line 1"]);

        // Append more content
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(file.path())
            .unwrap();
        writeln!(f, "line 2").unwrap();
        f.flush().unwrap();

        // Re-open the reader's file handle
        reader.file = Some(File::open(file.path()).unwrap());

        // Second read should only get new lines
        let lines = reader.read_lines().unwrap();
        assert_eq!(lines, vec!["line 2"]);
    }

    #[test]
    fn test_reader_initialize_offset_end() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "existing content").unwrap();
        file.flush().unwrap();

        let mut f = File::open(file.path()).unwrap();
        let fp = Fingerprint::new(&mut f, 1000).unwrap();

        let mut reader = FileReader::new(file.path(), fp, 0, 1000, 1024 * 1024).unwrap();
        reader.initialize_offset(false).unwrap(); // start_at: end

        // Offset should be at the end of the file
        assert!(reader.offset() > 0);

        // Should not read existing content
        let lines = reader.read_lines().unwrap();
        assert!(lines.is_empty());
    }

    #[test]
    fn test_reader_state_serialization() {
        let fp = Fingerprint::from_bytes(b"test content".to_vec());
        let state = FileReaderState {
            fingerprint: fp,
            offset: 100,
        };

        let json = serde_json::to_string(&state).unwrap();
        let loaded: FileReaderState = serde_json::from_str(&json).unwrap();

        assert_eq!(loaded.offset, 100);
        assert_eq!(loaded.fingerprint.bytes(), b"test content");
    }
}
