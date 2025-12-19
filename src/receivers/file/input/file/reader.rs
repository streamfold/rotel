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
    /// The open file handle
    file: Option<File>,
    /// Buffered reader
    reader: Option<BufReader<File>>,
    /// Maximum size of the fingerprint
    fingerprint_size: usize,
    /// Maximum size of a single log line
    max_log_size: usize,
    /// Generation counter for tracking file age
    generation: u64,
    /// Whether we've reached EOF
    eof: bool,
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
            reader: None,
            fingerprint_size,
            max_log_size,
            generation: 0,
            eof: false,
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

        // Get the file, or return empty if not available
        let file = match self.file.take() {
            Some(f) => f,
            None => return Ok(lines),
        };

        // Seek to the current offset
        let mut file = file;
        file.seek(SeekFrom::Start(self.offset))?;

        // Create a buffered reader
        let reader = BufReader::new(file);
        let mut current_offset = self.offset;

        for line_result in reader.lines() {
            match line_result {
                Ok(line) => {
                    // Track bytes read (line + newline)
                    let bytes_read = line.len() as u64 + 1;
                    current_offset += bytes_read;

                    // Check max log size
                    if line.len() > self.max_log_size {
                        // Truncate the line
                        let truncated = line.chars().take(self.max_log_size).collect::<String>();
                        lines.push(truncated);
                    } else if !line.is_empty() {
                        lines.push(line);
                    }

                    // Update fingerprint as we read
                    self.update_fingerprint_progress(current_offset);
                }
                Err(e) => {
                    if e.kind() == io::ErrorKind::InvalidData {
                        // Skip invalid UTF-8 lines
                        continue;
                    }
                    return Err(e);
                }
            }
        }

        self.offset = current_offset;
        self.eof = true;

        // Re-open the file for next read
        self.file = Some(File::open(&self.path)?);

        Ok(lines)
    }

    /// Update the fingerprint as we read more of the file
    fn update_fingerprint_progress(&mut self, current_offset: u64) {
        // If fingerprint is not yet full and we've read past its end, it would be
        // extended on the next fingerprint check. For simplicity, we don't update
        // it during line reading - it will be updated on the next poll cycle.
        let _ = current_offset;
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
        self.reader = None;
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
