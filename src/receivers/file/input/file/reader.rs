// SPDX-License-Identifier: Apache-2.0

use std::fs::File;
use std::io::{self, BufRead, BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};

/// FileReader manages reading from a single file
pub struct FileReader {
    /// Path to the file
    path: PathBuf,
    /// Current offset in bytes
    offset: u64,
    /// The open file handle (kept open for reuse)
    file: Option<File>,
    /// Maximum size of a single log line
    max_log_size: usize,
    /// Whether we've reached EOF
    eof: bool,
    /// Reusable line buffer to avoid allocations
    line_buffer: String,
}

impl FileReader {
    /// Create a new FileReader for a file
    pub fn new(path: impl AsRef<Path>, offset: u64, max_log_size: usize) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::open(&path)?;

        Ok(Self {
            path,
            offset,
            file: Some(file),
            max_log_size,
            eof: false,
            line_buffer: String::with_capacity(1024),
        })
    }

    /// Create a FileReader from an existing file handle.
    /// This avoids reopening the file and is more efficient when the caller
    /// already has an open handle.
    pub fn from_file(file: File, path: PathBuf, offset: u64, max_log_size: usize) -> Self {
        Self {
            path,
            offset,
            file: Some(file),
            max_log_size,
            eof: false,
            line_buffer: String::with_capacity(1024),
        }
    }

    /// Get the file path
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get the file name
    pub fn file_name(&self) -> Option<&str> {
        self.path.file_name().and_then(|n| n.to_str())
    }

    /// Get the current offset
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Reopen the file handle (e.g., after file rotation or to refresh)
    pub fn reopen(&mut self) -> io::Result<()> {
        self.file = Some(File::open(&self.path)?);
        Ok(())
    }

    /// Check if we're at EOF
    pub fn is_eof(&self) -> bool {
        self.eof
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
        let mut lines = Vec::new();
        self.read_lines_into(|line| {
            lines.push(line);
        })?;
        Ok(lines)
    }

    /// Read new lines from the file, calling the callback for each line.
    /// This avoids allocating a Vec<String> when the caller can process lines directly.
    pub fn read_lines_into<F>(&mut self, mut on_line: F) -> io::Result<()>
    where
        F: FnMut(String),
    {
        self.eof = false;

        // Take the file handle temporarily - we'll put it back after
        let mut file = match self.file.take() {
            Some(f) => f,
            None => return Ok(()),
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

                    // Check max log size and call callback
                    if line.len() > self.max_log_size {
                        // Truncate the line
                        let truncated = line.chars().take(self.max_log_size).collect::<String>();
                        on_line(truncated);
                    } else {
                        on_line(line.to_string());
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

        let reader = FileReader::new(file.path(), 0, 1024 * 1024).unwrap();

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

        let mut reader = FileReader::new(file.path(), 0, 1024 * 1024).unwrap();

        let lines = reader.read_lines().unwrap();
        assert_eq!(lines, vec!["line 1", "line 2", "line 3"]);
        assert!(reader.is_eof());
    }

    #[test]
    fn test_reader_incremental_read() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "line 1").unwrap();
        file.flush().unwrap();

        let mut reader = FileReader::new(file.path(), 0, 1024 * 1024).unwrap();

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
        reader.reopen().unwrap();

        // Second read should only get new lines
        let lines = reader.read_lines().unwrap();
        assert_eq!(lines, vec!["line 2"]);
    }

    #[test]
    fn test_reader_initialize_offset_end() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "existing content").unwrap();
        file.flush().unwrap();

        let mut reader = FileReader::new(file.path(), 0, 1024 * 1024).unwrap();
        reader.initialize_offset(false).unwrap(); // start_at: end

        // Offset should be at the end of the file
        assert!(reader.offset() > 0);

        // Should not read existing content
        let lines = reader.read_lines().unwrap();
        assert!(lines.is_empty());
    }

    #[test]
    fn test_reader_resume_from_offset() {
        // Create a file with 5 lines
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "line 1").unwrap(); // 7 bytes (including \n)
        writeln!(file, "line 2").unwrap(); // 7 bytes
        writeln!(file, "line 3").unwrap(); // 7 bytes
        writeln!(file, "line 4").unwrap(); // 7 bytes
        writeln!(file, "line 5").unwrap(); // 7 bytes
        file.flush().unwrap();

        // Start at offset after first 3 lines (simulating persisted state)
        let persisted_offset: u64 = 21; // 3 lines * 7 bytes = 21 bytes

        // Create reader starting at persisted offset
        let mut reader = FileReader::new(file.path(), persisted_offset, 1024 * 1024).unwrap();

        // Verify reader starts at persisted offset
        assert_eq!(reader.offset(), persisted_offset);

        // Read lines - should only get lines 4 and 5 (not 1, 2, 3)
        let lines = reader.read_lines().unwrap();

        assert_eq!(lines.len(), 2, "Should only read 2 lines after offset");
        assert_eq!(lines[0], "line 4", "First line should be 'line 4'");
        assert_eq!(lines[1], "line 5", "Second line should be 'line 5'");

        // Verify offset is now at end of file
        assert_eq!(reader.offset(), 35); // 5 lines * 7 bytes = 35 bytes
        assert!(reader.is_eof());

        // Reading again should return no lines (no duplicates)
        let lines_again = reader.read_lines().unwrap();
        assert!(
            lines_again.is_empty(),
            "Should not return any lines on second read"
        );
    }

    #[test]
    fn test_reader_with_new_content() {
        // Create a file with initial content
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "old 1").unwrap(); // 6 bytes
        writeln!(file, "old 2").unwrap(); // 6 bytes
        writeln!(file, "old 3").unwrap(); // 6 bytes
        file.flush().unwrap();

        // Start at end of file (simulating we processed all content before)
        let persisted_offset: u64 = 18; // 3 lines * 6 bytes = 18 bytes

        // Create reader at persisted offset
        let mut reader = FileReader::new(file.path(), persisted_offset, 1024 * 1024).unwrap();

        // Initially should read nothing (we're at end of existing content)
        let lines = reader.read_lines().unwrap();
        assert!(
            lines.is_empty(),
            "Should not read old content after resumption"
        );

        // Now append new content to the file
        {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(file.path())
                .unwrap();
            writeln!(f, "new 1").unwrap();
            writeln!(f, "new 2").unwrap();
            f.flush().unwrap();
        }

        // Reopen file handle to see new content
        reader.reopen().unwrap();

        // Read again - should only get the new lines
        let new_lines = reader.read_lines().unwrap();
        assert_eq!(new_lines.len(), 2, "Should read 2 new lines");
        assert_eq!(new_lines[0], "new 1");
        assert_eq!(new_lines[1], "new 2");

        // Verify no duplicates on subsequent read
        let lines_again = reader.read_lines().unwrap();
        assert!(lines_again.is_empty(), "Should not return duplicates");
    }
}
