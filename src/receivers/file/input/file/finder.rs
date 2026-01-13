use glob::glob;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::receivers::file::error::{Error, Result};

/// Trait for finding files to process
pub trait FileFinder: Send {
    /// Find all files that should be processed
    fn find_files(&self) -> Result<Vec<PathBuf>>;
}

/// GlobFileFinder finds files matching include patterns while excluding others
#[derive(Debug, Clone)]
pub struct GlobFileFinder {
    include: Vec<String>,
    /// Pre-compiled exclude patterns for efficient matching
    exclude_patterns: Vec<glob::Pattern>,
}

impl GlobFileFinder {
    /// Create a new GlobFileFinder with the given include and exclude patterns.
    /// Both include and exclude patterns are validated at construction time.
    /// Returns an error if any include pattern is invalid.
    /// Invalid exclude patterns are logged and skipped.
    pub fn new(include: Vec<String>, exclude: Vec<String>) -> Result<Self> {
        // Pre-validate include patterns - these are required, so invalid patterns are errors
        for pattern in &include {
            glob::Pattern::new(pattern).map_err(|e| {
                Error::InvalidGlob(format!("invalid include pattern '{}': {}", pattern, e))
            })?;
        }

        // Pre-compile exclude patterns - invalid patterns are warnings (non-fatal)
        let exclude_patterns = exclude
            .iter()
            .filter_map(|pattern| match glob::Pattern::new(pattern) {
                Ok(p) => Some(p),
                Err(e) => {
                    tracing::warn!("Invalid exclude pattern '{}': {}", pattern, e);
                    None
                }
            })
            .collect();

        Ok(Self {
            include,
            exclude_patterns,
        })
    }

    /// Check if a path matches any of the pre-compiled exclude patterns
    #[inline]
    fn is_excluded(&self, path: &Path) -> bool {
        self.exclude_patterns
            .iter()
            .any(|pattern| pattern.matches_path(path))
    }
}

impl FileFinder for GlobFileFinder {
    /// Find all files matching the include patterns, excluding those matching exclude patterns
    fn find_files(&self) -> Result<Vec<PathBuf>> {
        let mut seen = HashSet::new();
        let mut paths = Vec::new();

        for pattern in &self.include {
            let matches = glob(pattern).map_err(|e| Error::InvalidGlob(e.to_string()))?;

            for entry in matches {
                let path = entry.map_err(|e| Error::Io(e.into_error()))?;

                // Skip directories
                if path.is_dir() {
                    continue;
                }

                // Check pre-compiled exclude patterns
                if self.is_excluded(&path) {
                    continue;
                }

                // Add to result if not seen
                if seen.insert(path.clone()) {
                    paths.push(path);
                }
            }
        }

        Ok(paths)
    }
}

/// Mock file finder for testing error handling thresholds
#[cfg(test)]
pub struct MockFileFinder {
    /// Number of successful calls before failing
    pub fail_after: Option<usize>,
    /// Current call count
    pub call_count: std::cell::Cell<usize>,
    /// Files to return on success
    pub files: Vec<PathBuf>,
}

#[cfg(test)]
impl MockFileFinder {
    /// Create a MockFileFinder that always succeeds with empty results
    pub fn new() -> Self {
        Self {
            fail_after: None,
            call_count: std::cell::Cell::new(0),
            files: vec![],
        }
    }

    /// Create a MockFileFinder that fails after N successful calls
    pub fn fail_after(n: usize) -> Self {
        Self {
            fail_after: Some(n),
            call_count: std::cell::Cell::new(0),
            files: vec![],
        }
    }

    /// Set the files to return on success
    pub fn with_files(mut self, files: Vec<PathBuf>) -> Self {
        self.files = files;
        self
    }
}

#[cfg(test)]
impl FileFinder for MockFileFinder {
    fn find_files(&self) -> Result<Vec<PathBuf>> {
        let count = self.call_count.get();
        self.call_count.set(count + 1);

        if let Some(fail_after) = self.fail_after {
            if count >= fail_after {
                return Err(Error::InvalidGlob("mock error".to_string()));
            }
        }

        Ok(self.files.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn setup_test_files(dir: &TempDir) -> Vec<PathBuf> {
        let files = vec!["test1.log", "test2.log", "other.txt", "ignored.log"];

        for name in &files {
            let path = dir.path().join(name);
            fs::write(&path, format!("content of {}", name)).unwrap();
        }

        files.iter().map(|f| dir.path().join(f)).collect()
    }

    #[test]
    fn test_finder_basic() {
        let dir = TempDir::new().unwrap();
        setup_test_files(&dir);

        let pattern = format!("{}/*.log", dir.path().display());
        let finder = GlobFileFinder::new(vec![pattern], vec![]).unwrap();

        let files = finder.find_files().unwrap();
        assert_eq!(files.len(), 3); // test1.log, test2.log, ignored.log
    }

    #[test]
    fn test_finder_with_exclude() {
        let dir = TempDir::new().unwrap();
        setup_test_files(&dir);

        let include = format!("{}/*.log", dir.path().display());
        let exclude = format!("{}/ignored.log", dir.path().display());
        let finder = GlobFileFinder::new(vec![include], vec![exclude]).unwrap();

        let files = finder.find_files().unwrap();
        assert_eq!(files.len(), 2); // test1.log, test2.log
    }

    #[test]
    fn test_finder_no_duplicates() {
        let dir = TempDir::new().unwrap();
        setup_test_files(&dir);

        // Include the same pattern twice
        let pattern = format!("{}/*.log", dir.path().display());
        let finder = GlobFileFinder::new(vec![pattern.clone(), pattern], vec![]).unwrap();

        let files = finder.find_files().unwrap();
        assert_eq!(files.len(), 3); // Should not have duplicates
    }

    #[test]
    fn test_finder_empty_include() {
        let finder = GlobFileFinder::new(vec![], vec![]).unwrap();
        let files = finder.find_files().unwrap();
        assert!(files.is_empty());
    }

    #[test]
    fn test_finder_discovers_new_file_created_after_start() {
        let dir = TempDir::new().unwrap();

        // Create initial file
        let initial_file = dir.path().join("existing.log");
        fs::write(&initial_file, "initial content").unwrap();

        // Create finder with pattern that matches .log files
        let pattern = format!("{}/*.log", dir.path().display());
        let finder = GlobFileFinder::new(vec![pattern], vec![]).unwrap();

        // First find - should see only the existing file
        let files = finder.find_files().unwrap();
        assert_eq!(files.len(), 1, "Should find 1 existing file");
        assert!(
            files
                .iter()
                .any(|p| p.file_name().unwrap() == "existing.log"),
            "Should find existing.log"
        );

        // Now create a NEW file that matches the pattern (simulating log rotation or new log file)
        let new_file = dir.path().join("newfile.log");
        fs::write(&new_file, "new file content\nline 2\nline 3").unwrap();

        // Second find - should now discover the new file
        let files = finder.find_files().unwrap();
        assert_eq!(files.len(), 2, "Should find 2 files after new file created");
        assert!(
            files
                .iter()
                .any(|p| p.file_name().unwrap() == "existing.log"),
            "Should still find existing.log"
        );
        assert!(
            files
                .iter()
                .any(|p| p.file_name().unwrap() == "newfile.log"),
            "Should discover newfile.log"
        );

        // Create another file with different extension - should NOT be discovered
        let txt_file = dir.path().join("other.txt");
        fs::write(&txt_file, "text file").unwrap();

        let files = finder.find_files().unwrap();
        assert_eq!(
            files.len(),
            2,
            "Should still find only 2 .log files, not .txt"
        );
        assert!(
            !files.iter().any(|p| p.file_name().unwrap() == "other.txt"),
            "Should NOT find other.txt"
        );
    }

    #[test]
    fn test_finder_discovers_file_in_initially_empty_directory() {
        let dir = TempDir::new().unwrap();

        // Create finder with pattern - directory exists but has no matching files
        let pattern = format!("{}/*.log", dir.path().display());
        let finder = GlobFileFinder::new(vec![pattern], vec![]).unwrap();

        // First find - no files exist yet
        let files = finder.find_files().unwrap();
        assert!(files.is_empty(), "Should find no files initially");

        // Create a file that matches the pattern
        let new_file = dir.path().join("first.log");
        fs::write(&new_file, "first log entry").unwrap();

        // Second find - should discover the new file
        let files = finder.find_files().unwrap();
        assert_eq!(files.len(), 1, "Should discover new file");
        assert!(
            files.iter().any(|p| p.file_name().unwrap() == "first.log"),
            "Should find first.log"
        );
    }

    #[test]
    fn test_finder_respects_exclude_for_new_files() {
        let dir = TempDir::new().unwrap();

        // Create finder with include and exclude patterns
        let include = format!("{}/*.log", dir.path().display());
        let exclude = format!("{}/*_debug.log", dir.path().display());
        let finder = GlobFileFinder::new(vec![include], vec![exclude]).unwrap();

        // Initially empty
        let files = finder.find_files().unwrap();
        assert!(files.is_empty());

        // Create a normal log file - should be discovered
        let app_log = dir.path().join("app.log");
        fs::write(&app_log, "app log").unwrap();

        let files = finder.find_files().unwrap();
        assert_eq!(files.len(), 1);
        assert!(files.iter().any(|p| p.file_name().unwrap() == "app.log"));

        // Create a debug log file - should be excluded even though it matches *.log
        let debug_log = dir.path().join("app_debug.log");
        fs::write(&debug_log, "debug log").unwrap();

        let files = finder.find_files().unwrap();
        assert_eq!(
            files.len(),
            1,
            "Should still only find 1 file (debug excluded)"
        );
        assert!(
            !files
                .iter()
                .any(|p| p.file_name().unwrap() == "app_debug.log"),
            "Should NOT find app_debug.log due to exclude pattern"
        );
    }
}
