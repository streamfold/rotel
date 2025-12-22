use glob::glob;
use std::collections::HashSet;
use std::path::PathBuf;

use crate::receivers::file::error::{Error, Result};

/// FileFinder finds files matching include patterns while excluding others
#[derive(Debug, Clone)]
pub struct FileFinder {
    include: Vec<String>,
    exclude: Vec<String>,
}

impl FileFinder {
    /// Create a new FileFinder with the given include and exclude patterns
    pub fn new(include: Vec<String>, exclude: Vec<String>) -> Self {
        Self { include, exclude }
    }

    /// Find all files matching the include patterns, excluding those matching exclude patterns
    pub fn find_files(&self) -> Result<Vec<PathBuf>> {
        let mut seen = HashSet::new();
        let mut paths = Vec::new();

        for pattern in &self.include {
            let matches = glob(pattern).map_err(|e| Error::InvalidGlob(e.to_string()))?;

            'matches: for entry in matches {
                let path = entry.map_err(|e| Error::Io(e.into_error()))?;

                // Skip directories
                if path.is_dir() {
                    continue;
                }

                // Check exclude patterns
                for exclude in &self.exclude {
                    if let Ok(exclude_matches) = glob(exclude) {
                        for exclude_entry in exclude_matches.flatten() {
                            if path == exclude_entry {
                                continue 'matches;
                            }
                        }
                    }

                    // Also try pattern matching directly
                    if let Ok(pattern) = glob::Pattern::new(exclude) {
                        if pattern.matches_path(&path) {
                            continue 'matches;
                        }
                    }
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
        let finder = FileFinder::new(vec![pattern], vec![]);

        let files = finder.find_files().unwrap();
        assert_eq!(files.len(), 3); // test1.log, test2.log, ignored.log
    }

    #[test]
    fn test_finder_with_exclude() {
        let dir = TempDir::new().unwrap();
        setup_test_files(&dir);

        let include = format!("{}/*.log", dir.path().display());
        let exclude = format!("{}/ignored.log", dir.path().display());
        let finder = FileFinder::new(vec![include], vec![exclude]);

        let files = finder.find_files().unwrap();
        assert_eq!(files.len(), 2); // test1.log, test2.log
    }

    #[test]
    fn test_finder_no_duplicates() {
        let dir = TempDir::new().unwrap();
        setup_test_files(&dir);

        // Include the same pattern twice
        let pattern = format!("{}/*.log", dir.path().display());
        let finder = FileFinder::new(vec![pattern.clone(), pattern], vec![]);

        let files = finder.find_files().unwrap();
        assert_eq!(files.len(), 3); // Should not have duplicates
    }

    #[test]
    fn test_finder_empty_include() {
        let finder = FileFinder::new(vec![], vec![]);
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
        let finder = FileFinder::new(vec![pattern], vec![]);

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
        let finder = FileFinder::new(vec![pattern], vec![]);

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
        let finder = FileFinder::new(vec![include], vec![exclude]);

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
