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
}
