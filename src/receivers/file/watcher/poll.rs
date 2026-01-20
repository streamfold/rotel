// SPDX-License-Identifier: Apache-2.0

//! Polling-based file watcher as a fallback for systems where native
//! file system notifications are unavailable or unreliable (e.g., NFS).

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};

use super::traits::{FileEvent, FileWatcher, WatcherError};

/// File metadata for change detection
#[derive(Debug, Clone)]
struct FileState {
    /// Last modification time
    modified: SystemTime,
    /// File size in bytes
    size: u64,
}

impl FileState {
    fn from_metadata(metadata: &fs::Metadata) -> Option<Self> {
        Some(Self {
            modified: metadata.modified().ok()?,
            size: metadata.len(),
        })
    }
}

/// Polling-based file watcher.
///
/// Periodically scans watched directories to detect file changes.
/// Use this for NFS and other network file systems where native
/// watching is unreliable.
pub struct PollWatcher {
    /// Directories being watched
    watched_dirs: Vec<PathBuf>,
    /// Known file states from last poll
    file_states: HashMap<PathBuf, FileState>,
    /// Poll interval
    poll_interval: Duration,
    /// Last poll time
    last_poll: Instant,
    /// Pending events from last poll
    pending_events: Vec<FileEvent>,
}

impl PollWatcher {
    /// Create a new poll watcher for the given directories.
    pub fn new(directories: &[&Path], poll_interval: Duration) -> Result<Self, WatcherError> {
        let watched_dirs: Vec<PathBuf> = directories.iter().map(|p| p.to_path_buf()).collect();

        let mut watcher = Self {
            watched_dirs,
            file_states: HashMap::new(),
            poll_interval,
            last_poll: Instant::now() - poll_interval, // Ensure first poll runs immediately
            pending_events: Vec::new(),
        };

        // Initial scan to populate file states
        watcher.scan_all()?;

        Ok(watcher)
    }

    /// Add a directory to watch
    fn add_directory(&mut self, path: PathBuf) -> Result<(), WatcherError> {
        if !self.watched_dirs.contains(&path) {
            self.watched_dirs.push(path);
        }
        Ok(())
    }

    /// Remove a directory from watching
    fn remove_directory(&mut self, path: &Path) -> Result<(), WatcherError> {
        self.watched_dirs.retain(|p| p != path);
        // Remove any file states under this directory
        self.file_states.retain(|p, _| !p.starts_with(path));
        Ok(())
    }

    /// Scan all watched directories for changes
    fn scan_all(&mut self) -> Result<(), WatcherError> {
        let mut events = Vec::new();
        let mut seen_files: HashSet<PathBuf> = HashSet::with_capacity(self.file_states.len());

        // Scan each watched directory - iterate by index to avoid borrowing issues
        for i in 0..self.watched_dirs.len() {
            let dir = self.watched_dirs[i].clone();
            if let Err(e) = self.scan_directory(&dir, &mut seen_files, &mut events) {
                tracing::debug!("Error scanning directory {:?}: {}", dir, e);
            }
        }

        // Check for removed files and remove them from state
        self.file_states.retain(|path, _| {
            if seen_files.contains(path) {
                true
            } else {
                events.push(FileEvent::remove(path.clone()));
                false
            }
        });

        self.pending_events.extend(events);
        self.last_poll = Instant::now();

        Ok(())
    }

    /// Scan a single directory for file changes
    fn scan_directory(
        &mut self,
        dir: &Path,
        seen_files: &mut HashSet<PathBuf>,
        events: &mut Vec<FileEvent>,
    ) -> Result<(), WatcherError> {
        let entries = fs::read_dir(dir)?;

        for entry in entries.flatten() {
            let path = entry.path();

            // Only track regular files
            let metadata = match entry.metadata() {
                Ok(m) if m.is_file() => m,
                _ => continue,
            };

            let new_state = match FileState::from_metadata(&metadata) {
                Some(s) => s,
                None => continue,
            };

            // Mark this file as seen
            seen_files.insert(path.clone());

            // Check if this is a new file or has changed, update state in place
            match self.file_states.entry(path) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    // New file
                    events.push(FileEvent::create(entry.key().clone()));
                    entry.insert(new_state);
                }
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    let old_state = entry.get();
                    // Check for modifications
                    if new_state.modified != old_state.modified || new_state.size != old_state.size
                    {
                        events.push(FileEvent::modify(entry.key().clone()));
                        entry.insert(new_state);
                    }
                }
            }
        }

        Ok(())
    }

    /// Check if a poll is due
    fn should_poll(&self) -> bool {
        self.last_poll.elapsed() >= self.poll_interval
    }

    /// Perform a poll if due
    fn poll_if_needed(&mut self) -> Result<(), WatcherError> {
        if self.should_poll() {
            self.scan_all()?;
        }
        Ok(())
    }
}

impl FileWatcher for PollWatcher {
    fn watch(&mut self, path: &Path) -> Result<(), WatcherError> {
        let path = path.to_path_buf();

        // Determine if this is a file or directory
        let metadata = fs::metadata(&path)?;

        if metadata.is_dir() {
            self.add_directory(path)?;
        } else if metadata.is_file() {
            // Watch the parent directory
            if let Some(parent) = path.parent() {
                self.add_directory(parent.to_path_buf())?;
            }
        }

        // Rescan to pick up new files
        self.scan_all()?;
        Ok(())
    }

    fn unwatch(&mut self, path: &Path) -> Result<(), WatcherError> {
        self.remove_directory(path)
    }

    fn try_recv(&mut self) -> Result<Vec<FileEvent>, WatcherError> {
        // Poll if needed
        self.poll_if_needed()?;

        // Drain pending events
        Ok(std::mem::take(&mut self.pending_events))
    }

    fn recv_timeout(&mut self, timeout: Duration) -> Result<Vec<FileEvent>, WatcherError> {
        let deadline = Instant::now() + timeout;

        loop {
            // Poll if needed
            self.poll_if_needed()?;

            // Check for events
            if !self.pending_events.is_empty() {
                return Ok(std::mem::take(&mut self.pending_events));
            }

            // Check if we've exceeded the timeout
            if Instant::now() >= deadline {
                return Ok(Vec::new());
            }

            // Sleep until next poll or timeout, whichever is sooner
            let time_to_next_poll = self.poll_interval.saturating_sub(self.last_poll.elapsed());
            let time_to_deadline = deadline.saturating_duration_since(Instant::now());
            let sleep_duration = time_to_next_poll.min(time_to_deadline);

            if !sleep_duration.is_zero() {
                std::thread::sleep(sleep_duration);
            }
        }
    }

    fn is_native(&self) -> bool {
        false
    }

    fn backend_name(&self) -> &'static str {
        "poll"
    }
}

#[cfg(test)]
mod tests {
    use super::super::traits::FileEventKind;
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::TempDir;

    #[test]
    fn test_poll_watcher_create() {
        let temp_dir = TempDir::new().unwrap();
        let watcher = PollWatcher::new(&[temp_dir.path()], Duration::from_millis(100));
        assert!(watcher.is_ok());
    }

    #[test]
    fn test_poll_watcher_detects_new_file() {
        let temp_dir = TempDir::new().unwrap();
        let mut watcher = PollWatcher::new(&[temp_dir.path()], Duration::from_millis(50)).unwrap();

        // Clear any initial events
        let _ = watcher.try_recv();

        // Create a new file
        let file_path = temp_dir.path().join("test.log");
        File::create(&file_path).unwrap();

        // Force a poll
        std::thread::sleep(Duration::from_millis(60));
        let events = watcher.try_recv().unwrap();

        assert!(!events.is_empty(), "Should detect new file");
        let has_create = events.iter().any(|e| e.kind == FileEventKind::Create);
        assert!(has_create, "Should have create event");
    }

    #[test]
    fn test_poll_watcher_detects_file_modify() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.log");

        // Create file first
        {
            let mut file = File::create(&file_path).unwrap();
            file.write_all(b"initial\n").unwrap();
        }

        let mut watcher = PollWatcher::new(&[temp_dir.path()], Duration::from_millis(50)).unwrap();

        // Clear initial events
        let _ = watcher.try_recv();

        // Wait a bit to ensure different mtime
        std::thread::sleep(Duration::from_millis(100));

        // Modify the file
        {
            let mut file = fs::OpenOptions::new()
                .append(true)
                .open(&file_path)
                .unwrap();
            file.write_all(b"more content\n").unwrap();
        }

        // Wait for poll
        std::thread::sleep(Duration::from_millis(60));
        let events = watcher.try_recv().unwrap();

        assert!(!events.is_empty(), "Should detect file modification");
        let has_modify = events.iter().any(|e| e.kind == FileEventKind::Modify);
        assert!(has_modify, "Should have modify event");
    }

    #[test]
    fn test_poll_watcher_detects_file_remove() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.log");

        // Create file first
        File::create(&file_path).unwrap();

        let mut watcher = PollWatcher::new(&[temp_dir.path()], Duration::from_millis(50)).unwrap();

        // Clear initial events
        let _ = watcher.try_recv();

        // Remove the file
        fs::remove_file(&file_path).unwrap();

        // Wait for poll
        std::thread::sleep(Duration::from_millis(60));
        let events = watcher.try_recv().unwrap();

        assert!(!events.is_empty(), "Should detect file removal");
        let has_remove = events.iter().any(|e| e.kind == FileEventKind::Remove);
        assert!(has_remove, "Should have remove event");
    }

    #[test]
    fn test_poll_watcher_is_not_native() {
        let temp_dir = TempDir::new().unwrap();
        let watcher = PollWatcher::new(&[temp_dir.path()], Duration::from_millis(100)).unwrap();
        assert!(!watcher.is_native());
    }

    #[test]
    fn test_poll_watcher_backend_name() {
        let temp_dir = TempDir::new().unwrap();
        let watcher = PollWatcher::new(&[temp_dir.path()], Duration::from_millis(100)).unwrap();
        assert_eq!(watcher.backend_name(), "poll");
    }

    #[test]
    #[ignore]
    fn test_poll_watcher_recv_timeout() {
        let temp_dir = TempDir::new().unwrap();
        let mut watcher = PollWatcher::new(&[temp_dir.path()], Duration::from_millis(500)).unwrap();

        // Clear any events
        let _ = watcher.try_recv();

        // Should timeout with no events
        let start = Instant::now();
        let events = watcher.recv_timeout(Duration::from_millis(100)).unwrap();
        let elapsed = start.elapsed();

        assert!(events.is_empty());
        assert!(elapsed >= Duration::from_millis(100));
        assert!(elapsed < Duration::from_millis(200)); // Should not wait too long
    }
}
