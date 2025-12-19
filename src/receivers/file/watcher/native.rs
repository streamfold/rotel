// SPDX-License-Identifier: Apache-2.0

//! Native file system watcher using the `notify` crate.
//!
//! Uses OS-level file system notifications:
//! - Linux: inotify
//! - macOS: FSEvents
//! - Windows: ReadDirectoryChangesW

use std::path::Path;
use std::sync::mpsc::{channel, Receiver, TryRecvError};
use std::sync::Mutex;
use std::time::Duration;

use notify::{
    Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher,
};

use super::traits::{FileEvent, FileEventKind, FileWatcher, WatcherError};

/// Native file system watcher using OS-level notifications.
pub struct NativeWatcher {
    watcher: RecommendedWatcher,
    receiver: Mutex<Receiver<Result<Event, notify::Error>>>,
}

impl NativeWatcher {
    /// Create a new native watcher with the specified debounce interval.
    pub fn new(debounce: Duration) -> Result<Self, WatcherError> {
        let (tx, rx) = channel();

        let config = Config::default()
            .with_poll_interval(debounce);

        let watcher = RecommendedWatcher::new(
            move |res| {
                let _ = tx.send(res);
            },
            config,
        )
        .map_err(|e| WatcherError::Init(e.to_string()))?;

        Ok(Self {
            watcher,
            receiver: Mutex::new(rx),
        })
    }

    /// Convert a notify event to our FileEvent type
    fn convert_event(event: Event) -> Option<FileEvent> {
        let kind = match event.kind {
            EventKind::Create(_) => FileEventKind::Create,
            EventKind::Modify(_) => FileEventKind::Modify,
            EventKind::Remove(_) => FileEventKind::Remove,
            EventKind::Access(_) => return None, // Ignore access events
            EventKind::Other => FileEventKind::Other,
            EventKind::Any => FileEventKind::Other,
        };

        if event.paths.is_empty() {
            return None;
        }

        Some(FileEvent::new(kind, event.paths))
    }
}

impl FileWatcher for NativeWatcher {
    fn watch(&mut self, path: &Path) -> Result<(), WatcherError> {
        self.watcher
            .watch(path, RecursiveMode::NonRecursive)
            .map_err(|e| WatcherError::Watch(e.to_string()))
    }

    fn unwatch(&mut self, path: &Path) -> Result<(), WatcherError> {
        self.watcher
            .unwatch(path)
            .map_err(|e| WatcherError::Watch(e.to_string()))
    }

    fn try_recv(&mut self) -> Result<Vec<FileEvent>, WatcherError> {
        let mut events = Vec::new();

        let receiver = self.receiver.lock()
            .map_err(|e| WatcherError::Channel(format!("mutex poisoned: {}", e)))?;

        loop {
            match receiver.try_recv() {
                Ok(Ok(event)) => {
                    if let Some(file_event) = Self::convert_event(event) {
                        events.push(file_event);
                    }
                }
                Ok(Err(e)) => {
                    tracing::warn!("File watcher error: {}", e);
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    return Err(WatcherError::Channel("watcher channel disconnected".into()));
                }
            }
        }

        Ok(events)
    }

    fn recv_timeout(&mut self, timeout: Duration) -> Result<Vec<FileEvent>, WatcherError> {
        let mut events = Vec::new();

        let receiver = self.receiver.lock()
            .map_err(|e| WatcherError::Channel(format!("mutex poisoned: {}", e)))?;

        // First wait for at least one event with timeout
        match receiver.recv_timeout(timeout) {
            Ok(Ok(event)) => {
                if let Some(file_event) = Self::convert_event(event) {
                    events.push(file_event);
                }
            }
            Ok(Err(e)) => {
                tracing::warn!("File watcher error: {}", e);
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                return Ok(events);
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                return Err(WatcherError::Channel("watcher channel disconnected".into()));
            }
        }

        // Drop the lock before calling try_recv again
        drop(receiver);

        // Then drain any additional pending events
        events.extend(self.try_recv()?);

        Ok(events)
    }

    fn is_native(&self) -> bool {
        true
    }

    fn backend_name(&self) -> &'static str {
        #[cfg(target_os = "linux")]
        {
            "inotify"
        }
        #[cfg(target_os = "macos")]
        {
            "FSEvents"
        }
        #[cfg(target_os = "windows")]
        {
            "ReadDirectoryChangesW"
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
        {
            "native"
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{self, File};
    use std::io::Write;
    use tempfile::TempDir;

    #[test]
    fn test_native_watcher_create() {
        let watcher = NativeWatcher::new(Duration::from_millis(100));
        assert!(watcher.is_ok());
    }

    #[test]
    fn test_native_watcher_watch_directory() {
        let temp_dir = TempDir::new().unwrap();
        let mut watcher = NativeWatcher::new(Duration::from_millis(50)).unwrap();

        let result = watcher.watch(temp_dir.path());
        assert!(result.is_ok());
    }

    #[test]
    fn test_native_watcher_detects_file_create() {
        let temp_dir = TempDir::new().unwrap();
        let mut watcher = NativeWatcher::new(Duration::from_millis(50)).unwrap();
        watcher.watch(temp_dir.path()).unwrap();

        // Create a file
        let file_path = temp_dir.path().join("test.log");
        File::create(&file_path).unwrap();

        // Wait a bit for the event
        std::thread::sleep(Duration::from_millis(200));

        let events = watcher.try_recv().unwrap();
        assert!(!events.is_empty(), "Should detect file creation");

        // At least one create event
        let has_create = events.iter().any(|e| e.kind == FileEventKind::Create);
        assert!(has_create, "Should have a create event");
    }

    #[test]
    fn test_native_watcher_detects_file_modify() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.log");

        // Create file first
        let mut file = File::create(&file_path).unwrap();
        file.write_all(b"initial content\n").unwrap();
        file.flush().unwrap();
        drop(file);

        // Wait for file system to settle
        std::thread::sleep(Duration::from_millis(100));

        let mut watcher = NativeWatcher::new(Duration::from_millis(50)).unwrap();
        watcher.watch(temp_dir.path()).unwrap();

        // Clear any initial events from the watch setup
        std::thread::sleep(Duration::from_millis(100));
        let _ = watcher.try_recv();

        // Modify the file
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&file_path)
            .unwrap();
        file.write_all(b"more content\n").unwrap();
        file.flush().unwrap();
        drop(file);

        // Use recv_timeout to wait for events with a longer timeout
        // FSEvents on macOS can have noticeable latency
        let events = watcher.recv_timeout(Duration::from_secs(2)).unwrap();

        // Should have at least one event (might be modify, or create if file was recreated)
        assert!(!events.is_empty(), "Should detect file modification");

        // Check for modify or create event (some systems report create on open-for-write)
        let has_change = events.iter().any(|e| {
            e.kind == FileEventKind::Modify || e.kind == FileEventKind::Create
        });
        assert!(has_change, "Should have a modify or create event");
    }

    #[test]
    fn test_native_watcher_is_native() {
        let watcher = NativeWatcher::new(Duration::from_millis(100)).unwrap();
        assert!(watcher.is_native());
    }

    #[test]
    fn test_native_watcher_backend_name() {
        let watcher = NativeWatcher::new(Duration::from_millis(100)).unwrap();
        let name = watcher.backend_name();
        assert!(!name.is_empty());

        #[cfg(target_os = "linux")]
        assert_eq!(name, "inotify");

        #[cfg(target_os = "macos")]
        assert_eq!(name, "FSEvents");
    }
}
