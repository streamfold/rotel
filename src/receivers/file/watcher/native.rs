// SPDX-License-Identifier: Apache-2.0

//! Native file system watcher using the `notify` crate with debouncing.
//!
//! Uses OS-level file system notifications:
//! - Linux: inotify
//! - macOS: FSEvents
//! - Windows: ReadDirectoryChangesW
//!
//! Events are debounced using notify-debouncer-mini to coalesce rapid changes
//! (e.g., many writes to a log file) into periodic batches.

use std::path::Path;
use std::sync::Mutex;
use std::sync::mpsc::{Receiver, TryRecvError, channel};
use std::time::Duration;

use notify::RecursiveMode;
use notify_debouncer_mini::{DebouncedEvent, DebouncedEventKind, Debouncer, new_debouncer};

use super::traits::{FileEvent, FileEventKind, FileWatcher, WatcherError};

/// Native file system watcher using OS-level notifications with debouncing.
///
/// The debouncer coalesces rapid file system events into periodic batches,
/// significantly reducing CPU usage when tailing files with high write rates.
pub struct NativeWatcher {
    debouncer: Debouncer<notify::RecommendedWatcher>,
    receiver: Mutex<Receiver<Result<Vec<DebouncedEvent>, notify::Error>>>,
}

impl NativeWatcher {
    /// Create a new native watcher with the specified debounce interval.
    ///
    /// The debounce interval controls how long the watcher waits to coalesce
    /// events for the same file before emitting them. A typical value is 200ms.
    pub fn new(debounce: Duration) -> Result<Self, WatcherError> {
        let (tx, rx) = channel();

        let debouncer = new_debouncer(debounce, move |res| {
            let _ = tx.send(res);
        })
        .map_err(|e| WatcherError::Init(e.to_string()))?;

        Ok(Self {
            debouncer,
            receiver: Mutex::new(rx),
        })
    }

    /// Convert a debounced event to our FileEvent type.
    ///
    /// DebouncedEventKind only has two variants:
    /// - Any: A change occurred (could be create, modify, or delete)
    /// - AnyContinuous: Ongoing changes (e.g., continuous writes)
    ///
    /// Since both indicate "something changed", we map them to Modify.
    /// The coordinator's poll() will determine the actual file state.
    fn convert_event(event: DebouncedEvent) -> FileEvent {
        // Both Any and AnyContinuous mean "file changed" - map to Modify
        // The coordinator will determine actual state (new file, modified, deleted)
        let kind = match event.kind {
            DebouncedEventKind::Any => FileEventKind::Modify,
            DebouncedEventKind::AnyContinuous => FileEventKind::Modify,
            // Handle potential future variants
            _ => FileEventKind::Other,
        };

        FileEvent::new(kind, vec![event.path])
    }

    /// Convert a batch of debounced events to FileEvents
    fn convert_events(events: Vec<DebouncedEvent>) -> Vec<FileEvent> {
        events.into_iter().map(Self::convert_event).collect()
    }
}

impl FileWatcher for NativeWatcher {
    fn watch(&mut self, path: &Path) -> Result<(), WatcherError> {
        self.debouncer
            .watcher()
            .watch(path, RecursiveMode::NonRecursive)
            .map_err(|e| WatcherError::Watch(e.to_string()))
    }

    fn unwatch(&mut self, path: &Path) -> Result<(), WatcherError> {
        self.debouncer
            .watcher()
            .unwatch(path)
            .map_err(|e| WatcherError::Watch(e.to_string()))
    }

    fn try_recv(&mut self) -> Result<Vec<FileEvent>, WatcherError> {
        let mut all_events = Vec::new();

        let receiver = self
            .receiver
            .lock()
            .map_err(|e| WatcherError::Channel(format!("mutex poisoned: {}", e)))?;

        loop {
            match receiver.try_recv() {
                Ok(Ok(events)) => {
                    all_events.extend(Self::convert_events(events));
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

        Ok(all_events)
    }

    fn recv_timeout(&mut self, timeout: Duration) -> Result<Vec<FileEvent>, WatcherError> {
        let mut all_events = Vec::new();

        let receiver = self
            .receiver
            .lock()
            .map_err(|e| WatcherError::Channel(format!("mutex poisoned: {}", e)))?;

        // First wait for at least one batch with timeout
        match receiver.recv_timeout(timeout) {
            Ok(Ok(events)) => {
                all_events.extend(Self::convert_events(events));
            }
            Ok(Err(e)) => {
                tracing::warn!("File watcher error: {}", e);
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                return Ok(all_events);
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                return Err(WatcherError::Channel("watcher channel disconnected".into()));
            }
        }

        // Drop the lock before calling try_recv again
        drop(receiver);

        // Then drain any additional pending batches
        all_events.extend(self.try_recv()?);

        Ok(all_events)
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

        // Wait for debounce interval plus some buffer
        std::thread::sleep(Duration::from_millis(200));

        let events = watcher.try_recv().unwrap();
        assert!(!events.is_empty(), "Should detect file creation");
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

        // Should have at least one event
        assert!(!events.is_empty(), "Should detect file modification");
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

    #[test]
    fn test_debouncing_coalesces_rapid_writes() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("rapid.log");

        // Create file first
        File::create(&file_path).unwrap();

        let mut watcher = NativeWatcher::new(Duration::from_millis(100)).unwrap();
        watcher.watch(temp_dir.path()).unwrap();

        // Clear initial events
        std::thread::sleep(Duration::from_millis(150));
        let _ = watcher.try_recv();

        // Perform many rapid writes
        for i in 0..100 {
            let mut file = fs::OpenOptions::new()
                .append(true)
                .open(&file_path)
                .unwrap();
            writeln!(file, "line {}", i).unwrap();
        }

        // Wait for debounce to complete
        std::thread::sleep(Duration::from_millis(200));

        let events = watcher.try_recv().unwrap();

        // Should have coalesced into a small number of events (not 100)
        // The exact number depends on timing, but should be much less than 100
        assert!(
            events.len() < 20,
            "Expected debouncing to coalesce events, got {} events",
            events.len()
        );
    }
}
