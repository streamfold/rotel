// SPDX-License-Identifier: Apache-2.0

//! Traits and types for file system watchers.

use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

/// Error type for watcher operations
#[derive(Debug)]
pub enum WatcherError {
    /// Failed to initialize the watcher
    Init(String),
    /// Failed to watch a path
    Watch(String),
    /// IO error
    Io(std::io::Error),
    /// Channel error
    Channel(String),
}

impl fmt::Display for WatcherError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WatcherError::Init(msg) => write!(f, "watcher initialization failed: {}", msg),
            WatcherError::Watch(msg) => write!(f, "watch failed: {}", msg),
            WatcherError::Io(e) => write!(f, "IO error: {}", e),
            WatcherError::Channel(msg) => write!(f, "channel error: {}", msg),
        }
    }
}

impl std::error::Error for WatcherError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            WatcherError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for WatcherError {
    fn from(e: std::io::Error) -> Self {
        WatcherError::Io(e)
    }
}

/// Kind of file event
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileEventKind {
    /// File was created
    Create,
    /// File was modified (content changed)
    Modify,
    /// File was removed/deleted
    Remove,
    /// File was renamed (provides old and new paths)
    Rename,
    /// Catch-all for other events
    Other,
}

/// A file system event
#[derive(Debug, Clone)]
pub struct FileEvent {
    /// The kind of event
    pub kind: FileEventKind,
    /// The path(s) affected by the event
    pub paths: Vec<PathBuf>,
}

impl FileEvent {
    /// Create a new file event
    pub fn new(kind: FileEventKind, paths: Vec<PathBuf>) -> Self {
        Self { kind, paths }
    }

    /// Create a create event
    pub fn create(path: PathBuf) -> Self {
        Self::new(FileEventKind::Create, vec![path])
    }

    /// Create a modify event
    pub fn modify(path: PathBuf) -> Self {
        Self::new(FileEventKind::Modify, vec![path])
    }

    /// Create a remove event
    pub fn remove(path: PathBuf) -> Self {
        Self::new(FileEventKind::Remove, vec![path])
    }

    /// Create a rename event
    pub fn rename(from: PathBuf, to: PathBuf) -> Self {
        Self::new(FileEventKind::Rename, vec![from, to])
    }
}

/// Trait for file system watchers.
///
/// Implementations can use native OS file system notifications or polling.
pub trait FileWatcher {
    /// Add a path to watch.
    ///
    /// For directories, this typically watches all files within the directory.
    fn watch(&mut self, path: &std::path::Path) -> Result<(), WatcherError>;

    /// Remove a path from watching.
    fn unwatch(&mut self, path: &std::path::Path) -> Result<(), WatcherError>;

    /// Try to receive the next batch of events.
    ///
    /// This method should return immediately with any available events,
    /// or return an empty vector if no events are pending.
    fn try_recv(&mut self) -> Result<Vec<FileEvent>, WatcherError>;

    /// Receive events with a timeout.
    ///
    /// Blocks until events are available or the timeout expires.
    /// Returns an empty vector if the timeout expires with no events.
    fn recv_timeout(&mut self, timeout: Duration) -> Result<Vec<FileEvent>, WatcherError>;

    /// Check if the watcher is using native OS notifications.
    ///
    /// Returns true for inotify/kqueue/FSEvents watchers, false for poll watchers.
    fn is_native(&self) -> bool;

    /// Get the name of the watcher backend for logging.
    fn backend_name(&self) -> &'static str;
}

use super::{NativeWatcher, PollWatcher};

/// Enum-based watcher that avoids dynamic dispatch.
/// This provides static dispatch while allowing runtime selection of watcher type.
pub enum AnyWatcher {
    Native(NativeWatcher),
    Poll(PollWatcher),
    #[cfg(test)]
    Mock(MockWatcher),
}

/// Mock watcher for testing error handling thresholds
#[cfg(test)]
pub struct MockWatcher {
    /// Number of successful recv_timeout calls before failing
    pub fail_after: Option<usize>,
    /// Current call count
    pub call_count: std::cell::Cell<usize>,
    /// Whether to return events or empty vec on success
    pub return_events: bool,
}

#[cfg(test)]
impl MockWatcher {
    /// Create a new MockWatcher that always succeeds
    pub fn new() -> Self {
        Self {
            fail_after: None,
            call_count: std::cell::Cell::new(0),
            return_events: false,
        }
    }

    /// Create a MockWatcher that fails after N successful calls
    pub fn fail_after(n: usize) -> Self {
        Self {
            fail_after: Some(n),
            call_count: std::cell::Cell::new(0),
            return_events: false,
        }
    }
}

#[cfg(test)]
impl FileWatcher for MockWatcher {
    fn watch(&mut self, _path: &std::path::Path) -> Result<(), WatcherError> {
        Ok(())
    }

    fn unwatch(&mut self, _path: &std::path::Path) -> Result<(), WatcherError> {
        Ok(())
    }

    fn try_recv(&mut self) -> Result<Vec<FileEvent>, WatcherError> {
        Ok(vec![])
    }

    fn recv_timeout(&mut self, _timeout: Duration) -> Result<Vec<FileEvent>, WatcherError> {
        let count = self.call_count.get();
        self.call_count.set(count + 1);

        if let Some(fail_after) = self.fail_after {
            if count >= fail_after {
                return Err(WatcherError::Channel("mock error".to_string()));
            }
        }

        Ok(vec![])
    }

    fn is_native(&self) -> bool {
        false
    }

    fn backend_name(&self) -> &'static str {
        "mock"
    }
}

impl FileWatcher for AnyWatcher {
    fn watch(&mut self, path: &std::path::Path) -> Result<(), WatcherError> {
        match self {
            AnyWatcher::Native(w) => w.watch(path),
            AnyWatcher::Poll(w) => w.watch(path),
            #[cfg(test)]
            AnyWatcher::Mock(w) => w.watch(path),
        }
    }

    fn unwatch(&mut self, path: &std::path::Path) -> Result<(), WatcherError> {
        match self {
            AnyWatcher::Native(w) => w.unwatch(path),
            AnyWatcher::Poll(w) => w.unwatch(path),
            #[cfg(test)]
            AnyWatcher::Mock(w) => w.unwatch(path),
        }
    }

    fn try_recv(&mut self) -> Result<Vec<FileEvent>, WatcherError> {
        match self {
            AnyWatcher::Native(w) => w.try_recv(),
            AnyWatcher::Poll(w) => w.try_recv(),
            #[cfg(test)]
            AnyWatcher::Mock(w) => w.try_recv(),
        }
    }

    fn recv_timeout(&mut self, timeout: Duration) -> Result<Vec<FileEvent>, WatcherError> {
        match self {
            AnyWatcher::Native(w) => w.recv_timeout(timeout),
            AnyWatcher::Poll(w) => w.recv_timeout(timeout),
            #[cfg(test)]
            AnyWatcher::Mock(w) => w.recv_timeout(timeout),
        }
    }

    fn is_native(&self) -> bool {
        match self {
            AnyWatcher::Native(w) => w.is_native(),
            AnyWatcher::Poll(w) => w.is_native(),
            #[cfg(test)]
            AnyWatcher::Mock(w) => w.is_native(),
        }
    }

    fn backend_name(&self) -> &'static str {
        match self {
            AnyWatcher::Native(w) => w.backend_name(),
            AnyWatcher::Poll(w) => w.backend_name(),
            #[cfg(test)]
            AnyWatcher::Mock(w) => w.backend_name(),
        }
    }
}
