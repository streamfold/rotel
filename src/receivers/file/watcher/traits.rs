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
