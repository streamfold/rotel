// SPDX-License-Identifier: Apache-2.0

//! File receiver for tailing log files.
//!
//! This receiver watches files matching glob patterns, reads new lines as they
//! are appended, optionally parses them, and converts them to OTLP log records.
//!
//! Features:
//! - Inode-based file tracking across renames and rotations
//! - Offset persistence for resume after restarts
//! - JSON, regex, and nginx log parsers

pub mod config;
pub mod error;
pub mod input;
pub mod offset_committer;
pub mod offset_tracker;
pub mod parser;
pub mod persistence;
pub mod receiver;
pub mod watcher;

pub use config::FileReceiverConfig;
pub use error::{Error, Result};
pub use input::{FileFinder, FileInputConfig, FileReader, StartAt};
pub use offset_committer::{FileOffsetCommitter, OffsetCommitterConfig, TrackedFileInfo};
pub use offset_tracker::{FileOffsetTracker, LineOffset};
pub use parser::{JsonParser, ParsedLog, Parser, RegexParser};
pub use persistence::{JsonFileDatabase, JsonFilePersister, Persister, PersisterExt};
pub use receiver::FileReceiver;
pub use watcher::{FileWatcher, WatchMode, WatcherConfig};
