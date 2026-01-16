// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON parsing error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Persistence error: {0}")]
    Persistence(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Field error: {0}")]
    Field(String),

    #[error("File not found: {0}")]
    FileNotFound(PathBuf),

    #[error("Invalid glob pattern: {0}")]
    InvalidGlob(String),

    #[error("Channel send error")]
    ChannelSend,

    #[error("Regex error: {0}")]
    Regex(String),
}

pub type Result<T> = std::result::Result<T, Error>;
