// SPDX-License-Identifier: Apache-2.0

use thiserror::Error;

#[derive(Error, Debug)]
pub enum RedisStreamExportError {
    #[error("Redis error: {0}")]
    RedisError(#[from] redis::RedisError),

    #[error("JSON serialization error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("Invalid configuration: {0}")]
    ConfigurationError(String),

    #[error("TLS error: {0}")]
    TlsError(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, RedisStreamExportError>;
