// SPDX-License-Identifier: Apache-2.0

use std::fmt;

#[derive(Debug)]
pub enum FluentReceiverError {
    IoError(std::io::Error),
    SocketBindError(String),
    DeserializationError(String),
    ConfigurationError(String),
    UnsupportedCompression(String),
}

impl fmt::Display for FluentReceiverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FluentReceiverError::IoError(e) => write!(f, "IO error: {}", e),
            FluentReceiverError::SocketBindError(s) => {
                write!(f, "Failed to bind to socket: {}", s)
            }
            FluentReceiverError::DeserializationError(s) => {
                write!(f, "Failed to deserialize message: {}", s)
            }
            FluentReceiverError::ConfigurationError(s) => {
                write!(f, "Configuration error: {}", s)
            }
            FluentReceiverError::UnsupportedCompression(s) => {
                write!(f, "Unsupported compression: {}", s)
            }
        }
    }
}

impl std::error::Error for FluentReceiverError {}

impl From<std::io::Error> for FluentReceiverError {
    fn from(err: std::io::Error) -> Self {
        FluentReceiverError::IoError(err)
    }
}

pub type Result<T> = std::result::Result<T, FluentReceiverError>;
