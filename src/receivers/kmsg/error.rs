// SPDX-License-Identifier: Apache-2.0

use std::fmt;
use std::io;

#[derive(Debug)]
pub enum KmsgReceiverError {
    ParseError(String),
    ConfigurationError(String),
    BootTimeError(String),
    IoError(io::Error),
}

impl fmt::Display for KmsgReceiverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KmsgReceiverError::ParseError(s) => write!(f, "Parse error: {}", s),
            KmsgReceiverError::ConfigurationError(s) => write!(f, "Configuration error: {}", s),
            KmsgReceiverError::BootTimeError(s) => write!(f, "Boot time error: {}", s),
            KmsgReceiverError::IoError(e) => write!(f, "I/O error: {}", e),
        }
    }
}

impl std::error::Error for KmsgReceiverError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            KmsgReceiverError::IoError(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for KmsgReceiverError {
    fn from(err: io::Error) -> Self {
        KmsgReceiverError::IoError(err)
    }
}

pub type Result<T> = std::result::Result<T, KmsgReceiverError>;
