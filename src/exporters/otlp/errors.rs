// SPDX-License-Identifier: Apache-2.0

use http::response::Parts;
use std::error::Error;
use std::fmt;
use tonic::Status;
use tower::BoxError;

/// ExporterError is the result of exporting a trace msg
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub enum ExporterError {
    /// Processing error
    Generic(String),

    Connect,

    /// HTTP error resulting in invalid status code
    Http(Parts, Option<String>),

    /// GRPC Error status, from tonic
    Grpc(Status),
}

impl fmt::Display for ExporterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExporterError::Generic(msg) => write!(f, "Generic error: {}", msg),
            ExporterError::Connect => write!(f, "Failed to connect"),
            ExporterError::Http(status, resp) => match resp {
                None => write!(f, "HTTP error: {}", status.status),
                Some(text) => write!(f, "HTTP error: {}:{}", status.status, text),
            },
            ExporterError::Grpc(status) => write!(f, "GRPC error: {}", status),
        }
    }
}

impl Error for ExporterError {}

/// Determine if an error is retryable
pub fn is_retryable_error<Resp>(result: &Result<Resp, BoxError>) -> Option<bool> {
    if result.is_ok() {
        return None;
    }

    let err = result.as_ref().err().unwrap();

    let downcast_err = err.downcast_ref::<ExporterError>();
    if downcast_err.is_none() {
        return Some(false);
    }

    // Http and Grpc errors are converted into Ok() responses and checked
    // higher up, only check the remaining error conditions here
    match downcast_err.unwrap() {
        ExporterError::Connect => Some(true),
        _ => Some(false),
    }
}
