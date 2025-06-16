use std::fmt;

#[derive(Debug)]
pub enum Error {
    ArnParseError(String),
    RequestBuildError(http::Error),
    SignatureError(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::RequestBuildError(e) => write!(f, "HTTP request build error: {}", e),
            Error::SignatureError(msg) => write!(f, "AWS signature error: {}", msg),
            Error::ArnParseError(arn) => write!(f, "Invalid ARN: {}", arn),
        }
    }
}

impl std::error::Error for Error {}
