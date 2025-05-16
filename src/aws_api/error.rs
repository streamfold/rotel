use http::uri::InvalidUri;
use std::fmt;
use tower::BoxError;

#[derive(Debug)]
pub enum Error {
    ArnParseError(String),
    UriParseError(InvalidUri),
    RequestBuildError(http::Error),
    HttpError(hyper_util::client::legacy::Error),
    HttpResponseError(hyper::Error),
    HttpResponseErrorParse(BoxError),
    SignatureError(String),
    SerdeError(serde_json::Error),
    AwsError { code: String, message: String },
    InvalidSecrets(Vec<String>),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::RequestBuildError(e) => write!(f, "HTTP request build error: {}", e),
            Error::SignatureError(msg) => write!(f, "AWS signature error: {}", msg),
            Error::SerdeError(e) => write!(f, "Serialization error: {}", e),
            Error::AwsError { code, message } => write!(f, "AWS error [{}]: {}", code, message),
            Error::ArnParseError(arn) => write!(f, "Invalid ARN: {}", arn),
            Error::HttpError(e) => write!(f, "HTTP error: {}", e),
            Error::HttpResponseError(e) => write!(f, "Failed to parse HTTP response: {}", e),
            Error::HttpResponseErrorParse(e) => write!(f, "Failed to parse HTTP response: {}", e),
            Error::UriParseError(e) => write!(f, "Unable to parse endpoint url: {}", e),
            Error::InvalidSecrets(params) => {
                write!(f, "Unable to lookup secret values: {:?}", params)
            }
        }
    }
}

impl std::error::Error for Error {}

impl From<InvalidUri> for Error {
    fn from(err: InvalidUri) -> Self {
        Error::UriParseError(err)
    }
}

impl From<BoxError> for Error {
    fn from(err: BoxError) -> Self {
        Error::HttpResponseErrorParse(err)
    }
}

impl From<hyper_util::client::legacy::Error> for Error {
    fn from(err: hyper_util::client::legacy::Error) -> Self {
        Error::HttpError(err)
    }
}

impl From<hyper::Error> for Error {
    fn from(err: hyper::Error) -> Self {
        Error::HttpResponseError(err)
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::SerdeError(err)
    }
}
