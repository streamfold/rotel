use bytes::Bytes;
use flate2::write::GzDecoder;
use serde::Deserialize;
use std::{fmt::Display, io::Write, str};
use tower::BoxError;

use crate::exporters::http::{client::ResponseDecode, response::Response, types::ContentEncoding};

// These may apply to AWS in general, might be able to generalize later
#[derive(Debug, Clone)]
pub(crate) enum AwsEmfResponse {
    Empty,
    Unknown(String, String),
    ExpiredTokenException(String),
    InvalidParameterException(String),
    ResourceNotFoundException(String),
    ServiceUnavailableException(String),
    UnrecognizedClientException(String),
    ResourceAlreadyExistsException(String),
}

impl Display for AwsEmfResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AwsEmfResponse::Empty => write!(f, ""),
            AwsEmfResponse::Unknown(_type, msg) => write!(f, "Unknown error: {} ({})", _type, msg),
            AwsEmfResponse::ExpiredTokenException(msg) => {
                write!(f, "ExpiredTokenException: {}", msg)
            }
            AwsEmfResponse::InvalidParameterException(msg) => {
                write!(f, "InvalidParameterException: {}", msg)
            }
            AwsEmfResponse::ResourceNotFoundException(msg) => {
                write!(f, "ResourceNotFoundException: {}", msg)
            }
            AwsEmfResponse::ServiceUnavailableException(msg) => {
                write!(f, "ServiceUnavailableException: {}", msg)
            }
            AwsEmfResponse::UnrecognizedClientException(msg) => {
                write!(f, "UnrecognizedClientException: {}", msg)
            }
            AwsEmfResponse::ResourceAlreadyExistsException(msg) => {
                write!(f, "ResourceAlreadyExistsException: {}", msg)
            }
        }
    }
}

#[derive(Deserialize)]
struct AwsEmfResponsePayload {
    #[serde(rename = "__type")]
    _type: Option<String>,

    message: Option<String>,
}

#[derive(Default, Clone)]
pub(crate) struct AwsEmfDecoder;

impl ResponseDecode<AwsEmfResponse> for AwsEmfDecoder {
    fn decode(&self, body: Bytes, ce: ContentEncoding) -> Result<AwsEmfResponse, BoxError> {
        let body = match ce {
            ContentEncoding::None => body,
            ContentEncoding::Gzip => gzip_decode(body)?,
        };

        let payload: AwsEmfResponsePayload = match serde_json::from_slice(&body) {
            Ok(p) => p,
            Err(_e) => {
                // Unclear if all responses are covered, just save as a string
                let str_payload = str::from_utf8(&body)
                    .map(|s| s.to_string())
                    .map_err(|e| format!("error decoding response: {}", e))?;
                return Ok(AwsEmfResponse::Unknown(
                    "ParseError".to_string(),
                    str_payload,
                ));
            }
        };

        match payload._type {
            Some(t) => {
                let msg = payload.message.unwrap_or_default();

                match t.as_str() {
                    "ExpiredTokenException" => Ok(AwsEmfResponse::ExpiredTokenException(msg)),
                    "InvalidParameterException" => {
                        Ok(AwsEmfResponse::InvalidParameterException(msg))
                    }
                    "ResourceNotFoundException" => {
                        Ok(AwsEmfResponse::ResourceNotFoundException(msg))
                    }
                    "ServiceUnavailableException" => {
                        Ok(AwsEmfResponse::ServiceUnavailableException(msg))
                    }
                    "UnrecognizedClientException" => {
                        Ok(AwsEmfResponse::UnrecognizedClientException(msg))
                    }
                    "ResourceAlreadyExistsException" => {
                        Ok(AwsEmfResponse::ResourceAlreadyExistsException(msg))
                    }
                    _ => Ok(AwsEmfResponse::Unknown(t, msg)),
                }
            }
            None => Ok(AwsEmfResponse::Empty),
        }
    }
}

fn gzip_decode(body: Bytes) -> Result<Bytes, BoxError> {
    let buf_out = Vec::new();
    let mut dec = GzDecoder::new(buf_out);
    if let Err(e) = dec.write_all(body.as_ref()) {
        return Err(format!("failed to GZ decode response: {}", e).into());
    }

    match dec.finish() {
        Ok(buf) => Ok(Bytes::from(buf)),
        Err(e) => Err(format!("failed to finish gzip decode of response: {}", e).into()),
    }
}

pub(crate) fn is_retryable_error(
    result: &Result<Response<AwsEmfResponse>, BoxError>,
) -> Option<bool> {
    // Force these to be retried since we'll try to create the log stream/group
    if let Ok(Response::Http(_, Some(AwsEmfResponse::ResourceNotFoundException(_)))) = result {
        return Some(true);
    }

    // Fall back to normal processing
    None
}
