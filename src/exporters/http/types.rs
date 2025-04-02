// SPDX-License-Identifier: Apache-2.0

use bytes::Bytes;
use http::HeaderValue;
use http::request::Request as HttpRequest;
use http_body_util::Full;
use tower_http::BoxError;

// Types for all http requests
pub type Request = HttpRequest<Full<Bytes>>;

#[derive(Debug, Clone, PartialEq)]
pub enum ContentEncoding {
    None,
    Gzip,
}

impl TryFrom<&HeaderValue> for ContentEncoding {
    type Error = BoxError;

    fn try_from(value: &HeaderValue) -> Result<Self, Self::Error> {
        if value == "gzip" {
            Ok(ContentEncoding::Gzip)
        } else {
            Err(format!("unknown Content-Encoding: {:?}", value).into())
        }
    }
}
