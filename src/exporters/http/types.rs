// SPDX-License-Identifier: Apache-2.0

use http::HeaderValue;
use tower_http::BoxError;

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
