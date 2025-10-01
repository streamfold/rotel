// SPDX-License-Identifier: Apache-2.0

use crate::topology::payload::MessageMetadata;
use bytes::Bytes;
use http_body_util::Full;

/// Trait for extracting metadata from request bodies
pub trait MetadataExtractor {
    fn take_metadata(&mut self) -> Option<Vec<MessageMetadata>>;
}

// Implementation for Full<Bytes> - no metadata
impl MetadataExtractor for Full<Bytes> {
    fn take_metadata(&mut self) -> Option<Vec<MessageMetadata>> {
        None
    }
}

// Implementation for Bytes - no metadata
impl MetadataExtractor for Bytes {
    fn take_metadata(&mut self) -> Option<Vec<MessageMetadata>> {
        None
    }
}

// Implementation for String - no metadata (used in some HTTP scenarios)
impl MetadataExtractor for String {
    fn take_metadata(&mut self) -> Option<Vec<MessageMetadata>> {
        None
    }
}

// Implementation for &str - no metadata (used in some HTTP scenarios)
impl MetadataExtractor for &str {
    fn take_metadata(&mut self) -> Option<Vec<MessageMetadata>> {
        None
    }
}
