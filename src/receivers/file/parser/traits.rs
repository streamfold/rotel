// SPDX-License-Identifier: Apache-2.0

use crate::receivers::file::entry::Entry;
use crate::receivers::file::error::Result;

/// Parser transforms an Entry's body (typically a raw log line) by extracting
/// structured fields into the entry's attributes.
///
/// Following the OpenTelemetry pattern:
/// - The body remains the original log line (or can be replaced with a specific field)
/// - Parsed fields are added to attributes
pub trait Parser: Send + Sync {
    /// Parse the entry's body and add extracted fields to attributes.
    /// The body is preserved by default (unless the parser specifically replaces it).
    fn parse(&self, entry: Entry) -> Result<Entry>;

    /// Parse a raw string value and return the parsed result as a JSON value.
    /// This is useful for testing or standalone parsing without an Entry.
    fn parse_str(&self, value: &str) -> Result<serde_json::Value>;
}

/// Extension trait for parsing multiple entries
pub trait ParserExt: Parser {
    /// Parse multiple entries, returning results for each
    fn parse_all(&self, entries: Vec<Entry>) -> Vec<Result<Entry>> {
        entries.into_iter().map(|e| self.parse(e)).collect()
    }

    /// Parse multiple entries, filtering out failures
    fn parse_all_ok(&self, entries: Vec<Entry>) -> Vec<Entry> {
        entries
            .into_iter()
            .filter_map(|e| self.parse(e).ok())
            .collect()
    }
}

// Blanket implementation
impl<T: Parser> ParserExt for T {}
