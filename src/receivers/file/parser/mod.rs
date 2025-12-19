//! Log parsing utilities.
//!
//! This module provides parsers for transforming raw log strings into structured data.
//!
//! # Available Parsers
//!
//! - [`RegexParser`] - Parse logs using regular expressions with named capture groups
//! - [`JsonParser`] - Parse JSON-formatted logs
//! - [`nginx`] - Pre-configured parsers for nginx access and error logs

mod json;
pub mod nginx;
mod regex;
mod traits;

pub use json::JsonParser;
pub use regex::RegexParser;
pub use traits::{Parser, ParserExt};
