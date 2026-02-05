// SPDX-License-Identifier: Apache-2.0

//! A trivial Rotel processor that adds a custom attribute to every span.
//!
//! This is an example of how to build a Rust processor for Rotel.

use rotel_rust_processor_sdk::prelude::*;
use rotel_rust_processor_sdk::{export_processor, ROption};

/// A simple processor that adds a marker attribute to every span.
#[derive(Default)]
pub struct TrivialProcessor;

impl RotelProcessor for TrivialProcessor {
    fn process_spans(
        &self,
        spans: &mut RResourceSpans,
        _context: &ROption<RRequestContext>,
    ) {
        // Add a custom attribute to every span to prove the processor ran
        for scope_spans in spans.scope_spans.iter_mut() {
            for span in scope_spans.spans.iter_mut() {
                span.attributes.push(RKeyValue::string(
                    "rotel.rust_processor",
                    "trivial_processor",
                ));
            }
        }
    }
}

// Export the processor so Rotel can load it
export_processor!(TrivialProcessor);
