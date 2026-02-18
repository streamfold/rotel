// SPDX-License-Identifier: Apache-2.0

//! An example async Rotel processor that demonstrates I/O-capable processing.
//!
//! Implements `AsyncProcessor` with `async fn` methods. The runtime and `block_on`
//! bridging are handled automatically by `export_async_processor!`.

use rotel_rust_processor_sdk::prelude::*;
use rotel_rust_processor_sdk::{export_async_processor, ROption};

/// An async processor that enriches spans with additional attributes.
#[derive(Default)]
pub struct AsyncEnrichmentProcessor;

impl AsyncProcessor for AsyncEnrichmentProcessor {
    async fn process_spans(
        &self,
        mut spans: RResourceSpans,
        _context: ROption<RRequestContext>,
    ) -> RResourceSpans {
        // Simulate an async enrichment lookup (e.g., calling an external API)
        let enrichment_value = async_lookup().await;

        for scope_spans in spans.scope_spans.iter_mut() {
            for span in scope_spans.spans.iter_mut() {
                span.attributes.push(RKeyValue::string(
                    "enrichment.source",
                    enrichment_value.as_str(),
                ));
                span.attributes
                    .push(RKeyValue::string("rotel.async_processor", "enrichment"));
            }
        }

        spans
    }
}

/// Simulated async enrichment lookup.
async fn async_lookup() -> String {
    // In a real processor, this would call an external service.
    tokio::time::sleep(std::time::Duration::from_micros(1)).await;
    "async-enriched".to_string()
}

// Export the async processor so Rotel can load it
export_async_processor!(AsyncEnrichmentProcessor);
