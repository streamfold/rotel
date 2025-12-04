// SPDX-License-Identifier: Apache-2.0

#[cfg(feature = "rdkafka")]
pub mod kafka;
pub mod otlp;
pub mod otlp_output;

use opentelemetry::global;
use opentelemetry::metrics::Meter;

pub fn get_meter() -> Meter {
    global::meter("receivers")
}
