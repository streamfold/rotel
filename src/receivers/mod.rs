// SPDX-License-Identifier: Apache-2.0

pub mod kafka;
pub mod otlp;
pub mod otlp_output;

#[cfg(feature = "fluent_receiver")]
pub mod fluent;

use opentelemetry::global;
use opentelemetry::metrics::Meter;

pub fn get_meter() -> Meter {
    global::meter("receivers")
}
