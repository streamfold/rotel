// SPDX-License-Identifier: Apache-2.0

pub mod otlp_grpc;
pub mod otlp_http;
pub mod otlp_output;

use opentelemetry::global;
use opentelemetry::metrics::Meter;

pub fn get_meter() -> Meter {
    global::meter("receivers")
}
