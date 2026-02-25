// SPDX-License-Identifier: Apache-2.0

#[cfg(feature = "rdkafka")]
pub mod kafka;
pub mod otlp;
pub mod otlp_output;

#[cfg(feature = "fluent_receiver")]
pub mod fluent;

#[cfg(feature = "file_receiver")]
pub mod file;

#[cfg(all(target_os = "linux", feature = "kmsg_receiver"))]
pub mod kmsg;

use opentelemetry::global;
use opentelemetry::metrics::Meter;

pub fn get_meter() -> Meter {
    global::meter("receivers")
}
