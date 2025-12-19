// SPDX-License-Identifier: Apache-2.0

//! Persistence for storing file offsets and state.
//!
//! Uses JSON file storage with atomic writes for reliable offset tracking.

mod json_file;
mod store;

pub use json_file::{JsonFileDatabase, JsonFilePersister};
pub use store::{Persister, PersisterExt};
