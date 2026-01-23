// SPDX-License-Identifier: Apache-2.0

//! Persistence for storing file offsets and state.
//!
//! Uses JSON file storage with atomic writes for reliable offset tracking.

mod json_file;
mod schema;
mod store;

pub use json_file::{JsonFileDatabase, JsonFilePersister};
pub use schema::{
    KNOWN_FILES_KEY, PERSISTED_STATE_VERSION, PersistedFileEntryV1, PersistedFileStateV0,
    PersistedStateV0, PersistedStateV1, file_id_to_key,
};
#[cfg(test)]
pub use store::MockPersister;
pub use store::{Persister, PersisterExt};
