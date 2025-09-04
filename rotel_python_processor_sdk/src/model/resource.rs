use crate::model::common::{REntityRef, RKeyValue};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct RResource {
    pub attributes: Arc<Mutex<Vec<Arc<Mutex<RKeyValue>>>>>,
    pub dropped_attributes_count: Arc<Mutex<u32>>,
    pub entity_refs: Arc<Mutex<Vec<Arc<Mutex<REntityRef>>>>>,
}
