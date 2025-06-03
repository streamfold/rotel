use crate::model::common::RKeyValue;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct RResource {
    pub attributes: Arc<Mutex<Vec<Arc<Mutex<RKeyValue>>>>>,
    pub dropped_attributes_count: Arc<Mutex<u32>>,
}
