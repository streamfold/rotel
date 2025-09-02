use crate::py::common::*;
use opentelemetry_proto::tonic::common::v1::KeyValue;
#[allow(deprecated)]
use pyo3::{IntoPy, PyObject, PyResult, Python};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct RAnyValue {
    pub value: Arc<Mutex<Option<RValue>>>,
}

#[derive(Debug, Clone)]
#[allow(clippy::enum_variant_names)]
pub enum RValue {
    StringValue(String),
    BoolValue(bool),
    IntValue(i64),
    DoubleValue(f64),
    RVArrayValue(RArrayValue),
    KvListValue(RKeyValueList),
    BytesValue(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct RArrayValue {
    pub values: Arc<Mutex<Vec<Arc<Mutex<Option<RAnyValue>>>>>>,
}

#[allow(deprecated)]
impl RArrayValue {
    pub(crate) fn convert_to_py(&self, py: Python) -> PyResult<PyObject> {
        Ok(ArrayValue(self.values.clone()).into_py(py))
    }
}

#[derive(Debug, Clone)]
pub struct RKeyValueList {
    pub values: Arc<Mutex<Vec<RKeyValue>>>,
}

#[allow(deprecated)]
impl RKeyValueList {
    pub(crate) fn convert_to_py(&self, py: Python) -> PyResult<PyObject> {
        Ok(KeyValueList(self.values.clone()).into_py(py))
    }
}

#[derive(Debug, Clone)]
pub struct RKeyValue {
    pub key: Arc<Mutex<String>>,
    pub value: Arc<Mutex<Option<RAnyValue>>>,
}

#[derive(Debug, Clone)]
pub struct REntityRef {
    pub schema_url: Arc<Mutex<String>>,
    pub r#type: Arc<Mutex<String>>,
    pub id_keys: Arc<Mutex<Vec<String>>>,
    pub description_keys: Arc<Mutex<Vec<String>>>,
}

#[derive(Debug, Clone, Default)]
pub struct RInstrumentationScope {
    pub name: String,
    pub version: String,
    pub attributes_arc: Option<Arc<Mutex<Vec<RKeyValue>>>>,
    pub attributes_raw: Vec<KeyValue>,
    pub dropped_attributes_count: u32,
}
