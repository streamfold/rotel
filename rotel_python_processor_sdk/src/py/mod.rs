pub mod common;
pub mod logs;
pub mod resource;
pub mod trace;

use crate::py;
use crate::py::common::KeyValue;
use py::common::*;
use py::logs::*;
use py::resource::*;
use py::trace::*;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::sync::PoisonError;

// Helper function to reduce duplication
fn handle_poison_error<T>(_: PoisonError<T>) -> PyErr {
    PyErr::new::<PyRuntimeError, _>("Failed to lock mutex")
}

#[pyclass]
struct LoggingStdout;

#[pymethods]
impl LoggingStdout {
    fn write(&self, data: &str) {
        println!("stdout from python: {:?}", data);
    }
}

// Python module definition
#[pymodule]
pub fn rotel_sdk(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let open_telemetry_module = PyModule::new(m.py(), "open_telemetry")?;
    let trace_module = PyModule::new(open_telemetry_module.py(), "trace")?;
    let resource_module = PyModule::new(open_telemetry_module.py(), "resource")?;
    let common_module = PyModule::new(open_telemetry_module.py(), "common")?;
    let logs_module = PyModule::new(open_telemetry_module.py(), "logs")?; // Added logs module
    let trace_v1_module = PyModule::new(trace_module.py(), "v1")?;
    let common_v1_module = PyModule::new(common_module.py(), "v1")?;
    let resource_v1_module = PyModule::new(resource_module.py(), "v1")?;
    let logs_v1_module = PyModule::new(logs_module.py(), "v1")?; // Added logs v1 module

    trace_module.add_submodule(&trace_v1_module)?;
    common_module.add_submodule(&common_v1_module)?;
    resource_module.add_submodule(&resource_v1_module)?;
    open_telemetry_module.add_submodule(&trace_module)?;
    open_telemetry_module.add_submodule(&resource_module)?;
    open_telemetry_module.add_submodule(&common_module)?;
    m.add_submodule(&open_telemetry_module)?;

    m.py()
        .import("sys")?
        .getattr("modules")?
        .set_item("rotel_sdk.open_telemetry", &open_telemetry_module)?;

    m.py()
        .import("sys")?
        .getattr("modules")?
        .set_item("rotel_sdk.open_telemetry.trace", &trace_module)?;
    m.py()
        .import("sys")?
        .getattr("modules")?
        .set_item("rotel_sdk.open_telemetry.trace.v1", &trace_v1_module)?;

    m.py()
        .import("sys")?
        .getattr("modules")?
        .set_item("rotel_sdk.open_telemetry.resource", &resource_module)?;
    m.py()
        .import("sys")?
        .getattr("modules")?
        .set_item("rotel_sdk.open_telemetry.resource.v1", &resource_v1_module)?;

    m.py()
        .import("sys")?
        .getattr("modules")?
        .set_item("rotel_sdk.open_telemetry.common", &common_module)?;
    m.py()
        .import("sys")?
        .getattr("modules")?
        .set_item("rotel_sdk.open_telemetry.common.v1", &common_v1_module)?;

    m.py()
        .import("sys")?
        .getattr("modules")?
        .set_item("rotel_sdk.open_telemetry.logs", &logs_module)?;
    m.py()
        .import("sys")?
        .getattr("modules")?
        .set_item("rotel_sdk.open_telemetry.logs.v1", &logs_v1_module)?;

    common_v1_module.add_class::<AnyValue>()?;
    common_v1_module.add_class::<ArrayValue>()?;
    common_v1_module.add_class::<KeyValueList>()?;
    common_v1_module.add_class::<KeyValue>()?;
    common_v1_module.add_class::<Attributes>()?;
    common_v1_module.add_class::<InstrumentationScope>()?;

    resource_v1_module.add_class::<Resource>()?;

    trace_v1_module.add_class::<ResourceSpans>()?;
    trace_v1_module.add_class::<ScopeSpans>()?;
    trace_v1_module.add_class::<Span>()?;
    trace_v1_module.add_class::<Event>()?;
    trace_v1_module.add_class::<Link>()?;
    trace_v1_module.add_class::<Status>()?;
    trace_v1_module.add_class::<StatusCode>()?;

    logs_v1_module.add_class::<ResourceLogs>()?;
    logs_v1_module.add_class::<ScopeLogs>()?;
    logs_v1_module.add_class::<LogRecords>()?; // Added LogRecords class
    logs_v1_module.add_class::<LogRecord>()?; // Added LogRecord class

    Ok(())
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::*;
    use crate::model::common::RValue::*;
    use chrono::Utc;
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    use pyo3::ffi::c_str;
    use std::ffi::CString;
    use std::sync::{Arc, Once};
    use utilities::otlp::FakeOTLP;

    static INIT: Once = Once::new();

    pub fn initialize() {
        INIT.call_once(|| {
            pyo3::append_to_inittab!(rotel_sdk);
            pyo3::prepare_freethreaded_python();
        });
    }

    fn run_script<'py, T: IntoPyObject<'py>>(script: &str, py: Python<'py>, pv: T) -> PyResult<()> {
        _run_script(script, py, pv, None)
    }

    fn _run_script<'py, T: IntoPyObject<'py>>(
        script: &str,
        py: Python<'py>,
        pv: T,
        process_fn: Option<String>,
    ) -> PyResult<()> {
        let sys = py.import("sys")?;
        sys.setattr("stdout", LoggingStdout.into_py(py))?;
        let code = std::fs::read_to_string(format!("./python_tests/{}", script))?;
        let py_mod = PyModule::from_code(
            py,
            CString::new(code)?.as_c_str(),
            c_str!("example.py"),
            c_str!("example"),
        )?;

        let result_py_object = py_mod
            .getattr(process_fn.unwrap_or("process".to_string()))?
            .call1((pv,));
        if result_py_object.is_err() {
            let err = result_py_object.unwrap_err();
            return Err(err);
        }
        // For debugging python stdout -- println!("{:?}", result_py_object.unwrap());
        Ok(())
    }
    //
    //     #[test]
    //     fn test_read_any_value() {
    //         initialize();
    //         let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
    //         let any_value_arc = Arc::new(Mutex::new(Some(RAnyValue {
    //             value: arc_value.clone(),
    //         })));
    //
    //         let pv = AnyValue {
    //             inner: any_value_arc.clone(),
    //         };
    //         Python::with_gil(|py| -> PyResult<()> { run_script("read_value_test.py", py, pv) })
    //             .unwrap();
    //         let av = any_value_arc.lock().unwrap().clone().unwrap();
    //         let avx = av.value.lock().unwrap().clone();
    //         match avx.unwrap() {
    //             StringValue(s) => {
    //                 assert_eq!(s, "foo");
    //             }
    //             _ => panic!("wrong type"),
    //         }
    //         println!("{:?}", any_value_arc.lock().unwrap().clone().unwrap());
    //     }
    //
    //     #[test]
    //     fn write_string_any_value() {
    //         initialize();
    //         let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
    //         let any_value_arc = Arc::new(Mutex::new(Some(RAnyValue {
    //             value: arc_value.clone(),
    //         })));
    //         let pv = AnyValue {
    //             inner: any_value_arc.clone(),
    //         };
    //         Python::with_gil(|py| -> PyResult<()> { run_script("write_string_value_test.py", py, pv) })
    //             .unwrap();
    //         let av = any_value_arc.lock().unwrap().clone().unwrap();
    //         let avx = av.value.lock().unwrap().clone();
    //         match avx.unwrap() {
    //             StringValue(s) => {
    //                 assert_eq!(s, "changed");
    //             }
    //             _ => panic!("wrong type"),
    //         }
    //         println!("{:?}", any_value_arc.lock().unwrap().clone().unwrap());
    //     }
    //
    //     #[test]
    //     fn write_bool_any_value() {
    //         initialize();
    //         let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
    //         let any_value_arc = Arc::new(Mutex::new(Some(RAnyValue {
    //             value: arc_value.clone(),
    //         })));
    //
    //         let pv = AnyValue {
    //             inner: any_value_arc.clone(),
    //         };
    //
    //         Python::with_gil(|py| -> PyResult<()> { run_script("write_bool_value_test.py", py, pv) })
    //             .unwrap();
    //         match arc_value.lock().unwrap().clone().unwrap() {
    //             BoolValue(b) => {
    //                 assert!(b);
    //             }
    //             _ => panic!("wrong type"),
    //         }
    //         println!("{:?}", any_value_arc.lock().unwrap().clone().unwrap());
    //     }
    //
    //     #[test]
    //     fn write_bytes_any_value() {
    //         initialize();
    //         let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
    //         let any_value_arc = Arc::new(Mutex::new(Some(RAnyValue {
    //             value: arc_value.clone(),
    //         })));
    //
    //         let pv = AnyValue {
    //             inner: any_value_arc.clone(),
    //         };
    //
    //         Python::with_gil(|py| -> PyResult<()> { run_script("write_bytes_value_test.py", py, pv) })
    //             .unwrap();
    //         match arc_value.lock().unwrap().clone().unwrap() {
    //             BytesValue(b) => {
    //                 assert_eq!(b"111111".to_vec(), b);
    //             }
    //             _ => panic!("wrong type"),
    //         }
    //         println!("{:?}", any_value_arc.lock().unwrap().clone().unwrap());
    //     }
    //
    //     #[test]
    //     fn read_key_value_key() {
    //         initialize();
    //         let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
    //         let any_value_arc = Arc::new(Mutex::new(Some(RAnyValue {
    //             value: arc_value.clone(),
    //         })));
    //         let key = Arc::new(Mutex::new("key".to_string()));
    //
    //         let kv = KeyValue {
    //             inner: Arc::new(Mutex::new(RKeyValue {
    //                 key: key.clone(),
    //                 value: any_value_arc.clone(),
    //             })),
    //         };
    //
    //         Python::with_gil(|py| -> PyResult<()> { run_script("read_key_value_key_test.py", py, kv) })
    //             .unwrap();
    //         let av = key.clone().lock().unwrap().clone();
    //         assert_eq!(av, "key".to_string());
    //         println!("{:?}", av);
    //     }
    //
    //     #[test]
    //     fn write_key_value_key() {
    //         initialize();
    //         let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
    //         let any_value_arc = Arc::new(Mutex::new(Some(RAnyValue {
    //             value: arc_value.clone(),
    //         })));
    //         let key = Arc::new(Mutex::new("key".to_string()));
    //
    //         let kv = KeyValue {
    //             inner: Arc::new(Mutex::new(RKeyValue {
    //                 key: key.clone(),
    //                 value: any_value_arc.clone(),
    //             })),
    //         };
    //
    //         Python::with_gil(|py| -> PyResult<()> {
    //             run_script("write_key_value_key_test.py", py, kv)
    //         })
    //         .unwrap();
    //         let av = key.clone().lock().unwrap().clone();
    //         assert_eq!(av, "new_key".to_string());
    //         println!("{:?}", av);
    //     }
    //
    //     #[test]
    //     fn read_key_value_value() {
    //         initialize();
    //         let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
    //         let any_value_arc = Arc::new(Mutex::new(Some(RAnyValue {
    //             value: arc_value.clone(),
    //         })));
    //         let key = Arc::new(Mutex::new("key".to_string()));
    //
    //         let kv = KeyValue {
    //             inner: Arc::new(Mutex::new(RKeyValue {
    //                 key: key.clone(),
    //                 value: any_value_arc.clone(),
    //             })),
    //         };
    //
    //         Python::with_gil(|py| -> PyResult<()> {
    //             run_script("read_key_value_value_test.py", py, kv)
    //         })
    //         .unwrap();
    //         match arc_value.lock().unwrap().clone().unwrap() {
    //             StringValue(s) => {
    //                 assert_eq!(s, "foo");
    //             }
    //             _ => panic!("wrong type"),
    //         }
    //         println!("{:?}", any_value_arc.lock().unwrap().clone().unwrap());
    //     }
    //
    //     #[test]
    //     fn write_key_value_value() {
    //         initialize();
    //         let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
    //         let any_value_arc = Arc::new(Mutex::new(Some(RAnyValue {
    //             value: arc_value.clone(),
    //         })));
    //         let key = Arc::new(Mutex::new("key".to_string()));
    //
    //         let kv = KeyValue {
    //             inner: Arc::new(Mutex::new(RKeyValue {
    //                 key: key.clone(),
    //                 value: any_value_arc.clone(),
    //             })),
    //         };
    //
    //         Python::with_gil(|py| -> PyResult<()> {
    //             run_script("write_key_value_value_test.py", py, kv)
    //         })
    //         .unwrap();
    //         match arc_value.lock().unwrap().clone().unwrap() {
    //             StringValue(s) => {
    //                 assert_eq!(s, "changed");
    //             }
    //             _ => panic!("wrong type"),
    //         }
    //         println!("{:?}", any_value_arc.lock().unwrap().clone().unwrap());
    //     }
    //
    //     #[test]
    //     fn write_key_value_bytes_value() {
    //         initialize();
    //         let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
    //         let any_value_arc = Arc::new(Mutex::new(Some(RAnyValue {
    //             value: arc_value.clone(),
    //         })));
    //         let key = Arc::new(Mutex::new("key".to_string()));
    //
    //         let kv = KeyValue {
    //             inner: Arc::new(Mutex::new(RKeyValue {
    //                 key: key.clone(),
    //                 value: any_value_arc.clone(),
    //             })),
    //         };
    //
    //         Python::with_gil(|py| -> PyResult<()> {
    //             run_script("write_key_value_bytes_value_test.py", py, kv)
    //         })
    //         .unwrap();
    //         match arc_value.lock().unwrap().clone().unwrap() {
    //             BytesValue(s) => {
    //                 assert_eq!(b"111111".to_vec(), s);
    //             }
    //             _ => panic!("wrong type"),
    //         }
    //         println!("{:?}", any_value_arc.lock().unwrap().clone().unwrap());
    //     }
    //
    //     #[test]
    //     fn read_resource_attributes() {
    //         initialize();
    //         let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
    //         let any_value_arc = Arc::new(Mutex::new(Some(RAnyValue {
    //             value: arc_value.clone(),
    //         })));
    //         let key = Arc::new(Mutex::new("key".to_string()));
    //
    //         let kv = RKeyValue {
    //             key: key.clone(),
    //             value: any_value_arc.clone(),
    //         };
    //
    //         let kv_arc = Arc::new(Mutex::new(kv));
    //
    //         let resource = Resource {
    //             attributes: Arc::new(Mutex::new(vec![kv_arc.clone()])),
    //             dropped_attributes_count: Arc::new(Mutex::new(0)),
    //         };
    //
    //         Python::with_gil(|py| -> PyResult<()> {
    //             run_script("read_resource_attributes_test.py", py, resource)
    //         })
    //         .unwrap();
    //     }
    //
    //     #[test]
    //     fn read_and_write_attributes_array_value() {
    //         initialize();
    //
    //         let arc_value = Some(StringValue("foo".to_string()));
    //         let any_value_arc = Some(RAnyValue {
    //             value: Arc::new(Mutex::new(arc_value)),
    //         });
    //         let array_value = crate::model::common::RArrayValue {
    //             values: Arc::new(Mutex::new(vec![Arc::new(Mutex::new(
    //                 any_value_arc.clone(),
    //             ))])),
    //         };
    //         let array_value_arc = Arc::new(Mutex::new(Some(RVArrayValue(array_value))));
    //         let any_value_array_value_wrapper = Some(RAnyValue {
    //             value: array_value_arc.clone(),
    //         });
    //
    //         let any_value_array_value_wrapper_arc = Arc::new(Mutex::new(any_value_array_value_wrapper));
    //
    //         let key = Arc::new(Mutex::new("key".to_string()));
    //         let kv = RKeyValue {
    //             key: key.clone(),
    //             value: any_value_array_value_wrapper_arc.clone(),
    //         };
    //
    //         let kv_arc = Arc::new(Mutex::new(kv));
    //         let attrs = Arc::new(Mutex::new(vec![kv_arc.clone()]));
    //
    //         let resource = Resource {
    //             attributes: attrs.clone(),
    //             dropped_attributes_count: Arc::new(Mutex::new(0)),
    //         };
    //
    //         Python::with_gil(|py| -> PyResult<()> {
    //             run_script(
    //                 "read_and_write_attributes_array_value_test.py",
    //                 py,
    //                 resource,
    //             )
    //         })
    //         .unwrap();
    //
    //         let attrs = Arc::into_inner(attrs).unwrap();
    //         let mut attrs = attrs.into_inner().unwrap();
    //         let attr = Arc::into_inner(attrs.pop().unwrap()).unwrap();
    //         let attr = attr.into_inner().unwrap();
    //         let key = attr.key.lock().unwrap();
    //         assert_eq!("my_array", *key);
    //         let value = attr.value.lock().unwrap();
    //         assert!(value.is_some());
    //         let v = value.clone().unwrap();
    //         let v = v.value.lock().unwrap();
    //         assert!(v.is_some());
    //         let v = v.clone().unwrap();
    //         match v {
    //             RVArrayValue(av) => {
    //                 println!("{:?}", av);
    //                 let mut vals = av.values.lock().unwrap();
    //                 let vv = vals.pop().unwrap();
    //                 let v = vv.lock().unwrap();
    //                 let v = v.clone().unwrap();
    //                 let v = v.value.clone().lock().unwrap().clone().unwrap();
    //                 match v {
    //                     IntValue(v) => {
    //                         assert_eq!(v, 123456789)
    //                     }
    //                     _ => panic!("wrong value type"),
    //                 }
    //             }
    //             _ => panic!("wrong value type"),
    //         }
    //     }
    //
    //     #[test]
    //     fn read_and_write_attributes_key_value_list_value() {
    //         initialize();
    //
    //         let value = Some(StringValue("foo".to_string()));
    //         let any_value = Some(RAnyValue {
    //             value: Arc::new(Mutex::new(value)),
    //         });
    //         let any_value_arc = Arc::new(Mutex::new(any_value));
    //         let arc_key = Arc::new(Mutex::new("inner_key".to_string()));
    //
    //         let kev_value = RKeyValue {
    //             key: arc_key.clone(),
    //             value: any_value_arc.clone(),
    //         };
    //
    //         let kv_list = crate::model::common::RKeyValueList {
    //             values: Arc::new(Mutex::new(vec![kev_value])),
    //         };
    //
    //         let array_value_arc = Arc::new(Mutex::new(Some(KvListValue(kv_list))));
    //         let any_value_array_value_wrapper = Some(RAnyValue {
    //             value: array_value_arc.clone(),
    //         });
    //
    //         let any_value_array_value_wrapper_arc = Arc::new(Mutex::new(any_value_array_value_wrapper));
    //
    //         let key = Arc::new(Mutex::new("key".to_string()));
    //         let kv = RKeyValue {
    //             key: key.clone(),
    //             value: any_value_array_value_wrapper_arc.clone(),
    //         };
    //
    //         let kv_arc = Arc::new(Mutex::new(kv));
    //
    //         let attrs_arc = Arc::new(Mutex::new(vec![kv_arc.clone()]));
    //         let resource = Resource {
    //             attributes: attrs_arc.clone(),
    //             dropped_attributes_count: Arc::new(Mutex::new(0)),
    //         };
    //
    //         Python::with_gil(|py| -> PyResult<()> {
    //             run_script(
    //                 "read_and_write_attributes_key_value_list_test.py",
    //                 py,
    //                 resource,
    //             )
    //         })
    //         .unwrap();
    //
    //         let mut value = attrs_arc.lock().unwrap();
    //         let value = value.pop().unwrap();
    //         let value = Arc::into_inner(value).unwrap().into_inner().unwrap();
    //         let value = Arc::into_inner(value.value).unwrap().into_inner().unwrap();
    //         let value = value.unwrap().value;
    //         let value = Arc::into_inner(value)
    //             .unwrap()
    //             .into_inner()
    //             .unwrap()
    //             .unwrap();
    //         match value {
    //             KvListValue(k) => {
    //                 let mut value = k.values.lock().unwrap().clone();
    //                 let value = value.pop();
    //                 match value {
    //                     None => {
    //                         panic!("wrong type")
    //                     }
    //                     Some(v) => {
    //                         let v = v.value.lock().unwrap().clone();
    //                         match v {
    //                             None => {
    //                                 panic!("wrong type")
    //                             }
    //                             Some(v) => {
    //                                 let value = v.value.lock().unwrap().clone().unwrap();
    //                                 match value {
    //                                     IntValue(i) => assert_eq!(100, i),
    //                                     _ => panic!("wrong type"),
    //                                 }
    //                             }
    //                         }
    //                     }
    //                 }
    //             }
    //             _ => panic!("wrong type"),
    //         }
    //     }
    //
    //     #[test]
    //     fn write_resource_attributes_key_value_key() {
    //         initialize();
    //         let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
    //         let any_value_arc = Arc::new(Mutex::new(Some(RAnyValue {
    //             value: arc_value.clone(),
    //         })));
    //         let key = Arc::new(Mutex::new("key".to_string()));
    //
    //         let kv = RKeyValue {
    //             key: key.clone(),
    //             value: any_value_arc.clone(),
    //         };
    //
    //         let kv_arc = Arc::new(Mutex::new(kv));
    //
    //         let resource = Resource {
    //             attributes: Arc::new(Mutex::new(vec![kv_arc.clone()])),
    //             dropped_attributes_count: Arc::new(Mutex::new(0)),
    //         };
    //
    //         Python::with_gil(|py| -> PyResult<()> {
    //             run_script(
    //                 "write_resource_attributes_key_value_key_test.py",
    //                 py,
    //                 resource,
    //             )
    //         })
    //         .unwrap();
    //         let av = key.clone().lock().unwrap().clone();
    //         assert_eq!(av, "new_key".to_string());
    //         println!("{:?}", av);
    //     }
    //
    //     #[test]
    //     fn write_resource_attributes_key_value_value() {
    //         initialize();
    //         let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
    //         let any_value_arc = Arc::new(Mutex::new(Some(RAnyValue {
    //             value: arc_value.clone(),
    //         })));
    //         let key = Arc::new(Mutex::new("key".to_string()));
    //
    //         let kv = RKeyValue {
    //             key: key.clone(),
    //             value: any_value_arc.clone(),
    //         };
    //
    //         let kv_arc = Arc::new(Mutex::new(kv));
    //
    //         let resource = Resource {
    //             attributes: Arc::new(Mutex::new(vec![kv_arc.clone()])),
    //             dropped_attributes_count: Arc::new(Mutex::new(0)),
    //         };
    //
    //         Python::with_gil(|py| -> PyResult<()> {
    //             run_script(
    //                 "write_resource_attributes_key_value_value_test.py",
    //                 py,
    //                 resource,
    //             )
    //         })
    //         .unwrap();
    //         match arc_value.lock().unwrap().clone().unwrap() {
    //             StringValue(s) => {
    //                 assert_eq!(s, "changed");
    //             }
    //             _ => panic!("wrong type"),
    //         }
    //         println!("{:?}", any_value_arc.lock().unwrap().clone().unwrap());
    //     }
    //
    //     #[test]
    //     fn resource_attributes_append_attribute() {
    //         initialize();
    //         let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
    //         let any_value_arc = Arc::new(Mutex::new(Some(RAnyValue {
    //             value: arc_value.clone(),
    //         })));
    //         let key = Arc::new(Mutex::new("key".to_string()));
    //
    //         let kv = RKeyValue {
    //             key: key.clone(),
    //             value: any_value_arc.clone(),
    //         };
    //
    //         let kv_arc = Arc::new(Mutex::new(kv));
    //         let attrs_arc = Arc::new(Mutex::new(vec![kv_arc.clone()]));
    //         let resource = Resource {
    //             attributes: attrs_arc.clone(),
    //             dropped_attributes_count: Arc::new(Mutex::new(0)),
    //         };
    //
    //         Python::with_gil(|py| -> PyResult<()> {
    //             run_script("resource_attributes_append_attribute.py", py, resource)
    //         })
    //         .unwrap();
    //         println!("{:#?}", attrs_arc.lock().unwrap());
    //     }
    //
    //     #[test]
    //     fn resource_attributes_set_attributes() {
    //         initialize();
    //         let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
    //         let any_value_arc = Arc::new(Mutex::new(Some(RAnyValue {
    //             value: arc_value.clone(),
    //         })));
    //         let key = Arc::new(Mutex::new("key".to_string()));
    //
    //         let kv = RKeyValue {
    //             key: key.clone(),
    //             value: any_value_arc.clone(),
    //         };
    //
    //         let kv_arc = Arc::new(Mutex::new(kv));
    //         let attrs_arc = Arc::new(Mutex::new(vec![kv_arc.clone()]));
    //         let resource = Resource {
    //             attributes: attrs_arc.clone(),
    //             dropped_attributes_count: Arc::new(Mutex::new(0)),
    //         };
    //
    //         Python::with_gil(|py| -> PyResult<()> {
    //             run_script("resource_attributes_set_attributes.py", py, resource)
    //         })
    //         .unwrap();
    //         println!("{:#?}", attrs_arc.lock().unwrap());
    //         let attrs = attrs_arc.lock().unwrap();
    //         assert_eq!(2, attrs.len());
    //         for kv in attrs.iter() {
    //             let guard = kv.lock();
    //             let kv_guard = guard.unwrap();
    //             let key = kv_guard.key.lock().unwrap().to_string();
    //             let value = kv_guard.value.lock().unwrap();
    //             assert_ne!(key, "key");
    //             assert!(key == "double.value" || key == "os.version");
    //             assert!(value.is_some());
    //             let av = value.clone().unwrap();
    //             let value = av.value.lock().unwrap();
    //             assert!(value.is_some());
    //         }
    //     }
    //
    //     #[test]
    //     fn resource_spans_append_attributes() {
    //         initialize();
    //         let export_req = utilities::otlp::FakeOTLP::trace_service_request_with_spans(1, 1);
    //         let resource_spans = crate::model::otel_transform::transform_resource_spans(
    //             export_req.resource_spans[0].clone(),
    //         );
    //         let py_resource_spans = ResourceSpans {
    //             resource: resource_spans.resource.clone(),
    //             scope_spans: Arc::new(Mutex::new(vec![])),
    //             schema_url: resource_spans.schema_url,
    //         };
    //         Python::with_gil(|py| -> PyResult<()> {
    //             run_script("resource_spans_append_attribute.py", py, py_resource_spans)
    //         })
    //         .unwrap();
    //         println!("{:#?}", resource_spans.resource.lock().unwrap());
    //     }
    //
    //     #[test]
    //     fn resource_spans_iterate_spans() {
    //         initialize();
    //         let export_req = utilities::otlp::FakeOTLP::trace_service_request_with_spans(1, 1);
    //         let resource_spans = crate::model::otel_transform::transform_resource_spans(
    //             export_req.resource_spans[0].clone(),
    //         );
    //         let py_resource_spans = ResourceSpans {
    //             resource: resource_spans.resource.clone(),
    //             scope_spans: resource_spans.scope_spans.clone(),
    //             schema_url: resource_spans.schema_url,
    //         };
    //         Python::with_gil(|py| -> PyResult<()> {
    //             run_script("resource_spans_iterate_spans.py", py, py_resource_spans)
    //         })
    //         .unwrap();
    //         println!("{:#?}", resource_spans.resource.lock().unwrap());
    //     }
    //
    //     #[test]
    //     fn read_and_write_instrumentation_scope() {
    //         initialize();
    //         let export_req = utilities::otlp::FakeOTLP::trace_service_request_with_spans(1, 1);
    //         let resource_spans = crate::model::otel_transform::transform_resource_spans(
    //             export_req.resource_spans[0].clone(),
    //         );
    //         let py_resource_spans = ResourceSpans {
    //             resource: resource_spans.resource.clone(),
    //             scope_spans: resource_spans.scope_spans.clone(),
    //             schema_url: resource_spans.schema_url,
    //         };
    //         Python::with_gil(|py| -> PyResult<()> {
    //             run_script(
    //                 "read_and_write_instrumentation_scope_test.py",
    //                 py,
    //                 py_resource_spans,
    //             )
    //         })
    //         .unwrap();
    //
    //         let scope_spans_vec = Arc::into_inner(resource_spans.scope_spans).unwrap();
    //         let scope_spans_vec = scope_spans_vec.into_inner().unwrap();
    //
    //         let mut scope_spans = crate::model::py_transform::transform_spans(scope_spans_vec);
    //         let scope_spans = scope_spans.pop().unwrap();
    //         let scope = scope_spans.scope.unwrap();
    //         assert_eq!("name_changed", scope.name);
    //         assert_eq!("0.0.2", scope.version);
    //         assert_eq!(100, scope.dropped_attributes_count);
    //         assert_eq!(scope.attributes.len(), 2);
    //         for attr in &scope.attributes {
    //             let value = attr.value.clone().unwrap();
    //             let value = value.value.unwrap();
    //             match attr.key.as_str() {
    //                 "key_changed" => match value {
    //                     opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i) => {
    //                         assert_eq!(i, 200);
    //                     }
    //                     _ => {
    //                         panic!("wrong type for key_changed: {:?}", value);
    //                     }
    //                 },
    //                 "severity" => match value {
    //                     opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s) => {
    //                         assert_eq!(s, "WARN");
    //                     }
    //                     _ => {
    //                         panic!("wrong type for severity: {:?}", value);
    //                     }
    //                 },
    //                 _ => {
    //                     panic!("unexpected key")
    //                 }
    //             }
    //         }
    //     }
    //     #[test]
    //     fn set_instrumentation_scope() {
    //         initialize();
    //         let export_req = FakeOTLP::trace_service_request_with_spans(1, 1);
    //         let resource_spans = crate::model::otel_transform::transform_resource_spans(
    //             export_req.resource_spans[0].clone(),
    //         );
    //         let py_resource_spans = ResourceSpans {
    //             resource: resource_spans.resource.clone(),
    //             scope_spans: resource_spans.scope_spans.clone(),
    //             schema_url: resource_spans.schema_url,
    //         };
    //         Python::with_gil(|py| -> PyResult<()> {
    //             run_script("set_instrumentation_scope_test.py", py, py_resource_spans)
    //         })
    //         .unwrap();
    //
    //         let scope_spans_vec = Arc::into_inner(resource_spans.scope_spans).unwrap();
    //         let scope_spans_vec = scope_spans_vec.into_inner().unwrap();
    //
    //         let mut scope_spans = crate::model::py_transform::transform_spans(scope_spans_vec);
    //         let scope_spans = scope_spans.pop().unwrap();
    //         let scope = scope_spans.scope.unwrap();
    //         assert_eq!("name_changed", scope.name);
    //         assert_eq!("0.0.2", scope.version);
    //         assert_eq!(100, scope.dropped_attributes_count);
    //         assert_eq!(scope.attributes.len(), 1);
    //         for attr in &scope.attributes {
    //             let value = attr.value.clone().unwrap();
    //             let value = value.value.unwrap();
    //             match attr.key.as_str() {
    //                 "severity" => match value {
    //                     Value::StringValue(s) => {
    //                         assert_eq!(s, "WARN");
    //                     }
    //                     _ => {
    //                         panic!("wrong type for severity: {:?}", value);
    //                     }
    //                 },
    //                 _ => {
    //                     panic!("unexpected key")
    //                 }
    //             }
    //         }
    //     }
    //
    //     #[test]
    //     fn read_and_write_spans() {
    //         initialize();
    //         let export_req = FakeOTLP::trace_service_request_with_spans(1, 1);
    //         let resource_spans = crate::model::otel_transform::transform_resource_spans(
    //             export_req.resource_spans[0].clone(),
    //         );
    //         let py_resource_spans = ResourceSpans {
    //             resource: resource_spans.resource.clone(),
    //             scope_spans: resource_spans.scope_spans.clone(),
    //             schema_url: resource_spans.schema_url,
    //         };
    //         Python::with_gil(|py| -> PyResult<()> {
    //             run_script("read_and_write_spans_test.py", py, py_resource_spans)
    //         })
    //         .unwrap();
    //
    //         let resource = Arc::into_inner(resource_spans.resource);
    //         let resource = resource.unwrap().into_inner().unwrap().unwrap();
    //         let dropped = Arc::into_inner(resource.dropped_attributes_count);
    //         let dropped = dropped.unwrap().into_inner().unwrap();
    //
    //         assert_eq!(15, dropped);
    //
    //         let scope_spans_vec = Arc::into_inner(resource_spans.scope_spans).unwrap();
    //         let scope_spans_vec = scope_spans_vec.into_inner().unwrap();
    //
    //         let mut scope_spans = crate::model::py_transform::transform_spans(scope_spans_vec);
    //         let mut scope_spans = scope_spans.pop().unwrap();
    //         let mut span = scope_spans.spans.pop().unwrap();
    //         assert_eq!(b"5555555555".to_vec(), span.trace_id);
    //         assert_eq!(b"6666666666".to_vec(), span.span_id);
    //         assert_eq!("test=1234567890", span.trace_state);
    //         assert_eq!(b"7777777777".to_vec(), span.parent_span_id);
    //         assert_eq!(1, span.flags);
    //         assert_eq!("py_processed_span", span.name);
    //         assert_eq!(4, span.kind);
    //         assert_eq!(1234567890, span.start_time_unix_nano);
    //         assert_eq!(1234567890, span.end_time_unix_nano);
    //         assert_eq!(100, span.dropped_attributes_count);
    //         assert_eq!(200, span.dropped_events_count);
    //         assert_eq!(300, span.dropped_links_count);
    //         assert_eq!("error message", span.status.clone().unwrap().message);
    //         assert_eq!(2, span.status.unwrap().code);
    //         assert_eq!(1, span.events.len());
    //         assert_eq!("py_processed_event", span.events[0].name);
    //         assert_eq!(1234567890, span.events[0].time_unix_nano);
    //         assert_eq!(400, span.events[0].dropped_attributes_count);
    //         assert_eq!(1, span.events[0].attributes.len());
    //         assert_eq!("event_attr_key", &span.events[0].attributes[0].key);
    //         let value = span.events[0].attributes[0]
    //             .value
    //             .clone()
    //             .unwrap()
    //             .value
    //             .unwrap();
    //         match value {
    //             Value::StringValue(s) => {
    //                 assert_eq!("event_attr_value", s)
    //             }
    //             _ => panic!("unexpected type"),
    //         }
    //
    //         assert_eq!(2, span.links.len());
    //         // get the newly added link
    //         let new_link = span.links.remove(1);
    //         assert_eq!(b"88888888".to_vec(), new_link.trace_id);
    //         assert_eq!(b"99999999".to_vec(), new_link.span_id);
    //         assert_eq!("test=1234567890", new_link.trace_state);
    //         assert_eq!(300, new_link.dropped_attributes_count);
    //         assert_eq!(1, new_link.flags);
    //         assert_eq!(1, new_link.attributes.len());
    //         let value = new_link.attributes[0].value.clone().unwrap().value.unwrap();
    //         match value {
    //             Value::StringValue(s) => {
    //                 assert_eq!("link_attr_value", s)
    //             }
    //             _ => panic!("unexpected type"),
    //         }
    //
    //         assert_eq!(3, span.attributes.len());
    //         let new_attr = span.attributes.remove(2);
    //         assert_eq!("span_attr_key2", new_attr.key);
    //         let value = new_attr.value.clone().unwrap().value.unwrap();
    //         match value {
    //             Value::StringValue(s) => {
    //                 assert_eq!("span_attr_value2", s)
    //             }
    //             _ => panic!("unexpected type"),
    //         }
    //     }
    //     #[test]
    //     fn set_scope_spans_span_test() {
    //         initialize();
    //         let export_req = FakeOTLP::trace_service_request_with_spans(1, 1);
    //         let resource_spans = crate::model::otel_transform::transform_resource_spans(
    //             export_req.resource_spans[0].clone(),
    //         );
    //         let py_resource_spans = ResourceSpans {
    //             resource: resource_spans.resource.clone(),
    //             scope_spans: resource_spans.scope_spans.clone(),
    //             schema_url: resource_spans.schema_url,
    //         };
    //         Python::with_gil(|py| -> PyResult<()> {
    //             run_script("set_scope_spans_span_test.py", py, py_resource_spans)
    //         })
    //         .unwrap();
    //
    //         let scope_spans_vec = Arc::into_inner(resource_spans.scope_spans).unwrap();
    //         let scope_spans_vec = scope_spans_vec.into_inner().unwrap();
    //
    //         let mut scope_spans = crate::model::py_transform::transform_spans(scope_spans_vec);
    //         let mut scope_spans = scope_spans.pop().unwrap();
    //         let mut span = scope_spans.spans.pop().unwrap();
    //         assert_eq!(b"5555555555".to_vec(), span.trace_id);
    //         assert_eq!(b"6666666666".to_vec(), span.span_id);
    //         assert_eq!("test=1234567890", span.trace_state);
    //         assert_eq!(b"7777777777".to_vec(), span.parent_span_id);
    //         assert_eq!(1, span.flags);
    //         assert_eq!("py_processed_span", span.name);
    //         assert_eq!(4, span.kind);
    //         assert_eq!(1234567890, span.start_time_unix_nano);
    //         assert_eq!(1234567890, span.end_time_unix_nano);
    //         assert_eq!(100, span.dropped_attributes_count);
    //         assert_eq!(200, span.dropped_events_count);
    //         assert_eq!(300, span.dropped_links_count);
    //         assert_eq!("error message", span.status.clone().unwrap().message);
    //         assert_eq!(2, span.status.unwrap().code);
    //         assert_eq!(1, span.events.len());
    //         assert_eq!("py_processed_event", span.events[0].name);
    //         assert_eq!(1234567890, span.events[0].time_unix_nano);
    //         assert_eq!(400, span.events[0].dropped_attributes_count);
    //         assert_eq!(1, span.events[0].attributes.len());
    //         assert_eq!("event_attr_key", &span.events[0].attributes[0].key);
    //         let value = span.events[0].attributes[0]
    //             .value
    //             .clone()
    //             .unwrap()
    //             .value
    //             .unwrap();
    //         match value {
    //             Value::StringValue(s) => {
    //                 assert_eq!("event_attr_value", s)
    //             }
    //             _ => panic!("unexpected type"),
    //         }
    //
    //         assert_eq!(1, span.links.len());
    //         // get the newly added link
    //         let new_link = span.links.remove(0);
    //         assert_eq!(b"88888888".to_vec(), new_link.trace_id);
    //         assert_eq!(b"99999999".to_vec(), new_link.span_id);
    //         assert_eq!("test=1234567890", new_link.trace_state);
    //         assert_eq!(300, new_link.dropped_attributes_count);
    //         assert_eq!(1, new_link.flags);
    //         assert_eq!(1, new_link.attributes.len());
    //         let value = new_link.attributes[0].value.clone().unwrap().value.unwrap();
    //         match value {
    //             Value::StringValue(s) => {
    //                 assert_eq!("link_attr_value", s)
    //             }
    //             _ => panic!("unexpected type"),
    //         }
    //
    //         assert_eq!(1, span.attributes.len());
    //         let new_attr = span.attributes.remove(0);
    //         assert_eq!("span_attr_key", new_attr.key);
    //         let value = new_attr.value.clone().unwrap().value.unwrap();
    //         match value {
    //             Value::StringValue(s) => {
    //                 assert_eq!("span_attr_value", s)
    //             }
    //             _ => panic!("unexpected type"),
    //         }
    //     }
    //     #[test]
    //     fn set_resource_spans_resource() {
    //         initialize();
    //         let export_req = FakeOTLP::trace_service_request_with_spans(1, 1);
    //         let resource_spans = crate::model::otel_transform::transform_resource_spans(
    //             export_req.resource_spans[0].clone(),
    //         );
    //         let py_resource_spans = ResourceSpans {
    //             resource: resource_spans.resource.clone(),
    //             scope_spans: resource_spans.scope_spans.clone(),
    //             schema_url: resource_spans.schema_url,
    //         };
    //         Python::with_gil(|py| -> PyResult<()> {
    //             run_script(
    //                 "write_resource_spans_resource_test.py",
    //                 py,
    //                 py_resource_spans,
    //             )
    //         })
    //         .unwrap();
    //
    //         let resource = Arc::into_inner(resource_spans.resource).unwrap();
    //         let resource = resource.into_inner().unwrap().unwrap();
    //         let resource = crate::model::py_transform::transform_resource(resource).unwrap();
    //         assert_eq!(2, resource.attributes.len());
    //         assert_eq!(35, resource.dropped_attributes_count);
    //         for attr in &resource.attributes {
    //             match attr.key.as_str() {
    //                 "key" => assert_eq!(
    //                     Value::StringValue("value".to_string()),
    //                     attr.value.clone().unwrap().value.unwrap()
    //                 ),
    //                 "boolean" => assert_eq!(
    //                     Value::BoolValue(true),
    //                     attr.value.clone().unwrap().value.unwrap()
    //                 ),
    //                 _ => panic!("unexpected attribute key"),
    //             }
    //         }
    //     }
    //     #[test]
    //     fn set_span_events() {
    //         initialize();
    //         let export_req = FakeOTLP::trace_service_request_with_spans(1, 1);
    //         let resource_spans = crate::model::otel_transform::transform_resource_spans(
    //             export_req.resource_spans[0].clone(),
    //         );
    //         let py_resource_spans = ResourceSpans {
    //             resource: resource_spans.resource.clone(),
    //             scope_spans: resource_spans.scope_spans.clone(),
    //             schema_url: resource_spans.schema_url,
    //         };
    //         Python::with_gil(|py| -> PyResult<()> {
    //             run_script("write_span_events_test.py", py, py_resource_spans)
    //         })
    //         .unwrap();
    //
    //         let scope_spans_vec = Arc::into_inner(resource_spans.scope_spans).unwrap();
    //         let scope_spans_vec = scope_spans_vec.into_inner().unwrap();
    //
    //         let mut scope_spans = crate::model::py_transform::transform_spans(scope_spans_vec);
    //         let mut scope_spans = scope_spans.pop().unwrap();
    //         let span = scope_spans.spans.pop().unwrap();
    //
    //         assert_eq!(2, span.events.len());
    //         let event = &span.events[0];
    //         assert_eq!("first_event", event.name);
    //         assert_eq!(123, event.time_unix_nano);
    //         assert_eq!(1, event.dropped_attributes_count);
    //         assert_eq!(1, event.attributes.len());
    //         let attr = &event.attributes[0];
    //         assert_eq!("first_event_attr_key", attr.key);
    //         assert_eq!(
    //             Value::StringValue("first_event_attr_value".to_string()),
    //             attr.value.clone().unwrap().value.unwrap()
    //         );
    //
    //         let event = &span.events[1];
    //         assert_eq!("second_event", event.name);
    //         assert_eq!(456, event.time_unix_nano);
    //         assert_eq!(2, event.dropped_attributes_count);
    //         assert_eq!(1, event.attributes.len());
    //         let attr = &event.attributes[0];
    //         assert_eq!("second_event_attr_key", attr.key);
    //         assert_eq!(
    //             Value::StringValue("second_event_attr_value".to_string()),
    //             attr.value.clone().unwrap().value.unwrap()
    //         )
    //     }
    //     #[test]
    //     fn set_scope_spans() {
    //         initialize();
    //         let export_req = FakeOTLP::trace_service_request_with_spans(1, 1);
    //         let resource_spans = crate::model::otel_transform::transform_resource_spans(
    //             export_req.resource_spans[0].clone(),
    //         );
    //         let py_resource_spans = ResourceSpans {
    //             resource: resource_spans.resource.clone(),
    //             scope_spans: resource_spans.scope_spans.clone(),
    //             schema_url: resource_spans.schema_url,
    //         };
    //         Python::with_gil(|py| -> PyResult<()> {
    //             run_script("write_scope_spans_test.py", py, py_resource_spans)
    //         })
    //         .unwrap();
    //
    //         let scope_spans_vec = Arc::into_inner(resource_spans.scope_spans).unwrap();
    //         let scope_spans_vec = scope_spans_vec.into_inner().unwrap();
    //
    //         let mut scope_spans = crate::model::py_transform::transform_spans(scope_spans_vec);
    //         let mut scope_spans = scope_spans.pop().unwrap();
    //         assert_eq!(
    //             "https://github.com/streamfold/rotel",
    //             scope_spans.schema_url
    //         );
    //         let inst_scope = scope_spans.scope.unwrap();
    //         assert_eq!("rotel-sdk-new", inst_scope.name);
    //         assert_eq!("v1.0.1", inst_scope.version);
    //         let attr = &inst_scope.attributes[0];
    //         assert_eq!("rotel-sdk", attr.key);
    //         assert_eq!(
    //             Value::StringValue("v1.0.0".to_string()),
    //             attr.value.clone().unwrap().value.unwrap()
    //         );
    //
    //         let span = scope_spans.spans.pop().unwrap();
    //         assert_eq!(b"5555555555".to_vec(), span.trace_id);
    //         assert_eq!(b"6666666666".to_vec(), span.span_id);
    //         assert_eq!("test=1234567890", span.trace_state);
    //         assert_eq!(b"7777777777".to_vec(), span.parent_span_id);
    //         assert_eq!(1, span.flags);
    //         assert_eq!("py_processed_span", span.name);
    //         assert_eq!(4, span.kind);
    //         assert_eq!(1234567890, span.start_time_unix_nano);
    //         assert_eq!(1234567890, span.end_time_unix_nano);
    //         let attr = &span.attributes[0];
    //         assert_eq!("span_attr_key", attr.key);
    //         assert_eq!(
    //             Value::StringValue("span_attr_value".to_string()),
    //             attr.value.clone().unwrap().value.unwrap()
    //         );
    //     }
    //
    //     #[test]
    //     fn set_spans() {
    //         initialize();
    //         let export_req = FakeOTLP::trace_service_request_with_spans(1, 1);
    //         let resource_spans = crate::model::otel_transform::transform_resource_spans(
    //             export_req.resource_spans[0].clone(),
    //         );
    //         let py_resource_spans = ResourceSpans {
    //             resource: resource_spans.resource.clone(),
    //             scope_spans: resource_spans.scope_spans.clone(),
    //             schema_url: resource_spans.schema_url,
    //         };
    //         Python::with_gil(|py| -> PyResult<()> {
    //             run_script("write_spans_test.py", py, py_resource_spans)
    //         })
    //         .unwrap();
    //
    //         let scope_spans_vec = Arc::into_inner(resource_spans.scope_spans).unwrap();
    //         let scope_spans_vec = scope_spans_vec.into_inner().unwrap();
    //
    //         let mut scope_spans = crate::model::py_transform::transform_spans(scope_spans_vec);
    //         let mut scope_spans = scope_spans.pop().unwrap();
    //         let span = scope_spans.spans.pop().unwrap();
    //         assert_eq!(b"5555555555".to_vec(), span.trace_id);
    //         assert_eq!(b"6666666666".to_vec(), span.span_id);
    //         assert_eq!("test=1234567890", span.trace_state);
    //         assert_eq!(b"7777777777".to_vec(), span.parent_span_id);
    //         assert_eq!(1, span.flags);
    //         assert_eq!("py_processed_span", span.name);
    //         assert_eq!(4, span.kind);
    //         assert_eq!(1234567890, span.start_time_unix_nano);
    //         assert_eq!(1234567890, span.end_time_unix_nano);
    //         let attr = &span.attributes[0];
    //         assert_eq!("span_attr_key", attr.key);
    //         assert_eq!(
    //             Value::StringValue("span_attr_value".to_string()),
    //             attr.value.clone().unwrap().value.unwrap()
    //         );
    //     }
    //
    //     #[test]
    //     fn read_and_write_log_record() {
    //         initialize();
    //
    //         // Create a mock ResourceLogs protobuf object for testing
    //         // In a real scenario, you might use a utility like FakeOTLP if available for logs.
    //         let initial_log_record = opentelemetry_proto::tonic::logs::v1::LogRecord {
    //             time_unix_nano: 1000000000,
    //             observed_time_unix_nano: 1000000001,
    //             severity_number: 9, // INFO
    //             severity_text: "INFO".to_string(),
    //             body: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
    //                 value: Some(Value::StringValue("initial log message".to_string())),
    //             }),
    //             attributes: vec![
    //                 opentelemetry_proto::tonic::common::v1::KeyValue {
    //                     key: "log.source".to_string(),
    //                     value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
    //                         value: Some(Value::StringValue("my_app".to_string())),
    //                     }),
    //                 },
    //                 opentelemetry_proto::tonic::common::v1::KeyValue {
    //                     key: "component".to_string(),
    //                     value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
    //                         value: Some(Value::StringValue("backend".to_string())),
    //                     }),
    //                 },
    //             ],
    //             dropped_attributes_count: 0,
    //             flags: 0,
    //             trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
    //             span_id: vec![17, 18, 19, 20, 21, 22, 23, 24],
    //             event_name: "".to_string(),
    //         };
    //
    //         let initial_scope_logs = opentelemetry_proto::tonic::logs::v1::ScopeLogs {
    //             scope: Some(
    //                 opentelemetry_proto::tonic::common::v1::InstrumentationScope {
    //                     name: "test-instrumentation-scope".to_string(),
    //                     version: "1.0.0".to_string(),
    //                     attributes: vec![],
    //                     dropped_attributes_count: 0,
    //                 },
    //             ),
    //             log_records: vec![initial_log_record],
    //             schema_url: "http://example.com/logs-schema".to_string(),
    //         };
    //
    //         let export_req = opentelemetry_proto::tonic::logs::v1::ResourceLogs {
    //             resource: Some(opentelemetry_proto::tonic::resource::v1::Resource {
    //                 attributes: vec![],
    //                 dropped_attributes_count: 0,
    //             }),
    //             scope_logs: vec![initial_scope_logs],
    //             schema_url: "http://example.com/resource-logs-schema".to_string(),
    //         };
    //
    //         // Transform the protobuf ResourceLogs into our internal RResourceLogs
    //         let r_resource_logs =
    //             crate::model::otel_transform::transform_resource_logs(export_req.clone());
    //
    //         // Create the Python-exposed ResourceLogs object
    //         let py_resource_logs = ResourceLogs {
    //             resource: r_resource_logs.resource.clone(),
    //             scope_logs: r_resource_logs.scope_logs.clone(),
    //             schema_url: r_resource_logs.schema_url.clone(),
    //         };
    //
    //         // Execute the Python script
    //         Python::with_gil(|py| -> PyResult<()> {
    //             run_script("read_and_write_logs_test.py", py, py_resource_logs)
    //         })
    //         .unwrap();
    //
    //         let scope_logs_vec = Arc::into_inner(r_resource_logs.scope_logs)
    //             .unwrap()
    //             .into_inner()
    //             .unwrap();
    //         let mut transformed_scope_logs = crate::model::py_transform::transform_logs(scope_logs_vec);
    //
    //         // Assert the changes made by the Python script
    //         assert_eq!(transformed_scope_logs.len(), 1);
    //         let mut scope_log = transformed_scope_logs.remove(0);
    //         assert_eq!(scope_log.log_records.len(), 1);
    //         let log_record = scope_log.log_records.remove(0);
    //
    //         assert_eq!(log_record.time_unix_nano, 2000000000);
    //         assert_eq!(log_record.observed_time_unix_nano, 2000000001);
    //         assert_eq!(log_record.severity_number, 13);
    //         assert_eq!(log_record.severity_text, "ERROR");
    //
    //         let body_value = log_record.body.unwrap().value.unwrap();
    //         match body_value {
    //             Value::StringValue(s) => {
    //                 assert_eq!(s, "processed log message");
    //             }
    //             _ => panic!("Body value is not a string"),
    //         }
    //
    //         assert_eq!(log_record.dropped_attributes_count, 5);
    //         assert_eq!(log_record.flags, 1);
    //         assert_eq!(log_record.trace_id, b"abcdefghijklmnop".to_vec());
    //         assert_eq!(log_record.span_id, b"qrstuvwx".to_vec());
    //
    //         assert_eq!(log_record.attributes.len(), 3);
    //         // Verify the new attribute
    //         let new_attr = &log_record.attributes[2];
    //         assert_eq!(new_attr.key, "new_log_attr");
    //         assert_eq!(
    //             new_attr.value.clone().unwrap().value.unwrap(),
    //             Value::StringValue("new_log_value".to_string())
    //         );
    //
    //         // Verify the modified attribute
    //         let modified_attr = &log_record.attributes[1];
    //         assert_eq!(modified_attr.key, "component");
    //         assert_eq!(
    //             modified_attr.value.clone().unwrap().value.unwrap(),
    //             Value::StringValue("modified_component_value".to_string())
    //         );
    //     }
    //
    //     #[test]
    //     fn add_new_log_record() {
    //         initialize();
    //
    //         // Create a mock ResourceLogs protobuf object with an empty ScopeLogs initially
    //         let initial_scope_logs = opentelemetry_proto::tonic::logs::v1::ScopeLogs {
    //             scope: Some(
    //                 opentelemetry_proto::tonic::common::v1::InstrumentationScope {
    //                     name: "test-instrumentation-scope".to_string(),
    //                     version: "1.0.0".to_string(),
    //                     attributes: vec![],
    //                     dropped_attributes_count: 0,
    //                 },
    //             ),
    //             log_records: vec![], // Start with no log records
    //             schema_url: "http://example.com/logs-schema".to_string(),
    //         };
    //
    //         let export_req = opentelemetry_proto::tonic::logs::v1::ResourceLogs {
    //             resource: Some(opentelemetry_proto::tonic::resource::v1::Resource {
    //                 attributes: vec![],
    //                 dropped_attributes_count: 0,
    //             }),
    //             scope_logs: vec![initial_scope_logs],
    //             schema_url: "http://example.com/resource-logs-schema".to_string(),
    //         };
    //
    //         // Transform the protobuf ResourceLogs into our internal RResourceLogs
    //         let r_resource_logs =
    //             crate::model::otel_transform::transform_resource_logs(export_req.clone());
    //
    //         // Create the Python-exposed ResourceLogs object
    //         let py_resource_logs = ResourceLogs {
    //             resource: r_resource_logs.resource.clone(),
    //             scope_logs: r_resource_logs.scope_logs.clone(),
    //             schema_url: r_resource_logs.schema_url.clone(),
    //         };
    //
    //         // Execute the Python script that adds a new log record
    //         Python::with_gil(|py| -> PyResult<()> {
    //             run_script("add_log_record_test.py", py, py_resource_logs)
    //         })
    //         .unwrap();
    //
    //         // Transform the modified RResourceLogs back into protobuf format
    //         let mut resource = r_resource_logs.resource.lock().unwrap();
    //         let _resource_proto = resource
    //             .take()
    //             .map(|r| crate::model::py_transform::transform_resource(r).unwrap());
    //
    //         let scope_logs_vec = Arc::into_inner(r_resource_logs.scope_logs)
    //             .unwrap()
    //             .into_inner()
    //             .unwrap();
    //         let mut transformed_scope_logs = crate::model::py_transform::transform_logs(scope_logs_vec);
    //
    //         // Assert the changes made by the Python script
    //         assert_eq!(transformed_scope_logs.len(), 1);
    //         let mut scope_log = transformed_scope_logs.remove(0);
    //         assert_eq!(scope_log.log_records.len(), 1); // Expecting one log record now
    //         let log_record = scope_log.log_records.remove(0);
    //
    //         assert_eq!(log_record.time_unix_nano, 9876543210);
    //         assert_eq!(log_record.observed_time_unix_nano, 9876543211);
    //         assert_eq!(log_record.severity_number, 17); // FATAL
    //         assert_eq!(log_record.severity_text, "FATAL");
    //
    //         let body_value = log_record.body.unwrap().value.unwrap();
    //         match body_value {
    //             Value::StringValue(s) => {
    //                 assert_eq!(s, "This is a newly added log message.");
    //             }
    //             _ => panic!("Body value is not a string"),
    //         }
    //
    //         assert_eq!(log_record.attributes.len(), 1);
    //         let new_attr = &log_record.attributes[0];
    //         assert_eq!(new_attr.key, "new_log_key");
    //         assert_eq!(
    //             new_attr.value.clone().unwrap().value.unwrap(),
    //             Value::StringValue("new_log_value".to_string())
    //         );
    //
    //         assert_eq!(log_record.dropped_attributes_count, 2);
    //         assert_eq!(log_record.flags, 4);
    //         assert_eq!(log_record.trace_id, b"fedcba9876543210".to_vec());
    //         assert_eq!(log_record.span_id, b"fedcba98".to_vec());
    //     }
    //
    //     #[test]
    //     fn remove_log_record_test() {
    //         initialize();
    //
    //         // Create a mock ResourceLogs protobuf object with two initial LogRecords
    //         let first_log_record = opentelemetry_proto::tonic::logs::v1::LogRecord {
    //             time_unix_nano: 1000000000,
    //             observed_time_unix_nano: 1000000001,
    //             severity_number: 9, // INFO
    //             severity_text: "INFO".to_string(),
    //             body: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
    //                 value: Some(
    //                     opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
    //                         "first log message".to_string(),
    //                     ),
    //                 ),
    //             }),
    //             attributes: vec![],
    //             dropped_attributes_count: 0,
    //             flags: 0,
    //             trace_id: vec![],
    //             span_id: vec![],
    //             event_name: "first_event".to_string(),
    //         };
    //
    //         let second_log_record = opentelemetry_proto::tonic::logs::v1::LogRecord {
    //             time_unix_nano: 2000000000,
    //             observed_time_unix_nano: 2000000001,
    //             severity_number: 13, // ERROR
    //             severity_text: "ERROR".to_string(),
    //             body: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
    //                 value: Some(Value::StringValue("second log message".to_string())),
    //             }),
    //             attributes: vec![],
    //             dropped_attributes_count: 0,
    //             flags: 0,
    //             trace_id: vec![],
    //             span_id: vec![],
    //             event_name: "second_event".to_string(),
    //         };
    //
    //         let initial_scope_logs = opentelemetry_proto::tonic::logs::v1::ScopeLogs {
    //             scope: Some(
    //                 opentelemetry_proto::tonic::common::v1::InstrumentationScope {
    //                     name: "test-instrumentation-scope".to_string(),
    //                     version: "1.0.0".to_string(),
    //                     attributes: vec![],
    //                     dropped_attributes_count: 0,
    //                 },
    //             ),
    //             log_records: vec![first_log_record, second_log_record], // Two log records
    //             schema_url: "http://example.com/logs-schema".to_string(),
    //         };
    //
    //         let export_req = opentelemetry_proto::tonic::logs::v1::ResourceLogs {
    //             resource: Some(opentelemetry_proto::tonic::resource::v1::Resource {
    //                 attributes: vec![],
    //                 dropped_attributes_count: 0,
    //             }),
    //             scope_logs: vec![initial_scope_logs],
    //             schema_url: "http://example.com/resource-logs-schema".to_string(),
    //         };
    //
    //         // Transform the protobuf ResourceLogs into our internal RResourceLogs
    //         let r_resource_logs =
    //             crate::model::otel_transform::transform_resource_logs(export_req.clone());
    //
    //         // Create the Python-exposed ResourceLogs object
    //         let py_resource_logs = ResourceLogs {
    //             resource: r_resource_logs.resource.clone(),
    //             scope_logs: r_resource_logs.scope_logs.clone(),
    //             schema_url: r_resource_logs.schema_url.clone(),
    //         };
    //
    //         // Execute the Python script that removes a log record
    //         Python::with_gil(|py| -> PyResult<()> {
    //             run_script("remove_log_record_test.py", py, py_resource_logs)
    //         })
    //         .unwrap();
    //
    //         // Transform the modified RResourceLogs back into protobuf format
    //         let mut resource = r_resource_logs.resource.lock().unwrap();
    //         let _resource_proto = resource
    //             .take()
    //             .map(|r| crate::model::py_transform::transform_resource(r).unwrap());
    //
    //         let scope_logs_vec = Arc::into_inner(r_resource_logs.scope_logs)
    //             .unwrap()
    //             .into_inner()
    //             .unwrap();
    //         let mut transformed_scope_logs = crate::model::py_transform::transform_logs(scope_logs_vec);
    //
    //         // Assert the changes made by the Python script
    //         assert_eq!(transformed_scope_logs.len(), 1);
    //         let mut scope_log = transformed_scope_logs.remove(0);
    //         assert_eq!(scope_log.log_records.len(), 1); // Expecting one log record now
    //         let log_record = scope_log.log_records.remove(0);
    //
    //         // Verify that the correct log record remains
    //         let body_value = log_record.body.unwrap().value.unwrap();
    //         match body_value {
    //             Value::StringValue(s) => {
    //                 assert_eq!(s, "first log message");
    //             }
    //             _ => panic!("Body value is not the expected string"),
    //         }
    //     }
    //
    //     #[test]
    //     fn traces_delitem_test() {
    //         initialize();
    //         let av = opentelemetry_proto::tonic::common::v1::ArrayValue {
    //             values: vec![
    //                 opentelemetry_proto::tonic::common::v1::AnyValue {
    //                     value: Some(Value::StringValue("first value".to_string())),
    //                 },
    //                 opentelemetry_proto::tonic::common::v1::AnyValue {
    //                     value: Some(Value::StringValue("second value".to_string())),
    //                 },
    //             ],
    //         };
    //         let kvlist = opentelemetry_proto::tonic::common::v1::KeyValueList {
    //             values: vec![
    //                 opentelemetry_proto::tonic::common::v1::KeyValue {
    //                     key: "first_key".to_string(),
    //                     value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
    //                         value: Some(Value::BoolValue(true)),
    //                     }),
    //                 },
    //                 opentelemetry_proto::tonic::common::v1::KeyValue {
    //                     key: "second_key".to_string(),
    //                     value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
    //                         value: Some(Value::StringValue("second_value".to_string())),
    //                     }),
    //                 },
    //             ],
    //         };
    //         let resource = opentelemetry_proto::tonic::resource::v1::Resource {
    //             attributes: vec![
    //                 opentelemetry_proto::tonic::common::v1::KeyValue {
    //                     key: "first_attr".to_string(),
    //                     value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
    //                         value: Some(Value::KvlistValue(kvlist)),
    //                     }),
    //                 },
    //                 opentelemetry_proto::tonic::common::v1::KeyValue {
    //                     key: "second_attr".to_string(),
    //                     value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
    //                         value: Some(Value::ArrayValue(av)),
    //                     }),
    //                 },
    //                 opentelemetry_proto::tonic::common::v1::KeyValue {
    //                     key: "third_attr".to_string(),
    //                     value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
    //                         value: Some(Value::StringValue("to_remove_value".to_string())),
    //                     }),
    //                 },
    //             ],
    //             dropped_attributes_count: 0,
    //         };
    //
    //         let mut spans = FakeOTLP::trace_spans(2);
    //         let now_ns = Utc::now().timestamp_nanos_opt().unwrap();
    //
    //         // Adding additional data here to test __delitem__
    //         spans[0].events.push(v1::span::Event {
    //             time_unix_nano: now_ns as u64,
    //             name: "second test event".to_string(),
    //             attributes: vec![],
    //             dropped_attributes_count: 0,
    //         });
    //
    //         spans[0].links.push(v1::span::Link {
    //             trace_id: vec![5, 5, 5, 5, 5, 5, 5, 5],
    //             span_id: vec![6, 6, 6, 6, 6, 6, 6, 6],
    //             trace_state: "secondtrace=00f067aa0ba902b7".to_string(),
    //             attributes: vec![],
    //             dropped_attributes_count: 0,
    //             flags: 0,
    //         });
    //
    //         let first_scope_spans = v1::ScopeSpans {
    //             scope: None,
    //             spans,
    //             schema_url: "".to_string(),
    //         };
    //
    //         let second_scope_spans = v1::ScopeSpans {
    //             scope: None,
    //             spans: vec![],
    //             schema_url: "".to_string(),
    //         };
    //
    //         let resource_spans = v1::ResourceSpans {
    //             resource: Some(resource),
    //             scope_spans: vec![first_scope_spans, second_scope_spans],
    //             schema_url: "".to_string(),
    //         };
    //
    //         // Transform the protobuf ResourceLogs into our internal RResourceLogs
    //         let r_resource_spans =
    //             crate::model::otel_transform::transform_resource_spans(resource_spans);
    //
    //         // Create the Python-exposed ResourceLogs object
    //         let py_resource_spans = ResourceSpans {
    //             resource: r_resource_spans.resource.clone(),
    //             scope_spans: r_resource_spans.scope_spans.clone(),
    //             schema_url: r_resource_spans.schema_url.clone(),
    //         };
    //
    //         // Execute the Python script that removes a log record
    //         Python::with_gil(|py| -> PyResult<()> {
    //             run_script("traces_delitem_test.py", py, py_resource_spans)
    //         })
    //         .unwrap();
    //
    //         let mut resource = r_resource_spans.resource.lock().unwrap();
    //         let mut resource_p = resource
    //             .take()
    //             .map(|r| crate::model::py_transform::transform_resource(r).unwrap())
    //             .unwrap();
    //
    //         assert_eq!(2, resource_p.attributes.len());
    //         // Assert the kvlist only has one item now
    //         let kvlist = resource_p
    //             .attributes
    //             .remove(0)
    //             .value
    //             .unwrap()
    //             .value
    //             .unwrap();
    //         match kvlist {
    //             Value::KvlistValue(mut kvl) => {
    //                 assert_eq!(1, kvl.values.len());
    //                 let value = kvl.values.remove(0);
    //                 assert_eq!(value.key, "second_key");
    //                 let value = value.value.clone().unwrap().value.unwrap();
    //                 match value {
    //                     Value::StringValue(s) => {
    //                         assert_eq!(s, "second_value");
    //                     }
    //                     _ => {
    //                         panic!("expected StringValue")
    //                     }
    //                 }
    //             }
    //             _ => {
    //                 panic!("expected kvlist")
    //             }
    //         }
    //
    //         let arvalue = resource_p
    //             .attributes
    //             .remove(0)
    //             .value
    //             .unwrap()
    //             .value
    //             .unwrap();
    //         match arvalue {
    //             Value::ArrayValue(mut av) => {
    //                 assert_eq!(1, av.values.len());
    //                 let value = av.values.remove(0).value.unwrap();
    //                 match value {
    //                     Value::StringValue(s) => {
    //                         assert_eq!(s, "first value")
    //                     }
    //                     _ => {
    //                         panic!("expected StringValue");
    //                     }
    //                 }
    //             }
    //             _ => {
    //                 panic!("expected ArrayValue");
    //             }
    //         }
    //
    //         // Verify the second scope span has been removed
    //         let scope_spans_vec = Arc::into_inner(r_resource_spans.scope_spans)
    //             .unwrap()
    //             .into_inner()
    //             .unwrap();
    //         let mut scope_spans_vec = crate::model::py_transform::transform_spans(scope_spans_vec);
    //         assert_eq!(1, scope_spans_vec.len());
    //         let scope_spans = scope_spans_vec.remove(0);
    //         let mut spans = scope_spans.spans;
    //         assert_eq!(1, spans.len());
    //         let span = spans.remove(0);
    //         assert_eq!(1, span.events.len());
    //         assert_eq!(span.events[0].name, "second test event");
    //         assert_eq!(1, span.links.len());
    //         assert_eq!(span.links[0].trace_state, "secondtrace=00f067aa0ba902b7")
    //     }
    //
    //     #[test]
    //     fn attributes_processor_test() {
    //         initialize();
    //         let mut log_request = FakeOTLP::logs_service_request();
    //
    //         let attrs = vec![
    //             opentelemetry_proto::tonic::common::v1::KeyValue {
    //                 key: "http.status_code".to_string(),
    //                 value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
    //                     value: Some(Value::IntValue(200)),
    //                 }),
    //             },
    //             opentelemetry_proto::tonic::common::v1::KeyValue {
    //                 key: "user.id".to_string(),
    //                 value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
    //                     value: Some(Value::StringValue("abc-123".to_string())),
    //                 }),
    //             },
    //             opentelemetry_proto::tonic::common::v1::KeyValue {
    //                 key: "user.email".to_string(),
    //                 value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
    //                     value: Some(Value::StringValue("test@example.com".to_string())),
    //                 }),
    //             },
    //             opentelemetry_proto::tonic::common::v1::KeyValue {
    //                 key: "request.id".to_string(),
    //                 value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
    //                     value: Some(Value::StringValue("req-456".to_string())),
    //                 }),
    //             },
    //             opentelemetry_proto::tonic::common::v1::KeyValue {
    //                 key: "message".to_string(),
    //                 value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
    //                     value: Some(Value::StringValue("User login successful.".to_string())),
    //                 }),
    //             },
    //             opentelemetry_proto::tonic::common::v1::KeyValue {
    //                 key: "raw_data".to_string(),
    //                 value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
    //                     value: Some(Value::StringValue("id:123,name:Alice,age:30".to_string())),
    //                 }),
    //             },
    //             opentelemetry_proto::tonic::common::v1::KeyValue {
    //                 key: "temp_str_int".to_string(),
    //                 value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
    //                     value: Some(Value::StringValue("123".to_string())),
    //                 }),
    //             },
    //             opentelemetry_proto::tonic::common::v1::KeyValue {
    //                 key: "temp_str_bool".to_string(),
    //                 value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
    //                     value: Some(Value::StringValue("true".to_string())),
    //                 }),
    //             },
    //             opentelemetry_proto::tonic::common::v1::KeyValue {
    //                 key: "temp_str_float".to_string(),
    //                 value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
    //                     value: Some(Value::DoubleValue(10.0)),
    //                 }),
    //             },
    //             opentelemetry_proto::tonic::common::v1::KeyValue {
    //                 key: "path".to_string(),
    //                 value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
    //                     value: Some(Value::StringValue("https://rotel.dev/blog".to_string())),
    //                 }),
    //             },
    //             opentelemetry_proto::tonic::common::v1::KeyValue {
    //                 key: "super.secret".to_string(),
    //                 value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
    //                     value: Some(Value::StringValue("don't tell anyone".to_string())),
    //                 }),
    //             },
    //             opentelemetry_proto::tonic::common::v1::KeyValue {
    //                 key: "trace.id".to_string(),
    //                 value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
    //                     value: Some(Value::StringValue("12345678".to_string())),
    //                 }),
    //             },
    //         ];
    //         log_request.resource_logs[0].scope_logs[0].log_records[0].attributes = attrs.clone();
    //
    //         // Transform the protobuf ResourceLogs into our internal RResourceLogs
    //         let r_resource_logs = crate::model::otel_transform::transform_resource_logs(
    //             log_request.resource_logs[0].clone(),
    //         );
    //
    //         // Create the Python-exposed ResourceLogs object
    //         let py_resource_logs = ResourceLogs {
    //             resource: r_resource_logs.resource.clone(),
    //             scope_logs: r_resource_logs.scope_logs.clone(),
    //             schema_url: r_resource_logs.schema_url.clone(),
    //         };
    //
    //         // Execute the Python script that removes a log record
    //         Python::with_gil(|py| -> PyResult<()> {
    //             _run_script(
    //                 "attributes_processor_test.py",
    //                 py,
    //                 py_resource_logs,
    //                 Some("process_logs".to_string()),
    //             )
    //         })
    //         .unwrap();
    //
    //         let scope_logs_vec = Arc::into_inner(r_resource_logs.scope_logs)
    //             .unwrap()
    //             .into_inner()
    //             .unwrap();
    //         let mut scope_logs = crate::model::py_transform::transform_logs(scope_logs_vec);
    //
    //         let log_record = scope_logs.pop().unwrap().log_records.pop().unwrap();
    //         let attrs_to_verify: HashMap<
    //             String,
    //             Option<opentelemetry_proto::tonic::common::v1::AnyValue>,
    //         > = log_record
    //             .attributes
    //             .into_iter()
    //             .map(|kv| (kv.key, kv.value))
    //             .collect();
    //
    //         let verify_attrs = |mut attrs: HashMap<
    //             String,
    //             Option<opentelemetry_proto::tonic::common::v1::AnyValue>,
    //         >| {
    //             let host_name = attrs.remove("host.name").unwrap();
    //             match host_name.unwrap().value.unwrap() {
    //                 Value::StringValue(h) => {
    //                     assert_eq!(h, "my-server-1");
    //                 }
    //                 _ => panic!("Unexpected value type"),
    //             }
    //             let http_status_code = attrs.remove("http.status_code").unwrap();
    //             match http_status_code.unwrap().value.unwrap() {
    //                 Value::StringValue(c) => {
    //                     assert_eq!(c, "OK");
    //                 }
    //                 _ => panic!("Unexpected value type"),
    //             }
    //             let env = attrs.remove("env").unwrap();
    //             match env.unwrap().value.unwrap() {
    //                 Value::StringValue(e) => {
    //                     assert_eq!(e, "production");
    //                 }
    //                 _ => panic!("Unexpected value type"),
    //             }
    //             let email = attrs.remove("email").unwrap();
    //             match email.unwrap().value.unwrap() {
    //                 Value::StringValue(h) => {
    //                     assert_eq!(h, "test@example.com");
    //                 }
    //                 _ => panic!("Unexpected value type"),
    //             }
    //             let user_id = attrs.remove("user.id").unwrap();
    //             match user_id.unwrap().value.unwrap() {
    //                 Value::StringValue(s) => {
    //                     assert_eq!(
    //                         s,
    //                         "5942d94f524882e0f29bf0a1e5a6dcc952eea1c0c21dd3588a3fc7db9716db0c"
    //                     );
    //                 }
    //                 _ => panic!("Unexpected value type"),
    //             }
    //             let trace_id = attrs.remove("trace.id").unwrap();
    //             match trace_id.unwrap().value.unwrap() {
    //                 Value::StringValue(s) => {
    //                     assert_eq!(
    //                         s,
    //                         "ef797c8118f02dfb649607dd5d3f8c7623048c9c063d532cc95c5ed7a898a64f"
    //                     );
    //                 }
    //                 _ => panic!("Unexpected value type"),
    //             }
    //             let id = attrs.remove("extracted_id").unwrap();
    //             match id.unwrap().value.unwrap() {
    //                 Value::StringValue(s) => {
    //                     assert_eq!(s, "123");
    //                 }
    //                 _ => panic!("Unexpected value type"),
    //             }
    //             let temp_str_int = attrs.remove("temp_str_int").unwrap();
    //             match temp_str_int.unwrap().value.unwrap() {
    //                 Value::IntValue(i) => {
    //                     assert_eq!(i, 123);
    //                 }
    //                 _ => panic!("Unexpected value type"),
    //             }
    //             let temp_str_bool = attrs.remove("temp_str_bool").unwrap();
    //             match temp_str_bool.unwrap().value.unwrap() {
    //                 Value::BoolValue(b) => {
    //                     assert_eq!(b, true);
    //                 }
    //                 _ => panic!("Unexpected value type"),
    //             }
    //             let temp_str_float = attrs.remove("temp_str_float").unwrap();
    //             match temp_str_float.unwrap().value.unwrap() {
    //                 Value::DoubleValue(d) => {
    //                     assert_eq!(d, 10.0);
    //                 }
    //                 _ => panic!("Unexpected value type"),
    //             }
    //             let path = attrs.remove("path");
    //             assert_eq!(path, None);
    //             let super_secret = attrs.remove("super.secret");
    //             assert_eq!(super_secret, None);
    //         };
    //
    //         verify_attrs(attrs_to_verify);
    //
    //         let mut trace_request = FakeOTLP::trace_service_request();
    //         trace_request.resource_spans[0].scope_spans[0].spans[0].attributes = attrs.clone();
    //
    //         // Transform the protobuf ResourceLogs into our internal RResourceLogs
    //         let r_resource_spans = crate::model::otel_transform::transform_resource_spans(
    //             trace_request.resource_spans[0].clone(),
    //         );
    //
    //         // Create the Python-exposed ResourceLogs object
    //         let py_resource_spans = ResourceSpans {
    //             resource: r_resource_spans.resource.clone(),
    //             scope_spans: r_resource_spans.scope_spans.clone(),
    //             schema_url: r_resource_spans.schema_url.clone(),
    //         };
    //
    //         // Execute the Python script that removes a log record
    //         Python::with_gil(|py| -> PyResult<()> {
    //             _run_script(
    //                 "attributes_processor_test.py",
    //                 py,
    //                 py_resource_spans,
    //                 Some("process_spans".to_string()),
    //             )
    //         })
    //         .unwrap();
    //
    //         let scope_spans_vec = Arc::into_inner(r_resource_spans.scope_spans)
    //             .unwrap()
    //             .into_inner()
    //             .unwrap();
    //         let mut scope_spans = crate::model::py_transform::transform_spans(scope_spans_vec);
    //
    //         let span = scope_spans.pop().unwrap().spans.pop().unwrap();
    //         let attrs_to_verify: HashMap<
    //             String,
    //             Option<opentelemetry_proto::tonic::common::v1::AnyValue>,
    //         > = span
    //             .attributes
    //             .into_iter()
    //             .map(|kv| (kv.key, kv.value))
    //             .collect();
    //
    //         verify_attrs(attrs_to_verify);
    //     }
    #[test]
    fn redaction_processor_test() {
        initialize();
        let resource_attrs = vec![
            opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "host.arch".to_string(),
                value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(Value::StringValue("amd64".to_string())),
                }),
            },
            opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "os.type".to_string(),
                value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(Value::StringValue("linux".to_string())),
                }),
            },
            opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "region".to_string(),
                value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(Value::StringValue("us-east-1".to_string())),
                }),
            },
            opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "service.name".to_string(),
                value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(Value::StringValue("my-service".to_string())),
                }),
            },
            opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "unallowed_resource_key".to_string(),
                value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(Value::StringValue("some_value".to_string())),
                }),
            },
            opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "deployment.environment".to_string(),
                value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(Value::StringValue("prod".to_string())),
                }),
            },
        ];

        let first_span_attrs = vec![
            opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "user.email".to_string(),
                value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(Value::StringValue("test@example.com".to_string())),
                }),
            },
            opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "http.url".to_string(),
                value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(Value::StringValue(
                        "https://example.com/sensitive/path".to_string(),
                    )),
                }),
            },
            opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "db.statement".to_string(),
                value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(Value::StringValue(
                        "SELECT * FROM users WHERE id = 1".to_string(),
                    )),
                }),
            },
            opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "operation".to_string(),
                value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(Value::StringValue("get_data".to_string())),
                }),
            },
            opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "some_token".to_string(),
                value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(Value::StringValue("abcdef123".to_string())),
                }),
            },
            opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "api_key_header".to_string(),
                value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(Value::StringValue("xyz789".to_string())),
                }),
            },
            opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "safe_attribute".to_string(),
                value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(Value::StringValue("this should remain".to_string())),
                }),
            },
            opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "unallowed_span_key".to_string(),
                value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(Value::StringValue("should_be_deleted".to_string())),
                }),
            },
            opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "my_company_email".to_string(),
                value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(Value::StringValue("user@mycompany.com".to_string())),
                }),
            },
        ];

        let mut trace_request = FakeOTLP::trace_service_request();
        trace_request.resource_spans[0].resource =
            Some(opentelemetry_proto::tonic::resource::v1::Resource {
                attributes: resource_attrs.clone(),
                dropped_attributes_count: 0,
            });
        trace_request.resource_spans[0].scope_spans[0].spans[0].attributes =
            first_span_attrs.clone();

        // Transform the protobuf ResourceLogs into our internal RResourceLogs
        let r_resource_spans = crate::model::otel_transform::transform_resource_spans(
            trace_request.resource_spans[0].clone(),
        );

        // Create the Python-exposed ResourceLogs object
        let py_resource_spans = ResourceSpans {
            resource: r_resource_spans.resource.clone(),
            scope_spans: r_resource_spans.scope_spans.clone(),
            schema_url: r_resource_spans.schema_url.clone(),
        };

        // Execute the Python script that removes a log record
        Python::with_gil(|py| -> PyResult<()> {
            _run_script(
                "redaction_processor_test.py",
                py,
                py_resource_spans,
                Some("process_spans".to_string()),
            )
        })
        .unwrap();

        let scope_spans_vec = Arc::into_inner(r_resource_spans.scope_spans)
            .unwrap()
            .into_inner()
            .unwrap();
        let mut scope_spans = crate::model::py_transform::transform_spans(scope_spans_vec);

        let span = scope_spans.pop().unwrap().spans.pop().unwrap();
        println!("span is : {:?}", span);
    }

    //     MockSpan(
    //     name="another-span",
    //     attributes={
    // "another.token.value": "a_secret", # Blocked key pattern
    // "customer_card_number": "4111222233334444", # Blocked value
    // "allowed_key_present": "data", # Allowed key
    // "os.type": "macos", # Allowed key
    // "ignored_key_for_this_span": "irrelevant" # Ignored key (hypothetical)
    // }
    //     )
    //     ]
    //     )
    //     ]
    //     )
    //     ]
    //     )
}
