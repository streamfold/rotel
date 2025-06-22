pub mod common;
pub mod logs;
pub mod metrics;
pub mod resource;
pub mod trace;

use crate::py;
use crate::py::common::KeyValue;
use crate::py::metrics::{
    AggregationTemporality, DataPointFlags, Exemplar, ExemplarValue, ExponentialHistogram,
    ExponentialHistogramBuckets, ExponentialHistogramDataPoint, Gauge, Histogram,
    HistogramDataPoint, Metric, MetricData, NumberDataPoint, NumberDataPointValue, ResourceMetrics,
    ScopeMetrics, Sum, Summary, SummaryDataPoint, ValueAtQuantile,
};
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
    let metrics_module = PyModule::new(open_telemetry_module.py(), "metrics")?; // Added logs module
    let trace_v1_module = PyModule::new(trace_module.py(), "v1")?;
    let common_v1_module = PyModule::new(common_module.py(), "v1")?;
    let resource_v1_module = PyModule::new(resource_module.py(), "v1")?;
    let logs_v1_module = PyModule::new(logs_module.py(), "v1")?;
    let metrics_v1_module = PyModule::new(metrics_module.py(), "v1")?; // Added logs v1 module

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

    m.py()
        .import("sys")?
        .getattr("modules")?
        .set_item("rotel_sdk.open_telemetry.metrics", &metrics_module)?;
    m.py()
        .import("sys")?
        .getattr("modules")?
        .set_item("rotel_sdk.open_telemetry.metrics.v1", &metrics_v1_module)?;

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

    metrics_v1_module.add_class::<ResourceMetrics>()?;
    metrics_v1_module.add_class::<ScopeMetrics>()?;
    metrics_v1_module.add_class::<Metric>()?;
    metrics_v1_module.add_class::<MetricData>()?;
    metrics_v1_module.add_class::<Gauge>()?;
    metrics_v1_module.add_class::<Sum>()?;
    metrics_v1_module.add_class::<Histogram>()?;
    metrics_v1_module.add_class::<ExponentialHistogram>()?;
    metrics_v1_module.add_class::<Summary>()?;
    metrics_v1_module.add_class::<NumberDataPoint>()?;
    metrics_v1_module.add_class::<HistogramDataPoint>()?;
    metrics_v1_module.add_class::<Exemplar>()?;
    metrics_v1_module.add_class::<ExemplarValue>()?;
    metrics_v1_module.add_class::<AggregationTemporality>()?;
    metrics_v1_module.add_class::<DataPointFlags>()?;
    metrics_v1_module.add_class::<NumberDataPointValue>()?;
    metrics_v1_module.add_class::<ExponentialHistogramBuckets>()?;
    metrics_v1_module.add_class::<ExponentialHistogramDataPoint>()?;
    metrics_v1_module.add_class::<SummaryDataPoint>()?;
    metrics_v1_module.add_class::<ValueAtQuantile>()?;

    Ok(())
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::*;
    use crate::model::common::RValue::*;
    use crate::model::common::{RAnyValue, RKeyValue};
    use crate::model::{otel_transform, py_transform};
    use crate::py::metrics::ResourceMetrics;
    use chrono::Utc;
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    use opentelemetry_proto::tonic::trace::v1;
    use pyo3::ffi::c_str;
    use std::collections::HashMap;
    use std::ffi::CString;
    use std::sync::{Arc, Mutex, Once};
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

    #[test]
    fn test_read_any_value() {
        initialize();
        let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
        let any_value_arc = Arc::new(Mutex::new(Some(RAnyValue {
            value: arc_value.clone(),
        })));

        let pv = AnyValue {
            inner: any_value_arc.clone(),
        };
        Python::with_gil(|py| -> PyResult<()> { run_script("read_value_test.py", py, pv) })
            .unwrap();
        let av = any_value_arc.lock().unwrap().clone().unwrap();
        let avx = av.value.lock().unwrap().clone();
        match avx.unwrap() {
            StringValue(s) => {
                assert_eq!(s, "foo");
            }
            _ => panic!("wrong type"),
        }
        println!("{:?}", any_value_arc.lock().unwrap().clone().unwrap());
    }

    #[test]
    fn write_string_any_value() {
        initialize();
        let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
        let any_value_arc = Arc::new(Mutex::new(Some(RAnyValue {
            value: arc_value.clone(),
        })));
        let pv = AnyValue {
            inner: any_value_arc.clone(),
        };
        Python::with_gil(|py| -> PyResult<()> { run_script("write_string_value_test.py", py, pv) })
            .unwrap();
        let av = any_value_arc.lock().unwrap().clone().unwrap();
        let avx = av.value.lock().unwrap().clone();
        match avx.unwrap() {
            StringValue(s) => {
                assert_eq!(s, "changed");
            }
            _ => panic!("wrong type"),
        }
        println!("{:?}", any_value_arc.lock().unwrap().clone().unwrap());
    }

    #[test]
    fn write_bool_any_value() {
        initialize();
        let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
        let any_value_arc = Arc::new(Mutex::new(Some(RAnyValue {
            value: arc_value.clone(),
        })));

        let pv = AnyValue {
            inner: any_value_arc.clone(),
        };

        Python::with_gil(|py| -> PyResult<()> { run_script("write_bool_value_test.py", py, pv) })
            .unwrap();
        match arc_value.lock().unwrap().clone().unwrap() {
            BoolValue(b) => {
                assert!(b);
            }
            _ => panic!("wrong type"),
        }
        println!("{:?}", any_value_arc.lock().unwrap().clone().unwrap());
    }

    #[test]
    fn write_bytes_any_value() {
        initialize();
        let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
        let any_value_arc = Arc::new(Mutex::new(Some(RAnyValue {
            value: arc_value.clone(),
        })));

        let pv = AnyValue {
            inner: any_value_arc.clone(),
        };

        Python::with_gil(|py| -> PyResult<()> { run_script("write_bytes_value_test.py", py, pv) })
            .unwrap();
        match arc_value.lock().unwrap().clone().unwrap() {
            BytesValue(b) => {
                assert_eq!(b"111111".to_vec(), b);
            }
            _ => panic!("wrong type"),
        }
        println!("{:?}", any_value_arc.lock().unwrap().clone().unwrap());
    }

    #[test]
    fn read_key_value_key() {
        initialize();
        let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
        let any_value_arc = Arc::new(Mutex::new(Some(RAnyValue {
            value: arc_value.clone(),
        })));
        let key = Arc::new(Mutex::new("key".to_string()));

        let kv = KeyValue {
            inner: Arc::new(Mutex::new(RKeyValue {
                key: key.clone(),
                value: any_value_arc.clone(),
            })),
        };

        Python::with_gil(|py| -> PyResult<()> { run_script("read_key_value_key_test.py", py, kv) })
            .unwrap();
        let av = key.clone().lock().unwrap().clone();
        assert_eq!(av, "key".to_string());
        println!("{:?}", av);
    }

    #[test]
    fn write_key_value_key() {
        initialize();
        let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
        let any_value_arc = Arc::new(Mutex::new(Some(RAnyValue {
            value: arc_value.clone(),
        })));
        let key = Arc::new(Mutex::new("key".to_string()));

        let kv = KeyValue {
            inner: Arc::new(Mutex::new(RKeyValue {
                key: key.clone(),
                value: any_value_arc.clone(),
            })),
        };

        Python::with_gil(|py| -> PyResult<()> {
            run_script("write_key_value_key_test.py", py, kv)
        })
        .unwrap();
        let av = key.clone().lock().unwrap().clone();
        assert_eq!(av, "new_key".to_string());
        println!("{:?}", av);
    }

    #[test]
    fn read_key_value_value() {
        initialize();
        let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
        let any_value_arc = Arc::new(Mutex::new(Some(RAnyValue {
            value: arc_value.clone(),
        })));
        let key = Arc::new(Mutex::new("key".to_string()));

        let kv = KeyValue {
            inner: Arc::new(Mutex::new(RKeyValue {
                key: key.clone(),
                value: any_value_arc.clone(),
            })),
        };

        Python::with_gil(|py| -> PyResult<()> {
            run_script("read_key_value_value_test.py", py, kv)
        })
        .unwrap();
        match arc_value.lock().unwrap().clone().unwrap() {
            StringValue(s) => {
                assert_eq!(s, "foo");
            }
            _ => panic!("wrong type"),
        }
        println!("{:?}", any_value_arc.lock().unwrap().clone().unwrap());
    }

    #[test]
    fn write_key_value_value() {
        initialize();
        let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
        let any_value_arc = Arc::new(Mutex::new(Some(RAnyValue {
            value: arc_value.clone(),
        })));
        let key = Arc::new(Mutex::new("key".to_string()));

        let kv = KeyValue {
            inner: Arc::new(Mutex::new(RKeyValue {
                key: key.clone(),
                value: any_value_arc.clone(),
            })),
        };

        Python::with_gil(|py| -> PyResult<()> {
            run_script("write_key_value_value_test.py", py, kv)
        })
        .unwrap();
        match arc_value.lock().unwrap().clone().unwrap() {
            StringValue(s) => {
                assert_eq!(s, "changed");
            }
            _ => panic!("wrong type"),
        }
        println!("{:?}", any_value_arc.lock().unwrap().clone().unwrap());
    }

    #[test]
    fn write_key_value_bytes_value() {
        initialize();
        let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
        let any_value_arc = Arc::new(Mutex::new(Some(RAnyValue {
            value: arc_value.clone(),
        })));
        let key = Arc::new(Mutex::new("key".to_string()));

        let kv = KeyValue {
            inner: Arc::new(Mutex::new(RKeyValue {
                key: key.clone(),
                value: any_value_arc.clone(),
            })),
        };

        Python::with_gil(|py| -> PyResult<()> {
            run_script("write_key_value_bytes_value_test.py", py, kv)
        })
        .unwrap();
        match arc_value.lock().unwrap().clone().unwrap() {
            BytesValue(s) => {
                assert_eq!(b"111111".to_vec(), s);
            }
            _ => panic!("wrong type"),
        }
        println!("{:?}", any_value_arc.lock().unwrap().clone().unwrap());
    }

    #[test]
    fn read_resource_attributes() {
        initialize();
        let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
        let any_value_arc = Arc::new(Mutex::new(Some(RAnyValue {
            value: arc_value.clone(),
        })));
        let key = Arc::new(Mutex::new("key".to_string()));

        let kv = RKeyValue {
            key: key.clone(),
            value: any_value_arc.clone(),
        };

        let kv_arc = Arc::new(Mutex::new(kv));

        let resource = Resource {
            attributes: Arc::new(Mutex::new(vec![kv_arc.clone()])),
            dropped_attributes_count: Arc::new(Mutex::new(0)),
        };

        Python::with_gil(|py| -> PyResult<()> {
            run_script("read_resource_attributes_test.py", py, resource)
        })
        .unwrap();
    }

    #[test]
    fn read_and_write_attributes_array_value() {
        initialize();

        let arc_value = Some(StringValue("foo".to_string()));
        let any_value_arc = Some(RAnyValue {
            value: Arc::new(Mutex::new(arc_value)),
        });
        let array_value = crate::model::common::RArrayValue {
            values: Arc::new(Mutex::new(vec![Arc::new(Mutex::new(
                any_value_arc.clone(),
            ))])),
        };
        let array_value_arc = Arc::new(Mutex::new(Some(RVArrayValue(array_value))));
        let any_value_array_value_wrapper = Some(RAnyValue {
            value: array_value_arc.clone(),
        });

        let any_value_array_value_wrapper_arc = Arc::new(Mutex::new(any_value_array_value_wrapper));

        let key = Arc::new(Mutex::new("key".to_string()));
        let kv = RKeyValue {
            key: key.clone(),
            value: any_value_array_value_wrapper_arc.clone(),
        };

        let kv_arc = Arc::new(Mutex::new(kv));
        let attrs = Arc::new(Mutex::new(vec![kv_arc.clone()]));

        let resource = Resource {
            attributes: attrs.clone(),
            dropped_attributes_count: Arc::new(Mutex::new(0)),
        };

        Python::with_gil(|py| -> PyResult<()> {
            run_script(
                "read_and_write_attributes_array_value_test.py",
                py,
                resource,
            )
        })
        .unwrap();

        let attrs = Arc::into_inner(attrs).unwrap();
        let mut attrs = attrs.into_inner().unwrap();
        let attr = Arc::into_inner(attrs.pop().unwrap()).unwrap();
        let attr = attr.into_inner().unwrap();
        let key = attr.key.lock().unwrap();
        assert_eq!("my_array", *key);
        let value = attr.value.lock().unwrap();
        assert!(value.is_some());
        let v = value.clone().unwrap();
        let v = v.value.lock().unwrap();
        assert!(v.is_some());
        let v = v.clone().unwrap();
        match v {
            RVArrayValue(av) => {
                println!("{:?}", av);
                let mut vals = av.values.lock().unwrap();
                let vv = vals.pop().unwrap();
                let v = vv.lock().unwrap();
                let v = v.clone().unwrap();
                let v = v.value.clone().lock().unwrap().clone().unwrap();
                match v {
                    IntValue(v) => {
                        assert_eq!(v, 123456789)
                    }
                    _ => panic!("wrong value type"),
                }
            }
            _ => panic!("wrong value type"),
        }
    }

    #[test]
    fn read_and_write_attributes_key_value_list_value() {
        initialize();

        let value = Some(StringValue("foo".to_string()));
        let any_value = Some(RAnyValue {
            value: Arc::new(Mutex::new(value)),
        });
        let any_value_arc = Arc::new(Mutex::new(any_value));
        let arc_key = Arc::new(Mutex::new("inner_key".to_string()));

        let kev_value = RKeyValue {
            key: arc_key.clone(),
            value: any_value_arc.clone(),
        };

        let kv_list = crate::model::common::RKeyValueList {
            values: Arc::new(Mutex::new(vec![kev_value])),
        };

        let array_value_arc = Arc::new(Mutex::new(Some(KvListValue(kv_list))));
        let any_value_array_value_wrapper = Some(RAnyValue {
            value: array_value_arc.clone(),
        });

        let any_value_array_value_wrapper_arc = Arc::new(Mutex::new(any_value_array_value_wrapper));

        let key = Arc::new(Mutex::new("key".to_string()));
        let kv = RKeyValue {
            key: key.clone(),
            value: any_value_array_value_wrapper_arc.clone(),
        };

        let kv_arc = Arc::new(Mutex::new(kv));

        let attrs_arc = Arc::new(Mutex::new(vec![kv_arc.clone()]));
        let resource = Resource {
            attributes: attrs_arc.clone(),
            dropped_attributes_count: Arc::new(Mutex::new(0)),
        };

        Python::with_gil(|py| -> PyResult<()> {
            run_script(
                "read_and_write_attributes_key_value_list_test.py",
                py,
                resource,
            )
        })
        .unwrap();

        let mut value = attrs_arc.lock().unwrap();
        let value = value.pop().unwrap();
        let value = Arc::into_inner(value).unwrap().into_inner().unwrap();
        let value = Arc::into_inner(value.value).unwrap().into_inner().unwrap();
        let value = value.unwrap().value;
        let value = Arc::into_inner(value)
            .unwrap()
            .into_inner()
            .unwrap()
            .unwrap();
        match value {
            KvListValue(k) => {
                let mut value = k.values.lock().unwrap().clone();
                let value = value.pop();
                match value {
                    None => {
                        panic!("wrong type")
                    }
                    Some(v) => {
                        let v = v.value.lock().unwrap().clone();
                        match v {
                            None => {
                                panic!("wrong type")
                            }
                            Some(v) => {
                                let value = v.value.lock().unwrap().clone().unwrap();
                                match value {
                                    IntValue(i) => assert_eq!(100, i),
                                    _ => panic!("wrong type"),
                                }
                            }
                        }
                    }
                }
            }
            _ => panic!("wrong type"),
        }
    }

    #[test]
    fn write_resource_attributes_key_value_key() {
        initialize();
        let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
        let any_value_arc = Arc::new(Mutex::new(Some(RAnyValue {
            value: arc_value.clone(),
        })));
        let key = Arc::new(Mutex::new("key".to_string()));

        let kv = RKeyValue {
            key: key.clone(),
            value: any_value_arc.clone(),
        };

        let kv_arc = Arc::new(Mutex::new(kv));

        let resource = Resource {
            attributes: Arc::new(Mutex::new(vec![kv_arc.clone()])),
            dropped_attributes_count: Arc::new(Mutex::new(0)),
        };

        Python::with_gil(|py| -> PyResult<()> {
            run_script(
                "write_resource_attributes_key_value_key_test.py",
                py,
                resource,
            )
        })
        .unwrap();
        let av = key.clone().lock().unwrap().clone();
        assert_eq!(av, "new_key".to_string());
        println!("{:?}", av);
    }

    #[test]
    fn write_resource_attributes_key_value_value() {
        initialize();
        let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
        let any_value_arc = Arc::new(Mutex::new(Some(RAnyValue {
            value: arc_value.clone(),
        })));
        let key = Arc::new(Mutex::new("key".to_string()));

        let kv = RKeyValue {
            key: key.clone(),
            value: any_value_arc.clone(),
        };

        let kv_arc = Arc::new(Mutex::new(kv));

        let resource = Resource {
            attributes: Arc::new(Mutex::new(vec![kv_arc.clone()])),
            dropped_attributes_count: Arc::new(Mutex::new(0)),
        };

        Python::with_gil(|py| -> PyResult<()> {
            run_script(
                "write_resource_attributes_key_value_value_test.py",
                py,
                resource,
            )
        })
        .unwrap();
        match arc_value.lock().unwrap().clone().unwrap() {
            StringValue(s) => {
                assert_eq!(s, "changed");
            }
            _ => panic!("wrong type"),
        }
        println!("{:?}", any_value_arc.lock().unwrap().clone().unwrap());
    }

    #[test]
    fn resource_attributes_append_attribute() {
        initialize();
        let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
        let any_value_arc = Arc::new(Mutex::new(Some(RAnyValue {
            value: arc_value.clone(),
        })));
        let key = Arc::new(Mutex::new("key".to_string()));

        let kv = RKeyValue {
            key: key.clone(),
            value: any_value_arc.clone(),
        };

        let kv_arc = Arc::new(Mutex::new(kv));
        let attrs_arc = Arc::new(Mutex::new(vec![kv_arc.clone()]));
        let resource = Resource {
            attributes: attrs_arc.clone(),
            dropped_attributes_count: Arc::new(Mutex::new(0)),
        };

        Python::with_gil(|py| -> PyResult<()> {
            run_script("resource_attributes_append_attribute.py", py, resource)
        })
        .unwrap();
        println!("{:#?}", attrs_arc.lock().unwrap());
    }

    #[test]
    fn resource_attributes_set_attributes() {
        initialize();
        let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
        let any_value_arc = Arc::new(Mutex::new(Some(RAnyValue {
            value: arc_value.clone(),
        })));
        let key = Arc::new(Mutex::new("key".to_string()));

        let kv = RKeyValue {
            key: key.clone(),
            value: any_value_arc.clone(),
        };

        let kv_arc = Arc::new(Mutex::new(kv));
        let attrs_arc = Arc::new(Mutex::new(vec![kv_arc.clone()]));
        let resource = Resource {
            attributes: attrs_arc.clone(),
            dropped_attributes_count: Arc::new(Mutex::new(0)),
        };

        Python::with_gil(|py| -> PyResult<()> {
            run_script("resource_attributes_set_attributes.py", py, resource)
        })
        .unwrap();
        println!("{:#?}", attrs_arc.lock().unwrap());
        let attrs = attrs_arc.lock().unwrap();
        assert_eq!(2, attrs.len());
        for kv in attrs.iter() {
            let guard = kv.lock();
            let kv_guard = guard.unwrap();
            let key = kv_guard.key.lock().unwrap().to_string();
            let value = kv_guard.value.lock().unwrap();
            assert_ne!(key, "key");
            assert!(key == "double.value" || key == "os.version");
            assert!(value.is_some());
            let av = value.clone().unwrap();
            let value = av.value.lock().unwrap();
            assert!(value.is_some());
        }
    }

    #[test]
    fn resource_spans_append_attributes() {
        initialize();
        let export_req = utilities::otlp::FakeOTLP::trace_service_request_with_spans(1, 1);
        let resource_spans = crate::model::otel_transform::transform_resource_spans(
            export_req.resource_spans[0].clone(),
        );
        let py_resource_spans = ResourceSpans {
            resource: resource_spans.resource.clone(),
            scope_spans: Arc::new(Mutex::new(vec![])),
            schema_url: resource_spans.schema_url,
        };
        Python::with_gil(|py| -> PyResult<()> {
            run_script("resource_spans_append_attribute.py", py, py_resource_spans)
        })
        .unwrap();
        println!("{:#?}", resource_spans.resource.lock().unwrap());
    }

    #[test]
    fn resource_spans_iterate_spans() {
        initialize();
        let export_req = utilities::otlp::FakeOTLP::trace_service_request_with_spans(1, 1);
        let resource_spans = crate::model::otel_transform::transform_resource_spans(
            export_req.resource_spans[0].clone(),
        );
        let py_resource_spans = ResourceSpans {
            resource: resource_spans.resource.clone(),
            scope_spans: resource_spans.scope_spans.clone(),
            schema_url: resource_spans.schema_url,
        };
        Python::with_gil(|py| -> PyResult<()> {
            run_script("resource_spans_iterate_spans.py", py, py_resource_spans)
        })
        .unwrap();
        println!("{:#?}", resource_spans.resource.lock().unwrap());
    }

    #[test]
    fn read_and_write_instrumentation_scope() {
        initialize();
        let export_req = utilities::otlp::FakeOTLP::trace_service_request_with_spans(1, 1);
        let resource_spans = crate::model::otel_transform::transform_resource_spans(
            export_req.resource_spans[0].clone(),
        );
        let py_resource_spans = ResourceSpans {
            resource: resource_spans.resource.clone(),
            scope_spans: resource_spans.scope_spans.clone(),
            schema_url: resource_spans.schema_url,
        };
        Python::with_gil(|py| -> PyResult<()> {
            run_script(
                "read_and_write_instrumentation_scope_test.py",
                py,
                py_resource_spans,
            )
        })
        .unwrap();

        let scope_spans_vec = Arc::into_inner(resource_spans.scope_spans).unwrap();
        let scope_spans_vec = scope_spans_vec.into_inner().unwrap();

        let mut scope_spans = crate::model::py_transform::transform_spans(scope_spans_vec);
        let scope_spans = scope_spans.pop().unwrap();
        let scope = scope_spans.scope.unwrap();
        assert_eq!("name_changed", scope.name);
        assert_eq!("0.0.2", scope.version);
        assert_eq!(100, scope.dropped_attributes_count);
        assert_eq!(scope.attributes.len(), 2);
        for attr in &scope.attributes {
            let value = attr.value.clone().unwrap();
            let value = value.value.unwrap();
            match attr.key.as_str() {
                "key_changed" => match value {
                    opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i) => {
                        assert_eq!(i, 200);
                    }
                    _ => {
                        panic!("wrong type for key_changed: {:?}", value);
                    }
                },
                "severity" => match value {
                    opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s) => {
                        assert_eq!(s, "WARN");
                    }
                    _ => {
                        panic!("wrong type for severity: {:?}", value);
                    }
                },
                _ => {
                    panic!("unexpected key")
                }
            }
        }
    }
    #[test]
    fn set_instrumentation_scope() {
        initialize();
        let export_req = FakeOTLP::trace_service_request_with_spans(1, 1);
        let resource_spans = crate::model::otel_transform::transform_resource_spans(
            export_req.resource_spans[0].clone(),
        );
        let py_resource_spans = ResourceSpans {
            resource: resource_spans.resource.clone(),
            scope_spans: resource_spans.scope_spans.clone(),
            schema_url: resource_spans.schema_url,
        };
        Python::with_gil(|py| -> PyResult<()> {
            run_script("set_instrumentation_scope_test.py", py, py_resource_spans)
        })
        .unwrap();

        let scope_spans_vec = Arc::into_inner(resource_spans.scope_spans).unwrap();
        let scope_spans_vec = scope_spans_vec.into_inner().unwrap();

        let mut scope_spans = crate::model::py_transform::transform_spans(scope_spans_vec);
        let scope_spans = scope_spans.pop().unwrap();
        let scope = scope_spans.scope.unwrap();
        assert_eq!("name_changed", scope.name);
        assert_eq!("0.0.2", scope.version);
        assert_eq!(100, scope.dropped_attributes_count);
        assert_eq!(scope.attributes.len(), 1);
        for attr in &scope.attributes {
            let value = attr.value.clone().unwrap();
            let value = value.value.unwrap();
            match attr.key.as_str() {
                "severity" => match value {
                    Value::StringValue(s) => {
                        assert_eq!(s, "WARN");
                    }
                    _ => {
                        panic!("wrong type for severity: {:?}", value);
                    }
                },
                _ => {
                    panic!("unexpected key")
                }
            }
        }
    }

    #[test]
    fn read_and_write_spans() {
        initialize();
        let export_req = FakeOTLP::trace_service_request_with_spans(1, 1);
        let resource_spans = crate::model::otel_transform::transform_resource_spans(
            export_req.resource_spans[0].clone(),
        );
        let py_resource_spans = ResourceSpans {
            resource: resource_spans.resource.clone(),
            scope_spans: resource_spans.scope_spans.clone(),
            schema_url: resource_spans.schema_url,
        };
        Python::with_gil(|py| -> PyResult<()> {
            run_script("read_and_write_spans_test.py", py, py_resource_spans)
        })
        .unwrap();

        let resource = Arc::into_inner(resource_spans.resource);
        let resource = resource.unwrap().into_inner().unwrap().unwrap();
        let dropped = Arc::into_inner(resource.dropped_attributes_count);
        let dropped = dropped.unwrap().into_inner().unwrap();

        assert_eq!(15, dropped);

        let scope_spans_vec = Arc::into_inner(resource_spans.scope_spans).unwrap();
        let scope_spans_vec = scope_spans_vec.into_inner().unwrap();

        let mut scope_spans = crate::model::py_transform::transform_spans(scope_spans_vec);
        let mut scope_spans = scope_spans.pop().unwrap();
        let mut span = scope_spans.spans.pop().unwrap();
        assert_eq!(b"5555555555".to_vec(), span.trace_id);
        assert_eq!(b"6666666666".to_vec(), span.span_id);
        assert_eq!("test=1234567890", span.trace_state);
        assert_eq!(b"7777777777".to_vec(), span.parent_span_id);
        assert_eq!(1, span.flags);
        assert_eq!("py_processed_span", span.name);
        assert_eq!(4, span.kind);
        assert_eq!(1234567890, span.start_time_unix_nano);
        assert_eq!(1234567890, span.end_time_unix_nano);
        assert_eq!(100, span.dropped_attributes_count);
        assert_eq!(200, span.dropped_events_count);
        assert_eq!(300, span.dropped_links_count);
        assert_eq!("error message", span.status.clone().unwrap().message);
        assert_eq!(2, span.status.unwrap().code);
        assert_eq!(1, span.events.len());
        assert_eq!("py_processed_event", span.events[0].name);
        assert_eq!(1234567890, span.events[0].time_unix_nano);
        assert_eq!(400, span.events[0].dropped_attributes_count);
        assert_eq!(1, span.events[0].attributes.len());
        assert_eq!("event_attr_key", &span.events[0].attributes[0].key);
        let value = span.events[0].attributes[0]
            .value
            .clone()
            .unwrap()
            .value
            .unwrap();
        match value {
            Value::StringValue(s) => {
                assert_eq!("event_attr_value", s)
            }
            _ => panic!("unexpected type"),
        }

        assert_eq!(2, span.links.len());
        // get the newly added link
        let new_link = span.links.remove(1);
        assert_eq!(b"88888888".to_vec(), new_link.trace_id);
        assert_eq!(b"99999999".to_vec(), new_link.span_id);
        assert_eq!("test=1234567890", new_link.trace_state);
        assert_eq!(300, new_link.dropped_attributes_count);
        assert_eq!(1, new_link.flags);
        assert_eq!(1, new_link.attributes.len());
        let value = new_link.attributes[0].value.clone().unwrap().value.unwrap();
        match value {
            Value::StringValue(s) => {
                assert_eq!("link_attr_value", s)
            }
            _ => panic!("unexpected type"),
        }

        assert_eq!(3, span.attributes.len());
        let new_attr = span.attributes.remove(2);
        assert_eq!("span_attr_key2", new_attr.key);
        let value = new_attr.value.clone().unwrap().value.unwrap();
        match value {
            Value::StringValue(s) => {
                assert_eq!("span_attr_value2", s)
            }
            _ => panic!("unexpected type"),
        }
    }
    #[test]
    fn set_scope_spans_span_test() {
        initialize();
        let export_req = FakeOTLP::trace_service_request_with_spans(1, 1);
        let resource_spans = crate::model::otel_transform::transform_resource_spans(
            export_req.resource_spans[0].clone(),
        );
        let py_resource_spans = ResourceSpans {
            resource: resource_spans.resource.clone(),
            scope_spans: resource_spans.scope_spans.clone(),
            schema_url: resource_spans.schema_url,
        };
        Python::with_gil(|py| -> PyResult<()> {
            run_script("set_scope_spans_span_test.py", py, py_resource_spans)
        })
        .unwrap();

        let scope_spans_vec = Arc::into_inner(resource_spans.scope_spans).unwrap();
        let scope_spans_vec = scope_spans_vec.into_inner().unwrap();

        let mut scope_spans = crate::model::py_transform::transform_spans(scope_spans_vec);
        let mut scope_spans = scope_spans.pop().unwrap();
        let mut span = scope_spans.spans.pop().unwrap();
        assert_eq!(b"5555555555".to_vec(), span.trace_id);
        assert_eq!(b"6666666666".to_vec(), span.span_id);
        assert_eq!("test=1234567890", span.trace_state);
        assert_eq!(b"7777777777".to_vec(), span.parent_span_id);
        assert_eq!(1, span.flags);
        assert_eq!("py_processed_span", span.name);
        assert_eq!(4, span.kind);
        assert_eq!(1234567890, span.start_time_unix_nano);
        assert_eq!(1234567890, span.end_time_unix_nano);
        assert_eq!(100, span.dropped_attributes_count);
        assert_eq!(200, span.dropped_events_count);
        assert_eq!(300, span.dropped_links_count);
        assert_eq!("error message", span.status.clone().unwrap().message);
        assert_eq!(2, span.status.unwrap().code);
        assert_eq!(1, span.events.len());
        assert_eq!("py_processed_event", span.events[0].name);
        assert_eq!(1234567890, span.events[0].time_unix_nano);
        assert_eq!(400, span.events[0].dropped_attributes_count);
        assert_eq!(1, span.events[0].attributes.len());
        assert_eq!("event_attr_key", &span.events[0].attributes[0].key);
        let value = span.events[0].attributes[0]
            .value
            .clone()
            .unwrap()
            .value
            .unwrap();
        match value {
            Value::StringValue(s) => {
                assert_eq!("event_attr_value", s)
            }
            _ => panic!("unexpected type"),
        }

        assert_eq!(1, span.links.len());
        // get the newly added link
        let new_link = span.links.remove(0);
        assert_eq!(b"88888888".to_vec(), new_link.trace_id);
        assert_eq!(b"99999999".to_vec(), new_link.span_id);
        assert_eq!("test=1234567890", new_link.trace_state);
        assert_eq!(300, new_link.dropped_attributes_count);
        assert_eq!(1, new_link.flags);
        assert_eq!(1, new_link.attributes.len());
        let value = new_link.attributes[0].value.clone().unwrap().value.unwrap();
        match value {
            Value::StringValue(s) => {
                assert_eq!("link_attr_value", s)
            }
            _ => panic!("unexpected type"),
        }

        assert_eq!(1, span.attributes.len());
        let new_attr = span.attributes.remove(0);
        assert_eq!("span_attr_key", new_attr.key);
        let value = new_attr.value.clone().unwrap().value.unwrap();
        match value {
            Value::StringValue(s) => {
                assert_eq!("span_attr_value", s)
            }
            _ => panic!("unexpected type"),
        }
    }
    #[test]
    fn set_resource_spans_resource() {
        initialize();
        let export_req = FakeOTLP::trace_service_request_with_spans(1, 1);
        let resource_spans = crate::model::otel_transform::transform_resource_spans(
            export_req.resource_spans[0].clone(),
        );
        let py_resource_spans = ResourceSpans {
            resource: resource_spans.resource.clone(),
            scope_spans: resource_spans.scope_spans.clone(),
            schema_url: resource_spans.schema_url,
        };
        Python::with_gil(|py| -> PyResult<()> {
            run_script(
                "write_resource_spans_resource_test.py",
                py,
                py_resource_spans,
            )
        })
        .unwrap();

        let resource = Arc::into_inner(resource_spans.resource).unwrap();
        let resource = resource.into_inner().unwrap().unwrap();
        let resource = crate::model::py_transform::transform_resource(resource).unwrap();
        assert_eq!(2, resource.attributes.len());
        assert_eq!(35, resource.dropped_attributes_count);
        for attr in &resource.attributes {
            match attr.key.as_str() {
                "key" => assert_eq!(
                    Value::StringValue("value".to_string()),
                    attr.value.clone().unwrap().value.unwrap()
                ),
                "boolean" => assert_eq!(
                    Value::BoolValue(true),
                    attr.value.clone().unwrap().value.unwrap()
                ),
                _ => panic!("unexpected attribute key"),
            }
        }
    }
    #[test]
    fn set_span_events() {
        initialize();
        let export_req = FakeOTLP::trace_service_request_with_spans(1, 1);
        let resource_spans = crate::model::otel_transform::transform_resource_spans(
            export_req.resource_spans[0].clone(),
        );
        let py_resource_spans = ResourceSpans {
            resource: resource_spans.resource.clone(),
            scope_spans: resource_spans.scope_spans.clone(),
            schema_url: resource_spans.schema_url,
        };
        Python::with_gil(|py| -> PyResult<()> {
            run_script("write_span_events_test.py", py, py_resource_spans)
        })
        .unwrap();

        let scope_spans_vec = Arc::into_inner(resource_spans.scope_spans).unwrap();
        let scope_spans_vec = scope_spans_vec.into_inner().unwrap();

        let mut scope_spans = crate::model::py_transform::transform_spans(scope_spans_vec);
        let mut scope_spans = scope_spans.pop().unwrap();
        let span = scope_spans.spans.pop().unwrap();

        assert_eq!(2, span.events.len());
        let event = &span.events[0];
        assert_eq!("first_event", event.name);
        assert_eq!(123, event.time_unix_nano);
        assert_eq!(1, event.dropped_attributes_count);
        assert_eq!(1, event.attributes.len());
        let attr = &event.attributes[0];
        assert_eq!("first_event_attr_key", attr.key);
        assert_eq!(
            Value::StringValue("first_event_attr_value".to_string()),
            attr.value.clone().unwrap().value.unwrap()
        );

        let event = &span.events[1];
        assert_eq!("second_event", event.name);
        assert_eq!(456, event.time_unix_nano);
        assert_eq!(2, event.dropped_attributes_count);
        assert_eq!(1, event.attributes.len());
        let attr = &event.attributes[0];
        assert_eq!("second_event_attr_key", attr.key);
        assert_eq!(
            Value::StringValue("second_event_attr_value".to_string()),
            attr.value.clone().unwrap().value.unwrap()
        )
    }
    #[test]
    fn set_scope_spans() {
        initialize();
        let export_req = FakeOTLP::trace_service_request_with_spans(1, 1);
        let resource_spans = crate::model::otel_transform::transform_resource_spans(
            export_req.resource_spans[0].clone(),
        );
        let py_resource_spans = ResourceSpans {
            resource: resource_spans.resource.clone(),
            scope_spans: resource_spans.scope_spans.clone(),
            schema_url: resource_spans.schema_url,
        };
        Python::with_gil(|py| -> PyResult<()> {
            run_script("write_scope_spans_test.py", py, py_resource_spans)
        })
        .unwrap();

        let scope_spans_vec = Arc::into_inner(resource_spans.scope_spans).unwrap();
        let scope_spans_vec = scope_spans_vec.into_inner().unwrap();

        let mut scope_spans = crate::model::py_transform::transform_spans(scope_spans_vec);
        let mut scope_spans = scope_spans.pop().unwrap();
        assert_eq!(
            "https://github.com/streamfold/rotel",
            scope_spans.schema_url
        );
        let inst_scope = scope_spans.scope.unwrap();
        assert_eq!("rotel-sdk-new", inst_scope.name);
        assert_eq!("v1.0.1", inst_scope.version);
        let attr = &inst_scope.attributes[0];
        assert_eq!("rotel-sdk", attr.key);
        assert_eq!(
            Value::StringValue("v1.0.0".to_string()),
            attr.value.clone().unwrap().value.unwrap()
        );

        let span = scope_spans.spans.pop().unwrap();
        assert_eq!(b"5555555555".to_vec(), span.trace_id);
        assert_eq!(b"6666666666".to_vec(), span.span_id);
        assert_eq!("test=1234567890", span.trace_state);
        assert_eq!(b"7777777777".to_vec(), span.parent_span_id);
        assert_eq!(1, span.flags);
        assert_eq!("py_processed_span", span.name);
        assert_eq!(4, span.kind);
        assert_eq!(1234567890, span.start_time_unix_nano);
        assert_eq!(1234567890, span.end_time_unix_nano);
        let attr = &span.attributes[0];
        assert_eq!("span_attr_key", attr.key);
        assert_eq!(
            Value::StringValue("span_attr_value".to_string()),
            attr.value.clone().unwrap().value.unwrap()
        );
    }

    #[test]
    fn set_spans() {
        initialize();
        let export_req = FakeOTLP::trace_service_request_with_spans(1, 1);
        let resource_spans = crate::model::otel_transform::transform_resource_spans(
            export_req.resource_spans[0].clone(),
        );
        let py_resource_spans = ResourceSpans {
            resource: resource_spans.resource.clone(),
            scope_spans: resource_spans.scope_spans.clone(),
            schema_url: resource_spans.schema_url,
        };
        Python::with_gil(|py| -> PyResult<()> {
            run_script("write_spans_test.py", py, py_resource_spans)
        })
        .unwrap();

        let scope_spans_vec = Arc::into_inner(resource_spans.scope_spans).unwrap();
        let scope_spans_vec = scope_spans_vec.into_inner().unwrap();

        let mut scope_spans = crate::model::py_transform::transform_spans(scope_spans_vec);
        let mut scope_spans = scope_spans.pop().unwrap();
        let span = scope_spans.spans.pop().unwrap();
        assert_eq!(b"5555555555".to_vec(), span.trace_id);
        assert_eq!(b"6666666666".to_vec(), span.span_id);
        assert_eq!("test=1234567890", span.trace_state);
        assert_eq!(b"7777777777".to_vec(), span.parent_span_id);
        assert_eq!(1, span.flags);
        assert_eq!("py_processed_span", span.name);
        assert_eq!(4, span.kind);
        assert_eq!(1234567890, span.start_time_unix_nano);
        assert_eq!(1234567890, span.end_time_unix_nano);
        let attr = &span.attributes[0];
        assert_eq!("span_attr_key", attr.key);
        assert_eq!(
            Value::StringValue("span_attr_value".to_string()),
            attr.value.clone().unwrap().value.unwrap()
        );
    }

    #[test]
    fn read_and_write_log_record() {
        initialize();

        // Create a mock ResourceLogs protobuf object for testing
        // In a real scenario, you might use a utility like FakeOTLP if available for logs.
        let initial_log_record = opentelemetry_proto::tonic::logs::v1::LogRecord {
            time_unix_nano: 1000000000,
            observed_time_unix_nano: 1000000001,
            severity_number: 9, // INFO
            severity_text: "INFO".to_string(),
            body: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                value: Some(Value::StringValue("initial log message".to_string())),
            }),
            attributes: vec![
                opentelemetry_proto::tonic::common::v1::KeyValue {
                    key: "log.source".to_string(),
                    value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                        value: Some(Value::StringValue("my_app".to_string())),
                    }),
                },
                opentelemetry_proto::tonic::common::v1::KeyValue {
                    key: "component".to_string(),
                    value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                        value: Some(Value::StringValue("backend".to_string())),
                    }),
                },
            ],
            dropped_attributes_count: 0,
            flags: 0,
            trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            span_id: vec![17, 18, 19, 20, 21, 22, 23, 24],
            event_name: "".to_string(),
        };

        let initial_scope_logs = opentelemetry_proto::tonic::logs::v1::ScopeLogs {
            scope: Some(
                opentelemetry_proto::tonic::common::v1::InstrumentationScope {
                    name: "test-instrumentation-scope".to_string(),
                    version: "1.0.0".to_string(),
                    attributes: vec![],
                    dropped_attributes_count: 0,
                },
            ),
            log_records: vec![initial_log_record],
            schema_url: "http://example.com/logs-schema".to_string(),
        };

        let export_req = opentelemetry_proto::tonic::logs::v1::ResourceLogs {
            resource: Some(opentelemetry_proto::tonic::resource::v1::Resource {
                attributes: vec![],
                dropped_attributes_count: 0,
            }),
            scope_logs: vec![initial_scope_logs],
            schema_url: "http://example.com/resource-logs-schema".to_string(),
        };

        // Transform the protobuf ResourceLogs into our internal RResourceLogs
        let r_resource_logs =
            crate::model::otel_transform::transform_resource_logs(export_req.clone());

        // Create the Python-exposed ResourceLogs object
        let py_resource_logs = ResourceLogs {
            resource: r_resource_logs.resource.clone(),
            scope_logs: r_resource_logs.scope_logs.clone(),
            schema_url: r_resource_logs.schema_url.clone(),
        };

        // Execute the Python script
        Python::with_gil(|py| -> PyResult<()> {
            run_script("read_and_write_logs_test.py", py, py_resource_logs)
        })
        .unwrap();

        let scope_logs_vec = Arc::into_inner(r_resource_logs.scope_logs)
            .unwrap()
            .into_inner()
            .unwrap();
        let mut transformed_scope_logs = crate::model::py_transform::transform_logs(scope_logs_vec);

        // Assert the changes made by the Python script
        assert_eq!(transformed_scope_logs.len(), 1);
        let mut scope_log = transformed_scope_logs.remove(0);
        assert_eq!(scope_log.log_records.len(), 1);
        let log_record = scope_log.log_records.remove(0);

        assert_eq!(log_record.time_unix_nano, 2000000000);
        assert_eq!(log_record.observed_time_unix_nano, 2000000001);
        assert_eq!(log_record.severity_number, 13);
        assert_eq!(log_record.severity_text, "ERROR");

        let body_value = log_record.body.unwrap().value.unwrap();
        match body_value {
            Value::StringValue(s) => {
                assert_eq!(s, "processed log message");
            }
            _ => panic!("Body value is not a string"),
        }

        assert_eq!(log_record.dropped_attributes_count, 5);
        assert_eq!(log_record.flags, 1);
        assert_eq!(log_record.trace_id, b"abcdefghijklmnop".to_vec());
        assert_eq!(log_record.span_id, b"qrstuvwx".to_vec());

        assert_eq!(log_record.attributes.len(), 3);
        // Verify the new attribute
        let new_attr = &log_record.attributes[2];
        assert_eq!(new_attr.key, "new_log_attr");
        assert_eq!(
            new_attr.value.clone().unwrap().value.unwrap(),
            Value::StringValue("new_log_value".to_string())
        );

        // Verify the modified attribute
        let modified_attr = &log_record.attributes[1];
        assert_eq!(modified_attr.key, "component");
        assert_eq!(
            modified_attr.value.clone().unwrap().value.unwrap(),
            Value::StringValue("modified_component_value".to_string())
        );
    }

    #[test]
    fn add_new_log_record() {
        initialize();

        // Create a mock ResourceLogs protobuf object with an empty ScopeLogs initially
        let initial_scope_logs = opentelemetry_proto::tonic::logs::v1::ScopeLogs {
            scope: Some(
                opentelemetry_proto::tonic::common::v1::InstrumentationScope {
                    name: "test-instrumentation-scope".to_string(),
                    version: "1.0.0".to_string(),
                    attributes: vec![],
                    dropped_attributes_count: 0,
                },
            ),
            log_records: vec![], // Start with no log records
            schema_url: "http://example.com/logs-schema".to_string(),
        };

        let export_req = opentelemetry_proto::tonic::logs::v1::ResourceLogs {
            resource: Some(opentelemetry_proto::tonic::resource::v1::Resource {
                attributes: vec![],
                dropped_attributes_count: 0,
            }),
            scope_logs: vec![initial_scope_logs],
            schema_url: "http://example.com/resource-logs-schema".to_string(),
        };

        // Transform the protobuf ResourceLogs into our internal RResourceLogs
        let r_resource_logs =
            crate::model::otel_transform::transform_resource_logs(export_req.clone());

        // Create the Python-exposed ResourceLogs object
        let py_resource_logs = ResourceLogs {
            resource: r_resource_logs.resource.clone(),
            scope_logs: r_resource_logs.scope_logs.clone(),
            schema_url: r_resource_logs.schema_url.clone(),
        };

        // Execute the Python script that adds a new log record
        Python::with_gil(|py| -> PyResult<()> {
            run_script("add_log_record_test.py", py, py_resource_logs)
        })
        .unwrap();

        // Transform the modified RResourceLogs back into protobuf format
        let mut resource = r_resource_logs.resource.lock().unwrap();
        let _resource_proto = resource
            .take()
            .map(|r| crate::model::py_transform::transform_resource(r).unwrap());

        let scope_logs_vec = Arc::into_inner(r_resource_logs.scope_logs)
            .unwrap()
            .into_inner()
            .unwrap();
        let mut transformed_scope_logs = crate::model::py_transform::transform_logs(scope_logs_vec);

        // Assert the changes made by the Python script
        assert_eq!(transformed_scope_logs.len(), 1);
        let mut scope_log = transformed_scope_logs.remove(0);
        assert_eq!(scope_log.log_records.len(), 1); // Expecting one log record now
        let log_record = scope_log.log_records.remove(0);

        assert_eq!(log_record.time_unix_nano, 9876543210);
        assert_eq!(log_record.observed_time_unix_nano, 9876543211);
        assert_eq!(log_record.severity_number, 17); // FATAL
        assert_eq!(log_record.severity_text, "FATAL");

        let body_value = log_record.body.unwrap().value.unwrap();
        match body_value {
            Value::StringValue(s) => {
                assert_eq!(s, "This is a newly added log message.");
            }
            _ => panic!("Body value is not a string"),
        }

        assert_eq!(log_record.attributes.len(), 1);
        let new_attr = &log_record.attributes[0];
        assert_eq!(new_attr.key, "new_log_key");
        assert_eq!(
            new_attr.value.clone().unwrap().value.unwrap(),
            Value::StringValue("new_log_value".to_string())
        );

        assert_eq!(log_record.dropped_attributes_count, 2);
        assert_eq!(log_record.flags, 4);
        assert_eq!(log_record.trace_id, b"fedcba9876543210".to_vec());
        assert_eq!(log_record.span_id, b"fedcba98".to_vec());
    }

    #[test]
    fn remove_log_record_test() {
        initialize();

        // Create a mock ResourceLogs protobuf object with two initial LogRecords
        let first_log_record = opentelemetry_proto::tonic::logs::v1::LogRecord {
            time_unix_nano: 1000000000,
            observed_time_unix_nano: 1000000001,
            severity_number: 9, // INFO
            severity_text: "INFO".to_string(),
            body: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                value: Some(
                    opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                        "first log message".to_string(),
                    ),
                ),
            }),
            attributes: vec![],
            dropped_attributes_count: 0,
            flags: 0,
            trace_id: vec![],
            span_id: vec![],
            event_name: "first_event".to_string(),
        };

        let second_log_record = opentelemetry_proto::tonic::logs::v1::LogRecord {
            time_unix_nano: 2000000000,
            observed_time_unix_nano: 2000000001,
            severity_number: 13, // ERROR
            severity_text: "ERROR".to_string(),
            body: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                value: Some(Value::StringValue("second log message".to_string())),
            }),
            attributes: vec![],
            dropped_attributes_count: 0,
            flags: 0,
            trace_id: vec![],
            span_id: vec![],
            event_name: "second_event".to_string(),
        };

        let initial_scope_logs = opentelemetry_proto::tonic::logs::v1::ScopeLogs {
            scope: Some(
                opentelemetry_proto::tonic::common::v1::InstrumentationScope {
                    name: "test-instrumentation-scope".to_string(),
                    version: "1.0.0".to_string(),
                    attributes: vec![],
                    dropped_attributes_count: 0,
                },
            ),
            log_records: vec![first_log_record, second_log_record], // Two log records
            schema_url: "http://example.com/logs-schema".to_string(),
        };

        let export_req = opentelemetry_proto::tonic::logs::v1::ResourceLogs {
            resource: Some(opentelemetry_proto::tonic::resource::v1::Resource {
                attributes: vec![],
                dropped_attributes_count: 0,
            }),
            scope_logs: vec![initial_scope_logs],
            schema_url: "http://example.com/resource-logs-schema".to_string(),
        };

        // Transform the protobuf ResourceLogs into our internal RResourceLogs
        let r_resource_logs =
            crate::model::otel_transform::transform_resource_logs(export_req.clone());

        // Create the Python-exposed ResourceLogs object
        let py_resource_logs = ResourceLogs {
            resource: r_resource_logs.resource.clone(),
            scope_logs: r_resource_logs.scope_logs.clone(),
            schema_url: r_resource_logs.schema_url.clone(),
        };

        // Execute the Python script that removes a log record
        Python::with_gil(|py| -> PyResult<()> {
            run_script("remove_log_record_test.py", py, py_resource_logs)
        })
        .unwrap();

        // Transform the modified RResourceLogs back into protobuf format
        let mut resource = r_resource_logs.resource.lock().unwrap();
        let _resource_proto = resource
            .take()
            .map(|r| crate::model::py_transform::transform_resource(r).unwrap());

        let scope_logs_vec = Arc::into_inner(r_resource_logs.scope_logs)
            .unwrap()
            .into_inner()
            .unwrap();
        let mut transformed_scope_logs = crate::model::py_transform::transform_logs(scope_logs_vec);

        // Assert the changes made by the Python script
        assert_eq!(transformed_scope_logs.len(), 1);
        let mut scope_log = transformed_scope_logs.remove(0);
        assert_eq!(scope_log.log_records.len(), 1); // Expecting one log record now
        let log_record = scope_log.log_records.remove(0);

        // Verify that the correct log record remains
        let body_value = log_record.body.unwrap().value.unwrap();
        match body_value {
            Value::StringValue(s) => {
                assert_eq!(s, "first log message");
            }
            _ => panic!("Body value is not the expected string"),
        }
    }

    #[test]
    fn traces_delitem_test() {
        initialize();
        let av = opentelemetry_proto::tonic::common::v1::ArrayValue {
            values: vec![
                opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(Value::StringValue("first value".to_string())),
                },
                opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(Value::StringValue("second value".to_string())),
                },
            ],
        };
        let kvlist = opentelemetry_proto::tonic::common::v1::KeyValueList {
            values: vec![
                opentelemetry_proto::tonic::common::v1::KeyValue {
                    key: "first_key".to_string(),
                    value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                        value: Some(Value::BoolValue(true)),
                    }),
                },
                opentelemetry_proto::tonic::common::v1::KeyValue {
                    key: "second_key".to_string(),
                    value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                        value: Some(Value::StringValue("second_value".to_string())),
                    }),
                },
            ],
        };
        let resource = opentelemetry_proto::tonic::resource::v1::Resource {
            attributes: vec![
                opentelemetry_proto::tonic::common::v1::KeyValue {
                    key: "first_attr".to_string(),
                    value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                        value: Some(Value::KvlistValue(kvlist)),
                    }),
                },
                opentelemetry_proto::tonic::common::v1::KeyValue {
                    key: "second_attr".to_string(),
                    value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                        value: Some(Value::ArrayValue(av)),
                    }),
                },
                opentelemetry_proto::tonic::common::v1::KeyValue {
                    key: "third_attr".to_string(),
                    value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                        value: Some(Value::StringValue("to_remove_value".to_string())),
                    }),
                },
            ],
            dropped_attributes_count: 0,
        };

        let mut spans = FakeOTLP::trace_spans(2);
        let now_ns = Utc::now().timestamp_nanos_opt().unwrap();

        // Adding additional data here to test __delitem__
        spans[0].events.push(v1::span::Event {
            time_unix_nano: now_ns as u64,
            name: "second test event".to_string(),
            attributes: vec![],
            dropped_attributes_count: 0,
        });

        spans[0].links.push(v1::span::Link {
            trace_id: vec![5, 5, 5, 5, 5, 5, 5, 5],
            span_id: vec![6, 6, 6, 6, 6, 6, 6, 6],
            trace_state: "secondtrace=00f067aa0ba902b7".to_string(),
            attributes: vec![],
            dropped_attributes_count: 0,
            flags: 0,
        });

        let first_scope_spans = v1::ScopeSpans {
            scope: None,
            spans,
            schema_url: "".to_string(),
        };

        let second_scope_spans = v1::ScopeSpans {
            scope: None,
            spans: vec![],
            schema_url: "".to_string(),
        };

        let resource_spans = v1::ResourceSpans {
            resource: Some(resource),
            scope_spans: vec![first_scope_spans, second_scope_spans],
            schema_url: "".to_string(),
        };

        // Transform the protobuf ResourceLogs into our internal RResourceLogs
        let r_resource_spans =
            crate::model::otel_transform::transform_resource_spans(resource_spans);

        // Create the Python-exposed ResourceLogs object
        let py_resource_spans = ResourceSpans {
            resource: r_resource_spans.resource.clone(),
            scope_spans: r_resource_spans.scope_spans.clone(),
            schema_url: r_resource_spans.schema_url.clone(),
        };

        // Execute the Python script that removes a log record
        Python::with_gil(|py| -> PyResult<()> {
            run_script("traces_delitem_test.py", py, py_resource_spans)
        })
        .unwrap();

        let mut resource = r_resource_spans.resource.lock().unwrap();
        let mut resource_p = resource
            .take()
            .map(|r| crate::model::py_transform::transform_resource(r).unwrap())
            .unwrap();

        assert_eq!(2, resource_p.attributes.len());
        // Assert the kvlist only has one item now
        let kvlist = resource_p
            .attributes
            .remove(0)
            .value
            .unwrap()
            .value
            .unwrap();
        match kvlist {
            Value::KvlistValue(mut kvl) => {
                assert_eq!(1, kvl.values.len());
                let value = kvl.values.remove(0);
                assert_eq!(value.key, "second_key");
                let value = value.value.clone().unwrap().value.unwrap();
                match value {
                    Value::StringValue(s) => {
                        assert_eq!(s, "second_value");
                    }
                    _ => {
                        panic!("expected StringValue")
                    }
                }
            }
            _ => {
                panic!("expected kvlist")
            }
        }

        let arvalue = resource_p
            .attributes
            .remove(0)
            .value
            .unwrap()
            .value
            .unwrap();
        match arvalue {
            Value::ArrayValue(mut av) => {
                assert_eq!(1, av.values.len());
                let value = av.values.remove(0).value.unwrap();
                match value {
                    Value::StringValue(s) => {
                        assert_eq!(s, "first value")
                    }
                    _ => {
                        panic!("expected StringValue");
                    }
                }
            }
            _ => {
                panic!("expected ArrayValue");
            }
        }

        // Verify the second scope span has been removed
        let scope_spans_vec = Arc::into_inner(r_resource_spans.scope_spans)
            .unwrap()
            .into_inner()
            .unwrap();
        let mut scope_spans_vec = crate::model::py_transform::transform_spans(scope_spans_vec);
        assert_eq!(1, scope_spans_vec.len());
        let scope_spans = scope_spans_vec.remove(0);
        let mut spans = scope_spans.spans;
        assert_eq!(1, spans.len());
        let span = spans.remove(0);
        assert_eq!(1, span.events.len());
        assert_eq!(span.events[0].name, "second test event");
        assert_eq!(1, span.links.len());
        assert_eq!(span.links[0].trace_state, "secondtrace=00f067aa0ba902b7")
    }

    #[test]
    fn attributes_processor_test() {
        initialize();
        let mut log_request = FakeOTLP::logs_service_request();

        let attrs = vec![
            opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "http.status_code".to_string(),
                value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(Value::IntValue(200)),
                }),
            },
            opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "user.id".to_string(),
                value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(Value::StringValue("abc-123".to_string())),
                }),
            },
            opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "user.email".to_string(),
                value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(Value::StringValue("test@example.com".to_string())),
                }),
            },
            opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "request.id".to_string(),
                value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(Value::StringValue("req-456".to_string())),
                }),
            },
            opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "message".to_string(),
                value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(Value::StringValue("User login successful.".to_string())),
                }),
            },
            opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "raw_data".to_string(),
                value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(Value::StringValue("id:123,name:Alice,age:30".to_string())),
                }),
            },
            opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "temp_str_int".to_string(),
                value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(Value::StringValue("123".to_string())),
                }),
            },
            opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "temp_str_bool".to_string(),
                value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(Value::StringValue("true".to_string())),
                }),
            },
            opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "temp_str_float".to_string(),
                value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(Value::DoubleValue(10.0)),
                }),
            },
            opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "path".to_string(),
                value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(Value::StringValue("https://rotel.dev/blog".to_string())),
                }),
            },
            opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "super.secret".to_string(),
                value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(Value::StringValue("don't tell anyone".to_string())),
                }),
            },
            opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "trace.id".to_string(),
                value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(Value::StringValue("12345678".to_string())),
                }),
            },
        ];
        log_request.resource_logs[0].scope_logs[0].log_records[0].attributes = attrs.clone();

        // Transform the protobuf ResourceLogs into our internal RResourceLogs
        let r_resource_logs = crate::model::otel_transform::transform_resource_logs(
            log_request.resource_logs[0].clone(),
        );

        // Create the Python-exposed ResourceLogs object
        let py_resource_logs = ResourceLogs {
            resource: r_resource_logs.resource.clone(),
            scope_logs: r_resource_logs.scope_logs.clone(),
            schema_url: r_resource_logs.schema_url.clone(),
        };

        // Execute the Python script that removes a log record
        Python::with_gil(|py| -> PyResult<()> {
            _run_script(
                "attributes_processor_test.py",
                py,
                py_resource_logs,
                Some("process_logs".to_string()),
            )
        })
        .unwrap();

        let scope_logs_vec = Arc::into_inner(r_resource_logs.scope_logs)
            .unwrap()
            .into_inner()
            .unwrap();
        let mut scope_logs = crate::model::py_transform::transform_logs(scope_logs_vec);

        let log_record = scope_logs.pop().unwrap().log_records.pop().unwrap();
        let attrs_to_verify: HashMap<
            String,
            Option<opentelemetry_proto::tonic::common::v1::AnyValue>,
        > = log_record
            .attributes
            .into_iter()
            .map(|kv| (kv.key, kv.value))
            .collect();

        let verify_attrs = |mut attrs: HashMap<
            String,
            Option<opentelemetry_proto::tonic::common::v1::AnyValue>,
        >| {
            let host_name = attrs.remove("host.name").unwrap();
            match host_name.unwrap().value.unwrap() {
                Value::StringValue(h) => {
                    assert_eq!(h, "my-server-1");
                }
                _ => panic!("Unexpected value type"),
            }
            let http_status_code = attrs.remove("http.status_code").unwrap();
            match http_status_code.unwrap().value.unwrap() {
                Value::StringValue(c) => {
                    assert_eq!(c, "OK");
                }
                _ => panic!("Unexpected value type"),
            }
            let env = attrs.remove("env").unwrap();
            match env.unwrap().value.unwrap() {
                Value::StringValue(e) => {
                    assert_eq!(e, "production");
                }
                _ => panic!("Unexpected value type"),
            }
            let email = attrs.remove("email").unwrap();
            match email.unwrap().value.unwrap() {
                Value::StringValue(h) => {
                    assert_eq!(h, "test@example.com");
                }
                _ => panic!("Unexpected value type"),
            }
            let user_id = attrs.remove("user.id").unwrap();
            match user_id.unwrap().value.unwrap() {
                Value::StringValue(s) => {
                    assert_eq!(
                        s,
                        "5942d94f524882e0f29bf0a1e5a6dcc952eea1c0c21dd3588a3fc7db9716db0c"
                    );
                }
                _ => panic!("Unexpected value type"),
            }
            let trace_id = attrs.remove("trace.id").unwrap();
            match trace_id.unwrap().value.unwrap() {
                Value::StringValue(s) => {
                    assert_eq!(
                        s,
                        "ef797c8118f02dfb649607dd5d3f8c7623048c9c063d532cc95c5ed7a898a64f"
                    );
                }
                _ => panic!("Unexpected value type"),
            }
            let id = attrs.remove("extracted_id").unwrap();
            match id.unwrap().value.unwrap() {
                Value::StringValue(s) => {
                    assert_eq!(s, "123");
                }
                _ => panic!("Unexpected value type"),
            }
            let temp_str_int = attrs.remove("temp_str_int").unwrap();
            match temp_str_int.unwrap().value.unwrap() {
                Value::IntValue(i) => {
                    assert_eq!(i, 123);
                }
                _ => panic!("Unexpected value type"),
            }
            let temp_str_bool = attrs.remove("temp_str_bool").unwrap();
            match temp_str_bool.unwrap().value.unwrap() {
                Value::BoolValue(b) => {
                    assert_eq!(b, true);
                }
                _ => panic!("Unexpected value type"),
            }
            let temp_str_float = attrs.remove("temp_str_float").unwrap();
            match temp_str_float.unwrap().value.unwrap() {
                Value::DoubleValue(d) => {
                    assert_eq!(d, 10.0);
                }
                _ => panic!("Unexpected value type"),
            }
            let path = attrs.remove("path");
            assert_eq!(path, None);
            let super_secret = attrs.remove("super.secret");
            assert_eq!(super_secret, None);
        };

        verify_attrs(attrs_to_verify);

        let mut trace_request = FakeOTLP::trace_service_request();
        trace_request.resource_spans[0].scope_spans[0].spans[0].attributes = attrs.clone();

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
                "attributes_processor_test.py",
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
        let attrs_to_verify: HashMap<
            String,
            Option<opentelemetry_proto::tonic::common::v1::AnyValue>,
        > = span
            .attributes
            .into_iter()
            .map(|kv| (kv.key, kv.value))
            .collect();

        verify_attrs(attrs_to_verify);
    }
    #[test]
    fn redaction_processor_restrictive_test() {
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

        let span_attrs = vec![
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
        trace_request.resource_spans[0].scope_spans[0].spans[0].attributes = span_attrs.clone();

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
                "redaction_processor_restrictive_test.py",
                py,
                py_resource_spans,
                Some("process_spans".to_string()),
            )
        })
        .unwrap();

        let resource = Arc::into_inner(r_resource_spans.resource)
            .unwrap()
            .into_inner()
            .unwrap()
            .unwrap();
        let resource = crate::model::py_transform::transform_resource(resource).unwrap();
        assert_eq!(9, resource.attributes.len());
        let kv_map: HashMap<String, Option<opentelemetry_proto::tonic::common::v1::AnyValue>> =
            resource
                .attributes
                .into_iter()
                .map(|kv| (kv.key.clone(), kv.value.clone()))
                .collect();

        let expected_attributes = HashMap::from([
            ("host.arch", "amd64"),
            ("os.type", "linux"),
            ("region", "us-east-1"),
            ("service.name", "my-service"),
            ("deployment.environment", "prod"),
            (
                "redaction.resource.redacted_keys.names",
                "unallowed_resource_key",
            ),
            (
                "redaction.resource.allowed_keys.names",
                "deployment.environment,host.arch,os.type,region,service.name",
            ),
            ("redaction.resource.redacted_keys.count", "1"),
            ("redaction.resource.allowed_keys.count", "5"),
        ]);

        let validate =
            |kv_map: HashMap<String, Option<opentelemetry_proto::tonic::common::v1::AnyValue>>,
             expected_attributes: HashMap<&str, &str>| {
                for (key, value) in kv_map {
                    let ev = expected_attributes.get(key.as_str());
                    match ev {
                        None => {
                            panic!("key {:?} not found in expected attributes", key)
                        }
                        Some(v) => match value.unwrap().value.unwrap() {
                            Value::StringValue(sv) => {
                                assert_eq!(v.to_string(), sv.to_string());
                            }
                            Value::IntValue(i) => {
                                let v: i64 = v.parse().expect("can't convert to int");
                                assert_eq!(v, i);
                            }
                            _ => {
                                panic!("expected string value")
                            }
                        },
                    }
                }
            };

        validate(kv_map, expected_attributes);

        let scope_spans_vec = Arc::into_inner(r_resource_spans.scope_spans)
            .unwrap()
            .into_inner()
            .unwrap();
        let mut scope_spans = crate::model::py_transform::transform_spans(scope_spans_vec);

        let span = scope_spans.pop().unwrap().spans.pop().unwrap();
        println!("span is {:?}", span);
        assert_eq!(7, span.attributes.len());

        let kv_map: HashMap<String, Option<opentelemetry_proto::tonic::common::v1::AnyValue>> =
            span.attributes
                .into_iter()
                .map(|kv| (kv.key.clone(), kv.value.clone()))
                .collect();

        let expected_attributes = HashMap::from([
            ("operation", "get_data"),
            ("safe_attribute", "this should remain"),
            ("redaction.span.redacted_keys.count", "7"),
            ("redaction.span.allowed_keys.names", "operation"),
            ("redaction.span.allowed_keys.count", "1"),
            ("redaction.span.redacted_keys.names", "api_key_header,db.statement,http.url,my_company_email,some_token,unallowed_span_key,user.email"),
            ("redaction.span.ignored_keys.count", "1"),
        ]);

        validate(kv_map, expected_attributes)
    }

    #[test]
    fn redaction_processor_blocking_test() {
        initialize();
        let span_attrs = vec![
            opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "session_token".to_string(),
                value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(Value::StringValue("foo".to_string())),
                }),
            },
            opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "api_key".to_string(),
                value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(Value::StringValue("bar".to_string())),
                }),
            },
            opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "password_hash".to_string(),
                value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(Value::StringValue("xyz123".to_string())),
                }),
            },
            opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "visa".to_string(),
                value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(Value::StringValue("4123456789012".to_string())),
                }),
            },
            opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "mastercard".to_string(),
                value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(Value::StringValue("5234567890123456".to_string())),
                }),
            },
            opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "bl_email".to_string(),
                value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(Value::StringValue("foo@bar.com".to_string())),
                }),
            },
            opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "allowed_email".to_string(),
                value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(Value::StringValue("test@mycompany.com".to_string())),
                }),
            },
            opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "ip".to_string(),
                value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(Value::StringValue("10.0.1.100".to_string())),
                }),
            },
            opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "query".to_string(),
                value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(Value::StringValue("SELECT * FROM USERS".to_string())),
                }),
            },
        ];

        let mut trace_request = FakeOTLP::trace_service_request();
        trace_request.resource_spans[0].scope_spans[0].spans[0].attributes = span_attrs.clone();

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
                "redaction_processor_blocking_test.py",
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
        assert_eq!(13, span.attributes.len());

        let kv_map: HashMap<String, Option<opentelemetry_proto::tonic::common::v1::AnyValue>> =
            span.attributes
                .into_iter()
                .map(|kv| (kv.key.clone(), kv.value.clone()))
                .collect();

        let expected_attributes = HashMap::from([
            ("visa", "8d9e470bdd9f6e6e18023938497857dc"),
            ("mastercard", "aad9ce9b62a1f697745fcd81314b877f"),
            ("ip", "f5c45dc3c8fb04f638ad27a711910390"),
            ("query", "06c445f7ade97a964f7c466575f8b508"),
            ("bl_email", "f3ada405ce890b6f8204094deb12d8a8"),
            ("session_token", "acbd18db4cc2f85cedef654fccc4a4d8"),
            ("allowed_email", "test@mycompany.com"),
            ("api_key", "37b51d194a7513e45b56f6524f2d51f2"),
            ("password_hash", "613d3b9c91e9445abaeca02f2342e5a6"),
            ("redaction.span.allowed_keys.names", "allowed_email,api_key,bl_email,ip,mastercard,password_hash,query,session_token,visa"),
            ("redaction.span.allowed_keys.count", "9"),
            ("redaction.span.masked_keys.names", "api_key,bl_email,ip,mastercard,password_hash,query,session_token,visa"),
            ("redaction.span.masked_keys.count", "8"),
        ]);

        for (key, value) in kv_map {
            let ev = expected_attributes.get(key.as_str());
            match ev {
                None => {
                    panic!("key {:?} not found in expected attributes", key)
                }
                Some(v) => match value.unwrap().value.unwrap() {
                    Value::StringValue(sv) => {
                        assert_eq!(v.to_string(), sv.to_string());
                    }
                    Value::IntValue(i) => {
                        let v: i64 = v.parse().expect("can't convert to int");
                        assert_eq!(v, i);
                    }
                    _ => {
                        panic!("expected string value")
                    }
                },
            }
        }
    }

    #[test]
    fn redaction_processor_log_body_test() {
        initialize();
        let mut logs_request = FakeOTLP::logs_service_request();
        let log_body = opentelemetry_proto::tonic::common::v1::AnyValue {
            value: Some(Value::StringValue(
                "Login successful: password=1234567890".to_string(),
            )),
        };
        logs_request.resource_logs[0].scope_logs[0].log_records[0].body = Some(log_body);

        // Transform the protobuf ResourceLogs into our internal RResourceLogs
        let r_resource_logs = crate::model::otel_transform::transform_resource_logs(
            logs_request.resource_logs[0].clone(),
        );

        // Create the Python-exposed ResourceLogs object
        let py_resource_logs = ResourceLogs {
            resource: r_resource_logs.resource.clone(),
            scope_logs: r_resource_logs.scope_logs.clone(),
            schema_url: r_resource_logs.schema_url.clone(),
        };

        // Execute the Python script that removes a log record
        Python::with_gil(|py| -> PyResult<()> {
            _run_script(
                "redaction_processor_log_body_test.py",
                py,
                py_resource_logs,
                Some("process_logs".to_string()),
            )
        })
        .unwrap();

        let scope_logs_vec = Arc::into_inner(r_resource_logs.scope_logs)
            .unwrap()
            .into_inner()
            .unwrap();
        let mut scope_logs = crate::model::py_transform::transform_logs(scope_logs_vec);
        let log = scope_logs.pop().unwrap().log_records.pop().unwrap();
        let body = log.body.unwrap().value.unwrap();
        match body {
            Value::StringValue(b) => {
                assert_eq!(b, "6a3840901d0f07c768b96a0fad2cabe2");
            }
            _ => panic!("Unexpected log record value type"),
        }

        let mut logs_request = FakeOTLP::logs_service_request();
        let log_body = opentelemetry_proto::tonic::common::v1::AnyValue {
            value: Some(Value::KvlistValue(
                opentelemetry_proto::tonic::common::v1::KeyValueList {
                    values: vec![opentelemetry_proto::tonic::common::v1::KeyValue {
                        key: "another_body".to_string(),
                        value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                            value: Some(Value::StringValue(
                                "Login successful: password=1234567890".to_string(),
                            )),
                        }),
                    }],
                },
            )),
        };
        logs_request.resource_logs[0].scope_logs[0].log_records[0].body = Some(log_body);

        // Transform the protobuf ResourceLogs into our internal RResourceLogs
        let r_resource_logs = crate::model::otel_transform::transform_resource_logs(
            logs_request.resource_logs[0].clone(),
        );

        // Create the Python-exposed ResourceLogs object
        let py_resource_logs = ResourceLogs {
            resource: r_resource_logs.resource.clone(),
            scope_logs: r_resource_logs.scope_logs.clone(),
            schema_url: r_resource_logs.schema_url.clone(),
        };

        // Execute the Python script that removes a log record
        Python::with_gil(|py| -> PyResult<()> {
            _run_script(
                "redaction_processor_log_body_test.py",
                py,
                py_resource_logs,
                Some("process_logs".to_string()),
            )
        })
        .unwrap();

        let scope_logs_vec = Arc::into_inner(r_resource_logs.scope_logs)
            .unwrap()
            .into_inner()
            .unwrap();
        let mut scope_logs = crate::model::py_transform::transform_logs(scope_logs_vec);
        let log = scope_logs.pop().unwrap().log_records.pop().unwrap();
        let body = log.body.unwrap().value.unwrap();
        match body {
            Value::KvlistValue(mut kvl) => {
                let v = kvl.values.pop().unwrap();
                match v.value.unwrap().value.unwrap() {
                    Value::StringValue(b) => {
                        assert_eq!(b, "6a3840901d0f07c768b96a0fad2cabe2");
                    }
                    _ => {
                        panic!("Unexpected log record value type");
                    }
                }
            }
            _ => panic!("Unexpected log record value type"),
        }

        let mut logs_request = FakeOTLP::logs_service_request();
        let log_body = opentelemetry_proto::tonic::common::v1::AnyValue {
            value: Some(Value::ArrayValue(
                opentelemetry_proto::tonic::common::v1::ArrayValue {
                    values: vec![
                        opentelemetry_proto::tonic::common::v1::AnyValue {
                            value: Some(Value::StringValue(
                                "Login failed: password=1234567890".to_string(),
                            )),
                        },
                        opentelemetry_proto::tonic::common::v1::AnyValue {
                            value: Some(Value::KvlistValue(
                                opentelemetry_proto::tonic::common::v1::KeyValueList {
                                    values: vec![
                                        opentelemetry_proto::tonic::common::v1::KeyValue {
                                            key: "another_body".to_string(),
                                            value: Some(
                                                opentelemetry_proto::tonic::common::v1::AnyValue {
                                                    value: Some(Value::StringValue(
                                                        "Login successful: password=1234567890"
                                                            .to_string(),
                                                    )),
                                                },
                                            ),
                                        },
                                    ],
                                },
                            )),
                        },
                    ],
                },
            )),
        };
        logs_request.resource_logs[0].scope_logs[0].log_records[0].body = Some(log_body);

        // Transform the protobuf ResourceLogs into our internal RResourceLogs
        let r_resource_logs = crate::model::otel_transform::transform_resource_logs(
            logs_request.resource_logs[0].clone(),
        );

        // Create the Python-exposed ResourceLogs object
        let py_resource_logs = ResourceLogs {
            resource: r_resource_logs.resource.clone(),
            scope_logs: r_resource_logs.scope_logs.clone(),
            schema_url: r_resource_logs.schema_url.clone(),
        };

        // Execute the Python script that removes a log record
        Python::with_gil(|py| -> PyResult<()> {
            _run_script(
                "redaction_processor_log_body_test.py",
                py,
                py_resource_logs,
                Some("process_logs".to_string()),
            )
        })
        .unwrap();
        let scope_logs_vec = Arc::into_inner(r_resource_logs.scope_logs)
            .unwrap()
            .into_inner()
            .unwrap();
        let mut scope_logs = crate::model::py_transform::transform_logs(scope_logs_vec);
        let log = scope_logs.pop().unwrap().log_records.pop().unwrap();
        let body = log.body.unwrap().value.unwrap();
        match body {
            Value::ArrayValue(mut av) => {
                assert_eq!(av.values.len(), 2);
                while !av.values.is_empty() {
                    let v = av.values.pop().unwrap();
                    match v.value.unwrap() {
                        Value::StringValue(b) => {
                            assert_eq!(b, "34c37eb5ba287d52490cc8c2e3ebc231");
                        }
                        Value::KvlistValue(mut kvl) => {
                            let v = kvl.values.pop().unwrap();
                            match v.value.unwrap().value.unwrap() {
                                Value::StringValue(b) => {
                                    assert_eq!(b, "6a3840901d0f07c768b96a0fad2cabe2");
                                }
                                _ => {
                                    panic!("Unexpected log record value type");
                                }
                            }
                        }
                        _ => {
                            panic!("Unexpected log record value type");
                        }
                    }
                }
            }
            _ => {
                panic!("Unexpected log record value type");
            }
        }
    }

    #[test]
    fn read_and_write_metrics_test() {
        initialize();

        // Create comprehensive initial OpenTelemetry protobuf with ALL metric types
        let resource_metrics = opentelemetry_proto::tonic::metrics::v1::ResourceMetrics {
            resource: Some(opentelemetry_proto::tonic::resource::v1::Resource {
                attributes: vec![
                    opentelemetry_proto::tonic::common::v1::KeyValue {
                        key: "initial_resource_key".to_string(),
                        value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                            value: Some(Value::StringValue("initial_resource_value".to_string())),
                        }),
                    },
                ],
                dropped_attributes_count: 10,
            }),
            scope_metrics: vec![
                opentelemetry_proto::tonic::metrics::v1::ScopeMetrics {
                    scope: Some(opentelemetry_proto::tonic::common::v1::InstrumentationScope {
                        name: "initial_scope_name".to_string(),
                        version: "1.0.0".to_string(),
                        attributes: vec![
                            opentelemetry_proto::tonic::common::v1::KeyValue {
                                key: "initial_scope_key".to_string(),
                                value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                                    value: Some(Value::BoolValue(true)),
                                }),
                            },
                        ],
                        dropped_attributes_count: 5,
                    }),
                    metrics: vec![
                        // 1. Gauge metric
                        opentelemetry_proto::tonic::metrics::v1::Metric {
                            name: "initial_gauge_metric".to_string(),
                            description: "initial_gauge_description".to_string(),
                            unit: "initial_gauge_unit".to_string(),
                            metadata: vec![
                                opentelemetry_proto::tonic::common::v1::KeyValue {
                                    key: "gauge_metadata_key".to_string(),
                                    value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                                        value: Some(Value::StringValue("gauge_metadata_value".to_string())),
                                    }),
                                },
                            ],
                            data: Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(
                                opentelemetry_proto::tonic::metrics::v1::Gauge {
                                    data_points: vec![
                                        opentelemetry_proto::tonic::metrics::v1::NumberDataPoint {
                                            attributes: vec![
                                                opentelemetry_proto::tonic::common::v1::KeyValue {
                                                    key: "gauge_dp_key".to_string(),
                                                    value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                                                        value: Some(Value::StringValue("gauge_dp_value".to_string())),
                                                    }),
                                                },
                                            ],
                                            start_time_unix_nano: 1000,
                                            time_unix_nano: 2000,
                                            exemplars: vec![
                                                opentelemetry_proto::tonic::metrics::v1::Exemplar {
                                                    filtered_attributes: vec![
                                                        opentelemetry_proto::tonic::common::v1::KeyValue {
                                                            key: "gauge_exemplar_key".to_string(),
                                                            value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                                                                value: Some(Value::StringValue("gauge_exemplar_value".to_string())),
                                                            }),
                                                        },
                                                    ],
                                                    time_unix_nano: 1500,
                                                    span_id: b"\x01\x02\x03\x04\x05\x06\x07\x08".to_vec(),
                                                    trace_id: b"\x08\x07\x06\x05\x04\x03\x02\x01\x08\x07\x06\x05\x04\x03\x02\x01".to_vec(),
                                                    value: Some(opentelemetry_proto::tonic::metrics::v1::exemplar::Value::AsDouble(15.5)),
                                                },
                                            ],
                                            flags: 0,
                                            value: Some(opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(123.45)),
                                        },
                                    ],
                                },
                            )),
                        },
                        // 2. Sum metric
                        opentelemetry_proto::tonic::metrics::v1::Metric {
                            name: "initial_sum_metric".to_string(),
                            description: "initial_sum_description".to_string(),
                            unit: "initial_sum_unit".to_string(),
                            metadata: vec![
                                opentelemetry_proto::tonic::common::v1::KeyValue {
                                    key: "sum_metadata_key".to_string(),
                                    value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                                        value: Some(Value::IntValue(42)),
                                    }),
                                },
                            ],
                            data: Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Sum(
                                opentelemetry_proto::tonic::metrics::v1::Sum {
                                    data_points: vec![
                                        opentelemetry_proto::tonic::metrics::v1::NumberDataPoint {
                                            attributes: vec![
                                                opentelemetry_proto::tonic::common::v1::KeyValue {
                                                    key: "sum_dp_key".to_string(),
                                                    value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                                                        value: Some(Value::StringValue("sum_dp_value".to_string())),
                                                    }),
                                                },
                                            ],
                                            start_time_unix_nano: 3000,
                                            time_unix_nano: 4000,
                                            exemplars: vec![],
                                            flags: 0,
                                            value: Some(opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsInt(100)),
                                        },
                                    ],
                                    aggregation_temporality: opentelemetry_proto::tonic::metrics::v1::AggregationTemporality::Delta as i32,
                                    is_monotonic: true,
                                },
                            )),
                        },
                        // 3. Histogram metric
                        opentelemetry_proto::tonic::metrics::v1::Metric {
                            name: "initial_histogram_metric".to_string(),
                            description: "initial_histogram_description".to_string(),
                            unit: "initial_histogram_unit".to_string(),
                            metadata: vec![
                                opentelemetry_proto::tonic::common::v1::KeyValue {
                                    key: "histogram_metadata_key".to_string(),
                                    value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                                        value: Some(Value::BoolValue(false)),
                                    }),
                                },
                            ],
                            data: Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Histogram(
                                opentelemetry_proto::tonic::metrics::v1::Histogram {
                                    data_points: vec![
                                        opentelemetry_proto::tonic::metrics::v1::HistogramDataPoint {
                                            attributes: vec![
                                                opentelemetry_proto::tonic::common::v1::KeyValue {
                                                    key: "histogram_dp_key".to_string(),
                                                    value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                                                        value: Some(Value::StringValue("histogram_dp_value".to_string())),
                                                    }),
                                                },
                                            ],
                                            start_time_unix_nano: 5000,
                                            time_unix_nano: 6000,
                                            count: 50,
                                            sum: Some(500.0),
                                            bucket_counts: vec![5, 10, 15, 20],
                                            explicit_bounds: vec![1.0, 5.0, 10.0],
                                            exemplars: vec![
                                                opentelemetry_proto::tonic::metrics::v1::Exemplar {
                                                    filtered_attributes: vec![],
                                                    time_unix_nano: 5500,
                                                    span_id: b"\x11\x12\x13\x14\x15\x16\x17\x18".to_vec(),
                                                    trace_id: b"\x18\x17\x16\x15\x14\x13\x12\x11\x18\x17\x16\x15\x14\x13\x12\x11".to_vec(),
                                                    value: Some(opentelemetry_proto::tonic::metrics::v1::exemplar::Value::AsInt(25)),
                                                },
                                            ],
                                            flags: 1,
                                            min: Some(0.1),
                                            max: Some(20.0),
                                        },
                                    ],
                                    aggregation_temporality: opentelemetry_proto::tonic::metrics::v1::AggregationTemporality::Cumulative as i32,
                                },
                            )),
                        },
                        // 4. ExponentialHistogram metric
                        opentelemetry_proto::tonic::metrics::v1::Metric {
                            name: "initial_exp_histogram_metric".to_string(),
                            description: "initial_exp_histogram_description".to_string(),
                            unit: "initial_exp_histogram_unit".to_string(),
                            metadata: vec![
                                opentelemetry_proto::tonic::common::v1::KeyValue {
                                    key: "exp_histogram_metadata_key".to_string(),
                                    value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                                        value: Some(Value::DoubleValue(3.14)),
                                    }),
                                },
                            ],
                            data: Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::ExponentialHistogram(
                                opentelemetry_proto::tonic::metrics::v1::ExponentialHistogram {
                                    data_points: vec![
                                        opentelemetry_proto::tonic::metrics::v1::ExponentialHistogramDataPoint {
                                            attributes: vec![
                                                opentelemetry_proto::tonic::common::v1::KeyValue {
                                                    key: "exp_histogram_dp_key".to_string(),
                                                    value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                                                        value: Some(Value::StringValue("exp_histogram_dp_value".to_string())),
                                                    }),
                                                },
                                            ],
                                            start_time_unix_nano: 7000,
                                            time_unix_nano: 8000,
                                            count: 75,
                                            sum: Some(750.0),
                                            scale: 1,
                                            zero_count: 3,
                                            positive: Some(opentelemetry_proto::tonic::metrics::v1::exponential_histogram_data_point::Buckets {
                                                offset: 2,
                                                bucket_counts: vec![2, 4, 6, 8],
                                            }),
                                            negative: Some(opentelemetry_proto::tonic::metrics::v1::exponential_histogram_data_point::Buckets {
                                                offset: -1,
                                                bucket_counts: vec![1, 2, 3],
                                            }),
                                            flags: 0,
                                            exemplars: vec![
                                                opentelemetry_proto::tonic::metrics::v1::Exemplar {
                                                    filtered_attributes: vec![
                                                        opentelemetry_proto::tonic::common::v1::KeyValue {
                                                            key: "exp_exemplar_key".to_string(),
                                                            value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                                                                value: Some(Value::StringValue("exp_exemplar_value".to_string())),
                                                            }),
                                                        },
                                                    ],
                                                    time_unix_nano: 7500,
                                                    span_id: b"\x21\x22\x23\x24\x25\x26\x27\x28".to_vec(),
                                                    trace_id: b"\x28\x27\x26\x25\x24\x23\x22\x21\x28\x27\x26\x25\x24\x23\x22\x21".to_vec(),
                                                    value: Some(opentelemetry_proto::tonic::metrics::v1::exemplar::Value::AsDouble(37.5)),
                                                },
                                            ],
                                            min: Some(0.5),
                                            max: Some(50.0),
                                            zero_threshold: 0.01,
                                        },
                                    ],
                                    aggregation_temporality: opentelemetry_proto::tonic::metrics::v1::AggregationTemporality::Delta as i32,
                                },
                            )),
                        },
                        // 5. Summary metric
                        opentelemetry_proto::tonic::metrics::v1::Metric {
                            name: "initial_summary_metric".to_string(),
                            description: "initial_summary_description".to_string(),
                            unit: "initial_summary_unit".to_string(),
                            metadata: vec![
                                opentelemetry_proto::tonic::common::v1::KeyValue {
                                    key: "summary_metadata_key".to_string(),
                                    value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                                        value: Some(Value::BytesValue(b"summary_bytes".to_vec())),
                                    }),
                                },
                            ],
                            data: Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Summary(
                                opentelemetry_proto::tonic::metrics::v1::Summary {
                                    data_points: vec![
                                        opentelemetry_proto::tonic::metrics::v1::SummaryDataPoint {
                                            attributes: vec![
                                                opentelemetry_proto::tonic::common::v1::KeyValue {
                                                    key: "summary_dp_key".to_string(),
                                                    value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                                                        value: Some(Value::StringValue("summary_dp_value".to_string())),
                                                    }),
                                                },
                                            ],
                                            start_time_unix_nano: 9000,
                                            time_unix_nano: 10000,
                                            count: 25,
                                            sum: 250.0,
                                            quantile_values: vec![
                                                opentelemetry_proto::tonic::metrics::v1::summary_data_point::ValueAtQuantile {
                                                    quantile: 0.5,
                                                    value: 10.0,
                                                },
                                                opentelemetry_proto::tonic::metrics::v1::summary_data_point::ValueAtQuantile {
                                                    quantile: 0.95,
                                                    value: 19.0,
                                                },
                                                opentelemetry_proto::tonic::metrics::v1::summary_data_point::ValueAtQuantile {
                                                    quantile: 0.99,
                                                    value: 19.8,
                                                },
                                            ],
                                            flags: 1,
                                        },
                                    ],
                                },
                            )),
                        },
                    ],
                    schema_url: "initial_schema_url_scope".to_string(),
                },
            ],
            schema_url: "initial_schema_url_resource".to_string(),
        };

        // Transform to our internal model
        let r_resource_metrics = otel_transform::transform_resource_metrics(resource_metrics);
        let py_resource_metrics = ResourceMetrics {
            resource: r_resource_metrics.resource.clone(),
            scope_metrics: r_resource_metrics.scope_metrics.clone(),
            schema_url: r_resource_metrics.schema_url.clone(),
        };

        // Execute the Python script that will verify initial values and then mutate them
        Python::with_gil(|py| -> PyResult<()> {
            _run_script(
                "read_and_write_metrics_test.py",
                py,
                py_resource_metrics,
                Some("process_metrics".to_string()),
            )
        })
        .unwrap();

        // --- RUST VERIFICATION OF PYTHON MUTATIONS ---

        // Transform back to protobuf for verification
        let scope_metrics_vec = Arc::into_inner(r_resource_metrics.scope_metrics)
            .unwrap()
            .into_inner()
            .unwrap();
        let scope_metrics = py_transform::transform_metrics(scope_metrics_vec);

        assert_eq!(scope_metrics.len(), 1);
        let proto_scope_metrics = &scope_metrics[0];

        // Should still have 5 metrics after Python processing
        assert_eq!(proto_scope_metrics.metrics.len(), 5);

        // --- Verify ResourceMetrics mutations ---
        let resource = Arc::into_inner(r_resource_metrics.resource)
            .unwrap()
            .into_inner()
            .unwrap()
            .unwrap();
        let resource_proto = py_transform::transform_resource(resource).unwrap();
        assert_eq!(resource_proto.dropped_attributes_count, 25); // Changed from 10 to 25
        assert_eq!(resource_proto.attributes.len(), 2); // Added one attribute
        assert_eq!(
            resource_proto.attributes[1].key,
            "python_added_resource_key"
        );

        // --- Verify ScopeMetrics mutations ---
        assert_eq!(
            proto_scope_metrics.schema_url,
            "python_modified_schema_url_scope"
        );
        let proto_scope = proto_scope_metrics.scope.as_ref().unwrap();
        assert_eq!(proto_scope.name, "python_modified_scope_name");
        assert_eq!(proto_scope.version, "python_modified_scope_version");
        assert_eq!(proto_scope.dropped_attributes_count, 15); // Changed from 5 to 15
        assert_eq!(proto_scope.attributes.len(), 2); // Added one attribute
        assert_eq!(proto_scope.attributes[1].key, "python_added_scope_key");

        // --- Verify Gauge metric mutations ---
        let gauge_metric = &proto_scope_metrics.metrics[0];
        assert_eq!(gauge_metric.name, "python_modified_gauge_metric");
        assert_eq!(
            gauge_metric.description,
            "python_modified_gauge_description"
        );
        assert_eq!(gauge_metric.unit, "python_modified_gauge_unit");
        assert_eq!(gauge_metric.metadata.len(), 2); // Added one metadata
        assert_eq!(gauge_metric.metadata[1].key, "python_added_gauge_metadata");

        match gauge_metric.data.as_ref().unwrap() {
            opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(g) => {
                assert_eq!(g.data_points.len(), 2); // Added one data point
                let dp = &g.data_points[0];
                assert_eq!(dp.start_time_unix_nano, 1100); // Changed from 1000
                assert_eq!(dp.time_unix_nano, 2200); // Changed from 2000
                assert_eq!(dp.flags, 1); // Changed from 0
                assert_eq!(dp.attributes.len(), 2); // Added one attribute
                assert_eq!(dp.exemplars.len(), 2); // Added one exemplar
            }
            _ => panic!("Expected Gauge metric data"),
        }

        // --- Verify Sum metric mutations ---
        let sum_metric = &proto_scope_metrics.metrics[1];
        assert_eq!(sum_metric.name, "python_modified_sum_metric");
        assert_eq!(sum_metric.description, "python_modified_sum_description");
        assert_eq!(sum_metric.unit, "python_modified_sum_unit");

        match sum_metric.data.as_ref().unwrap() {
            opentelemetry_proto::tonic::metrics::v1::metric::Data::Sum(s) => {
                assert_eq!(
                    s.aggregation_temporality,
                    opentelemetry_proto::tonic::metrics::v1::AggregationTemporality::Cumulative
                        as i32
                ); // Changed from Delta
                assert!(!s.is_monotonic); // Changed from true to false
                assert_eq!(s.data_points.len(), 2); // Added one data point
                let dp = &s.data_points[0];
                assert_eq!(dp.start_time_unix_nano, 3300); // Changed from 3000
                assert_eq!(dp.time_unix_nano, 4400); // Changed from 4000
            }
            _ => panic!("Expected Sum metric data"),
        }

        // --- Verify Histogram metric mutations ---
        let hist_metric = &proto_scope_metrics.metrics[2];
        assert_eq!(hist_metric.name, "python_modified_histogram_metric");
        assert_eq!(
            hist_metric.description,
            "python_modified_histogram_description"
        );
        assert_eq!(hist_metric.unit, "python_modified_histogram_unit");

        match hist_metric.data.as_ref().unwrap() {
            opentelemetry_proto::tonic::metrics::v1::metric::Data::Histogram(h) => {
                assert_eq!(
                    h.aggregation_temporality,
                    opentelemetry_proto::tonic::metrics::v1::AggregationTemporality::Delta as i32
                ); // Changed from Cumulative
                assert_eq!(h.data_points.len(), 2); // Added one data point
                let dp = &h.data_points[0];
                assert_eq!(dp.count, 75); // Changed from 50
                assert_eq!(dp.sum.unwrap(), 750.0); // Changed from 500.0
                assert_eq!(dp.bucket_counts, vec![10, 20, 30, 40]); // Changed values
                assert_eq!(dp.explicit_bounds, vec![2.0, 10.0, 20.0]); // Changed values
                assert_eq!(dp.min.unwrap(), 0.2); // Changed from 0.1
                assert_eq!(dp.max.unwrap(), 25.0); // Changed from 20.0
            }
            _ => panic!("Expected Histogram metric data"),
        }

        // --- Verify ExponentialHistogram metric mutations ---
        let exp_hist_metric = &proto_scope_metrics.metrics[3];
        assert_eq!(exp_hist_metric.name, "python_modified_exp_histogram_metric");
        assert_eq!(
            exp_hist_metric.description,
            "python_modified_exp_histogram_description"
        );
        assert_eq!(exp_hist_metric.unit, "python_modified_exp_histogram_unit");

        match exp_hist_metric.data.as_ref().unwrap() {
            opentelemetry_proto::tonic::metrics::v1::metric::Data::ExponentialHistogram(eh) => {
                assert_eq!(
                    eh.aggregation_temporality,
                    opentelemetry_proto::tonic::metrics::v1::AggregationTemporality::Cumulative
                        as i32
                ); // Changed from Delta
                assert_eq!(eh.data_points.len(), 2); // Added one data point
                let dp = &eh.data_points[0];
                assert_eq!(dp.count, 100); // Changed from 75
                assert_eq!(dp.sum.unwrap(), 1000.0); // Changed from 750.0
                assert_eq!(dp.scale, 2); // Changed from 1
                assert_eq!(dp.zero_count, 5); // Changed from 3
                assert_eq!(dp.zero_threshold, 0.02); // Changed from 0.01

                // Verify positive buckets were modified
                let positive = dp.positive.as_ref().unwrap();
                assert_eq!(positive.offset, 3); // Changed from 2
                assert_eq!(positive.bucket_counts, vec![4, 8, 12, 16]); // Changed values

                // Verify negative buckets were modified
                let negative = dp.negative.as_ref().unwrap();
                assert_eq!(negative.offset, -2); // Changed from -1
                assert_eq!(negative.bucket_counts, vec![2, 4, 6]); // Changed values
            }
            _ => panic!("Expected ExponentialHistogram metric data"),
        }

        // --- Verify Summary metric mutations ---
        let summary_metric = &proto_scope_metrics.metrics[4];
        assert_eq!(summary_metric.name, "python_modified_summary_metric");
        assert_eq!(
            summary_metric.description,
            "python_modified_summary_description"
        );
        assert_eq!(summary_metric.unit, "python_modified_summary_unit");

        match summary_metric.data.as_ref().unwrap() {
            opentelemetry_proto::tonic::metrics::v1::metric::Data::Summary(s) => {
                assert_eq!(s.data_points.len(), 2); // Added one data point
                let dp = &s.data_points[0];
                assert_eq!(dp.count, 50); // Changed from 25
                assert_eq!(dp.sum, 500.0); // Changed from 250.0
                assert_eq!(dp.flags, 0); // Changed from 1

                // Verify quantile values were modified
                assert_eq!(dp.quantile_values.len(), 4); // Added one quantile
                assert_eq!(dp.quantile_values[0].quantile, 0.5);
                assert_eq!(dp.quantile_values[0].value, 20.0); // Changed from 10.0
                assert_eq!(dp.quantile_values[1].quantile, 0.95);
                assert_eq!(dp.quantile_values[1].value, 38.0); // Changed from 19.0
                assert_eq!(dp.quantile_values[2].quantile, 0.99);
                assert_eq!(dp.quantile_values[2].value, 39.6); // Changed from 19.8
                assert_eq!(dp.quantile_values[3].quantile, 0.999); // New quantile
                assert_eq!(dp.quantile_values[3].value, 39.96); // New value
            }
            _ => panic!("Expected Summary metric data"),
        }
        println!("All comprehensive metric mutation verifications passed!");
    }
}
