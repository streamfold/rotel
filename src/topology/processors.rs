// SPDX-License-Identifier: Apache-2.0

#[cfg(feature = "pyo3")]
use rotel_sdk::model::register_processor;
#[cfg(feature = "pyo3")]
use rotel_sdk::py::request_context::RequestContext as PyRequestContext;
#[cfg(feature = "pyo3")]
use std::env;
#[cfg(feature = "pyo3")]
use std::path::Path;
use tower::BoxError;

use crate::topology::generic_pipeline::Inspect;
use crate::topology::payload::Message;

#[cfg(feature = "pyo3")]
use rotel_sdk::model::PythonProcessable;

/// Trait for types that can be processed by Python processors
#[cfg(not(feature = "pyo3"))]
pub trait PythonProcessable {
    fn process(self, processor: &str) -> Self;
}

#[cfg(not(feature = "pyo3"))]
impl PythonProcessable for opentelemetry_proto::tonic::trace::v1::ResourceSpans {
    fn process(self, _processor: &str) -> Self {
        // Noop
        self
    }
}

#[cfg(not(feature = "pyo3"))]
impl PythonProcessable for opentelemetry_proto::tonic::metrics::v1::ResourceMetrics {
    fn process(self, _processor: &str) -> Self {
        // Noop
        self
    }
}

#[cfg(not(feature = "pyo3"))]
impl PythonProcessable for opentelemetry_proto::tonic::logs::v1::ResourceLogs {
    fn process(self, _processor: &str) -> Self {
        // Noop
        self
    }
}

/// Container for Python processors
#[derive(Debug)]
pub struct Processors {
    #[allow(dead_code)] // Only accessed in pyo3
    processor_modules: Vec<String>,
}

impl Processors {
    /// Create an empty Processors instance (used when pyo3 feature is disabled)
    pub fn empty() -> Self {
        Self {
            processor_modules: Vec::new(),
        }
    }

    /// Initialize processors from a list of processor file paths
    #[cfg(feature = "pyo3")]
    pub fn initialize(processor_files: Vec<String>) -> Result<Self, BoxError> {
        let mut processor_modules = vec![];
        let current_dir = env::current_dir()?;

        for (processor_idx, file) in processor_files.iter().enumerate() {
            let file_path = Path::new(file);

            // Use absolute path if provided, otherwise make relative to current directory
            let script_path = if file_path.is_absolute() {
                file_path.to_path_buf()
            } else {
                current_dir.join(file_path)
            };

            let code = match std::fs::read_to_string(&script_path) {
                Ok(c) => c,
                Err(e) => {
                    return Err(format!(
                        "Failed to read processor script {}: {}",
                        script_path.display(),
                        e
                    )
                    .into());
                }
            };

            let module = format!("rotel_processor_{}", processor_idx);

            match register_processor(code, file.clone(), module.clone()) {
                Ok(_) => {
                    processor_modules.push(module);
                }
                Err(e) => {
                    return Err(format!("Failed to register processor {}: {}", file, e).into());
                }
            }
        }

        Ok(Self { processor_modules })
    }

    /// Initialize processors from a list of processor file paths (noop when pyo3 is disabled)
    #[cfg(not(feature = "pyo3"))]
    pub fn initialize(_processor_files: Vec<String>) -> Result<Self, BoxError> {
        Ok(Self::empty())
    }

    /// Run the processors on a message
    #[cfg(not(feature = "pyo3"))]
    pub fn run<T>(&self, message: Message<T>, inspector: &impl Inspect<T>) -> Message<T>
    where
        T: PythonProcessable,
    {
        inspector.inspect(&message.payload);
        message
    }

    /// Run the processors on a message
    #[cfg(feature = "pyo3")]
    pub fn run<T>(&self, message: Message<T>, inspector: &impl Inspect<T>) -> Message<T>
    where
        T: PythonProcessable,
    {
        let mut items = message.payload;
        let request_context = message.request_context.clone();
        let mut py_request_context: Option<PyRequestContext> = None;
        match message.request_context {
            None => {}
            Some(ctx) => py_request_context = Some(ctx.into()),
        }

        // Invoke current middleware layer
        let len_processor_modules = self.processor_modules.len();
        if len_processor_modules > 0 {
            inspector.inspect_with_prefix(Some("OTLP payload before processing".into()), &items);
        } else {
            inspector.inspect(&items);
        }

        for p in &self.processor_modules {
            let mut new_items = Vec::new();

            while !items.is_empty() {
                let item = items.pop();
                if let Some(item) = item {
                    let result = item.process(p, py_request_context.clone());
                    new_items.push(result);
                }
            }
            items = new_items;
        }

        if len_processor_modules > 0 {
            inspector.inspect_with_prefix(Some("OTLP payload after processing".into()), &items);
        }

        // Wrap the processed items back into a Message
        Message {
            metadata: message.metadata,
            request_context,
            payload: items,
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "pyo3")]
    use super::*;

    #[cfg(feature = "pyo3")]
    #[test]
    fn test_initialize_processors_success() {
        // Create a temporary Python file with a valid processor
        let processor_code = r#"
def process_spans(resource_spans):
    # Simple processor that doesn't modify anything
    pass

def process_metrics(resource_metrics):
    pass

def process_logs(resource_logs):
    pass
"#;

        let temp_file = tempfile::NamedTempFile::new().expect("Failed to create temp file");
        std::fs::write(temp_file.path(), processor_code).expect("Failed to write to temp file");

        let proc_path = temp_file.path().to_string_lossy().to_string();

        // Initialize processors - should succeed
        let result = Processors::initialize(vec![proc_path]);

        assert!(result.is_ok(), "Expected initialization to succeed");
        let processors = result.unwrap();
        assert_eq!(processors.processor_modules.len(), 1);
        assert_eq!(processors.processor_modules[0], "rotel_processor_0");
    }

    #[cfg(feature = "pyo3")]
    #[test]
    fn test_initialize_processors_file_not_found() {
        // Try to initialize with a non-existent file
        let result = Processors::initialize(vec!["non_existent_processor.py".to_string()]);

        assert!(result.is_err(), "Expected initialization to fail");
        let err = result.unwrap_err();
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("Failed to read processor script"),
            "Expected error message about failing to read script, got: {}",
            err_msg
        );
    }

    #[cfg(feature = "pyo3")]
    #[test]
    fn test_initialize_processors_invalid_python_syntax() {
        // Create a temporary Python file with invalid syntax
        let invalid_code = r#"
def process_spans(resource_spans)
    # Missing colon - syntax error
    pass
"#;

        let temp_file = tempfile::NamedTempFile::new().expect("Failed to create temp file");
        std::fs::write(temp_file.path(), invalid_code).expect("Failed to write to temp file");

        let proc_path = temp_file.path().to_string_lossy().to_string();

        // Initialize processors - should fail due to syntax error
        let result = Processors::initialize(vec![proc_path]);

        assert!(
            result.is_err(),
            "Expected initialization to fail with invalid Python syntax"
        );
        let err = result.unwrap_err();
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("Failed to register processor"),
            "Expected error message about failing to register processor, got: {}",
            err_msg
        );
    }

    #[cfg(feature = "pyo3")]
    #[test]
    fn test_initialize_multiple_processors() {
        // Create two valid processor files
        let processor_code1 = r#"
def process_spans(resource_spans):
    pass
def process_metrics(resource_metrics):
    pass
def process_logs(resource_logs):
    pass
"#;

        let processor_code2 = r#"
def process_spans(resource_spans):
    pass
def process_metrics(resource_metrics):
    pass
def process_logs(resource_logs):
    pass
"#;

        let temp_file1 = tempfile::NamedTempFile::new().expect("Failed to create temp file 1");
        let temp_file2 = tempfile::NamedTempFile::new().expect("Failed to create temp file 2");

        std::fs::write(temp_file1.path(), processor_code1).expect("Failed to write to temp file 1");
        std::fs::write(temp_file2.path(), processor_code2).expect("Failed to write to temp file 2");

        let proc_path1 = temp_file1.path().to_string_lossy().to_string();
        let proc_path2 = temp_file2.path().to_string_lossy().to_string();

        // Initialize processors - should succeed
        let result = Processors::initialize(vec![proc_path1, proc_path2]);

        assert!(result.is_ok(), "Expected initialization to succeed");
        let processors = result.unwrap();
        assert_eq!(processors.processor_modules.len(), 2);
        assert_eq!(processors.processor_modules[0], "rotel_processor_0");
        assert_eq!(processors.processor_modules[1], "rotel_processor_1");
    }
}
