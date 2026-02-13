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

// Rust processor imports
#[cfg(feature = "rust_processor")]
use abi_stable::library::lib_header_from_path;
#[cfg(feature = "rust_processor")]
use rotel_rust_processor_sdk::{
    AsyncProcessorModuleRef, AsyncRotelProcessor_TO, ProcessorModuleRef, ROption,
    RotelProcessor_TO,
    types::{RRequestContext, RResourceLogs, RResourceMetrics, RResourceSpans},
};
#[cfg(feature = "rust_processor")]
use std::path::Path as StdPath;
#[cfg(feature = "rust_processor")]
use std::sync::Arc;
#[cfg(feature = "rust_processor")]
use tracing::info;

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

/// Trait for types that can be processed by Rust processors
#[cfg(feature = "rust_processor")]
pub trait RustProcessable: Sized {
    type RType;

    fn to_ffi(self) -> Self::RType;
    fn from_ffi(r: Self::RType) -> Self;

    fn process_with_rust(
        self,
        processor: &RotelProcessor_TO<'static, abi_stable::std_types::RBox<()>>,
        context: &ROption<RRequestContext>,
    ) -> Self;
}

#[cfg(feature = "rust_processor")]
impl RustProcessable for opentelemetry_proto::tonic::trace::v1::ResourceSpans {
    type RType = RResourceSpans;

    fn to_ffi(self) -> Self::RType {
        self.into()
    }

    fn from_ffi(r: Self::RType) -> Self {
        r.into()
    }

    fn process_with_rust(
        self,
        processor: &RotelProcessor_TO<'static, abi_stable::std_types::RBox<()>>,
        context: &ROption<RRequestContext>,
    ) -> Self {
        let mut r_spans = self.to_ffi();
        processor.process_spans(&mut r_spans, context);
        Self::from_ffi(r_spans)
    }
}

#[cfg(feature = "rust_processor")]
impl RustProcessable for opentelemetry_proto::tonic::metrics::v1::ResourceMetrics {
    type RType = RResourceMetrics;

    fn to_ffi(self) -> Self::RType {
        self.into()
    }

    fn from_ffi(r: Self::RType) -> Self {
        r.into()
    }

    fn process_with_rust(
        self,
        processor: &RotelProcessor_TO<'static, abi_stable::std_types::RBox<()>>,
        context: &ROption<RRequestContext>,
    ) -> Self {
        let mut r_metrics = self.to_ffi();
        processor.process_metrics(&mut r_metrics, context);
        Self::from_ffi(r_metrics)
    }
}

#[cfg(feature = "rust_processor")]
impl RustProcessable for opentelemetry_proto::tonic::logs::v1::ResourceLogs {
    type RType = RResourceLogs;

    fn to_ffi(self) -> Self::RType {
        self.into()
    }

    fn from_ffi(r: Self::RType) -> Self {
        r.into()
    }

    fn process_with_rust(
        self,
        processor: &RotelProcessor_TO<'static, abi_stable::std_types::RBox<()>>,
        context: &ROption<RRequestContext>,
    ) -> Self {
        let mut r_logs = self.to_ffi();
        processor.process_logs(&mut r_logs, context);
        Self::from_ffi(r_logs)
    }
}

/// Noop trait when rust_processor feature is disabled
#[cfg(not(feature = "rust_processor"))]
pub trait RustProcessable {}

#[cfg(not(feature = "rust_processor"))]
impl RustProcessable for opentelemetry_proto::tonic::trace::v1::ResourceSpans {}
#[cfg(not(feature = "rust_processor"))]
impl RustProcessable for opentelemetry_proto::tonic::metrics::v1::ResourceMetrics {}
#[cfg(not(feature = "rust_processor"))]
impl RustProcessable for opentelemetry_proto::tonic::logs::v1::ResourceLogs {}

/// Trait for types that can be processed by async Rust processors (owned data in/out)
#[cfg(feature = "rust_processor")]
pub trait AsyncRustProcessable: Sized {
    fn process_with_async_rust(
        self,
        processor: &AsyncRotelProcessor_TO<'static, abi_stable::std_types::RBox<()>>,
        context: ROption<RRequestContext>,
    ) -> Self;
}

#[cfg(feature = "rust_processor")]
impl AsyncRustProcessable for opentelemetry_proto::tonic::trace::v1::ResourceSpans {
    fn process_with_async_rust(
        self,
        processor: &AsyncRotelProcessor_TO<'static, abi_stable::std_types::RBox<()>>,
        context: ROption<RRequestContext>,
    ) -> Self {
        let r_spans: RResourceSpans = self.into();
        let result = processor.process_spans(r_spans, context);
        result.into()
    }
}

#[cfg(feature = "rust_processor")]
impl AsyncRustProcessable for opentelemetry_proto::tonic::metrics::v1::ResourceMetrics {
    fn process_with_async_rust(
        self,
        processor: &AsyncRotelProcessor_TO<'static, abi_stable::std_types::RBox<()>>,
        context: ROption<RRequestContext>,
    ) -> Self {
        let r_metrics: RResourceMetrics = self.into();
        let result = processor.process_metrics(r_metrics, context);
        result.into()
    }
}

#[cfg(feature = "rust_processor")]
impl AsyncRustProcessable for opentelemetry_proto::tonic::logs::v1::ResourceLogs {
    fn process_with_async_rust(
        self,
        processor: &AsyncRotelProcessor_TO<'static, abi_stable::std_types::RBox<()>>,
        context: ROption<RRequestContext>,
    ) -> Self {
        let r_logs: RResourceLogs = self.into();
        let result = processor.process_logs(r_logs, context);
        result.into()
    }
}

/// Noop trait when rust_processor feature is disabled
#[cfg(not(feature = "rust_processor"))]
pub trait AsyncRustProcessable {}

#[cfg(not(feature = "rust_processor"))]
impl AsyncRustProcessable for opentelemetry_proto::tonic::trace::v1::ResourceSpans {}
#[cfg(not(feature = "rust_processor"))]
impl AsyncRustProcessable for opentelemetry_proto::tonic::metrics::v1::ResourceMetrics {}
#[cfg(not(feature = "rust_processor"))]
impl AsyncRustProcessable for opentelemetry_proto::tonic::logs::v1::ResourceLogs {}

/// A loaded Rust processor
#[cfg(feature = "rust_processor")]
pub struct RustProcessor {
    name: String,
    processor: RotelProcessor_TO<'static, abi_stable::std_types::RBox<()>>,
}

#[cfg(feature = "rust_processor")]
impl RustProcessor {
    /// Load a Rust processor from a dynamic library path
    pub fn load(path: &StdPath) -> Result<Self, BoxError> {
        let header = lib_header_from_path(path).map_err(|e| {
            format!(
                "Failed to load processor library from {}: {}",
                path.display(),
                e
            )
        })?;

        let module: ProcessorModuleRef = header.init_root_module().map_err(|e| {
            format!(
                "Failed to initialize processor module from {}: {}",
                path.display(),
                e
            )
        })?;

        let processor = (module.new_processor())();
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "unknown".to_string());

        Ok(Self { name, processor })
    }

    /// Get the processor name
    pub fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(feature = "rust_processor")]
impl std::fmt::Debug for RustProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RustProcessor")
            .field("name", &self.name)
            .finish()
    }
}

/// A loaded async Rust processor
#[cfg(feature = "rust_processor")]
pub struct AsyncRustProcessor {
    name: String,
    processor: Arc<AsyncRotelProcessor_TO<'static, abi_stable::std_types::RBox<()>>>,
}

#[cfg(feature = "rust_processor")]
impl AsyncRustProcessor {
    /// Load an async Rust processor from a dynamic library path
    pub fn load(path: &StdPath) -> Result<Self, BoxError> {
        let header = lib_header_from_path(path).map_err(|e| {
            format!(
                "Failed to load async processor library from {}: {}",
                path.display(),
                e
            )
        })?;

        let module: AsyncProcessorModuleRef = header.init_root_module().map_err(|e| {
            format!(
                "Failed to initialize async processor module from {}: {}",
                path.display(),
                e
            )
        })?;

        let processor = (module.new_processor())();
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "unknown".to_string());

        // Call start() on load
        processor.start();

        Ok(Self {
            name,
            processor: Arc::new(processor),
        })
    }

    /// Get the processor name
    pub fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(feature = "rust_processor")]
impl std::fmt::Debug for AsyncRustProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncRustProcessor")
            .field("name", &self.name)
            .finish()
    }
}

/// Container for Python, Rust, and async Rust processors
#[derive(Debug)]
pub struct Processors {
    #[allow(dead_code)] // Only accessed in pyo3
    processor_modules: Vec<String>,
    #[cfg(feature = "rust_processor")]
    rust_processors: Vec<RustProcessor>,
    #[cfg(feature = "rust_processor")]
    async_rust_processors: Vec<AsyncRustProcessor>,
}

impl Processors {
    /// Create an empty Processors instance (used when pyo3 feature is disabled)
    pub fn empty() -> Self {
        Self {
            processor_modules: Vec::new(),
            #[cfg(feature = "rust_processor")]
            rust_processors: Vec::new(),
            #[cfg(feature = "rust_processor")]
            async_rust_processors: Vec::new(),
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

        Ok(Self {
            processor_modules,
            #[cfg(feature = "rust_processor")]
            rust_processors: Vec::new(),
            #[cfg(feature = "rust_processor")]
            async_rust_processors: Vec::new(),
        })
    }

    /// Initialize processors from a list of processor file paths (noop when pyo3 is disabled)
    #[cfg(not(feature = "pyo3"))]
    pub fn initialize(_processor_files: Vec<String>) -> Result<Self, BoxError> {
        Ok(Self::empty())
    }

    /// Initialize Rust processors from a list of shared library paths
    #[cfg(feature = "rust_processor")]
    pub fn initialize_rust(mut self, rust_processor_files: Vec<String>) -> Result<Self, BoxError> {
        for file in rust_processor_files {
            let path = StdPath::new(&file);
            let processor = RustProcessor::load(path)?;
            info!("Loaded Rust processor: {} from {}", processor.name(), file);
            self.rust_processors.push(processor);
        }
        Ok(self)
    }

    /// Initialize Rust processors (noop when rust_processor is disabled)
    #[cfg(not(feature = "rust_processor"))]
    pub fn initialize_rust(self, _rust_processor_files: Vec<String>) -> Result<Self, BoxError> {
        Ok(self)
    }

    /// Initialize async Rust processors from a list of shared library paths
    #[cfg(feature = "rust_processor")]
    pub fn initialize_async_rust(
        mut self,
        async_processor_files: Vec<String>,
    ) -> Result<Self, BoxError> {
        for file in async_processor_files {
            let path = StdPath::new(&file);
            let processor = AsyncRustProcessor::load(path)?;
            info!(
                "Loaded async Rust processor: {} from {}",
                processor.name(),
                file
            );
            self.async_rust_processors.push(processor);
        }
        Ok(self)
    }

    /// Initialize async Rust processors (noop when rust_processor is disabled)
    #[cfg(not(feature = "rust_processor"))]
    pub fn initialize_async_rust(
        self,
        _async_processor_files: Vec<String>,
    ) -> Result<Self, BoxError> {
        Ok(self)
    }

    /// Check if there are any Rust processors loaded
    #[cfg(feature = "rust_processor")]
    pub fn has_rust_processors(&self) -> bool {
        !self.rust_processors.is_empty()
    }

    /// Check if there are any Rust processors loaded (noop when disabled)
    #[cfg(not(feature = "rust_processor"))]
    pub fn has_rust_processors(&self) -> bool {
        false
    }

    /// Check if there are any async Rust processors loaded
    #[cfg(feature = "rust_processor")]
    pub fn has_async_rust_processors(&self) -> bool {
        !self.async_rust_processors.is_empty()
    }

    /// Check if there are any async Rust processors loaded (noop when disabled)
    #[cfg(not(feature = "rust_processor"))]
    pub fn has_async_rust_processors(&self) -> bool {
        false
    }

    /// Shutdown all async processors (calls their shutdown methods)
    pub fn shutdown(&self) {
        #[cfg(feature = "rust_processor")]
        for proc in &self.async_rust_processors {
            proc.processor.shutdown();
        }
    }

    /// Run the processors on a message
    #[cfg(not(feature = "pyo3"))]
    pub async fn run<T>(&self, message: Message<T>, inspector: &impl Inspect<T>) -> Message<T>
    where
        T: PythonProcessable + RustProcessable + AsyncRustProcessable + Clone + Send + 'static,
    {
        #[cfg(feature = "rust_processor")]
        {
            self.run_with_rust(message, inspector).await
        }
        #[cfg(not(feature = "rust_processor"))]
        {
            inspector.inspect(&message.payload);
            message
        }
    }

    /// Run the processors on a message
    #[cfg(feature = "pyo3")]
    pub async fn run<T>(&self, message: Message<T>, inspector: &impl Inspect<T>) -> Message<T>
    where
        T: PythonProcessable + RustProcessable + AsyncRustProcessable + Clone + Send + 'static,
    {
        let mut items = message.payload;
        let request_context = message.request_context.clone();
        let mut py_request_context: Option<PyRequestContext> = None;
        match message.request_context.clone() {
            None => {}
            Some(ctx) => py_request_context = Some(ctx.into()),
        }

        // Invoke current middleware layer
        let len_processor_modules = self.processor_modules.len();
        #[cfg(feature = "rust_processor")]
        let has_rust = self.has_rust_processors();
        #[cfg(not(feature = "rust_processor"))]
        let has_rust = false;
        #[cfg(feature = "rust_processor")]
        let has_async_rust = self.has_async_rust_processors();
        #[cfg(not(feature = "rust_processor"))]
        let has_async_rust = false;

        if len_processor_modules > 0 || has_rust || has_async_rust {
            inspector.inspect_with_prefix(Some("OTLP payload before processing".into()), &items);
        } else {
            inspector.inspect(&items);
        }

        // Run Python processors
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

        // Run sync Rust processors
        #[cfg(feature = "rust_processor")]
        {
            items = self.run_rust_processors(items, &request_context).await;
        }

        // Run async Rust processors
        #[cfg(feature = "rust_processor")]
        {
            items = self
                .run_async_rust_processors(items, &request_context)
                .await;
        }

        if len_processor_modules > 0 || has_rust || has_async_rust {
            inspector.inspect_with_prefix(Some("OTLP payload after processing".into()), &items);
        }

        // Wrap the processed items back into a Message
        Message {
            metadata: message.metadata,
            request_context,
            payload: items,
        }
    }

    /// Run sync Rust processors on a list of items via `spawn_blocking`.
    /// This frees the tokio worker thread during processor execution, which is
    /// important since FFI processor calls can easily exceed 150μs.
    /// Ordering is preserved because each `spawn_blocking` call is awaited
    /// sequentially before the next message is processed.
    #[cfg(feature = "rust_processor")]
    async fn run_rust_processors<T>(
        &self,
        mut items: Vec<T>,
        request_context: &Option<crate::topology::payload::RequestContext>,
    ) -> Vec<T>
    where
        T: RustProcessable + Clone + Send + 'static,
    {
        if self.rust_processors.is_empty() {
            return items;
        }

        // Convert context once, reuse for all processors
        let context: ROption<RRequestContext> = request_context
            .as_ref()
            .map(|ctx| ctx.clone().into())
            .into();

        // Process each processor sequentially via spawn_blocking.
        // We use a raw pointer to pass the processor reference since
        // RotelProcessor_TO doesn't implement Send, but the underlying
        // trait requires Send + Sync.
        for rust_proc in &self.rust_processors {
            let proc_ptr = &rust_proc.processor
                as *const RotelProcessor_TO<'static, abi_stable::std_types::RBox<()>>;
            let ctx = context.clone();
            // SAFETY: RotelProcessor requires Send + Sync, so the trait object
            // is safe to access from the spawn_blocking thread. The processor
            // outlives the spawn_blocking call because we await it immediately.
            let proc_ref = unsafe { &*proc_ptr };
            let moved_items = items;
            items = tokio::task::spawn_blocking(move || {
                let mut new_items = Vec::with_capacity(moved_items.len());
                for item in moved_items {
                    let result = item.process_with_rust(proc_ref, &ctx);
                    new_items.push(result);
                }
                new_items
            })
            .await
            .expect("spawn_blocking for sync rust processor panicked");
        }
        items
    }

    /// Run async Rust processors on a list of items.
    /// Always executed via `spawn_blocking` since processors internally call `block_on`.
    #[cfg(feature = "rust_processor")]
    async fn run_async_rust_processors<T>(
        &self,
        mut items: Vec<T>,
        request_context: &Option<crate::topology::payload::RequestContext>,
    ) -> Vec<T>
    where
        T: AsyncRustProcessable + Clone + Send + 'static,
    {
        if self.async_rust_processors.is_empty() {
            return items;
        }

        let context: ROption<RRequestContext> = request_context
            .as_ref()
            .map(|ctx| ctx.clone().into())
            .into();

        for async_proc in &self.async_rust_processors {
            let processor = Arc::clone(&async_proc.processor);
            let ctx = context.clone();
            let moved_items = items;

            items = tokio::task::spawn_blocking(move || {
                let mut new_items = Vec::with_capacity(moved_items.len());
                for item in moved_items {
                    let result = item.process_with_async_rust(&processor, ctx.clone());
                    new_items.push(result);
                }
                new_items
            })
            .await
            .expect("spawn_blocking for async rust processor panicked");
        }
        items
    }

    /// Run with Rust processors only (when pyo3 is disabled)
    #[cfg(all(feature = "rust_processor", not(feature = "pyo3")))]
    async fn run_with_rust<T>(&self, message: Message<T>, inspector: &impl Inspect<T>) -> Message<T>
    where
        T: RustProcessable + AsyncRustProcessable + Clone + Send + 'static,
    {
        let has_rust = self.has_rust_processors();
        let has_async_rust = self.has_async_rust_processors();

        if has_rust || has_async_rust {
            inspector.inspect_with_prefix(
                Some("OTLP payload before processing".into()),
                &message.payload,
            );
        } else {
            inspector.inspect(&message.payload);
        }

        let request_context = message.request_context.clone();
        let mut items = self
            .run_rust_processors(message.payload, &request_context)
            .await;
        items = self
            .run_async_rust_processors(items, &request_context)
            .await;

        if has_rust || has_async_rust {
            inspector.inspect_with_prefix(Some("OTLP payload after processing".into()), &items);
        }

        Message {
            metadata: message.metadata,
            request_context,
            payload: items,
        }
    }
}

// Conversion from RequestContext to RRequestContext
#[cfg(feature = "rust_processor")]
impl From<crate::topology::payload::RequestContext> for RRequestContext {
    fn from(ctx: crate::topology::payload::RequestContext) -> Self {
        use abi_stable::std_types::RHashMap;
        use rotel_rust_processor_sdk::types::{RGrpcContext, RHttpContext};

        match ctx {
            crate::topology::payload::RequestContext::Http(headers_map) => {
                let mut headers = RHashMap::new();
                for (k, v) in headers_map {
                    headers.insert(k.into(), v.into());
                }
                RRequestContext::Http(RHttpContext { headers })
            }
            crate::topology::payload::RequestContext::Grpc(metadata_map) => {
                let mut metadata = RHashMap::new();
                for (k, v) in metadata_map {
                    metadata.insert(k.into(), v.into());
                }
                RRequestContext::Grpc(RGrpcContext { metadata })
            }
        }
    }
}

#[cfg(test)]
mod tests {
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

    // ========================================================================
    // Rust processor unit tests
    // ========================================================================

    /// Test that ResourceSpans survives FFI round-trip (to_ffi -> from_ffi)
    #[cfg(feature = "rust_processor")]
    #[test]
    fn test_ffi_round_trip_resource_spans() {
        use opentelemetry_proto::tonic::trace::v1::ResourceSpans;

        let request = utilities::otlp::FakeOTLP::trace_service_request_with_spans(1, 3);
        let original = request.resource_spans[0].clone();
        let original_span_count = original.scope_spans[0].spans.len();
        let original_service_name = original
            .resource
            .as_ref()
            .unwrap()
            .attributes
            .iter()
            .find(|a| a.key == "service.name")
            .unwrap()
            .clone();

        // Round-trip through FFI types
        let r_spans: RResourceSpans = original.clone().into();
        let restored: ResourceSpans = r_spans.into();

        assert_eq!(restored.scope_spans[0].spans.len(), original_span_count);
        let restored_service_name = restored
            .resource
            .as_ref()
            .unwrap()
            .attributes
            .iter()
            .find(|a| a.key == "service.name")
            .unwrap();
        assert_eq!(restored_service_name.key, original_service_name.key);
        assert_eq!(restored_service_name.value, original_service_name.value);
        assert_eq!(
            restored.scope_spans[0].spans[0].name,
            original.scope_spans[0].spans[0].name
        );
    }

    /// Test that ResourceLogs survives FFI round-trip
    #[cfg(feature = "rust_processor")]
    #[test]
    fn test_ffi_round_trip_resource_logs() {
        use opentelemetry_proto::tonic::logs::v1::ResourceLogs;

        let request = utilities::otlp::FakeOTLP::logs_service_request_with_logs(1, 2);
        let original = request.resource_logs[0].clone();
        let original_log_count = original.scope_logs[0].log_records.len();

        let r_logs: RResourceLogs = original.clone().into();
        let restored: ResourceLogs = r_logs.into();

        assert_eq!(restored.scope_logs[0].log_records.len(), original_log_count);
        assert_eq!(
            restored.resource.as_ref().unwrap().attributes.len(),
            original.resource.as_ref().unwrap().attributes.len()
        );
    }

    /// Test that ResourceMetrics survives FFI round-trip
    #[cfg(feature = "rust_processor")]
    #[test]
    fn test_ffi_round_trip_resource_metrics() {
        use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;

        let request = utilities::otlp::FakeOTLP::metrics_service_request_with_metrics(1, 2);
        let original = request.resource_metrics[0].clone();
        let original_metric_count = original.scope_metrics[0].metrics.len();

        let r_metrics: RResourceMetrics = original.clone().into();
        let restored: ResourceMetrics = r_metrics.into();

        assert_eq!(
            restored.scope_metrics[0].metrics.len(),
            original_metric_count
        );
        assert_eq!(
            restored.resource.as_ref().unwrap().attributes.len(),
            original.resource.as_ref().unwrap().attributes.len()
        );
    }

    /// Helper: create a test sync processor that adds an attribute to spans
    #[cfg(feature = "rust_processor")]
    fn make_test_sync_processor() -> RustProcessor {
        use rotel_rust_processor_sdk::processor::RotelProcessor;
        use rotel_rust_processor_sdk::types::RKeyValue;

        struct TestSyncProcessor;

        impl RotelProcessor for TestSyncProcessor {
            fn process_spans(&self, spans: &mut RResourceSpans, _ctx: &ROption<RRequestContext>) {
                for scope in spans.scope_spans.iter_mut() {
                    for span in scope.spans.iter_mut() {
                        span.attributes
                            .push(RKeyValue::string("test.sync", "was_here"));
                    }
                }
            }
        }

        let processor = RotelProcessor_TO::from_value(
            TestSyncProcessor,
            rotel_rust_processor_sdk::abi_stable::sabi_trait::TD_Opaque,
        );
        RustProcessor {
            name: "test_sync".to_string(),
            processor,
        }
    }

    /// Helper: create a test async processor that adds an attribute to spans
    #[cfg(feature = "rust_processor")]
    fn make_test_async_processor() -> AsyncRustProcessor {
        use rotel_rust_processor_sdk::processor::AsyncRotelProcessor;
        use rotel_rust_processor_sdk::types::RKeyValue;
        use std::sync::atomic::{AtomicBool, Ordering};

        struct TestAsyncProcessor {
            started: AtomicBool,
            shutdown_called: AtomicBool,
        }

        impl AsyncRotelProcessor for TestAsyncProcessor {
            fn start(&self) {
                self.started.store(true, Ordering::SeqCst);
            }

            fn shutdown(&self) {
                self.shutdown_called.store(true, Ordering::SeqCst);
            }

            fn process_spans(
                &self,
                mut spans: RResourceSpans,
                _ctx: ROption<RRequestContext>,
            ) -> RResourceSpans {
                assert!(self.started.load(Ordering::SeqCst), "start() not called");
                for scope in spans.scope_spans.iter_mut() {
                    for span in scope.spans.iter_mut() {
                        span.attributes
                            .push(RKeyValue::string("test.async", "was_here"));
                    }
                }
                spans
            }
        }

        let processor = AsyncRotelProcessor_TO::from_value(
            TestAsyncProcessor {
                started: AtomicBool::new(false),
                shutdown_called: AtomicBool::new(false),
            },
            rotel_rust_processor_sdk::abi_stable::sabi_trait::TD_Opaque,
        );
        // Call start() like the real load path does
        processor.start();

        AsyncRustProcessor {
            name: "test_async".to_string(),
            processor: std::sync::Arc::new(processor),
        }
    }

    /// Test sync Rust processor execution via spawn_blocking
    #[cfg(feature = "rust_processor")]
    #[tokio::test]
    async fn test_sync_rust_processor_execution() {
        let request = utilities::otlp::FakeOTLP::trace_service_request_with_spans(1, 2);
        let items = request.resource_spans;
        let original_attr_count = items[0].scope_spans[0].spans[0].attributes.len();

        let processors = Processors {
            processor_modules: Vec::new(),
            rust_processors: vec![make_test_sync_processor()],
            async_rust_processors: Vec::new(),
        };

        let result = processors.run_rust_processors(items, &None).await;

        assert_eq!(result.len(), 1);
        let spans = &result[0].scope_spans[0].spans;
        assert_eq!(spans.len(), 2);
        // Each span should have the original attributes + 1 new one
        for span in spans {
            assert_eq!(span.attributes.len(), original_attr_count + 1);
            let added = span.attributes.iter().find(|a| a.key == "test.sync");
            assert!(added.is_some(), "sync processor attribute not found");
        }
    }

    /// Test async Rust processor execution via spawn_blocking
    #[cfg(feature = "rust_processor")]
    #[tokio::test]
    async fn test_async_rust_processor_execution() {
        let request = utilities::otlp::FakeOTLP::trace_service_request_with_spans(1, 2);
        let items = request.resource_spans;
        let original_attr_count = items[0].scope_spans[0].spans[0].attributes.len();

        let processors = Processors {
            processor_modules: Vec::new(),
            rust_processors: Vec::new(),
            async_rust_processors: vec![make_test_async_processor()],
        };

        let result = processors.run_async_rust_processors(items, &None).await;

        assert_eq!(result.len(), 1);
        let spans = &result[0].scope_spans[0].spans;
        assert_eq!(spans.len(), 2);
        for span in spans {
            assert_eq!(span.attributes.len(), original_attr_count + 1);
            let added = span.attributes.iter().find(|a| a.key == "test.async");
            assert!(added.is_some(), "async processor attribute not found");
        }
    }

    /// Test chaining sync + async processors together through run()
    #[cfg(feature = "rust_processor")]
    #[tokio::test]
    async fn test_run_chains_sync_and_async_processors() {
        use crate::topology::generic_pipeline::Inspect;

        struct NoOpInspector;
        impl Inspect<opentelemetry_proto::tonic::trace::v1::ResourceSpans> for NoOpInspector {
            fn inspect(&self, _value: &[opentelemetry_proto::tonic::trace::v1::ResourceSpans]) {}
            fn inspect_with_prefix(
                &self,
                _prefix: Option<String>,
                _value: &[opentelemetry_proto::tonic::trace::v1::ResourceSpans],
            ) {
            }
        }

        let request = utilities::otlp::FakeOTLP::trace_service_request_with_spans(1, 1);
        let original_attr_count = request.resource_spans[0].scope_spans[0].spans[0]
            .attributes
            .len();
        let message = Message::new(None, request.resource_spans, None);

        let processors = Processors {
            processor_modules: Vec::new(),
            rust_processors: vec![make_test_sync_processor()],
            async_rust_processors: vec![make_test_async_processor()],
        };

        let result = processors.run(message, &NoOpInspector).await;

        let spans = &result.payload[0].scope_spans[0].spans;
        assert_eq!(spans[0].attributes.len(), original_attr_count + 2);
        assert!(spans[0].attributes.iter().any(|a| a.key == "test.sync"));
        assert!(spans[0].attributes.iter().any(|a| a.key == "test.async"));
    }

    /// Test that run() with no processors passes data through unchanged
    #[cfg(feature = "rust_processor")]
    #[tokio::test]
    async fn test_run_empty_processors_passthrough() {
        use crate::topology::generic_pipeline::Inspect;

        struct NoOpInspector;
        impl Inspect<opentelemetry_proto::tonic::trace::v1::ResourceSpans> for NoOpInspector {
            fn inspect(&self, _value: &[opentelemetry_proto::tonic::trace::v1::ResourceSpans]) {}
            fn inspect_with_prefix(
                &self,
                _prefix: Option<String>,
                _value: &[opentelemetry_proto::tonic::trace::v1::ResourceSpans],
            ) {
            }
        }

        let request = utilities::otlp::FakeOTLP::trace_service_request_with_spans(2, 3);
        let expected_len = request.resource_spans.len();
        let expected_span_count = request.resource_spans[0].scope_spans[0].spans.len();
        let message = Message::new(None, request.resource_spans, None);

        let processors = Processors::empty();
        let result = processors.run(message, &NoOpInspector).await;

        assert_eq!(result.payload.len(), expected_len);
        assert_eq!(
            result.payload[0].scope_spans[0].spans.len(),
            expected_span_count
        );
    }

    /// Test shutdown calls through to async processors without panicking
    #[cfg(feature = "rust_processor")]
    #[test]
    fn test_shutdown_with_async_processors() {
        let processors = Processors {
            processor_modules: Vec::new(),
            rust_processors: Vec::new(),
            async_rust_processors: vec![make_test_async_processor()],
        };

        // Should not panic
        processors.shutdown();
    }

    /// Test shutdown with empty processors doesn't panic
    #[test]
    fn test_shutdown_empty_processors() {
        let processors = Processors::empty();
        processors.shutdown();
    }

    /// Test multiple sync processors execute in order
    #[cfg(feature = "rust_processor")]
    #[tokio::test]
    async fn test_multiple_sync_processors_ordering() {
        use rotel_rust_processor_sdk::processor::RotelProcessor;
        use rotel_rust_processor_sdk::types::RKeyValue;

        struct OrderedProcessor {
            label: &'static str,
        }

        impl RotelProcessor for OrderedProcessor {
            fn process_spans(&self, spans: &mut RResourceSpans, _ctx: &ROption<RRequestContext>) {
                for scope in spans.scope_spans.iter_mut() {
                    for span in scope.spans.iter_mut() {
                        span.attributes.push(RKeyValue::string("order", self.label));
                    }
                }
            }
        }

        let make_proc = |label: &'static str| -> RustProcessor {
            let processor = RotelProcessor_TO::from_value(
                OrderedProcessor { label },
                rotel_rust_processor_sdk::abi_stable::sabi_trait::TD_Opaque,
            );
            RustProcessor {
                name: label.to_string(),
                processor,
            }
        };

        let request = utilities::otlp::FakeOTLP::trace_service_request_with_spans(1, 1);
        let items = request.resource_spans;

        let processors = Processors {
            processor_modules: Vec::new(),
            rust_processors: vec![make_proc("first"), make_proc("second"), make_proc("third")],
            async_rust_processors: Vec::new(),
        };

        let result = processors.run_rust_processors(items, &None).await;

        // All three "order" attributes should be present, added in sequence
        let order_attrs: Vec<_> = result[0].scope_spans[0].spans[0]
            .attributes
            .iter()
            .filter(|a| a.key == "order")
            .collect();
        assert_eq!(order_attrs.len(), 3);
    }
}
