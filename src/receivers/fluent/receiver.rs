// SPDX-License-Identifier: Apache-2.0

use crate::receivers::fluent::config::FluentReceiverConfig;
use crate::receivers::fluent::error::{FluentReceiverError, Result};
use crate::receivers::fluent::message::Message;
use crate::receivers::otlp_output::OTLPOutput;
use crate::topology::payload;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use rmpv::Value;
use std::error::Error;
use std::fs;
use std::io::Cursor;
use tokio::io::AsyncReadExt;
use tokio::net::{UnixListener, UnixStream};
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024; // 16MB max message size

pub struct FluentReceiver {
    config: FluentReceiverConfig,
    traces_output: Option<OTLPOutput<payload::Message<ResourceSpans>>>,
    metrics_output: Option<OTLPOutput<payload::Message<ResourceMetrics>>>,
    logs_output: Option<OTLPOutput<payload::Message<ResourceLogs>>>,
    listener: UnixListener,
}

#[derive(Clone)]
struct ConnectionHandler {
    traces_output: Option<OTLPOutput<payload::Message<ResourceSpans>>>,
    metrics_output: Option<OTLPOutput<payload::Message<ResourceMetrics>>>,
    logs_output: Option<OTLPOutput<payload::Message<ResourceLogs>>>,
}

impl FluentReceiver {
    pub fn new(
        config: FluentReceiverConfig,
        traces_output: Option<OTLPOutput<payload::Message<ResourceSpans>>>,
        metrics_output: Option<OTLPOutput<payload::Message<ResourceMetrics>>>,
        logs_output: Option<OTLPOutput<payload::Message<ResourceLogs>>>,
    ) -> Result<Self> {
        // Validate that at least one signal type is enabled
        if !config.traces && !config.metrics && !config.logs {
            return Err(FluentReceiverError::ConfigurationError(
                "At least one signal type (traces, metrics, or logs) must be enabled".to_string(),
            ));
        }

        // Remove the socket file if it already exists
        let socket_path = &config.socket_path;
        if socket_path.exists() {
            fs::remove_file(socket_path).map_err(|e| {
                FluentReceiverError::SocketBindError(format!(
                    "Failed to remove existing socket file {}: {}",
                    socket_path.display(),
                    e
                ))
            })?;
        }

        // Create parent directories if they don't exist
        if let Some(parent) = socket_path.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent).map_err(|e| {
                    FluentReceiverError::SocketBindError(format!(
                        "Failed to create parent directories for {}: {}",
                        socket_path.display(),
                        e
                    ))
                })?;
            }
        }

        // Bind to the UNIX socket
        let listener = UnixListener::bind(socket_path).map_err(|e| {
            FluentReceiverError::SocketBindError(format!(
                "Failed to bind to UNIX socket {}: {}",
                socket_path.display(),
                e
            ))
        })?;

        info!(
            socket_path = socket_path.display().to_string(),
            "Fluent receiver bound to UNIX socket"
        );

        Ok(Self {
            config,
            traces_output,
            metrics_output,
            logs_output,
            listener,
        })
    }

    fn create_handler(&self) -> ConnectionHandler {
        ConnectionHandler {
            traces_output: self.traces_output.clone(),
            metrics_output: self.metrics_output.clone(),
            logs_output: self.logs_output.clone(),
        }
    }
}

impl ConnectionHandler {
    async fn handle_connection(self, mut stream: UnixStream) {
        debug!("New connection accepted on Fluent receiver");

        let mut buffer = vec![0u8; MAX_MESSAGE_SIZE];

        loop {
            match stream.read(&mut buffer).await {
                Ok(0) => {
                    // Connection closed
                    debug!("Connection closed by peer");
                    break;
                }
                Ok(n) => {
                    debug!("Received {} bytes from Fluent socket", n);

                    // Process the received data
                    if let Err(e) = self.process_message(&buffer[..n]).await {
                        warn!("Failed to process message: {}", e);
                    }
                }
                Err(e) => {
                    error!("Error reading from socket: {}", e);
                    break;
                }
            }
        }

        debug!("Connection handler finished");
    }

    async fn process_message(&self, data: &[u8]) -> Result<()> {
        debug!("Processing message of {} bytes", data.len());

        // Try to decode as Message struct first
        match rmp_serde::from_slice::<Message>(data) {
            Ok(message) => {
                println!("Decoded Fluent Message: {:?}", message);
                // TODO: Convert to OTLP format and send to appropriate output channel
                return Ok(());
            }
            Err(e) => {
                error!("Failed to decode as Message struct: {}", e);
            }
        }

        // Fall back to generic MessagePack decoding
        let mut cursor = Cursor::new(data);
        match rmpv::decode::read_value(&mut cursor) {
            Ok(value) => {
                //println!("Decoded MessagePack message: {:#?}", value);

                // Log the structure in a more readable format
                println!("Received Fluent MessagePack");
                self.log_msgpack_structure(&value, 0);

                // TODO: Convert to OTLP format and send to appropriate output channel
                Ok(())
            }
            Err(e) => {
                // If MessagePack decoding fails, try to interpret as other formats
                warn!("Failed to decode as MessagePack: {}", e);

                // Try to decode as UTF-8 text
                match std::str::from_utf8(data) {
                    Ok(utf8_str) => {
                        println!("Received UTF-8 text: {}", utf8_str);
                    }
                    Err(_) => {
                        // Hex dump for binary data
                        println!("Received binary data, hex dump:");
                        for (i, chunk) in data.chunks(32).enumerate() {
                            let offset = i * 32;
                            let hex_part: String = chunk
                                .iter()
                                .map(|b| format!("{:02x}", b))
                                .collect::<Vec<_>>()
                                .join(" ");

                            println!("{:08x}  {}", offset, hex_part);
                        }
                    }
                }

                Err(FluentReceiverError::DeserializationError(format!(
                    "Failed to decode message: {}",
                    e
                )))
            }
        }
    }

    fn log_msgpack_structure(&self, value: &Value, indent: usize) {
        let prefix = "  ".repeat(indent);
        match value {
            Value::Nil => println!("{}Nil", prefix),
            Value::Boolean(b) => println!("{}Boolean: {}", prefix, b),
            Value::Integer(i) => println!("{}Integer: {}", prefix, i),
            Value::F32(f) => println!("{}F32: {}", prefix, f),
            Value::F64(f) => println!("{}F64: {}", prefix, f),
            Value::String(s) => {
                if let Some(utf8_str) = s.as_str() {
                    println!("{}String: \"{}\"", prefix, utf8_str);
                } else {
                    println!("{}String (binary): {:?}", prefix, s);
                }
            }
            Value::Binary(b) => {
                if b.len() <= 64 {
                    println!("{}Binary({} bytes): {:?}", prefix, b.len(), b);
                } else {
                    println!("{}Binary({} bytes): {:?}...", prefix, b.len(), &b[..64]);
                }
            }
            Value::Array(arr) => {
                println!("{}Array({} elements):", prefix, arr.len());
                for (i, item) in arr.iter().enumerate() {
                    println!("{}[{}]:", prefix, i);
                    self.log_msgpack_structure(item, indent + 1);
                }
            }
            Value::Map(map) => {
                println!("{}Map({} entries):", prefix, map.len());
                for (key, val) in map {
                    println!("{}Key:", prefix);
                    self.log_msgpack_structure(key, indent + 1);
                    println!("{}Value:", prefix);
                    self.log_msgpack_structure(val, indent + 1);
                }
            }
            Value::Ext(tag, data) => {
                println!(
                    "{}Ext(tag={}, {} bytes): {:?}",
                    prefix,
                    tag,
                    data.len(),
                    data
                );
            }
        }
    }
}

impl FluentReceiver {
    pub async fn run(
        &mut self,
        receivers_cancel: CancellationToken,
    ) -> std::result::Result<(), Box<dyn Error + Send + Sync>> {
        info!(
            socket_path = self.config.socket_path.display().to_string(),
            "Starting Fluent receiver"
        );

        loop {
            select! {
                result = self.listener.accept() => {
                    match result {
                        Ok((stream, _addr)) => {
                            debug!("Accepted new connection");

                            // Spawn a task to handle this connection
                            let handler = self.create_handler();
                            tokio::spawn(async move {
                                handler.handle_connection(stream).await;
                            });
                        }
                        Err(e) => {
                            error!("Error accepting connection: {}", e);
                        }
                    }
                }
                _ = receivers_cancel.cancelled() => {
                    println!("Fluent receiver shutting down");
                    break;
                }
            }
        }

        // Cleanup: remove the socket file
        if self.config.socket_path.exists() {
            if let Err(e) = fs::remove_file(&self.config.socket_path) {
                warn!(
                    "Failed to remove socket file {}: {}",
                    self.config.socket_path.display(),
                    e
                );
            }
        }

        Ok(())
    }
}

impl Drop for FluentReceiver {
    fn drop(&mut self) {
        // Ensure socket file is cleaned up
        if self.config.socket_path.exists() {
            let _ = fs::remove_file(&self.config.socket_path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_receiver_creation_with_valid_config() {
        let temp_dir = TempDir::new().unwrap();
        let socket_path = temp_dir.path().join("test.sock");

        let config = FluentReceiverConfig::new(socket_path.clone()).with_logs(true);

        let receiver = FluentReceiver::new(config, None, None, None);
        assert!(receiver.is_ok());

        // Cleanup
        drop(receiver);
        assert!(!socket_path.exists());
    }

    #[tokio::test]
    async fn test_receiver_creation_fails_without_signals() {
        let temp_dir = TempDir::new().unwrap();
        let socket_path = temp_dir.path().join("test.sock");

        let config = FluentReceiverConfig::new(socket_path)
            .with_traces(false)
            .with_metrics(false)
            .with_logs(false);

        let receiver = FluentReceiver::new(config, None, None, None);
        assert!(receiver.is_err());

        if let Err(FluentReceiverError::ConfigurationError(msg)) = receiver {
            assert!(msg.contains("At least one signal type"));
        } else {
            panic!("Expected ConfigurationError");
        }
    }

    #[tokio::test]
    async fn test_socket_cleanup_on_drop() {
        let temp_dir = TempDir::new().unwrap();
        let socket_path = temp_dir.path().join("test.sock");

        let config = FluentReceiverConfig::new(socket_path.clone()).with_logs(true);

        let receiver = FluentReceiver::new(config, None, None, None).unwrap();
        assert!(socket_path.exists());

        drop(receiver);
        assert!(!socket_path.exists());
    }

    #[tokio::test]
    async fn test_msgpack_decoding() {
        use rmpv::Value;

        let temp_dir = TempDir::new().unwrap();
        let socket_path = temp_dir.path().join("test.sock");

        let config = FluentReceiverConfig::new(socket_path.clone()).with_logs(true);
        let receiver = FluentReceiver::new(config, None, None, None).unwrap();
        let handler = receiver.create_handler();

        // Create a test MessagePack message
        // Fluentd forward protocol format: [tag, [[timestamp, record], ...]]
        let tag = Value::String("docker.container".into());
        let timestamp = Value::Integer(1234567890.into());
        let record = Value::Map(vec![
            (
                Value::String("log".into()),
                Value::String("Test log message".into()),
            ),
            (
                Value::String("container_id".into()),
                Value::String("abc123".into()),
            ),
        ]);
        let entry = Value::Array(vec![timestamp, record]);
        let entries = Value::Array(vec![entry]);
        let message = Value::Array(vec![tag, entries]);

        // Encode the message
        let mut buffer = Vec::new();
        rmpv::encode::write_value(&mut buffer, &message).unwrap();

        // Process the message
        let result = handler.process_message(&buffer).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_msgpack_simple_types() {
        let temp_dir = TempDir::new().unwrap();
        let socket_path = temp_dir.path().join("test.sock");

        let config = FluentReceiverConfig::new(socket_path.clone()).with_logs(true);
        let receiver = FluentReceiver::new(config, None, None, None).unwrap();
        let handler = receiver.create_handler();

        // Test various MessagePack types
        let test_values = vec![
            Value::Nil,
            Value::Boolean(true),
            Value::Integer(42.into()),
            Value::F64(3.14),
            Value::String("Hello, World!".into()),
            Value::Array(vec![
                Value::Integer(1.into()),
                Value::Integer(2.into()),
                Value::Integer(3.into()),
            ]),
            Value::Map(vec![
                (Value::String("key1".into()), Value::String("value1".into())),
                (Value::String("key2".into()), Value::Integer(123.into())),
            ]),
        ];

        for test_value in test_values {
            let mut buffer = Vec::new();
            rmpv::encode::write_value(&mut buffer, &test_value).unwrap();

            let result = handler.process_message(&buffer).await;
            assert!(result.is_ok());
        }
    }

    #[tokio::test]
    async fn test_invalid_msgpack_data() {
        let temp_dir = TempDir::new().unwrap();
        let socket_path = temp_dir.path().join("test.sock");

        let config = FluentReceiverConfig::new(socket_path.clone()).with_logs(true);
        let receiver = FluentReceiver::new(config, None, None, None).unwrap();
        let handler = receiver.create_handler();

        // Test with incomplete MessagePack data
        // This is an incomplete MessagePack array that should fail to decode
        let invalid_msgpack = vec![0x93]; // Array with 3 elements but no data following
        let result = handler.process_message(&invalid_msgpack).await;
        // This should fail because the MessagePack is incomplete
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_valid_msgpack_decodes_successfully() {
        let temp_dir = TempDir::new().unwrap();
        let socket_path = temp_dir.path().join("test.sock");

        let config = FluentReceiverConfig::new(socket_path.clone()).with_logs(true);
        let receiver = FluentReceiver::new(config, None, None, None).unwrap();
        let handler = receiver.create_handler();

        // Even some random bytes might decode as valid MessagePack
        // (e.g., 0xFF is a valid negative fixint in MessagePack)
        // So we just verify that valid MessagePack works
        let valid_msgpack = vec![0xc0]; // Nil in MessagePack
        let result = handler.process_message(&valid_msgpack).await;
        assert!(result.is_ok());
    }
}
