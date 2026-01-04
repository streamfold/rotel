// SPDX-License-Identifier: Apache-2.0

// Utility to generate OTLP telemetry data files for testing and debugging

use bytes::Bytes;
use clap::{Args, Parser, Subcommand, ValueEnum};
use http::header::{CONTENT_TYPE, HeaderValue};
use http::{Method, Request};
use http_body_util::Full;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use prost::Message;
use std::fs;
use std::path::PathBuf;
use std::process::ExitCode;
use std::str::FromStr;
use tokio;
use utilities::otlp::FakeOTLP;

#[derive(Debug, Parser)]
#[command(name = "generate-otlp")]
#[command(bin_name = "generate-otlp")]
#[command(version, about = "Generate OTLP telemetry data files for testing")]
struct Arguments {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Generate trace data
    Traces(GenerateArgs),
    /// Generate metrics data
    Metrics(GenerateArgs),
    /// Generate logs data
    Logs(GenerateArgs),
    /// Return version
    Version,
}

#[derive(Debug, Clone, ValueEnum)]
enum Protocol {
    Http,
    Grpc,
}

#[derive(Debug, Args)]
struct GenerateArgs {
    /// Write to file instead of sending to endpoint
    #[arg(long)]
    file: Option<PathBuf>,

    /// Protocol to use when sending (ignored if --file is set)
    #[arg(short, long, value_enum, default_value = "http")]
    protocol: Protocol,

    /// Endpoint address (host:port) for gRPC (default: 0.0.0.0:4317)
    #[arg(long, default_value = "0.0.0.0:4317")]
    grpc_endpoint: String,

    /// Endpoint address (host:port) for HTTP (default: 0.0.0.0:4318)
    #[arg(long, default_value = "0.0.0.0:4318")]
    http_endpoint: String,

    /// Number of resource spans/metrics/logs to generate
    #[arg(short, long, default_value = "1")]
    resources: usize,

    /// Number of spans/metrics/logs per resource
    #[arg(short, long, default_value = "1")]
    items: usize,

    /// Include dummy test headers (my-custom-header, another-header) for testing header context
    #[arg(long)]
    include_headers: bool,
}

fn main() -> ExitCode {
    let args = Arguments::parse();

    match args.command {
        Commands::Version => {
            println!("generate-otlp {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        Commands::Traces(gen_args) => {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(generate_traces(gen_args))
        }
        Commands::Metrics(gen_args) => {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(generate_metrics(gen_args))
        }
        Commands::Logs(gen_args) => {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(generate_logs(gen_args))
        }
    }
}

async fn generate_traces(args: GenerateArgs) -> ExitCode {
    let trace_req = FakeOTLP::trace_service_request_with_spans(args.resources, args.items);

    if let Some(file_path) = args.file {
        // Write to file
        let mut buf = Vec::with_capacity(trace_req.encoded_len());
        if let Err(e) = trace_req.encode(&mut buf) {
            eprintln!("Failed to encode trace request: {}", e);
            return ExitCode::FAILURE;
        }

        if let Err(e) = fs::write(&file_path, &buf) {
            eprintln!(
                "Failed to write trace file to {}: {}",
                file_path.display(),
                e
            );
            return ExitCode::FAILURE;
        }

        println!(
            "✓ Trace file created at {} ({} bytes, {} resource spans, {} spans total)",
            file_path.display(),
            buf.len(),
            args.resources,
            args.resources * args.items
        );
        ExitCode::SUCCESS
    } else {
        // Send to endpoint
        send_traces(trace_req, &args).await
    }
}

async fn generate_metrics(args: GenerateArgs) -> ExitCode {
    let metrics_req = FakeOTLP::metrics_service_request_with_metrics(args.resources, args.items);

    if let Some(file_path) = args.file {
        // Write to file
        let mut buf = Vec::with_capacity(metrics_req.encoded_len());
        if let Err(e) = metrics_req.encode(&mut buf) {
            eprintln!("Failed to encode metrics request: {}", e);
            return ExitCode::FAILURE;
        }

        if let Err(e) = fs::write(&file_path, &buf) {
            eprintln!(
                "Failed to write metrics file to {}: {}",
                file_path.display(),
                e
            );
            return ExitCode::FAILURE;
        }

        println!(
            "✓ Metrics file created at {} ({} bytes, {} resource metrics, {} metrics total)",
            file_path.display(),
            buf.len(),
            args.resources,
            args.resources * args.items
        );
        ExitCode::SUCCESS
    } else {
        // Send to endpoint
        send_metrics(metrics_req, &args).await
    }
}

async fn generate_logs(args: GenerateArgs) -> ExitCode {
    let logs_req = FakeOTLP::logs_service_request_with_logs(args.resources, args.items);

    if let Some(file_path) = args.file {
        // Write to file
        let mut buf = Vec::with_capacity(logs_req.encoded_len());
        if let Err(e) = logs_req.encode(&mut buf) {
            eprintln!("Failed to encode logs request: {}", e);
            return ExitCode::FAILURE;
        }

        if let Err(e) = fs::write(&file_path, &buf) {
            eprintln!(
                "Failed to write logs file to {}: {}",
                file_path.display(),
                e
            );
            return ExitCode::FAILURE;
        }

        println!(
            "✓ Logs file created at {} ({} bytes, {} resource logs, {} log records total)",
            file_path.display(),
            buf.len(),
            args.resources,
            args.resources * args.items
        );
        ExitCode::SUCCESS
    } else {
        // Send to endpoint
        send_logs(logs_req, &args).await
    }
}

// Helper functions to send telemetry data

async fn send_traces(
    trace_req: opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest,
    args: &GenerateArgs,
) -> ExitCode {
    match args.protocol {
        Protocol::Http => {
            send_http(
                trace_req,
                &format!("http://{}/v1/traces", args.http_endpoint),
                "traces",
                args.include_headers,
            )
            .await
        }
        Protocol::Grpc => {
            send_grpc(
                trace_req,
                &format!("http://{}", args.grpc_endpoint),
                "/opentelemetry.proto.collector.trace.v1.TraceService/Export",
                "traces",
                args.include_headers,
            )
            .await
        }
    }
}

async fn send_metrics(
    metrics_req: opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest,
    args: &GenerateArgs,
) -> ExitCode {
    match args.protocol {
        Protocol::Http => {
            send_http(
                metrics_req,
                &format!("http://{}/v1/metrics", args.http_endpoint),
                "metrics",
                args.include_headers,
            )
            .await
        }
        Protocol::Grpc => {
            send_grpc(
                metrics_req,
                &format!("http://{}", args.grpc_endpoint),
                "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export",
                "metrics",
                args.include_headers,
            )
            .await
        }
    }
}

async fn send_logs(
    logs_req: opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest,
    args: &GenerateArgs,
) -> ExitCode {
    match args.protocol {
        Protocol::Http => {
            send_http(
                logs_req,
                &format!("http://{}/v1/logs", args.http_endpoint),
                "logs",
                args.include_headers,
            )
            .await
        }
        Protocol::Grpc => {
            send_grpc(
                logs_req,
                &format!("http://{}", args.grpc_endpoint),
                "/opentelemetry.proto.collector.logs.v1.LogsService/Export",
                "logs",
                args.include_headers,
            )
            .await
        }
    }
}

async fn send_http<T: prost::Message>(
    msg: T,
    url: &str,
    telemetry_type: &str,
    include_headers: bool,
) -> ExitCode {
    let client: Client<HttpConnector, Full<Bytes>> =
        Client::builder(TokioExecutor::new()).build_http();

    let mut buf = Vec::with_capacity(msg.encoded_len());
    if let Err(e) = msg.encode(&mut buf) {
        eprintln!("Failed to encode {} request: {}", telemetry_type, e);
        return ExitCode::FAILURE;
    }

    let uri = match http::Uri::from_str(url) {
        Ok(uri) => uri,
        Err(e) => {
            eprintln!("Invalid URL {}: {}", url, e);
            return ExitCode::FAILURE;
        }
    };

    let mut req_builder = Request::builder().method(Method::POST).uri(uri).header(
        CONTENT_TYPE,
        HeaderValue::from_static("application/x-protobuf"),
    );

    // Add dummy test headers if requested
    if include_headers {
        let headers = FakeOTLP::example_headers();
        for (key, value) in headers {
            if let Ok(header_value) = HeaderValue::from_str(&value) {
                req_builder = req_builder.header(key, header_value);
            }
        }
    }

    let req = match req_builder.body(Full::new(Bytes::from(buf))) {
        Ok(req) => req,
        Err(e) => {
            eprintln!("Failed to build HTTP request: {}", e);
            return ExitCode::FAILURE;
        }
    };

    match client.request(req).await {
        Ok(resp) => {
            let status = resp.status();
            if status.is_success() {
                println!(
                    "✓ Successfully sent {} to {} (status: {})",
                    telemetry_type, url, status
                );
                ExitCode::SUCCESS
            } else {
                eprintln!(
                    "✗ Failed to send {} to {} (status: {})",
                    telemetry_type, url, status
                );
                ExitCode::FAILURE
            }
        }
        Err(e) => {
            eprintln!("✗ Failed to send {} to {}: {}", telemetry_type, url, e);
            ExitCode::FAILURE
        }
    }
}

async fn send_grpc<T: prost::Message>(
    msg: T,
    base_url: &str,
    service_path: &str,
    telemetry_type: &str,
    include_headers: bool,
) -> ExitCode {
    use bytes::BufMut;
    use bytes::BytesMut;

    // Encode as gRPC (with 5-byte header)
    const GRPC_HEADER_SIZE: usize = 5;
    let mut uncompressed = BytesMut::with_capacity(1024);
    if let Err(e) = msg.encode(&mut uncompressed) {
        eprintln!("Failed to encode {} request: {}", telemetry_type, e);
        return ExitCode::FAILURE;
    }

    let mut buf = BytesMut::with_capacity(GRPC_HEADER_SIZE + uncompressed.len());
    buf.put_u8(0); // compression flag (0 = uncompressed)
    buf.put_u32(uncompressed.len() as u32);
    buf.put_slice(&uncompressed);

    let client: Client<HttpConnector, Full<Bytes>> =
        Client::builder(TokioExecutor::new()).build_http();

    let url = format!("{}{}", base_url, service_path);
    let uri = match http::Uri::from_str(&url) {
        Ok(uri) => uri,
        Err(e) => {
            eprintln!("Invalid URL {}: {}", url, e);
            return ExitCode::FAILURE;
        }
    };

    let mut req_builder = Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header(CONTENT_TYPE, HeaderValue::from_static("application/grpc"));

    // Add dummy test headers (gRPC metadata) if requested
    if include_headers {
        let headers = FakeOTLP::example_headers();
        for (key, value) in headers {
            if let Ok(header_value) = HeaderValue::from_str(&value) {
                req_builder = req_builder.header(key, header_value);
            }
        }
    }

    let req = match req_builder.body(Full::new(buf.freeze())) {
        Ok(req) => req,
        Err(e) => {
            eprintln!("Failed to build gRPC request: {}", e);
            return ExitCode::FAILURE;
        }
    };

    match client.request(req).await {
        Ok(resp) => {
            let status = resp.status();
            if status.is_success() {
                println!(
                    "✓ Successfully sent {} to {} (status: {})",
                    telemetry_type, url, status
                );
                ExitCode::SUCCESS
            } else {
                eprintln!(
                    "✗ Failed to send {} to {} (status: {})",
                    telemetry_type, url, status
                );
                ExitCode::FAILURE
            }
        }
        Err(e) => {
            eprintln!("✗ Failed to send {} to {}: {}", telemetry_type, url, e);
            ExitCode::FAILURE
        }
    }
}
