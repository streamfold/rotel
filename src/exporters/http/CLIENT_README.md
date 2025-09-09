# HTTP Client

This document describes the HTTP client implementation that combines the functionality of the previous `GrpcClient` and `HttpClient` into a single `Client`.

## Overview

The `Client` provides a single implementation that supports both gRPC and HTTP protocols through a `Protocol` enum. This simplifies the codebase by eliminating duplicate code while maintaining protocol-specific behavior where needed.

## Key Features

- **Single Implementation**: One client handles both HTTP and gRPC protocols
- **Protocol-Specific Behavior**: Automatically adapts behavior based on the selected protocol
- **Backward Compatibility**: Type aliases maintain existing API compatibility
- **Shared Infrastructure**: Common TLS, networking, and error handling code

## Protocol Differences

### gRPC Protocol
- Strict content encoding validation (only `None` and `Gzip` supported)
- Parses gRPC status from headers and trailers
- Returns gRPC errors for non-OK status codes
- Requires `Default` trait bound on response types for historical compatibility

### HTTP Protocol
- Flexible content encoding handling
- Attempts to decode error response bodies
- Warns about unexpected trailers
- More lenient error handling

## Usage

### Direct Usage
```rust
use crate::exporters::http::client::{Client, Protocol};
use crate::exporters::http::tls::Config;

// For HTTP
let http_client = Client::build(
    Config::default(), 
    Protocol::Http, 
    decoder
)?;

// For gRPC  
let grpc_client = Client::build(
    Config::default(), 
    Protocol::Grpc, 
    decoder
)?;
```

### Migration Required
All code must be updated to use the `Client` directly with the `Protocol` enum.

## Migration

### From HttpClient
```rust
// Before
use crate::exporters::http::http_client::HttpClient;
let client = HttpClient::build(tls_config, decoder)?;

// After  
use crate::exporters::http::client::{Client, Protocol};
let client = Client::build(tls_config, Protocol::Http, decoder)?;
```

### From GrpcClient
```rust
// Before
use crate::exporters::http::grpc_client::GrpcClient;
let client = GrpcClient::build(tls_config, decoder)?;

// After
use crate::exporters::http::client::{Client, Protocol};
let client = Client::build(tls_config, Protocol::Grpc, decoder)?;
```

## Implementation Details

### Shared Components
- TLS configuration and connection handling
- Request/response processing pipeline
- Tower Service trait implementation
- Error handling and retry logic

### Protocol-Specific Logic
- Content encoding validation
- Status code interpretation
- Trailer handling
- Error response processing

## Files

- `client.rs` - Main implementation (includes both `Client` and shared utilities)

The original client files have been removed. All code must be updated to use `Client` directly.