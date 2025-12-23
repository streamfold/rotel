// SPDX-License-Identifier: Apache-2.0

use criterion::Throughput;
use criterion::{BatchSize, Criterion};
use criterion::{criterion_group, criterion_main};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use prost::Message;
use rotel::exporters::otlp;
use rotel::exporters::otlp::request::RequestBuilder;
use rotel::exporters::otlp::{CompressionEncoding, Endpoint, Protocol, request};
use rotel::telemetry::RotelCounter;
use utilities::otlp::FakeOTLP;

#[derive(Clone)]
struct TestInput {
    rb: RequestBuilder<ExportTraceServiceRequest>,
    reqs: ExportTraceServiceRequest,
}

fn build_request_builder() -> RequestBuilder<ExportTraceServiceRequest> {
    let config = otlp::config_builder(
        "traces",
        Endpoint::Base("http://localhost:4317".to_string()),
        Protocol::Grpc,
    )
    .with_compression_encoding(Some(CompressionEncoding::Gzip));
    request::build_traces(&config, RotelCounter::NoOpCounter).unwrap()
}

fn encode_trace(c: &mut Criterion) {
    let req_small = FakeOTLP::trace_service_request();
    let req_medium = FakeOTLP::trace_service_request_with_spans(1, 100);
    let req_large = FakeOTLP::trace_service_request_with_spans(1, 1000);
    let req_max = FakeOTLP::trace_service_request_with_spans(1, 8192);
    let input = [
        ("small", req_small),
        ("medium", req_medium),
        ("large", req_large),
        ("max", req_max),
    ];

    let mut group = c.benchmark_group("encode traces");
    for size_and_req in input.iter() {
        group.throughput(Throughput::Bytes(size_and_req.1.encoded_len() as u64));
        group.bench_function(format!("encode_traces {}", size_and_req.0), move |b| {
            b.iter_batched(
                || TestInput {
                    rb: build_request_builder(),
                    reqs: size_and_req.1.clone(),
                },
                |ti| {
                    ti.rb.encode(
                        ExportTraceServiceRequest {
                            resource_spans: ti.reqs.resource_spans,
                        },
                        100,
                        None,
                    )
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

criterion_group!(benches, encode_trace);
criterion_main!(benches);
