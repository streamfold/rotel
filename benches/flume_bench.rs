// SPDX-License-Identifier: Apache-2.0

use criterion::Criterion;
use criterion::Throughput;
use criterion::{criterion_group, criterion_main};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use prost::Message;
use rotel::bounded_channel::bounded;
use tokio::select;
use utilities::otlp::FakeOTLP;

fn flume_throughput_otel_traces(c: &mut Criterion) {
    let (tx, rx) = bounded::<ExportTraceServiceRequest>(5);
    let req_small = FakeOTLP::trace_service_request();
    let req_medium = FakeOTLP::trace_service_request_with_spans(1, 100);
    let req_large = FakeOTLP::trace_service_request_with_spans(1, 1000);
    let input = [
        ("small", req_small),
        ("medium", req_medium),
        ("large", req_large),
    ];
    let mut group = c.benchmark_group("flume_throughput");
    for size_and_req in input.iter() {
        group.throughput(Throughput::Bytes(size_and_req.1.encoded_len() as u64));
        // TODO - Still digging into this, ideally we should not have to spawn the tokio runtime here, clone the receiver, and clone the request.
        // When you want to consume the input you can use iter_batched and iter_batched_ref. https://docs.rs/criterion/latest/criterion/struct.Bencher.html#method.iter_batched
        // Need to read more criterion tests with tokio and continue digging.
        group.bench_with_input(
            format!("flume_throughput {}", size_and_req.0),
            &size_and_req.1,
            |b, req| {
                let tr = tokio::runtime::Runtime::new().unwrap();
                let mut rx_c = rx.clone();
                tr.spawn(async move {
                    loop {
                        select! {
                            v = rx_c.next() => {
                                match v {
                                    None => { },
                                    Some(_d) => {
                                    }
                                }
                            }
                        }
                    }
                });
                b.to_async(tr)
                    .iter(|| tx.send(criterion::black_box(req.clone())));
            },
        );
    }
    group.finish();
}

criterion_group!(benches, flume_throughput_otel_traces);
criterion_main!(benches);
