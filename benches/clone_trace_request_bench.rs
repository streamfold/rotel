// SPDX-License-Identifier: Apache-2.0

use criterion::Throughput;
use criterion::{criterion_group, criterion_main};
use criterion::{BatchSize, Criterion};
use prost::Message;
use utilities::otlp::FakeOTLP;

fn clone_traces(c: &mut Criterion) {
    let req_small = FakeOTLP::trace_service_request();
    let req_medium = FakeOTLP::trace_service_request_with_spans(1, 100);
    let req_large = FakeOTLP::trace_service_request_with_spans(1, 1000);
    let input = [
        ("small", req_small),
        ("medium", req_medium),
        ("large", req_large),
    ];
    let mut group = c.benchmark_group("clone_traces");
    for size_and_req in input.iter() {
        group.throughput(Throughput::Bytes(size_and_req.1.encoded_len() as u64));
        // TODO - Still digging into this, ideally we should not have to spawn the tokio runtime here, clone the receiver, and clone the request.
        // When you want to consume the input you can use iter_batched and iter_batched_ref. https://docs.rs/criterion/latest/criterion/struct.Bencher.html#method.iter_batched
        // Need to read more criterion tests with tokio and continue digging.
        group.bench_with_input(
            format!("clone_traces {}", size_and_req.0),
            &size_and_req.1,
            |b, req| {
                b.iter(|| req.clone());
            },
        );
        group.bench_function("take traces", move |b| {
            b.iter_batched(
                || size_and_req.1.clone(),
                |mut req| std::mem::take(&mut req),
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

criterion_group!(benches, clone_traces);
criterion_main!(benches);
