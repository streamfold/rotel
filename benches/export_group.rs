// SPDX-License-Identifier: Apache-2.0
//
// Benchmarks for the ExportGroup<T> hot path.
//
// Bench A: baseline — one exporter channel, always acks (no group overhead).
// Bench B: group CLOSED — 2-member group, primary (m0) always acks.
// Bench C: group OPEN steady-state — 2-member group pre-tripped, m1 always acks.
// Bench D: group retry worst-case — m0 always nacks, m1 acks (2× clones per batch).
//
// Acceptance target: B and C within ~15% of A on per-batch throughput.
// Numbers are reported in the PR; D is informational only.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use rotel::bounded_channel::{BoundedReceiver, bounded};
use rotel::topology::export_group::ExportGroupBuilder;
use rotel::topology::payload::{Ack, ExporterError, Message};
use std::time::Duration;
use tokio::runtime::Runtime;
use utilities::otlp::FakeOTLP;

// ─── helpers ─────────────────────────────────────────────────────────────────

fn make_batch(resource_spans: &[ResourceSpans]) -> Vec<Message<ResourceSpans>> {
    resource_spans
        .iter()
        .cloned()
        .map(|rs| Message::new(None, vec![rs], None))
        .collect()
}

/// Spawn a task that drains `rx`, always acks the forwarder metadata, and loops.
fn spawn_always_ack(rt: &Runtime, rx: BoundedReceiver<Vec<Message<ResourceSpans>>>) {
    let mut rx = rx;
    rt.spawn(async move {
        loop {
            match rx.next().await {
                None => break,
                Some(batch) => {
                    for msg in &batch {
                        if let Some(meta) = &msg.metadata {
                            let _ = meta.ack().await;
                        }
                    }
                }
            }
        }
    });
}

/// Spawn a task that drains `rx`, always nacks the forwarder metadata, and loops.
fn spawn_always_nack(rt: &Runtime, rx: BoundedReceiver<Vec<Message<ResourceSpans>>>) {
    let mut rx = rx;
    rt.spawn(async move {
        loop {
            match rx.next().await {
                None => break,
                Some(batch) => {
                    for msg in &batch {
                        if let Some(meta) = &msg.metadata {
                            let _ = meta.nack(ExporterError::Cancelled).await;
                        }
                    }
                }
            }
        }
    });
}

// ─── Bench A: baseline (no group) ────────────────────────────────────────────

fn bench_baseline(c: &mut Criterion, spans: &[ResourceSpans], label: &str) {
    let rt = Runtime::new().unwrap();
    let (tx, rx) = bounded::<Vec<Message<ResourceSpans>>>(256);
    spawn_always_ack(&rt, rx);

    let batch = make_batch(spans);
    let batch_bytes = spans
        .iter()
        .map(|s| prost::Message::encoded_len(s))
        .sum::<usize>() as u64;

    let mut group = c.benchmark_group("export_group");
    group.throughput(Throughput::Bytes(batch_bytes));
    group.bench_function(BenchmarkId::new("A_baseline", label), |b| {
        b.to_async(&rt).iter(|| async {
            tx.send(criterion::black_box(batch.clone())).await.unwrap();
        });
    });
    group.finish();
}

// ─── Bench B: group CLOSED (m0 always acks) ──────────────────────────────────

fn bench_group_closed(c: &mut Criterion, spans: &[ResourceSpans], label: &str) {
    let rt = Runtime::new().unwrap();
    let _guard = rt.enter();
    let (m0_tx, m0_rx) = bounded::<Vec<Message<ResourceSpans>>>(256);
    let (m1_tx, _m1_rx) = bounded::<Vec<Message<ResourceSpans>>>(256);
    spawn_always_ack(&rt, m0_rx);

    let group = ExportGroupBuilder::<ResourceSpans>::new(256)
        .add_member(m0_tx)
        .add_member(m1_tx)
        .trip_after(100)
        .probe_after(Duration::from_secs(30))
        .build();
    let sender = group.sender();

    let batch = make_batch(spans);
    let batch_bytes = spans
        .iter()
        .map(|s| prost::Message::encoded_len(s))
        .sum::<usize>() as u64;

    let mut crit_group = c.benchmark_group("export_group");
    crit_group.throughput(Throughput::Bytes(batch_bytes));
    crit_group.bench_function(BenchmarkId::new("B_group_closed", label), |b| {
        b.to_async(&rt).iter(|| async {
            sender
                .send(criterion::black_box(batch.clone()))
                .await
                .unwrap();
        });
    });
    crit_group.finish();
}

// ─── Bench C: group OPEN steady-state (m1 always acks, active=1) ─────────────

fn bench_group_open(c: &mut Criterion, spans: &[ResourceSpans], label: &str) {
    let rt = Runtime::new().unwrap();
    let _guard = rt.enter();
    let (m0_tx, _m0_rx) = bounded::<Vec<Message<ResourceSpans>>>(256);
    let (m1_tx, m1_rx) = bounded::<Vec<Message<ResourceSpans>>>(256);
    // Drop m0_rx so sends to m0 fail → breaker trips on the first batch.
    drop(_m0_rx);
    spawn_always_ack(&rt, m1_rx);

    let group = ExportGroupBuilder::<ResourceSpans>::new(256)
        .add_member(m0_tx)
        .add_member(m1_tx)
        .trip_after(1) // trip after first nack
        .probe_after(Duration::from_secs(3600)) // disable probe during bench
        .build();
    let mut active_rx = group.subscribe_active();
    let sender = group.sender();

    // Prime: send one batch to trip the breaker (m0 closed → immediate nack → walk to m1).
    rt.block_on(async {
        let prime = make_batch(spans);
        sender.send(prime).await.unwrap();
        active_rx.wait_for(|&v| v == 1).await.unwrap();
    });

    let batch = make_batch(spans);
    let batch_bytes = spans
        .iter()
        .map(|s| prost::Message::encoded_len(s))
        .sum::<usize>() as u64;

    let mut crit_group = c.benchmark_group("export_group");
    crit_group.throughput(Throughput::Bytes(batch_bytes));
    crit_group.bench_function(BenchmarkId::new("C_group_open", label), |b| {
        b.to_async(&rt).iter(|| async {
            sender
                .send(criterion::black_box(batch.clone()))
                .await
                .unwrap();
        });
    });
    crit_group.finish();
}

// ─── Bench D: retry worst-case (m0 nacks → m1 acks, 2× clones) ───────────────

fn bench_group_retry(c: &mut Criterion, spans: &[ResourceSpans], label: &str) {
    let rt = Runtime::new().unwrap();
    let _guard = rt.enter();
    let (m0_tx, m0_rx) = bounded::<Vec<Message<ResourceSpans>>>(256);
    let (m1_tx, m1_rx) = bounded::<Vec<Message<ResourceSpans>>>(256);
    spawn_always_nack(&rt, m0_rx);
    spawn_always_ack(&rt, m1_rx);

    let group = ExportGroupBuilder::<ResourceSpans>::new(256)
        .add_member(m0_tx)
        .add_member(m1_tx)
        .trip_after(u32::MAX) // never trip; always retry
        .probe_after(Duration::ZERO)
        .build();
    let sender = group.sender();

    let batch = make_batch(spans);
    let batch_bytes = spans
        .iter()
        .map(|s| prost::Message::encoded_len(s))
        .sum::<usize>() as u64;

    let mut crit_group = c.benchmark_group("export_group");
    crit_group.throughput(Throughput::Bytes(batch_bytes));
    crit_group.bench_function(BenchmarkId::new("D_group_retry", label), |b| {
        b.to_async(&rt).iter(|| async {
            sender
                .send(criterion::black_box(batch.clone()))
                .await
                .unwrap();
        });
    });
    crit_group.finish();
}

// ─── entry point ─────────────────────────────────────────────────────────────

fn export_group_benchmarks(c: &mut Criterion) {
    let small_req = FakeOTLP::trace_service_request();
    let medium_req = FakeOTLP::trace_service_request_with_spans(1, 10);
    let large_req = FakeOTLP::trace_service_request_with_spans(1, 100);

    let cases: &[(&str, &[ResourceSpans])] = &[
        ("small", &small_req.resource_spans),
        ("medium", &medium_req.resource_spans),
        ("large", &large_req.resource_spans),
    ];

    for (label, spans) in cases {
        bench_baseline(c, spans, label);
        bench_group_closed(c, spans, label);
        bench_group_open(c, spans, label);
        bench_group_retry(c, spans, label);
    }
}

criterion_group!(benches, export_group_benchmarks);
criterion_main!(benches);
