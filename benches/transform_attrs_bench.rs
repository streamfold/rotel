// SPDX-License-Identifier: Apache-2.0

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rotel::exporters::clickhouse::{Compression, transformer::Transformer};
use rotel::otlp::cvattr::{ConvertedAttrKeyValue, ConvertedAttrValue};

fn create_test_attributes() -> Vec<ConvertedAttrKeyValue> {
    vec![
        ConvertedAttrKeyValue(
            "service.name".to_string(),
            ConvertedAttrValue::String("my-service".to_string()),
        ),
        ConvertedAttrKeyValue(
            "http.method".to_string(),
            ConvertedAttrValue::String("GET".to_string()),
        ),
        ConvertedAttrKeyValue(
            "user.email".to_string(),
            ConvertedAttrValue::String("user@example.com".to_string()),
        ),
        ConvertedAttrKeyValue("http.status_code".to_string(), ConvertedAttrValue::Int(200)),
    ]
}

fn benchmark_transform_attrs_json(c: &mut Criterion) {
    let attrs = create_test_attributes();

    let transformer_json = Transformer::new(Compression::None, true, false);

    let mut group = c.benchmark_group("transform_attrs_json");

    // Benchmark original implementation
    group.bench_with_input(BenchmarkId::new("original", "basic"), &attrs, |b, attrs| {
        b.iter(|| transformer_json.transform_attrs(attrs));
    });

    // Benchmark optimized implementation
    group.bench_with_input(BenchmarkId::new("fast", "basic"), &attrs, |b, attrs| {
        b.iter(|| transformer_json.transform_attrs_fast(attrs));
    });

    group.finish();
}

fn benchmark_varying_sizes(c: &mut Criterion) {
    let transformer_json = Transformer::new(Compression::None, true, true);

    let sizes = [1, 4, 10, 25];

    for size in sizes.iter() {
        let attrs: Vec<ConvertedAttrKeyValue> = (0..*size)
            .map(|i| {
                if i % 4 == 3 {
                    // Every 4th attribute is an integer
                    ConvertedAttrKeyValue(
                        format!("metric.value.{}", i),
                        ConvertedAttrValue::Int(i as i64),
                    )
                } else {
                    // Others are strings with dots
                    ConvertedAttrKeyValue(
                        format!("service.component.{}", i),
                        ConvertedAttrValue::String(format!("component-{}", i)),
                    )
                }
            })
            .collect();

        let mut group = c.benchmark_group("transform_attrs_size_comparison");

        // JSON benchmarks
        group.bench_with_input(
            BenchmarkId::new("json_original", size),
            &attrs,
            |b, attrs| {
                b.iter(|| transformer_json.transform_attrs(attrs));
            },
        );

        group.bench_with_input(BenchmarkId::new("json_fast", size), &attrs, |b, attrs| {
            b.iter(|| transformer_json.transform_attrs_fast(attrs));
        });

        group.finish();
    }
}

criterion_group!(
    benches,
    benchmark_transform_attrs_json,
    benchmark_varying_sizes
);
criterion_main!(benches);
