// Copyright 2026 Matteo Merli <matteo.merli@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Histograms exported through `Producer` on an SDK `ManualReader`. The registry is
//! process-wide and tests run in parallel, so each test uses its own scope.

#![cfg(feature = "sdk")]

mod common;

use std::sync::Barrier;
use std::thread;
use std::time::Duration;

use common::Reader;
use nanometer::{HistogramDef, Meter, Producer};
use opentelemetry::KeyValue;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, HistogramDataPoint, MetricData};
use opentelemetry_sdk::metrics::{ManualReader, SdkMeterProvider, Temporality};

fn setup() -> (SdkMeterProvider, Reader) {
    common::setup(
        ManualReader::builder()
            .with_producer(Producer::new())
            .build(),
    )
}

#[derive(Debug, PartialEq)]
struct Point {
    attributes: Vec<KeyValue>,
    bounds: Vec<f64>,
    counts: Vec<u64>,
    count: u64,
    sum: f64,
}

#[derive(Debug)]
struct Exported {
    unit: String,
    /// Whether the values export as `f64` (in seconds), not `u64`.
    float: bool,
    points: Vec<Point>,
}

/// Runs one collection and returns histogram `name` from `scope`, if exported.
fn collect(reader: &Reader, scope: &str, name: &str) -> Option<Exported> {
    let rm = common::collect(reader);
    rm.scope_metrics()
        .filter(|sm| sm.scope().name() == scope)
        .flat_map(|sm| sm.metrics())
        .filter(|m| m.name() == name)
        .find_map(|metric| {
            let (float, points) = match metric.data() {
                AggregatedMetrics::U64(MetricData::Histogram(h)) => {
                    assert_eq!(h.temporality(), Temporality::Cumulative);
                    (
                        false,
                        h.data_points().map(|p| point(p, p.sum() as f64)).collect(),
                    )
                }
                AggregatedMetrics::F64(MetricData::Histogram(h)) => {
                    assert_eq!(h.temporality(), Temporality::Cumulative);
                    (true, h.data_points().map(|p| point(p, p.sum())).collect())
                }
                _ => return None,
            };
            Some(Exported {
                unit: metric.unit().to_owned(),
                float,
                points,
            })
        })
}

fn point<T>(p: &HistogramDataPoint<T>, sum: f64) -> Point {
    Point {
        attributes: p.attributes().cloned().collect(),
        bounds: p.bounds().collect(),
        counts: p.bucket_counts().collect(),
        count: p.count(),
        sum,
    }
}

#[test]
fn values_count_in_the_first_bucket_whose_bound_is_at_least_the_value() {
    const DEPTH: HistogramDef = HistogramDef::new("depth")
        .unit("{item}")
        .buckets(&[10, 100]);
    let (provider, reader) = setup();
    let scope = "values_count_in_the_first_bucket";
    let depth = Meter::new(&provider, scope).histogram(DEPTH, &[KeyValue::new("q", "a")]);
    for v in [0, 10, 11, 100, 101, 5_000] {
        depth.record(v);
    }
    let exported = collect(&reader, scope, "depth").unwrap();
    assert_eq!((exported.unit.as_str(), exported.float), ("{item}", false));
    assert_eq!(
        exported.points,
        [Point {
            attributes: vec![KeyValue::new("q", "a")],
            bounds: vec![10.0, 100.0],
            counts: vec![2, 2, 2],
            count: 6,
            sum: 5_222.0,
        }]
    );
}

#[test]
fn instances_on_other_threads_are_summed_and_kept_after_exit() {
    let (provider, reader) = setup();
    let scope = "histogram_instances_on_other_threads";
    let meter = Meter::new(&provider, scope);
    let local = meter.histogram(HistogramDef::new("size").buckets(&[10]), &[]);
    local.record(5);
    let barrier = Barrier::new(2);
    thread::scope(|s| {
        s.spawn(|| {
            let remote = meter.histogram(HistogramDef::new("size").buckets(&[10]), &[]);
            remote.record(20);
            remote.record(30);
            barrier.wait();
            barrier.wait();
        });
        barrier.wait();
        let point = &collect(&reader, scope, "size").unwrap().points[0];
        assert_eq!((point.counts.as_slice(), point.sum), (&[1, 2][..], 55.0));
        barrier.wait();
    });
    // The thread is gone and its instance dropped.
    let point = &collect(&reader, scope, "size").unwrap().points[0];
    assert_eq!((point.counts.as_slice(), point.sum), (&[1, 2][..], 55.0));
    // A new instance reuses the dropped one's cells, which must start from zero.
    drop(local);
    let again = meter.histogram(HistogramDef::new("size").buckets(&[10]), &[]);
    again.record(1);
    let point = &collect(&reader, scope, "size").unwrap().points[0];
    assert_eq!((point.counts.as_slice(), point.sum), (&[2, 2][..], 56.0));
}

#[test]
fn latency_records_microseconds_and_exports_seconds() {
    let (provider, reader) = setup();
    let scope = "latency_records_microseconds_and_exports_seconds";
    let latency = Meter::new(&provider, scope).latency("put_latency", &[]);
    latency.record(Duration::from_micros(15));
    latency.record(Duration::from_millis(3));
    latency.record(Duration::from_secs(60));

    let exported = collect(&reader, scope, "put_latency").unwrap();
    assert_eq!((exported.unit.as_str(), exported.float), ("s", true));
    let point = &exported.points[0];
    assert_eq!(point.bounds.len(), 19);
    assert_eq!((point.bounds[0], point.bounds[18]), (0.000_01, 10.0));
    // 15 µs is at most 20 µs, 3 ms at most 5 ms, and 60 s above the last bound.
    let mut counts = vec![0; 20];
    counts[1] = 1;
    counts[8] = 1;
    counts[19] = 1;
    assert_eq!(point.counts, counts);
    assert!((point.sum - 60.003_015).abs() < 1e-9, "{}", point.sum);
}

#[test]
fn timer_records_on_drop_and_stop_but_not_discard() {
    let (provider, reader) = setup();
    let scope = "timer_records_on_drop_and_stop_but_not_discard";
    let latency = Meter::new(&provider, scope).latency("op_latency", &[]);
    {
        let _timer = latency.start();
    }
    let elapsed = latency.start().stop();
    latency.start().discard();

    let point = &collect(&reader, scope, "op_latency").unwrap().points[0];
    assert_eq!(point.count, 2);
    assert!(point.sum >= elapsed.as_micros() as f64 / 1e6);
}

#[test]
fn presets_carry_their_buckets_and_units() {
    let (provider, reader) = setup();
    let scope = "presets_carry_their_buckets_and_units";
    let meter = Meter::new(&provider, scope);
    meter
        .histogram(HistogramDef::bytes("payload"), &[])
        .record(100);
    meter
        .histogram(HistogramDef::counts("batch").unit("{message}"), &[])
        .record(3);

    let payload = collect(&reader, scope, "payload").unwrap();
    assert_eq!(payload.unit, "By");
    let kib = |n: f64| n * 1024.0;
    assert_eq!(
        payload.points[0].bounds,
        [
            64.0,
            256.0,
            kib(1.0),
            kib(4.0),
            kib(16.0),
            kib(64.0),
            kib(256.0)
        ]
        .into_iter()
        .chain([1.0, 4.0, 16.0, 64.0].map(|m| kib(1024.0 * m)))
        .collect::<Vec<_>>()
    );
    assert_eq!(payload.points[0].counts[1], 1);

    let batch = collect(&reader, scope, "batch").unwrap();
    assert_eq!(batch.unit, "{message}");
    assert_eq!(
        batch.points[0].bounds,
        [
            1, 2, 5, 10, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000
        ]
        .map(f64::from)
    );
    assert_eq!(batch.points[0].counts[2], 1);
}

#[test]
fn conflicting_definitions_keep_the_first() {
    let (provider, reader) = setup();
    let scope = "histogram_conflicting_definitions_keep_the_first";
    let meter = Meter::new(&provider, scope);
    let first = meter.histogram(HistogramDef::new("size").buckets(&[10]), &[]);
    let second = meter.histogram(HistogramDef::new("size").buckets(&[1, 2, 3]), &[]);
    first.record(5);
    second.record(5);
    let point = &collect(&reader, scope, "size").unwrap().points[0];
    assert_eq!(
        (point.bounds.as_slice(), point.counts.as_slice()),
        (&[10.0][..], &[2, 0][..])
    );

    // A histogram named like an existing counter is not exported.
    meter.counter("requests", &[]).add(1);
    meter.histogram("requests", &[]).record(1);
    assert!(collect(&reader, scope, "requests").is_none());
}

#[test]
#[should_panic(expected = "bucket bounds must increase")]
fn bucket_bounds_must_increase() {
    // Outside a const context the check runs, and panics, at run time.
    let _ = HistogramDef::new("x").buckets(&[5, 5]);
}
