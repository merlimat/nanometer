//! Counters exported through an SDK `ManualReader`. The registry is process-wide and
//! tests run in parallel, so each test uses its own scope.

use std::collections::HashMap;
use std::sync::{Arc, Barrier, Weak};
use std::thread;
use std::time::Duration;

use nanometer::{CounterDef, Meter};
use opentelemetry::KeyValue;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
use opentelemetry_sdk::metrics::reader::MetricReader;
use opentelemetry_sdk::metrics::{
    InstrumentKind, ManualReader, Pipeline, SdkMeterProvider, Temporality,
};

/// `with_reader` takes the reader by value; this keeps a handle to collect with.
#[derive(Clone, Debug)]
struct Reader(Arc<ManualReader>);

impl MetricReader for Reader {
    fn register_pipeline(&self, pipeline: Weak<Pipeline>) {
        self.0.register_pipeline(pipeline)
    }
    fn collect(&self, rm: &mut ResourceMetrics) -> OTelSdkResult {
        self.0.collect(rm)
    }
    fn force_flush(&self) -> OTelSdkResult {
        self.0.force_flush()
    }
    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.0.shutdown_with_timeout(timeout)
    }
    fn temporality(&self, kind: InstrumentKind) -> Temporality {
        self.0.temporality(kind)
    }
}

fn setup(temporality: Temporality) -> (SdkMeterProvider, Reader) {
    let reader = Reader(Arc::new(
        ManualReader::builder()
            .with_temporality(temporality)
            .build(),
    ));
    let provider = SdkMeterProvider::builder()
        .with_reader(reader.clone())
        .build();
    (provider, reader)
}

struct Exported {
    unit: String,
    points: HashMap<Vec<KeyValue>, u64>,
}

/// Runs one collection and returns the u64 sum `name` from `scope`.
fn collect(reader: &Reader, scope: &str, name: &str) -> Exported {
    let mut rm = ResourceMetrics::default();
    reader.collect(&mut rm).unwrap();
    let metric = rm
        .scope_metrics()
        .filter(|sm| sm.scope().name() == scope)
        .flat_map(|sm| sm.metrics())
        .find(|m| m.name() == name)
        .unwrap_or_else(|| panic!("{scope}/{name} not exported"));
    let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() else {
        panic!("{scope}/{name} is not a u64 sum");
    };
    let points = sum
        .data_points()
        .map(|p| (p.attributes().cloned().collect(), p.value()))
        .collect();
    Exported {
        unit: metric.unit().to_owned(),
        points,
    }
}

fn points<const N: usize>(points: [(Vec<KeyValue>, u64); N]) -> HashMap<Vec<KeyValue>, u64> {
    HashMap::from(points)
}

#[test]
fn instances_of_one_series_are_summed() {
    let (provider, reader) = setup(Temporality::Cumulative);
    let scope = "instances_of_one_series_are_summed";
    let attrs = [KeyValue::new("op", "put")];
    // Two meters for the same scope, as two cores would create them. One observable per
    // instance would export 7 here: the SDK keeps the last observation per attribute set.
    let core0 = Meter::new(&provider, scope).counter("requests", &attrs);
    let core1 = Meter::new(&provider, scope).counter("requests", &attrs);
    core0.add(5);
    core1.add(7);
    assert_eq!(
        collect(&reader, scope, "requests").points,
        points([(attrs.to_vec(), 12)])
    );
}

#[test]
fn instances_on_other_threads_are_summed_and_kept_after_exit() {
    let (provider, reader) = setup(Temporality::Cumulative);
    let scope = "instances_on_other_threads_are_summed_and_kept_after_exit";
    let meter = Meter::new(&provider, scope);
    let local = meter.counter("requests", &[]);
    local.add(5);
    let barrier = Barrier::new(2);
    thread::scope(|s| {
        s.spawn(|| {
            let remote = meter.counter("requests", &[]);
            remote.add(7);
            barrier.wait();
            barrier.wait();
        });
        barrier.wait();
        assert_eq!(
            collect(&reader, scope, "requests").points,
            points([(vec![], 12)])
        );
        barrier.wait();
    });
    // The thread is gone and its instance dropped.
    assert_eq!(
        collect(&reader, scope, "requests").points,
        points([(vec![], 12)])
    );
}

#[test]
fn dropped_instances_fold_into_their_series() {
    let (provider, reader) = setup(Temporality::Cumulative);
    let scope = "dropped_instances_fold_into_their_series";
    let meter = Meter::new(&provider, scope);
    let first = meter.counter("requests", &[]);
    first.add(5);
    assert_eq!(
        collect(&reader, scope, "requests").points,
        points([(vec![], 5)])
    );
    drop(first);
    assert_eq!(
        collect(&reader, scope, "requests").points,
        points([(vec![], 5)])
    );
    // Reuses the dropped instance's cell, which must start again from zero.
    let second = meter.counter("requests", &[]);
    second.add(2);
    assert_eq!(
        collect(&reader, scope, "requests").points,
        points([(vec![], 7)])
    );
}

#[test]
fn attribute_order_and_repeated_keys_do_not_split_series() {
    let (provider, reader) = setup(Temporality::Cumulative);
    let scope = "attribute_order_and_repeated_keys_do_not_split_series";
    let meter = Meter::new(&provider, scope);
    let kv = KeyValue::new;
    let a = meter.counter("requests", &[kv("a", 1), kv("b", 2)]);
    let b = meter.counter("requests", &[kv("b", 2), kv("a", 1)]);
    let c = meter.counter("requests", &[kv("a", 0), kv("b", 2), kv("a", 1)]);
    let d = meter.counter("requests", &[kv("a", 2)]);
    a.add(1);
    b.add(2);
    c.add(4);
    d.add(8);
    assert_eq!(
        collect(&reader, scope, "requests").points,
        points([(vec![kv("a", 1), kv("b", 2)], 7), (vec![kv("a", 2)], 8)])
    );
}

#[test]
fn conflicting_definition_keeps_the_first() {
    const BYTES: CounterDef = CounterDef::new("bytes").unit("By");
    let (provider, reader) = setup(Temporality::Cumulative);
    let scope = "conflicting_definition_keeps_the_first";
    let meter = Meter::new(&provider, scope);
    let a = meter.counter(BYTES, &[]);
    let b = meter.counter(CounterDef::new("bytes").unit("KiBy"), &[]);
    a.add(1);
    b.add(2);
    let exported = collect(&reader, scope, "bytes");
    assert_eq!(exported.unit, "By");
    assert_eq!(exported.points, points([(vec![], 3)]));
}

#[test]
fn delta_reader_gets_deltas() {
    let (provider, reader) = setup(Temporality::Delta);
    let scope = "delta_reader_gets_deltas";
    let requests = Meter::new(&provider, scope).counter("requests", &[]);
    requests.add(5);
    assert_eq!(
        collect(&reader, scope, "requests").points,
        points([(vec![], 5)])
    );
    requests.add(3);
    assert_eq!(
        collect(&reader, scope, "requests").points,
        points([(vec![], 3)])
    );
}
