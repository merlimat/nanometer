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

//! Gauges exported through an SDK `ManualReader`. The registry is process-wide and
//! tests run in parallel, so each test uses its own scope.

mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering::Relaxed};
use std::thread;

use common::Reader;
use nanometer::{GaugeDef, Meter};
use opentelemetry::KeyValue;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{ManualReader, SdkMeterProvider};

fn setup() -> (SdkMeterProvider, Reader) {
    common::setup(ManualReader::builder().build())
}

/// Runs one collection and returns the points of the i64 gauge `name` from `scope`;
/// empty if it has none.
fn collect(reader: &Reader, scope: &str, name: &str) -> HashMap<Vec<KeyValue>, i64> {
    let rm = common::collect(reader);
    let Some(metric) = rm
        .scope_metrics()
        .filter(|sm| sm.scope().name() == scope)
        .flat_map(|sm| sm.metrics())
        .find(|m| m.name() == name)
    else {
        return HashMap::new();
    };
    let AggregatedMetrics::I64(MetricData::Gauge(gauge)) = metric.data() else {
        panic!("{scope}/{name} is not an i64 gauge: {:?}", metric.data());
    };
    gauge
        .data_points()
        .map(|p| (p.attributes().cloned().collect(), p.value()))
        .collect()
}

fn point(attributes: &[KeyValue], value: i64) -> HashMap<Vec<KeyValue>, i64> {
    HashMap::from([(attributes.to_vec(), value)])
}

#[test]
fn the_last_set_wins_across_handles_and_threads() {
    let (provider, reader) = setup();
    let scope = "the_last_set_wins_across_handles_and_threads";
    let meter = Meter::new(&provider, scope);
    let attrs = [KeyValue::new("topic", "a")];
    let first = meter.gauge("backlog", &attrs);
    let second = meter.gauge("backlog", &attrs);
    first.set(5);
    second.set(7);
    assert_eq!(collect(&reader, scope, "backlog"), point(&attrs, 7));
    thread::scope(|s| {
        s.spawn(|| first.set(-3));
    });
    assert_eq!(collect(&reader, scope, "backlog"), point(&attrs, -3));
}

#[test]
fn a_gauge_exports_nothing_until_set() {
    let (provider, reader) = setup();
    let scope = "a_gauge_exports_nothing_until_set";
    let gauge = Meter::new(&provider, scope).gauge("backlog", &[]);
    assert_eq!(collect(&reader, scope, "backlog"), HashMap::new());
    gauge.set(0);
    assert_eq!(collect(&reader, scope, "backlog"), point(&[], 0));
}

#[test]
fn a_series_leaves_with_its_last_handle() {
    let (provider, reader) = setup();
    let scope = "a_series_leaves_with_its_last_handle";
    let meter = Meter::new(&provider, scope);
    let first = meter.gauge("backlog", &[]);
    let second = meter.gauge("backlog", &[]);
    first.set(4);
    drop(first);
    assert_eq!(collect(&reader, scope, "backlog"), point(&[], 4));
    drop(second);
    assert_eq!(collect(&reader, scope, "backlog"), HashMap::new());
    // A new handle starts a new series, unset.
    let third = meter.gauge("backlog", &[]);
    assert_eq!(collect(&reader, scope, "backlog"), HashMap::new());
    third.set(1);
    assert_eq!(collect(&reader, scope, "backlog"), point(&[], 1));
}

#[test]
fn observable_gauges_read_their_callback_at_each_export() {
    let (provider, reader) = setup();
    let scope = "observable_gauges_read_their_callback_at_each_export";
    let meter = Meter::new(&provider, scope);
    let depth = Arc::new(AtomicI64::new(3));
    let read = Arc::clone(&depth);
    let a = [KeyValue::new("queue", "a")];
    let b = [KeyValue::new("queue", "b")];
    let _a = meter.observable_gauge("depth", &a, move || read.load(Relaxed));
    let gauge_b = meter.observable_gauge("depth", &b, || 10);
    assert_eq!(
        collect(&reader, scope, "depth"),
        HashMap::from([(a.to_vec(), 3), (b.to_vec(), 10)])
    );
    depth.store(8, Relaxed);
    drop(gauge_b);
    assert_eq!(collect(&reader, scope, "depth"), point(&a, 8));
}

#[test]
fn a_second_gauge_for_a_series_is_ignored() {
    let (provider, reader) = setup();
    let scope = "a_second_gauge_for_a_series_is_ignored";
    let meter = Meter::new(&provider, scope);
    let attrs = [KeyValue::new("queue", "a")];
    let first = meter.observable_gauge("depth", &attrs, || 1);
    let second = meter.observable_gauge("depth", &attrs, || 2);
    // A set gauge can't take a callback's series either.
    let set = meter.gauge("depth", &attrs);
    set.set(3);
    assert_eq!(collect(&reader, scope, "depth"), point(&attrs, 1));
    // Dropping the ignored ones leaves the first in place.
    drop((second, set));
    assert_eq!(collect(&reader, scope, "depth"), point(&attrs, 1));
    drop(first);
    assert_eq!(collect(&reader, scope, "depth"), HashMap::new());

    // Nor can a callback take a set gauge's series.
    let set = meter.gauge("level", &attrs);
    set.set(5);
    let _ignored = meter.observable_gauge("level", &attrs, || 6);
    assert_eq!(collect(&reader, scope, "level"), point(&attrs, 5));
}

#[test]
fn definition_carries_unit_and_description() {
    const BACKLOG: GaugeDef = GaugeDef::new("backlog")
        .unit("By")
        .description("Bytes not yet acknowledged");
    let (provider, reader) = setup();
    let scope = "gauge_definition_carries_unit_and_description";
    let backlog = Meter::new(&provider, scope).gauge(BACKLOG, &[]);
    backlog.set(1);
    let rm = common::collect(&reader);
    let metric = rm
        .scope_metrics()
        .filter(|sm| sm.scope().name() == scope)
        .flat_map(|sm| sm.metrics())
        .next()
        .unwrap();
    assert_eq!(
        (metric.unit(), metric.description()),
        ("By", "Bytes not yet acknowledged")
    );
}

#[test]
fn a_gauge_named_like_another_kind_is_not_exported() {
    let (provider, reader) = setup();
    let scope = "a_gauge_named_like_another_kind_is_not_exported";
    let meter = Meter::new(&provider, scope);
    meter.counter("requests", &[]).add(1);
    meter.gauge("requests", &[]).set(9);
    let _ignored = meter.observable_gauge("requests", &[], || 9);
    let rm = common::collect(&reader);
    let metrics: Vec<_> = rm
        .scope_metrics()
        .filter(|sm| sm.scope().name() == scope)
        .flat_map(|sm| sm.metrics())
        .collect();
    assert_eq!(metrics.len(), 1);
    assert!(matches!(
        metrics[0].data(),
        AggregatedMetrics::U64(MetricData::Sum(_))
    ));
}
