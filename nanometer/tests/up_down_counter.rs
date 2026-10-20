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

//! Up-down counters exported through an SDK `ManualReader`. The registry is
//! process-wide and tests run in parallel, so each test uses its own scope.

mod common;

use std::collections::HashMap;
use std::sync::mpsc;
use std::thread;

use common::Reader;
use nanometer::{Meter, UpDownCounterDef};
use opentelemetry::KeyValue;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{ManualReader, SdkMeterProvider};

fn setup() -> (SdkMeterProvider, Reader) {
    common::setup(ManualReader::builder().build())
}

/// Runs one collection and returns the points of the i64 sum `name` from `scope`.
fn collect(reader: &Reader, scope: &str, name: &str) -> HashMap<Vec<KeyValue>, i64> {
    let rm = common::collect(reader);
    let metric = rm
        .scope_metrics()
        .filter(|sm| sm.scope().name() == scope)
        .flat_map(|sm| sm.metrics())
        .find(|m| m.name() == name)
        .unwrap_or_else(|| panic!("{scope}/{name} not exported"));
    let AggregatedMetrics::I64(MetricData::Sum(sum)) = metric.data() else {
        panic!("{scope}/{name} is not an i64 sum: {:?}", metric.data());
    };
    assert!(!sum.is_monotonic());
    sum.data_points()
        .map(|p| (p.attributes().cloned().collect(), p.value()))
        .collect()
}

#[test]
fn increments_and_decrements_on_any_instance_sum_up() {
    let (provider, reader) = setup();
    let scope = "increments_and_decrements_on_any_instance_sum_up";
    let attrs = [KeyValue::new("pool", "a")];
    // A connection opened on one core and closed on another.
    let core0 = Meter::new(&provider, scope).up_down_counter("connections", &attrs);
    let core1 = Meter::new(&provider, scope).up_down_counter("connections", &attrs);
    core0.add(5);
    core1.add(-2);
    thread::scope(|s| {
        let (provider, attrs) = (&provider, &attrs);
        let (recorded_tx, recorded) = mpsc::channel();
        let (collected_tx, collected) = mpsc::channel::<()>();
        s.spawn(move || {
            let core2 = Meter::new(provider, scope).up_down_counter("connections", attrs);
            core2.add(10);
            core2.add(-12);
            recorded_tx.send(()).unwrap();
            // Until the main thread has collected, or has panicked and dropped its end.
            let _ = collected.recv();
        });
        recorded.recv().unwrap();
        assert_eq!(
            collect(&reader, scope, "connections"),
            HashMap::from([(attrs.to_vec(), 1)])
        );
        drop(collected_tx);
    });
    // The thread is gone and its instance dropped, -2 included.
    assert_eq!(
        collect(&reader, scope, "connections"),
        HashMap::from([(attrs.to_vec(), 1)])
    );
}

#[test]
fn totals_can_go_negative() {
    let (provider, reader) = setup();
    let scope = "up_down_totals_can_go_negative";
    let queued = Meter::new(&provider, scope).up_down_counter("queued", &[]);
    queued.add(-3);
    assert_eq!(
        collect(&reader, scope, "queued"),
        HashMap::from([(vec![], -3)])
    );
}

#[test]
fn dropped_instances_keep_counting() {
    let (provider, reader) = setup();
    let scope = "up_down_dropped_instances_keep_counting";
    let meter = Meter::new(&provider, scope);
    let first = meter.up_down_counter("queued", &[]);
    first.add(4);
    drop(first);
    // Reuses the dropped instance's cell, which must start again from zero.
    let second = meter.up_down_counter("queued", &[]);
    second.add(-1);
    assert_eq!(
        collect(&reader, scope, "queued"),
        HashMap::from([(vec![], 3)])
    );
}

#[test]
fn definition_carries_unit_and_description() {
    const QUEUED: UpDownCounterDef = UpDownCounterDef::new("queued")
        .unit("{message}")
        .description("Messages waiting");
    let (provider, reader) = setup();
    let scope = "up_down_definition_carries_unit_and_description";
    Meter::new(&provider, scope)
        .up_down_counter(QUEUED, &[])
        .add(1);
    let rm = common::collect(&reader);
    let metric = rm
        .scope_metrics()
        .filter(|sm| sm.scope().name() == scope)
        .flat_map(|sm| sm.metrics())
        .next()
        .unwrap();
    assert_eq!(
        (metric.unit(), metric.description()),
        ("{message}", "Messages waiting")
    );
}

#[test]
fn a_counter_and_an_up_down_counter_do_not_share_a_name() {
    let (provider, reader) = setup();
    let scope = "a_counter_and_an_up_down_counter_do_not_share_a_name";
    let meter = Meter::new(&provider, scope);
    meter.counter("requests", &[]).add(1);
    meter.up_down_counter("requests", &[]).add(-5);
    meter.up_down_counter("inflight", &[]).add(2);
    meter.counter("inflight", &[]).add(7);

    let rm = common::collect(&reader);
    let metrics: Vec<_> = rm
        .scope_metrics()
        .filter(|sm| sm.scope().name() == scope)
        .flat_map(|sm| sm.metrics())
        .collect();
    // Only the first of each name is exported.
    assert_eq!(metrics.len(), 2);
    for metric in metrics {
        match (metric.name(), metric.data()) {
            ("requests", AggregatedMetrics::U64(MetricData::Sum(sum))) => {
                assert_eq!(sum.data_points().next().unwrap().value(), 1);
            }
            ("inflight", AggregatedMetrics::I64(MetricData::Sum(sum))) => {
                assert_eq!(sum.data_points().next().unwrap().value(), 2);
            }
            (name, data) => panic!("unexpected {name}: {data:?}"),
        }
    }
}
