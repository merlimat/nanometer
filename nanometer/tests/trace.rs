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

//! Span recording and export. Drains take spans from every recorder in the process,
//! so these tests take turns through `serial`.

mod common;

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
#[cfg(feature = "sdk-trace")]
use std::future::{self, Future};
use std::sync::{Mutex, MutexGuard};
use std::thread;

use common::Reader;
use nanometer::Meter;
use nanometer::trace::{
    self, Attribute, MAX_ATTRIBUTES, SpanCapacity, SpanKind, SpanRecord, SpanStart, Status, Value,
};
use opentelemetry::{KeyValue, SpanId, TraceFlags, TraceId};
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{ManualReader, SdkMeterProvider};
#[cfg(feature = "sdk-trace")]
use {
    nanometer::trace::Collector,
    opentelemetry::trace as otel,
    opentelemetry_sdk::error::OTelSdkResult,
    opentelemetry_sdk::trace::{SpanData, SpanExporter},
    std::time::{Duration, SystemTime},
};

/// Counts heap allocations per thread.
struct CountingAlloc;

thread_local! {
    static ALLOCS: Cell<u64> = const { Cell::new(0) };
}

fn count_alloc() {
    let _ = ALLOCS.try_with(|n| n.set(n.get() + 1));
}

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count_alloc();
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        count_alloc();
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

static SERIAL: Mutex<()> = Mutex::new(());

/// Takes the test's turn, and empties the rings of earlier tests.
fn serial() -> MutexGuard<'static, ()> {
    let guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    trace::drain(usize::MAX, |_, _| {});
    guard
}

fn setup(scope: &'static str) -> (SdkMeterProvider, Reader, Meter) {
    let (provider, reader) = common::setup(ManualReader::builder().build());
    let meter = Meter::new(&provider, scope);
    (provider, reader, meter)
}

/// The spans drained, with their scopes.
fn drained() -> Vec<(&'static str, SpanRecord)> {
    let mut spans = Vec::new();
    trace::drain(usize::MAX, |scope, record| spans.push((scope, *record)));
    spans
}

/// `nanometer.spans.dropped{reason}` of `scope`.
fn dropped(reader: &Reader, scope: &str, reason: &'static str) -> u64 {
    let rm = common::collect(reader);
    let Some(metric) = rm
        .scope_metrics()
        .filter(|sm| sm.scope().name() == scope)
        .flat_map(|sm| sm.metrics())
        .find(|m| m.name() == "nanometer.spans.dropped")
    else {
        return 0;
    };
    let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() else {
        panic!("nanometer.spans.dropped is not a u64 sum");
    };
    sum.data_points()
        .filter(|p| {
            p.attributes()
                .any(|kv| kv == &KeyValue::new("reason", reason))
        })
        .map(|p| p.value())
        .sum()
}

fn trace_id(n: u8) -> TraceId {
    TraceId::from_bytes([n; 16])
}

fn span_id(n: u8) -> SpanId {
    SpanId::from_bytes([n; 8])
}

fn start(n: u8) -> SpanStart {
    SpanStart::new("op", trace_id(1), span_id(n))
}

#[cfg(feature = "sdk-trace")]
#[derive(Debug, Default)]
struct InMemory(Mutex<Vec<Vec<SpanData>>>);

#[cfg(feature = "sdk-trace")]
impl SpanExporter for InMemory {
    fn export(&self, batch: Vec<SpanData>) -> impl Future<Output = OTelSdkResult> + Send {
        self.0.lock().unwrap().push(batch);
        future::ready(Ok(()))
    }
}

#[test]
fn spans_record_what_they_are_given() {
    let _turn = serial();
    let (_provider, _reader, meter) = setup("spans_record");
    let spans = meter.span_recorder(SpanCapacity::default());

    let root = spans
        .start(SpanStart::new("put", trace_id(9), span_id(1)).kind(SpanKind::Client))
        .unwrap();
    let child = spans
        .start(
            SpanStart::new("append", trace_id(9), span_id(2))
                .parent(span_id(1))
                .flags(TraceFlags::new(3)),
        )
        .unwrap();
    spans.attribute(child, "records", 12u64);
    spans.attribute(child, "offset", -3i64);
    spans.attribute(child, "fsync", true);
    spans.attribute(child, "topic", "events");
    spans.event(child, "queued");
    spans.event(child, "written");
    spans.link(child, trace_id(4), span_id(5), TraceFlags::SAMPLED);
    spans.status(child, Status::Error("disk full"));
    spans.end(child);
    spans.end(root);

    let records = drained();
    assert_eq!(records.len(), 2);
    let (scope, child) = &records[0];
    assert_eq!(*scope, "spans_record");
    assert_eq!(child.name(), "append");
    assert_eq!(
        (child.trace_id(), child.span_id(), child.parent_span_id()),
        (trace_id(9), span_id(2), span_id(1))
    );
    assert!(!child.parent_is_remote());
    assert_eq!(child.flags(), TraceFlags::new(3));
    assert_eq!(child.kind(), SpanKind::Internal);
    assert_eq!(child.status(), Status::Error("disk full"));
    assert_eq!(
        child.attributes(),
        [
            Attribute {
                key: "records",
                value: Value::U64(12)
            },
            Attribute {
                key: "offset",
                value: Value::I64(-3)
            },
            Attribute {
                key: "fsync",
                value: Value::Bool(true)
            },
            Attribute {
                key: "topic",
                value: Value::Str("events")
            },
        ]
    );
    let events: Vec<_> = child.events().iter().map(|e| e.name).collect();
    assert_eq!(events, ["queued", "written"]);
    let ticks = [
        child.start_ticks(),
        child.events()[0].ticks,
        child.events()[1].ticks,
        child.end_ticks(),
    ];
    assert!(ticks.is_sorted(), "{ticks:?}");
    assert_eq!(child.links().len(), 1);
    assert_eq!(
        (child.links()[0].trace_id, child.links()[0].span_id),
        (trace_id(4), span_id(5))
    );

    let (_, root) = &records[1];
    assert_eq!(root.name(), "put");
    assert_eq!(root.kind(), SpanKind::Client);
    assert_eq!(root.parent_span_id(), SpanId::INVALID);
    assert_eq!(root.flags(), TraceFlags::SAMPLED);
    assert_eq!(root.status(), Status::Unset);
    assert!(root.attributes().is_empty() && root.events().is_empty());
    assert!(root.start_ticks() <= child.start_ticks() && child.end_ticks() <= root.end_ticks());
}

#[test]
fn a_full_slab_fails_the_start_and_counts_it() {
    let _turn = serial();
    let scope = "full_slab";
    let (_provider, reader, meter) = setup(scope);
    let spans = meter.span_recorder(SpanCapacity {
        active: 2,
        finished: 8,
    });
    let a = spans.start(start(1)).unwrap();
    let _b = spans.start(start(2)).unwrap();
    assert_eq!(spans.start(start(3)), None);
    assert_eq!(spans.start(start(4)), None);
    assert_eq!(spans.active(), 2);
    assert_eq!(dropped(&reader, scope, "active_full"), 2);

    spans.end(a);
    assert!(spans.start(start(5)).is_some());
    assert_eq!(dropped(&reader, scope, "active_full"), 2);
}

#[test]
fn a_full_ring_drops_the_span_and_counts_it() {
    let _turn = serial();
    let scope = "full_ring";
    let (_provider, reader, meter) = setup(scope);
    let spans = meter.span_recorder(SpanCapacity {
        active: 8,
        finished: 2,
    });
    for n in 1..=5 {
        let span = spans.start(start(n)).unwrap();
        spans.end(span);
    }
    let ids: Vec<_> = drained().iter().map(|(_, r)| r.span_id()).collect();
    assert_eq!(ids, [span_id(1), span_id(2)]);
    assert_eq!(dropped(&reader, scope, "finished_full"), 3);
    assert_eq!(spans.active(), 0);

    // Drained: room again.
    let span = spans.start(start(6)).unwrap();
    spans.end(span);
    let ids: Vec<_> = drained().iter().map(|(_, r)| r.span_id()).collect();
    assert_eq!(ids, [span_id(6)]);
}

#[test]
fn a_handle_does_nothing_once_its_span_ended() {
    let _turn = serial();
    let (_provider, _reader, meter) = setup("stale_handle");
    let spans = meter.span_recorder(SpanCapacity {
        active: 1,
        finished: 8,
    });
    let old = spans.start(start(1)).unwrap();
    spans.end(old);
    // The same slot, a new generation.
    let new = spans.start(start(2)).unwrap();
    assert_ne!(old, new);
    spans.attribute(old, "stale", true);
    spans.event(old, "stale");
    spans.status(old, Status::Ok);
    spans.end(old);
    spans.discard(old);
    assert_eq!(spans.active(), 1);
    spans.end(new);
    spans.end(new);

    let records = drained();
    let ids: Vec<_> = records.iter().map(|(_, r)| r.span_id()).collect();
    assert_eq!(ids, [span_id(1), span_id(2)]);
    let new = &records[1].1;
    assert!(new.attributes().is_empty() && new.events().is_empty());
    assert_eq!(new.status(), Status::Unset);
}

#[test]
fn discarded_spans_are_not_exported() {
    let _turn = serial();
    let (_provider, _reader, meter) = setup("discard");
    let spans = meter.span_recorder(SpanCapacity {
        active: 1,
        finished: 8,
    });
    let span = spans.start(start(1)).unwrap();
    spans.discard(span);
    assert_eq!(spans.active(), 0);
    let span = spans.start(start(2)).unwrap();
    spans.end(span);
    let ids: Vec<_> = drained().iter().map(|(_, r)| r.span_id()).collect();
    assert_eq!(ids, [span_id(2)]);
}

#[test]
fn spans_past_the_limits_count_as_dropped() {
    let _turn = serial();
    let (_provider, _reader, meter) = setup("span_limits");
    let spans = meter.span_recorder(SpanCapacity::default());
    let span = spans.start(start(1)).unwrap();
    for _ in 0..MAX_ATTRIBUTES + 2 {
        spans.attribute(span, "k", 1u64);
    }
    spans.end(span);
    let records = drained();
    assert_eq!(records[0].1.attributes().len(), MAX_ATTRIBUTES);
    assert_eq!(records[0].1.dropped_attributes(), 2);
}

#[test]
fn spans_of_a_dropped_recorder_still_drain() {
    let _turn = serial();
    let (_provider, _reader, meter) = setup("dropped_recorder");
    let spans = meter.span_recorder(SpanCapacity::default());
    let span = spans.start(start(1)).unwrap();
    spans.end(span);
    // In progress: lost with the recorder.
    spans.start(start(2)).unwrap();
    drop(spans);
    let ids: Vec<_> = drained().iter().map(|(_, r)| r.span_id()).collect();
    assert_eq!(ids, [span_id(1)]);
    assert!(drained().is_empty());
}

#[test]
fn drain_stops_at_its_limit_and_goes_on_from_there() {
    let _turn = serial();
    let (_provider, _reader, meter) = setup("drain_limit");
    let a = meter.span_recorder(SpanCapacity::default());
    let b = meter.span_recorder(SpanCapacity::default());
    for n in 0..3 {
        let span = a.start(start(n)).unwrap();
        a.end(span);
        let span = b.start(start(10 + n)).unwrap();
        b.end(span);
    }
    let mut ids = Vec::new();
    loop {
        let mut batch = Vec::new();
        let taken = trace::drain(2, |_, r| batch.push(r.span_id()));
        assert_eq!(taken, batch.len());
        assert!(taken <= 2);
        if taken == 0 {
            break;
        }
        ids.extend(batch);
    }
    ids.sort_by_key(|id| id.to_bytes());
    let expected: Vec<_> = [0, 1, 2, 10, 11, 12].map(span_id).into();
    assert_eq!(ids, expected);
}

#[test]
fn recording_does_not_allocate() {
    let _turn = serial();
    let (_provider, _reader, meter) = setup("no_alloc");
    let spans = meter.span_recorder(SpanCapacity {
        active: 4,
        finished: 4,
    });
    let record = |n: u8| {
        let Some(span) = spans.start(start(n).parent(span_id(0))) else {
            return;
        };
        spans.attribute(span, "n", u64::from(n));
        spans.attribute(span, "topic", "events");
        spans.event(span, "queued");
        spans.event(span, "written");
        spans.link(span, trace_id(2), span_id(3), TraceFlags::SAMPLED);
        spans.status(span, Status::Ok);
        spans.end(span);
    };
    // Warm up: the dropped-span counters' first add, the clock.
    record(0);
    let before = ALLOCS.with(Cell::get);
    // Fills the ring, then drops: both paths.
    for n in 1..=16 {
        record(n);
    }
    let allocs = ALLOCS.with(Cell::get) - before;
    assert_eq!(allocs, 0);
    assert_eq!(drained().len(), 4);
}

#[test]
fn spans_drain_from_another_thread_while_recording() {
    let _turn = serial();
    let scope = "concurrent_drain";
    let (_provider, reader, meter) = setup(scope);
    let total: u32 = if cfg!(miri) { 50 } else { 20_000 };
    let mut ids = Vec::new();
    thread::scope(|s| {
        let meter = &meter;
        let recorder = s.spawn(move || {
            let spans = meter.span_recorder(SpanCapacity {
                active: 4,
                finished: 16,
            });
            for n in 0..total {
                let span = spans
                    .start(SpanStart::new("op", trace_id(1), span_id(1)))
                    .unwrap();
                spans.attribute(span, "n", n);
                spans.end(span);
            }
        });
        while !recorder.is_finished() {
            trace::drain(usize::MAX, |_, r| ids.push(r.attributes()[0].value));
        }
    });
    trace::drain(usize::MAX, |_, r| ids.push(r.attributes()[0].value));
    let dropped = dropped(&reader, scope, "finished_full");
    assert_eq!(ids.len() as u64 + dropped, u64::from(total));
    // In order, none twice.
    let ns: Vec<u64> = ids
        .iter()
        .map(|v| match v {
            Value::U64(n) => *n,
            other => panic!("{other:?}"),
        })
        .collect();
    assert!(ns.windows(2).all(|w| w[0] < w[1]));
}

#[cfg(feature = "sdk-trace")]
#[test]
fn the_collector_exports_span_data() {
    let _turn = serial();
    let (_provider, _reader, meter) = setup("collector");
    let spans = meter.span_recorder(SpanCapacity::default());
    let before = SystemTime::now();
    let span = spans
        .start(
            SpanStart::new("append", trace_id(9), span_id(2))
                .remote_parent(span_id(1))
                .kind(SpanKind::Server),
        )
        .unwrap();
    spans.attribute(span, "records", 12u64);
    spans.attribute(span, "huge", u64::MAX);
    spans.attribute(span, "offset", -3i64);
    spans.attribute(span, "fsync", true);
    spans.attribute(span, "topic", "events");
    spans.event(span, "written");
    for n in 0..10 {
        spans.link(span, trace_id(4), span_id(n), TraceFlags::SAMPLED);
    }
    spans.status(span, Status::Error("disk full"));
    thread::sleep(Duration::from_millis(2));
    spans.end(span);
    let after = SystemTime::now();

    let exporter = InMemory::default();
    futures_executor::block_on(Collector::new().export(&exporter)).unwrap();
    let batches = exporter.0.into_inner().unwrap();
    assert_eq!(batches.len(), 1);
    let data = &batches[0][0];
    assert_eq!(data.name, "append");
    assert_eq!(data.span_context.trace_id(), trace_id(9));
    assert_eq!(data.span_context.span_id(), span_id(2));
    assert!(data.span_context.is_sampled());
    assert_eq!(data.parent_span_id, span_id(1));
    assert!(data.parent_span_is_remote);
    assert_eq!(data.span_kind, otel::SpanKind::Server);
    assert_eq!(data.instrumentation_scope.name(), "collector");
    assert_eq!(
        data.status,
        otel::Status::Error {
            description: "disk full".into()
        }
    );
    assert_eq!(
        data.attributes,
        [
            KeyValue::new("records", 12),
            KeyValue::new("huge", u64::MAX.to_string()),
            KeyValue::new("offset", -3),
            KeyValue::new("fsync", true),
            KeyValue::new("topic", "events"),
        ]
    );
    assert_eq!(data.dropped_attributes_count, 0);
    assert_eq!(data.events.len(), 1);
    assert_eq!(data.events[0].name, "written");
    assert_eq!(data.links.len(), 8);
    assert_eq!(data.links.dropped_count, 2);
    assert_eq!(data.links[3].span_context.span_id(), span_id(3));

    // Wall times, within the test's, give or take the clock's calibration.
    let slack = Duration::from_millis(1);
    assert!(data.start_time + slack >= before, "{data:?}");
    assert!(data.end_time <= after + slack, "{data:?}");
    assert!(data.start_time <= data.events[0].timestamp);
    assert!(data.events[0].timestamp <= data.end_time);
    let elapsed = data.end_time.duration_since(data.start_time).unwrap();
    assert!(elapsed >= Duration::from_millis(1), "{elapsed:?}");
}

#[cfg(feature = "sdk-trace")]
#[test]
fn the_collector_exports_in_batches() {
    let _turn = serial();
    let (_provider, _reader, meter) = setup("batches");
    let spans = meter.span_recorder(SpanCapacity::default());
    let end = |n| {
        let span = spans.start(start(n)).unwrap();
        spans.end(span);
    };
    (0..5).for_each(end);
    let exporter = InMemory::default();
    let collector = Collector::new().with_max_batch(2);
    futures_executor::block_on(collector.export(&exporter)).unwrap();
    let sizes: Vec<_> = exporter.0.lock().unwrap().iter().map(Vec::len).collect();
    assert_eq!(sizes, [2, 2, 1]);

    // Nothing to export: no call.
    futures_executor::block_on(collector.export(&exporter)).unwrap();
    assert_eq!(exporter.0.lock().unwrap().len(), 3);
}
