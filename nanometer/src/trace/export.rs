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

use std::borrow::Cow;

use opentelemetry::trace::{self as otel, Event, Link, SpanContext, TraceState};
use opentelemetry::{InstrumentationScope, Key, KeyValue, StringValue};
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::trace::{SpanData, SpanEvents, SpanExporter, SpanLinks};

use super::{Anchor, Attribute, SpanKind, SpanRecord, Status, Value, drain};

/// Exports the spans of every [`SpanRecorder`](super::SpanRecorder) in the process
/// through an OTel SDK `SpanExporter`, such as OTLP's.
///
/// Call [`export`](Collector::export) from one thread, often enough that no
/// recorder's ring fills between two calls: every 100 ms, say. It drains the rings
/// into batches of at most `max_batch` spans and exports them one after the other.
/// The SDK's exporters are async; with a blocking one, drive it as the SDK's batch
/// processor does:
///
/// ```no_run
/// # fn run(exporter: impl opentelemetry_sdk::trace::SpanExporter + 'static) {
/// let collector = nanometer::trace::Collector::new();
/// std::thread::spawn(move || loop {
///     std::thread::sleep(std::time::Duration::from_millis(100));
///     if let Err(err) = futures_executor::block_on(collector.export(&exporter)) {
///         eprintln!("span export failed: {err}");
///     }
/// });
/// # }
/// ```
///
/// Give the exporter the app's `Resource` with `SpanExporter::set_resource` first.
/// Collectors share the rings: two of them split the spans between them.
#[derive(Clone, Debug)]
pub struct Collector {
    max_batch: usize,
}

impl Default for Collector {
    fn default() -> Self {
        Collector::new()
    }
}

impl Collector {
    /// A collector with batches of at most 512 spans, the SDK batch processor's
    /// default.
    pub fn new() -> Self {
        Collector { max_batch: 512 }
    }

    /// Sets the most spans per exported batch.
    pub fn with_max_batch(mut self, max_batch: usize) -> Self {
        self.max_batch = max_batch.max(1);
        self
    }

    /// Takes up to `max` finished spans from the recorders' rings, as `SpanData`.
    pub fn collect(&self, max: usize) -> Vec<SpanData> {
        let anchor = Anchor::now();
        let mut spans = Vec::new();
        drain(max, |scope, record| {
            spans.push(span_data(&anchor, scope, record))
        });
        spans
    }

    /// Drains the recorders' rings and exports their spans, in batches of at most
    /// `max_batch`. Stops at the first batch that isn't full, so a recorder that keeps
    /// finishing spans doesn't keep it going forever, or at the first error, which it
    /// returns: the spans of the failed batch are lost.
    pub async fn export<E: SpanExporter>(&self, exporter: &E) -> OTelSdkResult {
        loop {
            let batch = self.collect(self.max_batch);
            if batch.is_empty() {
                return Ok(());
            }
            let full = batch.len() == self.max_batch;
            exporter.export(batch).await?;
            if !full {
                return Ok(());
            }
        }
    }
}

fn span_data(anchor: &Anchor, scope: &'static str, record: &SpanRecord) -> SpanData {
    let mut events = SpanEvents::default();
    events.events = record
        .events()
        .iter()
        .map(|e| Event::new(e.name, anchor.system_time(e.ticks), Vec::new(), 0))
        .collect();
    events.dropped_count = record.dropped_events();
    let mut links = SpanLinks::default();
    links.links = record
        .links()
        .iter()
        .map(|l| {
            let context = SpanContext::new(l.trace_id, l.span_id, l.flags, false, TraceState::NONE);
            Link::new(context, Vec::new(), 0)
        })
        .collect();
    links.dropped_count = record.dropped_links();
    SpanData {
        span_context: SpanContext::new(
            record.trace_id(),
            record.span_id(),
            record.flags(),
            false,
            TraceState::NONE,
        ),
        parent_span_id: record.parent_span_id(),
        parent_span_is_remote: record.parent_is_remote(),
        span_kind: match record.kind() {
            SpanKind::Internal => otel::SpanKind::Internal,
            SpanKind::Server => otel::SpanKind::Server,
            SpanKind::Client => otel::SpanKind::Client,
            SpanKind::Producer => otel::SpanKind::Producer,
            SpanKind::Consumer => otel::SpanKind::Consumer,
        },
        name: Cow::Borrowed(record.name()),
        start_time: anchor.system_time(record.start_ticks()),
        end_time: anchor.system_time(record.end_ticks()),
        attributes: record.attributes().iter().map(key_value).collect(),
        dropped_attributes_count: record.dropped_attributes(),
        events,
        links,
        status: match record.status() {
            Status::Unset => otel::Status::Unset,
            Status::Ok => otel::Status::Ok,
            Status::Error(description) => otel::Status::Error {
                description: Cow::Borrowed(description),
            },
        },
        instrumentation_scope: InstrumentationScope::builder(scope).build(),
    }
}

fn key_value(attribute: &Attribute) -> KeyValue {
    let value: opentelemetry::Value = match attribute.value {
        Value::U64(v) => match i64::try_from(v) {
            Ok(v) => v.into(),
            Err(_) => StringValue::from(v.to_string()).into(),
        },
        Value::I64(v) => v.into(),
        Value::Bool(v) => v.into(),
        Value::Str(v) => v.into(),
    };
    KeyValue::new(Key::from_static_str(attribute.key), value)
}
