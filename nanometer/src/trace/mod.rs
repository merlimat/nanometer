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

//! Span recording for sampled OpenTelemetry spans, on the hot path of a thread per
//! core.
//!
//! Each thread or core owns a [`SpanRecorder`]: spans in progress live in its
//! preallocated slab, addressed by an [`ActiveSpan`], and finished spans go to a
//! bounded ring that only that recorder writes. Starting, annotating and ending a span
//! takes no lock and allocates nothing. An exporter on another thread takes the
//! finished spans from every recorder's ring, with [`drain`], or as OTel SDK
//! `SpanData` through `Collector` (feature `sdk-trace`).
//!
//! nanometer records the spans it is given: sampling, trace IDs and context
//! propagation are the application's.
//!
//! ```
//! use nanometer::trace::{SpanCapacity, SpanKind, SpanStart, Status};
//! use opentelemetry::{SpanId, TraceId};
//!
//! # let meter = nanometer::Meter::new(&*opentelemetry::global::meter_provider(), "doc");
//! // Once per core, kept in the core's state.
//! let spans = meter.span_recorder(SpanCapacity::default());
//!
//! // For a request the application sampled:
//! let trace_id = TraceId::from_bytes([7; 16]);
//! let start = SpanStart::new("append", trace_id, SpanId::from_bytes([1; 8]))
//!     .kind(SpanKind::Server);
//! if let Some(span) = spans.start(start) {
//!     spans.attribute(span, "records", 12u64);
//!     spans.event(span, "written");
//!     spans.status(span, Status::Ok);
//!     spans.end(span);
//! }
//! ```

mod recorder;
mod ring;

#[cfg(feature = "sdk-trace")]
mod export;

use std::time::{Duration, SystemTime};

use opentelemetry::{SpanId, TraceFlags, TraceId};

#[cfg(feature = "sdk-trace")]
pub use export::Collector;
pub use recorder::{ActiveSpan, SpanCapacity, SpanRecorder};
pub use ring::drain;

use crate::latency;

/// Attributes a span holds; more are counted as dropped.
pub const MAX_ATTRIBUTES: usize = 8;
/// Events a span holds; more are counted as dropped.
pub const MAX_EVENTS: usize = 4;
/// Links a span holds; more are counted as dropped.
pub const MAX_LINKS: usize = 8;

/// The role of a span, as OpenTelemetry's `SpanKind`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum SpanKind {
    /// An operation inside the application, the default.
    #[default]
    Internal,
    /// The server side of a remote request.
    Server,
    /// The client side of a remote request.
    Client,
    /// The sender of an asynchronous message.
    Producer,
    /// The receiver of an asynchronous message.
    Consumer,
}

/// The outcome of a span, as OpenTelemetry's `Status`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum Status {
    /// Not set, the default.
    #[default]
    Unset,
    /// Completed successfully.
    Ok,
    /// Failed, with a description.
    Error(&'static str),
}

/// An attribute value. OTLP integers are signed: a `U64` above `i64::MAX` exports as
/// its decimal string.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Value {
    /// An unsigned integer.
    U64(u64),
    /// A signed integer.
    I64(i64),
    /// A boolean.
    Bool(bool),
    /// A static string.
    Str(&'static str),
}

impl From<u64> for Value {
    fn from(v: u64) -> Self {
        Value::U64(v)
    }
}

impl From<u32> for Value {
    fn from(v: u32) -> Self {
        Value::U64(v.into())
    }
}

impl From<usize> for Value {
    fn from(v: usize) -> Self {
        Value::U64(v as u64)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::I64(v)
    }
}

impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::I64(v.into())
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}

impl From<&'static str> for Value {
    fn from(v: &'static str) -> Self {
        Value::Str(v)
    }
}

/// A span attribute.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Attribute {
    /// The key.
    pub key: &'static str,
    /// The value.
    pub value: Value,
}

/// A span event: a name and a time, in clock ticks.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SpanEvent {
    /// The name.
    pub name: &'static str,
    /// When it happened, in ticks of nanometer's clock (see [`Anchor`]).
    pub ticks: u64,
}

/// A link to another span.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SpanLink {
    /// The linked span's trace.
    pub trace_id: TraceId,
    /// The linked span.
    pub span_id: SpanId,
    /// The linked span's trace flags.
    pub flags: TraceFlags,
}

/// What a span starts with: its name, IDs, parent and kind.
///
/// A root span by default, of kind [`Internal`](SpanKind::Internal), with the sampled
/// flag set.
///
/// ```
/// use nanometer::trace::{SpanKind, SpanStart};
/// use opentelemetry::{SpanId, TraceId};
///
/// let start = SpanStart::new("put", TraceId::from_bytes([1; 16]), SpanId::from_bytes([2; 8]))
///     .remote_parent(SpanId::from_bytes([3; 8]))
///     .kind(SpanKind::Server);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SpanStart {
    name: &'static str,
    trace_id: TraceId,
    span_id: SpanId,
    parent_span_id: SpanId,
    parent_is_remote: bool,
    flags: TraceFlags,
    kind: SpanKind,
}

impl SpanStart {
    /// A root span called `name`, with these IDs.
    #[inline]
    pub const fn new(name: &'static str, trace_id: TraceId, span_id: SpanId) -> Self {
        SpanStart {
            name,
            trace_id,
            span_id,
            parent_span_id: SpanId::INVALID,
            parent_is_remote: false,
            flags: TraceFlags::SAMPLED,
            kind: SpanKind::Internal,
        }
    }

    /// A child of `parent`, a span of this process.
    #[inline]
    pub const fn parent(mut self, parent: SpanId) -> Self {
        self.parent_span_id = parent;
        self.parent_is_remote = false;
        self
    }

    /// A child of `parent`, a span of another process, e.g. the client's span of a
    /// request this server handles.
    #[inline]
    pub const fn remote_parent(mut self, parent: SpanId) -> Self {
        self.parent_span_id = parent;
        self.parent_is_remote = true;
        self
    }

    /// Sets the kind.
    #[inline]
    pub const fn kind(mut self, kind: SpanKind) -> Self {
        self.kind = kind;
        self
    }

    /// Sets the trace flags, `SAMPLED` by default.
    #[inline]
    pub const fn flags(mut self, flags: TraceFlags) -> Self {
        self.flags = flags;
        self
    }
}

/// A span, as recorded: fixed size, with no heap data. Times are in ticks of
/// nanometer's clock; [`Anchor`] converts them to wall time.
#[derive(Clone, Copy, Debug)]
pub struct SpanRecord {
    trace_id: TraceId,
    span_id: SpanId,
    parent_span_id: SpanId,
    name: &'static str,
    start: u64,
    end: u64,
    status: Status,
    flags: TraceFlags,
    kind: SpanKind,
    parent_is_remote: bool,
    attribute_count: u8,
    event_count: u8,
    link_count: u8,
    dropped_attributes: u16,
    dropped_events: u16,
    dropped_links: u16,
    attributes: [Attribute; MAX_ATTRIBUTES],
    events: [SpanEvent; MAX_EVENTS],
    links: [SpanLink; MAX_LINKS],
}

impl SpanRecord {
    pub(crate) const EMPTY: SpanRecord = SpanRecord {
        trace_id: TraceId::INVALID,
        span_id: SpanId::INVALID,
        parent_span_id: SpanId::INVALID,
        name: "",
        start: 0,
        end: 0,
        status: Status::Unset,
        flags: TraceFlags::NOT_SAMPLED,
        kind: SpanKind::Internal,
        parent_is_remote: false,
        attribute_count: 0,
        event_count: 0,
        link_count: 0,
        dropped_attributes: 0,
        dropped_events: 0,
        dropped_links: 0,
        attributes: [Attribute {
            key: "",
            value: Value::Bool(false),
        }; MAX_ATTRIBUTES],
        events: [SpanEvent { name: "", ticks: 0 }; MAX_EVENTS],
        links: [SpanLink {
            trace_id: TraceId::INVALID,
            span_id: SpanId::INVALID,
            flags: TraceFlags::NOT_SAMPLED,
        }; MAX_LINKS],
    };

    /// Starts this record over as `start`, at `ticks`; leaves the arrays, which only
    /// count up to their lengths.
    #[inline]
    pub(crate) fn reset(&mut self, start: &SpanStart, ticks: u64) {
        self.trace_id = start.trace_id;
        self.span_id = start.span_id;
        self.parent_span_id = start.parent_span_id;
        self.name = start.name;
        self.start = ticks;
        self.end = ticks;
        self.status = Status::Unset;
        self.flags = start.flags;
        self.kind = start.kind;
        self.parent_is_remote = start.parent_is_remote;
        self.attribute_count = 0;
        self.event_count = 0;
        self.link_count = 0;
        self.dropped_attributes = 0;
        self.dropped_events = 0;
        self.dropped_links = 0;
    }

    /// Copies `src` into this record, but of its arrays only the entries in use: a
    /// span with two events copies 160 B, not all 784.
    #[inline]
    pub(crate) fn copy_from(&mut self, src: &SpanRecord) {
        // Exhaustive: a new field fails to compile here until it is copied.
        let &SpanRecord {
            trace_id,
            span_id,
            parent_span_id,
            name,
            start,
            end,
            status,
            flags,
            kind,
            parent_is_remote,
            attribute_count,
            event_count,
            link_count,
            dropped_attributes,
            dropped_events,
            dropped_links,
            attributes: _,
            events: _,
            links: _,
        } = src;
        self.trace_id = trace_id;
        self.span_id = span_id;
        self.parent_span_id = parent_span_id;
        self.name = name;
        self.start = start;
        self.end = end;
        self.status = status;
        self.flags = flags;
        self.kind = kind;
        self.parent_is_remote = parent_is_remote;
        self.attribute_count = attribute_count;
        self.event_count = event_count;
        self.link_count = link_count;
        self.dropped_attributes = dropped_attributes;
        self.dropped_events = dropped_events;
        self.dropped_links = dropped_links;
        let (a, e, l) = (
            usize::from(attribute_count),
            usize::from(event_count),
            usize::from(link_count),
        );
        self.attributes[..a].copy_from_slice(&src.attributes[..a]);
        self.events[..e].copy_from_slice(&src.events[..e]);
        self.links[..l].copy_from_slice(&src.links[..l]);
    }

    #[inline]
    pub(crate) fn push_attribute(&mut self, attribute: Attribute) {
        match self.attributes.get_mut(self.attribute_count as usize) {
            Some(slot) => {
                *slot = attribute;
                self.attribute_count += 1;
            }
            None => self.dropped_attributes = self.dropped_attributes.saturating_add(1),
        }
    }

    #[inline]
    pub(crate) fn push_event(&mut self, event: SpanEvent) {
        match self.events.get_mut(self.event_count as usize) {
            Some(slot) => {
                *slot = event;
                self.event_count += 1;
            }
            None => self.dropped_events = self.dropped_events.saturating_add(1),
        }
    }

    #[inline]
    pub(crate) fn push_link(&mut self, link: SpanLink) {
        match self.links.get_mut(self.link_count as usize) {
            Some(slot) => {
                *slot = link;
                self.link_count += 1;
            }
            None => self.dropped_links = self.dropped_links.saturating_add(1),
        }
    }

    #[inline]
    pub(crate) fn set_status(&mut self, status: Status) {
        self.status = status;
    }

    #[inline]
    pub(crate) fn set_end(&mut self, ticks: u64) {
        self.end = ticks;
    }

    /// The trace ID.
    pub fn trace_id(&self) -> TraceId {
        self.trace_id
    }

    /// The span ID.
    pub fn span_id(&self) -> SpanId {
        self.span_id
    }

    /// The parent's span ID, `SpanId::INVALID` for a root span.
    pub fn parent_span_id(&self) -> SpanId {
        self.parent_span_id
    }

    /// Whether the parent is a span of another process.
    pub fn parent_is_remote(&self) -> bool {
        self.parent_is_remote
    }

    /// The trace flags.
    pub fn flags(&self) -> TraceFlags {
        self.flags
    }

    /// The kind.
    pub fn kind(&self) -> SpanKind {
        self.kind
    }

    /// The name.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// The start, in clock ticks.
    pub fn start_ticks(&self) -> u64 {
        self.start
    }

    /// The end, in clock ticks.
    pub fn end_ticks(&self) -> u64 {
        self.end
    }

    /// The status.
    pub fn status(&self) -> Status {
        self.status
    }

    /// The attributes, in the order they were added.
    pub fn attributes(&self) -> &[Attribute] {
        &self.attributes[..self.attribute_count as usize]
    }

    /// The events, in the order they were added.
    pub fn events(&self) -> &[SpanEvent] {
        &self.events[..self.event_count as usize]
    }

    /// The links, in the order they were added.
    pub fn links(&self) -> &[SpanLink] {
        &self.links[..self.link_count as usize]
    }

    /// Attributes that didn't fit, past [`MAX_ATTRIBUTES`].
    pub fn dropped_attributes(&self) -> u32 {
        self.dropped_attributes.into()
    }

    /// Events that didn't fit, past [`MAX_EVENTS`].
    pub fn dropped_events(&self) -> u32 {
        self.dropped_events.into()
    }

    /// Links that didn't fit, past [`MAX_LINKS`].
    pub fn dropped_links(&self) -> u32 {
        self.dropped_links.into()
    }
}

/// A wall-clock time paired with a reading of nanometer's clock, to convert span
/// times from clock ticks to wall time.
///
/// The clock counts at a steady rate while the wall clock is adjusted, so take a
/// fresh anchor for each batch of spans: the time between the anchor and a span is
/// then short, and the drift over it negligible.
#[derive(Clone, Copy, Debug)]
pub struct Anchor {
    wall: SystemTime,
    ticks: u64,
    ns_per_tick: f64,
}

impl Anchor {
    /// The wall-clock time now, paired with the clock's reading. The first call in a
    /// process, if no latency handle or span recorder calibrated the clock, blocks for
    /// 10 to 200 ms to calibrate it.
    pub fn now() -> Self {
        let ticks = latency::ticks();
        // Pair the wall time with the middle of two clock readings.
        let before = ticks.clock.raw();
        let wall = SystemTime::now();
        let after = ticks.clock.raw();
        Anchor {
            wall,
            ticks: before + (after.saturating_sub(before)) / 2,
            ns_per_tick: 1.0 / ticks.ticks_per_ns,
        }
    }

    /// The wall time at `ticks` of the clock.
    pub fn system_time(&self, ticks: u64) -> SystemTime {
        let delta = ticks as i128 - self.ticks as i128;
        let ns = (delta.unsigned_abs() as f64 * self.ns_per_tick) as u64;
        let offset = Duration::from_nanos(ns);
        if delta >= 0 {
            self.wall + offset
        } else {
            self.wall - offset
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_size() {
        // Copied once per finished span, slab to ring: keep an eye on it.
        assert_eq!(std::mem::size_of::<SpanRecord>(), 784);
    }

    #[test]
    fn arrays_overflow_into_dropped_counts() {
        let mut record = SpanRecord::EMPTY;
        for i in 0..MAX_ATTRIBUTES + 2 {
            record.push_attribute(Attribute {
                key: "k",
                value: Value::U64(i as u64),
            });
        }
        for _ in 0..MAX_EVENTS + 3 {
            record.push_event(SpanEvent {
                name: "e",
                ticks: 1,
            });
        }
        for _ in 0..MAX_LINKS + 1 {
            record.push_link(SpanLink {
                trace_id: TraceId::INVALID,
                span_id: SpanId::INVALID,
                flags: TraceFlags::SAMPLED,
            });
        }
        assert_eq!(record.attributes().len(), MAX_ATTRIBUTES);
        assert_eq!(record.dropped_attributes(), 2);
        assert_eq!(record.events().len(), MAX_EVENTS);
        assert_eq!(record.dropped_events(), 3);
        assert_eq!(record.links().len(), MAX_LINKS);
        assert_eq!(record.dropped_links(), 1);
    }

    #[test]
    fn anchor_converts_both_ways() {
        let anchor = Anchor {
            wall: SystemTime::UNIX_EPOCH + Duration::from_secs(1_000),
            ticks: 1_000_000,
            ns_per_tick: 0.5,
        };
        let at = |ticks| {
            anchor
                .system_time(ticks)
                .duration_since(SystemTime::UNIX_EPOCH)
        };
        assert_eq!(at(1_000_000).unwrap(), Duration::from_secs(1_000));
        assert_eq!(
            at(1_000_100).unwrap(),
            Duration::from_nanos(1_000_000_000_050)
        );
        assert_eq!(at(999_900).unwrap(), Duration::from_nanos(999_999_999_950));
    }
}
