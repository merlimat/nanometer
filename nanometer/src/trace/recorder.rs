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

use std::cell::UnsafeCell;
use std::fmt;
use std::num::NonZeroU32;
use std::sync::Arc;

use opentelemetry::{KeyValue, SpanId, TraceFlags, TraceId};

use super::ring::{self, Producer, Ring};
use super::{Attribute, SpanEvent, SpanLink, SpanRecord, SpanStart, Status, Value};
use crate::latency::{self, Clock};
use crate::{CounterDef, LocalCounter, Meter};

/// Counts spans a recorder dropped, by `reason`: `active_full` when a span couldn't
/// start, `finished_full` when a finished span didn't fit in the ring.
const DROPPED: CounterDef = CounterDef::new("nanometer.spans.dropped")
    .unit("{span}")
    .description("Sampled spans that nanometer dropped, by reason");

/// Most spans in progress per recorder: an [`ActiveSpan`] has 16 bits of slot index.
const MAX_ACTIVE: usize = 1 << 16;

/// How many spans a [`SpanRecorder`] holds: in progress, and finished but not yet
/// drained.
///
/// Each span takes a [`SpanRecord`] of 784 B in either place, allocated up front.
/// Size the finished ring for the spans a core finishes between two drains, with
/// headroom: at 20,000 spans per second per core and a drain every 100 ms, 2,000.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SpanCapacity {
    /// Spans in progress, at most 65,536. Starting one more fails.
    pub active: usize,
    /// Finished spans waiting for a drain, rounded up to a power of two. Ending one
    /// more drops it.
    pub finished: usize,
}

impl Default for SpanCapacity {
    /// 1,024 in progress and 4,096 finished: 3.8 MiB.
    fn default() -> Self {
        SpanCapacity {
            active: 1_024,
            finished: 4_096,
        }
    }
}

/// A span in progress in a [`SpanRecorder`]: 4 bytes, `Copy`, and `Option<ActiveSpan>`
/// is 4 bytes too.
///
/// A slot index and a generation: once the span ends, the recorder ignores the
/// handle, even after the slot is reused (until the generation wraps, after 65,535
/// reuses of the slot). A handle belongs to the recorder that started it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ActiveSpan(NonZeroU32);

impl ActiveSpan {
    #[inline]
    fn new(index: usize, generation: u16) -> Self {
        let raw = (u32::from(generation) << 16) | index as u32;
        // `generation` is never 0.
        ActiveSpan(NonZeroU32::new(raw).unwrap())
    }

    #[inline]
    fn index(self) -> usize {
        (self.0.get() & 0xffff) as usize
    }

    #[inline]
    fn generation(self) -> u16 {
        (self.0.get() >> 16) as u16
    }

    /// The handle as a `u32`, never 0, e.g. to store in a field of an atomic or packed
    /// struct.
    pub fn to_raw(self) -> NonZeroU32 {
        self.0
    }

    /// The handle back from [`to_raw`](ActiveSpan::to_raw).
    pub fn from_raw(raw: NonZeroU32) -> Self {
        ActiveSpan(raw)
    }
}

struct Slot {
    record: SpanRecord,
    /// The generation of the span in this slot, or of the last one.
    generation: u16,
    live: bool,
}

struct Inner {
    slots: Box<[Slot]>,
    /// Indices of free slots, used last in, first out, so the next span reuses the
    /// slot that is likeliest in cache. Its capacity is the slab's: it never grows.
    free: Vec<u16>,
    producer: Producer,
}

impl Inner {
    #[inline]
    fn live(&mut self, span: ActiveSpan) -> Option<&mut Slot> {
        let slot = self.slots.get_mut(span.index())?;
        (slot.live && slot.generation == span.generation()).then_some(slot)
    }
}

/// Records sampled spans for one thread or core.
///
/// Keep one per core, as a plain field of the core's state. Spans in progress live in
/// a slab allocated up front and are addressed by an [`ActiveSpan`]; finished spans
/// go to a ring that only this recorder writes and [`drain`](super::drain) empties,
/// from another thread. Nothing on the way allocates or takes a lock.
///
/// When the slab is full, [`start`](SpanRecorder::start) returns `None`; when the
/// ring is full, [`end`](SpanRecorder::end) drops the span. Both count in the
/// counter `nanometer.spans.dropped` of the recorder's meter, by `reason`
/// (`active_full`, `finished_full`). A span's attributes, events and links past
/// [`MAX_ATTRIBUTES`](super::MAX_ATTRIBUTES), [`MAX_EVENTS`](super::MAX_EVENTS) and
/// [`MAX_LINKS`](super::MAX_LINKS) are dropped and counted in the span, as OTel does.
///
/// Calls with a handle whose span has ended do nothing. `!Send` and `!Sync`.
pub struct SpanRecorder {
    inner: UnsafeCell<Inner>,
    clock: Clock,
    active_full: LocalCounter,
    finished_full: LocalCounter,
}

impl SpanRecorder {
    // Cold: keeps setup out of the code around the record path.
    #[inline(never)]
    pub(crate) fn new(meter: &Meter, capacity: SpanCapacity) -> Self {
        let active = capacity.active.clamp(1, MAX_ACTIVE);
        let slots = (0..active)
            .map(|_| Slot {
                record: SpanRecord::EMPTY,
                generation: 0,
                live: false,
            })
            .collect();
        // Popped from the end: slot 0 first.
        let free = (0..active).rev().map(|i| i as u16).collect();
        let ring = Arc::new(Ring::new(capacity.finished, meter.scope));
        ring::register(&ring);
        let dropped = |reason| meter.counter(DROPPED, &[KeyValue::new("reason", reason)]);
        SpanRecorder {
            inner: UnsafeCell::new(Inner {
                slots,
                free,
                producer: Producer::new(ring),
            }),
            clock: latency::ticks().clock.clone(),
            active_full: dropped("active_full"),
            finished_full: dropped("finished_full"),
        }
    }

    #[inline(always)]
    #[allow(clippy::mut_from_ref)]
    fn inner(&self) -> &mut Inner {
        // SAFETY: `SpanRecorder` is `!Sync` (UnsafeCell) and `!Send` (LocalCounter),
        // and no method calls out while holding this reference, so it is the only one.
        unsafe { &mut *self.inner.get() }
    }

    /// The clock's reading now, in ticks: the clock that span times are in.
    #[inline]
    pub fn now(&self) -> u64 {
        self.clock.raw()
    }

    /// Starts a span now, or returns `None` if the recorder holds as many spans in
    /// progress as it can.
    #[inline]
    pub fn start(&self, start: SpanStart) -> Option<ActiveSpan> {
        self.start_at(start, self.clock.raw())
    }

    /// Starts a span at `ticks` of the clock (see [`now`](SpanRecorder::now)), e.g.
    /// when the operation started before it was sampled.
    #[inline]
    pub fn start_at(&self, start: SpanStart, ticks: u64) -> Option<ActiveSpan> {
        let inner = self.inner();
        let Some(index) = inner.free.pop() else {
            self.active_full.add(1);
            return None;
        };
        let slot = &mut inner.slots[usize::from(index)];
        slot.generation = slot.generation.checked_add(1).unwrap_or(1);
        slot.live = true;
        slot.record.reset(&start, ticks);
        Some(ActiveSpan::new(usize::from(index), slot.generation))
    }

    /// Adds an attribute. Add each key once: a repeated key is exported twice.
    #[inline]
    pub fn attribute(&self, span: ActiveSpan, key: &'static str, value: impl Into<Value>) {
        if let Some(slot) = self.inner().live(span) {
            slot.record.push_attribute(Attribute {
                key,
                value: value.into(),
            });
        }
    }

    /// Adds an event, now.
    #[inline]
    pub fn event(&self, span: ActiveSpan, name: &'static str) {
        let ticks = self.clock.raw();
        if let Some(slot) = self.inner().live(span) {
            slot.record.push_event(SpanEvent { name, ticks });
        }
    }

    /// Adds a link to another span, e.g. from a batch to the requests it carries.
    #[inline]
    pub fn link(&self, span: ActiveSpan, trace_id: TraceId, span_id: SpanId, flags: TraceFlags) {
        if let Some(slot) = self.inner().live(span) {
            slot.record.push_link(SpanLink {
                trace_id,
                span_id,
                flags,
            });
        }
    }

    /// Sets the status, replacing any set before.
    #[inline]
    pub fn status(&self, span: ActiveSpan, status: Status) {
        if let Some(slot) = self.inner().live(span) {
            slot.record.set_status(status);
        }
    }

    /// Ends the span now and queues it for export, or drops it if the ring is full.
    #[inline]
    pub fn end(&self, span: ActiveSpan) {
        self.end_at(span, self.clock.raw());
    }

    /// Ends the span at `ticks` of the clock, as [`end`](SpanRecorder::end).
    #[inline]
    pub fn end_at(&self, span: ActiveSpan, ticks: u64) {
        let inner = self.inner();
        let Some(slot) = inner.live(span) else {
            return;
        };
        slot.live = false;
        slot.record.set_end(ticks);
        let pushed = inner.producer.push(&inner.slots[span.index()].record);
        inner.free.push(span.index() as u16);
        if !pushed {
            self.finished_full.add(1);
        }
    }

    /// Ends the span without exporting it, e.g. for an operation that turned out not
    /// to matter.
    #[inline]
    pub fn discard(&self, span: ActiveSpan) {
        let inner = self.inner();
        if let Some(slot) = inner.live(span) {
            slot.live = false;
            inner.free.push(span.index() as u16);
        }
    }

    /// The number of spans in progress.
    pub fn active(&self) -> usize {
        let inner = self.inner();
        inner.slots.len() - inner.free.len()
    }
}

impl fmt::Debug for SpanRecorder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = self.inner();
        f.debug_struct("SpanRecorder")
            .field("active", &(inner.slots.len() - inner.free.len()))
            .field("capacity", &inner.slots.len())
            .finish_non_exhaustive()
    }
}
