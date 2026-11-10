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

//! Finished spans: one single-producer, single-consumer ring per recorder, and the
//! process-wide list of rings that [`drain`] empties.

use std::cell::UnsafeCell;
use std::iter;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use super::SpanRecord;

// 128 B rather than 64: adjacent-line prefetchers pull 64 B lines in pairs.
#[repr(align(128))]
struct Padded<T>(T);

/// Finished spans of one recorder. Its recorder is the only producer; the consumer
/// holds the registry lock.
pub(crate) struct Ring {
    slots: Box<[UnsafeCell<SpanRecord>]>,
    mask: usize,
    /// Spans pushed, ever: written by the producer only.
    head: Padded<AtomicUsize>,
    /// Spans taken, ever: written by the consumer only.
    tail: Padded<AtomicUsize>,
    /// The recorder is gone: drop the ring once it is empty.
    closed: AtomicBool,
    pub(crate) scope: &'static str,
}

// SAFETY: a slot is written by the producer only while the consumer can't read it
// (between `tail` and `tail + capacity`, outside `tail..head`), and read by the
// consumer only after the producer's `head` store has published it; `head` and
// `tail` hand slots back and forth with release stores and acquire loads.
unsafe impl Sync for Ring {}

impl Ring {
    /// A ring of at least `capacity` slots, rounded up to a power of two.
    pub(crate) fn new(capacity: usize, scope: &'static str) -> Self {
        let capacity = capacity.max(1).next_power_of_two();
        Ring {
            slots: iter::repeat_with(|| UnsafeCell::new(SpanRecord::EMPTY))
                .take(capacity)
                .collect(),
            mask: capacity - 1,
            head: Padded(AtomicUsize::new(0)),
            tail: Padded(AtomicUsize::new(0)),
            closed: AtomicBool::new(false),
            scope,
        }
    }
}

/// The producer's end of a ring, kept by its recorder.
pub(crate) struct Producer {
    ring: Arc<Ring>,
    /// Equal to `ring.head`, which only this producer writes.
    head: usize,
    /// `ring.tail` as last seen: read again only when the ring looks full, so a push
    /// doesn't touch the consumer's cache line.
    tail: usize,
}

impl Producer {
    pub(crate) fn new(ring: Arc<Ring>) -> Self {
        Producer {
            ring,
            head: 0,
            tail: 0,
        }
    }

    /// Copies `record` into the ring, or returns false if it is full.
    #[inline]
    pub(crate) fn push(&mut self, record: &SpanRecord) -> bool {
        let capacity = self.ring.slots.len();
        if self.head - self.tail == capacity {
            self.tail = self.ring.tail.0.load(Ordering::Acquire);
            if self.head - self.tail == capacity {
                return false;
            }
        }
        let slot = &self.ring.slots[self.head & self.ring.mask];
        // SAFETY: see `Ring`: `head - tail < capacity`, so the consumer is done with
        // this slot and won't read it before the store below.
        unsafe { (*slot.get()).copy_from(record) };
        self.head += 1;
        self.ring.head.0.store(self.head, Ordering::Release);
        true
    }
}

impl Drop for Producer {
    fn drop(&mut self) {
        let mut rings = lock();
        if self.ring.head.0.load(Ordering::Relaxed) == self.ring.tail.0.load(Ordering::Acquire) {
            // Empty: nothing left to export.
            rings.list.retain(|ring| !Arc::ptr_eq(ring, &self.ring));
        } else {
            // The next drain exports what is left, then drops the ring.
            self.ring.closed.store(true, Ordering::Relaxed);
        }
    }
}

pub(crate) struct Rings {
    list: Vec<Arc<Ring>>,
    /// Where the next drain starts, so that a drain cut short by its limit doesn't
    /// always favour the first rings.
    next: usize,
}

static RINGS: Mutex<Rings> = Mutex::new(Rings {
    list: Vec::new(),
    next: 0,
});

fn lock() -> MutexGuard<'static, Rings> {
    // Nothing panics while holding the lock but a drain callback, which leaves the
    // list consistent.
    RINGS.lock().unwrap_or_else(|e| e.into_inner())
}

pub(crate) fn register(ring: &Arc<Ring>) {
    lock().list.push(Arc::clone(ring));
}

/// Takes up to `max` finished spans from every recorder in the process, oldest first
/// within each recorder, and passes each to `f` with the scope of the meter that made
/// its recorder. Returns how many it took.
///
/// Recorders never wait for a drain: they fill their rings, and drop spans once a
/// ring is full. Drains are serialized by a process-wide lock, which recorders take
/// only when created or dropped. Spans that a dropped recorder finished are still
/// drained.
pub fn drain(max: usize, mut f: impl FnMut(&'static str, &SpanRecord)) -> usize {
    let mut rings = lock();
    let count = rings.list.len();
    let first = if count == 0 { 0 } else { rings.next % count };
    let mut taken = 0;
    for i in 0..count {
        if taken == max {
            rings.next = (first + i) % count;
            break;
        }
        let ring = &rings.list[(first + i) % count];
        let tail = ring.tail.0.load(Ordering::Relaxed);
        let head = ring.head.0.load(Ordering::Acquire);
        let end = head.min(tail.saturating_add(max - taken));
        for n in tail..end {
            // SAFETY: see `Ring`: `tail..head` was published by the producer, which
            // won't write these slots before the store below.
            let record = unsafe { &*ring.slots[n & ring.mask].get() };
            f(ring.scope, record);
        }
        ring.tail.0.store(end, Ordering::Release);
        taken += end - tail;
    }
    rings.list.retain(|ring| {
        !(ring.closed.load(Ordering::Relaxed)
            && ring.tail.0.load(Ordering::Relaxed) == ring.head.0.load(Ordering::Acquire))
    });
    taken
}
