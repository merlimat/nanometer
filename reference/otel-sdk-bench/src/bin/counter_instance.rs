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

//! Cost of a per-thread counter as the library would implement it:
//! A) handle caches a pointer to this thread's cell (compio-style local handle)
//! B) generic handle: thread_local slot id -> flat slab [slot][counter] -> load+store
//! C) same as B but nested Vec<Box<[AtomicU64]>> behind an Arc (bounds-checked)

use std::cell::Cell;
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Barrier};
use std::time::Instant;

const N: u64 = 200_000_000;
const SLOTS: usize = 4;
const CAP: usize = 64; // counters per slot; 64*8 B = 512 B, a multiple of 128 B

// ---- registry: one contiguous arena per slot, arenas 128B-aligned apart ----
#[repr(C, align(128))]
struct Arena([AtomicU64; CAP]);

struct Registry {
    arenas: Box<[Arena]>,
    next_slot: AtomicUsize,
}
impl Registry {
    fn new() -> Arc<Self> {
        Arc::new(Registry {
            arenas: (0..SLOTS).map(|_| Arena(std::array::from_fn(|_| AtomicU64::new(0)))).collect(),
            next_slot: AtomicUsize::new(0),
        })
    }
    fn total(&self, idx: usize) -> u64 {
        self.arenas.iter().map(|a| a.0[idx].load(Relaxed)).sum()
    }
}

thread_local! {
    static SLOT: Cell<usize> = const { Cell::new(usize::MAX) };
}

#[cold]
fn claim(reg: &Registry) -> usize {
    let s = reg.next_slot.fetch_add(1, Relaxed);
    assert!(s < SLOTS, "slot pool exhausted (real impl: fall back to shared fetch_add)");
    SLOT.set(s);
    s
}

#[inline(always)]
fn bump(c: &AtomicU64, v: u64) {
    c.store(c.load(Relaxed).wrapping_add(v), Relaxed);
}

/// B) flat: base pointer + slot stride, one TLS read
struct FlatCounter {
    reg: Arc<Registry>,
    base: *const AtomicU64,
    idx: usize,
}
unsafe impl Send for FlatCounter {}
unsafe impl Sync for FlatCounter {}
impl FlatCounter {
    fn new(reg: &Arc<Registry>, idx: usize) -> Self {
        FlatCounter { reg: reg.clone(), base: reg.arenas.as_ptr() as *const AtomicU64, idx }
    }
    #[inline(always)]
    fn add(&self, v: u64) {
        let mut s = SLOT.get();
        if s == usize::MAX {
            s = claim(&self.reg);
        }
        // SAFETY: s < SLOTS (checked in claim), idx < CAP (checked at creation in real code)
        let c = unsafe { &*self.base.add(s * CAP + self.idx) };
        bump(c, v);
    }
}

/// C) nested + bounds checked
struct NestedCounter {
    slots: Arc<Vec<Box<[AtomicU64]>>>,
    idx: usize,
}
thread_local! { static SLOT2: Cell<usize> = const { Cell::new(usize::MAX) }; }
static NEXT2: AtomicUsize = AtomicUsize::new(0);
impl NestedCounter {
    #[inline(always)]
    fn add(&self, v: u64) {
        let mut s = SLOT2.get();
        if s == usize::MAX {
            s = NEXT2.fetch_add(1, Relaxed);
            SLOT2.set(s);
        }
        bump(&self.slots[s][self.idx], v);
    }
}

fn time(threads: usize, f: impl Fn(usize) + Sync) -> f64 {
    let barrier = Barrier::new(threads);
    let per: Vec<f64> = std::thread::scope(|s| {
        (0..threads)
            .map(|t| {
                let (f, b) = (&f, &barrier);
                s.spawn(move || {
                    f(t); // warm: claims slot
                    b.wait();
                    let st = Instant::now();
                    for _ in 0..N {
                        f(black_box(t));
                    }
                    st.elapsed().as_nanos() as f64 / N as f64
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect()
    });
    per.iter().sum::<f64>() / threads as f64
}

fn main() {
    let empty = time(1, |t| {
        black_box(t);
    });
    println!("{:<52} {:>5.2} ns/op", "empty loop (overhead, not subtracted)", empty);

    for threads in [1, 2] {
        println!("--- {threads} thread(s), one counter, each thread writes its own cell ---");

        let reg = Registry::new();
        let r = &reg;
        let ns = time(threads, |t| bump(&r.arenas[t + 2].0[3], black_box(1)));
        println!("{:<52} {:>5.2} ns/op", "A) cached handle -> own cell (load+store)", ns);

        let reg = Registry::new();
        let c = FlatCounter::new(&reg, 3);
        let ns = time(threads, |_| c.add(black_box(1)));
        assert_eq!(reg.total(3), (N + 1) * threads as u64);
        println!("{:<52} {:>5.2} ns/op", "B) TLS slot + flat arena (load+store)", ns);

        NEXT2.store(0, Relaxed);
        let slots: Arc<Vec<Box<[AtomicU64]>>> =
            Arc::new((0..SLOTS).map(|_| (0..CAP).map(|_| AtomicU64::new(0)).collect()).collect());
        let c = NestedCounter { slots: slots.clone(), idx: 3 };
        let ns = time(threads, |_| c.add(black_box(1)));
        println!("{:<52} {:>5.2} ns/op", "C) TLS slot + nested Vec, bounds-checked", ns);

        let shared = AtomicU64::new(0);
        let ns = time(threads, |_| {
            shared.fetch_add(black_box(1), Relaxed);
        });
        println!("{:<52} {:>5.2} ns/op", "ref: one shared AtomicU64 fetch_add", ns);
    }
}
