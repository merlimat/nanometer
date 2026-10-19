//! Hot-path cost of OpenTelemetry Rust SDK 0.33.1 counters/histograms:
//! ns/op and heap allocations/op, single-threaded and with 2 threads.

use opentelemetry::metrics::MeterProvider as _;
use opentelemetry::{Key, KeyValue};
use opentelemetry_sdk::metrics::{Instrument, ManualReader, SdkMeterProvider, Stream, Temporality};
use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Instant;

// ---------- allocation counter ----------
struct Counting;
static ALLOCS: AtomicU64 = AtomicU64::new(0);
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        System.alloc(l)
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        System.dealloc(p, l)
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        System.realloc(p, l, n)
    }
}
#[global_allocator]
static GA: Counting = Counting;

const N: u64 = 10_000_000;

fn st(name: &str, mut f: impl FnMut()) {
    for _ in 0..N / 10 {
        f();
    }
    let a0 = ALLOCS.load(Ordering::Relaxed);
    let t = Instant::now();
    for _ in 0..N {
        f();
    }
    let ns = t.elapsed().as_nanos() as f64 / N as f64;
    let allocs = (ALLOCS.load(Ordering::Relaxed) - a0) as f64 / N as f64;
    println!("{name:<55} {ns:>8.1} ns/op  {allocs:>5.2} allocs/op");
}

fn mt<F: Fn(usize) + Sync>(name: &str, f: F) {
    let threads = 2;
    let barrier = Barrier::new(threads);
    let a0 = ALLOCS.load(Ordering::Relaxed);
    let per_thread: Vec<f64> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..threads)
            .map(|tid| {
                let (f, barrier) = (&f, &barrier);
                s.spawn(move || {
                    for _ in 0..N / 10 {
                        f(tid);
                    }
                    barrier.wait();
                    let t = Instant::now();
                    for _ in 0..N {
                        f(tid);
                    }
                    t.elapsed().as_nanos() as f64 / N as f64
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let allocs = (ALLOCS.load(Ordering::Relaxed) - a0) as f64 / (N as f64 * 1.1 * threads as f64);
    let avg = per_thread.iter().sum::<f64>() / threads as f64;
    println!("{name:<55} {avg:>8.1} ns/op  {allocs:>5.2} allocs/op  (per thread, 2 threads)");
}

fn provider(t: Temporality) -> SdkMeterProvider {
    SdkMeterProvider::builder()
        .with_reader(ManualReader::builder().with_temporality(t).build())
        .build()
}


#[allow(non_snake_case)]
fn main() {
    let ATTRS: [KeyValue; 3] = [
        KeyValue::new("method", "GET"),
        KeyValue::new("path", "/api/v1/topics"),
        KeyValue::new("status", "200"),
    ];
    println!("--- baselines (single thread) ---");
    let mut local = 0u64;
    st("plain u64 += 1 (per-core cell, owned)", || {
        local = black_box(local) + 1;
    });
    let cell = AtomicU64::new(0);
    st("AtomicU64 relaxed load+store (single writer)", || {
        let c = black_box(&cell);
        c.store(c.load(Ordering::Relaxed) + 1, Ordering::Relaxed);
    });
    st("AtomicU64 fetch_add (uncontended)", || {
        black_box(&cell).fetch_add(1, Ordering::Relaxed);
    });

    println!("--- Counter<u64>, cumulative ---");
    let p = provider(Temporality::Cumulative);
    let m = p.meter("bench");
    let c = m.u64_counter("c").build();
    st("add(1, &[])", || c.add(1, black_box(&[])));
    st("add(1, &STATIC_ATTRS[3])  (prebuilt slice)", || c.add(1, black_box(&ATTRS)));
    st("add(1, &[KeyValue::new(&'static str)..3])  inline", || {
        c.add(
            1,
            &[
                KeyValue::new("method", black_box("GET")),
                KeyValue::new("path", black_box("/api/v1/topics")),
                KeyValue::new("status", black_box("200")),
            ],
        )
    });
    let path_arc: Arc<str> = Arc::from("/api/v1/topics");
    st("inline attrs, 1 value is Arc<str>.clone()", || {
        c.add(
            1,
            &[
                KeyValue::new("method", "GET"),
                KeyValue::new("path", black_box(&path_arc).clone()),
                KeyValue::new("status", "200"),
            ],
        )
    });
    let path_string = String::from("/api/v1/topics");
    st("inline attrs, 1 value is String.clone()", || {
        c.add(
            1,
            &[
                KeyValue::new("method", "GET"),
                KeyValue::new("path", black_box(&path_string).clone()),
                KeyValue::new("status", "200"),
            ],
        )
    });
    // first-seen order is ATTRS (sorted). Now use a different (unsorted) order.
    let unsorted = [ATTRS[2].clone(), ATTRS[0].clone(), ATTRS[1].clone()];
    st("same attrs, different order than first-seen", || c.add(1, black_box(&unsorted)));
    let a4 = [ATTRS[0].clone(), ATTRS[1].clone(), ATTRS[2].clone(), KeyValue::new("tenant", "t1")];
    st("4 attrs (other series), prebuilt", || c.add(1, black_box(&a4)));

    println!("--- Counter<u64>, delta ---");
    let pd = provider(Temporality::Delta);
    let cd = pd.meter("bench").u64_counter("cd").build();
    st("add(1, &STATIC_ATTRS[3])", || cd.add(1, black_box(&ATTRS)));
    let fd = pd.meter("bench").f64_counter("fd").build();
    st("f64 counter add(1.0, &STATIC_ATTRS[3])  (CAS loop)", || fd.add(1.0, black_box(&ATTRS)));

    println!("--- Counter with a View (allowed_attribute_keys) ---");
    let pv = SdkMeterProvider::builder()
        .with_reader(ManualReader::builder().build())
        .with_view(|i: &Instrument| {
            (i.name() == "cv").then(|| {
                Stream::builder()
                    .with_allowed_attribute_keys(vec![Key::new("method"), Key::new("status")])
                    .build()
                    .unwrap()
            })
        })
        .build();
    let cv = pv.meter("bench").u64_counter("cv").build();
    st("add(1, &STATIC_ATTRS[3])  with attr-filter view", || cv.add(1, black_box(&ATTRS)));

    println!("--- Histogram<f64> (default 15 buckets), cumulative ---");
    let h = m.f64_histogram("h").build();
    let mut v = 0.0f64;
    st("record(v, &STATIC_ATTRS[3])", || {
        v = (v + 7.3) % 5000.0;
        h.record(black_box(v), black_box(&ATTRS))
    });
    let hu = m.u64_histogram("hu").build();
    let mut x = 0u64;
    st("u64 record(v, &STATIC_ATTRS[3])", || {
        x = (x + 7) % 5000;
        hu.record(black_box(x), black_box(&ATTRS))
    });

    println!("--- Bound instruments (experimental_metrics_bound_instruments) ---");
    let bc = c.bind(&ATTRS);
    st("BoundCounter<u64>.add(1)", || bc.add(black_box(1)));
    let bh = h.bind(&ATTRS);
    st("BoundHistogram<f64>.record(v)", || {
        v = (v + 7.3) % 5000.0;
        bh.record(black_box(v))
    });

    println!();
    println!("--- contention: 2 threads ---");
    let cells: Vec<PaddedCell> = (0..2).map(|_| PaddedCell(AtomicU64::new(0))).collect();
    mt("baseline: per-thread padded cell, load+store", |t| {
        let c = &black_box(&cells)[t].0;
        c.store(c.load(Ordering::Relaxed) + 1, Ordering::Relaxed);
    });
    let shared = AtomicU64::new(0);
    mt("baseline: shared AtomicU64 fetch_add", |_| {
        black_box(&shared).fetch_add(1, Ordering::Relaxed);
    });
    mt("unbound counter, SAME series", |_| c.add(1, black_box(&ATTRS)));
    let per_thread_attrs: Vec<[KeyValue; 4]> = (0..2)
        .map(|t| {
            [ATTRS[0].clone(), ATTRS[1].clone(), ATTRS[2].clone(), KeyValue::new("core", t as i64)]
        })
        .collect();
    mt("unbound counter, DISTINCT series per thread", |t| {
        c.add(1, black_box(&per_thread_attrs[t]))
    });
    mt("bound counter, SAME series (shared handle)", |_| bc.add(black_box(1)));
    let bcs: Vec<_> = per_thread_attrs.iter().map(|a| c.bind(a)).collect();
    mt("bound counter, DISTINCT series per thread", |t| bcs[t].add(black_box(1)));
    mt("unbound histogram, SAME series", |t| h.record(black_box(t as f64 * 3.0), black_box(&ATTRS)));
    mt("unbound histogram, DISTINCT series per thread", |t| {
        h.record(black_box(t as f64 * 3.0), black_box(&per_thread_attrs[t]))
    });
    mt("bound histogram, SAME series", |t| bh.record(black_box(t as f64 * 3.0)));
    let bhs: Vec<_> = per_thread_attrs.iter().map(|a| h.bind(a)).collect();
    mt("bound histogram, DISTINCT series per thread", |t| bhs[t].record(black_box(t as f64 * 3.0)));

    drop((bc, bh, bcs, bhs));
    let _ = (p.shutdown(), pd.shutdown(), pv.shutdown());
}

#[repr(align(128))]
struct PaddedCell(AtomicU64);
