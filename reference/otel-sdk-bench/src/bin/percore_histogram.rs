//! Oxia-#1457-style histogram adapted to thread-per-core: the attribute set is
//! resolved once into a series; each core owns one cache-line-aligned shard and is
//! its only writer, so recording is a bucket search plus plain relaxed load/store
//! (no RMW, no lock, no alloc). The collector sums shards with relaxed loads.

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Barrier};
use std::time::Instant;

struct Counting;
static ALLOCS: AtomicU64 = AtomicU64::new(0);
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        System.alloc(l)
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        System.dealloc(p, l)
    }
}
#[global_allocator]
static GA: Counting = Counting;

/// OTel default explicit bounds, as integers (e.g. microseconds).
const BOUNDS: [u64; 15] = [0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000];
const NB: usize = BOUNDS.len() + 1;

#[repr(align(128))] // avoid false sharing incl. adjacent-line prefetch
struct Shard {
    count: AtomicU64,
    sum: AtomicU64,
    buckets: [AtomicU64; NB],
}

impl Shard {
    fn new() -> Self {
        Shard { count: AtomicU64::new(0), sum: AtomicU64::new(0), buckets: std::array::from_fn(|_| AtomicU64::new(0)) }
    }
}

#[inline(always)]
fn bump(a: &AtomicU64, v: u64) {
    // Single writer: plain load + store, no lock-prefixed RMW.
    a.store(a.load(Relaxed).wrapping_add(v), Relaxed);
}

/// One series (name + resolved attributes), one shard per core.
pub struct HistogramSeries {
    shards: Box<[Shard]>,
}

/// Per-core recording handle. !Send so it can't leave the core that owns the shard.
pub struct CoreHistogram {
    series: Arc<HistogramSeries>,
    core: usize,
    _not_send: PhantomData<*const ()>,
}

impl HistogramSeries {
    pub fn new(cores: usize) -> Arc<Self> {
        Arc::new(HistogramSeries { shards: (0..cores).map(|_| Shard::new()).collect() })
    }
    pub fn handle(self: &Arc<Self>, core: usize) -> CoreHistogram {
        CoreHistogram { series: self.clone(), core, _not_send: PhantomData }
    }
    /// Collector side: (count, sum, buckets). Relaxed snapshot; fields may be
    /// skewed by in-flight records, same as any striped/adder histogram.
    pub fn snapshot(&self) -> (u64, u64, [u64; NB]) {
        let mut b = [0u64; NB];
        let (mut c, mut s) = (0, 0);
        for sh in self.shards.iter() {
            c += sh.count.load(Relaxed);
            s += sh.sum.load(Relaxed);
            for (o, x) in b.iter_mut().zip(&sh.buckets) {
                *o += x.load(Relaxed);
            }
        }
        (c, s, b)
    }
}

impl CoreHistogram {
    #[inline(always)]
    pub fn record(&self, v: u64) {
        // first bucket whose bound >= v (same rule as OTel / sort.SearchFloat64s)
        let i = BOUNDS.partition_point(|&b| b < v);
        // SAFETY of indexing: core < shards.len() checked at handle creation in real code
        let sh = &self.series.shards[self.core];
        bump(&sh.buckets[i], 1);
        bump(&sh.count, 1);
        bump(&sh.sum, v);
    }
}

const N: u64 = 20_000_000;

fn main() {
    let cores = 2;
    let series = HistogramSeries::new(cores);

    // single thread
    let h = series.handle(0);
    let mut v = 0u64;
    for _ in 0..N / 10 {
        v = (v + 7) % 12000;
        h.record(black_box(v));
    }
    let a0 = ALLOCS.load(Relaxed);
    let t = Instant::now();
    for _ in 0..N {
        v = (v + 7) % 12000;
        h.record(black_box(v));
    }
    let ns = t.elapsed().as_nanos() as f64 / N as f64;
    println!(
        "per-core histogram record, 1 thread:               {ns:>6.1} ns/op  {:.2} allocs/op",
        (ALLOCS.load(Relaxed) - a0) as f64 / N as f64
    );
    drop(h);

    // 2 threads, same series, each on its own shard
    let barrier = Barrier::new(cores);
    let per: Vec<f64> = std::thread::scope(|s| {
        (0..cores)
            .map(|core| {
                let (series, barrier) = (&series, &barrier);
                s.spawn(move || {
                    let h = series.handle(core);
                    let mut v = core as u64;
                    barrier.wait();
                    let t = Instant::now();
                    for _ in 0..N {
                        v = (v + 7) % 12000;
                        h.record(black_box(v));
                    }
                    t.elapsed().as_nanos() as f64 / N as f64
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect()
    });
    println!(
        "per-core histogram record, 2 threads, SAME series: {:>6.1} ns/op  (per thread)",
        per.iter().sum::<f64>() / cores as f64
    );
    let (c, _, b) = series.snapshot();
    assert_eq!(c, b.iter().sum::<u64>());
    println!("snapshot count = {c}");
}
