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

//! Record-path cost of nanometer's counter against the OTel SDK and plain atomics, in
//! ns/op and heap allocations/op, on one thread and on several: `cargo bench`.
//!
//! Every thread records into the same series, with three attributes, through its own
//! handle, after a warm-up, and all threads start timing together. ns/op is the mean
//! over threads. The empty loop's cost is reported, not subtracted.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::hint::black_box;
use std::process::Command;
use std::sync::Barrier;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::thread;
use std::time::Instant;
use std::{env, fs};

use nanometer::{HistogramDef, Meter};
use opentelemetry::KeyValue;
use opentelemetry::metrics::MeterProvider as _;
use opentelemetry_sdk::metrics::{ManualReader, SdkMeterProvider};
use quanta::Clock;

/// Counts heap allocations per thread, so each thread sees only its own.
struct CountingAlloc;

thread_local! {
    static ALLOCS: Cell<u64> = const { Cell::new(0) };
}

fn count_alloc() {
    // Never fails for a const-initialized local without a destructor.
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

// Calls per thread, by the expected cost of one call.
const FAST: u64 = 100_000_000;
const MEDIUM: u64 = 10_000_000;
const SLOW: u64 = 1_000_000;

/// Runs `iters` calls of the operation `setup` returns on each of `threads` threads.
/// Returns the mean ns per call and allocations per call.
fn run<S, F>(threads: usize, iters: u64, setup: S) -> (f64, f64)
where
    S: Fn() -> F + Sync,
    F: FnMut(),
{
    let start = Barrier::new(threads);
    let results: Vec<(f64, f64)> = thread::scope(|s| {
        let workers: Vec<_> = (0..threads)
            .map(|_| {
                let (setup, start) = (&setup, &start);
                s.spawn(move || {
                    let mut op = setup();
                    for _ in 0..iters / 10 {
                        op();
                    }
                    start.wait();
                    let allocs = ALLOCS.with(Cell::get);
                    let time = Instant::now();
                    for _ in 0..iters {
                        op();
                    }
                    let ns = time.elapsed().as_nanos() as f64 / iters as f64;
                    let allocs = (ALLOCS.with(Cell::get) - allocs) as f64 / iters as f64;
                    (ns, allocs)
                })
            })
            .collect();
        workers.into_iter().map(|w| w.join().unwrap()).collect()
    });
    let mean = |f: fn(&(f64, f64)) -> f64| results.iter().map(f).sum::<f64>() / threads as f64;
    (mean(|r| r.0), mean(|r| r.1))
}

fn case<S, F>(name: &str, threads: &[usize], iters: u64, setup: S)
where
    S: Fn() -> F + Sync,
    F: FnMut(),
{
    for &t in threads {
        let (ns, allocs) = run(t, iters, &setup);
        println!("{name:<40} {t:>7} {ns:>9.2} {allocs:>9.2}");
    }
}

fn cpu_model() -> String {
    let model = if cfg!(target_os = "macos") {
        Command::new("sysctl")
            .args(["-n", "machdep.cpu.brand_string"])
            .output()
            .ok()
            .and_then(|out| String::from_utf8(out.stdout).ok())
    } else {
        fs::read_to_string("/proc/cpuinfo").ok().and_then(|info| {
            let line = info.lines().find(|l| l.starts_with("model name"))?;
            Some(line.split_once(':')?.1.to_owned())
        })
    };
    model.map_or("unknown CPU".into(), |m| m.trim().to_owned())
}

fn main() {
    let cores = thread::available_parallelism().map_or(1, |n| n.get());
    let mut threads = vec![1, 2, cores];
    threads.dedup();

    println!(
        "{}, {cores} threads available, {}/{}",
        cpu_model(),
        env::consts::OS,
        env::consts::ARCH
    );
    println!();
    println!(
        "{:<40} {:>7} {:>9} {:>9}",
        "case", "threads", "ns/op", "allocs/op"
    );

    let attrs = [
        KeyValue::new("method", "GET"),
        KeyValue::new("path", "/api/v1/topics"),
        KeyValue::new("status", "200"),
    ];
    let provider = SdkMeterProvider::builder()
        .with_reader(ManualReader::builder().build())
        .build();
    let meter = Meter::new(&provider, "bench");
    let sdk = provider.meter("bench-sdk").u64_counter("requests").build();
    let shared = AtomicU64::new(0);

    case("empty loop", &threads, FAST, || || black_box(()));
    case("nanometer LocalCounter::add", &threads, FAST, || {
        let counter = meter.counter("requests", &attrs);
        move || counter.add(black_box(1))
    });
    case("shared AtomicU64::fetch_add", &threads, MEDIUM, || {
        let shared = &shared;
        move || {
            black_box(shared).fetch_add(1, Relaxed);
        }
    });
    case("OTel SDK bound counter add", &threads, MEDIUM, || {
        let bound = sdk.bind(&attrs);
        move || bound.add(black_box(1))
    });
    case("OTel SDK Counter::add", &threads, SLOW, || {
        let (sdk, attrs) = (&sdk, &attrs);
        move || sdk.add(1, black_box(attrs))
    });

    // Histograms: the latency preset's 19 bounds (10 µs to 10 s, in µs), with values
    // spread over the first 12 buckets.
    const BOUNDS: &[u64] = &[
        10, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000, 20_000, 50_000, 100_000, 200_000,
        500_000, 1_000_000, 2_000_000, 5_000_000, 10_000_000,
    ];
    let spread = || {
        let mut v = 0u64;
        move || {
            v = (v + 7_919) % 30_000;
            black_box(v)
        }
    };
    case("nanometer LocalHistogram::record", &threads, FAST, || {
        let histogram = meter.histogram(HistogramDef::new("latency").buckets(BOUNDS), &attrs);
        let mut value = spread();
        move || histogram.record(value())
    });
    // One clock read each: the latency timer reads quanta's clock twice, the SDK
    // timer below reads `Instant` twice.
    let clock = Clock::new();
    case("quanta Clock::raw", &threads, MEDIUM, || {
        let clock = clock.clone();
        move || {
            black_box(clock.raw());
        }
    });
    case("std Instant::now", &threads, MEDIUM, || {
        || {
            black_box(Instant::now());
        }
    });
    case("nanometer LocalLatency timer", &threads, MEDIUM, || {
        let latency = meter.latency("timed", &attrs);
        move || drop(black_box(latency.start()))
    });
    let sdk_histogram = provider
        .meter("bench-sdk")
        .u64_histogram("latency")
        .with_boundaries(BOUNDS.iter().map(|&b| b as f64).collect())
        .build();
    case("OTel SDK bound histogram record", &threads, SLOW, || {
        let bound = sdk_histogram.bind(&attrs);
        let mut value = spread();
        move || bound.record(value())
    });
    // The SDK way to time an operation: seconds as f64, the same bounds.
    let sdk_latency = provider
        .meter("bench-sdk")
        .f64_histogram("timed")
        .with_unit("s")
        .with_boundaries(BOUNDS.iter().map(|&b| b as f64 / 1e6).collect())
        .build();
    case("OTel SDK bound latency timer", &threads, SLOW, || {
        let bound = sdk_latency.bind(&attrs);
        move || {
            let start = black_box(Instant::now());
            bound.record(start.elapsed().as_secs_f64());
        }
    });
}
