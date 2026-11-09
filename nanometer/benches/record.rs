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
//! ns/op and heap allocations/op, on one thread and on several: `cargo bench`. Also
//! the cost of recording a sampled span.
//!
//! Every thread records into the same series, with three attributes, through its own
//! handle, after a warm-up, and all threads start timing together. ns/op is the mean
//! over threads. The empty loop's cost is reported, not subtracted.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::hint::black_box;
use std::process::Command;
use std::sync::Barrier;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::thread;
use std::time::{Duration, Instant};
use std::{env, fs};

use nanometer::trace::{self, SpanCapacity, SpanStart};
use nanometer::{HistogramDef, Meter};
use opentelemetry::metrics::MeterProvider as _;
use opentelemetry::trace::{Span as _, SpanKind, Tracer as _, TracerProvider as _};
use opentelemetry::{KeyValue, SpanId, TraceFlags, TraceId};
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::metrics::{ManualReader, SdkMeterProvider};
use opentelemetry_sdk::trace::{SdkTracerProvider, SpanData, SpanExporter};
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

/// Takes the SDK's spans and does nothing with them.
#[derive(Debug)]
struct NoopExporter;

impl SpanExporter for NoopExporter {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        black_box(batch);
        Ok(())
    }
}

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

/// As `case`, for spans: `drained` counts the spans a draining thread took meanwhile,
/// and the share of spans dropped on a full ring is printed after the allocations.
fn span_case<S, F>(name: &str, threads: &[usize], iters: u64, drained: &AtomicU64, setup: S)
where
    S: Fn() -> F + Sync,
    F: FnMut(),
{
    for &t in threads {
        let before = drained.load(Relaxed);
        let (ns, allocs) = run(t, iters, &setup);
        // What the drainer hasn't taken yet: the recorders are gone, their spans stay.
        let rest = trace::drain(usize::MAX, |_, _| {});
        let taken = drained.load(Relaxed) - before + rest as u64;
        let total = t as u64 * (iters + iters / 10);
        let dropped = 100.0 * (total - taken) as f64 / total as f64;
        println!("{name:<40} {t:>7} {ns:>9.2} {allocs:>9.2} {dropped:>8.1}% dropped");
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

    // Histograms: the latency preset's 45 bounds (Renard's R5 from 100 ns to 63 s, in
    // ns), with values spread over the first 29 buckets.
    const BOUNDS: &[u64] = &{
        const R5: [u64; 5] = [10, 16, 25, 40, 63];
        let mut bounds = [0; 45];
        let mut i = 0;
        while i < bounds.len() {
            bounds[i] = R5[i % 5] * 10u64.pow((i / 5) as u32 + 1);
            i += 1;
        }
        bounds
    };
    let spread = || {
        let mut v = 0u64;
        move || {
            v = (v + 7_919_000) % 30_000_000;
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
        .with_boundaries(BOUNDS.iter().map(|&b| b as f64 / 1e9).collect())
        .build();
    case("OTel SDK bound latency timer", &threads, SLOW, || {
        let bound = sdk_latency.bind(&attrs);
        move || {
            let start = black_box(Instant::now());
            bound.record(start.elapsed().as_secs_f64());
        }
    });

    // Spans: each thread records into its own recorder, as a core would, and one more
    // thread drains every recorder's ring every 50 µs, as an exporter would, if less
    // often. A thread that outpaces the drainer fills its ring and drops spans, which
    // skips the copy into the ring: the dropped share shows how much of a row that is.
    let stop = AtomicBool::new(false);
    let drained = AtomicU64::new(0);
    thread::scope(|s| {
        s.spawn(|| {
            while !stop.load(Relaxed) {
                let n = trace::drain(usize::MAX, |_, record| {
                    black_box(record.span_id());
                });
                drained.fetch_add(n as u64, Relaxed);
                // Sleeping is too coarse for this; spinning keeps the period.
                let now = Instant::now();
                while now.elapsed() < Duration::from_micros(50) {
                    std::hint::spin_loop();
                }
            }
        });
        let capacity = SpanCapacity {
            active: 16,
            finished: 1 << 13,
        };
        span_case(
            "nanometer span, 2 events",
            &threads,
            MEDIUM,
            &drained,
            || {
                let spans = meter.span_recorder(capacity);
                let mut n = 0u64;
                move || {
                    n += 1;
                    let start = SpanStart::new("append", TraceId::from(n as u128), SpanId::from(n))
                        .parent(SpanId::from(1));
                    let span = spans.start(black_box(start)).unwrap();
                    spans.event(span, "queued");
                    spans.event(span, "written");
                    spans.end(span);
                }
            },
        );
        span_case(
            "nanometer span, + 2 attributes, 1 link",
            &threads,
            MEDIUM,
            &drained,
            || {
                let spans = meter.span_recorder(capacity);
                let mut n = 0u64;
                move || {
                    n += 1;
                    let start = SpanStart::new("append", TraceId::from(n as u128), SpanId::from(n))
                        .parent(SpanId::from(1));
                    let span = spans.start(black_box(start)).unwrap();
                    spans.attribute(span, "records", n);
                    spans.attribute(span, "topic", "events");
                    spans.link(span, TraceId::from(7), SpanId::from(n), TraceFlags::SAMPLED);
                    spans.event(span, "queued");
                    spans.event(span, "written");
                    spans.end(span);
                }
            },
        );
        stop.store(true, Relaxed);
    });

    // The SDK's way: a sampled span through its batch processor, which hands finished
    // spans to its own thread over a channel, and drops them when its queue is full.
    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(NoopExporter)
        .build();
    let tracer = tracer_provider.tracer("bench-sdk");
    case("OTel SDK span, 2 events", &threads, SLOW, || {
        let tracer = &tracer;
        move || {
            let mut span = tracer
                .span_builder("append")
                .with_kind(SpanKind::Internal)
                .start(tracer);
            span.add_event("queued", Vec::new());
            span.add_event("written", Vec::new());
            span.end();
        }
    });
}
