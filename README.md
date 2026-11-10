# nanometer

Metric handles for Rust whose record path costs nanoseconds, exported through the
OpenTelemetry pipeline you already have.

nanometer is for code where a metric update sits on the hot path of every request.
Each thread or core owns its own instance of a metric and writes it without locks or
atomic read-modify-write; the exporter adds the instances up when it collects. The
main target is thread-per-core runtimes such as [compio](https://github.com/compio-rs/compio)
on io_uring, and it also works from ordinary multi-threaded code.

## Goals

- **A few nanoseconds per record.** No locks, no read-modify-write atomics, no
  allocation and no attribute handling on the record path. A counter `add` is a load
  and a store on a cell only its thread writes: 1.1–1.2 ns on 1 to 18 threads,
  where the OTel SDK's `Counter::add` takes 45 ns on one thread and 2.4 µs on 18.
  A histogram `record` over the latency preset's 45 bounds takes 2.7–4.1 ns, against
  15 ns and 680 ns for the SDK's bound histogram, and timing a latency 2.9–4.3 ns.
  Those are on an Apple M5 Pro; on server CPUs the timer takes 27–43 ns, as their
  counter is slower to read ([Performance](#performance)).
- **Your OTel setup, unchanged.** A library takes an OTel `MeterProvider` and a scope
  name; the app keeps its exporter. Libraries depend only on the `opentelemetry` API
  crate.
- **Thread-per-core first.** Local handles are `!Send`: create one per thread or core.
- **Attributes resolved once.** They are sorted and deduplicated when a handle is
  created, so the hot path never sees a `KeyValue`.
- **Correct totals.** Each metric has one OTel observable whose callback sums every
  instance; the SDK keeps only the last observation per attribute set, so one
  observable per instance would lose counts. When a handle is dropped its value
  folds into its series, so cumulative values never go backwards.

## Status

Early: the API is a draft and will change.

- [x] Process-wide registry, `LocalCounter`, export through an observable counter.
- [x] Benchmark harness reporting ns/op and allocs/op.
- [x] `LocalHistogram`, presets for latencies (with a timer), sizes in bytes and
  counts, and export as real OTLP histograms through `Producer`. OTel has no
  asynchronous histogram, and the Rust SDK lost its way to take aggregated data when
  `MetricProducer` was removed in 0.26
  ([details](docs/design-notes.md#4-export-plumbing-findings)). Until it is back
  upstream ([#2505](https://github.com/open-telemetry/opentelemetry-rust/issues/2505)),
  histogram export needs SDK 0.33.1 with `MetricProducer` re-added, from
  [merlimat/opentelemetry-rust](https://github.com/merlimat/opentelemetry-rust/tree/metric-producer-0.33.1);
  see [Exporting histograms](#exporting-histograms).
- [ ] Delta temporality for histograms: they export as cumulative.
- [x] `LocalUpDownCounter`, exported through observable up-down counters.
- [x] Gauges: `Gauge`, set from any thread, and `ObservableGauge`, read from a callback
  at each export.
- [ ] `Send + Sync` shared counter handles and a cardinality cap.
- [x] Span recording: sampled OpenTelemetry spans recorded per core without locks or
  allocation, exported through the OTel SDK's `SpanExporter`, such as OTLP's
  ([Recording spans](#recording-spans)).

## Example

A library creates its handles once, at startup, and records on the hot path:

```rust
use nanometer::{CounterDef, Meter};
use opentelemetry::KeyValue;

// A const definition keeps the unit and description with the name.
const BYTES_IN: CounterDef = CounterDef::new("bytes_in").unit("By");

// The app passes in its provider; the library names its scope.
let meter = Meter::new(&provider, "my-lib");

// Attributes are resolved here, once.
let attrs = [KeyValue::new("op", "put")];
let requests = meter.counter("requests", &attrs);
let bytes_in = meter.counter(BYTES_IN, &attrs);

// The hot path: a load and a store on this thread's own cell.
requests.add(1);
bytes_in.add(512);
```

Up-down counters also go down, for values such as open connections. The exported
value is the sum over all instances, so a decrement can come from another core than
the increment:

```rust
let connections = meter.up_down_counter("connections", &attrs);
connections.add(1);
connections.add(-1);
```

Gauges report a current value: set it, from any thread, or have nanometer read it
from a callback at each export. A series leaves the export when its last handle is
dropped, and each series takes one gauge, so give each gauge its own attributes:

```rust
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

let backlog = meter.gauge("backlog_bytes", &attrs);
backlog.set(4_096);

let queued = Arc::new(AtomicI64::new(0));
let depth = {
    let queued = Arc::clone(&queued);
    meter.observable_gauge("queue_depth", &attrs, move || queued.load(Ordering::Relaxed))
};
```

Histograms take bucket bounds, or a preset. A latency histogram has its buckets
built in: five per decade from 100 ns to 63 s, on Renard's R5 numbers (1, 1.6, 2.5,
4, 6.3), so a quantile that a backend estimates from them, the max included, is
within 1.6 times the true value in that range. It exports in
seconds and comes with a timer that reads the CPU's counter through
[quanta](https://docs.rs/quanta). The first latency handle in a process calibrates
that clock, which blocks for 10 to 200 ms, once:

```rust
use nanometer::HistogramDef;

const BATCH: HistogramDef = HistogramDef::counts("batch_size").unit("{message}");

let payload = meter.histogram(HistogramDef::bytes("payload_size"), &attrs);
let batch = meter.histogram(BATCH, &attrs);
let latency = meter.latency("put_latency", &attrs);

let timer = latency.start(); // records when dropped
payload.record(4_096);
batch.record(12);
drop(timer);

// Or record a Duration, or keep the timer's result:
let elapsed = latency.start().stop();
latency.record(elapsed);
```

Handles are `!Send`, so each thread or core creates its own. They all feed one series,
and the counts of a handle that is dropped, or of a thread that exits, still count:

```rust
let workers: Vec<_> = (0..4)
    .map(|_| {
        let meter = meter.clone();
        std::thread::spawn(move || {
            let requests = meter.counter("requests", &[KeyValue::new("op", "put")]);
            for _ in 0..1_000 {
                requests.add(1);
            }
        })
    })
    .collect();
for worker in workers {
    worker.join().unwrap();
}
// Together the workers added 4000 to requests{op="put"}.
```

The app builds its provider as usual, with any exporter:

```rust
use opentelemetry_sdk::metrics::SdkMeterProvider;

let provider = SdkMeterProvider::builder()
    .with_periodic_exporter(exporter) // e.g. OTLP to a Collector
    .build();
```

To use the global provider instead, pass `&*opentelemetry::global::meter_provider()`.

### Exporting histograms

Counters, up-down counters and gauges export through observable instruments, with
any provider. Histograms export through `nanometer::Producer`, an OTel
`MetricProducer` that the app registers with its reader, once. That takes the `sdk`
feature, in the app only; libraries keep the default features.

Until an OTel SDK release has `MetricProducer`
([open-telemetry/opentelemetry-rust#3802](https://github.com/open-telemetry/opentelemetry-rust/pull/3802)),
the app also patches OTel to 0.33.1 with it re-added, and turns it on in its own SDK
dependency:

```toml
[dependencies]
nanometer = { version = "0.2", features = ["sdk"] }
opentelemetry_sdk = { version = "=0.33.1", features = ["experimental_metrics_producer"] }

# Patch both crates: patching only the SDK pulls in a second copy of the API crate.
[patch.crates-io]
opentelemetry = { git = "https://github.com/merlimat/opentelemetry-rust", branch = "metric-producer-0.33.1" }
opentelemetry_sdk = { git = "https://github.com/merlimat/opentelemetry-rust", branch = "metric-producer-0.33.1" }
```

```rust
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};

let reader = PeriodicReader::builder(exporter) // e.g. OTLP to a Collector
    .with_producer(nanometer::Producer::new())
    .build();
let provider = SdkMeterProvider::builder().with_reader(reader).build();
```

The producer exports every histogram in the process as a cumulative OTLP histogram.
With a pull exporter that has no producer hook, such as Prometheus's, push OTLP to a
Collector or to Prometheus's OTLP receiver instead. docs.rs builds nanometer without
the `sdk` feature, so `Producer` is documented here and in its source.

## Recording spans

`nanometer::trace` records sampled OpenTelemetry spans the same way: each core owns
a `SpanRecorder`, and an exporter thread takes the finished spans from every core.
nanometer records the spans it is given; sampling, trace and span IDs and context
propagation stay with the application, which starts a span only for a request it
sampled.

A recorder is a plain value, kept as a field of the core's state, not a thread-local.
Spans in progress live in a slab it allocates up front, addressed by an `ActiveSpan`:
4 bytes, `Copy`, to carry in the application's own structs. A span is a fixed-size
`SpanRecord` of 784 B, with no heap data: IDs, kind, a `&'static str` name, start and
end in ticks of the CPU's counter (the latency timer's clock), status, up to 8
attributes (`u64`, `i64`, `bool` or `&'static str` values), 4 events and 8 links.
More are dropped and counted in the span, as OTel does. Ending a span copies it into
a ring that only its recorder writes. Nothing on the way allocates or takes a lock.

```rust
use nanometer::trace::{SpanCapacity, SpanKind, SpanStart, Status};
use opentelemetry::{SpanId, TraceFlags};

// Once per core: 1,024 spans in progress, 4,096 finished, 3.8 MiB.
let spans = meter.span_recorder(SpanCapacity::default());

// A request the application sampled, with its trace and span IDs:
let start = SpanStart::new("append", trace_id, span_id)
    .remote_parent(client_span_id)
    .kind(SpanKind::Server);
if let Some(span) = spans.start(start) {
    spans.attribute(span, "records", 12u64);
    spans.link(span, other_trace_id, other_span_id, TraceFlags::SAMPLED);
    spans.event(span, "written");
    spans.status(span, Status::Ok);
    spans.end(span);
}
```

When the slab is full, `start` returns `None`; when the ring is full, `end` drops the
span. Both count in the counter `nanometer.spans.dropped` of the recorder's meter,
by `reason` (`active_full`, `finished_full`). A handle whose span has ended is
ignored, even once its slot holds another span.

The app exports the spans of every recorder in the process with
`nanometer::trace::Collector`, through any OTel SDK `SpanExporter`. That takes the
`sdk-trace` feature, in the app only, and works with the SDK from crates.io: it needs
no patch.

```toml
[dependencies]
nanometer = { version = "0.2", features = ["sdk-trace"] }
```

```rust
let mut exporter = opentelemetry_otlp::SpanExporter::builder().with_http().build()?;
exporter.set_resource(&resource);
let collector = nanometer::trace::Collector::new(); // batches of up to 512 spans
std::thread::spawn(move || loop {
    std::thread::sleep(Duration::from_millis(100));
    if let Err(err) = futures_executor::block_on(collector.export(&exporter)) {
        eprintln!("span export failed: {err}");
    }
});
```

Drain often enough that no ring fills between two drains: at 20,000 spans per second
on a core and a drain every 100 ms, the default ring of 4,096 holds twice what it
needs. The collector converts the counter's ticks to wall time from an anchor, a
wall-clock reading paired with a counter reading, taken afresh for each batch.
`trace::drain` hands out the raw `SpanRecord`s instead, for an exporter of your own.

A sampled span with a parent, two events and its end takes 17 ns on one thread of
the M5 Pro, with another thread draining every ring every 50 µs, and allocates
nothing; the SDK's `Tracer` with its batch processor takes 127 ns and an allocation
for the same span (`benches/record.rs`; [details](docs/design-notes.md#8-span-recording)).

## Performance

Measured with `cargo bench` ([benches/record.rs](nanometer/benches/record.rs)), with
Rust 1.99, fat LTO and one codegen unit. Every thread records into the same series,
with three attributes, through its own handle. Each figure is the mean time per call
over the threads. No case allocates, and the empty loop, 0.2–0.3 ns on one thread, is
not subtracted.

- **Apple M5 Pro**, 18 cores of two kinds (6 + 12), under macOS: the median of three
  runs.
- **GitHub's hosted runners**, through
  [.github/workflows/bench.yml](.github/workflows/bench.yml): in a `rust:1` container
  on Linux and natively on Windows and macOS; start it from the Actions tab, or push a
  `bench/**` branch. They are shared virtual machines with 4 vCPUs, so their figures
  come from one run each, in November 2026, and the contended ones are rough. The x86
  runners land on different CPUs from run to run, an AMD EPYC 9V74 or 9V45 in three
  runs on each OS; both are shown for Linux.

All threads means one per CPU: 18 on the M5 Pro, 4 on the runners. The OTel rows use the SDK's bound instruments, its fastest path, and for
counters also the plain `Counter::add`, which takes the attributes on every call; for
latencies, a bound histogram in seconds timed with `Instant::now()`. The histograms
have the latency preset's 45 bounds, with the values spread over the first 29 buckets
([why these bounds](docs/design-notes.md#7-latency-bucket-bounds)).

| Counters, ns per `add` | macOS arm64, Apple M5 Pro | Linux x86-64, AMD EPYC 9V74 | Linux x86-64, AMD EPYC 9V45 | Windows x64, AMD EPYC 9V45 | Linux arm64, Neoverse-N2 |
|---|---:|---:|---:|---:|---:|
| nanometer, 1 thread | 1.2 | 0.6 | 0.3 | 0.3 | 1.8 |
| OTel bound, 1 thread | 1.8 | 2.9 | 6.1 | 6.7 | 7.7 |
| OTel `Counter::add`, 1 thread | 45 | 80 | 68 | 73 | 95 |
| nanometer, all threads | 1.1 | 1.1 | 0.6 | 0.5 | 1.8 |
| OTel bound, all threads | 148 | 63 | 75 | 53 | 47 |
| OTel `Counter::add`, all threads | 2,446 | 289 | 259 | 287 | 477 |

| Histograms, ns per `record` | macOS arm64, Apple M5 Pro | Linux x86-64, AMD EPYC 9V74 | Linux x86-64, AMD EPYC 9V45 | Windows x64, AMD EPYC 9V45 | Linux arm64, Neoverse-N2 |
|---|---:|---:|---:|---:|---:|
| nanometer, 1 thread | 2.7 | 5.2 | 3.7 | 3.8 | 6.6 |
| OTel, 1 thread | 15 | 20 | 15 | 16 | 26 |
| nanometer, all threads | 4.1 | 9.4 | 6.8 | 7.3 | 6.6 |
| OTel, all threads | 676 | 309 | 292 | 568 | 608 |

| Latency histograms, ns per timed call | macOS arm64, Apple M5 Pro | Linux x86-64, AMD EPYC 9V74 | Linux x86-64, AMD EPYC 9V45 | Windows x64, AMD EPYC 9V45 | Linux arm64, Neoverse-N2 |
|---|---:|---:|---:|---:|---:|
| nanometer, 1 thread | 2.9 | 43 | 34 | 35 | 27 |
| OTel, 1 thread | 57 | 88 | 84 | 104 | 107 |
| nanometer, all threads | 4.3 | 46 | 34 | 37 | 27 |
| OTel, all threads | 2,278 | 412 | 391 | 621 | 1,079 |

nanometer's handles cost a few nanoseconds everywhere and stay flat as threads are
added. On one thread the SDK's bound counter comes closest, since its `add` is a
single uncontended atomic add; as threads are added, their atomic adds all hit the
same cache line, while each nanometer handle writes cells of its own. The SDK's bound
histogram also takes a lock per series. Contended runs vary the most from run to run.
The M5's 18-thread runs mix both kinds of core, and on the x86 runners nanometer's
counter and histogram double at 4 threads: their 4 vCPUs are 2 cores with two
hyperthreads each (Windows reports 2 cores, 4 logical processors).

nanometer's timer reads the CPU's counter twice and records the ticks against bounds
converted to ticks once; the SDK way pays two `Instant::now()` calls plus its locked
`f64` record, which is what grows with threads. The bench also times single clock
reads. Reading the counter takes under half a nanosecond on the M5 but 9–13 ns on
the server CPUs, whatever the OS; `Instant::now()` takes 13 ns on
the M5 and 21–31 ns on the runners. So the latency timer takes 27–43 ns on the server
CPUs instead of 3, still 2–4 times less than the SDK's way on one thread. On x86 the
timer also costs 12–19 ns more than its two reads and a record add up to; on arm64 it
costs less than that sum.

## Building

```sh
cargo test --all-features
cargo clippy --all-targets --all-features
cargo fmt --check
cargo bench
```

Under Miri, the integration tests need clock access, because the SDK reads the time:

```sh
MIRIFLAGS=-Zmiri-disable-isolation cargo +nightly miri test --all-features
```

[docs/design-notes.md](docs/design-notes.md) has the measurements and reasoning behind
the design. `reference/` holds the prototypes from the design phase, as standalone
crates.

## License

Copyright 2026 Matteo Merli. Licensed under the [Apache License, Version 2.0](LICENSE).
