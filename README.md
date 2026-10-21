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
  where the OTel SDK's `Counter::add` takes 46 ns on one thread and 2.5–2.6 µs on 18.
  A histogram `record` takes 2.4–3.4 ns, against 11 ns and 590–670 ns for the SDK's
  bound histogram, and timing a latency 2.7–3.4 ns. Those are on an Apple M5 Pro; on
  server CPUs the timer takes 27–50 ns, as their counter is slower to read
  ([Performance](#performance)).
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
built in, 1-2-5 from 10 µs to 10 s, exports in seconds, and comes with a timer that
reads the CPU's counter through [quanta](https://docs.rs/quanta). The first latency
handle in a process calibrates that clock, which blocks for 10 to 200 ms, once:

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
nanometer = { version = "0.1", features = ["sdk"] }
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

## Performance

Measured with `cargo bench` ([benches/record.rs](nanometer/benches/record.rs)) on an
Apple M5 Pro (18 cores of two kinds, 6 + 12) under macOS, with Rust 1.99, fat LTO and
one codegen unit. Every thread records into the same series, with three attributes,
through its own handle. Each figure is the mean time per call over the threads, as a
range over three runs. No case allocates.

| ns per call | 1 thread | 2 threads | 18 threads |
|---|---:|---:|---:|
| nanometer `LocalCounter::add` | 1.1–1.2 | 1.1–1.2 | 1.1 |
| `AtomicU64::fetch_add` on one shared counter | 1.5 | 11 | 131–134 |
| OTel SDK bound counter `add` | 1.8 | 17 | 156–159 |
| OTel SDK `Counter::add` | 46 | 157–159 | 2,500–2,640 |
| nanometer `LocalHistogram::record` | 2.4–2.5 | 2.5 | 3.4 |
| OTel SDK bound histogram `record` | 10.8–10.9 | 48–50 | 589–668 |
| quanta `Clock::raw`, one counter read | 0.3–0.5 | 0.3–0.5 | 0.4 |
| `Instant::now()`, one read | 13 | 13–14 | 14–16 |
| nanometer latency timer, start and stop | 2.7–2.9 | 2.7–2.9 | 3.3–3.4 |
| OTel SDK bound histogram in seconds, timed with `Instant::now()` | 47–49 | 188–204 | 2,030–2,210 |
| Empty loop, not subtracted | 0.2 | 0.2 | 0.2 |

The histograms have the latency preset's 19 bounds, with values spread over the
first 12 buckets.

On one thread the SDK's bound counter comes close, since its `add` is a single
uncontended atomic add. As threads are added, their atomic adds all hit the same
cache line, while each nanometer handle writes cells of its own. The SDK's bound
histogram also takes a lock per series. Contended runs vary the most from run to run,
and the 18-thread runs mix both kinds of core. nanometer's timer reads the CPU's
counter twice, for well under a nanosecond on this CPU, and records the ticks against
bounds converted to ticks once; the SDK way pays two `Instant::now()` calls, about
25 ns here, plus its locked `f64` record, which is what grows with threads.

### Other platforms

[.github/workflows/bench.yml](.github/workflows/bench.yml) runs the same bench on
GitHub's hosted runners, in a `rust:1` container on Linux and natively on Windows and
macOS; start it from the Actions tab, or push a `bench/**` branch. The runners are
shared virtual machines with 4 vCPUs (3 on macOS), so these figures
come from one run each, in October 2026, and the contended ones are rough.
`ubuntu-latest` lands on different CPUs from run to run; two of them are shown. Each
cell is 1 thread / all threads.

| ns per call | Linux x86-64, AMD EPYC 7763 | Linux x86-64, Intel Xeon Platinum 8573C | Windows x64, AMD EPYC 7763 | Linux arm64, Neoverse-N2 | macOS arm64, Apple M1 (virtual) |
|---|---:|---:|---:|---:|---:|
| nanometer `LocalCounter::add` | 0.6 / 1.3 | 0.6 / 1.1 | 0.6 / 1.4 | 1.8 / 1.8 | 1.5 / 1.7 |
| `AtomicU64::fetch_add` on one shared counter | 2.3 / 77 | 8.5 / 67 | 2.3 / 72 | 4.2 / 24 | 2.9 / 17 |
| OTel SDK bound counter `add` | 4.4 / 90 | 7.6 / 84 | 5.0 / 83 | 7.6 / 44 | 5.8 / 29 |
| OTel SDK `Counter::add` | 100 / 392 | 84 / 401 | 116 / 274 | 96 / 408 | 119 / 365 |
| nanometer `LocalHistogram::record` | 5.4 / 9.6 | 4.4 / 8.9 | 5.6 / 9.9 | 5.6 / 5.6 | 4.5 / 5.4 |
| OTel SDK bound histogram `record` | 26 / 348 | 19 / 459 | 26 / 587 | 24 / 455 | 21 / 178 |
| quanta `Clock::raw`, one counter read | 11 / 12 | 8.4 / 12 | 11 / 12 | 13 / 13 | 1.0 / 1.2 |
| `Instant::now()`, one read | 28 / 31 | 19 / 25 | 38 / 43 | 31 / 31 | 33 / 31 |
| nanometer latency timer, start and stop | 50 / 51 | 31 / 37 | 50 / 54 | 27 / 27 | 6.9 / 7.4 |
| OTel SDK bound histogram in seconds, timed with `Instant::now()` | 99 / 472 | 79 / 665 | 117 / 568 | 104 / 806 | 113 / 905 |
| Empty loop, not subtracted | 0.3 / 0.7 | 0.3 / 0.6 | 0.3 / 0.7 | 0.3 / 0.3 | 0.4 / 0.4 |

As on the M5, nanometer's handles cost a few nanoseconds and stay flat as threads
are added, while the shared atomic and the SDK's instruments grow by tens to hundreds
of nanoseconds. On the x86 runners even the empty loop doubles at 4 threads: their 4
vCPUs are 2 cores with two hyperthreads each (Windows reports 2 cores, 4 logical
processors).

What changes is the cost of reading the CPU's counter. It is about a nanosecond on
Apple silicon, even virtualized, and 8–13 ns on the server CPUs, whatever the OS, so
the latency timer takes 27–50 ns there instead of 3. That is still 2–4 times less
than the SDK's way on one thread, and `Clock::raw` beats `Instant::now()` on every
platform. On x86 the timer also costs 10–21 ns more than its two reads and a record
add up to; on arm64 it costs about that sum or less.

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
