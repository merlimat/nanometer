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

Measured with `cargo bench` ([benches/record.rs](nanometer/benches/record.rs)), with
Rust 1.99, fat LTO and one codegen unit. Every thread records into the same series,
with three attributes, through its own handle. Each figure is the mean time per call
over the threads. No case allocates, and the empty loop, 0.2–0.4 ns on one thread, is
not subtracted.

- **Apple M5 Pro**, 18 cores of two kinds (6 + 12), under macOS: a range over three
  runs.
- **GitHub's hosted runners**, through
  [.github/workflows/bench.yml](.github/workflows/bench.yml): in a `rust:1` container
  on Linux and natively on Windows and macOS; start it from the Actions tab, or push a
  `bench/**` branch. They are shared virtual machines with 4 vCPUs (3 on macOS), so
  their figures come from one run each, in October 2026, and the contended ones are
  rough. `ubuntu-latest` lands on different CPUs from run to run; two of them are
  shown.

All threads means one per CPU: 18 on the M5 Pro, 4 on the runners, 3 on the macOS
runner. The OTel rows use the SDK's bound instruments, its fastest path, and for
counters also the plain `Counter::add`, which takes the attributes on every call; for
latencies, a bound histogram in seconds timed with `Instant::now()`. The histograms
have the latency preset's 19 bounds, with values spread over the first 12 buckets.

| Counters, ns per `add` | macOS arm64, Apple M5 Pro | Linux x86-64, AMD EPYC 7763 | Linux x86-64, Intel Xeon Platinum 8573C | Windows x64, AMD EPYC 7763 | Linux arm64, Neoverse-N2 | macOS arm64, Apple M1 (virtual) |
|---|---:|---:|---:|---:|---:|---:|
| nanometer, 1 thread | 1.1–1.2 | 0.6 | 0.6 | 0.6 | 1.8 | 1.5 |
| OTel bound, 1 thread | 1.8 | 4.4 | 7.6 | 5.0 | 7.6 | 5.8 |
| OTel `Counter::add`, 1 thread | 46 | 100 | 84 | 116 | 96 | 119 |
| nanometer, all threads | 1.1 | 1.3 | 1.1 | 1.4 | 1.8 | 1.7 |
| OTel bound, all threads | 156–159 | 90 | 84 | 83 | 44 | 29 |
| OTel `Counter::add`, all threads | 2,500–2,640 | 392 | 401 | 274 | 408 | 365 |

| Histograms, ns per `record` | macOS arm64, Apple M5 Pro | Linux x86-64, AMD EPYC 7763 | Linux x86-64, Intel Xeon Platinum 8573C | Windows x64, AMD EPYC 7763 | Linux arm64, Neoverse-N2 | macOS arm64, Apple M1 (virtual) |
|---|---:|---:|---:|---:|---:|---:|
| nanometer, 1 thread | 2.4–2.5 | 5.4 | 4.4 | 5.6 | 5.6 | 4.5 |
| OTel, 1 thread | 10.8–10.9 | 26 | 19 | 26 | 24 | 21 |
| nanometer, all threads | 3.4 | 9.6 | 8.9 | 9.9 | 5.6 | 5.4 |
| OTel, all threads | 589–668 | 348 | 459 | 587 | 455 | 178 |

| Latency histograms, ns per timed call | macOS arm64, Apple M5 Pro | Linux x86-64, AMD EPYC 7763 | Linux x86-64, Intel Xeon Platinum 8573C | Windows x64, AMD EPYC 7763 | Linux arm64, Neoverse-N2 | macOS arm64, Apple M1 (virtual) |
|---|---:|---:|---:|---:|---:|---:|
| nanometer, 1 thread | 2.7–2.9 | 50 | 31 | 50 | 27 | 6.9 |
| OTel, 1 thread | 47–49 | 99 | 79 | 117 | 104 | 113 |
| nanometer, all threads | 3.3–3.4 | 51 | 37 | 54 | 27 | 7.4 |
| OTel, all threads | 2,030–2,210 | 472 | 665 | 568 | 806 | 905 |

nanometer's handles cost a few nanoseconds everywhere and stay flat as threads are
added. On one thread the SDK's bound counter comes closest, since its `add` is a
single uncontended atomic add; as threads are added, their atomic adds all hit the
same cache line, while each nanometer handle writes cells of its own. The SDK's bound
histogram also takes a lock per series. Contended runs vary the most from run to run.
The M5's 18-thread runs mix both kinds of core, and on the x86 runners nanometer's
counter and histogram double at 4 threads, as an empty loop does: their 4 vCPUs are 2
cores with two hyperthreads each (Windows reports 2 cores, 4 logical processors).

nanometer's timer reads the CPU's counter twice and records the ticks against bounds
converted to ticks once; the SDK way pays two `Instant::now()` calls plus its locked
`f64` record, which is what grows with threads. The bench also times single clock
reads. Reading the counter takes 0.3–0.5 ns on the M5 and about 1 ns on the virtual
M1, but 8–13 ns on the server CPUs, whatever the OS; `Instant::now()` takes 13 ns on
the M5 and 19–38 ns on the runners. So the latency timer takes 27–50 ns on the server
CPUs instead of 3, still 2–4 times less than the SDK's way on one thread. On x86 the
timer also costs 10–21 ns more than its two reads and a record add up to; on arm64 it
costs about that sum or less.

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
