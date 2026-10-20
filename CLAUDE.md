# nanometer

Rust library of Counter / UpDownCounter / Gauge / Histogram handles whose record
path costs nanoseconds, exported through an existing OpenTelemetry pipeline.
Primary target: thread-per-core runtimes (compio on io_uring). Must also be usable
from ordinary multi-threaded code (tokio).

Deeper rationale, measurements and sources: `docs/design-notes.md`.
Working prototypes from the design phase: `reference/` (standalone crates, see below).

## Goals

- Record path: no locks, no read-modify-write atomics, no allocation, no attribute
  handling. Prototype targets: counter ~1–2 ns, histogram ~5 ns, flat across cores.
- Plug into the user's OTel setup: libraries pass in an OTel `MeterProvider` and a
  scope name; the app keeps its usual exporter.
- Libraries using nanometer depend only on the `opentelemetry` API crate where
  possible; SDK-level wiring (histogram export, see below) happens once in the app.

## API (draft, not decided)

```rust
let meter = nanometer::Meter::new(&provider, "my-lib");   // OTel provider + scope name

let reqs = meter.counter("requests", &attrs);              // LocalCounter: !Send
reqs.add(1);

const DEPTH: HistogramDef = HistogramDef::new("queue_depth")
    .unit("{item}").buckets(&[1, 10, 100, 1_000]);
let depth = meter.histogram(DEPTH, &attrs);                 // LocalHistogram: !Send
depth.record(v);
let size = meter.histogram(HistogramDef::bytes("payload"), &attrs); // presets: bytes, counts

let lat = meter.latency("put_latency", &attrs);            // LocalLatency: fixed 1-2-5
let _t = lat.start();                                      // buckets 10 µs–10 s, quanta
                                                           // ticks in, seconds out; Timer
                                                           // records on drop (.stop/.discard)

let shared = meter.shared_counter("requests", &attrs);     // Send + Sync, striped fetch_add
```

- First arg is `impl Into<CounterDef>` / `<HistogramDef>` / `<LatencyDef>`: `&str` gives a bare
  definition; `const` defs carry unit, description and buckets, defined in one place.
- `meter` is nanometer's own type, NOT an extension trait on `opentelemetry::metrics::Meter`:
  OTel's `Meter` exposes no scope or identity (single private `Arc` field), and scope is
  part of metric identity in OTLP.
- `counter()` / `histogram()` return `!Send` local handles (one instance per thread or
  core). `shared_*` variants exist for code that can't hold `!Send` across `.await`.
- Attributes are resolved once at handle creation; the hot path never sees `KeyValue`.

## Architecture (decided)

- **Process-wide registry** keyed by (scope, metric name) → series (attributes sorted and
  deduplicated) → instances (cells). Creating a handle = lock + hash lookup + one
  allocation; fine at core start or topic load, never per request.
- **Exactly one OTel observable instrument + one callback per metric**, summing all
  instances per series. NEVER register one observable per instance: the SDK keeps only
  the last observation per attribute set in a collection (verified: cores at 5 and 7
  exported as 7, not 12 — `reference/otel-sdk-bench/src/bin/dup_observable_trap.rs`).
- **Cells are `AtomicU64`, written with Relaxed load + store** (single writer, no
  `lock`-prefixed RMW; compiles to a plain `mov` on x86-64/ARM64). Not `Cell<u64>`: the
  exporter reads concurrently, which would be a data race (UB) even though the handle is
  `!Send`.
- **Memory:** allocate each instance's cells from a per-thread arena (8 B per series per
  thread; only arenas are cache-line separated). Padding every cell to 128 B costs 16×.
- **Drop:** fold an instance's final value into a per-series base so cumulative values
  never go backwards when a core or handle goes away.
- **Gauge:** one atomic per series, last write wins. **UpDownCounter:** per-instance `i64`,
  summed like counters.
- **Histogram:** per-instance bucket counts plus an integer sum (e.g. µs, not f64).
  Bucket = first bound >= value (`partition_point(|b| b < v)`, same rule as OTel).
  At export, count = sum of buckets, so a snapshot taken during in-flight records stays
  self-consistent. No min/max/exemplars initially.
- **Conflicting definitions** (same name, different kind/unit/buckets): warn, first wins.
- **Cardinality cap:** writes past the cap go to an overflow series tagged
  `otel.metric.overflow=true`, as OTel does.
- **No `Context` on the record path, no exemplars.**

## Histogram export — the blocker

Counters, up-down counters and gauges export through observable instruments and work with
any provider/exporter today. Histograms cannot, because OTel has no async histogram and the
Rust SDK offers no way to inject externally aggregated data:

- `MetricProducer` was removed in SDK 0.26 (PR #2105, to shrink the 1.0 scope — not a
  design objection; the spec defines it and Go/Java implement it).
- Since 0.30, metric data fields are `pub(crate)` and `PushMetricExporter::export` takes
  `&ResourceMetrics`.

Options (keep the choice behind an internal `Sink`-style trait so it can change without
touching users):
1. Upstream: re-add `MetricProducer` to opentelemetry-rust. Best long-term.
2. Fallback inside nanometer: back histograms with SDK bound histograms (0.32+, feature
   `experimental_metrics_bound_instruments`): ~31 ns uncontended, ~185 ns contended.
3. Pin SDK 0.29: wrap the app's `PushMetricExporter` and append to `&mut ResourceMetrics`.
   Verified end to end via the OTLP proto conversion (`reference/otel029-exporter-hook`).
   Forces the whole dependency tree onto 0.29; version skew is silent.
4. Fork 0.33 with the same small change via `[patch.crates-io]`.

**Chosen for v0 (user, Oct 19): option 4 now, option 1 in parallel.** The fork is branch
`metric-producer-0.33.1` of github.com/merlimat/opentelemetry-rust: feature
`experimental_metrics_producer` adds `MetricProducer`, `with_producer` on the readers,
and constructors for the data types. nanometer's workspace patches both `opentelemetry`
and `opentelemetry_sdk` to it (patching only the SDK pulls a second API crate), apps must
do the same, and `nanometer::Producer` (feature `sdk`) is the producer. The upstream PR
tracks opentelemetry-rust#2505. Never rewrite the fork branch: the patch pins it.

The Prometheus pull exporter can't be hooked this way (and is discontinued at 0.28): push
OTLP to a Collector or to Prometheus's OTLP receiver.

## Open decisions (ask before assuming)

- Exponential histograms (O(1) bucket index via bit ops; maps to OTel
  ExponentialHistogram / Prometheus native histograms): later. The presets use
  explicit buckets.
- Delta temporality for injected histograms (keep previous snapshot and diff). Today
  `Producer` exports cumulative only, whatever the reader's temporality.
- Series removal semantics (e.g. per-topic metrics when topics unload).
- `f64` counters: needed, or integers only?
- MSRV and edition.

## Suggested first steps

1. `cargo init --lib` as a workspace with the library crate; leave `reference/` untouched.
2. Registry + `LocalCounter` + observable export. Tests against a `ManualReader`,
   including the duplicate-observable trap and the drop-folding rule.
3. Benchmark harness (port `reference/otel-sdk-bench`): report ns/op and allocs/op.
4. `LocalHistogram` + the chosen export path.

## Conventions

- Benchmarks: release + `lto = "fat"`, `codegen-units = 1`; warm up; report ns/op AND
  allocs/op (counting global allocator); single-thread and multi-thread; state the
  hardware. Subtract or report the empty-loop overhead (~0.7 ns on the reference VM).
- Verify claims about OTel behavior against SDK source at the exact release tag, and with
  a small test where cheap. The OTel Rust API has changed a lot between minor versions.
- Pin OTel versions exactly (`=0.33.1`) until the export path is settled.

## reference/ (standalone crates, each with its own `[workspace]`)

- `otel-sdk-bench/` — OTel SDK 0.33.1:
  - `src/main.rs`: SDK hot-path cost (unbound/bound counters and histograms, allocation
    traps, 2-thread contention).
  - `src/bin/percore_histogram.rs`: per-core shard histogram prototype (~5 ns, 0 allocs).
  - `src/bin/counter_instance.rs`: `!Send` instance vs thread-local slot vs shared
    `fetch_add`.
  - `src/bin/dup_observable_trap.rs`: shows the last-observation-wins trap.
- `otel029-exporter-hook/` — OTel SDK 0.29.0: exporter wrapper that injects a histogram,
  checked through the OTLP proto conversion.

Run with `cargo run --release` (add `--bin <name>` for the extra binaries).
