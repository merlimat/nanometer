# nanometer — design notes

Background and evidence behind the decisions in `CLAUDE.md`, gathered Oct 15–19, 2026.
Unless stated otherwise, numbers come from a 2-vCPU cloud VM (Intel Xeon @ 2.80 GHz,
1 thread per core, no pinning), Rust 1.97, `--release`, `lto = "fat"`, `codegen-units = 1`.
Absolute numbers are slow on that VM (upstream reports ~50 ns unbound / ~2 ns bound on
faster hardware); the ratios are what matter. A smoke re-run on another VM on Oct 19 was
30–50% faster in absolute terms. The full raw run is in `otel_rust_metrics_cost.xlsx`.

## 1. Cost of the OTel Rust SDK record path (0.33.1)

3 attributes, steady state.

| Path | 1 thread | 2 threads, same series | 2 threads, distinct series | allocs/op |
|---|---|---|---|---|
| Counter `add(1, &attrs)` | 167 ns | 458 ns | 410 ns | 0 |
| Histogram `record(v, &attrs)` | 180 ns | 856 ns | 608 ns | 0 |
| Bound counter `add` (0.32+, experimental) | 13 ns | 41 ns | 10 ns | 0 |
| Bound histogram `record` | 31 ns | 185 ns | 27 ns | 0 |
| Per-core shard histogram (prototype) | 5.2 ns | 5.9 ns | — | 0 |
| Baseline: shared `AtomicU64::fetch_add` | 6 ns | 21 ns | — | 0 |
| Baseline: owned cell, relaxed load+store | 1.4 ns | — | 2.4 ns | 0 |

Why, from the source (`opentelemetry-sdk/src/metrics/internal/{mod,sum,histogram,aggregate}.rs`):
- Unbound path: read lock on a per-instrument `RwLock<HashMap<Vec<KeyValue>, Arc<Tracker>>>`
  plus SipHash over every attribute string, on every call.
- The RwLock reader count is shared by all series of an instrument, so even per-thread
  series bounce a cache line.
- Every histogram series is a `Mutex<Buckets>`; bound handles skip the lookup but still
  take that lock.
- Bound handles still do an atomic RMW plus a Release store to `has_been_updated` per record.
- No `Context` read and no exemplars on the record path.

### Allocation traps (1 allocation per call)

| Trigger | ns/op | vs no-alloc case | Cause | Avoid by |
|---|---|---|---|---|
| Same attributes in a different order from first-seen/sorted | 391 | 2.3× | lookup misses, `sort_and_dedup()` does `to_vec()` every call | always pass attributes in one fixed order |
| View with `allowed_attribute_keys`, unbound path | 288 | 1.7× | the filter collects into a new `Vec` every call | no view, or bind (filter applied once) |
| `String` attribute value | 193 | 1.2× | caller-side clone | `&'static str` or `Arc<str>` (0 allocs) |

The first record of a new series allocates (map insert); that's expected and one-time.

## 2. Prior art: Oxia PR #1457 (Go)

Replaced synchronous OTel SDK histograms with atomic, adder-backed histograms exported by a
`metric.Producer` registered via `prometheus.WithProducer`. One adder per bucket plus one
for the sum (striped only when contended, #1455); bounds moved into the library; bucket rule
"first bound >= value"; integer µs sum divided at export; no min/max/exemplars. Results on
M1 Max: 184.6 ns/1 alloc → 8.8 ns/0 allocs (serial), 244 ns → 5.6 ns (parallel-10).

nanometer goes one step further: under thread-per-core each instance has exactly one
writer, so increments need no RMW at all (Relaxed load + store).

## 3. Per-thread counter cost: `!Send` instance vs thread-local lookup

`reference/otel-sdk-bench/src/bin/counter_instance.rs`, 200M iterations, 3 runs. The empty
loop costs 0.6–0.8 ns and is not subtracted.

| Per add | 1 thread | 2 threads |
|---|---|---|
| `!Send` instance, own cell (relaxed load+store) | 1.8–2.0 ns | 1.8–2.2 ns |
| Thread-local slot + flat arena | 1.6 ns | 1.6–1.9 ns |
| Thread-local slot + nested `Vec`, bounds-checked | 2.2–2.5 ns | 2.3–2.8 ns |
| One shared `AtomicU64::fetch_add` | 7.5–7.7 ns | 29–31 ns |

The thread-local lookup is not the cost (it's one fs-relative load in an executable).
`!Send` instances won on simplicity: no slot pool, no exhaustion, no recycling on thread
exit, no unsafe stride math. A plain `Cell<u64>` flushed to the atomic each event-loop tick
could be cheaper still, but adds staleness and a flush hook; not worth it at ~1 add per request.

## 4. Export plumbing findings

- **Observable instruments work with any provider/exporter**, and the SDK converts
  cumulative observable values to delta for delta readers.
- **Last observation wins** per attribute set within a collection: two observable counters
  named `requests` with identical attributes, observing 5 and 7, export 7.
  → one callback per metric, summing all instances.
- **`MetricProducer` history:** existed in opentelemetry-rust, removed in 0.26.0
  (released 2024-09-30, PR #2105). Rationale: "an optional thing" with "no tests or
  documentation", removed "to keep the scope for 1.0 stable minimized". Nothing about
  re-adding it.
- **0.29.0 is the last release with an injection point:** all data fields `pub`,
  `PushMetricExporter::export(&self, metrics: &mut ResourceMetrics)`, `Metric.data:
  Box<dyn Aggregation>` with public `Histogram<T>` / `HistogramDataPoint<T>`.
  `opentelemetry-otlp` 0.29 (tonic and http) serializes with
  `ExportMetricsServiceRequest::from(&*metrics)`, which downcasts to `Histogram<i64|u64|f64>`.
  The wrapper test injected a cumulative histogram that came out with exact bucket counts,
  count and sum over two collections.
- **0.30.0 closed it:** fields `pub(crate)`, `export(&self, metrics: &ResourceMetrics)`.
- **0.29 costs:** whole tree pinned (any crate on `opentelemetry` 0.30+ gets separate
  types and a separate global, so its metrics are silently lost); matching
  `tracing-opentelemetry` is 0.30; no pull exporter (`opentelemetry-prometheus` is
  discontinued, at 0.28, and its collector calls its internal reader directly); delta
  temporality must be computed by the wrapper; misses four releases of fixes and features
  (e.g. bound instruments in 0.32).
- **OTel `Meter` exposes no identity or scope** (`pub struct Meter { pub(crate)
  instrument_provider: Arc<dyn InstrumentProvider + Send + Sync> }`), hence nanometer's
  own `Meter` type holding the scope name.

## 5. Ideas not yet decided

- Exponential (base-2) histograms: O(1) bucket index from leading zeros plus top mantissa
  bits instead of a binary search; maps to OTel ExponentialHistogram and Prometheus native
  histograms. A dense layout costs memory per series per instance (e.g. ~320 buckets at
  scale 3 over [1, 2^40)), so consider sparse or bounded ranges.
- Prometheus-only stopgap for histograms: export buckets as `_bucket{le}` observable
  counters plus `_sum`/`_count`. `histogram_quantile` works, but OTLP backends see sums,
  not a histogram.

## 6. Name

`nanometer`: an OTel Meter whose records cost nanoseconds. Free on crates.io as of
2026-10-16 (also free then: tinos, corecount, zerocount, bucketeer, tallyho, picometer).

## Sources

- opentelemetry-rust tags `opentelemetry_sdk-0.33.1`, `-0.30.0`, `-0.29.0`: SDK
  `metrics/internal/*`, `metrics/data/mod.rs`, `metrics/exporter.rs`, `benches/bound_instruments.rs`,
  `CHANGELOG.md` (v0.26.0 entry); `opentelemetry-proto/src/transform/metrics.rs`;
  `opentelemetry-otlp/src/exporter/{tonic/metrics.rs,http/mod.rs}`;
  `opentelemetry-prometheus/{README.md,src/lib.rs}`.
- https://github.com/open-telemetry/opentelemetry-rust/pull/2105 — MetricProducer removal.
- https://github.com/oxia-db/oxia/pull/1457 — adder-backed histograms + Producer (Go).
