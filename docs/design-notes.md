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

## 7. Latency bucket bounds

Chosen Nov 5, 2026. Backends estimate quantiles from bucket counts:
`histogram_quantile` finds the bucket that holds the quantile and interpolates
linearly inside it, so the estimate can be anywhere in that bucket. A max is only
available as `histogram_quantile(1, …)`, the upper bound of the slowest value's
bucket. A value above the last bound reads as that bound, and one in the first bucket
as anything from 0 to its bound.

nanometer 0.1's latency preset was 1-2-5 from 10 µs to 10 s, 19 bounds 2 to 2.5 times
apart: a 3 ms max read as 5 ms, and an operation of a few µs as anything up to 10 µs.
It is now Renard's R5 from 100 ns to 63 s, 45 bounds each 1.56 to 1.6 times the last,
so an estimate is within 1.6 times the true value: a 3 ms max reads as 4 ms. Of 0.1's
bounds, the powers of ten are still bounds, so a query that names one of those `le`
values still finds it; one that names a 2 or a 5, an SLO at 50 ms say, doesn't.

- **Not R10.** Ten per decade (1, 1.25, 1.6, 2, 2.5, 3.15, 4, 5, 6.3, 8) from 1 µs to
  100 s reads within 1.28 times and keeps all of 0.1's bounds, but takes 81 bounds. A
  backend that stores Prometheus-style buckets keeps a series per bucket, plus `_sum`
  and `_count`: 84 per histogram series, against 48 for R5 and 22 for 0.1.
- **Floor, 100 ns.** An operation served from memory, such as a cached get, can take a
  few hundred ns. A measurement includes about one counter read: under a nanosecond on
  Apple silicon, 8–13 ns on the server CPUs measured. Apple's counter moves every
  42 ns (its timebase is 24 MHz; the M5's counter reads in ns, in steps of about 42),
  and each bucket from 100 ns up spans at least one step.
- **Ceiling, 63 s.** The first R5 bound past 50 s. A client that gives a server 10 s to
  answer takes a little more for an operation that runs into that timeout, past 0.1's
  last bound. Ending at 40 s would save a series, at 100 s cost one.
- **Sum.** It counts in units of the largest power of two of ticks within 100 ns, 50 to
  100 ns (64 ns on the M5), and each record rounds down to the unit, so a mean reads
  at most one unit low: on the M5, 1,000 records of 300 ns export a mean of 256 ns,
  and of 2 µs a mean of 1.98 µs. 0.1's unit was the largest within a µs, under which
  those read 0 and 1.54 µs. A `u64` sum lasts 8 to 16 times less than one of 0.1's
  units: about three years at an average of 10,000 operations in flight on one series.
- **Cost.** The bucket lookup is `partition_point`, a branchless binary search: 6 steps
  instead of 5, about 0.2 ns more per record on the M5 Pro (below). Each handle holds
  47 cells instead of 21: 376 B, within a regular arena chunk (512 B), instead of
  168 B. Each exported point carries 46 buckets instead of 20.
- **A slower or busier core hides less of the search.** Each step waits for the
  previous step's load. With R10's 81 bounds, on the M5's 12 Performance cores, the
  slower of its two kinds (forced with background QoS), a record took about 17 ns
  instead of 13 with 19 bounds, against 3.0 and 2.5 ns on its 6 Super cores; and
  while other builds loaded the machine (load average 5 to 15), the histogram row of
  `benches/record.rs` varied from 3.1 to 7.1 ns per record from one run to the next,
  where 19 bounds held at 2.5 ns and the timer, which always records into the first
  bucket, at 3.3 ns. With R5's 45 bounds, under a load average of 7 to 28, that row
  stayed 0.12 to 0.23 ns above 19 bounds in 6 of 7 alternating runs. On GitHub's
  Neoverse-N2 runners, one thread's record took 6.6 ns with 45 bounds in November
  against 5.6 with 19 in October, where the counter (1.8 ns) and the timer (27 ns)
  didn't move. That row records back to back, the worst case for a long chain; one
  record per request overlaps it with other work.

`LocalHistogram::record` on one thread of the M5 Pro, ns per record, the same compiled
loop over each table (median of nine runs in three batches, while other work kept the
load average between 7 and 28, so compare rows rather than figures; the generator
alone costs 2.39 ns, the array read 0.25 ns):

| Bounds | Search steps | Values from the bench's generator, 0–30 ms | Values from an array, 0.5 µs–200 s log-uniform |
|---|---:|---:|---:|
| 1-2-5, 10 µs–10 s (19) | 5 | 2.61 | 2.03 |
| R5, 100 ns–63 s (45) | 6 | 2.84 | 2.26 |
| R10, 1 µs–100 s (81) | 7 | 3.22 | 2.69 |

On an idle machine the same loop took 2.40 and 1.82 ns with 19 bounds, 2.59 and
2.05 ns with R10 from 10 µs to 10 s (61 bounds, 6 steps), and 2.86 and 2.47 ns with 81.

## 8. Span recording

Added Nov 9, 2026, for redwood, a thread-per-core log store on compio that records
its metrics with nanometer and wants OpenTelemetry tracing too: free when off, and
cheap enough to sample 5% of appends in production. Volume to design for: up to
~350k spans/s per node in the worst case (1M records/s, one record per batch, 5%,
RF=3), more typically 35k/s or less. Spans live in nanometer because the shape is
the metrics' shape: each core records into memory of its own, with no lock and no
allocation, and an exporter collects.

- **Not `tracing` + `tracing-opentelemetry`.** Most of a span's cost (the
  registry's slab insert, its extensions, allocation) is paid even for a span that
  isn't sampled.
- **Not the SDK's `Tracer`.** It allocates every span, and its `BatchSpanProcessor`
  sends finished spans through one channel that every thread shares. Measured below:
  127 ns and one allocation per span on one thread, 3.4 µs on 18.
- **Sampling, IDs and propagation stay in the application.** nanometer records the
  spans it is given; it doesn't decide which, and never sees a `traceparent`.

Layout:

- **`SpanRecord`, 784 B, `Copy`, no heap data.** IDs (OTel's `TraceId`, `SpanId`,
  `TraceFlags`, which the API crate has without its `trace` feature), kind, status,
  a `&'static str` name, start and end in quanta ticks, and fixed arrays: 8
  attributes (`u64`/`i64`/`bool`/`&'static str`, 40 B each), 4 events (name and
  ticks, 24 B), 8 links (trace, span, flags: 32 B, as `TraceId` is a `u128`). More
  are dropped and counted, and export as OTel's dropped counts. 8 links, more than
  events, because a batch links the sampled requests it carries.
- **Slab of spans in progress, per recorder, allocated up front.** A LIFO free list
  of `u16` indices (capacity fixed, never grows). `ActiveSpan` is a `NonZeroU32`:
  16 bits of slot, 16 of generation, so a handle used after its span ended is
  ignored rather than writing into the slot's next span (until the generation wraps
  after 65,535 reuses). Full slab: `start` returns `None` and counts.
- **One SPSC ring of finished spans per recorder.** `head` and `tail` on separate
  128 B lines; the producer keeps its own copy of `head` and a cached `tail`, read
  again only when the ring looks full, so a push doesn't touch the consumer's line.
  `end` copies the header and only the array entries in use (160 B for a span with
  two events, rather than 784 B). Full ring: the span is dropped and counted.
- **Process-wide list of rings**, as the metric registry is: a library needs only a
  `Meter` to record spans, with the meter's scope as the spans' instrumentation
  scope, and the app runs one collector for every library. Recorders take the lock
  only when created or dropped; drains take it, which serializes consumers, so each
  ring keeps a single consumer even with two collectors. A dropped recorder's ring
  stays until drained. Drains limited by `max` resume at the next ring, round-robin.
- **Drop counts are nanometer counters** (`nanometer.spans.dropped{reason}`) on the
  recorder's meter: `LocalCounter`s, a load and a store when a span is dropped.
- **Recorder methods take `&self`**, like the metric handles: the state sits in an
  `UnsafeCell`, sound because the recorder is `!Sync` and `!Send` and no method calls
  out while holding it. redwood keeps state in `Rc`/`RefCell` fields; `&self` lets it
  share the recorder through an `Rc` without a `RefCell` borrow per call.

Export:

- **Ticks to wall time from an anchor**, a `SystemTime` paired with the middle of two
  counter reads, taken afresh for each batch, with quanta's calibrated ticks per ns.
  Spans are at most one drain interval older than their anchor, so wall-clock
  adjustments in between don't matter; two batches may disagree by the slew between
  their anchors, microseconds at most.
- **`Collector` builds the SDK's `SpanData`** and calls any `SpanExporter`, in
  batches of `max_batch` (512, the batch processor's default), and stops at the
  first batch that isn't full so a busy core can't keep it going. Its
  `SpanExporter::export` is async; a blocking caller uses `futures_executor::block_on`,
  as the SDK's batch processor does.
- **Feature `sdk-trace`, separate from `sdk`.** `SpanData` and `SpanExporter` are in
  the SDK release on crates.io, so span export needs no patch, unlike `Producer`;
  folding it into `sdk` would have required the fork for spans too. Recording needs
  no feature.

Cost, `benches/record.rs` on the Apple M5 Pro, one thread recording a span with a
parent, two events and end, while another thread drains every ring every 50 µs
(median of three runs; no span dropped, none allocates):

| | ns per span |
|---|---:|
| nanometer, start + 2 events + end | 16.9 |
| nanometer, + 2 attributes and 1 link | 16.8 |
| nanometer, start + 2 events + discard (no ring push; two runs) | 6.1 |
| OTel SDK `Tracer`, batch processor, 2 events | 127 (1 allocation) |

Most of a span is the push: its three or four lines of ring were last read by the
drainer's core, so writing them misses. On two threads a span takes 25–31 ns, not
investigated (the M5 has two kinds of core, and two rings of 6 MiB each exceed more
of the cache); on 18 the single drainer
can't keep up with recorders that do nothing else, and over half the spans drop.
Not yet measured on GitHub's x86 runners, where one read of the counter takes 9–13
ns: a span with two events reads it four times, so expect 36–52 ns more there. At
redwood's worst case, ~350k spans/s spread over its cores, a drainer thread has
microseconds per span to spare.

Open questions for redwood:

- Attribute values are `&'static str` at most: a topic name has to be interned (as
  attribute sets are, once, when a topic loads) or exported as a numeric ID.
- 8 attributes, 4 events and 8 links per span: enough for the append, client put and
  server put spans?
- No `TraceState` on spans or links: redwood's 25 B of context carry none.
- Span events have no attributes.

## Sources

- opentelemetry-rust tags `opentelemetry_sdk-0.33.1`, `-0.30.0`, `-0.29.0`: SDK
  `metrics/internal/*`, `metrics/data/mod.rs`, `metrics/exporter.rs`, `benches/bound_instruments.rs`,
  `CHANGELOG.md` (v0.26.0 entry); `opentelemetry-proto/src/transform/metrics.rs`;
  `opentelemetry-otlp/src/exporter/{tonic/metrics.rs,http/mod.rs}`;
  `opentelemetry-prometheus/{README.md,src/lib.rs}`.
- https://github.com/open-telemetry/opentelemetry-rust/pull/2105 — MetricProducer removal.
- https://github.com/oxia-db/oxia/pull/1457 — adder-backed histograms + Producer (Go).
