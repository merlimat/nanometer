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
  and a store on a cell only its thread writes. The targets, from the design
  prototypes, are about 1–2 ns per counter add and 5 ns per histogram record, flat
  as cores are added. For comparison, an OTel SDK 0.33.1 counter `add` with three
  attributes took 167 ns on one thread and 458 ns with two threads on one series, on
  a 2-vCPU Xeon VM ([measurements](docs/design-notes.md)).
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
- [ ] Benchmark harness reporting ns/op and allocs/op.
- [ ] `LocalHistogram`. Export is the open problem: OTel has no asynchronous
  histogram, and the Rust SDK no longer lets anything inject aggregated data
  (`MetricProducer` was removed in 0.26, and since 0.30 an exporter can't modify
  what it exports; [details](docs/design-notes.md#4-export-plumbing-findings)).
- [ ] Up-down counters, gauges, `Send + Sync` shared handles and a cardinality cap.

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

## Building

```sh
cargo test
cargo clippy --all-targets
cargo fmt --check
```

Under Miri, the integration tests need clock access, because the SDK reads the time:

```sh
MIRIFLAGS=-Zmiri-disable-isolation cargo +nightly miri test
```

[docs/design-notes.md](docs/design-notes.md) has the measurements and reasoning behind
the design. `reference/` holds the prototypes from the design phase, as standalone
crates.

## License

Copyright 2026 Matteo Merli. Licensed under the [Apache License, Version 2.0](LICENSE).
