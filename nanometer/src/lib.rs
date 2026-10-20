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

//! Metric handles whose record path costs nanoseconds, exported through an existing
//! OpenTelemetry pipeline.
//!
//! ```
//! use opentelemetry::KeyValue;
//!
//! let provider = opentelemetry::global::meter_provider();
//! let meter = nanometer::Meter::new(&*provider, "my-lib");
//!
//! let attrs = [KeyValue::new("op", "put")];
//! let requests = meter.counter("requests", &attrs);
//! let latency = meter.latency("put_latency", &attrs);
//!
//! let _timer = latency.start(); // records when dropped
//! requests.add(1);
//! ```
//!
//! Counters, up-down counters and gauges export through OTel observable instruments,
//! with any provider. Histograms
//! export through [`Producer`], registered with the app's reader, which needs the
//! `sdk` feature and an OTel SDK with `MetricProducer` support.

mod arena;
mod counter;
mod gauge;
mod histogram;
mod latency;
#[cfg(feature = "sdk")]
mod producer;
mod registry;
mod series;

pub use counter::{CounterDef, LocalCounter, LocalUpDownCounter, UpDownCounterDef};
pub use gauge::{Gauge, GaugeDef, ObservableGauge};
pub use histogram::{HistogramDef, LocalHistogram};
pub use latency::{LatencyDef, LocalLatency, Timer};
#[cfg(feature = "sdk")]
pub use producer::Producer;

use opentelemetry::KeyValue;
use opentelemetry::metrics::MeterProvider;

/// Creates metric handles for one instrumentation scope.
///
/// Metrics live in a process-wide registry keyed by (scope, metric name): handles for the
/// same metric share one export, however many `Meter`s create them. A counter's OTel
/// observable instrument is registered with the provider of the `Meter` that first
/// creates the counter.
#[derive(Clone, Debug)]
pub struct Meter {
    scope: &'static str,
    otel: opentelemetry::metrics::Meter,
}

impl Meter {
    /// A meter for `scope`, exporting through `provider`.
    pub fn new<P: MeterProvider + ?Sized>(provider: &P, scope: &'static str) -> Self {
        Meter {
            scope,
            otel: provider.meter(scope),
        }
    }

    /// A counter instance for the calling thread.
    ///
    /// Attributes are resolved here, once: order and repeated keys don't matter (the
    /// last value of a key wins). Takes a lock and allocates, so create handles at
    /// startup, not per request.
    pub fn counter(&self, def: impl Into<CounterDef>, attributes: &[KeyValue]) -> LocalCounter {
        LocalCounter::new(self, def.into(), attributes)
    }

    /// An up-down counter instance for the calling thread. Attributes are resolved as
    /// for [`counter`](Meter::counter).
    pub fn up_down_counter(
        &self,
        def: impl Into<UpDownCounterDef>,
        attributes: &[KeyValue],
    ) -> LocalUpDownCounter {
        LocalUpDownCounter::new(self, def.into(), attributes)
    }

    /// A gauge handle, set from any thread; handles for the same series share its value.
    /// Attributes are resolved as for [`counter`](Meter::counter).
    pub fn gauge(&self, def: impl Into<GaugeDef>, attributes: &[KeyValue]) -> Gauge {
        Gauge::new(self, def.into(), attributes)
    }

    /// A gauge that exports what `callback` returns, at each collection, until the
    /// returned handle is dropped. Attributes are resolved as for
    /// [`counter`](Meter::counter).
    pub fn observable_gauge(
        &self,
        def: impl Into<GaugeDef>,
        attributes: &[KeyValue],
        callback: impl Fn() -> i64 + Send + Sync + 'static,
    ) -> ObservableGauge {
        ObservableGauge::new(self, def.into(), attributes, callback)
    }

    /// A histogram instance for the calling thread. Attributes are resolved as for
    /// [`counter`](Meter::counter).
    pub fn histogram(
        &self,
        def: impl Into<HistogramDef>,
        attributes: &[KeyValue],
    ) -> LocalHistogram {
        LocalHistogram::new(self, def.into(), attributes)
    }

    /// A latency histogram instance for the calling thread, with a timer. Attributes are
    /// resolved as for [`counter`](Meter::counter).
    pub fn latency(&self, def: impl Into<LatencyDef>, attributes: &[KeyValue]) -> LocalLatency {
        LocalLatency::new(self, def.into(), attributes)
    }
}
