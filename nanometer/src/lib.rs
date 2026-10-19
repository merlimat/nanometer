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
//! let requests = meter.counter("requests", &[KeyValue::new("op", "put")]);
//! requests.add(1);
//! ```

mod arena;
mod counter;

pub use counter::{CounterDef, LocalCounter};

use opentelemetry::KeyValue;
use opentelemetry::metrics::MeterProvider;

/// Creates metric handles for one instrumentation scope.
///
/// Metrics live in a process-wide registry keyed by (scope, metric name): handles for the
/// same metric share one OTel observable instrument, however many `Meter`s create them.
/// That instrument is registered with the provider of the `Meter` that first creates the
/// metric.
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
}
