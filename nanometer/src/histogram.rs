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

use std::fmt;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use opentelemetry::KeyValue;

use crate::Meter;
use crate::registry::{self, Entry};
use crate::series::{Instance, SeriesSet, bump};

/// OpenTelemetry's default bucket bounds.
const DEFAULT_BUCKETS: &[u64] = &[
    0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1_000, 2_500, 5_000, 7_500, 10_000,
];

/// Powers of 4 from 64 B to 64 MiB.
const BYTES_BUCKETS: &[u64] = &[
    64,
    256,
    1 << 10,
    4 << 10,
    16 << 10,
    64 << 10,
    256 << 10,
    1 << 20,
    4 << 20,
    16 << 20,
    64 << 20,
];

/// 1-2-5 from 1 to 10,000.
const COUNTS_BUCKETS: &[u64] = &[
    1, 2, 5, 10, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000,
];

/// Name, unit, description and bucket bounds of a histogram of `u64` values.
///
/// A value counts in the first bucket whose bound is at least the value, and values
/// above the last bound in one more bucket, as in OpenTelemetry. `&str` converts into a
/// definition with OpenTelemetry's default bounds; presets cover common shapes:
///
/// ```
/// use nanometer::HistogramDef;
///
/// const QUEUE_DEPTH: HistogramDef = HistogramDef::new("queue_depth").buckets(&[1, 10, 100]);
/// const PAYLOAD: HistogramDef = HistogramDef::bytes("payload_size");
/// const BATCH: HistogramDef = HistogramDef::counts("batch_size").unit("{message}");
/// ```
///
/// For durations, see [`LatencyDef`](crate::LatencyDef).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HistogramDef {
    pub(crate) name: &'static str,
    pub(crate) unit: Option<&'static str>,
    pub(crate) description: Option<&'static str>,
    pub(crate) buckets: &'static [u64],
    /// A latency histogram: `buckets` are in µs, handles record clock ticks against
    /// them converted to ticks, and the export is in seconds.
    pub(crate) latency: bool,
}

impl HistogramDef {
    /// A histogram called `name`, with OpenTelemetry's default bucket bounds
    /// (0, 5, 10, 25 … 7,500, 10,000) and no unit or description.
    pub const fn new(name: &'static str) -> Self {
        HistogramDef {
            name,
            unit: None,
            description: None,
            buckets: DEFAULT_BUCKETS,
            latency: false,
        }
    }

    /// Sizes in bytes, unit `By`: powers of 4 from 64 B to 64 MiB.
    pub const fn bytes(name: &'static str) -> Self {
        HistogramDef::new(name).unit("By").buckets(BYTES_BUCKETS)
    }

    /// Counts, such as items per batch: 1-2-5 from 1 to 10,000.
    pub const fn counts(name: &'static str) -> Self {
        HistogramDef::new(name).buckets(COUNTS_BUCKETS)
    }

    /// A latency histogram with `buckets` in µs, exported in seconds.
    pub(crate) const fn latency(
        name: &'static str,
        description: Option<&'static str>,
        buckets: &'static [u64],
    ) -> Self {
        HistogramDef {
            name,
            unit: Some("s"),
            description,
            buckets,
            latency: true,
        }
    }

    /// Sets the unit, e.g. `"By"` or `"{message}"`.
    pub const fn unit(mut self, unit: &'static str) -> Self {
        self.unit = Some(unit);
        self
    }

    /// Sets the description.
    pub const fn description(mut self, description: &'static str) -> Self {
        self.description = Some(description);
        self
    }

    /// Sets the bucket bounds.
    ///
    /// # Panics
    ///
    /// If the bounds don't increase; at compile time for a `const` definition.
    pub const fn buckets(mut self, buckets: &'static [u64]) -> Self {
        let mut i = 1;
        while i < buckets.len() {
            assert!(buckets[i - 1] < buckets[i], "bucket bounds must increase");
            i += 1;
        }
        self.buckets = buckets;
        self
    }
}

impl From<&'static str> for HistogramDef {
    fn from(name: &'static str) -> Self {
        HistogramDef::new(name)
    }
}

/// One histogram metric: all its series, exported by [`Producer`](crate::Producer).
pub(crate) struct HistogramMetric {
    // Read by the producer, so only with the `sdk` feature, which the app enables.
    #[cfg_attr(not(feature = "sdk"), allow(dead_code))]
    pub(crate) scope: &'static str,
    pub(crate) def: HistogramDef,
    /// Start of the cumulative series.
    #[cfg_attr(not(feature = "sdk"), allow(dead_code))]
    pub(crate) start: SystemTime,
    pub(crate) series: Arc<Mutex<SeriesSet>>,
}

impl HistogramMetric {
    fn new(scope: &'static str, def: HistogramDef) -> Arc<Self> {
        Arc::new(HistogramMetric {
            scope,
            def,
            start: SystemTime::now(),
            // A count per bucket, one more for values above the last bound, and the sum.
            series: SeriesSet::new(def.buckets.len() + 2),
        })
    }

    fn get_or_register(meter: &Meter, def: HistogramDef) -> Arc<HistogramMetric> {
        let mut registry = registry::lock();
        match registry.get(&(meter.scope, def.name)) {
            // Latency and plain histograms count in different units, so they don't mix.
            Some(Entry::Histogram(metric)) if metric.def.latency == def.latency => {
                if metric.def != def {
                    registry::warn_conflict(meter.scope, def.name, &metric.def, &def);
                }
                Arc::clone(metric)
            }
            Some(other) => {
                registry::warn_conflict(meter.scope, def.name, &other.kind(), &def);
                // Not registered, so never exported.
                HistogramMetric::new(meter.scope, def)
            }
            None => {
                let metric = HistogramMetric::new(meter.scope, def);
                registry.insert(
                    (meter.scope, def.name),
                    Entry::Histogram(Arc::clone(&metric)),
                );
                metric
            }
        }
    }
}

/// A histogram instance owned by one thread: `record` finds the bucket, then does a
/// plain load and store on the bucket's count and on the sum.
///
/// `!Send` and `!Sync`: create one per thread or core. On drop its counts are folded into
/// the series, so the exported totals never go back. Exported only through a reader
/// with a [`Producer`](crate::Producer).
pub struct LocalHistogram {
    instance: Instance,
    buckets: &'static [u64],
    name: &'static str,
    _single_writer: PhantomData<*const ()>,
}

impl LocalHistogram {
    // Cold: keeps setup out of the code around the record path.
    #[inline(never)]
    pub(crate) fn new(meter: &Meter, def: HistogramDef, attributes: &[KeyValue]) -> Self {
        let metric = HistogramMetric::get_or_register(meter, def);
        // The registered definition, which may not be `def`.
        LocalHistogram::with_metric(&metric, metric.def.buckets, attributes)
    }

    /// A latency histogram, recording clock ticks against `buckets`, the definition's
    /// bounds in ticks.
    // Cold: keeps setup out of the code around the record path.
    #[inline(never)]
    pub(crate) fn latency(
        meter: &Meter,
        def: HistogramDef,
        attributes: &[KeyValue],
        buckets: &'static [u64],
    ) -> Self {
        let metric = HistogramMetric::get_or_register(meter, def);
        LocalHistogram::with_metric(&metric, buckets, attributes)
    }

    fn with_metric(
        metric: &HistogramMetric,
        buckets: &'static [u64],
        attributes: &[KeyValue],
    ) -> Self {
        debug_assert_eq!(buckets.len(), metric.def.buckets.len());
        LocalHistogram {
            instance: Instance::new(&metric.series, attributes),
            buckets,
            name: metric.def.name,
            _single_writer: PhantomData,
        }
    }

    /// Records `value`.
    #[inline]
    pub fn record(&self, value: u64) {
        self.add(value, value);
    }

    /// Records clock ticks, adding `ticks >> sum_shift` to the sum.
    #[inline]
    pub(crate) fn record_ticks(&self, ticks: u64, sum_shift: u32) {
        self.add(ticks, ticks >> sum_shift);
    }

    #[inline(always)]
    fn add(&self, value: u64, sum: u64) {
        let cells = self.instance.block().cells();
        let bucket = self.buckets.partition_point(|&bound| bound < value);
        bump(&cells[bucket], 1);
        bump(&cells[self.buckets.len() + 1], sum);
    }
}

impl fmt::Debug for LocalHistogram {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalHistogram")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}
