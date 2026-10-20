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
use std::mem;
use std::time::{Duration, Instant};

use opentelemetry::KeyValue;

use crate::Meter;
use crate::histogram::{HistogramDef, LocalHistogram};

/// 1-2-5 from 10 µs to 10 s, in µs.
const LATENCY_BUCKETS_US: &[u64] = &[
    10, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000, 20_000, 50_000, 100_000, 200_000,
    500_000, 1_000_000, 2_000_000, 5_000_000, 10_000_000,
];

/// Name and description of a latency histogram.
///
/// The buckets are fixed, 1-2-5 from 10 µs to 10 s (10, 20, 50 µs … 5, 10 s).
/// Latencies are recorded in whole microseconds and exported in seconds, unit `s`, as
/// the OpenTelemetry semantic conventions do for durations. `&str` converts into a
/// definition without a description.
///
/// ```
/// use nanometer::LatencyDef;
/// const PUT: LatencyDef = LatencyDef::new("put_latency").description("Time to apply a put");
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LatencyDef {
    name: &'static str,
    description: Option<&'static str>,
}

impl LatencyDef {
    /// A latency histogram called `name`, with no description.
    pub const fn new(name: &'static str) -> Self {
        LatencyDef {
            name,
            description: None,
        }
    }

    /// Sets the description.
    pub const fn description(mut self, description: &'static str) -> Self {
        self.description = Some(description);
        self
    }

    const fn histogram(self) -> HistogramDef {
        HistogramDef::micros_as_seconds(self.name, self.description, LATENCY_BUCKETS_US)
    }
}

impl From<&'static str> for LatencyDef {
    fn from(name: &'static str) -> Self {
        LatencyDef::new(name)
    }
}

/// A latency histogram instance owned by one thread, with a timer.
///
/// `!Send` and `!Sync`, like [`LocalHistogram`]: create one per thread or core.
///
/// ```
/// # let meter = nanometer::Meter::new(&*opentelemetry::global::meter_provider(), "doc");
/// let latency = meter.latency("put_latency", &[]);
///
/// // Records when the timer is dropped, at the end of the scope.
/// let _timer = latency.start();
/// ```
pub struct LocalLatency(LocalHistogram);

impl LocalLatency {
    pub(crate) fn new(meter: &Meter, def: LatencyDef, attributes: &[KeyValue]) -> Self {
        LocalLatency(LocalHistogram::new(meter, def.histogram(), attributes))
    }

    /// Records `latency`, in whole microseconds.
    #[inline]
    pub fn record(&self, latency: Duration) {
        self.0
            .record(u64::try_from(latency.as_micros()).unwrap_or(u64::MAX));
    }

    /// Starts a timer that records the time elapsed when it is dropped or stopped.
    #[must_use = "the timer records when it is dropped, so dropping it at once records ~0"]
    pub fn start(&self) -> Timer<'_> {
        Timer {
            latency: self,
            start: Instant::now(),
        }
    }
}

impl fmt::Debug for LocalLatency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("LocalLatency").field(&self.0).finish()
    }
}

/// Times one operation for a [`LocalLatency`]: records the time elapsed when dropped,
/// unless [stopped](Timer::stop) or [discarded](Timer::discard) first.
pub struct Timer<'a> {
    latency: &'a LocalLatency,
    start: Instant,
}

impl Timer<'_> {
    /// Records the time elapsed and returns it.
    pub fn stop(self) -> Duration {
        let elapsed = self.start.elapsed();
        self.latency.record(elapsed);
        // Recorded already: skip `drop`.
        mem::forget(self);
        elapsed
    }

    /// Drops the timer without recording, e.g. for an operation that failed.
    pub fn discard(self) {
        mem::forget(self);
    }
}

impl Drop for Timer<'_> {
    fn drop(&mut self) {
        self.latency.record(self.start.elapsed());
    }
}

impl fmt::Debug for Timer<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Timer")
            .field("latency", self.latency)
            .field("start", &self.start)
            .finish()
    }
}
