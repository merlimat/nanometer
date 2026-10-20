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
use std::sync::OnceLock;
use std::time::Duration;

use opentelemetry::KeyValue;

use crate::Meter;
use crate::histogram::{HistogramDef, LocalHistogram};

#[cfg(not(miri))]
use quanta::Clock;

/// 1-2-5 from 10 µs to 10 s, in µs.
const LATENCY_BUCKETS_US: &[u64] = &[
    10, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000, 20_000, 50_000, 100_000, 200_000,
    500_000, 1_000_000, 2_000_000, 5_000_000, 10_000_000,
];

/// Name and description of a latency histogram.
///
/// The buckets are fixed, 1-2-5 from 10 µs to 10 s (10, 20, 50 µs … 5, 10 s).
/// Latencies are timed with the CPU's counter, through [quanta], and exported in
/// seconds, unit `s`, as the OpenTelemetry semantic conventions do for durations.
/// `&str` converts into a definition without a description.
///
/// [quanta]: https://docs.rs/quanta
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
        HistogramDef::latency(self.name, self.description, LATENCY_BUCKETS_US)
    }
}

impl From<&'static str> for LatencyDef {
    fn from(name: &'static str) -> Self {
        LatencyDef::new(name)
    }
}

/// The clock latency handles read, and the bounds they compare ticks against.
struct Ticks {
    clock: Clock,
    /// `LATENCY_BUCKETS_US` in ticks.
    buckets: Box<[u64]>,
    /// Sums are kept in units of `1 << sum_shift` ticks, the largest power of two
    /// within a microsecond, so that a `u64` sum lasts as long as one in µs would. At
    /// 1 GHz, a sum of ticks would overflow within a year on a busy core.
    sum_shift: u32,
    ticks_per_ns: f64,
}

static TICKS: OnceLock<Ticks> = OnceLock::new();

/// The first call calibrates quanta's clock, which blocks for 10 to 200 ms, once per
/// process.
fn ticks() -> &'static Ticks {
    TICKS.get_or_init(|| {
        let clock = Clock::new();
        // quanta converts ticks to ns; invert it over a long span to go the other way.
        const SPAN: u64 = 1 << 40;
        let ticks_per_ns = SPAN as f64 / clock.delta_as_nanos(0, SPAN) as f64;
        let to_ticks = |us: u64| (us as f64 * 1_000.0 * ticks_per_ns) as u64;
        let buckets: Box<[u64]> = LATENCY_BUCKETS_US.iter().map(|&us| to_ticks(us)).collect();
        debug_assert!(buckets.windows(2).all(|w| w[0] < w[1]));
        Ticks {
            clock,
            buckets,
            sum_shift: to_ticks(1).checked_ilog2().unwrap_or(0),
            ticks_per_ns,
        }
    })
}

/// Seconds per unit of a latency histogram's sum.
#[cfg_attr(not(feature = "sdk"), allow(dead_code))]
pub(crate) fn seconds_per_sum_unit() -> f64 {
    let ticks = ticks();
    (1u64 << ticks.sum_shift) as f64 / ticks.ticks_per_ns / 1e9
}

/// A latency histogram instance owned by one thread, with a timer.
///
/// `!Send` and `!Sync`, like [`LocalHistogram`]: create one per thread or core. The
/// first latency handle in a process calibrates the clock, which blocks for 10 to
/// 200 ms; later ones don't.
///
/// ```
/// # let meter = nanometer::Meter::new(&*opentelemetry::global::meter_provider(), "doc");
/// let latency = meter.latency("put_latency", &[]);
///
/// // Records when the timer is dropped, at the end of the scope.
/// let _timer = latency.start();
/// ```
pub struct LocalLatency {
    histogram: LocalHistogram,
    clock: Clock,
    sum_shift: u32,
    ticks_per_ns: f64,
}

impl LocalLatency {
    // Cold: keeps setup out of the code around the record path.
    #[inline(never)]
    pub(crate) fn new(meter: &Meter, def: LatencyDef, attributes: &[KeyValue]) -> Self {
        let ticks = ticks();
        LocalLatency {
            histogram: LocalHistogram::latency(meter, def.histogram(), attributes, &ticks.buckets),
            clock: ticks.clock.clone(),
            sum_shift: ticks.sum_shift,
            ticks_per_ns: ticks.ticks_per_ns,
        }
    }

    /// Records `latency`.
    #[inline]
    pub fn record(&self, latency: Duration) {
        // Saturates for durations beyond the u64 range of ticks.
        let ticks = latency.as_nanos() as f64 * self.ticks_per_ns;
        self.record_ticks(ticks as u64);
    }

    #[inline(always)]
    fn record_ticks(&self, ticks: u64) {
        self.histogram.record_ticks(ticks, self.sum_shift);
    }

    /// Starts a timer that records the time elapsed when it is dropped or stopped.
    #[must_use = "the timer records when it is dropped, so dropping it at once records ~0"]
    #[inline]
    pub fn start(&self) -> Timer<'_> {
        Timer {
            latency: self,
            start: self.clock.raw(),
        }
    }
}

impl fmt::Debug for LocalLatency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("LocalLatency")
            .field(&self.histogram)
            .finish()
    }
}

/// Times one operation for a [`LocalLatency`]: records the time elapsed when dropped,
/// unless [stopped](Timer::stop) or [discarded](Timer::discard) first.
pub struct Timer<'a> {
    latency: &'a LocalLatency,
    /// Clock ticks, not nanoseconds.
    start: u64,
}

impl Timer<'_> {
    /// Records the time elapsed and returns it.
    pub fn stop(self) -> Duration {
        let end = self.latency.clock.raw();
        self.latency.record_ticks(end.saturating_sub(self.start));
        let elapsed = self.latency.clock.delta(self.start, end);
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
    #[inline]
    fn drop(&mut self) {
        let end = self.latency.clock.raw();
        self.latency.record_ticks(end.saturating_sub(self.start));
    }
}

impl fmt::Debug for Timer<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Timer")
            .field("latency", self.latency)
            .field("start_ticks", &self.start)
            .finish()
    }
}

/// Miri can't run quanta's inline assembly, so under Miri a tick is a nanosecond of
/// `Instant`.
#[cfg(miri)]
#[derive(Clone, Debug)]
struct Clock(std::time::Instant);

#[cfg(miri)]
impl Clock {
    fn new() -> Self {
        Clock(std::time::Instant::now())
    }

    fn raw(&self) -> u64 {
        self.0.elapsed().as_nanos() as u64
    }

    fn delta_as_nanos(&self, start: u64, end: u64) -> u64 {
        end.saturating_sub(start)
    }

    fn delta(&self, start: u64, end: u64) -> Duration {
        Duration::from_nanos(self.delta_as_nanos(start, end))
    }
}
