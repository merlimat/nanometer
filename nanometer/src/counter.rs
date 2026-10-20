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

use opentelemetry::KeyValue;
use opentelemetry::metrics::AsyncInstrument;

use crate::Meter;
use crate::registry::{self, Entry};
use crate::series::{Instance, SeriesSet, bump};

/// Name, unit and description of a counter.
///
/// `&str` converts into a bare definition; a `const` definition keeps unit and
/// description in one place:
///
/// ```
/// use nanometer::CounterDef;
/// const BYTES_IN: CounterDef = CounterDef::new("bytes_in").unit("By");
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CounterDef {
    pub(crate) name: &'static str,
    pub(crate) unit: Option<&'static str>,
    pub(crate) description: Option<&'static str>,
}

impl CounterDef {
    /// A counter called `name`, with no unit or description.
    pub const fn new(name: &'static str) -> Self {
        CounterDef {
            name,
            unit: None,
            description: None,
        }
    }

    /// Sets the unit, e.g. `"By"` or `"{request}"`.
    pub const fn unit(mut self, unit: &'static str) -> Self {
        self.unit = Some(unit);
        self
    }

    /// Sets the description.
    pub const fn description(mut self, description: &'static str) -> Self {
        self.description = Some(description);
        self
    }
}

impl From<&'static str> for CounterDef {
    fn from(name: &'static str) -> Self {
        CounterDef::new(name)
    }
}

/// Name, unit and description of an up-down counter, whose value can also go down, such
/// as a count of open connections or queued items.
///
/// `&str` converts into a bare definition:
///
/// ```
/// use nanometer::UpDownCounterDef;
/// const QUEUED: UpDownCounterDef = UpDownCounterDef::new("queued").unit("{message}");
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UpDownCounterDef(CounterDef);

impl UpDownCounterDef {
    /// An up-down counter called `name`, with no unit or description.
    pub const fn new(name: &'static str) -> Self {
        UpDownCounterDef(CounterDef::new(name))
    }

    /// Sets the unit, e.g. `"{connection}"`.
    pub const fn unit(self, unit: &'static str) -> Self {
        UpDownCounterDef(self.0.unit(unit))
    }

    /// Sets the description.
    pub const fn description(self, description: &'static str) -> Self {
        UpDownCounterDef(self.0.description(description))
    }
}

impl From<&'static str> for UpDownCounterDef {
    fn from(name: &'static str) -> Self {
        UpDownCounterDef::new(name)
    }
}

/// One counter or up-down counter metric: all its series, exported through a single
/// observable instrument.
pub(crate) struct CounterMetric {
    def: CounterDef,
    /// An up-down counter: its cells hold `i64`s, as two's complement.
    up_down: bool,
    series: Arc<Mutex<SeriesSet>>,
}

impl CounterMetric {
    fn new(def: CounterDef, up_down: bool) -> Arc<Self> {
        Arc::new(CounterMetric {
            def,
            up_down,
            series: SeriesSet::new(1),
        })
    }

    fn get_or_register(meter: &Meter, def: CounterDef, up_down: bool) -> Arc<CounterMetric> {
        let mut registry = registry::lock();
        match registry.get(&(meter.scope, def.name)) {
            Some(Entry::Counter(metric)) if metric.up_down == up_down => {
                if metric.def != def {
                    registry::warn_conflict(meter.scope, def.name, &metric.def, &def);
                }
                Arc::clone(metric)
            }
            Some(other) => {
                registry::warn_conflict(meter.scope, def.name, &other.kind(), &kind(up_down));
                // Not registered, so never exported.
                CounterMetric::new(def, up_down)
            }
            None => {
                let metric = CounterMetric::new(def, up_down);
                let observed = Arc::clone(&metric);
                // The SDK keeps the callback for the provider's lifetime; the returned
                // instrument is only a marker.
                if up_down {
                    let mut builder = meter.otel.i64_observable_up_down_counter(def.name);
                    if let Some(unit) = def.unit {
                        builder = builder.with_unit(unit);
                    }
                    if let Some(description) = def.description {
                        builder = builder.with_description(description);
                    }
                    let _ = builder
                        .with_callback(move |o| observed.observe_i64(o))
                        .build();
                } else {
                    let mut builder = meter.otel.u64_observable_counter(def.name);
                    if let Some(unit) = def.unit {
                        builder = builder.with_unit(unit);
                    }
                    if let Some(description) = def.description {
                        builder = builder.with_description(description);
                    }
                    let _ = builder.with_callback(move |o| observed.observe(o)).build();
                }
                registry.insert((meter.scope, def.name), Entry::Counter(Arc::clone(&metric)));
                metric
            }
        }
    }

    pub(crate) fn kind(&self) -> &'static str {
        kind(self.up_down)
    }

    // Both run inside the SDK's collection, under its pipeline lock, so they must not
    // take the registry lock.

    fn observe(&self, observer: &dyn AsyncInstrument<u64>) {
        let set = self.series.lock().unwrap();
        set.for_each(|attributes, totals| observer.observe(totals[0], attributes));
    }

    fn observe_i64(&self, observer: &dyn AsyncInstrument<i64>) {
        let set = self.series.lock().unwrap();
        set.for_each(|attributes, totals| observer.observe(totals[0] as i64, attributes));
    }
}

fn kind(up_down: bool) -> &'static str {
    if up_down {
        "an up-down counter"
    } else {
        "a counter"
    }
}

/// A counter instance owned by one thread: `add` is a plain load and store.
///
/// `!Send` and `!Sync`: create one per thread or core. On drop its value is folded into
/// the series, so the exported total never goes back.
pub struct LocalCounter {
    instance: Instance,
    name: &'static str,
    _single_writer: PhantomData<*const ()>,
}

impl LocalCounter {
    // Cold: keeps setup out of the code around the record path.
    #[inline(never)]
    pub(crate) fn new(meter: &Meter, def: CounterDef, attributes: &[KeyValue]) -> Self {
        let metric = CounterMetric::get_or_register(meter, def, false);
        LocalCounter {
            instance: Instance::new(&metric.series, attributes),
            name: metric.def.name,
            _single_writer: PhantomData,
        }
    }

    /// Adds `value` to the counter.
    #[inline]
    pub fn add(&self, value: u64) {
        bump(self.instance.block().cell(), value);
    }
}

impl fmt::Debug for LocalCounter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalCounter")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

/// An up-down counter instance owned by one thread: `add` is a plain load and store,
/// like [`LocalCounter`]'s, with negative values for decrements.
///
/// `!Send` and `!Sync`: create one per thread or core. A decrement may come from
/// another instance than the increment: the exported value is the sum over all
/// instances, and dropped instances keep counting.
pub struct LocalUpDownCounter {
    instance: Instance,
    name: &'static str,
    _single_writer: PhantomData<*const ()>,
}

impl LocalUpDownCounter {
    // Cold: keeps setup out of the code around the record path.
    #[inline(never)]
    pub(crate) fn new(meter: &Meter, def: UpDownCounterDef, attributes: &[KeyValue]) -> Self {
        let metric = CounterMetric::get_or_register(meter, def.0, true);
        LocalUpDownCounter {
            instance: Instance::new(&metric.series, attributes),
            name: metric.def.name,
            _single_writer: PhantomData,
        }
    }

    /// Adds `value`, which may be negative.
    #[inline]
    pub fn add(&self, value: i64) {
        // Two's complement: a wrapping add of the bits is a signed add.
        bump(self.instance.block().cell(), value as u64);
    }
}

impl fmt::Debug for LocalUpDownCounter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalUpDownCounter")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}
