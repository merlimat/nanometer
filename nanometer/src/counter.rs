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
    name: &'static str,
    unit: Option<&'static str>,
    description: Option<&'static str>,
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

/// One counter metric: all its series, exported through a single observable counter.
pub(crate) struct CounterMetric {
    def: CounterDef,
    series: Arc<Mutex<SeriesSet>>,
}

impl CounterMetric {
    fn new(def: CounterDef) -> Arc<Self> {
        Arc::new(CounterMetric {
            def,
            series: SeriesSet::new(1),
        })
    }

    fn get_or_register(meter: &Meter, def: CounterDef) -> Arc<CounterMetric> {
        let mut registry = registry::lock();
        match registry.get(&(meter.scope, def.name)) {
            Some(Entry::Counter(metric)) => {
                if metric.def != def {
                    registry::warn_conflict(meter.scope, def.name, &metric.def, &def);
                }
                Arc::clone(metric)
            }
            Some(Entry::Histogram(metric)) => {
                registry::warn_conflict(meter.scope, def.name, &metric.def, &def);
                // Not registered, so never exported.
                CounterMetric::new(def)
            }
            None => {
                let metric = CounterMetric::new(def);
                let mut builder = meter.otel.u64_observable_counter(def.name);
                if let Some(unit) = def.unit {
                    builder = builder.with_unit(unit);
                }
                if let Some(description) = def.description {
                    builder = builder.with_description(description);
                }
                let observed = Arc::clone(&metric);
                // The SDK keeps the callback for the provider's lifetime; the returned
                // instrument is only a marker.
                let _ = builder.with_callback(move |o| observed.observe(o)).build();
                registry.insert((meter.scope, def.name), Entry::Counter(Arc::clone(&metric)));
                metric
            }
        }
    }

    /// Runs inside the SDK's collection, under its pipeline lock, so it must not take
    /// the registry lock.
    fn observe(&self, observer: &dyn AsyncInstrument<u64>) {
        let set = self.series.lock().unwrap();
        set.for_each(|attributes, totals| observer.observe(totals[0], attributes));
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
    pub(crate) fn new(meter: &Meter, def: CounterDef, attributes: &[KeyValue]) -> Self {
        let metric = CounterMetric::get_or_register(meter, def);
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
