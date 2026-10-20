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

use std::collections::HashMap;
use std::collections::hash_map::Entry as Slot;
use std::fmt;
use std::sync::atomic::{AtomicI64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};

use opentelemetry::KeyValue;
use opentelemetry::metrics::AsyncInstrument;

use crate::Meter;
use crate::counter::CounterDef;
use crate::registry::{self, Entry};
use crate::series::normalize;

/// The value of a gauge that was never set, which exports nothing.
const UNSET: i64 = i64::MIN;

/// Name, unit and description of a gauge, set through [`Gauge`] or read through
/// [`ObservableGauge`].
///
/// `&str` converts into a bare definition:
///
/// ```
/// use nanometer::GaugeDef;
/// const BACKLOG: GaugeDef = GaugeDef::new("backlog").unit("By");
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GaugeDef(CounterDef);

impl GaugeDef {
    /// A gauge called `name`, with no unit or description.
    pub const fn new(name: &'static str) -> Self {
        GaugeDef(CounterDef::new(name))
    }

    /// Sets the unit, e.g. `"By"` or `"{message}"`.
    pub const fn unit(self, unit: &'static str) -> Self {
        GaugeDef(self.0.unit(unit))
    }

    /// Sets the description.
    pub const fn description(self, description: &'static str) -> Self {
        GaugeDef(self.0.description(description))
    }
}

impl From<&'static str> for GaugeDef {
    fn from(name: &'static str) -> Self {
        GaugeDef::new(name)
    }
}

type Callback = Arc<dyn Fn() -> i64 + Send + Sync>;

/// One gauge metric: all its series, exported through a single observable gauge.
pub(crate) struct GaugeMetric {
    scope: &'static str,
    def: CounterDef,
    state: Mutex<State>,
}

struct State {
    series: HashMap<Vec<KeyValue>, Source>,
    next_id: u64,
}

enum Source {
    /// Set through `Gauge` handles.
    Value {
        cell: Arc<AtomicI64>,
        handles: usize,
    },
    /// Read at export from an `ObservableGauge`'s callback.
    Callback { id: u64, callback: Callback },
}

impl GaugeMetric {
    fn new(scope: &'static str, def: CounterDef) -> Arc<Self> {
        Arc::new(GaugeMetric {
            scope,
            def,
            state: Mutex::new(State {
                series: HashMap::new(),
                next_id: 0,
            }),
        })
    }

    fn get_or_register(meter: &Meter, def: CounterDef) -> Arc<GaugeMetric> {
        let mut registry = registry::lock();
        match registry.get(&(meter.scope, def.name)) {
            Some(Entry::Gauge(metric)) => {
                if metric.def != def {
                    registry::warn_conflict(meter.scope, def.name, &metric.def, &def);
                }
                Arc::clone(metric)
            }
            Some(other) => {
                registry::warn_conflict(meter.scope, def.name, &other.kind(), &"a gauge");
                // Not registered, so never exported.
                GaugeMetric::new(meter.scope, def)
            }
            None => {
                let metric = GaugeMetric::new(meter.scope, def);
                let mut builder = meter.otel.i64_observable_gauge(def.name);
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
                registry.insert((meter.scope, def.name), Entry::Gauge(Arc::clone(&metric)));
                metric
            }
        }
    }

    /// Runs inside the SDK's collection, under its pipeline lock, so it must not take
    /// the registry lock. Callbacks run without this metric's lock held, so that they
    /// may drop gauges.
    fn observe(&self, observer: &dyn AsyncInstrument<i64>) {
        let mut callbacks = Vec::new();
        for (attributes, source) in &self.state.lock().unwrap().series {
            match source {
                Source::Value { cell, .. } => match cell.load(Relaxed) {
                    UNSET => {}
                    value => observer.observe(value, attributes),
                },
                Source::Callback { callback, .. } => {
                    callbacks.push((attributes.clone(), Arc::clone(callback)))
                }
            }
        }
        for (attributes, callback) in callbacks {
            observer.observe(callback(), &attributes);
        }
    }
}

/// A gauge whose value is set: `set` is a plain store into the one atomic its series
/// has, so the last write wins, from any thread.
///
/// Handles for the same series share that atomic. The series exports nothing until
/// its first `set`, and leaves the export when its last handle is dropped.
#[must_use = "dropping the last handle of a series removes it from the export"]
pub struct Gauge {
    cell: Arc<AtomicI64>,
    metric: Arc<GaugeMetric>,
    /// The series, or `None` if it belongs to an [`ObservableGauge`].
    series: Option<Vec<KeyValue>>,
}

impl Gauge {
    // Cold: keeps setup out of the code around the record path.
    #[inline(never)]
    pub(crate) fn new(meter: &Meter, def: GaugeDef, attributes: &[KeyValue]) -> Self {
        let metric = GaugeMetric::get_or_register(meter, def.0);
        let mut state = metric.state.lock().unwrap();
        let (cell, series) = match state.series.entry(normalize(attributes)) {
            Slot::Vacant(slot) => {
                let cell = Arc::new(AtomicI64::new(UNSET));
                let series = slot.key().clone();
                slot.insert(Source::Value {
                    cell: Arc::clone(&cell),
                    handles: 1,
                });
                (cell, Some(series))
            }
            Slot::Occupied(mut slot) => {
                let series = slot.key().clone();
                match slot.get_mut() {
                    Source::Value { cell, handles } => {
                        *handles += 1;
                        (Arc::clone(cell), Some(series))
                    }
                    Source::Callback { .. } => {
                        registry::warn_duplicate_series(metric.scope, metric.def.name, &series);
                        // Not in the series map, so never exported.
                        (Arc::new(AtomicI64::new(UNSET)), None)
                    }
                }
            }
        };
        drop(state);
        Gauge {
            cell,
            metric,
            series,
        }
    }

    /// Sets the gauge to `value`. `i64::MIN` reads as unset: the series exports nothing
    /// until the next set.
    #[inline]
    pub fn set(&self, value: i64) {
        self.cell.store(value, Relaxed);
    }
}

impl Drop for Gauge {
    fn drop(&mut self) {
        let Some(series) = &self.series else { return };
        let mut state = self.metric.state.lock().unwrap();
        if let Some(Source::Value { handles, .. }) = state.series.get_mut(series) {
            *handles -= 1;
            if *handles == 0 {
                state.series.remove(series);
            }
        }
    }
}

impl fmt::Debug for Gauge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Gauge")
            .field("name", &self.metric.def.name)
            .finish_non_exhaustive()
    }
}

/// A gauge read from a callback at each export, for as long as this handle lives.
///
/// The callback runs on the exporter's thread while the SDK collects, so it must be
/// quick, and must not create nanometer handles (dropping them is fine). Each series
/// has at most one callback: a second one for the same attributes is ignored, with a
/// warning.
#[must_use = "dropping the handle unregisters the callback"]
pub struct ObservableGauge {
    metric: Arc<GaugeMetric>,
    /// The series and the callback's id, or `None` if the series had a gauge already.
    registration: Option<(Vec<KeyValue>, u64)>,
}

impl ObservableGauge {
    #[inline(never)]
    pub(crate) fn new(
        meter: &Meter,
        def: GaugeDef,
        attributes: &[KeyValue],
        callback: impl Fn() -> i64 + Send + Sync + 'static,
    ) -> Self {
        let metric = GaugeMetric::get_or_register(meter, def.0);
        let mut guard = metric.state.lock().unwrap();
        let state = &mut *guard;
        let id = state.next_id;
        state.next_id += 1;
        let registration = match state.series.entry(normalize(attributes)) {
            Slot::Vacant(slot) => {
                let series = slot.key().clone();
                slot.insert(Source::Callback {
                    id,
                    callback: Arc::new(callback),
                });
                Some((series, id))
            }
            Slot::Occupied(slot) => {
                registry::warn_duplicate_series(metric.scope, metric.def.name, slot.key());
                None
            }
        };
        drop(guard);
        ObservableGauge {
            metric,
            registration,
        }
    }
}

impl Drop for ObservableGauge {
    fn drop(&mut self) {
        let Some((series, id)) = &self.registration else {
            return;
        };
        let mut state = self.metric.state.lock().unwrap();
        if matches!(state.series.get(series), Some(Source::Callback { id: i, .. }) if i == id) {
            state.series.remove(series);
        }
    }
}

impl fmt::Debug for ObservableGauge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObservableGauge")
            .field("name", &self.metric.def.name)
            .field("registered", &self.registration.is_some())
            .finish()
    }
}
