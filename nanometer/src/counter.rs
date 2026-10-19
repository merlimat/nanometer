use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt;
use std::marker::PhantomData;
use std::ptr;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, LazyLock, Mutex};

use opentelemetry::KeyValue;
use opentelemetry::metrics::AsyncInstrument;

use crate::Meter;
use crate::arena::{self, Slot};

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
    series: Mutex<SeriesSet>,
}

#[derive(Default)]
struct SeriesSet {
    index: HashMap<Vec<KeyValue>, usize>,
    series: Vec<Series>,
}

#[derive(Default)]
struct Series {
    /// Final values of dropped instances.
    base: u64,
    /// Cells of live instances.
    instances: Vec<Slot>,
}

type Registry = HashMap<(&'static str, &'static str), Arc<CounterMetric>>;

/// Process-wide: one metric, and so one observable, per (scope, metric name). Held while
/// registering with the SDK, which takes the SDK's pipeline lock.
static COUNTERS: LazyLock<Mutex<Registry>> = LazyLock::new(Default::default);

impl CounterMetric {
    fn get_or_register(meter: &Meter, def: CounterDef) -> Arc<CounterMetric> {
        let mut counters = COUNTERS.lock().unwrap();
        match counters.entry((meter.scope, def.name)) {
            Entry::Occupied(entry) => {
                let metric = entry.get();
                if metric.def != def {
                    opentelemetry::otel_warn!(
                        name: "ConflictingMetricDefinition",
                        scope = meter.scope,
                        metric = def.name,
                        kept = format!("{:?}", metric.def),
                        ignored = format!("{def:?}"),
                    );
                }
                Arc::clone(metric)
            }
            Entry::Vacant(entry) => {
                let metric = Arc::new(CounterMetric {
                    def,
                    series: Mutex::default(),
                });
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
                Arc::clone(entry.insert(metric))
            }
        }
    }

    /// Runs inside the SDK's collection, under its pipeline lock, so it must not take
    /// `COUNTERS`.
    fn observe(&self, observer: &dyn AsyncInstrument<u64>) {
        let set = self.series.lock().unwrap();
        for (attributes, &i) in &set.index {
            let series = &set.series[i];
            let total = series.instances.iter().fold(series.base, |sum, slot| {
                sum.wrapping_add(slot.cell().load(Relaxed))
            });
            observer.observe(total, attributes);
        }
    }
}

/// Sorts by key and drops repeated keys, keeping the last value, so that attribute
/// lists naming the same set map to the same series.
fn normalize(attributes: &[KeyValue]) -> Vec<KeyValue> {
    let mut attributes: Vec<KeyValue> = attributes.iter().rev().cloned().collect();
    // Stable sort: among equal keys the originally last one comes first and survives.
    attributes.sort_by(|a, b| a.key.cmp(&b.key));
    attributes.dedup_by(|a, b| a.key == b.key);
    attributes
}

/// A counter instance owned by one thread: `add` is a plain load and store.
///
/// `!Send` and `!Sync`: create one per thread or core. On drop its value is folded into
/// the series, so the exported total never goes back.
pub struct LocalCounter {
    slot: Slot,
    metric: Arc<CounterMetric>,
    series: usize,
    _single_writer: PhantomData<*const ()>,
}

impl LocalCounter {
    pub(crate) fn new(meter: &Meter, def: CounterDef, attributes: &[KeyValue]) -> Self {
        let metric = CounterMetric::get_or_register(meter, def);
        let attributes = normalize(attributes);
        let slot = arena::alloc();
        let mut guard = metric.series.lock().unwrap();
        let set = &mut *guard;
        let series = *set.index.entry(attributes).or_insert_with(|| {
            set.series.push(Series::default());
            set.series.len() - 1
        });
        set.series[series].instances.push(slot.clone());
        drop(guard);
        LocalCounter {
            slot,
            metric,
            series,
            _single_writer: PhantomData,
        }
    }

    /// Adds `value` to the counter.
    #[inline]
    pub fn add(&self, value: u64) {
        let cell = self.slot.cell();
        // Single writer, so no read-modify-write: the exporter only loads.
        cell.store(cell.load(Relaxed).wrapping_add(value), Relaxed);
    }
}

impl Drop for LocalCounter {
    fn drop(&mut self) {
        let mut set = self.metric.series.lock().unwrap();
        let series = &mut set.series[self.series];
        let i = series
            .instances
            .iter()
            .position(|slot| ptr::eq(slot.cell(), self.slot.cell()))
            .expect("live instance is registered in its series");
        let slot = series.instances.swap_remove(i);
        series.base = series.base.wrapping_add(slot.cell().load(Relaxed));
        drop(set);
        arena::free(slot);
    }
}

impl fmt::Debug for LocalCounter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalCounter")
            .field("name", &self.metric.def.name)
            .finish_non_exhaustive()
    }
}
