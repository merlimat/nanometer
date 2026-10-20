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

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::SystemTime;

use opentelemetry::InstrumentationScope;
use opentelemetry_sdk::error::OTelSdkError;
use opentelemetry_sdk::metrics::data::{
    AggregatedMetrics, Histogram, HistogramDataPoint, Metric, ScopeMetrics,
};
use opentelemetry_sdk::metrics::{MetricProducer, Temporality};

use crate::histogram::HistogramMetric;
use crate::registry::{self, Entry};

/// Exports nanometer's histograms through an OTel SDK reader.
///
/// OpenTelemetry has no asynchronous histogram, so histograms leave through a
/// `MetricProducer` registered with the app's reader, once:
///
/// ```
/// use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
/// use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
///
/// fn provider(exporter: impl PushMetricExporter) -> SdkMeterProvider {
///     let reader = PeriodicReader::builder(exporter)
///         .with_producer(nanometer::Producer::new())
///         .build();
///     SdkMeterProvider::builder().with_reader(reader).build()
/// }
/// ```
///
/// It exports every histogram in the process, whichever provider its `Meter` was made
/// with, as cumulative histograms.
#[derive(Debug, Default)]
pub struct Producer(());

impl Producer {
    /// A producer of all of nanometer's histograms.
    pub fn new() -> Self {
        Producer(())
    }
}

impl MetricProducer for Producer {
    fn produce(&self) -> Result<Vec<ScopeMetrics>, OTelSdkError> {
        let histograms: Vec<Arc<HistogramMetric>> = registry::lock()
            .values()
            .filter_map(|entry| match entry {
                Entry::Histogram(metric) => Some(Arc::clone(metric)),
                Entry::Counter(_) => None,
            })
            .collect();
        let now = SystemTime::now();
        let mut scopes: BTreeMap<&str, Vec<Metric>> = BTreeMap::new();
        for histogram in &histograms {
            if let Some(metric) = export(histogram, now) {
                scopes.entry(histogram.scope).or_default().push(metric);
            }
        }
        Ok(scopes
            .into_iter()
            .map(|(scope, metrics)| {
                ScopeMetrics::new(InstrumentationScope::builder(scope).build(), metrics)
            })
            .collect())
    }
}

/// The metric's series as one cumulative histogram, or `None` if it has none yet.
fn export(histogram: &HistogramMetric, now: SystemTime) -> Option<Metric> {
    let def = &histogram.def;
    let per_unit = def.per_unit as f64;
    let bounds: Vec<f64> = def.buckets.iter().map(|&b| b as f64 / per_unit).collect();
    let data = if def.per_unit == 1 {
        AggregatedMetrics::U64(points(histogram, &bounds, now, |sum| sum)?.into())
    } else {
        AggregatedMetrics::F64(points(histogram, &bounds, now, |sum| sum as f64 / per_unit)?.into())
    };
    Some(Metric::new(
        def.name,
        def.description.unwrap_or(""),
        def.unit.unwrap_or(""),
        data,
    ))
}

/// One data point per series, with the integer sum converted by `sum`.
fn points<T>(
    histogram: &HistogramMetric,
    bounds: &[f64],
    now: SystemTime,
    sum: impl Fn(u64) -> T,
) -> Option<Histogram<T>> {
    let mut points = Vec::new();
    histogram
        .series
        .lock()
        .unwrap()
        .for_each(|attributes, totals| {
            let (counts, total) = totals.split_at(totals.len() - 1);
            points.push(HistogramDataPoint::new(
                attributes.to_vec(),
                bounds.to_vec(),
                counts.to_vec(),
                sum(total[0]),
            ));
        });
    if points.is_empty() {
        return None;
    }
    Some(Histogram::new(
        points,
        histogram.start,
        now,
        Temporality::Cumulative,
    ))
}
