// Two "local counters" each registering their own ObservableCounter with the same name+attrs.
use opentelemetry::metrics::MeterProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
use opentelemetry_sdk::metrics::reader::MetricReader;
use opentelemetry_sdk::metrics::{ManualReader, SdkMeterProvider};
use std::sync::Arc;

#[derive(Debug, Clone)]
struct R(Arc<ManualReader>);
impl MetricReader for R {
    fn register_pipeline(&self, p: std::sync::Weak<opentelemetry_sdk::metrics::Pipeline>) { self.0.register_pipeline(p) }
    fn collect(&self, rm: &mut ResourceMetrics) -> opentelemetry_sdk::error::OTelSdkResult { self.0.collect(rm) }
    fn force_flush(&self) -> opentelemetry_sdk::error::OTelSdkResult { self.0.force_flush() }
    fn shutdown_with_timeout(&self, t: std::time::Duration) -> opentelemetry_sdk::error::OTelSdkResult { self.0.shutdown_with_timeout(t) }
    fn temporality(&self, k: opentelemetry_sdk::metrics::InstrumentKind) -> opentelemetry_sdk::metrics::Temporality { self.0.temporality(k) }
}

fn main() {
    let reader = R(Arc::new(ManualReader::builder().build()));
    let provider = SdkMeterProvider::builder().with_reader(reader.clone()).build();
    let meter = provider.meter("m");
    let attrs = [KeyValue::new("op", "put")];
    // core 0 has counted 5, core 1 has counted 7
    let a = attrs.clone();
    let _c0 = meter.u64_observable_counter("requests").with_callback(move |o| o.observe(5, &a)).build();
    let a = attrs.clone();
    let _c1 = meter.u64_observable_counter("requests").with_callback(move |o| o.observe(7, &a)).build();

    let mut rm = ResourceMetrics::default();
    reader.collect(&mut rm).unwrap();
    for sm in rm.scope_metrics() {
        for m in sm.metrics() {
            if let AggregatedMetrics::U64(MetricData::Sum(s)) = m.data() {
                for p in s.data_points() {
                    println!("{} {:?} = {}   (correct total would be 12)", m.name(), p.attributes().collect::<Vec<_>>(), p.value());
                }
            }
        }
    }
}
