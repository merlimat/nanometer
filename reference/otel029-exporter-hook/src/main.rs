//! Probe: on OTel Rust 0.29, can a wrapper library plug per-core histograms into the
//! user's normal pipeline by wrapping their PushMetricExporter?

use opentelemetry::metrics::MeterProvider as _;
use opentelemetry::{InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::metrics::v1::metric::Data;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::metrics::data::{
    Histogram, HistogramDataPoint, Metric, ResourceMetrics, ScopeMetrics,
};
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider, Temporality};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::SystemTime;

// ---------- per-slot histogram (single writer per slot) ----------
const BOUNDS: [u64; 4] = [10, 100, 1000, 10000];
const NB: usize = BOUNDS.len() + 1;

#[repr(align(128))]
struct Shard {
    sum: AtomicU64,
    buckets: [AtomicU64; NB],
}

struct FastHistogram {
    name: &'static str,
    attrs: Vec<KeyValue>,
    shards: Vec<Shard>,
    start: SystemTime,
}

impl FastHistogram {
    fn new(name: &'static str, attrs: Vec<KeyValue>, slots: usize) -> Arc<Self> {
        Arc::new(Self {
            name,
            attrs,
            shards: (0..slots)
                .map(|_| Shard { sum: AtomicU64::new(0), buckets: std::array::from_fn(|_| AtomicU64::new(0)) })
                .collect(),
            start: SystemTime::now(),
        })
    }
    #[inline]
    fn record(&self, slot: usize, v: u64) {
        let s = &self.shards[slot];
        let b = &s.buckets[BOUNDS.partition_point(|&x| x < v)];
        b.store(b.load(Relaxed) + 1, Relaxed);
        s.sum.store(s.sum.load(Relaxed) + v, Relaxed);
    }
    /// Cumulative data point; count derived from buckets so it is self-consistent.
    fn data_point(&self) -> HistogramDataPoint<u64> {
        let mut bucket_counts = vec![0u64; NB];
        let mut sum = 0;
        for s in &self.shards {
            sum += s.sum.load(Relaxed);
            for (o, b) in bucket_counts.iter_mut().zip(&s.buckets) {
                *o += b.load(Relaxed);
            }
        }
        HistogramDataPoint {
            attributes: self.attrs.clone(),
            count: bucket_counts.iter().sum(),
            bounds: BOUNDS.iter().map(|&b| b as f64).collect(),
            bucket_counts,
            min: None,
            max: None,
            sum,
            exemplars: vec![],
        }
    }
}

// ---------- the hook: wrap whatever exporter the app configured ----------
#[derive(Debug)]
struct Injecting<E> {
    inner: E,
    hists: Vec<Arc<FastHistogram>>,
}

impl std::fmt::Debug for FastHistogram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name)
    }
}

impl<E: PushMetricExporter> PushMetricExporter for Injecting<E> {
    async fn export(&self, rm: &mut ResourceMetrics) -> OTelSdkResult {
        assert_eq!(self.inner.temporality(), Temporality::Cumulative, "probe only does cumulative");
        let now = SystemTime::now();
        let metrics = self
            .hists
            .iter()
            .map(|h| Metric {
                name: h.name.into(),
                description: "".into(),
                unit: "us".into(),
                data: Box::new(Histogram {
                    data_points: vec![h.data_point()],
                    start_time: h.start,
                    time: now,
                    temporality: Temporality::Cumulative,
                }),
            })
            .collect();
        rm.scope_metrics.push(ScopeMetrics {
            scope: InstrumentationScope::builder("fastmetrics").build(),
            metrics,
        });
        self.inner.export(rm).await
    }
    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }
    fn shutdown(&self) -> OTelSdkResult {
        self.inner.shutdown()
    }
    fn temporality(&self) -> Temporality {
        self.inner.temporality()
    }
}

// ---------- stand-in for the app's OTLP exporter: same proto conversion ----------
#[derive(Debug, Default)]
struct OtlpEncodeExporter;

impl PushMetricExporter for OtlpEncodeExporter {
    async fn export(&self, rm: &mut ResourceMetrics) -> OTelSdkResult {
        // exactly what opentelemetry-otlp 0.29 does before sending (tonic and http)
        let req = ExportMetricsServiceRequest::from(&*rm);
        for sm in &req.resource_metrics[0].scope_metrics {
            for m in &sm.metrics {
                match &m.data {
                    Some(Data::Histogram(h)) => {
                        let p = &h.data_points[0];
                        println!(
                            "  OTLP [{}] histogram {:<10} count={} sum={} buckets={:?} temporality={}",
                            sm.scope.as_ref().unwrap().name, m.name, p.count, p.sum.unwrap(), p.bucket_counts, h.aggregation_temporality
                        );
                    }
                    Some(Data::Sum(s)) => {
                        for p in &s.data_points {
                            println!("  OTLP [{}] sum       {:<10} value={:?}", sm.scope.as_ref().unwrap().name, m.name, p.value);
                        }
                    }
                    other => println!("  OTLP {} {:?}", m.name, other.is_some()),
                }
            }
        }
        Ok(())
    }
    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }
    fn shutdown(&self) -> OTelSdkResult {
        Ok(())
    }
    fn temporality(&self) -> Temporality {
        Temporality::Cumulative
    }
}

fn main() {
    let slots = 2;
    let lat = FastHistogram::new("latency", vec![KeyValue::new("op", "put")], slots);

    // App side: its usual exporter, wrapped once.
    let exporter = Injecting { inner: OtlpEncodeExporter, hists: vec![lat.clone()] };
    let provider = SdkMeterProvider::builder()
        .with_reader(PeriodicReader::builder(exporter).with_interval(std::time::Duration::from_secs(3600)).build())
        .build();

    // Counters need no hook: per-slot cells behind an observable counter.
    let reqs: Arc<Vec<AtomicU64>> = Arc::new((0..slots).map(|_| AtomicU64::new(0)).collect());
    let r = reqs.clone();
    let _obs = provider
        .meter("fastmetrics")
        .u64_observable_counter("requests")
        .with_callback(move |o| o.observe(r.iter().map(|c| c.load(Relaxed)).sum(), &[KeyValue::new("op", "put")]))
        .build();

    for round in 1..=2 {
        std::thread::scope(|s| {
            for slot in 0..slots {
                let (lat, reqs) = (&lat, &reqs);
                s.spawn(move || {
                    for i in 0..1_000_000u64 {
                        lat.record(slot, i % 20_000);
                        let c = &reqs[slot];
                        c.store(c.load(Relaxed) + 1, Relaxed);
                    }
                });
            }
        });
        println!("collection {round}:");
        provider.force_flush().unwrap();
    }
    provider.shutdown().unwrap();
}
