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

//! A `ManualReader` the tests keep a handle to.

use std::sync::{Arc, Weak};
use std::time::Duration;

use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::metrics::data::ResourceMetrics;
use opentelemetry_sdk::metrics::reader::MetricReader;
use opentelemetry_sdk::metrics::{
    InstrumentKind, ManualReader, Pipeline, SdkMeterProvider, Temporality,
};

/// `with_reader` takes the reader by value; this keeps a handle to collect with.
#[derive(Clone, Debug)]
pub struct Reader(Arc<ManualReader>);

impl MetricReader for Reader {
    fn register_pipeline(&self, pipeline: Weak<Pipeline>) {
        self.0.register_pipeline(pipeline)
    }
    fn collect(&self, rm: &mut ResourceMetrics) -> OTelSdkResult {
        self.0.collect(rm)
    }
    fn force_flush(&self) -> OTelSdkResult {
        self.0.force_flush()
    }
    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.0.shutdown_with_timeout(timeout)
    }
    fn temporality(&self, kind: InstrumentKind) -> Temporality {
        self.0.temporality(kind)
    }
}

pub fn setup(reader: ManualReader) -> (SdkMeterProvider, Reader) {
    let reader = Reader(Arc::new(reader));
    let provider = SdkMeterProvider::builder()
        .with_reader(reader.clone())
        .build();
    (provider, reader)
}

/// Runs one collection.
pub fn collect(reader: &Reader) -> ResourceMetrics {
    let mut rm = ResourceMetrics::default();
    reader.collect(&mut rm).unwrap();
    rm
}
