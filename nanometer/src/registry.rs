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

//! The process-wide registry: one metric, and so one export, per (scope, metric name).

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, LazyLock, Mutex, MutexGuard};

use crate::counter::CounterMetric;
use crate::histogram::HistogramMetric;

pub(crate) enum Entry {
    Counter(Arc<CounterMetric>),
    Histogram(Arc<HistogramMetric>),
}

pub(crate) type Registry = HashMap<(&'static str, &'static str), Entry>;

/// Held while registering a counter's observable with the SDK, which takes the SDK's
/// pipeline lock; so nothing that runs under that lock may take this one.
static REGISTRY: LazyLock<Mutex<Registry>> = LazyLock::new(Default::default);

pub(crate) fn lock() -> MutexGuard<'static, Registry> {
    REGISTRY.lock().unwrap()
}

/// Same name, different definition: the first definition wins.
pub(crate) fn warn_conflict(scope: &str, name: &str, kept: &dyn Debug, ignored: &dyn Debug) {
    opentelemetry::otel_warn!(
        name: "ConflictingMetricDefinition",
        scope = scope,
        metric = name,
        kept = format!("{kept:?}"),
        ignored = format!("{ignored:?}"),
    );
}
