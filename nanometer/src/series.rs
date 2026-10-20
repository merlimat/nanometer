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

//! A metric's series and the instances that write them.

use std::collections::HashMap;
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};

use opentelemetry::KeyValue;

use crate::arena::{self, Block};

/// The series of one metric. Every instance has `width` cells: one for a counter, the
/// buckets plus the sum for a histogram.
pub(crate) struct SeriesSet {
    width: usize,
    index: HashMap<Vec<KeyValue>, usize>,
    series: Vec<Series>,
}

struct Series {
    /// Final values of dropped instances.
    base: Box<[u64]>,
    /// Cells of live instances.
    instances: Vec<Block>,
}

impl SeriesSet {
    pub(crate) fn new(width: usize) -> Arc<Mutex<SeriesSet>> {
        Arc::new(Mutex::new(SeriesSet {
            width,
            index: HashMap::new(),
            series: Vec::new(),
        }))
    }

    /// Calls `f` with each series' attributes and totals: its base plus its live
    /// instances, cell by cell.
    pub(crate) fn for_each(&self, mut f: impl FnMut(&[KeyValue], &[u64])) {
        let mut totals = vec![0; self.width];
        for (attributes, &i) in &self.index {
            let series = &self.series[i];
            totals.copy_from_slice(&series.base);
            for block in &series.instances {
                for (total, cell) in totals.iter_mut().zip(block.cells()) {
                    *total = total.wrapping_add(cell.load(Relaxed));
                }
            }
            f(attributes, &totals);
        }
    }
}

/// Sorts by key and drops repeated keys, keeping the last value, so that attribute
/// lists naming the same set map to the same series.
pub(crate) fn normalize(attributes: &[KeyValue]) -> Vec<KeyValue> {
    let mut attributes: Vec<KeyValue> = attributes.iter().rev().cloned().collect();
    // Stable sort: among equal keys the originally last one comes first and survives.
    attributes.sort_by(|a, b| a.key.cmp(&b.key));
    attributes.dedup_by(|a, b| a.key == b.key);
    attributes
}

/// One instance's cells, registered in its series. On drop their values are folded into
/// the series, so its totals never go back.
pub(crate) struct Instance {
    block: Block,
    set: Arc<Mutex<SeriesSet>>,
    series: usize,
}

impl Instance {
    pub(crate) fn new(set: &Arc<Mutex<SeriesSet>>, attributes: &[KeyValue]) -> Self {
        let attributes = normalize(attributes);
        let mut guard = set.lock().unwrap();
        let inner = &mut *guard;
        let block = arena::alloc(inner.width);
        let series = *inner.index.entry(attributes).or_insert_with(|| {
            inner.series.push(Series {
                base: vec![0; inner.width].into(),
                instances: Vec::new(),
            });
            inner.series.len() - 1
        });
        inner.series[series].instances.push(block.clone());
        drop(guard);
        Instance {
            block,
            set: Arc::clone(set),
            series,
        }
    }

    #[inline(always)]
    pub(crate) fn block(&self) -> &Block {
        &self.block
    }
}

impl Drop for Instance {
    fn drop(&mut self) {
        let mut set = self.set.lock().unwrap();
        let series = &mut set.series[self.series];
        let i = series
            .instances
            .iter()
            .position(|block| ptr::eq(block.cell(), self.block.cell()))
            .expect("live instance is registered in its series");
        let block = series.instances.swap_remove(i);
        for (base, cell) in series.base.iter_mut().zip(block.cells()) {
            *base = base.wrapping_add(cell.load(Relaxed));
        }
        drop(set);
        arena::free(block);
    }
}

/// Single writer, so no read-modify-write: exporters only load.
#[inline(always)]
pub(crate) fn bump(cell: &AtomicU64, value: u64) {
    cell.store(cell.load(Relaxed).wrapping_add(value), Relaxed);
}
