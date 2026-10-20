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

//! Per-thread arenas for instance cells.
//!
//! Each instance gets a block of 8-byte cells, one for a counter and one per bucket plus
//! the sum for a histogram, from the arena of the thread that creates it. Only chunks
//! are cache-line aligned, so cells written by different threads never share a line
//! while one thread's cells stay packed (padding every cell to 128 B would cost 16x).

use std::cell::RefCell;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::{iter, slice};

const LINE_CELLS: usize = 16;
const CHUNK_LINES: usize = 4;

// 128 B rather than 64: adjacent-line prefetchers pull 64 B lines in pairs. `repr(C)`
// with no padding, so a chunk's lines are one contiguous run of cells.
#[repr(C, align(128))]
struct Line([AtomicU64; LINE_CELLS]);

/// A run of cells, `CHUNK_CELLS` long, or longer for a block that needs more.
struct Chunk(Box<[Line]>);

impl Chunk {
    fn new(cells: usize) -> Arc<Self> {
        let lines = cells.div_ceil(LINE_CELLS).max(CHUNK_LINES);
        let line = || Line([const { AtomicU64::new(0) }; LINE_CELLS]);
        Arc::new(Chunk(iter::repeat_with(line).take(lines).collect()))
    }

    fn cells(&self) -> usize {
        self.0.len() * LINE_CELLS
    }
}

/// A shared reference to a block of cells that keeps its chunk alive.
#[derive(Clone)]
pub(crate) struct Block {
    first: NonNull<AtomicU64>,
    len: usize,
    _chunk: Arc<Chunk>,
}

// SAFETY: a `Block` is an `&[AtomicU64]` whose referent `_chunk` keeps alive, and
// `&[AtomicU64]` is `Send + Sync`.
unsafe impl Send for Block {}
unsafe impl Sync for Block {}

impl Block {
    fn new(chunk: Arc<Chunk>, start: usize, len: usize) -> Self {
        debug_assert!(start + len <= chunk.cells());
        // A chunk's lines are contiguous cells, so the block is cells start..start + len
        // of the chunk.
        let cells: *const AtomicU64 = chunk.0.as_ptr().cast();
        Block {
            // SAFETY: start + len <= chunk.cells(), within the chunk's allocation.
            first: unsafe { NonNull::new_unchecked(cells.add(start).cast_mut()) },
            len,
            _chunk: chunk,
        }
    }

    /// The first cell: the only one of a counter's block.
    #[inline(always)]
    pub(crate) fn cell(&self) -> &AtomicU64 {
        // SAFETY: `first` points into `_chunk`, which this block keeps alive.
        unsafe { self.first.as_ref() }
    }

    #[inline(always)]
    pub(crate) fn cells(&self) -> &[AtomicU64] {
        // SAFETY: `len` cells from `first` lie in `_chunk`, which this block keeps alive.
        unsafe { slice::from_raw_parts(self.first.as_ptr(), self.len) }
    }
}

struct Arena {
    chunk: Option<Arc<Chunk>>,
    next: usize,
    /// Freed blocks by length.
    free: Vec<Vec<Block>>,
}

thread_local! {
    static ARENA: RefCell<Arena> =
        const { RefCell::new(Arena { chunk: None, next: 0, free: Vec::new() }) };
}

impl Arena {
    fn alloc(&mut self, len: usize) -> Block {
        if let Some(block) = self.free.get_mut(len).and_then(Vec::pop) {
            return block;
        }
        match &self.chunk {
            Some(chunk) if self.next + len <= chunk.cells() => {
                self.next += len;
                Block::new(Arc::clone(chunk), self.next - len, len)
            }
            _ => {
                // What is left of the current chunk stays unused.
                let chunk = Chunk::new(len);
                self.next = len;
                self.chunk = Some(Arc::clone(&chunk));
                Block::new(chunk, 0, len)
            }
        }
    }

    fn free(&mut self, block: Block) {
        if self.free.len() <= block.len {
            self.free.resize_with(block.len + 1, Vec::new);
        }
        self.free[block.len].push(block);
    }
}

/// Returns a block of `len` zeroed cells from the current thread's arena.
pub(crate) fn alloc(len: usize) -> Block {
    ARENA
        .try_with(|arena| arena.borrow_mut().alloc(len))
        // The arena is gone during thread teardown: use a chunk of our own.
        .unwrap_or_else(|_| Block::new(Chunk::new(len), 0, len))
}

/// Gives a block back to the current thread's arena. Nothing else may read or write its
/// cells any more, and the caller must be the thread that allocated it.
pub(crate) fn free(block: Block) {
    for cell in block.cells() {
        cell.store(0, Relaxed);
    }
    // During thread teardown the arena may be gone; the block then just drops.
    let _ = ARENA.try_with(|arena| arena.borrow_mut().free(block));
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ptr;

    const CHUNK_CELLS: usize = LINE_CELLS * CHUNK_LINES;

    fn addr(cell: &AtomicU64) -> usize {
        cell as *const AtomicU64 as usize
    }

    #[test]
    fn freed_block_is_reused_zeroed() {
        let a = alloc(3);
        a.cells()[2].store(9, Relaxed);
        let first = addr(a.cell());
        free(a);
        // A block of another length doesn't take it.
        let other = alloc(1);
        assert_ne!(addr(other.cell()), first);
        let b = alloc(3);
        assert_eq!(addr(b.cell()), first);
        assert!(b.cells().iter().all(|c| c.load(Relaxed) == 0));
    }

    #[test]
    fn blocks_pack_a_chunk_before_the_next() {
        // A fresh thread starts with an empty arena.
        std::thread::spawn(|| {
            let blocks: Vec<Block> = (0..CHUNK_CELLS / 4).map(|_| alloc(4)).collect();
            let first = addr(blocks[0].cell());
            assert_eq!(first % 128, 0);
            for (i, block) in blocks.iter().enumerate() {
                assert_eq!(addr(block.cell()), first + 8 * 4 * i);
                assert!(ptr::eq(
                    &block.cells()[3],
                    (first + 8 * (4 * i + 3)) as *const _
                ));
            }
            // The chunk is full: the next block starts a new one.
            assert_eq!(addr(alloc(1).cell()) % 128, 0);
            // A block longer than a chunk gets a chunk of its own.
            let long = alloc(CHUNK_CELLS + 1);
            assert_eq!(addr(long.cell()) % 128, 0);
            assert_eq!(long.cells().len(), CHUNK_CELLS + 1);
        })
        .join()
        .unwrap();
    }
}
