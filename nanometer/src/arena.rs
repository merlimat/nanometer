//! Per-thread arenas for instance cells.
//!
//! Each instance gets one 8-byte cell from the arena of the thread that creates it. Only
//! chunks are cache-line aligned, so cells written by different threads never share a line
//! while one thread's cells stay packed (padding every cell to 128 B would cost 16x).

use std::cell::RefCell;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

const CELLS_PER_CHUNK: usize = 64;

// 128 B rather than 64: adjacent-line prefetchers pull 64 B lines in pairs.
#[repr(align(128))]
struct Chunk([AtomicU64; CELLS_PER_CHUNK]);

impl Chunk {
    fn new() -> Arc<Self> {
        Arc::new(Chunk([const { AtomicU64::new(0) }; CELLS_PER_CHUNK]))
    }
}

/// A shared reference to one cell that keeps its chunk alive.
#[derive(Clone)]
pub(crate) struct Slot {
    cell: NonNull<AtomicU64>,
    _chunk: Arc<Chunk>,
}

// SAFETY: a `Slot` is an `&AtomicU64` whose referent `_chunk` keeps alive, and
// `&AtomicU64` is `Send + Sync`.
unsafe impl Send for Slot {}
unsafe impl Sync for Slot {}

impl Slot {
    fn new(chunk: Arc<Chunk>, index: usize) -> Self {
        Slot {
            cell: NonNull::from(&chunk.0[index]),
            _chunk: chunk,
        }
    }

    #[inline(always)]
    pub(crate) fn cell(&self) -> &AtomicU64 {
        // SAFETY: `cell` points into `_chunk`, which this slot keeps alive.
        unsafe { self.cell.as_ref() }
    }
}

struct Arena {
    chunk: Option<Arc<Chunk>>,
    next: usize,
    free: Vec<Slot>,
}

thread_local! {
    static ARENA: RefCell<Arena> =
        const { RefCell::new(Arena { chunk: None, next: CELLS_PER_CHUNK, free: Vec::new() }) };
}

impl Arena {
    fn alloc(&mut self) -> Slot {
        if let Some(slot) = self.free.pop() {
            return slot;
        }
        if self.next == CELLS_PER_CHUNK {
            self.chunk = Some(Chunk::new());
            self.next = 0;
        }
        let chunk = Arc::clone(self.chunk.as_ref().unwrap());
        self.next += 1;
        Slot::new(chunk, self.next - 1)
    }
}

/// Returns a zeroed cell from the current thread's arena.
pub(crate) fn alloc() -> Slot {
    ARENA
        .try_with(|arena| arena.borrow_mut().alloc())
        // The arena is gone during thread teardown: use a chunk of our own.
        .unwrap_or_else(|_| Slot::new(Chunk::new(), 0))
}

/// Gives a cell back to the current thread's arena. Nothing else may read or write the
/// cell any more, and the caller must be the thread that allocated it.
pub(crate) fn free(slot: Slot) {
    slot.cell().store(0, Relaxed);
    // During thread teardown the arena may be gone; the slot then just drops.
    let _ = ARENA.try_with(|arena| arena.borrow_mut().free.push(slot));
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ptr;

    #[test]
    fn freed_cell_is_reused_zeroed() {
        let a = alloc();
        a.cell().store(9, Relaxed);
        let addr = a.cell() as *const AtomicU64;
        free(a);
        let b = alloc();
        assert!(ptr::eq(b.cell(), addr));
        assert_eq!(b.cell().load(Relaxed), 0);
    }

    #[test]
    fn cells_fill_a_chunk_before_the_next() {
        // A fresh thread starts with an empty arena.
        std::thread::spawn(|| {
            let slots: Vec<Slot> = (0..CELLS_PER_CHUNK + 1).map(|_| alloc()).collect();
            let addr = |i: usize| slots[i].cell() as *const AtomicU64 as usize;
            assert_eq!(addr(0) % 128, 0);
            assert!((0..CELLS_PER_CHUNK).all(|i| addr(i) == addr(0) + 8 * i));
            // The next cell starts a new chunk.
            assert_eq!(addr(CELLS_PER_CHUNK) % 128, 0);
        })
        .join()
        .unwrap();
    }
}
