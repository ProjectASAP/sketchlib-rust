//! CMSHeap: a convenient wrapper that pairs a [`CountMin`] sketch with an
//! [`HHHeap`] for automatic top-k heavy-hitter tracking.
//!
//! Every insertion updates both the frequency sketch and the heap, mirroring
//! the pattern used by [`FoldCMS`] but without folding complexity.

use crate::{
    CountMin, DefaultXxHasher, FastPath, HHHeap, MatrixStorage, RegularPath, SketchHasher,
    SketchInput, Vector2D, heap_item_to_sketch_input,
};

const DEFAULT_TOP_K: usize = 32;

/// A Count-Min Sketch paired with a top-k heavy-hitter heap.
///
/// Generic over the same type parameters as [`CountMin`].
pub struct CMSHeap<
    S: MatrixStorage = Vector2D<i64>,
    Mode = RegularPath,
    H: SketchHasher = DefaultXxHasher,
> {
    cms: CountMin<S, Mode, H>,
    heap: HHHeap,
}

// -- Construction for Vector2D-backed storage --------------------------------

impl<T, M, H: SketchHasher> CMSHeap<Vector2D<T>, M, H>
where
    T: Copy + Default + std::ops::AddAssign,
{
    /// Creates a new `CMSHeap` with the given CMS dimensions and heap capacity.
    pub fn new(rows: usize, cols: usize, top_k: usize) -> Self {
        CMSHeap {
            cms: CountMin::with_dimensions(rows, cols),
            heap: HHHeap::new(top_k),
        }
    }
}

// -- Construction from any MatrixStorage -------------------------------------

impl<S: MatrixStorage, M, H: SketchHasher> CMSHeap<S, M, H> {
    /// Creates a `CMSHeap` from a pre-built storage backend.
    pub fn from_storage(storage: S, top_k: usize) -> Self {
        CMSHeap {
            cms: CountMin::from_storage(storage),
            heap: HHHeap::new(top_k),
        }
    }
}

// -- Default impls -----------------------------------------------------------

impl Default for CMSHeap<Vector2D<i64>, RegularPath> {
    fn default() -> Self {
        Self::new(3, 4096, DEFAULT_TOP_K)
    }
}

impl Default for CMSHeap<Vector2D<i64>, FastPath> {
    fn default() -> Self {
        Self::new(3, 4096, DEFAULT_TOP_K)
    }
}

impl Default for CMSHeap<Vector2D<i32>, RegularPath> {
    fn default() -> Self {
        Self::new(3, 4096, DEFAULT_TOP_K)
    }
}

impl Default for CMSHeap<Vector2D<i32>, FastPath> {
    fn default() -> Self {
        Self::new(3, 4096, DEFAULT_TOP_K)
    }
}

// -- Shared accessors (all storage types) ------------------------------------

impl<S: MatrixStorage, M, H: SketchHasher> CMSHeap<S, M, H> {
    /// Returns a reference to the internal CMS.
    pub fn cms(&self) -> &CountMin<S, M, H> {
        &self.cms
    }

    /// Returns a mutable reference to the internal CMS.
    pub fn cms_mut(&mut self) -> &mut CountMin<S, M, H> {
        &mut self.cms
    }

    /// Returns a reference to the heavy-hitter heap.
    pub fn heap(&self) -> &HHHeap {
        &self.heap
    }

    /// Returns a mutable reference to the heavy-hitter heap.
    pub fn heap_mut(&mut self) -> &mut HHHeap {
        &mut self.heap
    }

    /// Number of rows in the underlying CMS.
    #[inline(always)]
    pub fn rows(&self) -> usize {
        self.cms.rows()
    }

    /// Number of columns in the underlying CMS.
    #[inline(always)]
    pub fn cols(&self) -> usize {
        self.cms.cols()
    }

    /// Clears both the CMS counters (by rebuilding) and the heap.
    pub fn clear_heap(&mut self) {
        self.heap.clear();
    }
}

// -- RegularPath insert / estimate / merge -----------------------------------

impl<S: MatrixStorage, H: SketchHasher> CMSHeap<S, RegularPath, H>
where
    S::Counter: Copy + Ord + From<i32> + Into<i64> + std::ops::AddAssign,
{
    /// Inserts a single observation and updates the top-k heap.
    #[inline]
    pub fn insert(&mut self, key: &SketchInput) {
        self.cms.insert(key);
        let est = self.cms.estimate(key);
        self.heap.update(key, est.into());
    }

    /// Inserts an observation with the given count and updates the top-k heap.
    #[inline]
    pub fn insert_many(&mut self, key: &SketchInput, many: S::Counter) {
        self.cms.insert_many(key, many);
        let est = self.cms.estimate(key);
        self.heap.update(key, est.into());
    }

    /// Inserts a batch of observations, updating the heap after each.
    pub fn bulk_insert(&mut self, values: &[SketchInput]) {
        for value in values {
            self.insert(value);
        }
    }

    /// Returns the CMS frequency estimate for the given key.
    #[inline]
    pub fn estimate(&self, key: &SketchInput) -> S::Counter {
        self.cms.estimate(key)
    }

    /// Merges another `CMSHeap` into `self`.
    ///
    /// After merging the CMS counters, all heap items from both sources are
    /// re-queried against the merged sketch to reconcile the top-k heap.
    pub fn merge(&mut self, other: &Self) {
        self.cms.merge(&other.cms);
        for item in other.heap.heap() {
            let key_ref = heap_item_to_sketch_input(&item.key);
            let est = self.cms.estimate(&key_ref);
            self.heap.update(&key_ref, est.into());
        }
    }
}

// -- FastPath insert / estimate / merge --------------------------------------

impl<S, H: SketchHasher> CMSHeap<S, FastPath, H>
where
    S: MatrixStorage + crate::FastPathHasher,
    S::Counter: Copy + Ord + From<i32> + Into<i64> + std::ops::AddAssign,
{
    /// Inserts a single observation using fast-path hashing and updates the heap.
    #[inline]
    pub fn insert(&mut self, key: &SketchInput) {
        self.cms.insert(key);
        let est = self.cms.estimate(key);
        self.heap.update(key, est.into());
    }

    /// Inserts an observation with the given count using fast-path hashing.
    #[inline]
    pub fn insert_many(&mut self, key: &SketchInput, many: S::Counter) {
        self.cms.insert_many(key, many);
        let est = self.cms.estimate(key);
        self.heap.update(key, est.into());
    }

    /// Inserts a batch of observations using fast-path hashing.
    pub fn bulk_insert(&mut self, values: &[SketchInput]) {
        for value in values {
            self.insert(value);
        }
    }

    /// Returns the CMS frequency estimate using fast-path hashing.
    #[inline]
    pub fn estimate(&self, key: &SketchInput) -> S::Counter {
        self.cms.estimate(key)
    }

    /// Merges another `CMSHeap` into `self`.
    pub fn merge(&mut self, other: &Self) {
        self.cms.merge(&other.cms);
        for item in other.heap.heap() {
            let key_ref = heap_item_to_sketch_input(&item.key);
            let est = self.cms.estimate(&key_ref);
            self.heap.update(&key_ref, est.into());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SketchInput;

    #[test]
    fn insert_and_estimate() {
        let mut sh = CMSHeap::<Vector2D<i64>, RegularPath>::new(3, 64, 10);
        let key = SketchInput::Str("hello");
        for _ in 0..5 {
            sh.insert(&key);
        }
        assert_eq!(sh.estimate(&key), 5);
    }

    #[test]
    fn heap_tracks_top_k() {
        let mut sh = CMSHeap::<Vector2D<i64>, RegularPath>::new(3, 256, 3);

        // Insert 5 distinct keys with different frequencies.
        for i in 1..=5u64 {
            let key = SketchInput::U64(i);
            for _ in 0..(i * 10) {
                sh.insert(&key);
            }
        }

        // Heap should contain at most 3 items (top-3).
        assert!(sh.heap().len() <= 3);

        // The top-3 should be keys 3, 4, 5 (counts 30, 40, 50).
        let mut counts: Vec<i64> = sh.heap().heap().iter().map(|item| item.count).collect();
        counts.sort_unstable();
        assert_eq!(counts, vec![30, 40, 50]);
    }

    #[test]
    fn merge_reconciles_heaps() {
        let mut a = CMSHeap::<Vector2D<i64>, RegularPath>::new(3, 256, 5);
        let mut b = CMSHeap::<Vector2D<i64>, RegularPath>::new(3, 256, 5);

        let key = SketchInput::Str("merge_key");
        for _ in 0..10 {
            a.insert(&key);
        }
        for _ in 0..20 {
            b.insert(&key);
        }

        a.merge(&b);

        // After merge the estimate should be the sum.
        assert_eq!(a.estimate(&key), 30);

        // The heap should reflect the merged estimate.
        let heap_item = a
            .heap()
            .heap()
            .iter()
            .find(|item| {
                let k = heap_item_to_sketch_input(&item.key);
                k == key
            })
            .expect("key should be in heap");
        assert_eq!(heap_item.count, 30);
    }

    #[test]
    fn fast_path_insert_and_estimate() {
        let mut sh = CMSHeap::<Vector2D<i64>, FastPath>::new(3, 64, 10);
        let key = SketchInput::Str("fast");
        for _ in 0..7 {
            sh.insert(&key);
        }
        assert_eq!(sh.estimate(&key), 7);
    }

    #[test]
    fn default_construction() {
        let sh = CMSHeap::<Vector2D<i64>, RegularPath>::default();
        assert_eq!(sh.rows(), 3);
        assert_eq!(sh.cols(), 4096);
        assert_eq!(sh.heap().capacity(), DEFAULT_TOP_K);
    }
}
