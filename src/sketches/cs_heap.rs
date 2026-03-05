//! CSHeap: a convenient wrapper that pairs a [`Count`] sketch with an
//! [`HHHeap`] for automatic top-k heavy-hitter tracking.
//!
//! Every insertion updates both the frequency sketch and the heap, mirroring
//! the pattern used by [`CMSHeap`] but with Count Sketch (median estimator).

use crate::{
    Count, DefaultXxHasher, FastPath, HHHeap, MatrixStorage, RegularPath, SketchHasher,
    SketchInput, Vector2D, heap_item_to_sketch_input,
};
use crate::sketches::count::{CountSketchCounter, FastPathSign};

const DEFAULT_TOP_K: usize = 32;

/// A Count Sketch paired with a top-k heavy-hitter heap.
///
/// Generic over the same type parameters as [`Count`].
pub struct CSHeap<
    S: MatrixStorage = Vector2D<i64>,
    Mode = RegularPath,
    H: SketchHasher = DefaultXxHasher,
> {
    cs: Count<S, Mode, H>,
    heap: HHHeap,
}

// -- Construction for Vector2D-backed storage --------------------------------

impl<T, M, H: SketchHasher> CSHeap<Vector2D<T>, M, H>
where
    T: CountSketchCounter,
{
    /// Creates a new `CSHeap` with the given CS dimensions and heap capacity.
    pub fn new(rows: usize, cols: usize, top_k: usize) -> Self {
        CSHeap {
            cs: Count::with_dimensions(rows, cols),
            heap: HHHeap::new(top_k),
        }
    }
}

// -- Construction from any MatrixStorage -------------------------------------

impl<S: MatrixStorage, M, H: SketchHasher> CSHeap<S, M, H>
where
    S::Counter: CountSketchCounter,
{
    /// Creates a `CSHeap` from a pre-built storage backend.
    pub fn from_storage(storage: S, top_k: usize) -> Self {
        CSHeap {
            cs: Count::from_storage(storage),
            heap: HHHeap::new(top_k),
        }
    }
}

// -- Default impls -----------------------------------------------------------

impl Default for CSHeap<Vector2D<i64>, RegularPath> {
    fn default() -> Self {
        Self::new(3, 4096, DEFAULT_TOP_K)
    }
}

impl Default for CSHeap<Vector2D<i64>, FastPath> {
    fn default() -> Self {
        Self::new(3, 4096, DEFAULT_TOP_K)
    }
}

impl Default for CSHeap<Vector2D<i32>, RegularPath> {
    fn default() -> Self {
        Self::new(3, 4096, DEFAULT_TOP_K)
    }
}

impl Default for CSHeap<Vector2D<i32>, FastPath> {
    fn default() -> Self {
        Self::new(3, 4096, DEFAULT_TOP_K)
    }
}

// -- Shared accessors (all storage types) ------------------------------------

impl<S: MatrixStorage, M, H: SketchHasher> CSHeap<S, M, H>
where
    S::Counter: CountSketchCounter,
{
    /// Returns a reference to the internal Count Sketch.
    pub fn cs(&self) -> &Count<S, M, H> {
        &self.cs
    }

    /// Returns a mutable reference to the internal Count Sketch.
    pub fn cs_mut(&mut self) -> &mut Count<S, M, H> {
        &mut self.cs
    }

    /// Returns a reference to the heavy-hitter heap.
    pub fn heap(&self) -> &HHHeap {
        &self.heap
    }

    /// Returns a mutable reference to the heavy-hitter heap.
    pub fn heap_mut(&mut self) -> &mut HHHeap {
        &mut self.heap
    }

    /// Number of rows in the underlying CS.
    #[inline(always)]
    pub fn rows(&self) -> usize {
        self.cs.rows()
    }

    /// Number of columns in the underlying CS.
    #[inline(always)]
    pub fn cols(&self) -> usize {
        self.cs.cols()
    }

    /// Clears the heap.
    pub fn clear_heap(&mut self) {
        self.heap.clear();
    }
}

// -- RegularPath insert / estimate / merge -----------------------------------

impl<S: MatrixStorage, H: SketchHasher> CSHeap<S, RegularPath, H>
where
    S::Counter: CountSketchCounter,
{
    /// Inserts a single observation and updates the top-k heap.
    #[inline]
    pub fn insert(&mut self, key: &SketchInput) {
        self.cs.insert(key);
        let est = self.cs.estimate(key);
        self.heap.update(key, est as i64);
    }

    /// Inserts an observation with the given count and updates the top-k heap.
    #[inline]
    pub fn insert_many(&mut self, key: &SketchInput, many: S::Counter) {
        self.cs.insert_many(key, many);
        let est = self.cs.estimate(key);
        self.heap.update(key, est as i64);
    }

    /// Inserts a batch of observations, updating the heap after each.
    pub fn bulk_insert(&mut self, values: &[SketchInput]) {
        for value in values {
            self.insert(value);
        }
    }

    /// Returns the CS frequency estimate (median) for the given key.
    #[inline]
    pub fn estimate(&self, key: &SketchInput) -> f64 {
        self.cs.estimate(key)
    }

    /// Merges another `CSHeap` into `self`.
    ///
    /// After merging the CS counters, all heap items from both sources are
    /// re-queried against the merged sketch to reconcile the top-k heap.
    pub fn merge(&mut self, other: &Self) {
        self.cs.merge(&other.cs);
        for item in other.heap.heap() {
            let key_ref = heap_item_to_sketch_input(&item.key);
            let est = self.cs.estimate(&key_ref);
            self.heap.update(&key_ref, est as i64);
        }
    }
}

// -- FastPath insert / estimate / merge --------------------------------------

impl<S, H: SketchHasher> CSHeap<S, FastPath, H>
where
    S: MatrixStorage + crate::FastPathHasher,
    S::Counter: CountSketchCounter,
    S::HashValueType: FastPathSign,
{
    /// Inserts a single observation using fast-path hashing and updates the heap.
    #[inline]
    pub fn insert(&mut self, key: &SketchInput) {
        self.cs.insert(key);
        let est = self.cs.estimate(key);
        self.heap.update(key, est as i64);
    }

    /// Inserts an observation with the given count using fast-path hashing.
    #[inline]
    pub fn insert_many(&mut self, key: &SketchInput, many: S::Counter) {
        self.cs.insert_many(key, many);
        let est = self.cs.estimate(key);
        self.heap.update(key, est as i64);
    }

    /// Inserts a batch of observations using fast-path hashing.
    pub fn bulk_insert(&mut self, values: &[SketchInput]) {
        for value in values {
            self.insert(value);
        }
    }

    /// Returns the CS frequency estimate (median) using fast-path hashing.
    #[inline]
    pub fn estimate(&self, key: &SketchInput) -> f64 {
        self.cs.estimate(key)
    }

    /// Merges another `CSHeap` into `self`.
    pub fn merge(&mut self, other: &Self) {
        self.cs.merge(&other.cs);
        for item in other.heap.heap() {
            let key_ref = heap_item_to_sketch_input(&item.key);
            let est = self.cs.estimate(&key_ref);
            self.heap.update(&key_ref, est as i64);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SketchInput;

    #[test]
    fn insert_and_estimate() {
        let mut sh = CSHeap::<Vector2D<i64>, RegularPath>::new(5, 256, 10);
        let key = SketchInput::Str("hello");
        for _ in 0..5 {
            sh.insert(&key);
        }
        assert!((sh.estimate(&key) - 5.0).abs() < 1e-9);
    }

    #[test]
    fn heap_tracks_top_k() {
        let mut sh = CSHeap::<Vector2D<i64>, RegularPath>::new(5, 1024, 3);

        // Insert 5 distinct keys with different frequencies.
        for i in 1..=5u64 {
            let key = SketchInput::U64(i);
            for _ in 0..(i * 100) {
                sh.insert(&key);
            }
        }

        // Heap should contain at most 3 items (top-3).
        assert!(sh.heap().len() <= 3);

        // The top-3 counts should be 300, 400, 500.
        let mut counts: Vec<i64> = sh.heap().heap().iter().map(|item| item.count).collect();
        counts.sort_unstable();
        assert_eq!(counts, vec![300, 400, 500]);
    }

    #[test]
    fn merge_reconciles_heaps() {
        let mut a = CSHeap::<Vector2D<i64>, RegularPath>::new(5, 256, 5);
        let mut b = CSHeap::<Vector2D<i64>, RegularPath>::new(5, 256, 5);

        let key = SketchInput::Str("merge_key");
        for _ in 0..10 {
            a.insert(&key);
        }
        for _ in 0..20 {
            b.insert(&key);
        }

        a.merge(&b);

        // After merge the estimate should be the sum.
        assert!((a.estimate(&key) - 30.0).abs() < 1e-9);

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
        let mut sh = CSHeap::<Vector2D<i64>, FastPath>::new(5, 256, 10);
        let key = SketchInput::Str("fast");
        for _ in 0..7 {
            sh.insert(&key);
        }
        assert!((sh.estimate(&key) - 7.0).abs() < 1e-9);
    }

    #[test]
    fn default_construction() {
        let sh = CSHeap::<Vector2D<i64>, RegularPath>::default();
        assert_eq!(sh.rows(), 3);
        assert_eq!(sh.cols(), 4096);
        assert_eq!(sh.heap().capacity(), DEFAULT_TOP_K);
    }
}
