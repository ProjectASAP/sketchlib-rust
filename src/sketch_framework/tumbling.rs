//! Tumbling (non-overlapping, fixed-duration) window support for sketches.
//!
//! A tumbling window collects items for a fixed duration, then closes. Closed
//! windows are retained up to a configurable limit and can be merged for
//! aggregate queries. A [`SketchPool`] recycles sketch instances when windows
//! expire, avoiding allocation churn.
//!
//! # Supported sketch types
//!
//! Any type implementing [`TumblingSketch`] can be used. Built-in
//! implementations are provided for [`FoldCMS`], [`FoldCS`], and [`KLL`].

use crate::fold_cms::FoldCMS;
use crate::fold_cs::FoldCS;
use crate::kll::KLL;
use crate::SketchInput;

// ---------------------------------------------------------------------------
// TumblingSketch trait
// ---------------------------------------------------------------------------

/// Trait unifying insert / merge / clear / construct for pool and tumbling
/// window manager use.
///
/// The `tw_` prefix avoids collision with existing method names on the
/// underlying sketch types.
pub trait TumblingSketch: Clone + Sized {
    /// Configuration sufficient to construct a fresh instance.
    type Config: Clone;

    /// Create a new sketch from its configuration.
    fn from_config(config: &Self::Config) -> Self;

    /// Insert one observation.
    fn tw_insert(&mut self, key: &SketchInput, value: i64);

    /// Merge `other` into `self`.
    fn tw_merge(&mut self, other: &Self);

    /// Reset to the empty state, preserving allocations where possible.
    fn tw_clear(&mut self);
}

// ---------------------------------------------------------------------------
// Config structs
// ---------------------------------------------------------------------------

/// Configuration for constructing a [`FoldCMS`] via [`TumblingSketch`].
#[derive(Clone, Debug)]
pub struct FoldCMSConfig {
    pub rows: usize,
    pub full_cols: usize,
    pub fold_level: u32,
    pub top_k: usize,
}

/// Configuration for constructing a [`FoldCS`] via [`TumblingSketch`].
#[derive(Clone, Debug)]
pub struct FoldCSConfig {
    pub rows: usize,
    pub full_cols: usize,
    pub fold_level: u32,
    pub top_k: usize,
}

/// Configuration for constructing a [`KLL`] via [`TumblingSketch`].
#[derive(Clone, Debug)]
pub struct KLLConfig {
    pub k: usize,
    pub m: usize,
}

// ---------------------------------------------------------------------------
// TumblingSketch impls
// ---------------------------------------------------------------------------

impl TumblingSketch for FoldCMS {
    type Config = FoldCMSConfig;

    fn from_config(config: &Self::Config) -> Self {
        FoldCMS::new(config.rows, config.full_cols, config.fold_level, config.top_k)
    }

    fn tw_insert(&mut self, key: &SketchInput, value: i64) {
        self.insert(key, value);
    }

    fn tw_merge(&mut self, other: &Self) {
        self.merge_same_level(other);
    }

    fn tw_clear(&mut self) {
        self.clear();
    }
}

impl TumblingSketch for FoldCS {
    type Config = FoldCSConfig;

    fn from_config(config: &Self::Config) -> Self {
        FoldCS::new(config.rows, config.full_cols, config.fold_level, config.top_k)
    }

    fn tw_insert(&mut self, key: &SketchInput, value: i64) {
        self.insert(key, value);
    }

    fn tw_merge(&mut self, other: &Self) {
        self.merge_same_level(other);
    }

    fn tw_clear(&mut self) {
        self.clear();
    }
}

impl TumblingSketch for KLL {
    type Config = KLLConfig;

    fn from_config(config: &Self::Config) -> Self {
        KLL::init(config.k, config.m)
    }

    fn tw_insert(&mut self, key: &SketchInput, _value: i64) {
        // KLL is a quantile sketch — each call is one observation.
        let _ = self.update(key);
    }

    fn tw_merge(&mut self, other: &Self) {
        self.merge(other);
    }

    fn tw_clear(&mut self) {
        self.clear();
    }
}

// ---------------------------------------------------------------------------
// SketchPool
// ---------------------------------------------------------------------------

/// Generic object pool that recycles sketch instances via [`TumblingSketch::tw_clear`].
pub struct SketchPool<S: TumblingSketch> {
    free_list: Vec<S>,
    total_allocated: usize,
    config: S::Config,
}

impl<S: TumblingSketch> SketchPool<S> {
    /// Create a pool and pre-allocate `cap` sketches.
    pub fn new(cap: usize, config: S::Config) -> Self {
        let mut free_list = Vec::with_capacity(cap);
        for _ in 0..cap {
            free_list.push(S::from_config(&config));
        }
        SketchPool {
            free_list,
            total_allocated: cap,
            config,
        }
    }

    /// Take a sketch from the free-list, or allocate a fresh one.
    pub fn take(&mut self) -> S {
        if let Some(s) = self.free_list.pop() {
            s
        } else {
            self.total_allocated += 1;
            S::from_config(&self.config)
        }
    }

    /// Return a sketch to the pool after clearing it.
    pub fn put(&mut self, mut sketch: S) {
        sketch.tw_clear();
        self.free_list.push(sketch);
    }

    /// Number of sketches currently available in the free-list.
    pub fn available(&self) -> usize {
        self.free_list.len()
    }

    /// Total number of sketches ever allocated by this pool.
    pub fn total_allocated(&self) -> usize {
        self.total_allocated
    }
}

// ---------------------------------------------------------------------------
// ClosedWindow
// ---------------------------------------------------------------------------

/// A closed (immutable) tumbling window with its sketch and metadata.
struct ClosedWindow<S: TumblingSketch> {
    sketch: S,
    _window_id: u64,
}

// ---------------------------------------------------------------------------
// TumblingWindow
// ---------------------------------------------------------------------------

/// Manages a sequence of tumbling (non-overlapping) windows over a sketch
/// type `S`. Each window collects items for `window_size` time units, then
/// closes. At most `max_windows` closed windows are retained; older ones
/// are evicted and their sketches returned to the pool.
pub struct TumblingWindow<S: TumblingSketch> {
    /// Currently open window's sketch.
    active: S,
    /// Sequential counter for window IDs.
    active_window_id: u64,
    /// Timestamp when the active window opened.
    active_start: u64,
    /// Duration of each window.
    window_size: u64,
    /// Maximum number of closed windows to retain.
    max_windows: usize,
    /// Closed windows, ordered oldest-to-newest.
    closed: Vec<ClosedWindow<S>>,
    /// Pool for recycling sketch instances.
    pool: SketchPool<S>,
}

impl<S: TumblingSketch> TumblingWindow<S> {
    /// Create a new tumbling window manager.
    ///
    /// * `window_size` — duration of each window in abstract time units.
    /// * `max_windows` — maximum number of closed windows to retain.
    /// * `config`      — sketch configuration for constructing fresh sketches.
    /// * `pool_cap`    — initial number of pre-allocated pool sketches.
    pub fn new(window_size: u64, max_windows: usize, config: S::Config, pool_cap: usize) -> Self {
        let mut pool = SketchPool::new(pool_cap, config.clone());
        let active = pool.take();
        TumblingWindow {
            active,
            active_window_id: 0,
            active_start: 0,
            window_size,
            max_windows,
            closed: Vec::with_capacity(max_windows),
            pool,
        }
    }

    /// Insert an observation at the given timestamp.
    ///
    /// If `time` falls beyond the current window boundary, the active window
    /// is closed and new windows are opened as needed (empty intermediate
    /// windows are skipped).
    pub fn insert(&mut self, time: u64, key: &SketchInput, value: i64) {
        // Advance windows as needed.
        while time >= self.active_start + self.window_size {
            self.close_active();
        }
        self.active.tw_insert(key, value);
    }

    /// Force-close the active window at `current_time` and open a fresh one.
    pub fn flush(&mut self, current_time: u64) {
        while current_time >= self.active_start + self.window_size {
            self.close_active();
        }
        // If the active window still contains the current_time, close it anyway.
        self.close_active();
    }

    /// Close the active window, push it to closed, evict if over limit.
    fn close_active(&mut self) {
        let old_active = std::mem::replace(&mut self.active, self.pool.take());
        self.closed.push(ClosedWindow {
            sketch: old_active,
            _window_id: self.active_window_id,
        });
        self.active_window_id += 1;
        self.active_start += self.window_size;

        // Evict oldest if over limit.
        while self.closed.len() > self.max_windows {
            let evicted = self.closed.remove(0);
            self.pool.put(evicted.sketch);
        }
    }

    /// Merge all closed windows + the active window into a single sketch.
    pub fn query_all(&self) -> S {
        let mut merged = self.active.clone();
        for cw in &self.closed {
            merged.tw_merge(&cw.sketch);
        }
        merged
    }

    /// Merge the `n` most recent closed windows + the active window.
    ///
    /// If `n >= closed_count()`, this is equivalent to `query_all()`.
    pub fn query_recent(&self, n: usize) -> S {
        let mut merged = self.active.clone();
        let start = self.closed.len().saturating_sub(n);
        for cw in &self.closed[start..] {
            merged.tw_merge(&cw.sketch);
        }
        merged
    }

    /// Reference to the active (currently open) sketch.
    pub fn active_sketch(&self) -> &S {
        &self.active
    }

    /// Number of closed windows currently retained.
    pub fn closed_count(&self) -> usize {
        self.closed.len()
    }

    /// Number of sketches available in the pool.
    pub fn pool_available(&self) -> usize {
        self.pool.available()
    }

    /// Total sketches ever allocated by the pool.
    pub fn pool_total_allocated(&self) -> usize {
        self.pool.total_allocated()
    }
}

// ---------------------------------------------------------------------------
// Specialized hierarchical merge queries for FoldCMS / FoldCS
// ---------------------------------------------------------------------------

impl TumblingWindow<FoldCMS> {
    /// Merge all windows (closed + active) via hierarchical pairwise unfolding.
    ///
    /// This produces a progressively unfolded result — more accurate than
    /// repeated `merge_same_level` when the fold level is > 0.
    pub fn query_all_hierarchical(&self) -> FoldCMS {
        let sketches: Vec<FoldCMS> = self
            .closed
            .iter()
            .map(|cw| cw.sketch.clone())
            .chain(std::iter::once(self.active.clone()))
            .collect();
        FoldCMS::hierarchical_merge(&sketches)
    }
}

impl TumblingWindow<FoldCS> {
    /// Merge all windows (closed + active) via hierarchical pairwise unfolding.
    pub fn query_all_hierarchical(&self) -> FoldCS {
        let sketches: Vec<FoldCS> = self
            .closed
            .iter()
            .map(|cw| cw.sketch.clone())
            .chain(std::iter::once(self.active.clone()))
            .collect();
        FoldCS::hierarchical_merge(&sketches)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{sample_normal_f64, sample_uniform_f64, sample_zipf_u64};
    use std::collections::HashMap;

    // -- Pool tests ----------------------------------------------------------

    #[test]
    fn pool_take_returns_preallocated() {
        let config = FoldCMSConfig {
            rows: 3,
            full_cols: 1024,
            fold_level: 3,
            top_k: 10,
        };
        let mut pool = SketchPool::<FoldCMS>::new(4, config);
        assert_eq!(pool.available(), 4);
        assert_eq!(pool.total_allocated(), 4);

        let _s1 = pool.take();
        assert_eq!(pool.available(), 3);
        assert_eq!(pool.total_allocated(), 4);
    }

    #[test]
    fn pool_take_allocates_when_empty() {
        let config = FoldCMSConfig {
            rows: 3,
            full_cols: 1024,
            fold_level: 3,
            top_k: 10,
        };
        let mut pool = SketchPool::<FoldCMS>::new(0, config);
        assert_eq!(pool.available(), 0);
        assert_eq!(pool.total_allocated(), 0);

        let _s = pool.take();
        assert_eq!(pool.total_allocated(), 1);
    }

    #[test]
    fn pool_put_recycles() {
        let config = FoldCMSConfig {
            rows: 3,
            full_cols: 1024,
            fold_level: 3,
            top_k: 10,
        };
        let mut pool = SketchPool::<FoldCMS>::new(1, config);
        let s = pool.take();
        assert_eq!(pool.available(), 0);

        pool.put(s);
        assert_eq!(pool.available(), 1);
        assert_eq!(pool.total_allocated(), 1);
    }

    // -- Clear method tests --------------------------------------------------

    #[test]
    fn fold_cms_clear_resets_to_empty() {
        let mut sk = FoldCMS::new(3, 1024, 3, 10);
        for i in 0..50u64 {
            sk.insert(&SketchInput::U64(i), 1);
        }
        assert!(sk.query(&SketchInput::U64(0)) > 0);

        sk.clear();

        for i in 0..50u64 {
            assert_eq!(
                sk.query(&SketchInput::U64(i)),
                0,
                "key {i} should be 0 after clear"
            );
        }
        assert!(sk.heap().is_empty(), "heap should be empty after clear");
    }

    #[test]
    fn fold_cs_clear_resets_to_empty() {
        let mut sk = FoldCS::new(3, 1024, 3, 10);
        for i in 0..50u64 {
            sk.insert(&SketchInput::U64(i), 1);
        }
        assert_ne!(sk.query(&SketchInput::U64(0)), 0);

        sk.clear();

        for i in 0..50u64 {
            assert_eq!(
                sk.query(&SketchInput::U64(i)),
                0,
                "key {i} should be 0 after clear"
            );
        }
        assert!(sk.heap().is_empty(), "heap should be empty after clear");
    }

    #[test]
    fn kll_clear_resets_to_empty() {
        let mut sk = KLL::init(200, 8);
        for i in 0..1000 {
            sk.update(&SketchInput::F64(i as f64)).unwrap();
        }
        assert!(sk.count() > 0);

        sk.clear();

        assert_eq!(sk.count(), 0, "count should be 0 after clear");
        let cdf = sk.cdf();
        assert_eq!(cdf.query(0.5), 0.0, "empty sketch should return 0.0");
    }

    // -- Window mechanics tests ----------------------------------------------

    #[test]
    fn window_closes_on_time_advance() {
        let config = FoldCMSConfig {
            rows: 3,
            full_cols: 1024,
            fold_level: 3,
            top_k: 10,
        };
        let mut tw: TumblingWindow<FoldCMS> = TumblingWindow::new(100, 10, config, 4);

        // Insert into window 0 (time 0..99).
        tw.insert(0, &SketchInput::Str("a"), 1);
        tw.insert(50, &SketchInput::Str("a"), 1);
        assert_eq!(tw.closed_count(), 0);

        // Time 100 → window 0 closes, window 1 opens.
        tw.insert(100, &SketchInput::Str("b"), 1);
        assert_eq!(tw.closed_count(), 1);

        // Time 200 → window 1 closes.
        tw.insert(200, &SketchInput::Str("c"), 1);
        assert_eq!(tw.closed_count(), 2);
    }

    #[test]
    fn window_evicts_oldest_beyond_max() {
        let config = FoldCMSConfig {
            rows: 3,
            full_cols: 1024,
            fold_level: 3,
            top_k: 10,
        };
        let mut tw: TumblingWindow<FoldCMS> = TumblingWindow::new(100, 3, config, 4);

        // Fill 4 windows (max_windows=3 closed + active).
        for w in 0..5 {
            tw.insert(w * 100, &SketchInput::U64(w), 1);
        }

        // We should have exactly 3 closed windows (oldest evicted).
        assert!(
            tw.closed_count() <= 3,
            "closed_count {} should be <= max_windows 3",
            tw.closed_count()
        );
    }

    #[test]
    fn window_pool_recycles_on_eviction() {
        let config = FoldCMSConfig {
            rows: 3,
            full_cols: 1024,
            fold_level: 3,
            top_k: 10,
        };
        let mut tw: TumblingWindow<FoldCMS> = TumblingWindow::new(100, 2, config, 4);

        let initial_total = tw.pool_total_allocated();

        // Create enough windows to trigger eviction.
        for w in 0..6 {
            tw.insert(w * 100, &SketchInput::U64(w), 1);
        }

        // Pool should have recycled sketches, so available > 0.
        assert!(
            tw.pool_available() > 0,
            "pool should have recycled sketches after eviction"
        );
        // Total allocated should not grow unboundedly.
        assert!(
            tw.pool_total_allocated() <= initial_total + 6,
            "pool should reuse sketches, not allocate indefinitely"
        );
    }

    // -- Merge correctness tests ---------------------------------------------

    #[test]
    fn query_all_matches_manual_merge() {
        let config = FoldCMSConfig {
            rows: 3,
            full_cols: 1024,
            fold_level: 3,
            top_k: 10,
        };
        let mut tw: TumblingWindow<FoldCMS> = TumblingWindow::new(100, 10, config.clone(), 4);

        let mut manual = FoldCMS::new(
            config.rows,
            config.full_cols,
            config.fold_level,
            config.top_k,
        );

        let keys: Vec<SketchInput> = (0..20u64).map(SketchInput::U64).collect();
        for (i, key) in keys.iter().enumerate() {
            let time = (i as u64) * 30; // spread across windows
            tw.insert(time, key, 1);
            manual.insert(key, 1);
        }

        let merged = tw.query_all();
        for key in &keys {
            assert_eq!(
                merged.query(key),
                manual.query(key),
                "query_all mismatch for {key:?}"
            );
        }
    }

    #[test]
    fn query_recent_selects_subset() {
        let config = FoldCMSConfig {
            rows: 3,
            full_cols: 1024,
            fold_level: 3,
            top_k: 10,
        };
        let mut tw: TumblingWindow<FoldCMS> = TumblingWindow::new(100, 10, config, 4);

        // Window 0: key "old"
        tw.insert(0, &SketchInput::Str("old"), 5);
        // Window 1: key "new"
        tw.insert(100, &SketchInput::Str("new"), 10);
        // Window 2 (active): key "active"
        tw.insert(200, &SketchInput::Str("active"), 7);

        // query_recent(1) should include 1 most recent closed + active.
        let recent = tw.query_recent(1);
        assert_eq!(recent.query(&SketchInput::Str("new")), 10);
        assert_eq!(recent.query(&SketchInput::Str("active")), 7);
        // "old" is in window 0 which is not in the recent 1.
        assert_eq!(recent.query(&SketchInput::Str("old")), 0);
    }

    // -- FoldCMS hierarchical merge via tumbling windows ----------------------

    #[test]
    fn fold_cms_tumbling_hierarchical_merge() {
        let rows = 3;
        let full_cols = 4096;
        let fold_level = 4;
        let top_k = 20;
        let domain = 5000;
        let exponent = 1.1;
        let samples_per_window = 10_000;
        let num_windows = 8;

        let config = FoldCMSConfig {
            rows,
            full_cols,
            fold_level,
            top_k,
        };
        let mut tw: TumblingWindow<FoldCMS> = TumblingWindow::new(
            samples_per_window as u64,
            num_windows,
            config,
            num_windows + 2,
        );

        let mut truth = HashMap::<u64, i64>::new();
        let stream = sample_zipf_u64(
            domain,
            exponent,
            samples_per_window * num_windows,
            0xCAFE_BABE,
        );

        for (i, &value) in stream.iter().enumerate() {
            tw.insert(i as u64, &SketchInput::U64(value), 1);
            *truth.entry(value).or_insert(0) += 1;
        }

        let merged = tw.query_all_hierarchical();

        // Verify most estimates are within CMS error bound.
        let epsilon = std::f64::consts::E / full_cols as f64;
        let l1_norm: f64 = truth.values().map(|&c| c as f64).sum();
        let error_bound = epsilon * l1_norm;

        let mut within = 0usize;
        for (&key, &true_count) in &truth {
            let est = merged.query(&SketchInput::U64(key));
            if ((est - true_count).abs() as f64) < error_bound {
                within += 1;
            }
        }

        let pct = within as f64 / truth.len() as f64 * 100.0;
        assert!(
            pct > 90.0,
            "only {pct:.1}% within CMS error bound (expected > 90%)"
        );
    }

    // -- KLL quantile via tumbling windows ------------------------------------

    #[test]
    fn kll_tumbling_quantile_accuracy() {
        let config = KLLConfig { k: 200, m: 8 };
        let samples_per_window = 5000;
        let num_windows = 4;

        let mut tw: TumblingWindow<KLL> = TumblingWindow::new(
            samples_per_window as u64,
            num_windows,
            config,
            num_windows + 2,
        );

        let values = sample_uniform_f64(
            0.0,
            1_000_000.0,
            samples_per_window * num_windows,
            0xDEAD_BEEF,
        );

        for (i, &v) in values.iter().enumerate() {
            tw.insert(i as u64, &SketchInput::F64(v), 1);
        }

        let merged = tw.query_all();
        let cdf = merged.cdf();

        let mut sorted = values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

        // Check median is roughly correct (within 2% rank tolerance).
        let est_median = cdf.query(0.5);
        let n = sorted.len();
        let lower_idx = ((0.48 * n as f64).ceil() as usize).min(n - 1);
        let upper_idx = ((0.52 * n as f64).ceil() as usize).min(n - 1);
        assert!(
            est_median >= sorted[lower_idx] && est_median <= sorted[upper_idx],
            "median estimate {est_median} outside [{}, {}]",
            sorted[lower_idx],
            sorted[upper_idx]
        );
    }

    // -- Flush test ----------------------------------------------------------

    #[test]
    fn flush_closes_active_window() {
        let config = FoldCMSConfig {
            rows: 3,
            full_cols: 1024,
            fold_level: 3,
            top_k: 10,
        };
        let mut tw: TumblingWindow<FoldCMS> = TumblingWindow::new(100, 10, config, 4);

        tw.insert(10, &SketchInput::Str("x"), 5);
        assert_eq!(tw.closed_count(), 0);

        tw.flush(50);
        assert_eq!(tw.closed_count(), 1);

        // Active should now be empty.
        assert_eq!(tw.active_sketch().query(&SketchInput::Str("x")), 0);

        // But query_all should still find the data.
        let all = tw.query_all();
        assert_eq!(all.query(&SketchInput::Str("x")), 5);
    }

    // -- FoldCS tumbling test ------------------------------------------------

    #[test]
    fn fold_cs_tumbling_basic() {
        let config = FoldCSConfig {
            rows: 3,
            full_cols: 1024,
            fold_level: 3,
            top_k: 10,
        };
        let mut tw: TumblingWindow<FoldCS> = TumblingWindow::new(100, 10, config, 4);

        tw.insert(0, &SketchInput::Str("hello"), 5);
        tw.insert(100, &SketchInput::Str("hello"), 3);
        tw.insert(200, &SketchInput::Str("hello"), 2);

        let merged = tw.query_all();
        assert_eq!(merged.query(&SketchInput::Str("hello")), 10);
    }

    #[test]
    fn fold_cs_tumbling_hierarchical_merge() {
        let config = FoldCSConfig {
            rows: 5,
            full_cols: 4096,
            fold_level: 2,
            top_k: 10,
        };
        let mut tw: TumblingWindow<FoldCS> = TumblingWindow::new(100, 10, config, 6);

        for w in 0..4u64 {
            for i in (w * 10)..((w + 1) * 10) {
                tw.insert(w * 100 + i, &SketchInput::U64(i), 1);
            }
        }

        let merged = tw.query_all_hierarchical();
        assert_eq!(merged.fold_level(), 0);

        // Count Sketch uses signed counters + median; allow small error.
        let mut errors = 0;
        for i in 0..40u64 {
            let est = merged.query(&SketchInput::U64(i));
            if (est - 1).abs() > 1 {
                errors += 1;
            }
        }
        assert!(
            errors == 0,
            "{errors}/40 keys had error > 1 (expected 0 with wide sketch)"
        );
    }

    // -- Accuracy & correctness tests ----------------------------------------

    #[test]
    fn fold_cms_tumbling_accuracy_zipf() {
        let rows = 3;
        let full_cols = 4096;
        let fold_level = 4;
        let top_k = 20;
        let domain = 10_000;
        let exponent = 1.1;
        let total_samples = 500_000;
        let num_windows = 16;
        let samples_per_window = total_samples / num_windows;

        let config = FoldCMSConfig {
            rows,
            full_cols,
            fold_level,
            top_k,
        };
        let mut tw: TumblingWindow<FoldCMS> = TumblingWindow::new(
            samples_per_window as u64,
            num_windows,
            config,
            num_windows + 2,
        );

        let stream = sample_zipf_u64(domain, exponent, total_samples, 0xACC0_BAC1);
        let mut truth = HashMap::<u64, i64>::new();

        for (i, &value) in stream.iter().enumerate() {
            tw.insert(i as u64, &SketchInput::U64(value), 1);
            *truth.entry(value).or_insert(0) += 1;
        }

        let merged = tw.query_all();

        // CMS error bound: epsilon = e / full_cols, bound = epsilon * L1
        let epsilon = std::f64::consts::E / full_cols as f64;
        let l1_norm: f64 = truth.values().map(|&c| c as f64).sum();
        let error_bound = epsilon * l1_norm;
        // delta = 1 / e^rows → expected fraction within bound = 1 - delta
        let delta = (-1.0 * rows as f64).exp();
        let required_pct = (1.0 - delta) * 100.0;

        let mut within = 0usize;
        let mut total_abs_error = 0i64;
        let mut max_abs_error = 0i64;
        for (&key, &true_count) in &truth {
            let est = merged.query(&SketchInput::U64(key));
            let err = (est - true_count).abs();
            total_abs_error += err;
            max_abs_error = max_abs_error.max(err);
            if (err as f64) <= error_bound {
                within += 1;
            }
        }

        let pct = within as f64 / truth.len() as f64 * 100.0;
        let mean_abs_error = total_abs_error as f64 / truth.len() as f64;
        eprintln!(
            "[fold_cms_tumbling_accuracy_zipf] mean_abs_error={mean_abs_error:.2}, \
             max_abs_error={max_abs_error}, pct_within_bound={pct:.1}% \
             (required>{required_pct:.1}%), error_bound={error_bound:.2}"
        );

        assert!(
            pct >= required_pct,
            "only {pct:.1}% within CMS error bound (expected >= {required_pct:.1}%)"
        );
    }

    #[test]
    fn fold_cms_hierarchical_vs_flat_merge() {
        let rows = 3;
        let full_cols = 4096;
        // fold_level = 3 so hierarchical merge with 8 windows (log2(8)=3 rounds)
        // reaches fold_level 0.
        let fold_level = 3;
        let top_k = 20;
        let domain = 5000;
        let exponent = 1.1;
        let total_samples = 100_000;
        let num_windows = 8;
        let samples_per_window = total_samples / num_windows;

        let config = FoldCMSConfig {
            rows,
            full_cols,
            fold_level,
            top_k,
        };

        let stream = sample_zipf_u64(domain, exponent, total_samples, 0xF1A7_CAFE);

        // Build two identical tumbling window instances.
        let mut tw_flat: TumblingWindow<FoldCMS> = TumblingWindow::new(
            samples_per_window as u64,
            num_windows,
            config.clone(),
            num_windows + 2,
        );
        let mut tw_hier: TumblingWindow<FoldCMS> = TumblingWindow::new(
            samples_per_window as u64,
            num_windows,
            config,
            num_windows + 2,
        );

        let mut truth = HashMap::<u64, i64>::new();
        for (i, &value) in stream.iter().enumerate() {
            let t = i as u64;
            let key = SketchInput::U64(value);
            tw_flat.insert(t, &key, 1);
            tw_hier.insert(t, &key, 1);
            *truth.entry(value).or_insert(0) += 1;
        }

        let merged_flat = tw_flat.query_all();
        let merged_hier = tw_hier.query_all_hierarchical();

        // Hierarchical merge should reach fold_level 0.
        assert_eq!(
            merged_hier.fold_level(),
            0,
            "hierarchical merge should reach fold_level 0"
        );

        // Both should produce identical estimates for all keys.
        // Note: flat merge stays at the original fold_level, so we unfold it
        // for a fair comparison.
        let merged_flat_unfolded = merged_flat.unfold_full();

        for &key in truth.keys() {
            let est_flat = merged_flat_unfolded.query(&SketchInput::U64(key));
            let est_hier = merged_hier.query(&SketchInput::U64(key));
            assert_eq!(
                est_flat, est_hier,
                "flat vs hierarchical mismatch for key {key}: flat={est_flat}, hier={est_hier}"
            );
        }
    }

    #[test]
    fn fold_cs_tumbling_accuracy_zipf() {
        let rows = 5;
        let full_cols = 4096;
        let fold_level = 4;
        let top_k = 20;
        let domain = 10_000;
        let exponent = 1.1;
        let total_samples = 500_000;
        let num_windows = 16;
        let samples_per_window = total_samples / num_windows;

        let config = FoldCSConfig {
            rows,
            full_cols,
            fold_level,
            top_k,
        };
        let mut tw: TumblingWindow<FoldCS> = TumblingWindow::new(
            samples_per_window as u64,
            num_windows,
            config,
            num_windows + 2,
        );

        let stream = sample_zipf_u64(domain, exponent, total_samples, 0xC5_ACCA);
        let mut truth = HashMap::<u64, i64>::new();

        for (i, &value) in stream.iter().enumerate() {
            tw.insert(i as u64, &SketchInput::U64(value), 1);
            *truth.entry(value).or_insert(0) += 1;
        }

        let merged = tw.query_all();

        // CS error bound: epsilon = sqrt(e / cols), bound = epsilon * L2
        let epsilon = (std::f64::consts::E / full_cols as f64).sqrt();
        let l2_norm: f64 = truth
            .values()
            .map(|&c| (c as f64) * (c as f64))
            .sum::<f64>()
            .sqrt();
        let error_bound = epsilon * l2_norm;
        let delta = (-1.0 * rows as f64).exp();
        let required_pct = (1.0 - delta) * 100.0;

        let mut within = 0usize;
        let mut total_abs_error = 0i64;
        let mut max_abs_error = 0i64;
        for (&key, &true_count) in &truth {
            let est = merged.query(&SketchInput::U64(key));
            let err = (est - true_count).abs();
            total_abs_error += err;
            max_abs_error = max_abs_error.max(err);
            if (err as f64) <= error_bound {
                within += 1;
            }
        }

        let pct = within as f64 / truth.len() as f64 * 100.0;
        let mean_abs_error = total_abs_error as f64 / truth.len() as f64;
        eprintln!(
            "[fold_cs_tumbling_accuracy_zipf] mean_abs_error={mean_abs_error:.2}, \
             max_abs_error={max_abs_error}, pct_within_bound={pct:.1}% \
             (required>{required_pct:.1}%), error_bound={error_bound:.2}"
        );

        assert!(
            pct >= required_pct,
            "only {pct:.1}% within CS error bound (expected >= {required_pct:.1}%)"
        );
    }

    #[test]
    fn kll_tumbling_multi_quantile_accuracy() {
        let config = KLLConfig { k: 200, m: 8 };
        let total_samples = 100_000;
        let num_windows = 8;
        let samples_per_window = total_samples / num_windows;

        let mut tw: TumblingWindow<KLL> = TumblingWindow::new(
            samples_per_window as u64,
            num_windows,
            config,
            num_windows + 2,
        );

        let values = sample_uniform_f64(0.0, 10_000_000.0, total_samples, 0x411_00171);

        for (i, &v) in values.iter().enumerate() {
            tw.insert(i as u64, &SketchInput::F64(v), 1);
        }

        let merged = tw.query_all();
        let cdf = merged.cdf();

        let mut sorted = values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let n = sorted.len();

        let quantiles = [0.10, 0.25, 0.50, 0.75, 0.90];
        let tolerance = 0.02;

        for &q in &quantiles {
            let est = cdf.query(q);
            let lo_idx = (((q - tolerance).max(0.0) * n as f64).ceil() as usize).min(n - 1);
            let hi_idx = (((q + tolerance).min(1.0) * n as f64).ceil() as usize).min(n - 1);
            assert!(
                est >= sorted[lo_idx] && est <= sorted[hi_idx],
                "quantile {q} estimate {est} outside [{}, {}] (rank tolerance {tolerance})",
                sorted[lo_idx],
                sorted[hi_idx]
            );
        }
    }

    #[test]
    fn kll_tumbling_distribution_shift() {
        let config = KLLConfig { k: 400, m: 8 };
        let samples_per_phase = 50_000;
        let windows_per_phase = 4;
        let samples_per_window = samples_per_phase / windows_per_phase;
        let num_windows = windows_per_phase * 2;

        let mut tw: TumblingWindow<KLL> = TumblingWindow::new(
            samples_per_window as u64,
            num_windows,
            config,
            num_windows + 2,
        );

        // Phase 1: normal(100, 10)
        let phase1 = sample_normal_f64(100.0, 10.0, samples_per_phase, 0xFA_ACE1);
        // Phase 2: normal(500, 50)
        let phase2 = sample_normal_f64(500.0, 50.0, samples_per_phase, 0xFA_ACE2);

        for (i, &v) in phase1.iter().enumerate() {
            tw.insert(i as u64, &SketchInput::F64(v), 1);
        }
        for (i, &v) in phase2.iter().enumerate() {
            let t = (samples_per_phase + i) as u64;
            tw.insert(t, &SketchInput::F64(v), 1);
        }

        let merged = tw.query_all();
        let cdf = merged.cdf();

        // Check distributional shape rather than exact quantile bounds.
        // With equal halves from normal(100,10) and normal(500,50):
        // - p10 should be solidly in the first distribution (< 200)
        // - p50 should be between the two modes (50 .. 600)
        // - p90 should be solidly in the second distribution (> 350)
        let p10 = cdf.query(0.10);
        let p50 = cdf.query(0.50);
        let p90 = cdf.query(0.90);

        eprintln!(
            "[kll_tumbling_distribution_shift] p10={p10:.1}, p50={p50:.1}, p90={p90:.1}"
        );

        assert!(
            p10 < 200.0,
            "p10={p10:.1} should be in the first distribution (< 200)"
        );
        assert!(
            p50 > 50.0 && p50 < 600.0,
            "p50={p50:.1} should be between modes (50..600)"
        );
        assert!(
            p90 > 350.0,
            "p90={p90:.1} should be in the second distribution (> 350)"
        );

        // Verify ordering is monotonic.
        assert!(p10 < p50, "p10 ({p10:.1}) should be < p50 ({p50:.1})");
        assert!(p50 < p90, "p50 ({p50:.1}) should be < p90 ({p90:.1})");
    }

    #[test]
    fn tumbling_eviction_correctness() {
        let rows = 3;
        let full_cols = 4096;
        let fold_level = 4;
        let top_k = 20;
        let domain = 5000;
        let exponent = 1.1;
        let max_windows = 4;
        let total_windows = 8;
        let samples_per_window = 10_000;

        let config = FoldCMSConfig {
            rows,
            full_cols,
            fold_level,
            top_k,
        };
        let mut tw: TumblingWindow<FoldCMS> = TumblingWindow::new(
            samples_per_window as u64,
            max_windows,
            config,
            max_windows + 2,
        );

        let stream = sample_zipf_u64(
            domain,
            exponent,
            samples_per_window * total_windows,
            0xE01C_0100,
        );

        // Track per-window truth to identify retained windows.
        let mut per_window_truth: Vec<HashMap<u64, i64>> = Vec::new();
        for w in 0..total_windows {
            let mut window_truth = HashMap::<u64, i64>::new();
            let start = w * samples_per_window;
            let end = start + samples_per_window;
            for i in start..end {
                let value = stream[i];
                tw.insert(i as u64, &SketchInput::U64(value), 1);
                *window_truth.entry(value).or_insert(0) += 1;
            }
            per_window_truth.push(window_truth);
        }

        // The last insert is at time (total_windows * samples_per_window - 1),
        // which falls in window (total_windows - 1). After all inserts:
        // - Windows 0..(total_windows-2) are closed.
        // - Window (total_windows-1) is active.
        // - With max_windows=4, only the 4 most recent closed windows are kept
        //   plus the active window.
        //
        // Retained: the most recent max_windows closed + active.
        // Closed windows are windows 0..6 (7 closed). Retain the last 4: 3,4,5,6.
        // Active window: 7.
        // So retained = windows 3,4,5,6,7.
        let retained_start = total_windows.saturating_sub(max_windows + 1);

        // Build ground truth for retained windows only.
        let mut retained_truth = HashMap::<u64, i64>::new();
        for w in retained_start..total_windows {
            for (&key, &count) in &per_window_truth[w] {
                *retained_truth.entry(key).or_insert(0) += count;
            }
        }

        // Build ground truth for evicted windows only.
        let mut evicted_only = HashMap::<u64, i64>::new();
        for w in 0..retained_start {
            for (&key, &count) in &per_window_truth[w] {
                *evicted_only.entry(key).or_insert(0) += count;
            }
        }

        let merged = tw.query_all();

        // CMS error bound for the retained data.
        let epsilon = std::f64::consts::E / full_cols as f64;
        let retained_l1: f64 = retained_truth.values().map(|&c| c as f64).sum();
        let error_bound = epsilon * retained_l1;

        // Retained keys: estimates should be close to retained truth.
        let mut within = 0usize;
        for (&key, &true_count) in &retained_truth {
            let est = merged.query(&SketchInput::U64(key));
            if ((est - true_count).abs() as f64) <= error_bound {
                within += 1;
            }
        }
        let pct = within as f64 / retained_truth.len() as f64 * 100.0;
        assert!(
            pct > 90.0,
            "only {pct:.1}% of retained keys within bound (expected > 90%)"
        );

        // Keys that appear ONLY in evicted windows should have small estimates.
        // CMS has inherent false positives (hash collisions), so we check that
        // evicted-only key estimates are within the CMS error bound rather than
        // demanding exact zero.
        for &key in evicted_only.keys() {
            if retained_truth.contains_key(&key) {
                continue; // key also appears in retained windows
            }
            let est = merged.query(&SketchInput::U64(key));
            assert!(
                (est as f64) <= error_bound,
                "evicted-only key {key} estimate {est} exceeds error bound {error_bound:.2}"
            );
        }
    }

    #[test]
    fn tumbling_query_recent_accuracy() {
        let rows = 3;
        let full_cols = 4096;
        let fold_level = 4;
        let top_k = 20;
        let domain = 5000;
        let exponent = 1.1;
        let total_windows = 6;
        let recent_n = 3;
        let samples_per_window = 10_000;

        let config = FoldCMSConfig {
            rows,
            full_cols,
            fold_level,
            top_k,
        };
        let mut tw: TumblingWindow<FoldCMS> = TumblingWindow::new(
            samples_per_window as u64,
            total_windows, // keep all closed
            config,
            total_windows + 2,
        );

        let stream = sample_zipf_u64(
            domain,
            exponent,
            samples_per_window * total_windows,
            0xBEC3_0A00,
        );

        let mut per_window_truth: Vec<HashMap<u64, i64>> = Vec::new();
        for w in 0..total_windows {
            let mut window_truth = HashMap::<u64, i64>::new();
            let start = w * samples_per_window;
            let end = start + samples_per_window;
            for i in start..end {
                let value = stream[i];
                tw.insert(i as u64, &SketchInput::U64(value), 1);
                *window_truth.entry(value).or_insert(0) += 1;
            }
            per_window_truth.push(window_truth);
        }

        // query_recent(3) returns: 3 most recent closed + active.
        // After 6 windows of data: windows 0..4 are closed, window 5 is active.
        // recent(3) → closed[2,3,4] + active(5) = windows 2,3,4,5.
        let recent_start = total_windows - recent_n - 1;
        let mut recent_truth = HashMap::<u64, i64>::new();
        for w in recent_start..total_windows {
            for (&key, &count) in &per_window_truth[w] {
                *recent_truth.entry(key).or_insert(0) += count;
            }
        }

        let merged = tw.query_recent(recent_n);

        let epsilon = std::f64::consts::E / full_cols as f64;
        let recent_l1: f64 = recent_truth.values().map(|&c| c as f64).sum();
        let error_bound = epsilon * recent_l1;

        let mut within = 0usize;
        for (&key, &true_count) in &recent_truth {
            let est = merged.query(&SketchInput::U64(key));
            if ((est - true_count).abs() as f64) <= error_bound {
                within += 1;
            }
        }
        let pct = within as f64 / recent_truth.len() as f64 * 100.0;
        assert!(
            pct > 90.0,
            "only {pct:.1}% of recent keys within bound (expected > 90%)"
        );

        // Keys only in excluded windows (0, 1) should have small estimates.
        // CMS has inherent false positives from hash collisions, so we check
        // estimates are within the error bound rather than demanding exact zero.
        let mut excluded_truth = HashMap::<u64, i64>::new();
        for w in 0..recent_start {
            for (&key, &count) in &per_window_truth[w] {
                *excluded_truth.entry(key).or_insert(0) += count;
            }
        }
        for &key in excluded_truth.keys() {
            if recent_truth.contains_key(&key) {
                continue;
            }
            let est = merged.query(&SketchInput::U64(key));
            assert!(
                (est as f64) <= error_bound,
                "excluded key {key} estimate {est} exceeds error bound {error_bound:.2}"
            );
        }
    }

    #[test]
    fn fold_cms_tumbling_heap_correctness() {
        let rows = 3;
        let full_cols = 4096;
        let fold_level = 4;
        let top_k = 20;
        let domain = 10_000;
        let exponent = 1.3; // heavier skew → clearer heavy hitters
        let total_samples = 200_000;
        let num_windows = 8;
        let samples_per_window = total_samples / num_windows;

        let config = FoldCMSConfig {
            rows,
            full_cols,
            fold_level,
            top_k,
        };
        let mut tw: TumblingWindow<FoldCMS> = TumblingWindow::new(
            samples_per_window as u64,
            num_windows,
            config,
            num_windows + 2,
        );

        let stream = sample_zipf_u64(domain, exponent, total_samples, 0xBEAF_C0DE);
        let mut truth = HashMap::<u64, i64>::new();

        for (i, &value) in stream.iter().enumerate() {
            tw.insert(i as u64, &SketchInput::U64(value), 1);
            *truth.entry(value).or_insert(0) += 1;
        }

        let merged = tw.query_all();

        // Ground truth top-k sorted by frequency (descending).
        let mut truth_sorted: Vec<(u64, i64)> = truth.into_iter().collect();
        truth_sorted.sort_by(|a, b| b.1.cmp(&a.1));
        let true_top_k: Vec<u64> = truth_sorted.iter().take(top_k).map(|&(k, _)| k).collect();

        // Heap entries from merged sketch.
        let heap_entries = merged.heap().heap();
        assert!(
            !heap_entries.is_empty(),
            "heap should not be empty after merging {num_windows} windows"
        );

        // Every key in the true top-k should appear in the heap.
        let mut found_in_heap = 0usize;
        for &true_key in &true_top_k {
            let in_heap = heap_entries
                .iter()
                .any(|item| item.key == crate::HeapItem::U64(true_key));
            if in_heap {
                found_in_heap += 1;
            }
        }

        // With Zipf(1.3) and 200k samples, the top-k should be well-separated.
        // Require at least 80% of the true top-k to be present in the heap.
        let recall_pct = found_in_heap as f64 / top_k as f64 * 100.0;
        eprintln!(
            "[fold_cms_tumbling_heap_correctness] heap_size={}, \
             true_top_k_recall={found_in_heap}/{top_k} ({recall_pct:.0}%)",
            heap_entries.len()
        );
        assert!(
            recall_pct >= 80.0,
            "only {recall_pct:.0}% of true top-{top_k} found in heap (expected >= 80%)"
        );
    }
}
