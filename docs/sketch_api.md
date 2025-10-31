# Sketch API

In this document is the comprehensive list of API of the sketches.

## Sketch Status Legend

Throughout this document, sketches are marked with status badges:

- **✅ RECOMMENDED** - Built on the common structure ([common_api.md](common_api.md)), optimized, and actively maintained. Use these for new projects.
- **⚠️ LEGACY** - Older implementations not using the common structure. Not recommended for new use, but still functional.

### Recommended Sketches (Common Structure)

The following sketches are built on the common API structure and are recommended for use:

- **CountMin (CMS)** - Frequency estimation
- **Count Sketch (CS)** - Frequency estimation with both standard and L2HH variants
- **HyperLogLog (HLL)** - Cardinality estimation (all three variants)
- **Hydra** - Framework for hierarchical heavy hitters
- **UnivMon** - Framework for universal monitoring

### Legacy Sketches

The following sketches are legacy implementations and not recommended for new projects:

- Coco, Elastic, KLL, UniformSampling, MicroScope, Locher

These legacy sketches are functional but do not use the optimized common structure with `Vector2D`, `Vector1D`, and shared hashing primitives.

---

## Table of Contents

1. [Frequency Sketches](#frequency-sketches)
2. [Cardinality Sketches](#cardinality-sketches)
3. [Quantile Sketches](#quantile-sketches)
4. [Sampling](#sampling)
5. [Windowed Sketches](#windowed-sketches)
6. [Heavy Hitters](#heavy-hitters)
7. [Frameworks](#frameworks)
8. [Usage Examples](#usage-examples)

---

## Frequency Sketches

### CountMin (CMS) ✅ RECOMMENDED

**File:** [src/sketches/countmin.rs](../src/sketches/countmin.rs)

There is one version of CountMin sketch that is built based on the Common API. Count-Min sketch provides approximate frequency counting with sub-linear space using multiple hash functions and returns the minimum counter value.

**Constructor Methods:**

```rust
fn default() -> Self                           // 3 rows × 4096 columns
fn with_dimensions(rows: usize, cols: usize) -> Self
```

**Insert Methods:**

```rust
fn insert(&mut self, value: &SketchInput)                    // Standard insert
fn fast_insert(&mut self, value: &SketchInput)               // Optimized insert
fn fast_insert_with_hash_value(&mut self, hashed_val: u128) // Pre-computed hash
```

**Query Methods:**

```rust
fn estimate(&self, value: &SketchInput) -> u64      // Frequency estimate
fn fast_estimate(&self, value: &SketchInput) -> u64 // Optimized query
```

**Merge Methods:**

```rust
fn merge(&mut self, other: &Self)  // Element-wise counter addition
```

**Serialization:**

```rust
fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError>
fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError>
```

**Other Methods:**

```rust
fn rows(&self) -> usize
fn cols(&self) -> usize
fn as_storage(&self) -> &Vector2D<u64>
fn debug(&self)
```

---

### Count Sketch (CS) ✅ RECOMMENDED

**File:** [src/sketches/count.rs](../src/sketches/count.rs)

There are two versions of the Count Sketch: standard Count Sketch, and Count Sketch optimized for L2-Heavy-Hitter (which will be helpful to UnivMon).

#### Count (Standard Count Sketch)

Count Sketch uses signed counters and random hash functions to provide unbiased frequency estimates. Returns the median of row estimates.

**Constructor Methods:**

```rust
fn default() -> Self                           // 3 rows × 4096 columns
fn with_dimensions(rows: usize, cols: usize) -> Self
```

**Insert Methods:**

```rust
fn insert(&mut self, value: &SketchInput)                    // Signed update (±1)
fn fast_insert(&mut self, value: &SketchInput)               // Optimized insert
fn fast_insert_with_hash_value(&mut self, hashed_val: u128) // Pre-computed hash
```

**Query Methods:**

```rust
fn estimate(&self, value: &SketchInput) -> f64      // Median of rows
fn fast_estimate(&self, value: &SketchInput) -> f64 // Optimized query
```

**Merge & Serialization:** Same pattern as CountMin

---

#### CountL2HH (Count Sketch with L2 Heavy Hitters)

Extended Count Sketch that maintains L2 norm for heavy hitter detection and supports weighted updates.

**Constructor Methods:**

```rust
fn default() -> Self
fn with_dimensions(rows: usize, cols: usize) -> Self
fn with_dimensions_and_seed(rows: usize, cols: usize, seed_idx: usize) -> Self
```

**Insert Methods:**

```rust
fn fast_insert_with_count(&mut self, val: &SketchInput, c: i64)                     // With L2 update
fn fast_insert_with_count_without_l2(&mut self, val: &SketchInput, c: i64)          // Without L2
fn fast_insert_with_count_and_hash(&mut self, hashed_val: u128, c: i64)             // Pre-computed hash
fn fast_insert_with_count_without_l2_and_hash(&mut self, hashed_val: u128, c: i64)
```

**Query Methods:**

```rust
fn fast_get_est(&self, val: &SketchInput) -> f64               // Frequency estimate
fn fast_get_est_with_hash(&self, hashed_val: u128) -> f64
fn fast_update_and_est(&mut self, val: &SketchInput, c: i64) -> f64  // Atomic update+query
fn fast_update_and_est_without_l2(&mut self, val: &SketchInput, c: i64) -> f64
fn get_l2(&self) -> f64        // L2 norm
fn get_l2_sqr(&self) -> f64    // Squared L2 norm
```

---

### Coco (Count-Min with Counters) ⚠️ LEGACY

**File:** [src/sketches/coco.rs](../src/sketches/coco.rs)

Coco sketch combines Count-Min with full key storage for exact partial key matching and aggregation.

**Constructor Methods:**

```rust
fn new() -> Self                                // 64 × 5 table
fn init_with_size(w: usize, d: usize) -> Self  // Custom dimensions
```

**Insert/Query Methods:**

```rust
fn insert(&mut self, key: &SketchInput, v: u64)  // Insert key-value
fn estimate(&mut self, partial_key: SketchInput) -> u64  // Partial key match
fn estimate_with_udf<F>(&mut self, partial_key: SketchInput, udf: F) -> u64  // Custom matcher
```

**Merge Methods:**

```rust
fn merge(&mut self, other: &Coco)  // Replay-based merging
```

---

### Elastic ⚠️ LEGACY

**File:** [src/sketches/elastic.rs](../src/sketches/elastic.rs)

Elastic sketch separates heavy hitters (elephant flows) from light flows using a two-part structure with voting-based eviction.

**Constructor Methods:**

```rust
fn new() -> Self                   // 8 heavy buckets
fn init_with_length(l: i32) -> Self  // Custom buckets
```

**Insert/Query Methods:**

```rust
fn insert(&mut self, id: String)      // Insert flow ID
fn query(&mut self, id: String) -> i32  // Frequency estimate
```

---

## Cardinality Sketches

### HyperLogLog ✅ RECOMMENDED

**File:** [src/sketches/hll.rs](../src/sketches/hll.rs)

There are three types of HyperLogLog with different characteristics:

#### HyperLogLog (Original)

Implements the algorithm from the original paper.

- **Accuracy:** Not good when correction is needed after raw estimation
- **Mergeable:** Yes
- **Speed:** Standard

```rust
fn new() -> Self                    // 16384 registers (14-bit precision)
fn insert(&mut self, obj: &SketchInput)
fn get_est(&self) -> usize          // Cardinality with bias correction
fn merge(&mut self, other: &HyperLogLog)
```

---

#### HllDf (DataFusion/Redis Algorithm)

Modified from DataFusion's HLL, same algorithm as Redis HLL. Modified to make it easier for serialization.

- **Accuracy:** Good and mergeable
- **Insertion speed:** Same as original HyperLogLog
- **Query speed:** Similar to original HyperLogLog

```rust
fn new() -> Self
fn insert(&mut self, obj: &SketchInput)
fn get_est(&self) -> usize          // Improved estimation using Ertl's algorithm
fn merge(&mut self, other: &HllDf)
```

---

#### HllDs (Apache DataSketches HIP Estimator)

Apache DataSketches-Java's HLL algorithm, also called HIP (Historical Inverse Probability) Estimator.

- **Accuracy:** Good, but **NOT mergeable**
- **Insertion speed:** Slightly slower than original (needs to update 3 more counters)
- **Query speed:** Super fast (one counter is the estimation, just return that counter)

```rust
fn new() -> Self
fn insert(&mut self, obj: &SketchInput)  // Updates streaming estimate
fn get_est(&self) -> usize               // Returns streaming estimate (O(1))
fn merge(&mut self, _: &HllDs)           // PANICS - not supported
```

**Note:** Choose HllDf for merge operations, HllDs for fastest query performance in non-merge scenarios.

---

## Quantile Sketches

### KLL ⚠️ LEGACY

**File:** [src/sketches/kll.rs](../src/sketches/kll.rs)

KLL (Karnin-Lang-Liberty) sketch provides approximate quantiles with mergeable compactors.

**Constructor Methods:**

```rust
fn init_kll(k: i32) -> Self  // k controls accuracy (higher = more accurate)
```

**Insert/Query Methods:**

```rust
fn update(&mut self, x: f64)                   // Insert value
fn quantile(&self, x: f64) -> f64              // Quantile for value x
fn rank(&self, x: f64) -> usize                // Rank of value x
fn count(&self) -> usize                       // Total items
fn cdf(&self) -> CDF                           // Full CDF
```

**CDF Methods:**

```rust
fn quantile(&self, x: f64) -> f64      // Fraction ≤ x
fn query(&self, p: f64) -> f64          // Value at quantile p
fn quantile_li(&self, x: f64) -> f64    // Linear interpolation
fn query_li(&self, p: f64) -> f64       // Linear interpolation
```

**Merge Methods:**

```rust
fn merge(&mut self, other: &KLL)
```

---

## Sampling

### UniformSampling ⚠️ LEGACY

**File:** [src/sketches/uniform.rs](../src/sketches/uniform.rs)

Maintains a uniform random sample from a stream using reservoir sampling.

**Constructor Methods:**

```rust
fn new(sample_rate: f64) -> Self                  // Default seed
fn with_seed(sample_rate: f64, seed: u64) -> Self // Custom seed
```

**Insert/Query Methods:**

```rust
fn update(&mut self, value: f64)                                    // Insert numeric
fn update_input(&mut self, value: &SketchInput) -> Result<(), &'static str>  // Insert any numeric
fn samples(&self) -> Vec<f64>                     // All samples
fn sample_at(&self, idx: usize) -> Option<f64>    // Sample at index
fn len(&self) -> usize
fn total_seen(&self) -> u64                       // Total items seen
```

**Merge Methods:**

```rust
fn merge(&mut self, other: &UniformSampling) -> Result<(), &'static str>
```

---

## Windowed Sketches

### MicroScope ⚠️ LEGACY (❌ Depracate soon)

**File:** [src/sketches/microscope.rs](../src/sketches/microscope.rs)

Sliding window sketch that tracks frequency over time windows using sub-windows.

**Constructor Methods:**

```rust
fn init_microscope(w: usize, t: usize) -> Self  // w: window size, t: sub-windows
```

**Methods:**

```rust
fn insert(&mut self, timestamp: u64)
fn query(&self, timestamp: u64) -> f64          // Frequency at timestamp
fn delete(&mut self, timestamp: u64)            // Remove from window
fn merge(&mut self, other: &MicroScope, ts: u64)  // Merge at timestamp
```

---

### ExponentialHistogram ⚠️ LEGACY

**File:** [src/sketch_framework/eh.rs](../src/sketch_framework/eh.rs)

**Note:** This framework can wrap any sketch via the Chapter interface. It is recommended to use it with recommended sketches (CountMin, Count, HyperLogLog).

Maintains exponentially-spaced buckets for sliding window queries over any sketch type.

**Constructor Methods:**

```rust
fn new(k: usize, window: u64, eh_type: Chapter) -> Self  // k buckets, time window, wrapped sketch
```

**Methods:**

```rust
fn update(&mut self, time: u64, val: &SketchInput)                 // Insert at timestamp
fn query_interval_merge(&self, t1: u64, t2: u64) -> Option<Chapter>  // Query range [t1,t2]
fn cover(&self, mint: u64, maxt: u64) -> bool      // Check if range covered
fn update_window(&mut self, window: u64)           // Change window size
fn volume_count(&self) -> usize                    // Number of volumes
fn get_memory_info(&self) -> (usize, Vec<usize>)   // Memory statistics
```

---

## Heavy Hitters

### Locher ⚠️ LEGACY

**File:** [src/sketches/locher.rs](../src/sketches/locher.rs)

Sketch for heavy hitter detection using heap-based tracking with median aggregation.

**Constructor Methods:**

```rust
fn new(r: usize, l: usize, k: usize) -> Self  // r rows, l columns, k heap size
```

**Methods:**

```rust
fn insert(&mut self, e: &String, _v: u64)  // Insert flow ID
fn estimate(&self, e: &str) -> f64         // Frequency estimate (median)
```

---

## Frameworks

### Hydra (Hierarchical Heavy Hitters) ✅ RECOMMENDED

**File:** [src/sketch_framework/hydra.rs](../src/sketch_framework/hydra.rs)

Hydra is a sketch framework for multi-dimensional hierarchical queries over subpopulations.

**Constructor Methods:**

```rust
fn default() -> Self                                               // 3 × 32 with CountMin
fn with_dimensions(r: usize, c: usize, sketch_type: HydraCounter) -> Self  // Custom
```

**Methods:**

```rust
fn update(&mut self, key: &str, value: &SketchInput)  // key: semicolon-separated (e.g., "US;CA;male")
fn query_key(&self, key: Vec<&str>, query: &HydraQuery) -> f64  // Query subpopulation
fn query_frequency(&self, key: Vec<&str>, value: &SketchInput) -> f64  // Convenience
fn query_quantile(&self, key: Vec<&str>, threshold: f64) -> f64  // Convenience
```

**HydraCounter Enum:**

```rust
enum HydraCounter {
    CM(CountMin),
    HLL(HllDf),
    KLL(KLL),
    // etc.
}
```

**HydraQuery Enum:**

```rust
enum HydraQuery {
    Frequency(SketchInput),
    Quantile(f64),
    Cardinality,
}
```

---

### UnivMon (Universal Monitoring) ✅ RECOMMENDED

**File:** [src/sketch_framework/univmon.rs](../src/sketch_framework/univmon.rs)

UnivMon is a sketch framework for computing multiple stream statistics (L1, L2, entropy, cardinality) from a single data structure.

**Constructor Methods:**

```rust
fn init_univmon(k: usize, r: usize, c: usize, l: usize, p_idx: i64) -> Self  // k: heap, r rows, c cols, l layers
fn new_univmon_pyramid(k: usize, r: usize, c: usize, l: usize, p_idx: i64) -> Self  // Pyramid variant
```

**Insert Methods:**

```rust
fn update(&mut self, key: &str, value: i64, bottom_layer_num: usize)
fn update_optimized(&mut self, key: &str, value: i64, bottom_layer_num: usize)
fn update_pyramid(&mut self, key: &str, value: i64, bottom_layer_num: usize)
```

**Query Methods:**

```rust
fn calc_l1(&self) -> f64          // L1 norm
fn calc_l2(&self) -> f64          // L2 norm
fn calc_entropy(&self) -> f64     // Shannon entropy
fn calc_card(&self) -> f64        // Cardinality
fn calc_g_sum<F>(&self, g: F, is_card: bool) -> f64  // Generic aggregation
```

**Merge Methods:**

```rust
fn merge_with(&mut self, other: &UnivMon)
```

---

### Chapter (Unified Interface) ⚠️ LEGACY (❌ Depracate soon)

**File:** [src/sketch_framework/chapter.rs](../src/sketch_framework/chapter.rs)

**Note:** Chapter provides a unified interface for all sketches. Prefer using it with recommended sketches (CountMin, Count, HyperLogLog) for optimal performance.

Chapter enum wrapper provides a unified interface across all sketch types for framework usage.

**Variants:**

```rust
enum Chapter<'a> {
    CM(CountMin),
    COCO(Coco<'a>),
    CU(CountL2HH),
    ELASTIC(Elastic),
    HLL(HllDf),
    KLL(KLL),
    UNIFORM(UniformSampling),
}
```

**Unified Methods:**

```rust
fn insert(&mut self, val: &SketchInput)                      // Unified insert
fn merge(&mut self, other: &Chapter) -> Result<(), &'static str>  // Unified merge
fn query(&self, key: &SketchInput) -> Result<f64, &'static str>  // Unified query
fn sketch_type(&self) -> &'static str                        // Get type name
```

---

## Usage Examples

### Basic Frequency Counting

```rust
use sketchlib_rust::sketches::countmin::CountMin;
use sketchlib_rust::common::SketchInput;

let mut cm = CountMin::default();
cm.insert(&SketchInput::U64(12345));
let freq = cm.estimate(&SketchInput::U64(12345));
```

### Cardinality Estimation

```rust
use sketchlib_rust::sketches::hll::HllDf;
use sketchlib_rust::common::SketchInput;

let mut hll = HllDf::new();
hll.insert(&SketchInput::String("item1".to_string()));
hll.insert(&SketchInput::String("item2".to_string()));
let cardinality = hll.get_est();
```

### Quantile Queries

```rust
use sketchlib_rust::sketches::kll::KLL;

let mut kll = KLL::init_kll(200);
kll.update(1.5);
kll.update(2.7);
kll.update(3.2);
let median = kll.cdf().query(0.5);
```

### Hierarchical Queries

```rust
use sketchlib_rust::sketch_framework::hydra::{Hydra, HydraQuery};
use sketchlib_rust::common::SketchInput;

let mut hydra = Hydra::default();
hydra.update("US;CA;male", &SketchInput::U64(1));
let freq = hydra.query_frequency(vec!["US", "CA"], &SketchInput::U64(1));
```

### Sliding Windows

```rust
use sketchlib_rust::sketch_framework::eh::ExponentialHistogram;
use sketchlib_rust::sketch_framework::chapter::Chapter;
use sketchlib_rust::sketches::countmin::CountMin;
use sketchlib_rust::common::SketchInput;

let cm = Chapter::CM(CountMin::default());
let mut eh = ExponentialHistogram::new(10, 1000, cm);
eh.update(100, &SketchInput::U64(42));
let result = eh.query_interval_merge(50, 150);
```

---

## Common Patterns

### Serialization

Most sketches support MessagePack serialization:

```rust
fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError>
fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError>
```

### Fast Paths

Many sketches provide optimized methods:

- `fast_insert` / `fast_estimate` - Combined hashing operations to reduce hash call
- Methods with `_with_hash_value` suffix - Accept pre-computed hashes for multi-sketch coordination

### SketchInput

All sketches accept the `SketchInput` enum for type-agnostic insertion:

```rust
pub enum SketchInput<'a> {
    I32(i32), I64(i64), U32(u32), U64(u64),
    F32(f32), F64(f64), Str(&'a str),
    String(String), Bytes(&'a [u8]),
}
```

---

## Summary

**Total Sketches:** 14+ distinct implementations

**✅ Recommended (Common Structure):**

- **Frequency:** CountMin ✅, Count ✅, CountL2HH ✅
- **Cardinality:** HyperLogLog (3 variants) ✅
- **Frameworks:** Hydra ✅, UnivMon ✅

**⚠️ Legacy Implementations:**

- **Frequency:** Coco ⚠️, Elastic ⚠️
- **Quantile:** KLL ⚠️
- **Sampling:** UniformSampling ⚠️
- **Windowed:** MicroScope ⚠️
- **Heavy Hitters:** Locher ⚠️

**Utility Frameworks:**

- **ExponentialHistogram** - Sliding window wrapper (use with recommended sketches)
- **Chapter** - Unified interface (prefer with recommended sketches)

**File Locations:**

- Core sketches: [src/sketches/](../src/sketches/)
- Frameworks: [src/sketch_framework/](../src/sketch_framework/)
- Common primitives: [src/common/](../src/common/) (see [common_api.md](common_api.md))

For implementation details and internal data structures, see [common_api.md](common_api.md).
