# FoldCMS: Folding Count-Min Sketch

**File:** [src/sketches/fold_cms.rs](../src/sketches/fold_cms.rs)

---

## Table of Contents

1. [Problem](#problem)
2. [Key Idea](#key-idea)
3. [Terminology](#terminology)
4. [Data Structure](#data-structure)
5. [Operations](#operations)
   - [Insert](#insert)
   - [Point Query](#point-query)
   - [Same-Level Merge](#same-level-merge)
   - [Unfold Merge](#unfold-merge)
   - [Hierarchical Merge](#hierarchical-merge)
   - [Conversion to Flat Counters](#conversion-to-flat-counters)
6. [Lazy Cell Expansion](#lazy-cell-expansion)
7. [Memory Analysis](#memory-analysis)
8. [Correctness](#correctness)
9. [Top-K Heap](#top-k-heap)
10. [API Reference](#api-reference)
11. [Examples](#examples)

---

## Problem

In time-windowed stream monitoring, a large query window is divided into
sub-windows. Each sub-window stores a Count-Min Sketch (CMS) to summarize
the items that arrived during that interval. At query time, sub-window
sketches are merged element-wise to answer frequency queries over the full
window.

The problem: every sub-window must allocate a full R x W CMS even when
the sub-window only sees a handful of distinct items. The CMS width W is
dictated by the accuracy requirement of the *full* query window, not
by the sub-window's cardinality.

```
10-minute window, W = 4096, R = 3

Standard CMS per sub-window:  3 x 4096 x 8 bytes = 98 KB
  x 10 sub-windows                                = 980 KB

But each 1-minute sub-window may only have ~200 distinct keys.
99.5% of cells in each sub-window are wasted zeros.
```

FoldCMS solves this by allocating only `W / 2^k` physical columns per
sub-window and tracking the column identity of each inserted item, so
that sub-windows can be unfolded back to the full width at merge time.

---

## Key Idea

Instead of giving every sub-window the full W columns, give it only
`W / 2^k` columns (where k is the **fold level**). Multiple full-CMS
columns are "folded" onto the same physical column.

Each physical cell remembers which full-CMS column each counter belongs to
by storing a `(full_col, count)` entry. When sub-windows are merged, entries
are "unfolded" — placed back into their correct position in a wider sketch.

```
Full CMS (level 0), W = 8:
  ┌───┬───┬───┬───┬───┬───┬───┬───┐
  │ 0 │ 1 │ 2 │ 3 │ 4 │ 5 │ 6 │ 7 │    8 cells, one counter each
  └───┴───┴───┴───┴───┴───┴───┴───┘

Folded (level 1), W/2 = 4 physical columns:
  ┌─────────┬─────────┬─────────┬─────────┐
  │  {0,4}  │  {1,5}  │  {2,6}  │  {3,7}  │  4 cells, each may hold
  └─────────┴─────────┴─────────┴─────────┘  up to 2 tagged entries

Folded (level 2), W/4 = 2 physical columns:
  ┌───────────────────┬───────────────────┐
  │   {0,2,4,6}       │   {1,3,5,7}       │  2 cells, each may hold
  └───────────────────┴───────────────────┘  up to 4 tagged entries
```

Hashing always uses the full width W (`hash(key) % W`), so accuracy
is identical to a standard W-column CMS regardless of fold level.

---

## Terminology

| Term | Definition |
|------|-----------|
| **W** (`full_cols`) | Target full-width CMS column count (power of 2). Determines accuracy. |
| **R** (`rows`) | Number of independent hash functions. Same across all levels. |
| **k** (`fold_level`) | Folding depth. `k = 0` is a standard CMS; `k > 0` folds columns by `2^k`. |
| **fold_cols** | Number of physical columns = `W / 2^k` = `W >> k`. |
| **full_col** | A key's column in the full W-column CMS: `hash_r(key) % W`. This is a permanent address that never changes. |
| **fold_col** | The physical column where the entry is stored: `full_col % fold_cols` = `full_col & (fold_cols - 1)`. |
| **FoldEntry** | A `(full_col, count)` pair stored in a cell. |
| **FoldCell** | A physical column cell. May contain zero, one, or multiple entries. |

---

## Data Structure

```
FoldCMS
├── rows: usize           R hash functions
├── fold_cols: usize      W / 2^k physical columns
├── full_cols: usize      W target columns (invariant)
├── fold_level: u32       k (folding depth)
├── cells: Vec<FoldCell>  flat R × fold_cols grid
└── heap: HHHeap          top-K heavy hitter heap

FoldEntry
├── full_col: u16         column in the full W-column CMS
└── count: i64            accumulated counter

FoldCell (enum)
├── Empty                 no entry (zero cost)
├── Single { full_col, count }    one entry (no heap alloc)
└── Collided(Vec<FoldEntry>)      2+ entries (real collision)
```

The `FoldCell` enum is the core of the memory optimization. See
[Lazy Cell Expansion](#lazy-cell-expansion) for details.

---

## Operations

### Insert

```
INSERT(key, delta):
    for each row r in 0..R:
        full_col  = hash_r(key) % W             // permanent address
        fold_col  = full_col & (fold_cols - 1)   // physical column

        cell = cells[r * fold_cols + fold_col]
        match cell:
            Empty:
                cell ← Single { full_col, count: delta }

            Single { existing_col, count }:
                if existing_col == full_col:
                    count += delta                // same entry, just add
                else:
                    cell ← Collided([             // REAL collision
                        (existing_col, count),
                        (full_col, delta)
                    ])

            Collided(entries):
                if entries has full_col:
                    entry.count += delta
                else:
                    entries.push((full_col, delta))

    // update top-K heap with new estimate
    est = QUERY(key)
    heap.update(key, est)
```

**Time:** O(R * E) per insert, where E is the average entries per cell
(typically 1 for sparse sub-windows).

### Point Query

```
QUERY(key) -> i64:
    min_count = +∞
    for each row r in 0..R:
        full_col  = hash_r(key) % W
        fold_col  = full_col & (fold_cols - 1)

        cell = cells[r * fold_cols + fold_col]
        row_count = cell.lookup(full_col)   // 0 if absent

        min_count = min(min_count, row_count)

    return min_count
```

**Time:** O(R * E). Returns exactly the same value as a standard CMS with
W columns.

### Same-Level Merge

Combines two FoldCMS at the same fold level **without** changing the
physical column count. Used to aggregate multiple sub-windows that share
the same time-span granularity.

```
MERGE_SAME_LEVEL(self, other):
    assert self.fold_level == other.fold_level

    for each cell index i in 0..R*fold_cols:
        self.cells[i].merge_from(other.cells[i])
        // merge_from calls cell.insert(full_col, count)
        // for each entry in other — triggers collision
        // upgrade only when distinct full_cols meet

    reconcile_heap(self, other)
```

Cell merge respects the lazy expansion rule: if both cells have the same
`full_col`, the entry stays `Single`. Only a genuinely new `full_col`
triggers `Collided`.

**Time:** O(R * fold_cols * E)

### Unfold Merge

Combines two **same-level** FoldCMS at level k into a new FoldCMS at
level k-1, doubling the physical column count. Entries are scattered
to their correct positions in the wider grid.

```
UNFOLD_MERGE(a, b) -> FoldCMS:      // both at level k, result at level k-1
    new_fold_cols = 2 * a.fold_cols  // = W / 2^(k-1)
    result = new FoldCMS(R, W, k-1)

    for each source in [a, b]:
        for each row r:
            for each fold_col c in 0..source.fold_cols:
                for each (full_col, count) in source.cells[r][c]:
                    new_fc = full_col & (new_fold_cols - 1)
                    result.cells[r][new_fc].insert(full_col, count)

    reconcile_heap(result, a, b)
    return result
```

The unfold geometry for an entry at old cell `c` with `full_col = f`:

```
Because f ≡ c (mod old_fold_cols), and new_fold_cols = 2 * old_fold_cols:

  new_fc = f & (new_fold_cols - 1)
         = f mod (2 * old_fold_cols)

This is either c  or  c + old_fold_cols, depending on bit (w-k) of f.
So each entry migrates to one of exactly two cells in the wider sketch.
```

**Time:** O(R * fold_cols * E) for each source sketch.

### Hierarchical Merge

Merges an arbitrary sequence of FoldCMS sketches via pairwise unfolding.

```
HIERARCHICAL_MERGE(sketches[0..n]) -> FoldCMS:
    current = sketches
    while |current| > 1:
        next = []
        for each adjacent pair (left, right):
            if left.level == right.level and level > 0:
                next.push(UNFOLD_MERGE(left, right))
            elif left.level == right.level and level == 0:
                next.push(SAME_LEVEL_MERGE(left, right))
            else:
                // mixed levels: unfold higher to match lower
                target = min(left.level, right.level)
                a = left.unfold_to(target)
                b = right.unfold_to(target)
                next.push(merge(a, b))  // unfold or same-level
        if |current| is odd:
            next.push(current.last)     // carry forward
        current = next
    return current[0]
```

Handles non-power-of-two counts and mixed fold levels gracefully.

### Conversion to Flat Counters

At any fold level, entries can be extracted to a standard R x W counter
array:

```
TO_FLAT_COUNTERS(self) -> i64[R][W]:
    out = zeros(R, W)
    for each row r:
        for each fold_col c:
            for each (full_col, count) in cells[r][c]:
                out[r][full_col] += count
    return out
```

This produces exactly the same array as a standard CMS that processed the
same stream.

---

## Lazy Cell Expansion

The critical memory optimization. Cells track entries only when needed:

```
                ┌─────────────────────────────────────────────────────┐
                │                    FoldCell                          │
                ├─────────┬───────────────────┬───────────────────────┤
                │  Empty  │      Single       │       Collided        │
                │         │ { full_col, count }│  Vec<FoldEntry>       │
                ├─────────┼───────────────────┼───────────────────────┤
 Heap alloc?    │   No    │        No         │        Yes            │
 Entries        │    0    │        1          │       2+              │
 Trigger        │ (init)  │  first insert     │ second DISTINCT       │
                │         │                   │ full_col arrives      │
                └─────────┴───────────────────┴───────────────────────┘
```

**Why this matters**: In a sparse sub-window (few distinct keys, high fold
level), most physical columns see zero or one distinct `full_col`. These
cells stay `Empty` or `Single` — no `Vec` is allocated. Only the rare cells
where two different full-CMS columns genuinely collide into the same
physical column pay for `Vec` allocation.

State transitions:

```
   Empty ──insert(f₁, δ)──▶ Single(f₁, δ)
                                 │
                    insert(f₁, δ')  │  insert(f₂, δ₂)  where f₂ ≠ f₁
                         │          │
                         ▼          ▼
                  Single(f₁, δ+δ')  Collided([(f₁, δ), (f₂, δ₂)])
                                         │
                            insert(f₁, δ')  │  insert(f₃, δ₃)
                                  │          │
                                  ▼          ▼
                        Collided(...)  Collided([..., (f₃, δ₃)])
```

A cell **never** transitions backward (Collided → Single → Empty). This is
safe because deletions are not supported; counters only accumulate.

---

## Memory Analysis

### Standard CMS

```
Memory = R × W × sizeof(counter)
       = R × W × 8 bytes  (i64)
```

Example: R=3, W=4096 → **98,304 bytes** per sub-window.

### FoldCMS

Memory has two components:

1. **Cell grid overhead**: `R × fold_cols × sizeof(FoldCell)`
   - `Empty` cells: just the enum discriminant (part of the grid, no extra alloc)
   - `Single` cells: enum discriminant + u16 + i64 (inline, no heap alloc)
   - `Collided` cells: enum discriminant + Vec header (24 bytes) + entries on heap

2. **Entry storage**: only for non-empty cells
   - Each `FoldEntry` = u16 + i64 = 10 bytes

Let D = number of distinct keys in the sub-window.

| Component | Formula |
|-----------|---------|
| Cell grid | R × (W / 2^k) × ~32 bytes |
| Active entries | ≤ R × D × 10 bytes |
| **Total** | **R × (W/2^k × 32 + D × 10)** |

### Comparison Table

Parameters: R = 3, W = 4096

| Fold Level k | fold_cols | D (distinct keys) | FoldCMS Memory | Standard CMS | Savings |
|----|-----|------|------|------|-----|
| 0 | 4096 | — | 393 KB | 96 KB | worse (no point folding to level 0 directly) |
| 2 | 1024 | 200 | 104 KB | 96 KB | ~1x |
| 4 | 256 | 200 | 30 KB | 96 KB | **3.2x** |
| 4 | 256 | 50 | 26 KB | 96 KB | **3.7x** |
| 6 | 64 | 200 | 12 KB | 96 KB | **8x** |
| 6 | 64 | 50 | 7.5 KB | 96 KB | **12.8x** |
| 8 | 16 | 200 | 7.5 KB | 96 KB | **12.8x** |
| 8 | 16 | 50 | 3 KB | 96 KB | **32x** |

**Rule of thumb**: FoldCMS saves memory when `D << W` and `k >= 3`.
The savings grow with higher fold levels and sparser sub-windows.

---

## Correctness

**Theorem**: FoldCMS produces exactly the same query results as a standard
CMS with W columns for any key, at any fold level.

**Proof**:

1. Both FoldCMS and standard CMS use the same hash functions:
   `full_col = hash_r(key) % W` for each row r.

2. In standard CMS, cell `[r][full_col]` accumulates `count` from all keys
   whose row-r hash maps to `full_col`.

3. In FoldCMS, the entry `(full_col, count)` stored in the physical cell
   at `fold_col = full_col % fold_cols` tracks exactly the same set of keys
   (those whose row-r hash maps to `full_col`) and accumulates the same
   count.

4. Entries with different `full_col` values in the same physical cell are
   stored separately and do not interfere.

5. Query looks up the specific `full_col` entry per row and returns the
   minimum — identical to looking up cell `[r][full_col]` in the full CMS.

**Corollary**: Folding introduces **zero additional approximation error**.
The CMS error bound is unchanged:

```
Pr[ estimate(key) - true_count(key) ≤ ε × ‖f‖₁ ] ≥ 1 - δ

where  ε = e / W    (depends on full_cols, not fold_cols)
       δ = e^(-R)
```

---

## Top-K Heap

Each FoldCMS maintains an `HHHeap` (bounded min-heap of `(key, count)`
items) for heavy hitter tracking.

**On insert**: After updating the cells, the current query estimate for the
key is computed and the heap is updated. If the key's count exceeds the
heap's minimum, it enters (or updates in) the heap.

**On merge**: After merging cells, all heap items from the other sketch are
re-queried against the merged sketch and the heap is reconciled. This
ensures the top-K reflects the combined counts.

```
Insert(key, delta):
    ... update cells ...
    est = Query(key)
    heap.update(key, est)

Merge(self, other):
    ... merge cells ...
    for item in other.heap:
        est = self.Query(item.key)
        self.heap.update(item.key, est)
```

---

## API Reference

### Constructor

```rust
// Create a folded sketch
// fold_level 0 = standard CMS, k = folded by 2^k
fn new(rows: usize, full_cols: usize, fold_level: u32, top_k: usize) -> FoldCMS

// Shorthand for fold_level = 0
fn new_full(rows: usize, full_cols: usize, top_k: usize) -> FoldCMS
```

### Insert & Query

```rust
fn insert(&mut self, key: &SketchInput, delta: i64)
fn insert_one(&mut self, key: &SketchInput)           // delta = 1
fn query(&self, key: &SketchInput) -> i64              // min across rows
```

### Merge

```rust
// Combine two same-level sketches (no unfolding)
fn merge_same_level(&mut self, other: &FoldCMS)

// Combine two level-k sketches into one level-(k-1) sketch
fn unfold_merge(a: &FoldCMS, b: &FoldCMS) -> FoldCMS

// Unfold to a specific level
fn unfold_to(&self, target_level: u32) -> FoldCMS

// Unfold all the way to level 0
fn unfold_full(&self) -> FoldCMS

// Pairwise merge of a slice of sketches
fn hierarchical_merge(sketches: &[FoldCMS]) -> FoldCMS
```

### Conversion & Inspection

```rust
fn to_flat_counters(&self) -> Vec<i64>     // R × W row-major counter array
fn rows(&self) -> usize
fn fold_cols(&self) -> usize
fn full_cols(&self) -> usize
fn fold_level(&self) -> u32
fn total_entries(&self) -> usize           // sum of entries across all cells
fn collided_cells(&self) -> usize          // cells with 2+ entries
fn heap(&self) -> &HHHeap
```

---

## Examples

### Rate Limiting (Per-User Request Counting)

```rust
use sketchlib_rust::{FoldCMS, SketchInput};

let rows = 3;
let full_cols = 4096;
let fold_level = 4;  // 256 physical columns per sub-window

// Epoch 1: 10:00–10:01
let mut epoch1 = FoldCMS::new(rows, full_cols, fold_level, 5);
epoch1.insert(&SketchInput::Str("user_001"), 350);
epoch1.insert(&SketchInput::Str("user_002"), 10);
epoch1.insert(&SketchInput::Str("user_003"), 600);

// Epoch 2: 10:01–10:02
let mut epoch2 = FoldCMS::new(rows, full_cols, fold_level, 5);
epoch2.insert(&SketchInput::Str("user_001"), 350);
epoch2.insert(&SketchInput::Str("user_002"), 5);
epoch2.insert(&SketchInput::Str("user_003"), 700);

// Merge (same level — no unfolding needed)
epoch1.merge_same_level(&epoch2);

assert_eq!(epoch1.query(&SketchInput::Str("user_001")), 700);
assert_eq!(epoch1.query(&SketchInput::Str("user_003")), 1300);
```

### DDoS Detection with Hierarchical Merge

```rust
use sketchlib_rust::{FoldCMS, SketchInput};

let rows = 3;
let full_cols = 4096;
let fold_level = 4;

let mut epochs: Vec<FoldCMS> = Vec::new();
for _ in 0..3 {
    epochs.push(FoldCMS::new(rows, full_cols, fold_level, 5));
}

epochs[0].insert(&SketchInput::Str("10.0.0.42"), 10_000);
epochs[1].insert(&SketchInput::Str("10.0.0.42"), 15_000);
epochs[2].insert(&SketchInput::Str("10.0.0.42"), 12_000);

let merged = FoldCMS::hierarchical_merge(&epochs);

let total = merged.query(&SketchInput::Str("10.0.0.42"));
assert_eq!(total, 37_000);
// total > 15_000 threshold → ALERT
```

### Choosing Fold Level

```
Given:
  W = 4096     (target accuracy)
  R = 3        (failure probability)
  D ≈ 200      (expected distinct keys per sub-window)

Fold level selection:
  k = 4 → fold_cols = 256 → ~30 KB per sub-window  (3.2x savings)
  k = 6 → fold_cols = 64  → ~12 KB per sub-window  (8x savings)
  k = 8 → fold_cols = 16  → ~7.5 KB per sub-window (12.8x savings)

Higher k = more memory savings but more entries per cell on collision.
Choose k such that fold_cols is at least a few times larger than D
to keep collisions rare.

Recommended: fold_cols ≈ 2×D to 4×D for a good balance.
  D = 200 → fold_cols ≈ 400–800 → k = 2 or 3
  D = 50  → fold_cols ≈ 100–200 → k = 4 or 5
```
