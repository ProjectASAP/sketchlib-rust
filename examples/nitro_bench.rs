use ahash::RandomState;
use clap::Parser;
use pcap::Capture;
use sketchlib_rust::Nitro;
use std::collections::HashMap;
use std::hash::{BuildHasher, Hash, Hasher};
use std::io::{self, Write}; // 引入 Write trait 用于 flush stdout
use std::path::PathBuf;
use std::time::Instant;

// To compile: $ RUSTFLAGS="-C target-cpu=native" cargo build --release --example nitro_bench
// To run: $ sudo ./target/release/examples/nitro_bench ~/data/*.pcap

// ==========================================
// 2. Main Logic & Structs (保持不变)
// ==========================================
const MAX_PKTS: usize = 100_000_000;
const HASH_SEEDS: [u64; 10] = [
    0x9b6b9076, 0x7c2b3e89, 0x1d4a0f32, 0x5a8f2c1e, 0x3f1d9e2b, 0x8a7c6b5d, 0x2e9f1a4c, 0x6b5d8a7c,
    0x4c3b2e1f, 0x0f1e2d3c,
];

#[derive(Clone, Copy, Debug, Default)]
struct HeapEntry {
    key: u32,
    count: u64,
}

struct NitroSketch {
    rows: usize,
    cols: usize,
    table: Vec<u64>,
    seeds: Vec<u64>,
    top_k: usize,
    heap: Vec<HeapEntry>,
    key_to_idx: HashMap<u32, usize, RandomState>,
    pub nitro: Nitro,
    hasher_builder: RandomState,
}

impl NitroSketch {
    pub fn new(rows: usize, cols: usize, top_k: usize, sample_rate: f64) -> Self {
        let mut seeds = Vec::new();
        for i in 0..rows {
            seeds.push(HASH_SEEDS[i % HASH_SEEDS.len()] + (i as u64 * 100));
        }
        let nitro = Nitro::init_nitro(sample_rate);
        Self {
            rows,
            cols,
            table: vec![0; rows * cols],
            seeds,
            top_k,
            heap: Vec::with_capacity(top_k),
            key_to_idx: HashMap::with_hasher(RandomState::new()),
            nitro,
            hasher_builder: RandomState::new(),
        }
    }

    #[inline(always)]
    pub fn add(&mut self, key: u32) {
        if !self.nitro.is_nitro_mode {
            self.sketch_update(key, 1);
            self.heap_update(key);
            return;
        }
        if self.nitro.to_skip >= self.rows {
            self.nitro.to_skip -= self.rows;
            return;
        }
        let mut cur_row = self.nitro.to_skip;
        loop {
            self.sketch_update_row(key, self.nitro.delta, cur_row);
            self.nitro.draw_geometric();
            if cur_row + self.nitro.to_skip + 1 >= self.rows {
                break;
            }
            cur_row += self.nitro.to_skip + 1;
        }
        self.nitro.to_skip = (cur_row + self.nitro.to_skip + 1) - self.rows;
        self.heap_update(key);
    }

    #[inline(always)]
    fn sketch_update(&mut self, key: u32, count: u64) {
        for r in 0..self.rows {
            self.sketch_update_row(key, count, r);
        }
    }

    #[inline(always)]
    fn sketch_update_row(&mut self, key: u32, count: u64, row_idx: usize) {
        let seed = self.seeds[row_idx];
        let mut hasher = self.hasher_builder.build_hasher();
        key.hash(&mut hasher);
        seed.hash(&mut hasher);
        let hash_val = hasher.finish();
        let col = (hash_val as usize) % self.cols;
        unsafe {
            *self.table.get_unchecked_mut(row_idx * self.cols + col) += count;
        }
    }

    #[inline(always)]
    fn query_sketch(&self, key: u32) -> u64 {
        let mut min_cnt = u64::MAX;
        for r in 0..self.rows {
            let seed = self.seeds[r];
            let mut hasher = self.hasher_builder.build_hasher();
            key.hash(&mut hasher);
            seed.hash(&mut hasher);
            let hash_val = hasher.finish();
            let col = (hash_val as usize) % self.cols;
            let val = unsafe { *self.table.get_unchecked(r * self.cols + col) };
            if val < min_cnt {
                min_cnt = val;
            }
        }
        min_cnt
    }

    #[inline(always)]
    fn heap_update(&mut self, key: u32) {
        if let Some(&idx) = self.key_to_idx.get(&key) {
            let est_count = self.query_sketch(key);
            self.heap[idx].count = est_count;
            self.sift_down(idx);
        } else {
            let est_count = self.query_sketch(key);
            if self.heap.len() < self.top_k {
                let idx = self.heap.len();
                self.heap.push(HeapEntry {
                    key,
                    count: est_count,
                });
                self.key_to_idx.insert(key, idx);
                self.sift_up(idx);
            } else if est_count > self.heap[0].count {
                let old_key = self.heap[0].key;
                self.key_to_idx.remove(&old_key);
                self.heap[0] = HeapEntry {
                    key,
                    count: est_count,
                };
                self.key_to_idx.insert(key, 0);
                self.sift_down(0);
            }
        }
    }

    fn sift_down(&mut self, mut node_idx: usize) {
        let len = self.heap.len();
        loop {
            let left = 2 * node_idx + 1;
            let right = 2 * node_idx + 2;
            let mut smallest = node_idx;
            if left < len && self.heap[left].count < self.heap[smallest].count {
                smallest = left;
            }
            if right < len && self.heap[right].count < self.heap[smallest].count {
                smallest = right;
            }
            if smallest != node_idx {
                self.swap(node_idx, smallest);
                node_idx = smallest;
            } else {
                break;
            }
        }
    }

    fn sift_up(&mut self, mut node_idx: usize) {
        while node_idx > 0 {
            let parent = (node_idx - 1) / 2;
            if self.heap[node_idx].count < self.heap[parent].count {
                self.swap(node_idx, parent);
                node_idx = parent;
            } else {
                break;
            }
        }
    }

    fn swap(&mut self, a: usize, b: usize) {
        self.heap.swap(a, b);
        let ka = self.heap[a].key;
        let kb = self.heap[b].key;
        *self.key_to_idx.get_mut(&ka).unwrap() = a;
        *self.key_to_idx.get_mut(&kb).unwrap() = b;
    }
}

// ==========================================
// 3. Arguments & Main (修改这里以匹配 CLI)
// ==========================================

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// List of pcap files (supports glob expansion from shell)
    #[arg(num_args = 1.., required = true)]
    files: Vec<PathBuf>,
}

fn run_test(name: &str, sample_rate: f64, keys: &[u32]) {
    println!("--- Test: {} (Sample Rate: {:.4}) ---", name, sample_rate);

    let rows = 5;
    let cols = 2048;
    let top_k = 512;

    let mut sketch = NitroSketch::new(rows, cols, top_k, sample_rate);

    let start = Instant::now();
    for &k in keys {
        sketch.add(k);
    }
    let duration = start.elapsed();
    let secs = duration.as_secs_f64();
    let count = keys.len();

    println!("Processed: {} packets", count);
    println!("Time:      {:.6} s", secs);
    println!(
        "Throughput: {:.2} Mops/sec",
        (count as f64 / secs) / 1_000_000.0
    );
    println!();
}

fn main() {
    let args = Args::parse();

    println!("Allocating memory for up to {} packets...", MAX_PKTS);
    let mut keys = Vec::with_capacity(MAX_PKTS);

    // Iterate over all provided files
    for file_path in args.files {
        // C-style print: "Reading X ... " (no newline yet)
        print!("Reading {:?} ... ", file_path);
        io::stdout().flush().unwrap();

        let mut cap = match Capture::from_file(&file_path) {
            Ok(c) => c,
            Err(e) => {
                println!("[Warning] Could not open file: {}", e);
                continue;
            }
        };

        let mut file_count = 0;
        while let Ok(packet) = cap.next_packet() {
            if keys.len() >= MAX_PKTS {
                break;
            }

            // Check IP offset (26) validity
            if packet.header.len >= 30 && packet.data.len() >= 30 {
                let dst_ip_bytes = &packet.data[26..30];
                let ip_u32 = u32::from_ne_bytes(dst_ip_bytes.try_into().unwrap());
                keys.push(ip_u32);
                file_count += 1;
            }
        }

        // Finish the line: "Added X packets."
        println!("Added {} packets.", file_count);

        if keys.len() >= MAX_PKTS {
            println!("Reached MAX_PKTS limit.");
            break;
        }
    }

    println!("\nTotal Loaded: {} packets.\n", keys.len());
    if keys.is_empty() {
        return;
    }

    run_test("Full Sampling (1.0)", 1.0, &keys);
    run_test("Nitro Sampling (0.01)", 0.01, &keys);
}
