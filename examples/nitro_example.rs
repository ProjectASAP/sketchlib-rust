use rand::{Rng, SeedableRng, rngs::StdRng};
use sketchlib_rust::{Nitro, SketchInput, hash_it_to_128};

const BIT_MASK: usize = (1_usize << 11) - 1;
const TO_SHIFT: usize = 11_usize;

fn build_input() -> Vec<u64> {
    let mut rng = StdRng::seed_from_u64(0x5eed_c0de_1234_5678);
    (0..1_000_000).map(|_| rng.random::<u64>()).collect()
}

#[inline(never)]
fn skip_packet_update(input: &SketchInput, sketch: &mut Vec<u32>, nitro: &mut Nitro) {
    let hashed = hash_it_to_128(0, input);
    if nitro.to_skip > 0 {
        nitro.to_skip -= 1;
    } else {
        for r in 0..5 {
            let idx = (hashed >> (TO_SHIFT * r)) as usize & BIT_MASK;
            sketch[2048 * r + idx] += 1;
        }
        nitro.draw_geometric();
    }
}

#[inline(never)]
fn skip_row_update(input: &SketchInput, sketch: &mut Vec<u32>, nitro: &mut Nitro) {
    let hashed = hash_it_to_128(0, input);
    if nitro.to_skip >= 5 {
        nitro.to_skip -= 5;
    } else {
        loop {
            let r = nitro.to_skip;
            let idx = (hashed >> (TO_SHIFT * r)) as usize & BIT_MASK;
            sketch[2048 * r + idx] += 1;
            nitro.draw_geometric();
            if nitro.to_skip >= 5 - r {
                nitro.to_skip -= 5 - r;
                break;
            } else {
                nitro.to_skip += r;
            }
        }
    }
}

pub fn main() {
    let value_to_insert = build_input();
    let mut nitro_row = Nitro::init_nitro(0.1);
    let mut nitro_pkt = Nitro::init_nitro(0.1);
    nitro_row.draw_geometric();
    nitro_pkt.draw_geometric();

    // imitate the fast update logic for cms
    let mut sketch_row = vec![0; 5 * 2048];
    let mut sketch_pkt = vec![0; 5 * 2048];

    // skip packet for update
    for val in &value_to_insert {
        skip_row_update(&SketchInput::U64(*val), &mut sketch_row, &mut nitro_row);
    }

    for val in &value_to_insert {
        skip_packet_update(&SketchInput::U64(*val), &mut sketch_pkt, &mut nitro_pkt);
    }
}
