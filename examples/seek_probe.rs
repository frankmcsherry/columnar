//! Decomposes the cost of forward seeks over bitvector bounds: from-scratch
//! `bounds(i)`, cursor `seek(i)`, and cursor stepping via `next_one` alone.

use std::time::Instant;
use columnar::{Bounds, NeverEmpty, Push};

const LISTS: usize = 1 << 16;
const REPS: usize = 50;

fn main() {
    let lengths: Vec<u64> = (0..LISTS).map(|i| ((i * i) % 16 + 1) as u64).collect();
    let mut bounds = NeverEmpty::default();
    for length in lengths.iter() {
        bounds.push(length);
    }

    // Gaps from the command line (or defaults), so they are runtime values
    // the compiler cannot specialize the query loops against.
    let gaps: Vec<usize> = if std::env::args().len() > 1 {
        std::env::args().skip(1).map(|s| s.parse().unwrap()).collect()
    } else {
        vec![1, 2, 4, 8, 16, 64, 256]
    };

    println!("{:>6} {:>12} {:>12} {:>12}", "gap", "scratch", "seek", "step");
    for gap in gaps {
        let queries = (LISTS + gap - 1) / gap;

        // (a) from-scratch bounds(i)
        let mut sum_a = 0u64;
        let t = Instant::now();
        for _ in 0..REPS {
            let mut index = 0;
            while index < LISTS {
                let (lower, upper) = bounds.bounds(index);
                sum_a += upper - lower;
                index += gap;
            }
        }
        let scratch = t.elapsed().as_nanos() as f64 / (REPS * queries) as f64;

        // (b) cursor seek(i)
        let mut sum_b = 0u64;
        let t = Instant::now();
        for _ in 0..REPS {
            let mut cursor = bounds.cursor();
            let mut index = 0;
            while index < LISTS {
                let (lower, upper) = cursor.seek(index);
                sum_b += upper - lower;
                index += gap;
            }
        }
        let seek = t.elapsed().as_nanos() as f64 / (REPS * queries) as f64;

        // (c) pure stepping: next_one through every end, reading each gap-th.
        let mut sum_c = 0u64;
        let t = Instant::now();
        for _ in 0..REPS {
            let mut cursor = bounds.bits.cursor();
            let mut prev = 0u64;
            let mut index = 0;
            while let Some(pos) = cursor.next_one() {
                let upper = pos as u64 + 1;
                if index % gap == 0 {
                    sum_c += upper - prev;
                }
                prev = upper;
                index += 1;
            }
        }
        let step = t.elapsed().as_nanos() as f64 / (REPS * queries) as f64;

        println!("{:>6} {:>11.2}n {:>11.2}n {:>11.2}n", gap, scratch, seek, step);
        // All three strategies must have read the same list lengths.
        assert_eq!(sum_a, sum_b);
        assert_eq!(sum_a, sum_c);
    }
}
