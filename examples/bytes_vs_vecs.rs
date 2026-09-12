//! Compares byte-payload columns stored as `Vecs<Vec<u8>>` (reference `Slice<&[u8]>`) against
//! the `Bytes`/`Strings` form (reference `&[u8]`), on the audit's worst case: keys with a long
//! shared prefix. Run with `cargo run --release --example bytes_vs_vecs`.

use columnar::{Borrow, Bytes, Columnar, Index, Len};
use std::hash::{Hash, Hasher};
use std::hint::black_box;
use std::time::Instant;

const ROWS: usize = 1_000_000;
const PREFIX: usize = 48;
const SUFFIX: usize = 8;

fn hash_one<H: Hash>(value: &H) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

/// Times sort-by-row, adjacent equality and per-row hashing over a borrowed container whose
/// rows are indexable and comparable.
fn measure<B: Copy + Len + Index>(borrow: B)
where
    B::Ref: Ord + Hash,
{
    let n = borrow.len();

    let mut order: Vec<usize> = (0..n).collect();
    let start = Instant::now();
    // `sort_by_key` cannot be used: the key borrows from the container.
    #[allow(clippy::unnecessary_sort_by)]
    order.sort_by(|&a, &b| borrow.get(a).cmp(&borrow.get(b)));
    black_box(&order);
    let sort = start.elapsed().as_secs_f64() * 1e9 / n as f64;

    let start = Instant::now();
    let mut eq_acc = 0usize;
    for i in 0..n - 1 {
        eq_acc += (borrow.get(i) == borrow.get(i + 1)) as usize;
    }
    black_box(eq_acc);
    let eq = start.elapsed().as_secs_f64() * 1e9 / n as f64;

    let start = Instant::now();
    let mut hash_acc = 0u64;
    for i in 0..n {
        hash_acc ^= hash_one(&borrow.get(i));
    }
    black_box(hash_acc);
    let hash = start.elapsed().as_secs_f64() * 1e9 / n as f64;

    println!("{:<22} {:>10.2} {:>10.2} {:>10.2}", "", sort, eq, hash);
}

fn main() {
    let mut state = 0x1234_5678_9abc_def0u64;
    let keys: Vec<Vec<u8>> = (0..ROWS)
        .map(|_| {
            let mut key = vec![0u8; PREFIX];
            for _ in 0..SUFFIX {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                key.push((state >> 33) as u8);
            }
            key
        })
        .collect();

    let vecs: <Vec<u8> as Columnar>::Container = Columnar::as_columns(keys.iter());
    let strings: <Bytes as Columnar>::Container =
        Columnar::as_columns(keys.iter().map(|k| Bytes(k.clone())).collect::<Vec<_>>().iter());

    println!("rows={ROWS}, prefix={PREFIX}, suffix={SUFFIX} (ns per row)");
    println!("{:<22} {:>10} {:>10} {:>10}", "container", "sort", "eq", "hash");
    print!("{:<22}", "Vecs<Vec<u8>>");
    measure(vecs.borrow());
    print!("{:<22}", "Bytes/Strings");
    measure(strings.borrow());
}
