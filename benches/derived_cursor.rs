//! Bench composed-cursor vs default-cursor on derived struct containers.
//!
//! Focus: derived struct with a Repeats field. Composed cursor would let the
//! Repeats field use its rank-free cursor; DefaultCursor falls back to get()
//! which calls rank() per element.

use bencher::{benchmark_group, benchmark_main, Bencher};
use bencher as _;
use columnar::{Columnar, Borrow, Index, Len, Repeats};

#[derive(Columnar, Debug, PartialEq, Eq, Clone)]
struct Row {
    key: u64,
    val: u64,
}

const N: u64 = 100_000;
const KEYS: u64 = 100;

fn populate() -> RowContainer<Repeats<Vec<u64>>, Vec<u64>> {
    use columnar::common::Push;
    let mut keys: Repeats<Vec<u64>> = Default::default();
    let mut vals: Vec<u64> = Vec::with_capacity(N as usize);
    for i in 0..N {
        Push::push(&mut keys, &(i / (N / KEYS)));
        vals.push(i);
    }
    RowContainer { key: keys, val: vals }
}

fn derived_get(bencher: &mut Bencher) {
    let container = populate();
    let borrowed = container.borrow();
    bencher.bytes = (N * 16) as u64;
    bencher.iter(|| {
        let mut sum = 0u64;
        for i in 0..borrowed.len() {
            let row = borrowed.get(i);
            sum = sum.wrapping_add(*row.key).wrapping_add(*row.val);
        }
        bencher::black_box(sum);
    });
}

fn derived_cursor(bencher: &mut Bencher) {
    let container = populate();
    let borrowed = container.borrow();
    bencher.bytes = (N * 16) as u64;
    bencher.iter(|| {
        let mut sum = 0u64;
        for row in borrowed.index_iter() {
            sum = sum.wrapping_add(*row.key).wrapping_add(*row.val);
        }
        bencher::black_box(sum);
    });
}

// Hand-written tuple for comparison — already gets composed cursor.
fn tuple_cursor(bencher: &mut Bencher) {
    use columnar::common::Push;
    let mut keys: Repeats<Vec<u64>> = Default::default();
    let mut vals: Vec<u64> = Vec::with_capacity(N as usize);
    for i in 0..N {
        Push::push(&mut keys, &(i / (N / KEYS)));
        vals.push(i);
    }
    let container = (keys, vals);
    let borrowed = container.borrow();
    bencher.bytes = (N * 16) as u64;
    bencher.iter(|| {
        let mut sum = 0u64;
        for (k, v) in borrowed.index_iter() {
            sum = sum.wrapping_add(*k).wrapping_add(*v);
        }
        bencher::black_box(sum);
    });
}

benchmark_group!(benches, derived_get, derived_cursor, tuple_cursor);
benchmark_main!(benches);
