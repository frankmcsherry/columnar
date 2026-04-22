//! Direct Repeats / Lookbacks cursor bench: measure rank-free iteration win.

use bencher::{benchmark_group, benchmark_main, Bencher};
use columnar::{Borrow, Index, Len, Repeats, Lookbacks};

const N: u64 = 100_000;

fn repeats_populate(run: u64) -> Repeats<Vec<u64>> {
    use columnar::common::Push;
    let mut r: Repeats<Vec<u64>> = Default::default();
    for i in 0..N {
        Push::push(&mut r, &(i / run));
    }
    r
}

fn repeats_get(bencher: &mut Bencher) {
    let r = repeats_populate(100);
    let borrowed = r.borrow();
    bencher.bytes = N * 8;
    bencher.iter(|| {
        let mut sum = 0u64;
        for i in 0..borrowed.len() {
            sum = sum.wrapping_add(*borrowed.get(i));
        }
        bencher::black_box(sum);
    });
}

fn repeats_cursor(bencher: &mut Bencher) {
    let r = repeats_populate(100);
    let borrowed = r.borrow();
    bencher.bytes = N * 8;
    bencher.iter(|| {
        let mut sum = 0u64;
        for v in borrowed.index_iter() {
            sum = sum.wrapping_add(*v);
        }
        bencher::black_box(sum);
    });
}

fn lookbacks_populate() -> Lookbacks<Vec<u64>> {
    use columnar::common::Push;
    let mut l: Lookbacks<Vec<u64>> = Default::default();
    for i in 0..N {
        Push::push(&mut l, &(i % 17));
    }
    l
}

fn lookbacks_get(bencher: &mut Bencher) {
    let l = lookbacks_populate();
    let borrowed = l.borrow();
    bencher.bytes = N * 8;
    bencher.iter(|| {
        let mut sum = 0u64;
        for i in 0..borrowed.len() {
            sum = sum.wrapping_add(*borrowed.get(i));
        }
        bencher::black_box(sum);
    });
}

fn lookbacks_cursor(bencher: &mut Bencher) {
    let l = lookbacks_populate();
    let borrowed = l.borrow();
    bencher.bytes = N * 8;
    bencher.iter(|| {
        let mut sum = 0u64;
        for v in borrowed.index_iter() {
            sum = sum.wrapping_add(*v);
        }
        bencher::black_box(sum);
    });
}

benchmark_group!(benches, repeats_get, repeats_cursor, lookbacks_get, lookbacks_cursor);
benchmark_main!(benches);
