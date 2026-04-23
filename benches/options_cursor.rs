//! Bench Options/Results cursor: does cursor speed up iteration vs get()?
//!
//! Default impl uses DefaultCursor which delegates to get() → rank() per element.
//! Same rank() overhead as Repeats before specialization.

use bencher::{benchmark_group, benchmark_main, Bencher};
use columnar::{Borrow, Index, Len, Options, Results, Sequence};

const N: u64 = 100_000;

fn options_populate() -> Options<Vec<u64>> {
    use columnar::common::Push;
    let mut opts: Options<Vec<u64>> = Default::default();
    for i in 0..N {
        if i % 3 == 0 { Push::push(&mut opts, None::<u64>); }
        else { Push::push(&mut opts, Some(i)); }
    }
    opts
}

fn options_get(bencher: &mut Bencher) {
    let opts = options_populate();
    let borrowed = opts.borrow();
    bencher.bytes = N * 8;
    bencher.iter(|| {
        let mut sum = 0u64;
        for i in 0..borrowed.len() {
            if let Some(v) = borrowed.get(i) {
                sum = sum.wrapping_add(*v);
            }
        }
        bencher::black_box(sum);
    });
}

fn options_cursor(bencher: &mut Bencher) {
    let opts = options_populate();
    let borrowed = opts.borrow();
    bencher.bytes = N * 8;
    bencher.iter(|| {
        let mut sum = 0u64;
        for v in borrowed.seq_iter() {
            if let Some(v) = v {
                sum = sum.wrapping_add(*v);
            }
        }
        bencher::black_box(sum);
    });
}

fn results_populate() -> Results<Vec<u64>, Vec<u32>> {
    use columnar::common::Push;
    let mut r: Results<Vec<u64>, Vec<u32>> = Default::default();
    for i in 0..N {
        if i % 5 == 0 { Push::push(&mut r, Err::<u64, u32>(i as u32)); }
        else { Push::push(&mut r, Ok::<u64, u32>(i)); }
    }
    r
}

fn results_get(bencher: &mut Bencher) {
    let r = results_populate();
    let borrowed = r.borrow();
    bencher.bytes = N * 8;
    bencher.iter(|| {
        let mut sum = 0u64;
        for i in 0..borrowed.len() {
            match borrowed.get(i) {
                Ok(v) => sum = sum.wrapping_add(*v),
                Err(e) => sum = sum.wrapping_add(*e as u64),
            }
        }
        bencher::black_box(sum);
    });
}

fn results_cursor(bencher: &mut Bencher) {
    let r = results_populate();
    let borrowed = r.borrow();
    bencher.bytes = N * 8;
    bencher.iter(|| {
        let mut sum = 0u64;
        for v in borrowed.seq_iter() {
            match v {
                Ok(v) => sum = sum.wrapping_add(*v),
                Err(e) => sum = sum.wrapping_add(*e as u64),
            }
        }
        bencher::black_box(sum);
    });
}

benchmark_group!(benches, options_get, options_cursor, results_get, results_cursor);
benchmark_main!(benches);
