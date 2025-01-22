use bencher::{benchmark_group, benchmark_main, Bencher};

use columnar::Columnar;

fn simd_rows_all(bencher: &mut Bencher) {
    let rows = (0 .. 1024u32).map(|i| (i as u16, i, i as u64)).collect::<Vec<_>>();
    bencher.bytes = 14 * 1024;
    bencher.iter(|| {
        let mut sum0 = 0;
        let mut sum1 = 0;
        let mut sum2 = 0;
        for (i, j, k) in rows.iter() {
            sum0 += i;
            sum1 += j;
            sum2 += k;
        }
        bencher::black_box((sum0, sum1, sum2));
    });
}

fn simd_rows_bad(bencher: &mut Bencher) {
    let rows = (0 .. 1024u32).map(|i| (i as u16, i, i as u64)).collect::<Vec<_>>();
    bencher.bytes = 14 * 1024;
    bencher.iter(|| {
        let sum0 = rows.iter().map(|x| x.0).sum::<u16>();
        let sum1 = rows.iter().map(|x| x.1).sum::<u32>();
        let sum2 = rows.iter().map(|x| x.2).sum::<u64>();
        bencher::black_box((sum0, sum1, sum2));
    });
}

fn simd_cols_all(bencher: &mut Bencher) {
    let rows = (0 .. 1024u32).map(|i| (i as u16, i, i as u64)).collect::<Vec<_>>();
    let cols = Columnar::into_columns(rows.into_iter());
    bencher.bytes = 14 * 1024;
    bencher.iter(|| {
        let sum0 = cols.0.iter().sum::<u16>();
        let sum1 = cols.1.iter().sum::<u32>();
        let sum2 = cols.2.iter().sum::<u64>();
        bencher::black_box((sum0, sum1, sum2));
    });
}

fn simd_rows_1st(bencher: &mut Bencher) {
    let rows = (0 .. 1024u32).map(|i| (i as u16, i, i as u64)).collect::<Vec<_>>();
    bencher.bytes = 2 * 1024;
    bencher.iter(|| {
        let mut sum = 0;
        for x in rows.iter() {
            sum += x.0;
        }
        bencher::black_box(sum);
    });
}

fn simd_rows_2nd(bencher: &mut Bencher) {
    let rows = (0 .. 1024u32).map(|i| (i as u16, i, i as u64)).collect::<Vec<_>>();
    bencher.bytes = 4 * 1024;
    bencher.iter(|| {
        let mut sum = 0;
        for x in rows.iter() {
            sum += x.1;
        }
        bencher::black_box(sum);
    });
}
fn simd_rows_3rd(bencher: &mut Bencher) {
    let rows = (0 .. 1024u32).map(|i| (i as u16, i, i as u64)).collect::<Vec<_>>();
    bencher.bytes = 8 * 1024;
    bencher.iter(|| {
        let mut sum = 0;
        for x in rows.iter() {
            sum += x.2;
        }
        bencher::black_box(sum);
    });
}

fn simd_cols_1st(bencher: &mut Bencher) {
    let rows = (0 .. 1024u32).map(|i| (i as u16, i, i as u64)).collect::<Vec<_>>();
    let cols = Columnar::into_columns(rows.into_iter());
    bencher.bytes = 2 * 1024;
    bencher.iter(|| {
        let sum = cols.0.iter().sum::<u16>();
        bencher::black_box(sum);
    });
}
fn simd_cols_2nd(bencher: &mut Bencher) {
    let rows = (0 .. 1024u32).map(|i| (i as u16, i, i as u64)).collect::<Vec<_>>();
    let cols = Columnar::into_columns(rows.into_iter());
    bencher.bytes = 4 * 1024;
    bencher.iter(|| {
        let sum = cols.1.iter().sum::<u32>();
        bencher::black_box(sum);
    });
}
fn simd_cols_3rd(bencher: &mut Bencher) {
    let rows = (0 .. 1024u32).map(|i| (i as u16, i, i as u64)).collect::<Vec<_>>();
    let cols = Columnar::into_columns(rows.into_iter());
    bencher.bytes = 8 * 1024;
    bencher.iter(|| {
        let sum = cols.2.iter().sum::<u64>();
        bencher::black_box(sum);
    });
}

benchmark_group!(
    cols,
    simd_cols_1st,
    simd_cols_2nd,
    simd_cols_3rd,
    simd_cols_all,
);
benchmark_group!(
    rows,
    simd_rows_1st,
    simd_rows_2nd,
    simd_rows_3rd,
    simd_rows_all,
    simd_rows_bad,
);
benchmark_main!(cols, rows);
