use bencher::{benchmark_group, benchmark_main, Bencher};

extern crate columnar;

use columnar::Columnar;

fn bench_simd_rows_all(bencher: &mut Bencher) {

    let rows = (0 .. 1024u32).map(|i| (i, i, i)).collect::<Vec<_>>();

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

fn bench_simd_rows_bad(bencher: &mut Bencher) {

    let rows = (0 .. 1024u32).map(|i| (i, i, i)).collect::<Vec<_>>();

    bencher.iter(|| {
        let sum0 = rows.iter().map(|x| x.0).sum::<u32>();
        let sum1 = rows.iter().map(|x| x.1).sum::<u32>();
        let sum2 = rows.iter().map(|x| x.2).sum::<u32>();
        bencher::black_box((sum0, sum1, sum2));
    });
}

fn bench_simd_cols_all(bencher: &mut Bencher) {

    let rows = (0 .. 1024u32).map(|i| (i, i, i)).collect::<Vec<_>>();
    let cols = Columnar::into_columns(rows.into_iter());

    bencher.iter(|| {
        let sum0 = cols.0.iter().sum::<u32>();
        let sum1 = cols.1.iter().sum::<u32>();
        let sum2 = cols.2.iter().sum::<u32>();
        bencher::black_box((sum0, sum1, sum2));
    });
}

fn bench_simd_rows_one(bencher: &mut Bencher) {

    let rows = (0 .. 1024u32).map(|i| (i, i, i)).collect::<Vec<_>>();

    bencher.iter(|| {
        let mut sum0 = 0;
        for (i, _j, _k) in rows.iter() {
            sum0 += i;
        }
        bencher::black_box(sum0);
    });
}

fn bench_simd_cols_one(bencher: &mut Bencher) {

    let rows = (0 .. 1024u32).map(|i| (i, i, i)).collect::<Vec<_>>();
    let cols = Columnar::into_columns(rows.into_iter());

    bencher.iter(|| {
        let sum0 = cols.0.iter().sum::<u32>();
        bencher::black_box(sum0);
    });
}

fn bench_simd_rows_mix(bencher: &mut Bencher) {

    let rows = (0 .. 1024u32).map(|i| (i, i, format!("{:?}", i))).collect::<Vec<_>>();

    bencher.iter(|| {
        let mut sum0 = 0;
        for (i, _j, _k) in rows.iter() {
            sum0 += i;
        }
        bencher::black_box(sum0);
    });
}

fn bench_simd_cols_mix(bencher: &mut Bencher) {

    let rows = (0 .. 1024u32).map(|i| (i, i, format!("{:?}", i))).collect::<Vec<_>>();
    let cols = Columnar::into_columns(rows.into_iter());

    bencher.iter(|| {
        let sum0 = cols.0.iter().sum::<u32>();
        bencher::black_box(sum0);
    });
}

benchmark_group!(
    cols,
    bench_simd_cols_one,
    bench_simd_cols_mix,
    bench_simd_cols_all,
);

benchmark_group!(
    rows,
    bench_simd_rows_one,
    bench_simd_rows_mix,
    bench_simd_rows_all,
    bench_simd_rows_bad,
);

benchmark_main!(cols, rows);