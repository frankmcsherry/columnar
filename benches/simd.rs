#![feature(test)]

extern crate columnar;
extern crate test;

use columnar::Columnar;
use test::Bencher;

#[bench]
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
        test::black_box((sum0, sum1, sum2));
    });
}

#[bench]
fn bench_simd_rows_bad(bencher: &mut Bencher) {

    let rows = (0 .. 1024u32).map(|i| (i, i, i)).collect::<Vec<_>>();

    bencher.iter(|| {
        let sum0 = rows.iter().map(|x| x.0).sum::<u32>();
        let sum1 = rows.iter().map(|x| x.1).sum::<u32>();
        let sum2 = rows.iter().map(|x| x.2).sum::<u32>();
        test::black_box((sum0, sum1, sum2));
    });
}

#[bench]
fn bench_simd_cols_all(bencher: &mut Bencher) {

    let rows = (0 .. 1024u32).map(|i| (i, i, i)).collect::<Vec<_>>();
    let cols = Columnar::into_columns(rows.into_iter());

    bencher.iter(|| {
        let sum0 = cols.0.iter().sum::<u32>();
        let sum1 = cols.1.iter().sum::<u32>();
        let sum2 = cols.2.iter().sum::<u32>();
        test::black_box((sum0, sum1, sum2));
    });
}

#[bench]
fn bench_simd_rows_one(bencher: &mut Bencher) {

    let rows = (0 .. 1024u32).map(|i| (i, i, i)).collect::<Vec<_>>();

    bencher.iter(|| {
        let mut sum0 = 0;
        for (i, _j, _k) in rows.iter() {
            sum0 += i;
        }
        test::black_box(sum0);
    });
}

#[bench]
fn bench_simd_cols_one(bencher: &mut Bencher) {

    let rows = (0 .. 1024u32).map(|i| (i, i, i)).collect::<Vec<_>>();
    let cols = Columnar::into_columns(rows.into_iter());

    bencher.iter(|| {
        let sum0 = cols.0.iter().sum::<u32>();
        test::black_box(sum0);
    });
}

#[bench]
fn bench_simd_rows_mix(bencher: &mut Bencher) {

    let rows = (0 .. 1024u32).map(|i| (i, i, format!("{:?}", i))).collect::<Vec<_>>();

    bencher.iter(|| {
        let mut sum0 = 0;
        for (i, _j, _k) in rows.iter() {
            sum0 += i;
        }
        test::black_box(sum0);
    });
}

#[bench]
fn bench_simd_cols_mix(bencher: &mut Bencher) {

    let rows = (0 .. 1024u32).map(|i| (i, i, format!("{:?}", i))).collect::<Vec<_>>();
    let cols = Columnar::into_columns(rows.into_iter());

    bencher.iter(|| {
        let sum0 = cols.0.iter().sum::<u32>();
        test::black_box(sum0);
    });
}