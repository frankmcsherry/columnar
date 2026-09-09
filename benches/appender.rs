//! Measures the cost of forming a `Vec<Vec<String>>` equivalent in columnar
//! form and serializing it, stage by stage.

use bencher::benchmark_main;

mod stages {
use bencher::{benchmark_group, Bencher};
use columnar::{Append, Borrow, Clear, Len, Push, Vecs, Strings};
use columnar::bytes::indexed;

/// Rows per batch and strings per row. Sized so the typed container lands
/// near 1.5 MiB (strings of ~14 bytes plus 8 byte bounds each).
const ROWS: usize = 2048;
const COLS: usize = 32;

fn fmt_into(s: &mut String, r: usize, c: usize) {
    use std::fmt::Write;
    write!(s, "{}-{}-grawwwwrr!", r, c).unwrap();
}

/// Builds the owned `Vec<Vec<String>>` from scratch, allocating each `String`.
fn build_owned() -> Vec<Vec<String>> {
    (0..ROWS).map(|r| (0..COLS).map(|c| { let mut s = String::new(); fmt_into(&mut s, r, c); s }).collect()).collect()
}

fn typed_bytes() -> u64 {
    let owned = build_owned();
    let mut vecs: Vecs<Strings> = Default::default();
    for row in &owned { vecs.push(row); }
    indexed::length_in_bytes(&vecs.borrow()) as u64
}

/// Stage 1 alone: format into fresh `String`s.
fn s1_format(b: &mut Bencher) {
    b.bytes = typed_bytes();
    b.iter(build_owned);
}

/// Stage 2 alone: push a prebuilt `Vec<Vec<String>>` into `Vecs<Strings>`.
fn s2_push(b: &mut Bencher) {
    b.bytes = typed_bytes();
    let owned = build_owned();
    let mut vecs: Vecs<Strings> = Default::default();
    b.iter(|| {
        vecs.clear();
        for row in &owned { vecs.push(row); }
    });
}

/// Stage 3 alone: encode a prebuilt `Vecs<Strings>`.
fn s3_encode(b: &mut Bencher) {
    b.bytes = typed_bytes();
    let owned = build_owned();
    let mut vecs: Vecs<Strings> = Default::default();
    for row in &owned { vecs.push(row); }
    let mut store: Vec<u64> = Vec::new();
    b.iter(|| {
        store.clear();
        indexed::encode(&mut store, &vecs.borrow());
    });
}

/// All three stages as done today.
fn s123_all(b: &mut Bencher) {
    b.bytes = typed_bytes();
    let mut vecs: Vecs<Strings> = Default::default();
    let mut store: Vec<u64> = Vec::new();
    b.iter(|| {
        let owned = build_owned();
        vecs.clear();
        for row in &owned { vecs.push(row); }
        store.clear();
        indexed::encode(&mut store, &vecs.borrow());
    });
}

/// Stage 1 with a reused `String` buffer, pushed row by row. Fairer baseline:
/// no per-string allocation, but still one copy from buffer into the column.
fn s12_reuse_buf(b: &mut Bencher) {
    b.bytes = typed_bytes();
    let mut vecs: Vecs<Strings> = Default::default();
    let mut buf = String::new();
    b.iter(|| {
        vecs.clear();
        for r in 0..ROWS {
            for c in 0..COLS {
                buf.clear();
                fmt_into(&mut buf, r, c);
                vecs.values.push(buf.as_str());
            }
            vecs.bounds.push(vecs.values.len() as u64);
        }
    });
}

/// Ceiling: write formatter output straight into `Vecs<Strings>` by hand,
/// touching the container internals. This is what an appender should match.
fn direct_manual(b: &mut Bencher) {
    b.bytes = typed_bytes();
    let mut vecs: Vecs<Strings> = Default::default();
    b.iter(|| {
        vecs.clear();
        for r in 0..ROWS {
            for c in 0..COLS {
                vecs.values.push(format_args!("{}-{}-grawwwwrr!", r, c));
            }
            vecs.bounds.push(vecs.values.len() as u64);
        }
    });
}

/// Ceiling plus encode.
fn direct_manual_encode(b: &mut Bencher) {
    b.bytes = typed_bytes();
    let mut vecs: Vecs<Strings> = Default::default();
    let mut store: Vec<u64> = Vec::new();
    b.iter(|| {
        vecs.clear();
        for r in 0..ROWS {
            for c in 0..COLS {
                vecs.values.push(format_args!("{}-{}-grawwwwrr!", r, c));
            }
            vecs.bounds.push(vecs.values.len() as u64);
        }
        store.clear();
        indexed::encode(&mut store, &vecs.borrow());
    });
}

/// A long payload so that copying, not integer formatting, dominates.
const LONG: &str = "grawwwwrr!grawwwwrr!grawwwwrr!grawwwwrr!grawwwwrr!grawwwwrr!grawwwwrr!grawwwwrr!grawwwwrr!grawwwwrr!grawwwwrr!grawwwwrr!grawwwwrr!grawwwwrr!grawwwwrr!grawwwwrr!grawwwwrr!grawwwwrr!grawwwwrr!grawwwwrr!grawwwwrr!grawwwwrr!grawwwwrr!grawwwwrr!";
const LONG_ROWS: usize = 256;

fn long_bytes() -> u64 {
    let mut vecs: Vecs<Strings> = Default::default();
    for r in 0..LONG_ROWS {
        for c in 0..COLS { vecs.values.push(format_args!("{}-{}-{}", r, c, LONG)); }
        vecs.bounds.push(vecs.values.len() as u64);
    }
    indexed::length_in_bytes(&vecs.borrow()) as u64
}

/// Long strings, reused `String` scratch buffer then copy into column.
fn long_reuse_buf(b: &mut Bencher) {
    use std::fmt::Write;
    b.bytes = long_bytes();
    let mut vecs: Vecs<Strings> = Default::default();
    let mut buf = String::new();
    b.iter(|| {
        vecs.clear();
        for r in 0..LONG_ROWS {
            for c in 0..COLS {
                buf.clear();
                write!(buf, "{}-{}-{}", r, c, LONG).unwrap();
                vecs.values.push(buf.as_str());
            }
            vecs.bounds.push(vecs.values.len() as u64);
        }
    });
}

/// Long strings, formatted straight into the column.
fn long_direct_manual(b: &mut Bencher) {
    b.bytes = long_bytes();
    let mut vecs: Vecs<Strings> = Default::default();
    b.iter(|| {
        vecs.clear();
        for r in 0..LONG_ROWS {
            for c in 0..COLS {
                vecs.values.push(format_args!("{}-{}-{}", r, c, LONG));
            }
            vecs.bounds.push(vecs.values.len() as u64);
        }
    });
}

/// Long strings, allocating a `String` per element then pushing rows.
fn long_alloc(b: &mut Bencher) {
    b.bytes = long_bytes();
    let mut vecs: Vecs<Strings> = Default::default();
    b.iter(|| {
        vecs.clear();
        for r in 0..LONG_ROWS {
            let row: Vec<String> = (0..COLS).map(|c| format!("{}-{}-{}", r, c, LONG)).collect();
            vecs.push(&row);
        }
    });
}

/// Long strings, formatted through the `Append` API.
fn long_appender(b: &mut Bencher) {
    use std::fmt::Write;
    b.bytes = long_bytes();
    let mut vecs: Vecs<Strings> = Default::default();
    b.iter(|| {
        vecs.clear();
        for r in 0..LONG_ROWS {
            let mut row = vecs.appender();
            for c in 0..COLS {
                write!(row.appender(), "{}-{}-{}", r, c, LONG).unwrap();
            }
        }
    });
}

/// Short strings, formatted through the `Append` API.
fn appender(b: &mut Bencher) {
    use std::fmt::Write;
    b.bytes = typed_bytes();
    let mut vecs: Vecs<Strings> = Default::default();
    b.iter(|| {
        vecs.clear();
        for r in 0..ROWS {
            let mut row = vecs.appender();
            for c in 0..COLS {
                write!(row.appender(), "{}-{}-grawwwwrr!", r, c).unwrap();
            }
        }
    });
}

/// Short strings through the `Append` API, then encode.
fn appender_encode(b: &mut Bencher) {
    use std::fmt::Write;
    b.bytes = typed_bytes();
    let mut vecs: Vecs<Strings> = Default::default();
    let mut store: Vec<u64> = Vec::new();
    b.iter(|| {
        vecs.clear();
        for r in 0..ROWS {
            let mut row = vecs.appender();
            for c in 0..COLS {
                write!(row.appender(), "{}-{}-grawwwwrr!", r, c).unwrap();
            }
        }
        store.clear();
        indexed::encode(&mut store, &vecs.borrow());
    });
}

benchmark_group!(long, long_alloc, long_reuse_buf, long_direct_manual, long_appender);

benchmark_group!(stages, s1_format, s2_push, s3_encode, s123_all, s12_reuse_buf, direct_manual, direct_manual_encode, appender, appender_encode);
}
use stages::{stages, long};
benchmark_main!(stages, long);
