//! Benchmark: decode + indexed access for various columnar shapes.
//!
//! For each shape we measure two things:
//!   1. **decode+index** — repeatedly decode from bytes then index.
//!   2. **index-only**   — decode once, then repeatedly index the borrowed form.
//!
//! The rule: we always perform field projections or variant matches on the
//! *container* before calling `.get(index)` on the projected column.

use columnar::*;
use columnar::common::{Push, Index};
use columnar::bytes::indexed::DecodedStore;

use std::hint::black_box;
use std::time::Instant;

// ── helpers ──────────────────────────────────────────────────────────

/// Time a closure over `iters` iterations, return ns per iteration.
fn bench_ns<F: FnMut()>(iters: u64, mut f: F) -> f64 {
    // warmup
    for _ in 0..iters.min(1000) { f(); }
    let start = Instant::now();
    for _ in 0..iters { f(); }
    start.elapsed().as_nanos() as f64 / iters as f64
}

/// Encode a container to bytes, return the backing store as `Vec<u64>`.
fn encode<C: ContainerBytes>(container: &C) -> Vec<u64>
where
    for<'a> C::Borrowed<'a>: AsBytes<'a>,
{
    let mut bytes: Vec<u8> = Vec::new();
    columnar::bytes::indexed::write(&mut bytes, &container.borrow()).unwrap();
    // Cast to u64 words (write guarantees 8-byte alignment and padding).
    assert!(bytes.len() % 8 == 0);
    let mut words: Vec<u64> = vec![0u64; bytes.len() / 8];
    bytemuck::cast_slice_mut(&mut words).copy_from_slice(&bytes);
    words
}

/// Decode a `Borrowed` from a word store.
fn decode<'a, B: FromBytes<'a>>(store: &'a [u64]) -> B {
    let ds = DecodedStore::new(store);
    B::from_store(&ds, &mut 0)
}

fn header(title: &str) {
    println!("\n=== {title} ===");
}

fn report(label: &str, decode_index_ns: f64, index_only_ns: f64) {
    println!(
        "  {label:<45} decode+index {decode_index_ns:>8.1} ns   index-only {index_only_ns:>8.1} ns   decode overhead {:.1} ns",
        decode_index_ns - index_only_ns
    );
}

// ── Shape 1: u64 ────────────────────────────────────────────────────

fn bench_u64(n: usize, iters: u64) {
    let mut c: ContainerOf<u64> = Default::default();
    for i in 0..n as u64 { c.push(i); }
    let store = encode(&c);
    let idx = n / 2;

    let di = bench_ns(iters, || {
        let borrowed: BorrowedOf<'_, u64> = decode(&store);
        black_box(Index::get(&borrowed, idx));
    });

    let borrowed: BorrowedOf<'_, u64> = decode(&store);
    let io = bench_ns(iters, || {
        black_box(Index::get(&borrowed, idx));
    });

    report(&format!("u64 (n={n})"), di, io);
}

// ── Shape 2: (String, u64, String) — project to field before get ───

fn bench_string_tuple(n: usize, iters: u64) {
    let mut c: ContainerOf<(String, u64, String)> = Default::default();
    for i in 0..n {
        c.push(&(format!("name_{i}"), i as u64, format!("city_{}", i % 50)));
    }
    let store = encode(&c);
    let idx = n / 2;

    // Project to field .0 (String column), then get.
    let di = bench_ns(iters, || {
        let borrowed: BorrowedOf<'_, (String, u64, String)> = decode(&store);
        let name: &[u8] = borrowed.0.get(idx);
        black_box(name);
    });

    let borrowed: BorrowedOf<'_, (String, u64, String)> = decode(&store);
    let io = bench_ns(iters, || {
        let name: &[u8] = borrowed.0.get(idx);
        black_box(name);
    });

    report(&format!("(String,u64,String).0 (n={n})"), di, io);

    // Project to field .1 (u64 column).
    let di2 = bench_ns(iters, || {
        let borrowed: BorrowedOf<'_, (String, u64, String)> = decode(&store);
        black_box(Index::get(&borrowed.1, idx));
    });

    let borrowed: BorrowedOf<'_, (String, u64, String)> = decode(&store);
    let io2 = bench_ns(iters, || {
        black_box(Index::get(&borrowed.1, idx));
    });

    report(&format!("(String,u64,String).1 (n={n})"), di2, io2);
}

// ── Shape 3: Vec<Vec<u64>> — nested vectors ─────────────────────────

fn bench_nested_vecs(n: usize, iters: u64) {
    let mut c: ContainerOf<Vec<Vec<u64>>> = Default::default();
    for i in 0..n {
        let inner: Vec<Vec<u64>> = (0..(i % 5) + 1)
            .map(|j| (0..(j % 8) + 1).map(|k| (i + j + k) as u64).collect())
            .collect();
        c.push(&inner);
    }
    let store = encode(&c);
    let idx = n / 2;

    // Navigate: outer.get(idx) → inner.get(0) → u64.
    let di = bench_ns(iters, || {
        let borrowed: BorrowedOf<'_, Vec<Vec<u64>>> = decode(&store);
        let outer_slice = borrowed.get(idx);
        let inner_slice = outer_slice.get(0);
        black_box(inner_slice.get(0));
    });

    let borrowed: BorrowedOf<'_, Vec<Vec<u64>>> = decode(&store);
    let io = bench_ns(iters, || {
        let outer_slice = borrowed.get(idx);
        let inner_slice = outer_slice.get(0);
        black_box(inner_slice.get(0));
    });

    report(&format!("Vec<Vec<u64>> [0][0] (n={n})"), di, io);
}

// ── Shape 4: Result<(u64, String), u64> — variant match then field ──

fn bench_result(n: usize, iters: u64) {
    let mut c: ContainerOf<Result<(u64, String), u64>> = Default::default();
    for i in 0..n {
        let val: Result<(u64, String), u64> = if i % 3 == 0 {
            Err(i as u64)
        } else {
            Ok((i as u64, format!("val_{i}")))
        };
        c.push(&val);
    }
    let store = encode(&c);
    // Pick an index we know is Ok (1 % 3 != 0).
    let idx = 1;

    // Variant match on the Result, then destructure.
    let di = bench_ns(iters, || {
        let borrowed: BorrowedOf<'_, Result<(u64, String), u64>> = decode(&store);
        match borrowed.get(idx) {
            Ok((num, _name)) => { black_box(num); },
            Err(code) => { black_box(code); },
        }
    });

    let borrowed: BorrowedOf<'_, Result<(u64, String), u64>> = decode(&store);
    let io = bench_ns(iters, || {
        match borrowed.get(idx) {
            Ok((num, _name)) => { black_box(num); },
            Err(code) => { black_box(code); },
        }
    });

    report(&format!("Result<(u64,String),u64> match (n={n})"), di, io);

    // Project to .oks column directly, rank, then index.
    let di2 = bench_ns(iters, || {
        let borrowed: BorrowedOf<'_, Result<(u64, String), u64>> = decode(&store);
        let rank = borrowed.indexes.rank(idx);
        let val = borrowed.oks.get(rank);
        black_box(val.0);
    });

    let borrowed: BorrowedOf<'_, Result<(u64, String), u64>> = decode(&store);
    let io2 = bench_ns(iters, || {
        let rank = borrowed.indexes.rank(idx);
        let val = borrowed.oks.get(rank);
        black_box(val.0);
    });

    report(&format!("Result .oks projected (n={n})"), di2, io2);
}

// ── Shape 5: Option<String> — variant match, then access ────────────

fn bench_option_string(n: usize, iters: u64) {
    let mut c: ContainerOf<Option<String>> = Default::default();
    for i in 0..n {
        if i % 4 == 0 {
            c.push(&None::<String>);
        } else {
            c.push(&Some(format!("item_{i}")));
        }
    }
    let store = encode(&c);
    let idx = 1;

    let di = bench_ns(iters, || {
        let borrowed: BorrowedOf<'_, Option<String>> = decode(&store);
        match borrowed.get(idx) {
            Some(s) => { black_box(s); },
            None => { black_box(()); },
        }
    });

    let borrowed: BorrowedOf<'_, Option<String>> = decode(&store);
    let io = bench_ns(iters, || {
        match borrowed.get(idx) {
            Some(s) => { black_box(s); },
            None => { black_box(()); },
        }
    });

    report(&format!("Option<String> match (n={n})"), di, io);

    // Project to .somes directly.
    let di2 = bench_ns(iters, || {
        let borrowed: BorrowedOf<'_, Option<String>> = decode(&store);
        let rank = borrowed.indexes.rank(idx);
        let s = borrowed.somes.get(rank);
        black_box(s);
    });

    let borrowed: BorrowedOf<'_, Option<String>> = decode(&store);
    let io2 = bench_ns(iters, || {
        let rank = borrowed.indexes.rank(idx);
        let s = borrowed.somes.get(rank);
        black_box(s);
    });

    report(&format!("Option<String> .somes projected (n={n})"), di2, io2);
}

// ── Shape 6: Derived enum — variant + field projection ──────────────

#[derive(Columnar)]
enum Event {
    Click(u64, u64),
    Scroll(i64),
    Key(String),
}

fn bench_derived_enum(n: usize, iters: u64) {
    let mut c: ContainerOf<Event> = Default::default();
    for i in 0..n {
        match i % 3 {
            0 => c.push(&Event::Click(i as u64, (i * 2) as u64)),
            1 => c.push(&Event::Scroll(i as i64)),
            _ => c.push(&Event::Key(format!("key_{i}"))),
        }
    }
    let store = encode(&c);
    let idx = 0; // Click

    // Full variant match via .get(idx).
    let di = bench_ns(iters, || {
        let borrowed: BorrowedOf<'_, Event> = decode(&store);
        match borrowed.get(idx) {
            EventReference::Click((x, y)) => { black_box((x, y)); },
            EventReference::Scroll(d) => { black_box(d); },
            EventReference::Key(k) => { black_box(k); },
        }
    });

    let borrowed: BorrowedOf<'_, Event> = decode(&store);
    let io = bench_ns(iters, || {
        match borrowed.get(idx) {
            EventReference::Click((x, y)) => { black_box((x, y)); },
            EventReference::Scroll(d) => { black_box(d); },
            EventReference::Key(k) => { black_box(k); },
        }
    });

    report(&format!("Event enum match (n={n})"), di, io);

    // Project to the Click.0 column (all x values for Click variants).
    let di2 = bench_ns(iters, || {
        let borrowed: BorrowedOf<'_, Event> = decode(&store);
        let x_col: &[u64] = borrowed.group_0.0;
        black_box(Index::get(&x_col, 0));
    });

    let borrowed: BorrowedOf<'_, Event> = decode(&store);
    let io2 = bench_ns(iters, || {
        let x_col: &[u64] = borrowed.group_0.0;
        black_box(Index::get(&x_col, 0));
    });

    report(&format!("Event .Click.0 projected (n={n})"), di2, io2);
}

// ── Shape 7: Wide tuple — many fields, project to last ──────────────

fn bench_wide_tuple(n: usize, iters: u64) {
    let mut c: ContainerOf<(u64, u64, u64, u64, String, u64, u64, u64)> = Default::default();
    for i in 0..n as u64 {
        c.push(&(i, i+1, i+2, i+3, format!("w_{i}"), i+5, i+6, i+7));
    }
    let store = encode(&c);
    let idx = n / 2;

    // Project to the last field (.7), then index.
    let di = bench_ns(iters, || {
        let borrowed: BorrowedOf<'_, (u64, u64, u64, u64, String, u64, u64, u64)> = decode(&store);
        black_box(Index::get(&borrowed.7, idx));
    });

    let borrowed: BorrowedOf<'_, (u64, u64, u64, u64, String, u64, u64, u64)> = decode(&store);
    let io = bench_ns(iters, || {
        black_box(Index::get(&borrowed.7, idx));
    });

    report(&format!("8-tuple .7 projected (n={n})"), di, io);

    // Project to the String field (.4).
    let di2 = bench_ns(iters, || {
        let borrowed: BorrowedOf<'_, (u64, u64, u64, u64, String, u64, u64, u64)> = decode(&store);
        let s: &[u8] = borrowed.4.get(idx);
        black_box(s);
    });

    let borrowed: BorrowedOf<'_, (u64, u64, u64, u64, String, u64, u64, u64)> = decode(&store);
    let io2 = bench_ns(iters, || {
        let s: &[u8] = borrowed.4.get(idx);
        black_box(s);
    });

    report(&format!("8-tuple .4 (String) projected (n={n})"), di2, io2);
}

// ── Shape 8: String vs Vec<u8> — isolate from_utf8 cost ─────────────

fn bench_string_vs_bytes(n: usize, iters: u64) {
    // Build identical data as String and as Vec<u8>.
    let mut c_str: ContainerOf<(String, u64)> = Default::default();
    let mut c_bytes: ContainerOf<(Vec<u8>, u64)> = Default::default();
    for i in 0..n {
        let s = format!("name_{i}");
        c_bytes.push(&(s.as_bytes().to_vec(), i as u64));
        c_str.push(&(s, i as u64));
    }
    let store_str = encode(&c_str);
    let store_bytes = encode(&c_bytes);
    let idx = n / 2;

    // String: index-only
    let borrowed_str: BorrowedOf<'_, (String, u64)> = decode(&store_str);
    let io_str = bench_ns(iters, || {
        let s: &[u8] = borrowed_str.0.get(idx);
        black_box(s);
    });

    // Vec<u8>: index-only (same shape, no from_utf8)
    let borrowed_bytes: BorrowedOf<'_, (Vec<u8>, u64)> = decode(&store_bytes);
    let io_bytes = bench_ns(iters, || {
        let s = borrowed_bytes.0.get(idx);
        black_box(s);
    });

    println!(
        "  {:<45} String {io_str:>8.1} ns   Vec<u8> {io_bytes:>8.1} ns   from_utf8 cost {:.1} ns",
        format!("index-only .0 (n={n})"),
        io_str - io_bytes
    );

    // Decode+index for both.
    let di_str = bench_ns(iters, || {
        let borrowed: BorrowedOf<'_, (String, u64)> = decode(&store_str);
        let s: &[u8] = borrowed.0.get(idx);
        black_box(s);
    });

    let di_bytes = bench_ns(iters, || {
        let borrowed: BorrowedOf<'_, (Vec<u8>, u64)> = decode(&store_bytes);
        let s = borrowed.0.get(idx);
        black_box(s);
    });

    println!(
        "  {:<45} String {di_str:>8.1} ns   Vec<u8> {di_bytes:>8.1} ns   from_utf8 cost {:.1} ns",
        format!("decode+index .0 (n={n})"),
        di_str - di_bytes
    );
}

// ── Shape 9: Stash round-trip (types that support try_from_bytes) ───

fn bench_stash(n: usize, iters: u64) {
    use columnar::bytes::stash::Stash;

    // u64 through Stash.
    {
        let mut c: ContainerOf<u64> = Default::default();
        for i in 0..n as u64 { c.push(i); }

        let mut bytes: Vec<u8> = Vec::new();
        columnar::bytes::indexed::write(&mut bytes, &c.borrow()).unwrap();
        let stash: Stash<ContainerOf<u64>, Vec<u8>> = Stash::try_from_bytes(bytes).expect("valid");

        let idx = n / 2;
        let di = bench_ns(iters, || {
            let borrowed = stash.borrow();
            black_box(Index::get(&borrowed, idx));
        });

        let borrowed = stash.borrow();
        let io = bench_ns(iters, || {
            black_box(Index::get(&borrowed, idx));
        });

        report(&format!("Stash<u64> (n={n})"), di, io);
    }

    // (String, u64, String) through Stash.
    {
        let mut c: ContainerOf<(String, u64, String)> = Default::default();
        for i in 0..n {
            c.push(&(format!("name_{i}"), i as u64, format!("city_{}", i % 50)));
        }

        let mut bytes: Vec<u8> = Vec::new();
        columnar::bytes::indexed::write(&mut bytes, &c.borrow()).unwrap();
        let stash: Stash<ContainerOf<(String, u64, String)>, Vec<u8>> =
            Stash::try_from_bytes(bytes).expect("valid");

        let idx = n / 2;
        let di = bench_ns(iters, || {
            let borrowed = stash.borrow();
            let name: &[u8] = borrowed.0.get(idx);
            black_box(name);
        });

        let borrowed = stash.borrow();
        let io = bench_ns(iters, || {
            let name: &[u8] = borrowed.0.get(idx);
            black_box(name);
        });

        report(&format!("Stash<(String,u64,String)>.0 (n={n})"), di, io);
    }
}

// ── Shape 10: Cache effects — sequential vs random at increasing scale

/// Simple pseudo-random index sequence (xorshift-style).
fn pseudo_random_indices(n: usize, count: usize) -> Vec<usize> {
    let mut indices = Vec::with_capacity(count);
    let mut state: u64 = 0xdeadbeef;
    for _ in 0..count {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        indices.push((state as usize) % n);
    }
    indices
}

/// Sequential indices that wrap around.
fn sequential_indices(n: usize, count: usize) -> Vec<usize> {
    (0..count).map(|i| i % n).collect()
}

/// Measure average ns/access over a batch of indices.
fn bench_access<F: Fn(usize)>(iters: u64, indices: &[usize], f: F) -> f64 {
    bench_ns(iters, || {
        for &idx in indices {
            f(idx);
        }
    }) / indices.len() as f64
}

fn bench_cache_effects(_iters: u64) {
    let sizes: Vec<usize> = vec![1_000, 10_000, 100_000, 1_000_000, 10_000_000];
    let access_count = 100_000;
    // Enough outer iters so the inner loop dominates; scale with n.
    let base_iters: u64 = 200;

    // Type labels for the combined table.
    let type_names = [
        "u64",
        "Vec<u8>",
        "VV<u64>",
        "String",
        "~str/1B",
        "~str/all",
        "Opt<Str>",
    ];

    // Print header.
    print!("  {:>12}", "n");
    for name in &type_names {
        print!(" {:>9} {:>9}", format!("{name}/s"), format!("{name}/r"));
    }
    println!();
    println!("  {}", "-".repeat(12 + type_names.len() * 20));

    for &n in &sizes {
        let iters = base_iters / (n as u64 / 1000).max(1);
        let iters = iters.max(2);
        let seq = sequential_indices(n, access_count);
        let rnd = pseudo_random_indices(n, access_count);

        // --- u64: dereference to force load ---
        let (u64_seq, u64_rnd) = {
            let mut c: ContainerOf<u64> = Default::default();
            for i in 0..n as u64 { c.push(i); }
            let store = encode(&c);
            let borrowed: BorrowedOf<'_, u64> = decode(&store);
            let s = bench_access(iters, &seq, |idx| { black_box(*Index::get(&borrowed, idx)); });
            let r = bench_access(iters, &rnd, |idx| { black_box(*Index::get(&borrowed, idx)); });
            (s, r)
        };

        // --- Vec<u8>: read first byte to force values load ---
        let (vu8_seq, vu8_rnd) = {
            let mut c: ContainerOf<Vec<u8>> = Default::default();
            for i in 0..n {
                c.push(&vec![(i % 256) as u8; (i % 32) + 4]);
            }
            let store = encode(&c);
            let borrowed: BorrowedOf<'_, Vec<u8>> = decode(&store);
            let s = bench_access(iters, &seq, |idx| {
                let s = borrowed.get(idx);
                black_box(s.get(0));
            });
            let r = bench_access(iters, &rnd, |idx| {
                let s = borrowed.get(idx);
                black_box(s.get(0));
            });
            (s, r)
        };

        // --- Vec<Vec<u64>>: three arrays, dereference inner value ---
        let (vv_seq, vv_rnd) = {
            let mut c: ContainerOf<Vec<Vec<u64>>> = Default::default();
            for i in 0..n {
                let inner: Vec<Vec<u64>> = (0..(i % 3) + 1)
                    .map(|j| vec![(i + j) as u64; (j % 4) + 1])
                    .collect();
                c.push(&inner);
            }
            let store = encode(&c);
            let borrowed: BorrowedOf<'_, Vec<Vec<u64>>> = decode(&store);
            let s = bench_access(iters, &seq, |idx| {
                let outer = borrowed.get(idx);
                let inner = outer.get(0);
                black_box(*inner.get(0));
            });
            let r = bench_access(iters, &rnd, |idx| {
                let outer = borrowed.get(idx);
                let inner = outer.get(0);
                black_box(*inner.get(0));
            });
            (s, r)
        };

        // --- String: from_utf8 forces byte read ---
        let (str_seq, str_rnd) = {
            let mut c: ContainerOf<String> = Default::default();
            for i in 0..n {
                c.push(&format!("item_{i}"));
            }
            let store = encode(&c);
            let borrowed: BorrowedOf<'_, String> = decode(&store);
            let s = bench_access(iters, &seq, |idx| { black_box(borrowed.get(idx)); });
            let r = bench_access(iters, &rnd, |idx| { black_box(borrowed.get(idx)); });
            (s, r)
        };

        // --- Vec<u8> string-sized: same layout, no from_utf8 ---
        // Two variants: read first byte only, and sum all bytes (to match from_utf8 work).
        let (vstr_seq, vstr_rnd, vstr_all_seq, vstr_all_rnd) = {
            let mut c: ContainerOf<Vec<u8>> = Default::default();
            for i in 0..n {
                c.push(format!("item_{i}").as_bytes());
            }
            let store = encode(&c);
            let borrowed: BorrowedOf<'_, Vec<u8>> = decode(&store);
            let s1 = bench_access(iters, &seq, |idx| {
                let s = borrowed.get(idx);
                black_box(s.get(0));
            });
            let r1 = bench_access(iters, &rnd, |idx| {
                let s = borrowed.get(idx);
                black_box(s.get(0));
            });
            // Sum all bytes to force reading every byte (like from_utf8 does).
            let s2 = bench_access(iters, &seq, |idx| {
                let s = borrowed.get(idx);
                let mut sum: u8 = 0;
                for &b in s.into_iter() { sum = sum.wrapping_add(b); }
                black_box(sum);
            });
            let r2 = bench_access(iters, &rnd, |idx| {
                let s = borrowed.get(idx);
                let mut sum: u8 = 0;
                for &b in s.into_iter() { sum = sum.wrapping_add(b); }
                black_box(sum);
            });
            (s1, r1, s2, r2)
        };

        // --- Option<String>: RankSelect + String ---
        let (opt_seq, opt_rnd) = {
            let mut c: ContainerOf<Option<String>> = Default::default();
            for i in 0..n {
                if i % 4 == 0 {
                    c.push(&None::<String>);
                } else {
                    c.push(&Some(format!("item_{i}")));
                }
            }
            let store = encode(&c);
            let borrowed: BorrowedOf<'_, Option<String>> = decode(&store);
            let s = bench_access(iters, &seq, |idx| {
                if let Some(s) = borrowed.get(idx) { black_box(s); }
            });
            let r = bench_access(iters, &rnd, |idx| {
                if let Some(s) = borrowed.get(idx) { black_box(s); }
            });
            (s, r)
        };

        print!("  {n:>12}");
        print!(" {u64_seq:>9.1} {u64_rnd:>9.1}");
        print!(" {vu8_seq:>9.1} {vu8_rnd:>9.1}");
        print!(" {vv_seq:>9.1} {vv_rnd:>9.1}");
        print!(" {str_seq:>9.1} {str_rnd:>9.1}");
        print!(" {vstr_seq:>9.1} {vstr_rnd:>9.1}");
        print!(" {vstr_all_seq:>9.1} {vstr_all_rnd:>9.1}");
        print!(" {opt_seq:>9.1} {opt_rnd:>9.1}");
        println!();
    }
}

// ── main ─────────────────────────────────────────────────────────────

fn main() {
    let n = 10_000;
    let iters = 1_000_000;

    header("Shape 8: String vs Vec<u8> — from_utf8 cost");
    bench_string_vs_bytes(n, iters);

    header("Shape 9: Stash round-trip (validated types)");
    bench_stash(n, iters);

    header("Shape 1: u64 (primitive)");
    bench_u64(n, iters);

    header("Shape 2: (String, u64, String) — field projection");
    bench_string_tuple(n, iters);

    header("Shape 3: Vec<Vec<u64>> — nested vectors");
    bench_nested_vecs(n, iters);

    header("Shape 4: Result<(u64, String), u64> — variant match");
    bench_result(n, iters);

    header("Shape 5: Option<String> — variant match");
    bench_option_string(n, iters);

    header("Shape 6: Derived enum — variant + field projection");
    bench_derived_enum(n, iters);

    header("Shape 7: Wide tuple — many fields");
    bench_wide_tuple(n, iters);

    header("Shape 10: Cache effects — random access at scale (ns/access)");
    bench_cache_effects(iters);

    println!();
}
