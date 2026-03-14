//! Benchmarks for Indexed::decode improvements.
//!
//! Measures decode + field access from encoded `[u64]` data,
//! exercising both simple and complex types, and separating
//! decode overhead from field access overhead.

use columnar::*;
use columnar::common::{Push, Index};
use columnar::bytes::Indexed;

use std::hint::black_box;
use std::time::Instant;

/// Time a closure over `iters` iterations, return ns per iteration.
fn bench_ns<F: FnMut()>(iters: u64, mut f: F) -> f64 {
    // Warmup
    for _ in 0..iters.min(1000) { f(); }
    let start = Instant::now();
    for _ in 0..iters { f(); }
    let elapsed = start.elapsed();
    elapsed.as_nanos() as f64 / iters as f64
}

/// Encode a container into Indexed format, returning the `[u64]` store.
fn encode_indexed<C: Borrow>(container: &C) -> Vec<u64>
where
    for<'a> C::Borrowed<'a>: AsBytes<'a>,
{
    let mut store = Vec::new();
    Indexed::encode(&mut store, &container.borrow());
    store
}

// ============================================================
// Experiment 1: Simple type (u64) — decode + access single field
// ============================================================

fn exp1_u64(n: usize, iters: u64) {
    let mut container: ContainerOf<u64> = Default::default();
    for i in 0..n as u64 { container.push(i); }
    let store = encode_indexed(&container);

    // Measure: decode + access element n/2
    let idx = n / 2;
    let ns_decode_access = bench_ns(iters, || {
        let mut slices = Indexed::decode(&store);
        let borrowed = <&[u64]>::from_bytes(&mut slices);
        black_box(borrowed[idx]);
    });

    // Measure: decode once, repeatedly access
    let mut slices_once = Indexed::decode(&store);
    let borrowed_once = <&[u64]>::from_bytes(&mut slices_once);
    let ns_access_only = bench_ns(iters, || {
        black_box(borrowed_once[idx]);
    });

    println!("  u64 (n={n}): decode+access = {ns_decode_access:.1} ns, access only = {ns_access_only:.1} ns, decode overhead = {:.1} ns",
        ns_decode_access - ns_access_only);
}

// ============================================================
// Experiment 2: Vec<u8> — decode + access, harder type
// ============================================================

fn exp2_vec_u8(n: usize, iters: u64) {
    let mut container: ContainerOf<Vec<u8>> = Default::default();
    for i in 0..n {
        container.push(vec![i as u8; (i % 32) + 1]);
    }
    let store = encode_indexed(&container);

    let idx = n / 2;

    // Decode + access
    let ns_decode_access = bench_ns(iters, || {
        let mut slices = Indexed::decode(&store);
        type B<'a> = <ContainerOf<Vec<u8>> as Borrow>::Borrowed<'a>;
        let borrowed = B::from_bytes(&mut slices);
        black_box(borrowed.get(idx));
    });

    // Access only
    let slices_vec: Vec<&[u8]> = Indexed::decode(&store).collect();
    let mut slices_iter = slices_vec.iter().copied();
    type B2<'a> = <ContainerOf<Vec<u8>> as Borrow>::Borrowed<'a>;
    let borrowed_once = B2::from_bytes(&mut slices_iter);
    let ns_access_only = bench_ns(iters, || {
        black_box(borrowed_once.get(idx));
    });

    println!("  Vec<u8> (n={n}): decode+access = {ns_decode_access:.1} ns, access only = {ns_access_only:.1} ns, decode overhead = {:.1} ns",
        ns_decode_access - ns_access_only);
}

// ============================================================
// Experiment 3: Stash end-to-end — from raw bytes to typed access
// ============================================================

fn exp3_stash_u64(n: usize, iters: u64) {
    use columnar::bytes::stash::Stash;

    let mut container: ContainerOf<u64> = Default::default();
    for i in 0..n as u64 { container.push(i); }

    // Serialize to bytes (as Stash would receive them)
    let mut bytes_buf: Vec<u8> = Vec::new();
    Indexed::write(&mut bytes_buf, &container.borrow()).unwrap();

    let stash: Stash<ContainerOf<u64>, Vec<u8>> = Stash::from(bytes_buf);

    let idx = n / 2;
    let ns = bench_ns(iters, || {
        let borrowed = stash.borrow();
        black_box(borrowed.get(idx));
    });
    println!("  Stash<u64> (n={n}): borrow+access = {ns:.1} ns");
}

fn exp3_stash_vec_u8(n: usize, iters: u64) {
    use columnar::bytes::stash::Stash;

    let mut container: ContainerOf<Vec<u8>> = Default::default();
    for i in 0..n {
        container.push(vec![i as u8; (i % 32) + 1]);
    }

    let mut bytes_buf: Vec<u8> = Vec::new();
    Indexed::write(&mut bytes_buf, &container.borrow()).unwrap();

    let stash: Stash<ContainerOf<Vec<u8>>, Vec<u8>> = Stash::from(bytes_buf);

    let idx = n / 2;
    let ns = bench_ns(iters, || {
        let borrowed = stash.borrow();
        black_box(borrowed.get(idx));
    });
    println!("  Stash<Vec<u8>> (n={n}): borrow+access = {ns:.1} ns");
}

// ============================================================
// Experiment 4: Scaling with tuple width — access the LAST field
// of (u64, u64, ..., u64) tuples of increasing width.
// This reveals whether skipping fields has cost.
// ============================================================

// We need concrete types for each tuple width.
// We'll do 1, 2, 3, 5, 8 fields.

fn exp4_tuple1(n: usize, iters: u64) {
    let mut c: ContainerOf<(u64,)> = Default::default();
    for i in 0..n as u64 { c.push(&(i,)); }
    let store = encode_indexed(&c);
    let idx = n / 2;

    let ns = bench_ns(iters, || {
        let mut slices = Indexed::decode(&store);
        type B<'a> = <ContainerOf<(u64,)> as Borrow>::Borrowed<'a>;
        let b = B::from_bytes(&mut slices);
        black_box(b.get(idx));
    });
    println!("  (u64,) x1 — last field: decode+access = {ns:.1} ns");
}

fn exp4_tuple2(n: usize, iters: u64) {
    let mut c: ContainerOf<(u64, u64)> = Default::default();
    for i in 0..n as u64 { c.push(&(i, i+1)); }
    let store = encode_indexed(&c);
    let idx = n / 2;

    let ns = bench_ns(iters, || {
        let mut slices = Indexed::decode(&store);
        type B<'a> = <ContainerOf<(u64, u64)> as Borrow>::Borrowed<'a>;
        let b = B::from_bytes(&mut slices);
        let (_a, b_val) = b.get(idx);
        black_box(b_val);
    });
    println!("  (u64, u64) x2 — last field: decode+access = {ns:.1} ns");
}

fn exp4_tuple3(n: usize, iters: u64) {
    let mut c: ContainerOf<(u64, u64, u64)> = Default::default();
    for i in 0..n as u64 { c.push(&(i, i+1, i+2)); }
    let store = encode_indexed(&c);
    let idx = n / 2;

    let ns = bench_ns(iters, || {
        let mut slices = Indexed::decode(&store);
        type B<'a> = <ContainerOf<(u64, u64, u64)> as Borrow>::Borrowed<'a>;
        let b = B::from_bytes(&mut slices);
        let (_a, _b, c_val) = b.get(idx);
        black_box(c_val);
    });
    println!("  (u64, u64, u64) x3 — last field: decode+access = {ns:.1} ns");
}

fn exp4_tuple5(n: usize, iters: u64) {
    let mut c: ContainerOf<(u64, u64, u64, u64, u64)> = Default::default();
    for i in 0..n as u64 { c.push(&(i, i+1, i+2, i+3, i+4)); }
    let store = encode_indexed(&c);
    let idx = n / 2;

    let ns = bench_ns(iters, || {
        let mut slices = Indexed::decode(&store);
        type B<'a> = <ContainerOf<(u64, u64, u64, u64, u64)> as Borrow>::Borrowed<'a>;
        let b = B::from_bytes(&mut slices);
        let (_a, _b, _c, _d, e_val) = b.get(idx);
        black_box(e_val);
    });
    println!("  (u64 x5) — last field: decode+access = {ns:.1} ns");
}

fn exp4_tuple8(n: usize, iters: u64) {
    let mut c: ContainerOf<(u64, u64, u64, u64, u64, u64, u64, u64)> = Default::default();
    for i in 0..n as u64 { c.push(&(i, i+1, i+2, i+3, i+4, i+5, i+6, i+7)); }
    let store = encode_indexed(&c);
    let idx = n / 2;

    let ns = bench_ns(iters, || {
        let mut slices = Indexed::decode(&store);
        type B<'a> = <ContainerOf<(u64, u64, u64, u64, u64, u64, u64, u64)> as Borrow>::Borrowed<'a>;
        let b = B::from_bytes(&mut slices);
        let (_a, _b, _c, _d, _e, _f, _g, h_val) = b.get(idx);
        black_box(h_val);
    });
    println!("  (u64 x8) — last field: decode+access = {ns:.1} ns");
}

// ============================================================
// Experiment 5: Decode iterator overhead — just iterate decode,
// don't construct anything. How much does decode itself cost?
// ============================================================

fn exp5_decode_only(n: usize, num_slices: usize, iters: u64) {
    // Create a type with `num_slices` byte slices.
    // We'll use a tuple of u64s, each contributing 1 slice.
    // But we need to be generic... let's just manually encode.
    // Actually, let's use the tuple types and just measure decode.

    // For simplicity, encode a (u64,) repeated, so we get `num_slices` slices.
    // Actually let's just encode directly to get the right number of slices.

    // Build a store with `num_slices` byte regions.
    let mut store: Vec<u64> = Vec::new();
    // First: write (num_slices + 1) offsets.
    let offsets = num_slices + 1;
    let offsets_end = (offsets * 8) as u64;
    store.push(offsets_end);
    let mut pos = offsets_end;
    for _ in 0..num_slices {
        let len = (n * 8) as u64; // n u64s per slice
        pos += len;
        store.push(pos);
    }
    // Then write the actual data
    for s in 0..num_slices {
        for i in 0..n {
            store.push((s * n + i) as u64);
        }
    }

    let ns = bench_ns(iters, || {
        let slices = Indexed::decode(&store);
        for slice in slices {
            black_box(slice);
        }
    });
    println!("  decode only ({num_slices} slices, {n} items each): {ns:.1} ns");
}

fn main() {
    let n = 1000;
    let iters = 1_000_000;

    println!("=== Experiment 1: Simple u64 decode + access ===");
    exp1_u64(n, iters);

    println!("\n=== Experiment 2: Vec<u8> decode + access ===");
    exp2_vec_u8(n, iters);

    println!("\n=== Experiment 3: Stash end-to-end ===");
    exp3_stash_u64(n, iters);
    exp3_stash_vec_u8(n, iters);

    println!("\n=== Experiment 4: Tuple width scaling (access last field) ===");
    exp4_tuple1(n, iters);
    exp4_tuple2(n, iters);
    exp4_tuple3(n, iters);
    exp4_tuple5(n, iters);
    exp4_tuple8(n, iters);

    println!("\n=== Experiment 5: Decode iterator overhead ===");
    exp5_decode_only(n, 1, iters);
    exp5_decode_only(n, 3, iters);
    exp5_decode_only(n, 5, iters);
    exp5_decode_only(n, 8, iters);
    exp5_decode_only(n, 16, iters);
}
