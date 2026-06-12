//! Benchmarks comparing the lengths containers: `Uppers`, `Strides`, `NeverEmpty`, `MaybeEmpty`.
//!
//! Each operation runs over two length distributions: `varied` (lengths 1..=16,
//! which spills `Strides` almost immediately) and `strided` (uniform length 8,
//! which `Strides` absorbs entirely into its head).

use bencher::{benchmark_group, benchmark_main, black_box, Bencher};
use columnar::LengthsContainer;
use columnar::{NeverEmpty, MaybeEmpty, Uppers};
use columnar::primitive::offsets::Strides;

const LISTS: usize = 4096;

fn lengths_varied() -> Vec<u64> { (0..LISTS).map(|i| ((i * i) % 16 + 1) as u64).collect() }
fn lengths_strided() -> Vec<u64> { vec![8u64; LISTS] }

fn build<BC: LengthsContainer>(lengths: &[u64]) -> BC {
    let mut bounds = BC::default();
    for length in lengths.iter() {
        bounds.push(length);
    }
    bounds
}

/// Pushes all lengths, from clear.
fn _push<BC: LengthsContainer>(bencher: &mut Bencher, lengths: Vec<u64>) {
    let mut bounds = BC::default();
    bencher.iter(|| {
        bounds.clear();
        for length in lengths.iter() {
            bounds.push(length);
        }
    });
}

/// Reads `bounds(index)` for every list, in order.
fn _bounds<BC: LengthsContainer>(bencher: &mut Bencher, lengths: Vec<u64>) {
    let bounds: BC = build(&lengths);
    bencher.iter(|| {
        let mut sum = 0u64;
        for index in 0..bounds.len() {
            let (lower, upper) = bounds.bounds(index);
            sum += upper - lower;
        }
        black_box(sum)
    });
}

/// Queries `rank` at offsets stepping through the values.
fn _rank<BC: LengthsContainer>(bencher: &mut Bencher, lengths: Vec<u64>) {
    let bounds: BC = build(&lengths);
    let total = bounds.total();
    bencher.iter(|| {
        let mut sum = 0usize;
        let mut offset = 0;
        while offset < total {
            sum += bounds.rank(offset);
            offset += 7;
        }
        black_box(sum)
    });
}

/// Extends from a misaligned range of a built source, from clear.
fn _extend<BC: LengthsContainer>(bencher: &mut Bencher, lengths: Vec<u64>) {
    let source: BC = build(&lengths);
    let mut target = BC::default();
    bencher.iter(|| {
        target.clear();
        target.push(&3u64);
        target.extend_from_self(source.borrow(), 1..lengths.len());
    });
}

fn uppers_push_varied(b: &mut Bencher) { _push::<Uppers>(b, lengths_varied()); }
fn strides_push_varied(b: &mut Bencher) { _push::<Strides>(b, lengths_varied()); }
fn never_empty_push_varied(b: &mut Bencher) { _push::<NeverEmpty>(b, lengths_varied()); }
fn maybe_empty_push_varied(b: &mut Bencher) { _push::<MaybeEmpty>(b, lengths_varied()); }
fn uppers_push_strided(b: &mut Bencher) { _push::<Uppers>(b, lengths_strided()); }
fn strides_push_strided(b: &mut Bencher) { _push::<Strides>(b, lengths_strided()); }
fn never_empty_push_strided(b: &mut Bencher) { _push::<NeverEmpty>(b, lengths_strided()); }
fn maybe_empty_push_strided(b: &mut Bencher) { _push::<MaybeEmpty>(b, lengths_strided()); }

fn uppers_bounds_varied(b: &mut Bencher) { _bounds::<Uppers>(b, lengths_varied()); }
fn strides_bounds_varied(b: &mut Bencher) { _bounds::<Strides>(b, lengths_varied()); }
fn never_empty_bounds_varied(b: &mut Bencher) { _bounds::<NeverEmpty>(b, lengths_varied()); }
fn maybe_empty_bounds_varied(b: &mut Bencher) { _bounds::<MaybeEmpty>(b, lengths_varied()); }
fn uppers_bounds_strided(b: &mut Bencher) { _bounds::<Uppers>(b, lengths_strided()); }
fn strides_bounds_strided(b: &mut Bencher) { _bounds::<Strides>(b, lengths_strided()); }
fn never_empty_bounds_strided(b: &mut Bencher) { _bounds::<NeverEmpty>(b, lengths_strided()); }
fn maybe_empty_bounds_strided(b: &mut Bencher) { _bounds::<MaybeEmpty>(b, lengths_strided()); }

fn uppers_rank_varied(b: &mut Bencher) { _rank::<Uppers>(b, lengths_varied()); }
fn strides_rank_varied(b: &mut Bencher) { _rank::<Strides>(b, lengths_varied()); }
fn never_empty_rank_varied(b: &mut Bencher) { _rank::<NeverEmpty>(b, lengths_varied()); }
fn maybe_empty_rank_varied(b: &mut Bencher) { _rank::<MaybeEmpty>(b, lengths_varied()); }
fn uppers_rank_strided(b: &mut Bencher) { _rank::<Uppers>(b, lengths_strided()); }
fn strides_rank_strided(b: &mut Bencher) { _rank::<Strides>(b, lengths_strided()); }
fn never_empty_rank_strided(b: &mut Bencher) { _rank::<NeverEmpty>(b, lengths_strided()); }
fn maybe_empty_rank_strided(b: &mut Bencher) { _rank::<MaybeEmpty>(b, lengths_strided()); }

fn uppers_extend_varied(b: &mut Bencher) { _extend::<Uppers>(b, lengths_varied()); }
fn strides_extend_varied(b: &mut Bencher) { _extend::<Strides>(b, lengths_varied()); }
fn never_empty_extend_varied(b: &mut Bencher) { _extend::<NeverEmpty>(b, lengths_varied()); }
fn maybe_empty_extend_varied(b: &mut Bencher) { _extend::<MaybeEmpty>(b, lengths_varied()); }
fn uppers_extend_strided(b: &mut Bencher) { _extend::<Uppers>(b, lengths_strided()); }
fn strides_extend_strided(b: &mut Bencher) { _extend::<Strides>(b, lengths_strided()); }
fn never_empty_extend_strided(b: &mut Bencher) { _extend::<NeverEmpty>(b, lengths_strided()); }
fn maybe_empty_extend_strided(b: &mut Bencher) { _extend::<MaybeEmpty>(b, lengths_strided()); }

benchmark_group!(
    push,
    uppers_push_varied, strides_push_varied, never_empty_push_varied, maybe_empty_push_varied,
    uppers_push_strided, strides_push_strided, never_empty_push_strided, maybe_empty_push_strided,
);
benchmark_group!(
    bounds,
    uppers_bounds_varied, strides_bounds_varied, never_empty_bounds_varied, maybe_empty_bounds_varied,
    uppers_bounds_strided, strides_bounds_strided, never_empty_bounds_strided, maybe_empty_bounds_strided,
);
benchmark_group!(
    rank,
    uppers_rank_varied, strides_rank_varied, never_empty_rank_varied, maybe_empty_rank_varied,
    uppers_rank_strided, strides_rank_strided, never_empty_rank_strided, maybe_empty_rank_strided,
);
benchmark_group!(
    extend,
    uppers_extend_varied, strides_extend_varied, never_empty_extend_varied, maybe_empty_extend_varied,
    uppers_extend_strided, strides_extend_strided, never_empty_extend_strided, maybe_empty_extend_strided,
);
// Forward seeks at decreasing density, from-scratch `bounds(index)` versus a
// forward cursor: how much does stepping forward save over re-anchoring with
// a `select` per query, as jumps shrink?

const SEEK_LISTS: usize = 1 << 16;

fn lengths_seek() -> Vec<u64> { (0..SEEK_LISTS).map(|i| ((i * i) % 16 + 1) as u64).collect() }

/// From-scratch `bounds(index)` at every `gap`-th list.
fn _seek_scratch<BC: LengthsContainer>(bencher: &mut Bencher, gap: usize) {
    let bounds: BC = build(&lengths_seek());
    bencher.iter(|| {
        let mut sum = 0u64;
        let mut index = 0;
        while index < SEEK_LISTS {
            let (lower, upper) = bounds.bounds(index);
            sum += upper - lower;
            index += gap;
        }
        black_box(sum)
    });
}

/// Cursor `seek(index)` at every `gap`-th list.
fn _seek_cursor_never(bencher: &mut Bencher, gap: usize) {
    let bounds: NeverEmpty = build(&lengths_seek());
    bencher.iter(|| {
        let mut sum = 0u64;
        let mut cursor = bounds.cursor();
        let mut index = 0;
        while index < SEEK_LISTS {
            let (lower, upper) = cursor.seek(index);
            sum += upper - lower;
            index += gap;
        }
        black_box(sum)
    });
}
fn _seek_cursor_maybe(bencher: &mut Bencher, gap: usize) {
    let bounds: MaybeEmpty = build(&lengths_seek());
    bencher.iter(|| {
        let mut sum = 0u64;
        let mut cursor = bounds.cursor();
        let mut index = 0;
        while index < SEEK_LISTS {
            let (lower, upper) = cursor.seek(index);
            sum += upper - lower;
            index += gap;
        }
        black_box(sum)
    });
}

fn uppers_scratch_g1(b: &mut Bencher) { _seek_scratch::<Uppers>(b, 1); }
fn uppers_scratch_g4(b: &mut Bencher) { _seek_scratch::<Uppers>(b, 4); }
fn uppers_scratch_g16(b: &mut Bencher) { _seek_scratch::<Uppers>(b, 16); }
fn uppers_scratch_g64(b: &mut Bencher) { _seek_scratch::<Uppers>(b, 64); }
fn uppers_scratch_g256(b: &mut Bencher) { _seek_scratch::<Uppers>(b, 256); }
fn uppers_scratch_g1024(b: &mut Bencher) { _seek_scratch::<Uppers>(b, 1024); }

fn never_empty_scratch_g1(b: &mut Bencher) { _seek_scratch::<NeverEmpty>(b, 1); }
fn never_empty_scratch_g4(b: &mut Bencher) { _seek_scratch::<NeverEmpty>(b, 4); }
fn never_empty_scratch_g16(b: &mut Bencher) { _seek_scratch::<NeverEmpty>(b, 16); }
fn never_empty_scratch_g64(b: &mut Bencher) { _seek_scratch::<NeverEmpty>(b, 64); }
fn never_empty_scratch_g256(b: &mut Bencher) { _seek_scratch::<NeverEmpty>(b, 256); }
fn never_empty_scratch_g1024(b: &mut Bencher) { _seek_scratch::<NeverEmpty>(b, 1024); }

fn never_empty_cursor_g1(b: &mut Bencher) { _seek_cursor_never(b, 1); }
fn never_empty_cursor_g4(b: &mut Bencher) { _seek_cursor_never(b, 4); }
fn never_empty_cursor_g16(b: &mut Bencher) { _seek_cursor_never(b, 16); }
fn never_empty_cursor_g64(b: &mut Bencher) { _seek_cursor_never(b, 64); }
fn never_empty_cursor_g256(b: &mut Bencher) { _seek_cursor_never(b, 256); }
fn never_empty_cursor_g1024(b: &mut Bencher) { _seek_cursor_never(b, 1024); }

fn maybe_empty_scratch_g1(b: &mut Bencher) { _seek_scratch::<MaybeEmpty>(b, 1); }
fn maybe_empty_scratch_g4(b: &mut Bencher) { _seek_scratch::<MaybeEmpty>(b, 4); }
fn maybe_empty_scratch_g16(b: &mut Bencher) { _seek_scratch::<MaybeEmpty>(b, 16); }
fn maybe_empty_scratch_g64(b: &mut Bencher) { _seek_scratch::<MaybeEmpty>(b, 64); }
fn maybe_empty_scratch_g256(b: &mut Bencher) { _seek_scratch::<MaybeEmpty>(b, 256); }
fn maybe_empty_scratch_g1024(b: &mut Bencher) { _seek_scratch::<MaybeEmpty>(b, 1024); }

fn maybe_empty_cursor_g1(b: &mut Bencher) { _seek_cursor_maybe(b, 1); }
fn maybe_empty_cursor_g4(b: &mut Bencher) { _seek_cursor_maybe(b, 4); }
fn maybe_empty_cursor_g16(b: &mut Bencher) { _seek_cursor_maybe(b, 16); }
fn maybe_empty_cursor_g64(b: &mut Bencher) { _seek_cursor_maybe(b, 64); }
fn maybe_empty_cursor_g256(b: &mut Bencher) { _seek_cursor_maybe(b, 256); }
fn maybe_empty_cursor_g1024(b: &mut Bencher) { _seek_cursor_maybe(b, 1024); }

benchmark_group!(
    seek,
    uppers_scratch_g1, uppers_scratch_g4, uppers_scratch_g16, uppers_scratch_g64, uppers_scratch_g256, uppers_scratch_g1024,
    never_empty_scratch_g1, never_empty_scratch_g4, never_empty_scratch_g16, never_empty_scratch_g64, never_empty_scratch_g256, never_empty_scratch_g1024,
    never_empty_cursor_g1, never_empty_cursor_g4, never_empty_cursor_g16, never_empty_cursor_g64, never_empty_cursor_g256, never_empty_cursor_g1024,
    maybe_empty_scratch_g1, maybe_empty_scratch_g4, maybe_empty_scratch_g16, maybe_empty_scratch_g64, maybe_empty_scratch_g256, maybe_empty_scratch_g1024,
    maybe_empty_cursor_g1, maybe_empty_cursor_g4, maybe_empty_cursor_g16, maybe_empty_cursor_g64, maybe_empty_cursor_g256, maybe_empty_cursor_g1024,
);
benchmark_main!(push, bounds, rank, extend, seek);
