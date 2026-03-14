//! Assembly inspection: O(1) field access vs O(k) tuple construction.
//!
//! We compare three approaches:
//! 1. OLD: construct all k fields via from_bytes, access field j  — O(k)
//! 2. NEW (eager): construct all k fields via from_u64s, access field j — O(k) but smaller constant
//! 3. NEW (direct): decode ONLY field j from store, skip all others — O(1)

use columnar::*;
use columnar::common::Index;
use columnar::bytes::indexed;

// Helper: decode a single field directly from the store as &[u64].
#[inline(always)]
fn decode_one(store: &[u64], field: usize) -> &[u64] {
    let slices = store[0] as usize / 8 - 1;
    let index = &store[..slices + 1];
    let last = index[slices] as usize;
    let last_w = (last + 7) / 8;
    let words = &store[..last_w];
    let upper = (index[field + 1] as usize).min(last);
    let lower = (((index[field] as usize) + 7) & !7).min(upper);
    let upper_w = ((upper + 7) / 8).min(words.len());
    let lower_w = (lower / 8).min(upper_w);
    &words[lower_w..upper_w]
}

// ================================================================
// OLD PATH: from_bytes (construct all k fields, access field j)
// ================================================================

#[no_mangle] pub fn old_3_f0(store: &[u64], i: usize) -> u64 {
    type T<'a> = (&'a [u64], &'a [u64], &'a [u64]);
    T::from_bytes(&mut indexed::decode(store)).0[i]
}
#[no_mangle] pub fn old_3_flast(store: &[u64], i: usize) -> u64 {
    type T<'a> = (&'a [u64], &'a [u64], &'a [u64]);
    T::from_bytes(&mut indexed::decode(store)).2[i]
}
#[no_mangle] pub fn old_5_f0(store: &[u64], i: usize) -> u64 {
    type T<'a> = (&'a [u64], &'a [u64], &'a [u64], &'a [u64], &'a [u64]);
    T::from_bytes(&mut indexed::decode(store)).0[i]
}
#[no_mangle] pub fn old_5_flast(store: &[u64], i: usize) -> u64 {
    type T<'a> = (&'a [u64], &'a [u64], &'a [u64], &'a [u64], &'a [u64]);
    T::from_bytes(&mut indexed::decode(store)).4[i]
}
#[no_mangle] pub fn old_8_f0(store: &[u64], i: usize) -> u64 {
    type T<'a> = (&'a [u64], &'a [u64], &'a [u64], &'a [u64],
                   &'a [u64], &'a [u64], &'a [u64], &'a [u64]);
    T::from_bytes(&mut indexed::decode(store)).0[i]
}
#[no_mangle] pub fn old_8_flast(store: &[u64], i: usize) -> u64 {
    type T<'a> = (&'a [u64], &'a [u64], &'a [u64], &'a [u64],
                   &'a [u64], &'a [u64], &'a [u64], &'a [u64]);
    T::from_bytes(&mut indexed::decode(store)).7[i]
}

// ================================================================
// NEW EAGER: from_u64s (construct all k fields, access field j)
// ================================================================

#[no_mangle] pub fn eager_3_f0(store: &[u64], i: usize) -> u64 {
    type T<'a> = (&'a [u64], &'a [u64], &'a [u64]);
    T::from_u64s(&mut indexed::decode_u64s(store)).0[i]
}
#[no_mangle] pub fn eager_3_flast(store: &[u64], i: usize) -> u64 {
    type T<'a> = (&'a [u64], &'a [u64], &'a [u64]);
    T::from_u64s(&mut indexed::decode_u64s(store)).2[i]
}
#[no_mangle] pub fn eager_5_f0(store: &[u64], i: usize) -> u64 {
    type T<'a> = (&'a [u64], &'a [u64], &'a [u64], &'a [u64], &'a [u64]);
    T::from_u64s(&mut indexed::decode_u64s(store)).0[i]
}
#[no_mangle] pub fn eager_5_flast(store: &[u64], i: usize) -> u64 {
    type T<'a> = (&'a [u64], &'a [u64], &'a [u64], &'a [u64], &'a [u64]);
    T::from_u64s(&mut indexed::decode_u64s(store)).4[i]
}
#[no_mangle] pub fn eager_8_f0(store: &[u64], i: usize) -> u64 {
    type T<'a> = (&'a [u64], &'a [u64], &'a [u64], &'a [u64],
                   &'a [u64], &'a [u64], &'a [u64], &'a [u64]);
    T::from_u64s(&mut indexed::decode_u64s(store)).0[i]
}
#[no_mangle] pub fn eager_8_flast(store: &[u64], i: usize) -> u64 {
    type T<'a> = (&'a [u64], &'a [u64], &'a [u64], &'a [u64],
                   &'a [u64], &'a [u64], &'a [u64], &'a [u64]);
    T::from_u64s(&mut indexed::decode_u64s(store)).7[i]
}

// ================================================================
// NEW DIRECT: decode ONLY the one field needed — should be O(1)
// ================================================================

#[no_mangle] pub fn direct_3_f0(store: &[u64], i: usize) -> u64 {
    decode_one(store, 0)[i]
}
#[no_mangle] pub fn direct_3_flast(store: &[u64], i: usize) -> u64 {
    decode_one(store, 2)[i]
}
#[no_mangle] pub fn direct_5_f0(store: &[u64], i: usize) -> u64 {
    decode_one(store, 0)[i]
}
#[no_mangle] pub fn direct_5_flast(store: &[u64], i: usize) -> u64 {
    decode_one(store, 4)[i]
}
#[no_mangle] pub fn direct_8_f0(store: &[u64], i: usize) -> u64 {
    decode_one(store, 0)[i]
}
#[no_mangle] pub fn direct_8_flast(store: &[u64], i: usize) -> u64 {
    decode_one(store, 7)[i]
}

// ================================================================
// PURE: hand-written from_u64s that is provably panic-free
// Just returns the word slice directly — no cast, no trim, no unwrap.
// ================================================================

#[inline(always)]
fn pure_from_u64s_one<'a>(words: &mut impl Iterator<Item=(&'a [u64], u8)>) -> &'a [u64] {
    match words.next() {
        Some((w, _)) => w,
        None => &[],
    }
}

#[no_mangle] pub fn pure_3_f0(store: &[u64], i: usize) -> u64 {
    let mut w = indexed::decode_u64s(store);
    let f0 = pure_from_u64s_one(&mut w);
    let _f1 = pure_from_u64s_one(&mut w);
    let _f2 = pure_from_u64s_one(&mut w);
    f0[i]
}

#[no_mangle] pub fn pure_8_f0(store: &[u64], i: usize) -> u64 {
    let mut w = indexed::decode_u64s(store);
    let f0 = pure_from_u64s_one(&mut w);
    let _f1 = pure_from_u64s_one(&mut w);
    let _f2 = pure_from_u64s_one(&mut w);
    let _f3 = pure_from_u64s_one(&mut w);
    let _f4 = pure_from_u64s_one(&mut w);
    let _f5 = pure_from_u64s_one(&mut w);
    let _f6 = pure_from_u64s_one(&mut w);
    let _f7 = pure_from_u64s_one(&mut w);
    f0[i]
}

#[no_mangle] pub fn pure_8_flast(store: &[u64], i: usize) -> u64 {
    let mut w = indexed::decode_u64s(store);
    let _f0 = pure_from_u64s_one(&mut w);
    let _f1 = pure_from_u64s_one(&mut w);
    let _f2 = pure_from_u64s_one(&mut w);
    let _f3 = pure_from_u64s_one(&mut w);
    let _f4 = pure_from_u64s_one(&mut w);
    let _f5 = pure_from_u64s_one(&mut w);
    let _f6 = pure_from_u64s_one(&mut w);
    let f7 = pure_from_u64s_one(&mut w);
    f7[i]
}

fn main() {
    let mut store = vec![0u64; 100];
    store[0] = 32; store[1] = 32; store[2] = 32; store[3] = 32;
    println!("{}", std::hint::black_box(direct_3_f0(std::hint::black_box(&store), 0)));
}
