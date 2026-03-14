//! Assembly inspection for decode paths.
//!
//! Compares three approaches to accessing a single field of a k-tuple
//! stored in Indexed-encoded `&[u64]` data:
//!
//! 1. `from_bytes` + `decode`: constructs all k fields, O(k)
//! 2. `from_u64s` + `decode_u64s`: non-panicking, LLVM eliminates unused fields, O(1) in k
//! 3. `decode_field` (random access): decodes one field directly, O(1) in k and j
//!
//! Build with: `cargo rustc --example decode_asm --release -- --emit asm`

use columnar::*;
use columnar::bytes::indexed;

// ================================================================
// from_bytes path (construct all k fields, access field j)
// ================================================================

#[no_mangle] pub fn bytes_3_f0(store: &[u64], i: usize) -> u64 {
    type T<'a> = (&'a [u64], &'a [u64], &'a [u64]);
    T::from_bytes(&mut indexed::decode(store)).0[i]
}
#[no_mangle] pub fn bytes_3_flast(store: &[u64], i: usize) -> u64 {
    type T<'a> = (&'a [u64], &'a [u64], &'a [u64]);
    T::from_bytes(&mut indexed::decode(store)).2[i]
}
#[no_mangle] pub fn bytes_8_f0(store: &[u64], i: usize) -> u64 {
    type T<'a> = (&'a [u64], &'a [u64], &'a [u64], &'a [u64],
                   &'a [u64], &'a [u64], &'a [u64], &'a [u64]);
    T::from_bytes(&mut indexed::decode(store)).0[i]
}
#[no_mangle] pub fn bytes_8_flast(store: &[u64], i: usize) -> u64 {
    type T<'a> = (&'a [u64], &'a [u64], &'a [u64], &'a [u64],
                   &'a [u64], &'a [u64], &'a [u64], &'a [u64]);
    T::from_bytes(&mut indexed::decode(store)).7[i]
}

// ================================================================
// from_u64s path (non-panicking, LLVM eliminates unused fields)
// ================================================================

#[no_mangle] pub fn u64s_3_f0(store: &[u64], i: usize) -> u64 {
    type T<'a> = (&'a [u64], &'a [u64], &'a [u64]);
    T::from_u64s(&mut indexed::decode_u64s(store)).0[i]
}
#[no_mangle] pub fn u64s_3_flast(store: &[u64], i: usize) -> u64 {
    type T<'a> = (&'a [u64], &'a [u64], &'a [u64]);
    T::from_u64s(&mut indexed::decode_u64s(store)).2[i]
}
#[no_mangle] pub fn u64s_8_f0(store: &[u64], i: usize) -> u64 {
    type T<'a> = (&'a [u64], &'a [u64], &'a [u64], &'a [u64],
                   &'a [u64], &'a [u64], &'a [u64], &'a [u64]);
    T::from_u64s(&mut indexed::decode_u64s(store)).0[i]
}
#[no_mangle] pub fn u64s_8_flast(store: &[u64], i: usize) -> u64 {
    type T<'a> = (&'a [u64], &'a [u64], &'a [u64], &'a [u64],
                   &'a [u64], &'a [u64], &'a [u64], &'a [u64]);
    T::from_u64s(&mut indexed::decode_u64s(store)).7[i]
}

// ================================================================
// Random access (decode one field directly, O(1) in both k and j)
// ================================================================

/// Decode field `k` directly from store as `(&[u64], u8)`.
/// Each call is independent — no iterator state.
#[inline(always)]
fn decode_field(store: &[u64], k: usize) -> (&[u64], u8) {
    let slices = store[0] as usize / 8 - 1;
    let index = &store[..slices + 1];
    let last = *index.last().unwrap_or(&0) as usize;
    let last_w = (last + 7) / 8;
    let words = &store[..last_w];
    let upper = (*index.get(k + 1).unwrap_or(&0) as usize).min(last);
    let lower = (((*index.get(k).unwrap_or(&0) as usize) + 7) & !7).min(upper);
    let upper_w = ((upper + 7) / 8).min(words.len());
    let lower_w = (lower / 8).min(upper_w);
    let tail = (upper % 8) as u8;
    (&words[lower_w..upper_w], tail)
}

#[no_mangle] pub fn field_3_f0(store: &[u64], i: usize) -> u64 {
    decode_field(store, 0).0[i]
}
#[no_mangle] pub fn field_3_flast(store: &[u64], i: usize) -> u64 {
    decode_field(store, 2).0[i]
}
#[no_mangle] pub fn field_8_f0(store: &[u64], i: usize) -> u64 {
    decode_field(store, 0).0[i]
}
#[no_mangle] pub fn field_8_flast(store: &[u64], i: usize) -> u64 {
    decode_field(store, 7).0[i]
}

fn main() {
    let mut store = vec![0u64; 100];
    store[0] = 32; store[1] = 32; store[2] = 32; store[3] = 32;
    println!("{}", std::hint::black_box(field_3_f0(std::hint::black_box(&store), 0)));
}
