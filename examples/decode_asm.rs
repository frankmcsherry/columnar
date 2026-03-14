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

// ================================================================
// DecodedStore path (random access, no iterator)
// ================================================================

#[no_mangle] pub fn store_3_f0(store: &[u64], i: usize) -> u64 {
    type T<'a> = (&'a [u64], &'a [u64], &'a [u64]);
    let ds = columnar::bytes::indexed::DecodedStore::new(store);
    T::from_store(&ds, &mut 0).0[i]
}
#[no_mangle] pub fn store_3_flast(store: &[u64], i: usize) -> u64 {
    type T<'a> = (&'a [u64], &'a [u64], &'a [u64]);
    let ds = columnar::bytes::indexed::DecodedStore::new(store);
    T::from_store(&ds, &mut 0).2[i]
}
#[no_mangle] pub fn store_8_f0(store: &[u64], i: usize) -> u64 {
    type T<'a> = (&'a [u64], &'a [u64], &'a [u64], &'a [u64],
                   &'a [u64], &'a [u64], &'a [u64], &'a [u64]);
    let ds = columnar::bytes::indexed::DecodedStore::new(store);
    T::from_store(&ds, &mut 0).0[i]
}
#[no_mangle] pub fn store_8_flast(store: &[u64], i: usize) -> u64 {
    type T<'a> = (&'a [u64], &'a [u64], &'a [u64], &'a [u64],
                   &'a [u64], &'a [u64], &'a [u64], &'a [u64]);
    let ds = columnar::bytes::indexed::DecodedStore::new(store);
    T::from_store(&ds, &mut 0).7[i]
}

// ================================================================
// Scaling test: k=16 via nested (8, 8) tuple
// ================================================================

type T8<'a> = (&'a [u64], &'a [u64], &'a [u64], &'a [u64],
               &'a [u64], &'a [u64], &'a [u64], &'a [u64]);

// (T8, T8) = 16 fields. Field 0 is .0.0, field 7 is .0.7, field 8 is .1.0, field 15 is .1.7
type T16<'a> = (T8<'a>, T8<'a>);

#[no_mangle] pub fn store_16_f0(store: &[u64], i: usize) -> u64 {
    let ds = columnar::bytes::indexed::DecodedStore::new(store);
    T16::from_store(&ds, &mut 0).0.0[i]
}
#[no_mangle] pub fn store_16_f7(store: &[u64], i: usize) -> u64 {
    let ds = columnar::bytes::indexed::DecodedStore::new(store);
    T16::from_store(&ds, &mut 0).0.7[i]
}
#[no_mangle] pub fn store_16_f8(store: &[u64], i: usize) -> u64 {
    let ds = columnar::bytes::indexed::DecodedStore::new(store);
    T16::from_store(&ds, &mut 0).1.0[i]
}
#[no_mangle] pub fn store_16_flast(store: &[u64], i: usize) -> u64 {
    let ds = columnar::bytes::indexed::DecodedStore::new(store);
    T16::from_store(&ds, &mut 0).1.7[i]
}

// For comparison: iterator path at k=16
#[no_mangle] pub fn u64s_16_f0(store: &[u64], i: usize) -> u64 {
    T16::from_u64s(&mut columnar::bytes::indexed::decode_u64s(store)).0.0[i]
}
#[no_mangle] pub fn u64s_16_flast(store: &[u64], i: usize) -> u64 {
    T16::from_u64s(&mut columnar::bytes::indexed::decode_u64s(store)).1.7[i]
}

// ================================================================
// Complex types: do unused complex fields get eliminated?
// ================================================================

// Access field 0 (u64) when field 1 is a Result
#[no_mangle] pub fn store_u64_result_f0(store: &[u64], i: usize) -> u64 {
    type T<'a> = (&'a [u64], columnar::Results<&'a [u64], &'a [u64], &'a [u64], &'a [u64], &'a u64>);
    let ds = columnar::bytes::indexed::DecodedStore::new(store);
    *T::from_store(&ds, &mut 0).0.get(i).unwrap()
}

// Access field 0 (u64) when field 1 is a Vec<u64>
#[no_mangle] pub fn store_u64_vec_f0(store: &[u64], i: usize) -> u64 {
    type T<'a> = (&'a [u64], columnar::Vecs<&'a [u64], &'a [u64]>);
    let ds = columnar::bytes::indexed::DecodedStore::new(store);
    *T::from_store(&ds, &mut 0).0.get(i).unwrap()
}

// Access field 0 (u64) when fields 1,2 are String and Vec<u32>
#[no_mangle] pub fn store_u64_string_vec_f0(store: &[u64], i: usize) -> u64 {
    type T<'a> = (&'a [u64], columnar::Strings<&'a [u64], &'a [u8]>, columnar::Vecs<&'a [u32], &'a [u64]>);
    let ds = columnar::bytes::indexed::DecodedStore::new(store);
    let (a, _b, _c) = T::from_store(&ds, &mut 0);
    a[i]
}

// Access field 2 (Vec<u32>) skipping u64 and String
#[no_mangle] pub fn store_u64_string_vec_flast(store: &[u64], i: usize) -> u32 {
    type T<'a> = (&'a [u64], columnar::Strings<&'a [u64], &'a [u8]>, columnar::Vecs<&'a [u32], &'a [u64]>);
    let ds = columnar::bytes::indexed::DecodedStore::new(store);
    let (_a, _b, c) = T::from_store(&ds, &mut 0);
    c.bounds[i] as u32
}

// Deeply nested: (u64, (u64, (u64, u64))) — access the innermost u64
#[no_mangle] pub fn store_nested_inner(store: &[u64], i: usize) -> u64 {
    type T<'a> = (&'a [u64], (&'a [u64], (&'a [u64], &'a [u64])));
    let ds = columnar::bytes::indexed::DecodedStore::new(store);
    T::from_store(&ds, &mut 0).1.1.1[i]
}

// Wide + deep: ((u64, u64, u64, u64), (u64, u64, u64, u64)) — access .1.3
#[no_mangle] pub fn store_wide_deep(store: &[u64], i: usize) -> u64 {
    type T<'a> = ((&'a [u64], &'a [u64], &'a [u64], &'a [u64]),
                  (&'a [u64], &'a [u64], &'a [u64], &'a [u64]));
    let ds = columnar::bytes::indexed::DecodedStore::new(store);
    T::from_store(&ds, &mut 0).1.3[i]
}

// For comparison: same types via from_u64s (iterator)
#[no_mangle] pub fn u64s_u64_string_vec_f0(store: &[u64], i: usize) -> u64 {
    type T<'a> = (&'a [u64], columnar::Strings<&'a [u64], &'a [u8]>, columnar::Vecs<&'a [u32], &'a [u64]>);
    let (a, _b, _c) = T::from_u64s(&mut columnar::bytes::indexed::decode_u64s(store));
    a[i]
}

#[no_mangle] pub fn u64s_nested_inner(store: &[u64], i: usize) -> u64 {
    type T<'a> = (&'a [u64], (&'a [u64], (&'a [u64], &'a [u64])));
    T::from_u64s(&mut columnar::bytes::indexed::decode_u64s(store)).1.1.1[i]
}

fn main() {
    let mut store = vec![0u64; 100];
    store[0] = 32; store[1] = 32; store[2] = 32; store[3] = 32;
    println!("{}", std::hint::black_box(store_3_f0(std::hint::black_box(&store), 0)));
}
