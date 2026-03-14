//! Assembly inspection for decode paths.
//!
//! Compares two approaches to accessing a single field of a k-tuple
//! stored in indexed-encoded `&[u64]` data:
//!
//! 1. `from_bytes` + `decode`: constructs all k fields, O(k)
//! 2. `from_store` + `DecodedStore`: random access, LLVM eliminates unused fields, O(1)
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
// DecodedStore path (random access, O(1) in both k and field position)
// ================================================================

#[no_mangle] pub fn store_3_f0(store: &[u64], i: usize) -> u64 {
    type T<'a> = (&'a [u64], &'a [u64], &'a [u64]);
    let ds = indexed::DecodedStore::new(store);
    T::from_store(&ds, &mut 0).0[i]
}
#[no_mangle] pub fn store_3_flast(store: &[u64], i: usize) -> u64 {
    type T<'a> = (&'a [u64], &'a [u64], &'a [u64]);
    let ds = indexed::DecodedStore::new(store);
    T::from_store(&ds, &mut 0).2[i]
}
#[no_mangle] pub fn store_8_f0(store: &[u64], i: usize) -> u64 {
    type T<'a> = (&'a [u64], &'a [u64], &'a [u64], &'a [u64],
                   &'a [u64], &'a [u64], &'a [u64], &'a [u64]);
    let ds = indexed::DecodedStore::new(store);
    T::from_store(&ds, &mut 0).0[i]
}
#[no_mangle] pub fn store_8_flast(store: &[u64], i: usize) -> u64 {
    type T<'a> = (&'a [u64], &'a [u64], &'a [u64], &'a [u64],
                   &'a [u64], &'a [u64], &'a [u64], &'a [u64]);
    let ds = indexed::DecodedStore::new(store);
    T::from_store(&ds, &mut 0).7[i]
}

// ================================================================
// Complex types: do unused complex fields get eliminated?
// ================================================================

#[no_mangle] pub fn store_u64_result_f0(store: &[u64], i: usize) -> u64 {
    type T<'a> = (&'a [u64], columnar::Results<&'a [u64], &'a [u64], &'a [u64], &'a [u64], &'a u64>);
    let ds = indexed::DecodedStore::new(store);
    *T::from_store(&ds, &mut 0).0.get(i).unwrap()
}

#[no_mangle] pub fn store_u64_string_vec_f0(store: &[u64], i: usize) -> u64 {
    type T<'a> = (&'a [u64], columnar::Strings<&'a [u64], &'a [u8]>, columnar::Vecs<&'a [u32], &'a [u64]>);
    let ds = indexed::DecodedStore::new(store);
    T::from_store(&ds, &mut 0).0[i]
}

#[no_mangle] pub fn store_nested_inner(store: &[u64], i: usize) -> u64 {
    type T<'a> = (&'a [u64], (&'a [u64], (&'a [u64], &'a [u64])));
    let ds = indexed::DecodedStore::new(store);
    T::from_store(&ds, &mut 0).1.1.1[i]
}

fn main() {
    let mut store = vec![0u64; 100];
    store[0] = 32; store[1] = 32; store[2] = 32; store[3] = 32;
    println!("{}", std::hint::black_box(store_3_f0(std::hint::black_box(&store), 0)));
}
