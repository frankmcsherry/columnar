//! Compile-only test that columnar works in a `no_std` environment.
//!
//! This crate depends on columnar with `default-features = false` and
//! declares `#![no_std]`. If columnar accidentally introduces a `std`
//! dependency, this crate will fail to compile.

#![no_std]
extern crate alloc;
use alloc::vec;

/// Exercise core columnar types without `std`.
pub fn smoke_test() {
    use columnar::{Push, Len, Index, Borrow};

    // Primitive container.
    let mut vals: Vec<u64> = Default::default();
    vals.push(1u64);
    vals.push(2u64);
    vals.push(3u64);
    assert_eq!(vals.len(), 3);

    // Strings container.
    let mut strings: columnar::Strings = Default::default();
    strings.push(b"hello" as &[u8]);
    strings.push(b"world" as &[u8]);
    let borrowed = strings.borrow();
    assert_eq!(borrowed.len(), 2);
    assert_eq!(borrowed.get(0), b"hello");

    // Vecs container.
    let mut vecs: columnar::Vecs<Vec<u64>> = Default::default();
    vecs.push(vec![1u64, 2, 3]);
    vecs.push(vec![4u64, 5]);
    let borrowed = vecs.borrow();
    assert_eq!(borrowed.len(), 2);

    // Options container.
    let mut opts: columnar::Options<Vec<u64>> = Default::default();
    opts.push(Some(42u64));
    opts.push(None::<u64>);
    assert_eq!(opts.len(), 2);

    // Results container.
    let mut res: columnar::Results<Vec<u64>, Vec<u8>> = Default::default();
    res.push(Ok::<u64, u8>(1));
    res.push(Err::<u64, u8>(2));
    assert_eq!(res.len(), 2);

    // Repeats container.
    let mut reps: columnar::Repeats<Vec<u64>> = Default::default();
    reps.push(&1u64);
    reps.push(&1u64);
    reps.push(&2u64);
    assert_eq!(reps.len(), 3);

    // AsBytes / FromBytes round-trip.
    use columnar::{AsBytes, FromBytes};
    let borrowed = strings.borrow();
    let rebuilt = columnar::Strings::<&[u64], &[u8]>::from_bytes(
        &mut borrowed.as_bytes().map(|(_, bytes)| bytes)
    );
    assert_eq!(rebuilt.len(), 2);
    assert_eq!(rebuilt.get(0), b"hello");
}

use alloc::vec::Vec;
