//! Tests for the `#[columnar(as = <Type>)]` field attribute, which stores a field through a
//! wrapper type's columnar machinery. With `as = Bytes`, a `Vec<u8>` field keeps its type but
//! gains a native `&[u8]` reference (via `Strings`) instead of `Slice<&[u8]>`.

use columnar::{Borrow, Columnar, Index, Len, Strings};

#[derive(Columnar, Clone, Debug, PartialEq)]
struct Record {
    id: u64,
    #[columnar(as = ::columnar::Bytes)]
    payload: Vec<u8>,
}

fn sample() -> Vec<Record> {
    vec![
        Record { id: 1, payload: b"abc".to_vec() },
        Record { id: 2, payload: vec![] },
        Record { id: 3, payload: vec![0xff; 200] },
    ]
}

#[test]
fn bytes_field_is_stored_as_strings() {
    let rows = sample();
    let columns = <Record as Columnar>::as_columns(rows.iter());
    // The field's container is `Strings`, not `Vecs<Vec<u8>>`.
    let _assert_type: &Strings = &columns.payload;

    let borrow = columns.borrow();
    assert_eq!(borrow.len(), rows.len());
    for (i, row) in rows.iter().enumerate() {
        let reference = borrow.get(i);
        assert_eq!(reference.id, &row.id);
        // The field reference is a native `&[u8]`.
        let payload: &[u8] = reference.payload;
        assert_eq!(payload, row.payload.as_slice());
        // Reconstruction returns the original `Vec<u8>`.
        assert_eq!(&<Record as Columnar>::into_owned(reference), row);
    }
}

#[test]
fn bytes_field_owned_push_and_copy_from() {
    let rows = sample();
    // Push owned records by value, exercising `Strings: Push<Vec<u8>>`.
    let columns = <Record as Columnar>::into_columns(rows.clone());
    let borrow = columns.borrow();
    for (i, row) in rows.iter().enumerate() {
        // `copy_from` reconstructs into an existing value, reusing its buffer.
        let mut target = Record { id: 0, payload: vec![0xaa; 8] };
        <Record as Columnar>::copy_from(&mut target, borrow.get(i));
        assert_eq!(&target, row);
    }
}

#[derive(Columnar, Clone, Debug, PartialEq)]
struct TupleRecord(u64, #[columnar(as = ::columnar::Bytes)] Vec<u8>);

#[test]
fn bytes_field_on_tuple_struct() {
    let rows = [TupleRecord(1, b"xy".to_vec()), TupleRecord(2, vec![])];
    let columns = <TupleRecord as Columnar>::as_columns(rows.iter());
    let borrow = columns.borrow();
    for (i, row) in rows.iter().enumerate() {
        let reference = borrow.get(i);
        // The derived reference for a tuple struct names its fields `f0`, `f1`, ...
        let payload: &[u8] = reference.f1;
        assert_eq!(payload, row.1.as_slice());
        assert_eq!(&<TupleRecord as Columnar>::into_owned(reference), row);
    }
}
