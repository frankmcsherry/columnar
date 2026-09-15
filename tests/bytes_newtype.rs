//! Tests for the `Bytes` newtype, which routes byte payloads to `Strings` so their reference
//! type is a native `&[u8]` rather than `Slice<&[u8]>`.

use columnar::{Borrow, Bytes, Columnar, ContainerBytes, Index, Len, Strings};

/// Serializes a container to its byte slices and reads it back, mirroring a round trip through
/// a network boundary.
fn round_trip<C: ContainerBytes>(container: &C) -> Vec<Vec<u8>> {
    use columnar::AsBytes;
    container.borrow().as_bytes().map(|(_, bytes)| bytes.to_vec()).collect()
}

fn from_bytes<'a, C: ContainerBytes>(slices: &'a [Vec<u8>]) -> C::Borrowed<'a> {
    let mut iter = slices.iter().map(|bytes| bytes.as_slice());
    columnar::FromBytes::from_bytes(&mut iter)
}

#[test]
fn column_of_bytes_reads_as_slices() {
    let rows: Vec<Bytes> = vec![
        Bytes(vec![]),
        Bytes(vec![1, 2, 3]),
        Bytes(b"hello".to_vec()),
        Bytes(vec![0xff; 300]),
        // Invalid UTF-8 is accepted, unlike `String`.
        Bytes(vec![0x80, 0xfe, 0x00]),
    ];

    let columns: <Bytes as Columnar>::Container = Columnar::as_columns(rows.iter());
    let borrow = columns.borrow();
    assert_eq!(borrow.len(), rows.len());
    for (i, row) in rows.iter().enumerate() {
        let got: &[u8] = borrow.get(i);
        assert_eq!(got, row.0.as_slice());
    }
}

#[test]
fn container_is_strings() {
    // The whole point of the newtype: the container is `Strings`, not `Vecs<Vec<u8>>`.
    let columns: <Bytes as Columnar>::Container = Columnar::as_columns([Bytes(vec![1, 2])].iter());
    let _assert_type: &Strings = &columns;
    // And the reference is a native `&[u8]`, so comparison and hashing are the standard slice ones.
    let borrow = columns.borrow();
    let reference: &[u8] = borrow.get(0);
    assert_eq!(reference, &[1u8, 2]);
}

#[test]
fn into_owned_round_trips() {
    let original = Bytes(b"round trip".to_vec());
    let columns: <Bytes as Columnar>::Container = Columnar::as_columns([original.clone()].iter());
    let borrow = columns.borrow();
    let owned = <Bytes as Columnar>::into_owned(borrow.get(0));
    assert_eq!(owned, original);
}

#[test]
fn bytes_round_trips_through_serialization() {
    let rows: Vec<Bytes> = vec![Bytes(vec![9, 8, 7]), Bytes(vec![]), Bytes(vec![42; 100])];
    let columns: <Bytes as Columnar>::Container = Columnar::as_columns(rows.iter());

    let slices = round_trip(&columns);
    let restored = from_bytes::<<Bytes as Columnar>::Container>(&slices);
    assert_eq!(restored.len(), rows.len());
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(restored.get(i), row.0.as_slice());
    }
}

#[test]
fn bytes_equality_against_slices() {
    let bytes = Bytes(b"abc".to_vec());
    let slice: &[u8] = b"abc";
    assert_eq!(bytes, *slice);
    assert_eq!(*slice, bytes);
    assert_eq!(bytes, slice);
    assert_eq!(slice, bytes);
    assert_ne!(bytes, b"abd".as_slice());
}

#[test]
fn bytes_borrows_as_slice() {
    use std::collections::HashMap;
    let mut map: HashMap<Bytes, u32> = HashMap::new();
    map.insert(Bytes(b"key".to_vec()), 7);
    // `Borrow<[u8]>` lets the map be probed with the borrowed `&[u8]` reference.
    assert_eq!(map.get(b"key".as_slice()), Some(&7));
}

#[derive(Columnar, Clone, Debug, PartialEq)]
struct Record {
    id: u64,
    payload: Bytes,
}

fn sample_records() -> Vec<Record> {
    vec![
        Record { id: 1, payload: Bytes(b"alpha".to_vec()) },
        Record { id: 2, payload: Bytes(vec![]) },
        Record { id: 3, payload: Bytes(vec![0x00, 0xff]) },
    ]
}

#[test]
fn derived_struct_field_uses_strings() {
    let records = sample_records();
    let columns = <Record as Columnar>::as_columns(records.iter());
    let borrow = columns.borrow();
    assert_eq!(borrow.len(), records.len());
    for (i, record) in records.iter().enumerate() {
        let reference = borrow.get(i);
        assert_eq!(reference.id, &record.id);
        // The field reference is a native `&[u8]`.
        let payload: &[u8] = reference.payload;
        assert_eq!(payload, record.payload.0.as_slice());
    }
}

#[derive(Columnar, Clone, Debug, PartialEq)]
struct Blob {
    payload: Bytes,
}

#[test]
fn derived_reference_compares_to_owned() {
    let blobs = [Blob { payload: Bytes(b"x".to_vec()) }, Blob { payload: Bytes(vec![]) }];
    let columns = <Blob as Columnar>::as_columns(blobs.iter());
    let borrow = columns.borrow();
    for (i, blob) in blobs.iter().enumerate() {
        // The derived `Reference == Struct` comparison relies on `&[u8]: PartialEq<Bytes>`.
        assert!(borrow.get(i) == *blob);
    }
}

#[test]
fn derived_struct_owned_push_and_copy_from() {
    let records = sample_records();

    // Push owned records by value, exercising `Strings: Push<Bytes>`.
    let columns = <Record as Columnar>::into_columns(records.clone());
    let borrow = columns.borrow();
    assert_eq!(borrow.len(), records.len());

    for (i, record) in records.iter().enumerate() {
        // `copy_from` reconstructs into an existing value, reusing its buffers.
        let mut target = Record { id: 0, payload: Bytes(vec![0xaa; 8]) };
        <Record as Columnar>::copy_from(&mut target, borrow.get(i));
        assert_eq!(&target, record);
    }
}
