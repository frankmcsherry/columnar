fn main() {

    use columnar::{Columnable, Len, IndexOwn};
    use columnar::bytes::{AsBytes, FromBytes};

    // A sequence of complex, nested, and variously typed records.
    let records =
    (0 .. 1024u64).map(|i| {
        (0 .. i).map(|j| {
            if (i - j) % 2 == 0 {
                Ok((j, format!("grawwwwrr!")))
            } else {
                Err(Some(vec![(); 1 << 40]))
            }
        }).collect::<Vec<_>>()
    });

    // An appendable replacement for `&[T]`: indexable, shareable.
    // Layout in memory is a small number of contiguous buffers,
    // even though `records` contains many small allocations.
    let columns = Columnable::as_columns(records.clone());

    // Each item in `columns` matches the original in `records`.
    // Equality testing is awkward, because the GAT reference types don't match.
    // For example, `Option<&T>` cannot be equated to `Option<T>` without help,
    // and tuples `(&A, &B, &C)` cannot be equated to `&(A, B, C)` without help.
    for (a, b) in columns.iter().zip(records) {
        assert_eq!(a.len(), b.len());
        for (a, b) in a.iter().zip(b) {
            match (a, b) {
                (Ok(a), Ok(b)) => {
                    assert_eq!(a.0, &b.0);
                    assert_eq!(a.1, b.1);
                },
                (Err(a), Err(b)) => {
                    match (a, b) {
                        (Some(a), Some(b)) => { assert_eq!(a.len(), b.len()); },
                        (None, None) => { },
                        _ => { panic!("Variant mismatch"); }
                    }
                },
                _ => { panic!("Variant mismatch"); }
            }
        }
    }

    // Report the small number of large buffers backing `columns`.
    for (align, bytes) in columns.as_bytes() {
        println!("align: {:?}, bytes.len(): {:?}", align, bytes.len());
    }

    // Borrow bytes from `columns`, and reconstruct a borrowed `columns`.
    // In practice, we would use serialized bytes from somewhere else.
    // Function defined to get type support, relating `T` to `T::Borrowed`.
    fn round_trip<T: AsBytes>(container: &T) -> T::Borrowed<'_> {
        // Grab a reference to underlying bytes, as if serialized.
        let mut bytes_iter = container.as_bytes().map(|(_, bytes)| bytes);
        FromBytes::from_bytes(&mut bytes_iter)
    }

    let borrowed = round_trip(&columns);

    // Project down to columns and variants using only field accessors.
    // This gets all Some(_) variants from the first tuple coordinate.
    let values: &[u64] = borrowed.values.oks.0;
    let total = values.iter().sum::<u64>();
    println!("Present values summed: {:?}", total);


    _main2();
}

fn _main2() {

    use columnar::adts::tree::{Tree, Trees};
    use columnar::adts::json::{Json, Jsons};

    use columnar::{Push, Len, IndexOwn, HeapSize};

    let mut tree = Tree { data: 0, kids: vec![] };
    for i in 0 .. 11 {
        let mut kids = Vec::with_capacity(i);
        for _ in 0 .. i {
            kids.push(tree.clone());
        }
        tree.data = i;
        tree.kids = kids;
    }

    let timer = std::time::Instant::now();
    let sum = tree.sum();
    let time = timer.elapsed();
    println!("{:?}\ttree summed: {:?}", time, sum);

    let timer = std::time::Instant::now();
    let clone = tree.clone();
    let time = timer.elapsed();
    println!("{:?}\ttree cloned", time);

    let timer = std::time::Instant::now();
    let mut cols = Trees::new();
    cols.push(tree);
    let time = timer.elapsed();
    println!("{:?}\tcols formed", time);

    let timer = std::time::Instant::now();
    if cols.index(0) != clone {
        println!("UNEQUAL!!!");
    }
    let time = timer.elapsed();
    println!("{:?}\tcompared", time);

    let timer = std::time::Instant::now();
    let sum = (&cols.values).iter().sum::<usize>();
    let time = timer.elapsed();
    println!("{:?}\tcols summed: {:?}", time, sum);

    let timer = std::time::Instant::now();
    let _ = cols.clone();
    let time = timer.elapsed();
    println!("{:?}\tcols cloned", time);

    use std::fs::File;
    use serde_json::Value as JsonValue;

    let timer = std::time::Instant::now();
    // let f = File::open("cities.json.txt").unwrap();
    let f = File::open("true.txt").unwrap();
    let records: Vec<JsonValue> = serde_json::from_reader(f).unwrap();
    let time = timer.elapsed();
    println!("{:?}\tread {} json records", time, records.len());

    let timer = std::time::Instant::now();
    let _ = records.clone();
    let time = timer.elapsed();
    println!("{:?}\tjson_vals cloned", time);

    let values = records.clone().into_iter().map(Json::from_json).collect::<Vec<_>>();
    println!("\t\tjson_vals heapsize: {:?}", values.heap_size().0);

    let timer = std::time::Instant::now();
    let mut json_cols = Jsons::default();
    json_cols.extend(values.iter());
    let time = timer.elapsed();
    println!("{:?}\tjson_cols formed", time);
    println!("\t\tjson_cols heapsize: {:?}", json_cols.heap_size().0);
    println!("\t\tjson_cols.roots:    {:?}", json_cols.roots.heap_size().0);
    println!("\t\tjson_cols.numbers:  {:?}", json_cols.numbers.heap_size().0);
    println!("\t\tjson_cols.strings:  {:?}", json_cols.strings.heap_size().0);
    println!("\t\tjson_cols.arrays:   {:?}", json_cols.arrays.heap_size().0);
    println!("\t\tjson_cols.objects:  {:?}", json_cols.objects.heap_size().0);
    println!("\t\tjson_cols.objects.values.0:  {:?}", json_cols.objects.values.0.heap_size().0);
    println!("\t\tjson_cols.objects.values.1:  {:?}", json_cols.objects.values.1.heap_size().0);

    println!("\t\tjson_cols.arrays.len: {:?}", json_cols.arrays.len());

    let timer = std::time::Instant::now();
    for (index, value) in values.iter().enumerate() {
        if (&json_cols).get(index) != *value {
            println!("Mismatch: {:?}: {:?}", index, value);
        }
    }
    let time = timer.elapsed();
    println!("{:?}\tcompared", time);

    let timer = std::time::Instant::now();
    let _ = json_cols.clone();
    let time = timer.elapsed();
    println!("{:?}\tjson_cols cloned", time);

    let timer = std::time::Instant::now();
    use serde::ser::Serialize;
    let mut encoded0 = Vec::new();
    let mut serializer = rmp_serde::Serializer::new(&mut encoded0).with_bytes(rmp_serde::config::BytesMode::ForceAll);
    values.serialize(&mut serializer).unwrap();
    let time = timer.elapsed();
    println!("{:?}\tjson_vals encode ({} bytes; msgpack)", time, encoded0.len());
    let timer = std::time::Instant::now();
    let decoded0: Vec<Json> = rmp_serde::from_slice(&encoded0[..]).unwrap();
    let time = timer.elapsed();
    println!("{:?}\tjson_vals decode", time);

    let timer = std::time::Instant::now();
    let mut encoded1 = Vec::new();
    let mut serializer = rmp_serde::Serializer::new(&mut encoded1).with_bytes(rmp_serde::config::BytesMode::ForceAll);
    json_cols.serialize(&mut serializer).unwrap();
    let time = timer.elapsed();
    println!("{:?}\tjson_cols encode ({} bytes; msgpack)", time, encoded1.len());
    let timer = std::time::Instant::now();
    let decoded1: Jsons = rmp_serde::from_slice(&encoded1[..]).unwrap();
    let time = timer.elapsed();
    println!("{:?}\tjson_cols decode", time);

    let timer = std::time::Instant::now();
    let encoded2: Vec<u8> = bincode::serialize(&json_cols).unwrap();
    let time = timer.elapsed();
    println!("{:?}\tjson_cols encode ({} bytes; bincode)", time, encoded2.len());

    assert_eq!(values, decoded0);
    assert_eq!(json_cols, decoded1);

}

