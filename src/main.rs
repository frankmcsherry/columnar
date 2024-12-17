use columnar::{Columnar, Container};

#[derive(Columnar)]
enum Group<T> {
    Solo(T),
    Team(Vec<T>),
}

fn main() {

    let mut roster = Vec::new();
    roster.push(Group::Solo(
        ("Alice".to_string(), 20u64)
    ));
    roster.push(Group::Team(vec![
        ("Bob".to_string(), 21),
        ("Carol".to_string(), 22),
    ]));

    // An appendable replacement for `&[T]`: indexable, shareable.
    // Layout in memory is a small number of contiguous buffers,
    // even though `roster` contains many small allocations.
    let mut columns = Columnar::as_columns(roster.iter());

    // Iterated column values should match the original `roster`.
    use columnar::Index;
    for (col, row) in columns.into_iter().zip(roster) {
        match (col, row) {
            (GroupReference::Solo(p0), Group::Solo(p1)) => {
                assert_eq!(p0.0, p1.0);
                assert_eq!(p0.1, &p1.1);
            },
            (GroupReference::Team(p0s), Group::Team(p1s)) => {
                assert_eq!(p0s.len(), p1s.len());
                for (p0, p1) in p0s.into_iter().zip(p1s) {
                    assert_eq!(p0.0, p1.0);
                    assert_eq!(p0.1, &p1.1);
                }
            },
            _ => { panic!("Variant mismatch"); }
        }
    }

    // Append a number of rows to `columns`.
    use columnar::Push;
    for index in 0 .. 1024 {
        columns.push(&Group::Team(vec![
            (format!("Brain{}", index), index),
            (format!("Brawn{}", index), index),
        ]));
    }

    // Report the fixed number of large buffers backing `columns`.
    use columnar::AsBytes;
    assert_eq!(columns.borrow().as_bytes().count(), 9);
    for (align, bytes) in columns.borrow().as_bytes() {
        println!("align: {:?}, bytes.len(): {:?}", align, bytes.len());
    }

    // Borrow raw bytes from `columns`, and reconstruct a borrowed `columns`.
    // In practice, we would use serialized bytes from somewhere else.
    // This local function gives type support, relating `T` to `T::Borrowed`.
    fn round_trip<'a, C: Columnar>(container: &'a C::Container) -> <C::Container as Container<C>>::Borrowed<'a> {
        // Grab a reference to underlying bytes, as if serialized.
        let borrow = container.borrow();
        let mut bytes_iter = borrow.as_bytes().map(|(_, bytes)| bytes);
        columnar::FromBytes::from_bytes(&mut bytes_iter)
    }

    let borrowed = round_trip::<Group<_>>(&columns);

    // Project down to columns and variants using field accessors.
    // This gets all ages from people in teams.
    let solo_values: &[u64] = borrowed.Solo.1;
    let team_values: &[u64] = borrowed.Team.values.1;
    let total = solo_values.iter().sum::<u64>() + team_values.iter().sum::<u64>();
    println!("Present values summed: {:?}", total);


    // _main2();
}

fn _main2() {

    use columnar::adts::tree::{Tree, Trees};
    use columnar::adts::json::{Json, Jsons};

    use columnar::{Push, Len, Index, HeapSize};

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

#[cfg(test)]
mod test {
    use columnar::Columnar;

    // Tests derived implementations for a struct with named fields.
    #[derive(Columnar, Debug)]
    struct Test1<T: Copy> where T: Clone {
        foo: Vec<T>,
        bar: i16,
    }

    // Tests derived implementations for a struct with unnamed fields.
    #[derive(Columnar, Debug)]
    struct Test2<T: Copy> (Vec<T>, i16) where T: Clone;

    // Tests derived implementations for an enum with valuable variants,
    // but including unit variants.
    #[derive(Columnar, Debug)]
    pub enum Test3<T> {
        Foo(Vec<T>, u8),
        Bar(i16),
        Void,
    }

    // Tests derived implementations for a struct with all unit variants.
    #[derive(Columnar, Debug)]
    pub enum Test4 {
        Foo,
        Bar,
    }
    
    // Tests derived implementations for a unit struct.
    #[derive(Columnar, Debug)]
    struct Test5;

    #[test]
    fn round_trip() {

        use columnar::Index;

        let test1s = vec![
            Test1 { foo: vec![1, 2, 3], bar: 4 },
            Test1 { foo: vec![5, 6, 7], bar: 8 },
        ];
        let test1c = columnar::Columnar::as_columns(test1s.iter());
        for (a, b) in test1s.into_iter().zip((&test1c).into_iter()) {
            assert_eq!(a.foo.len(), b.foo.len());
            assert_eq!(a.bar, *b.bar);
        }

        let test3s = vec![
            Test3::Foo(vec![1, 2, 3], 4),
            Test3::Bar(4),
        ];
        let test3c = columnar::Columnar::as_columns(test3s.iter());
        
        println!("{:?}", test3c);

        let iterc = (&test3c).into_iter();

        for (a, b) in test3s.into_iter().zip(iterc) {
            match (a, &b) {
                (Test3::Foo(a, b), Test3Reference::Foo((c, d))) => {
                    assert_eq!(a.len(), c.len());
                    assert_eq!(b, **d);
                },
                (Test3::Bar(a), Test3Reference::Bar(b)) => {
                    assert_eq!(a, **b);
                },
                _ => { panic!("Variant mismatch"); }
            }
        }

    }
}
