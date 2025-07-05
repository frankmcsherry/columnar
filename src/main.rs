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
    for (col, row) in columns.into_index_iter().zip(roster) {
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
            (format_args!("Brain{}", index), index),
            (format_args!("Brawn{}", index), index),
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
    fn round_trip<'a, C: Container>(container: &'a C) -> C::Borrowed<'a> {
        // Grab a reference to underlying bytes, as if serialized.
        let borrow = container.borrow();
        let mut bytes_iter = borrow.as_bytes().map(|(_, bytes)| bytes);
        columnar::FromBytes::from_bytes(&mut bytes_iter)
    }

    let borrowed = round_trip(&columns);

    // Project down to columns and variants using field accessors.
    // This gets all ages from people in teams.
    let solo_values: &[u64] = borrowed.Solo.1;
    let team_values: &[u64] = borrowed.Team.values.1;
    let total = solo_values.iter().sum::<u64>() + team_values.iter().sum::<u64>();
    println!("Present values summed: {:?}", total);
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
    #[columnar(derive(Ord, PartialOrd, PartialEq, Eq))]
    struct Test2<T: Copy> (Vec<T>, i16) where T: Clone;

    // Tests derived implementations for an enum with valuable variants,
    // but including unit variants.
    #[derive(Columnar, Debug)]
    #[columnar(derive(Ord, PartialOrd, PartialEq, Eq))]
    pub enum Test3<T> {
        Foo(Vec<T>, u8),
        Bar(i16),
        Void,
    }

    // Tests derived implementations for a struct with all unit variants.
    #[derive(Columnar, Debug, Copy, Clone)]
    pub enum Test4 {
        Foo,
        Bar,
    }
    
    // Tests derived implementations for a unit struct.
    #[derive(Columnar, Debug, Copy, Clone)]
    struct Test5;

    // Test derived implementations for the reference type.
    #[derive(Columnar, Debug)]
    #[columnar(derive(Ord, PartialOrd, PartialEq, Eq))]
    struct Test6 {
        bar: i16,
    }

    #[test]
    fn should_be_ord_eq() {
        fn is_ord_eq<T: Ord + PartialOrd + PartialEq + Eq>() {}
        is_ord_eq::<Test6Reference<i16>>();
        is_ord_eq::<Test6Reference<&i16>>();

        is_ord_eq::<Test3Reference<u8, u8, u8>>();
    }

    #[test]
    fn round_trip() {

        use columnar::Index;

        let test1s = vec![
            Test1 { foo: vec![1, 2, 3], bar: 4 },
            Test1 { foo: vec![5, 6, 7], bar: 8 },
        ];
        let test1c = columnar::Columnar::as_columns(test1s.iter());
        for (a, b) in test1s.into_iter().zip((&test1c).into_index_iter()) {
            assert_eq!(a.foo.len(), b.foo.len());
            assert_eq!(a.bar, *b.bar);
        }

        let test3s = vec![
            Test3::Foo(vec![1, 2, 3], 4),
            Test3::Bar(4),
        ];
        let test3c = columnar::Columnar::as_columns(test3s.iter());
        
        println!("{:?}", test3c);

        let iterc = (&test3c).into_index_iter();

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

    #[test]
    fn iterators_formatters() {

        use columnar::Push;

        let mut columns = <((), Vec<usize>) as Columnar>::Container::default();
        columns.push(((), 0 .. 10));

        let mut columns = <((), String) as Columnar>::Container::default();
        columns.push(((), format_args!("{:?}", 10)));

    }
}
