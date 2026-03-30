use columnar::{Columnar, ContainerBytes, Borrow};

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
                assert_eq!(p0.0, p1.0.as_bytes());
                assert_eq!(p0.1, &p1.1);
            },
            (GroupReference::Team(p0s), Group::Team(p1s)) => {
                assert_eq!(p0s.len(), p1s.len());
                for (p0, p1) in p0s.into_iter().zip(p1s) {
                    assert_eq!(p0.0, p1.0.as_bytes());
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
    fn round_trip<'a, C: ContainerBytes>(container: &'a C) -> C::Borrowed<'a> {
        // Grab a reference to underlying bytes, as if serialized.
        let borrow = container.borrow();
        let mut bytes_iter = borrow.as_bytes().map(|(_, bytes)| bytes);
        columnar::FromBytes::from_bytes(&mut bytes_iter)
    }

    let borrowed = round_trip(&columns);

    // Project down to columns and variants using field accessors.
    // This gets all ages from people in teams.
    let solo_values: &[u64] = borrowed.group_0.1;
    let team_values: &[u64] = borrowed.group_1.values.1;
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

    // Tests derived implementations for an enum with named fields.
    #[derive(Columnar, Debug)]
    pub enum Test7 {
        Click { x: u64, y: u64 },
        Scroll(i64),
        Idle,
    }

    #[test]
    fn named_enum_fields() {
        use columnar::{Borrow, Index, Len, Push, Columnar};

        let items = vec![
            Test7::Click { x: 10, y: 20 },
            Test7::Scroll(-5),
            Test7::Idle,
            Test7::Click { x: 30, y: 40 },
        ];

        let columns = Columnar::as_columns(items.iter());
        assert_eq!(columns.len(), 4);

        // Check indexing.
        match (&columns).get(0) {
            Test7Reference::Click((x, y)) => { assert_eq!(x, 10); assert_eq!(y, 20); },
            _ => panic!("expected Click"),
        }
        match (&columns).get(1) {
            Test7Reference::Scroll(d) => { assert_eq!(d, -5); },
            _ => panic!("expected Scroll"),
        }
        match (&columns).get(2) {
            Test7Reference::Idle(_) => {},
            _ => panic!("expected Idle"),
        }
        match (&columns).get(3) {
            Test7Reference::Click((x, y)) => { assert_eq!(x, 30); assert_eq!(y, 40); },
            _ => panic!("expected Click"),
        }

        // Check into_owned round-trip.
        let borrowed = columns.borrow();
        let owned: Test7 = Columnar::into_owned(borrowed.get(0));
        match owned {
            Test7::Click { x, y } => { assert_eq!(x, 10); assert_eq!(y, 20); },
            _ => panic!("expected Click"),
        }

        // Check push by reference.
        let mut columns2: columnar::ContainerOf<Test7> = Default::default();
        columns2.push(&Test7::Click { x: 99, y: 100 });
        match (&columns2).get(0) {
            Test7Reference::Click((x, y)) => { assert_eq!(x, 99); assert_eq!(y, 100); },
            _ => panic!("expected Click"),
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


    #[test]
    fn extend_from_self_enum() {
        use columnar::{Borrow, Container, Index, Len, Push};

        // Test data enum with multiple variants.
        let mut columns = <Test3<u8> as Columnar>::Container::default();
        columns.push(Test3::<u8>::Foo(vec![1, 2], 10));
        columns.push(Test3::<u8>::Bar(20));
        columns.push(Test3::<u8>::Foo(vec![3], 30));
        columns.push(Test3::<u8>::Void);

        let mut dest = <Test3<u8> as Columnar>::Container::default();
        dest.extend_from_self(columns.borrow(), 1..3);
        assert_eq!(dest.len(), 2);
        match dest.borrow().get(0) {
            Test3Reference::Bar(x) => assert_eq!(*x, 20),
            other => panic!("Expected Bar, got {:?}", other),
        }
        match dest.borrow().get(1) {
            Test3Reference::Foo((v, x)) => {
                assert_eq!(v.len(), 1);
                assert_eq!(*x, 30);
            },
            other => panic!("Expected Foo, got {:?}", other),
        }

        // Test unit enum.
        let mut tags = <Test4 as Columnar>::Container::default();
        tags.push(Test4::Foo);
        tags.push(Test4::Bar);
        tags.push(Test4::Foo);

        let mut dest_tags = <Test4 as Columnar>::Container::default();
        dest_tags.extend_from_self(tags.borrow(), 0..3);
        assert_eq!(dest_tags.len(), 3);
        assert!(matches!(dest_tags.borrow().get(0), Test4::Foo));
        assert!(matches!(dest_tags.borrow().get(1), Test4::Bar));
        assert!(matches!(dest_tags.borrow().get(2), Test4::Foo));
    }

    // Test names that collide with the prelude.
    #[derive(Columnar, Debug, Copy, Clone)]
    enum Strange { None, Some }

    #[derive(Columnar, Debug, Clone)]
    struct BoxedStr {
        value: Box<str>,
    }

    // Test shared storage: variants with the same field types share a container.
    #[derive(Columnar, Debug)]
    enum SharedTest {
        Left(u64),
        Right(u64),
        Name(String),
    }

    #[test]
    fn shared_storage_round_trip() {
        use columnar::{Borrow, Index, Len, Push, Columnar};

        let mut columns = <SharedTest as Columnar>::Container::default();
        columns.push(&SharedTest::Left(10));
        columns.push(&SharedTest::Right(20));
        columns.push(&SharedTest::Name("hello".to_string()));
        columns.push(&SharedTest::Left(30));
        columns.push(&SharedTest::Right(40));
        assert_eq!(columns.len(), 5);

        // Left and Right share group_0; verify correct indexing.
        match (&columns).get(0) {
            SharedTestReference::Left(v) => assert_eq!(v, &10),
            _ => panic!("expected Left"),
        }
        match (&columns).get(1) {
            SharedTestReference::Right(v) => assert_eq!(v, &20),
            _ => panic!("expected Right"),
        }
        match (&columns).get(2) {
            SharedTestReference::Name(s) => assert_eq!(s, "hello".as_bytes()),
            _ => panic!("expected Name"),
        }
        match (&columns).get(3) {
            SharedTestReference::Left(v) => assert_eq!(v, &30),
            _ => panic!("expected Left"),
        }

        // Verify the shared container has 4 elements (Left×2 + Right×2).
        assert_eq!(columns.group_0.len(), 4);
        // Name container has 1 element.
        assert_eq!(columns.group_1.len(), 1);

        // Round-trip through bytes.
        use columnar::{AsBytes, ContainerBytes};
        fn round_trip<'a, C: ContainerBytes>(container: &'a C) -> C::Borrowed<'a> {
            let borrow = container.borrow();
            let mut bytes_iter = borrow.as_bytes().map(|(_, bytes)| bytes);
            columnar::FromBytes::from_bytes(&mut bytes_iter)
        }
        let borrowed = round_trip(&columns);
        assert_eq!(borrowed.len(), 5);
        match borrowed.get(4) {
            SharedTestReference::Right(v) => assert_eq!(v, &40),
            _ => panic!("expected Right"),
        }
    }

    // Test shared storage with extend_from_self.
    #[test]
    fn shared_storage_extend() {
        use columnar::{Borrow, Container, Index, Len, Push, Columnar};

        let mut columns = <SharedTest as Columnar>::Container::default();
        columns.push(&SharedTest::Left(1));
        columns.push(&SharedTest::Right(2));
        columns.push(&SharedTest::Name("x".to_string()));
        columns.push(&SharedTest::Left(3));

        let mut dest = <SharedTest as Columnar>::Container::default();
        dest.extend_from_self(columns.borrow(), 1..3);
        assert_eq!(dest.len(), 2);
        match dest.borrow().get(0) {
            SharedTestReference::Right(v) => assert_eq!(*v, 2),
            _ => panic!("expected Right"),
        }
        match dest.borrow().get(1) {
            SharedTestReference::Name(s) => assert_eq!(s, "x".as_bytes()),
            _ => panic!("expected Name"),
        }
    }
}
