//! Types supporting flat / "columnar" layout for complex types.
//!
//! The intent is to re-layout `Vec<T>` types into vectors of reduced
//! complexity, repeatedly. One should be able to push and pop easily,
//! but indexing will be more complicated because we likely won't have
//! a real `T` lying around to return as a reference. Instead, we will
//! use Generic Associated Types (GATs) to provide alternate references.

pub mod bytes;

/// A type that can be represented in columnar form.
pub trait Columnable : Sized {
    /// The type that stores the columnar representation.
    type Columns: Push<Self> + Index + Clear + Default;

    /// Converts a sequence of the type into columnar form.
    fn as_columns<I>(selves: I) -> Self::Columns where I: Iterator<Item = Self>, Self: Sized {
        let mut columns: Self::Columns = Default::default();
        for item in selves {
            columns.push(item);
        }
        columns
    }
}

pub use common::{Clear, Len, Push, Index, IndexMut, HeapSize, Slice, IndexIter};
/// Common traits and types that are re-used throughout the module.
pub mod common {

    /// A type with a length.
    pub trait Len {
        /// The number of contained elements.
        fn len(&self) -> usize;
        /// Whether this contains no elements.
        fn is_empty(&self) -> bool {
            self.len() == 0
        }
    }

    impl<L: Len> Len for &L {
        #[inline(always)] fn len(&self) -> usize { L::len(*self) }
    }
    impl<L: Len> Len for &mut L {
        #[inline(always)] fn len(&self) -> usize { L::len(*self) }
    }

    /// A type that can accept items of type `T`.
    pub trait Push<T> {
        /// Pushes an item onto `self`.
        fn push(&mut self, item: T);
        /// Pushes elements of an iterator onto `self`.
        #[inline(always)] fn extend(&mut self, iter: impl IntoIterator<Item=T>) {
            for item in iter {
                self.push(item);
            }
        }
    }

    /// A type that can be accessed by `usize`.
    pub trait Index : Len {
        /// Type referencing an indexed element.
        type Ref<'a> where Self: 'a;
        fn get(&self, index: usize) -> Self::Ref<'_>;
        /// A reference to the last element, should one exist.
        #[inline(always)] fn last(&self) -> Option<Self::Ref<'_>> where Self: Len {
            if self.is_empty() { None }
            else { Some(self.get(self.len()-1)) }
        }
        /// An iterator over references to indexed elements.
        #[inline(always)] fn iter(&self) -> IndexIter<&Self> {
            IndexIter {
                index: 0,
                slice: self
            }
        }
    }

    impl<'t, T: Index> Index for &'t T {
        type Ref<'a> = T::Ref<'t> where Self: 'a;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref<'_> {
            T::get(*self, index)
        }
    }

    /// A type that can be mutably accessed by `usize`.
    pub trait IndexMut : Index {
        /// Type mutably referencing an indexed element.
        type IndexMut<'a> where Self: 'a;
        fn get_mut(& mut self, index: usize) -> Self::IndexMut<'_>;
        /// A reference to the last element, should one exist.
        #[inline(always)] fn last_mut(&mut self) -> Option<Self::IndexMut<'_>> where Self: Len {
            if self.is_empty() { None }
            else { Some(self.get_mut(self.len()-1)) }
        }
    }

    // These blanket implementations are unfortunate, in that they return lifetimes that depend
    // on the `&self` borrow, rather than the `'t` lifetime of the type itself. This is a result
    // of relying on the `T::index` and `T::index_mut` functions, which narrow the lifetime.
    impl<'t, T: Index> Index for &'t mut T {
        type Ref<'a> = T::Ref<'a> where Self: 'a;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref<'_> {
            T::get(self, index)
        }
    }
    impl<'t, T: IndexMut> IndexMut for &'t mut T {
        type IndexMut<'a> = T::IndexMut<'a> where Self: 'a;
        #[inline(always)] fn get_mut(&mut self, index: usize) -> Self::IndexMut<'_> {
            T::get_mut(self, index)
        }
    }

    pub trait Clear {
        /// Clears `self`, without changing its capacity.
        fn clear(&mut self);
    }

    pub trait HeapSize {
        /// Active (len) and allocated (cap) heap sizes in bytes.
        /// This should not include the size of `self` itself.
        fn heap_size(&self) -> (usize, usize) { (0, 0) }
    }

    impl HeapSize for serde_json::Number { }
    impl HeapSize for String {
        fn heap_size(&self) -> (usize, usize) {
            (self.len(), self.capacity())
        }
    }

    /// A struct representing a slice of a range of values.
    ///
    /// The lower and upper bounds should be meaningfully set on construction.
    pub struct Slice<S> {
        lower: usize,
        upper: usize,
        slice: S,
    }

    impl<S> Slice<S> {
        pub fn slice<R: std::ops::RangeBounds<usize>>(self, range: R) -> Self {
            use std::ops::Bound;
            let lower = match range.start_bound() {
                Bound::Included(s) => std::cmp::max(self.lower, *s),
                Bound::Excluded(s) => std::cmp::max(self.lower, *s+1),
                Bound::Unbounded => self.lower,
            };
            let upper = match range.end_bound() {
                Bound::Included(s) => std::cmp::min(self.upper, *s+1),
                Bound::Excluded(s) => std::cmp::min(self.upper, *s),
                Bound::Unbounded => self.upper,
            };
            assert!(lower <= upper);
            Self { lower, upper, slice: self.slice }
        }
        pub fn new(lower: usize, upper: usize, slice: S) -> Self {
            Self { lower, upper, slice }
        }
    }

    impl<S> Len for Slice<S> {
        #[inline(always)] fn len(&self) -> usize { self.upper - self.lower }
    }

    impl<S: Index> Index for Slice<S> {
        type Ref<'a> = S::Ref<'a> where S: 'a;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref<'_> {
            assert!(index < self.upper - self.lower);
            self.slice.get(self.lower + index)
        }
    }

    impl<S: IndexMut> IndexMut for Slice<S> {
        type IndexMut<'a> = S::IndexMut<'a> where S: 'a;
        #[inline(always)] fn get_mut(&mut self, index: usize) -> Self::IndexMut<'_> {
            assert!(index < self.upper - self.lower);
            self.slice.get_mut(self.lower + index)
        }
    }


    impl<S: Index> Slice<S> {
        pub fn iter(&self) -> IndexIter<&Self> {
            IndexIter {
                index: 0,
                slice: self
            }
        }
    }

    impl<'a, S: Index> IntoIterator for &'a Slice<S> {
        type Item = S::Ref<'a>;
        type IntoIter = IndexIter<&'a Slice<S>>;
        #[inline(always)] fn into_iter(self) -> Self::IntoIter {
            IndexIter {
                index: 0,
                slice: self
            }
        }
    }

    impl<S: Index, T> PartialEq<[T]> for Slice<S> where for<'a> S::Ref<'a>: PartialEq<&'a T>{
        fn eq(&self, other: &[T]) -> bool {
            if self.len() != other.len() { return false; }
            for (a, b) in self.iter().zip(other.iter()) {
                if a != b { return false; }
            }
            true
        }
    }
    impl<S: Index, T> PartialEq<Vec<T>> for Slice<S> where for<'a> S::Ref<'a>: PartialEq<&'a T> {
        fn eq(&self, other: &Vec<T>) -> bool {
            if self.len() != other.len() { return false; }
            for (a, b) in self.iter().zip(other.iter()) {
                if a != b { return false; }
            }
            true
        }
    }

    pub struct IndexIter<S: ?Sized> {
        index: usize,
        slice: S,
    }

    impl<S> IndexIter<S> {
        pub fn new(index: usize, slice: S) -> Self {
            Self { index, slice }
        }
    }

    impl<'a, S: Index + Len> Iterator for IndexIter<&'a S> {
        type Item = S::Ref<'a>;

        #[inline(always)] fn next(&mut self) -> Option<Self::Item> {
            if self.index < self.slice.len() {
                let result = self.slice.get(self.index);
                self.index += 1;
                Some(result)
            } else {
                None
            }
        }
    }
}

// The `Vec<T>` type is a fine form of storage that is not inherently columnar.
// This is ok, and we will still rely on it as a base case for many types.
// Simple types cannot be further reduced, and some complex types (e.g. `Rc<T>`)
// may have no other options but to use `Vec<T>`.
//
// Importantly, this implementation *allows* types to be stored in a  `Vec`, but
// it does not implement `Columnable` for them nor express a preference for storage.

// This implementation does not chase down the heap contributions of the owned items.
// It *should*, to give a fair assessment vis-a-vis other representations, but it is
// annoying for vectors of types we neither control nor anticipate with `HeapSize`.
impl<T: HeapSize> HeapSize for Vec<T> {
    fn heap_size(&self) -> (usize, usize) {
        let mut l = std::mem::size_of::<T>() * self.len();
        let mut c = std::mem::size_of::<T>() * self.capacity();
        for item in self.iter() {
            let (il, ic) = item.heap_size();
            l += il;
            c += ic;
        }
        (l, c)
    }
}

impl<T> Len for Vec<T> {
    #[inline(always)] fn len(&self) -> usize { self.len() }
}
impl<T> Push<T> for Vec<T> {
    #[inline(always)] fn push(&mut self, item: T) { self.push(item) }
}
impl<'a, T: Clone> Push<&'a T> for Vec<T> {
    #[inline(always)] fn push(&mut self, item: &'a T) { self.push(item.clone()) }
}
impl<'a, T: Clone> Push<&'a [T]> for Vec<T> {
    #[inline(always)] fn push(&mut self, item: &'a [T]) { self.clone_from_slice(item) }
}
impl<T> Index for Vec<T> {
    type Ref<'a> = &'a T where Self: 'a;
    #[inline(always)] fn get(&self, index: usize) -> Self::Ref<'_> { &self[index] }
}
impl<T> IndexMut for Vec<T> {
    type IndexMut<'a> = &'a mut T where Self: 'a;
    #[inline(always)] fn get_mut(&mut self, index: usize) -> Self::IndexMut<'_> { &mut self[index] }
}
impl<'t, T> Len for &'t [T] {
    #[inline(always)] fn len(&self) -> usize { <[T]>::len(self) }
}
impl<'t, T> Len for &'t mut [T] {
    #[inline(always)] fn len(&self) -> usize { <[T]>::len(self) }
}
impl<'t, T> Index for &'t [T] {
    type Ref<'a> = &'a T where Self: 'a;
    #[inline(always)] fn get(&self, index: usize) -> Self::Ref<'_> { &self[index] }
}
impl<'t, T> Index for &'t mut [T] {
    type Ref<'a> = &'a T where Self: 'a;
    #[inline(always)] fn get(&self, index: usize) -> Self::Ref<'_> { &self[index] }
}
impl<'t, T> IndexMut for &'t mut [T] {
    type IndexMut<'a> = &'a mut T where Self: 'a;
    #[inline(always)] fn get_mut(&mut self, index: usize) -> Self::IndexMut<'_> { &mut self[index] }
}
impl<T> Clear for Vec<T> {
    #[inline(always)] fn clear(&mut self) { self.clear() }
}

/// Types that prefer to be represented by `Vec<T>`.
pub mod primitive {

    /// An implementation of opinions for types that want to use `Vec<T>`.
    macro_rules! implement_columnable {
        ($($index_type:ty),*) => { $(
            impl crate::Columnable for $index_type {
                type Columns = Vec<$index_type>;
            }
            impl crate::HeapSize for $index_type { }
        )* }
    }

    implement_columnable!(char);
    implement_columnable!(u8, u16, u32, u64, u128, usize);
    implement_columnable!(i8, i16, i32, i64, i128, isize);
    implement_columnable!(f32, f64);

    pub use empty::Empties;
    /// A columnar store for `()`.
    mod empty {

        use crate::{Clear, Columnable, Len, Index, IndexMut, Push, HeapSize};

        #[derive(Default)]
        pub struct Empties { pub count: usize, pub empty: () }

        impl Len for Empties {
            fn len(&self) -> usize { self.count }
        }
        impl Index for Empties {
            type Ref<'a> = &'static ();
            // TODO: panic if out of bounds?
            #[inline(always)] fn get(&self, _index: usize) -> Self::Ref<'_> { &() }
        }
        impl IndexMut for Empties {
            type IndexMut<'a> = &'a mut ();
            // TODO: panic if out of bounds?
            #[inline(always)] fn get_mut(&mut self, _index: usize) -> Self::IndexMut<'_> { &mut self.empty }
        }
        impl Push<()> for Empties {
            // TODO: check for overflow?
            fn push(&mut self, _item: ()) { self.count += 1; }
        }
        impl Push<&()> for Empties {
            // TODO: check for overflow?
            fn push(&mut self, _item: &()) { self.count += 1; }
        }

        impl Columnable for () {
            type Columns = Empties;
        }
        impl HeapSize for Empties {
            fn heap_size(&self) -> (usize, usize) { (0, 0) }
        }
        impl Clear for Empties {
            fn clear(&mut self) { self.count = 0; }
        }
    }

    pub use boolean::Bools;
    /// A columnar store for `bool`.
    mod boolean {

        use crate::{Clear, Len, Index, Push, HeapSize};

        impl crate::Columnable for bool {
            type Columns = Bools;
        }

        /// A store for maintaining `Vec<bool>`.
        #[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
        pub struct Bools<VC = Vec<u64>> {
            /// The bundles of bits that form complete `u64` values.
            pub values: VC,
            /// The work-in-progress bits that are not yet complete.
            pub last_word: u64,
            /// The number of set bits in `bits.last()`.
            pub last_bits: u8,
        }


        impl<VC: Len> Len for Bools<VC> {
            #[inline(always)] fn len(&self) -> usize { self.values.len() * 64 + (self.last_bits as usize) }
        }

        // Our `Index` implementation needs `VC` to implement `std::ops::Index`,
        // because I couldn't figure out how to exress the constraint that the
        // `Index::Ref<'_>` type is always `u64` or `&u64` or something similar.
        impl<VC: Len+std::ops::Index<usize, Output=u64>> Index for Bools<VC> {
            type Ref<'a> = bool where VC: 'a;
            #[inline(always)] fn get(&self, index: usize) -> Self::Ref<'_> {
                let block = index / 64;
                let word = if block == self.values.len() {
                    self.last_word
                } else {
                    self.values[block]
                };
                let bit = index % 64;
                (word >> bit) & 1 == 1
            }
        }

        impl<VC: Push<u64>> Push<bool> for Bools<VC> {
            fn push(&mut self, bit: bool) {
                self.last_word |= (bit as u64) << self.last_bits;
                self.last_bits += 1;
                // If we have a fully formed word, commit it to `self.values`.
                if self.last_bits == 64 {
                    self.values.push(self.last_word);
                    self.last_word = 0;
                    self.last_bits = 0;
                }
            }
        }

        impl<VC: Clear> Clear for Bools<VC> {
            fn clear(&mut self) {
                self.values.clear();
                self.last_word = 0;
                self.last_bits = 0;
            }
        }

        impl<VC: HeapSize> HeapSize for Bools<VC> {
            fn heap_size(&self) -> (usize, usize) {
                self.values.heap_size()
            }
        }
    }

    pub use duration::Durations;
    /// A columnar store for `std::time::Duration`.
    mod duration {

        use crate::{Len, Index, Push, Clear, HeapSize};

        impl crate::Columnable for std::time::Duration {
            type Columns = Durations;
        }

        // `std::time::Duration` is equivalent to `(u64, u32)`, corresponding to seconds and nanoseconds.
        #[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
        pub struct Durations<SC = Vec<u64>, NC = Vec<u32>> {
            pub seconds: SC,
            pub nanoseconds: NC,
        }

        impl<SC: Len, NC> Len for Durations<SC, NC> {
            #[inline(always)] fn len(&self) -> usize { self.seconds.len() }
        }

        impl<SC: Index, NC: Index> Index for Durations<SC, NC> {
            type Ref<'a> = (SC::Ref<'a>, NC::Ref<'a>) where SC: 'a, NC: 'a;
            #[inline(always)] fn get(&self, index: usize) -> Self::Ref<'_> {
                (self.seconds.get(index), self.nanoseconds.get(index))
            }
        }

        impl<SC: Push<u64>, NC: Push<u32>> Push<std::time::Duration> for Durations<SC, NC> {
            fn push(&mut self, item: std::time::Duration) {
                self.seconds.push(item.as_secs());
                self.nanoseconds.push(item.subsec_nanos());
            }
        }

        impl<SC: Clear, NC: Clear> Clear for Durations<SC, NC> {
            fn clear(&mut self) {
                self.seconds.clear();
                self.nanoseconds.clear();
            }
        }

        impl<SC: HeapSize, NC: HeapSize> HeapSize for Durations<SC, NC> {
            fn heap_size(&self) -> (usize, usize) {
                let (l0, c0) = self.seconds.heap_size();
                let (l1, c1) = self.nanoseconds.heap_size();
                (l0 + l1, c0 + c1)
            }
        }
    }
}

pub use string::Strings;
pub mod string {

    use std::ops::{Deref, DerefMut};
    use super::{Clear, Columnable, Len, Index, IndexMut, Push, HeapSize};

    impl Columnable for String {
        type Columns = Strings;
    }
    impl Columnable for &str {
        type Columns = Strings;
    }

    /// A stand-in for `Vec<String>`.
    #[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
    pub struct Strings<BC = Vec<usize>, VC = Vec<u8>> {
        /// Bounds container; provides indexed access to offsets.
        pub bounds: BC,
        /// Values container; provides slice access to bytes.
        pub values: VC,
    }

    impl<BC: Len, VC> Len for Strings<BC, VC> {
        #[inline(always)] fn len(&self) -> usize { self.bounds.len() }
    }
    impl<BC: Len+Deref<Target=[usize]>, VC: Deref<Target=[u8]>> Index for Strings<BC, VC> {
        // type Ref<'a> = &'a [u8];
        type Ref<'a> = &'a str where BC: 'a, VC: 'a;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref<'_> {
            let lower = if index == 0 { 0 } else { self.bounds[index - 1] };
            let upper = self.bounds[index];
            std::str::from_utf8(&self.values[lower .. upper]).unwrap()
        }
    }
    // Arguably safe, because we don't assume UTF-8, but also off-brand.
    impl<BC: Len+Deref<Target=[usize]>, VC: DerefMut<Target=[u8]>> IndexMut for Strings<BC, VC> {
        type IndexMut<'a> = &'a mut [u8] where BC: 'a, VC: 'a;
        #[inline(always)] fn get_mut(&mut self, index: usize) -> Self::IndexMut<'_> {
            let lower = if index == 0 { 0 } else { self.bounds[index] };
            let upper = self.bounds[index];
            &mut self.values[lower .. upper]
        }
    }

    impl<BC: Push<usize>> Push<String> for Strings<BC> {
        #[inline(always)] fn push(&mut self, item: String) {
            self.values.extend_from_slice(item.as_bytes());
            self.bounds.push(self.values.len());
        }
    }
    impl<BC: Push<usize>> Push<&String> for Strings<BC> {
        #[inline(always)] fn push(&mut self, item: &String) {
            self.values.extend_from_slice(item.as_bytes());
            self.bounds.push(self.values.len());
        }
    }
    impl Push<&str> for Strings {
        fn push(&mut self, item: &str) {
            self.values.extend_from_slice(item.as_bytes());
            self.bounds.push(self.values.len());
        }
    }
    impl Clear for Strings {
        fn clear(&mut self) {
            self.bounds.clear();
            self.values.clear();
        }
    }
    impl HeapSize for Strings {
        fn heap_size(&self) -> (usize, usize) {
            let bl = std::mem::size_of::<usize>() * self.bounds.len();
            let bc = std::mem::size_of::<usize>() * self.bounds.capacity();
            let vl = self.values.len();
            let vc = self.values.capacity();
            (bl + vl, bc + vc)
        }
    }
}

pub use vector::Vecs;
pub mod vector {

    use std::ops::Deref;
    use super::{Clear, Columnable, Len, Index, IndexMut, Push, HeapSize, Slice};

    impl<T: Columnable> Columnable for Vec<T> {
        type Columns = Vecs<T::Columns>;
    }
    impl<'a, T: Columnable> Columnable for &'a [T] where T::Columns : Push<&'a T> {
        type Columns = Vecs<T::Columns>;
    }

    /// A stand-in for `Vec<Vec<T>>` for complex `T`.
    #[derive(Debug, Default, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    pub struct Vecs<TC, BC = Vec<usize>> {
        pub bounds: BC,
        pub values: TC,
    }

    impl<TC: Len> Vecs<TC> {
        pub fn push_iter<I>(&mut self, iter: I) where I: IntoIterator, TC: Push<I::Item> {
            self.values.extend(iter);
            self.bounds.push(self.values.len());
        }
    }

    impl<TC, BC: Len> Len for Vecs<TC, BC> {
        #[inline(always)] fn len(&self) -> usize { self.bounds.len() }
    }

    impl<TC, BC: Len+Deref<Target=[usize]>> Index for Vecs<TC, BC> {
        type Ref<'a> = Slice<&'a TC> where TC: 'a, BC: 'a;

        #[inline(always)]
        fn get(&self, index: usize) -> Self::Ref<'_> {
            let lower = if index == 0 { 0 } else { self.bounds[index - 1] };
            let upper = self.bounds[index];
            Slice::new(lower, upper, &self.values)
        }
    }
    impl<TC, BC: Len+Deref<Target=[usize]>> IndexMut for Vecs<TC, BC> {
        type IndexMut<'a> = Slice<&'a mut TC> where TC: 'a, BC: 'a;

        #[inline(always)]
        fn get_mut(&mut self, index: usize) -> Self::IndexMut<'_> {
            let lower = if index == 0 { 0 } else { self.bounds[index - 1] };
            let upper = self.bounds[index];
            Slice::new(lower, upper, &mut self.values)
        }
    }

    impl<T, TC: Push<T> + Len> Push<Vec<T>> for Vecs<TC> {
        fn push(&mut self, item: Vec<T>) {
            self.values.extend(item);
            self.bounds.push(self.values.len());
        }
    }
    impl<'a, T, TC: Push<&'a T> + Len> Push<&'a Vec<T>> for Vecs<TC> {
        fn push(&mut self, item: &'a Vec<T>) {
            self.push(&item[..]);
        }
    }
    impl<'a, T, TC: Push<&'a T> + Len> Push<&'a [T]> for Vecs<TC> {
        fn push(&mut self, item: &'a [T]) {
            self.values.extend(item.iter());
            self.bounds.push(self.values.len());
        }
    }
    impl<TC: Clear> Clear for Vecs<TC> {
        fn clear(&mut self) {
            self.bounds.clear();
            self.values.clear();
        }
    }

    impl<TC: HeapSize> HeapSize for Vecs<TC> {
        fn heap_size(&self) -> (usize, usize) {
            let (inner_l, inner_c) = self.values.heap_size();
            (
                std::mem::size_of::<usize>() * self.bounds.len() + inner_l,
                std::mem::size_of::<usize>() * self.bounds.capacity() + inner_c,
            )
        }
    }
}

#[allow(non_snake_case)]
pub mod tuple {

    use super::{Clear, Columnable, Len, Index, IndexMut, Push, HeapSize};

    // Implementations for tuple types.
    // These are all macro based, because the implementations are very similar.
    // The macro requires two names, one for the store and one for pushable types.
    macro_rules! tuple_impl {
        ( $($name:ident,$name2:ident)+) => (
            impl<$($name: Columnable),*> Columnable for ($($name,)*) {
                type Columns = ($($name::Columns,)*);
            }
            impl<$($name: Len),*> Len for ($($name,)*) {
                fn len(&self) -> usize {
                    self.0.len()
                }
            }
            impl<$($name: Clear),*> Clear for ($($name,)*) {
                fn clear(&mut self) {
                    let ($($name,)*) = self;
                    $($name.clear();)*
                }
            }
            impl<$($name: HeapSize),*> HeapSize for ($($name,)*) {
                fn heap_size(&self) -> (usize, usize) {
                    let ($($name,)*) = self;
                    let mut l = 0;
                    let mut c = 0;
                    $(let (l0, c0) = $name.heap_size(); l += l0; c += c0;)*
                    (l, c)
                }
            }
            impl<$($name: Index),*> Index for ($($name,)*) {
                type Ref<'a> = ($($name::Ref<'a>,)*) where $($name: 'a),*;
                fn get(&self, index: usize) -> Self::Ref<'_> {
                    let ($($name,)*) = self;
                    ($($name.get(index),)*)
                }
            }
            impl<$($name: IndexMut),*> IndexMut for ($($name,)*) {
                type IndexMut<'a> = ($($name::IndexMut<'a>,)*) where $($name: 'a),*;
                fn get_mut(&mut self, index: usize) -> Self::IndexMut<'_> {
                    let ($($name,)*) = self;
                    ($($name.get_mut(index),)*)
                }
            }
            impl<$($name2, $name: Push<$name2>),*> Push<($($name2,)*)> for ($($name,)*) {
                fn push(&mut self, item: ($($name2,)*)) {
                    let ($($name,)*) = self;
                    let ($($name2,)*) = item;
                    $($name.push($name2);)*
                }
            }
            impl<'a, $($name2, $name: Push<&'a $name2>),*> Push<&'a ($($name2,)*)> for ($($name,)*) {
                fn push(&mut self, item: &'a ($($name2,)*)) {
                    let ($($name,)*) = self;
                    let ($($name2,)*) = item;
                    $($name.push($name2);)*
                }
            }
        )
    }

    tuple_impl!(A,AA);
    tuple_impl!(A,AA B,BB);
    tuple_impl!(A,AA B,BB C,CC);
    tuple_impl!(A,AA B,BB C,CC D,DD);
    tuple_impl!(A,AA B,BB C,CC D,DD E,EE);
    tuple_impl!(A,AA B,BB C,CC D,DD E,EE F,FF);
    tuple_impl!(A,AA B,BB C,CC D,DD E,EE F,FF G,GG);
    tuple_impl!(A,AA B,BB C,CC D,DD E,EE F,FF G,GG H,HH);
    tuple_impl!(A,AA B,BB C,CC D,DD E,EE F,FF G,GG H,HH I,II);
    tuple_impl!(A,AA B,BB C,CC D,DD E,EE F,FF G,GG H,HH I,II J,JJ);

    #[cfg(test)]
    mod test {
        #[test]
        fn round_trip() {

            use crate::Columnable;
            use crate::common::{Index, Push, HeapSize, Len};

            let mut column: <(usize, u8, String) as Columnable>::Columns = Default::default();
            for i in 0..100 {
                column.push((i, i as u8, i.to_string()));
                column.push((i, i as u8, "".to_string()));
            }

            assert_eq!(column.len(), 200);
            assert_eq!(column.heap_size(), (3590, 4608));

            for i in 0..100 {
                assert_eq!(column.get(2*i+0), (&i, &(i as u8), i.to_string().as_str()));
                assert_eq!(column.get(2*i+1), (&i, &(i as u8), ""));
            }

            // Compare to the heap size of a `Vec<Option<usize>>`.
            let mut column: Vec<(usize, u8, String)> = Default::default();
            for i in 0..100 {
                column.push((i, i as u8, i.to_string()));
                column.push((i, i as u8, "".to_string()));
            }
            assert_eq!(column.heap_size(), (8000, 10240));

        }
    }
}

pub use sums::{BitsRank, result::Results, option::Options};
/// Containers for enumerations ("sum types") that store variants separately.
///
/// The main work of these types is storing a discriminant and index efficiently,
/// as containers for each of the variant types can hold the actual data.
pub mod sums {

    use crate::primitive::Bools;
    use crate::{Len, Index, Push, Clear, HeapSize};

    /// A store for maintaining `Vec<bool>` with fast rank access.
    /// This is not "succinct" in the technical sense, but it has
    /// similar goals.
    #[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
    pub struct BitsRank<CC = Vec<u64>, VC = Vec<u64>> {
        /// Counts of the number of cumulative set (true) bits,
        /// *after* each block of 64 bits.
        pub counts: CC,
        /// The bits themselves.
        pub values: Bools<VC>,
    }

    impl<CC, VC: Len+std::ops::Index<usize, Output=u64>> BitsRank<CC, VC> {
        #[inline] pub fn get(&self, index: usize) -> bool {
            self.values.get(index)
        }
    }
    impl<CC: std::ops::Index<usize, Output=u64>, VC: Len+std::ops::Index<usize, Output=u64>> BitsRank<CC, VC> {
        /// The number of set bits *strictly* preceding `index`.
        pub fn rank(&self, index: usize) -> usize {
            let block = index / 64;
            let bit = index % 64;
            let inter_count = if block == 0 { 0 } else { self.counts[block - 1] } as usize;
            let intra_word = if block == self.values.values.len() { self.values.last_word } else { self.values.values[block] };
            let intra_count = (intra_word & ((1 << bit) - 1)).count_ones() as usize;
            inter_count + intra_count
        }
    }
    impl<CC, VC: Len> BitsRank<CC, VC> {
        fn len(&self) -> usize {
            self.values.len()
        }
    }

    impl BitsRank {
        #[inline] pub fn push(&mut self, bit: bool) {
            self.values.push(bit);
            while self.counts.len() < self.values.len() / 64 {
                let last_count = self.counts.last().copied().unwrap_or(0);
                self.counts.push(last_count + (self.values.values.last().unwrap().count_ones() as u64));
            }
        }
        pub fn clear(&mut self) {
            self.counts.clear();
            self.values.clear();
        }
        pub fn heap_size(&self) -> (usize, usize) {
            let (l0, c0) = self.counts.heap_size();
            let (l1, c1) = self.values.heap_size();
            (l0 + l1, c0 + c1)
        }
    }

    pub mod result {

        use std::ops::Deref;

        use super::super::{Clear, Columnable, Len, Index, IndexMut, Push, HeapSize};
        use super::BitsRank;

        impl<S: Columnable, T: Columnable> Columnable for Result<S, T> {
            type Columns = Results<S::Columns, T::Columns>;
        }

        #[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
        pub struct Results<SC, TC, CC=Vec<u64>, VC=Vec<u64>> {
            /// Uses two bits for each item, one to indicate the variant and one (amortized)
            /// to enable efficient rank determination.
            pub indexes: BitsRank<CC, VC>,
            pub oks: SC,
            pub errs: TC,
        }

        impl<SC, TC, CC, VC: Len> Len for Results<SC, TC, CC, VC> {
            #[inline(always)] fn len(&self) -> usize { self.indexes.len() }
        }

        impl<SC: Index, TC: Index, CC: Deref<Target = [u64]>+std::ops::Index<usize, Output=u64>, VC: Len+Deref<Target = [u64]>+std::ops::Index<usize, Output=u64>> Index for Results<SC, TC, CC, VC> {
            type Ref<'a> = Result<SC::Ref<'a>, TC::Ref<'a>> where SC: 'a, TC: 'a, CC: 'a, VC: 'a;
            fn get(&self, index: usize) -> Self::Ref<'_> {
                if self.indexes.get(index) {
                    Ok(self.oks.get(self.indexes.rank(index)))
                } else {
                    Err(self.errs.get(index - self.indexes.rank(index)))
                }
            }
        }
        // NB: You are not allowed to change the variant, but can change its contents.
        impl<SC: IndexMut, TC: IndexMut, CC: Deref<Target = [u64]>+std::ops::Index<usize, Output=u64>, VC: Len+Deref<Target = [u64]>+std::ops::Index<usize, Output=u64>> IndexMut for Results<SC, TC, CC, VC> {
            type IndexMut<'a> = Result<SC::IndexMut<'a>, TC::IndexMut<'a>> where SC: 'a, TC: 'a, CC: 'a, VC: 'a;
            fn get_mut(&mut self, index: usize) -> Self::IndexMut<'_> {
                if self.indexes.get(index) {
                    Ok(self.oks.get_mut(self.indexes.rank(index)))
                } else {
                    Err(self.errs.get_mut(index - self.indexes.rank(index)))
                }
            }
        }

        impl<S, SC: Push<S> + Len, T, TC: Push<T> + Len> Push<Result<S, T>> for Results<SC, TC> {
            fn push(&mut self, item: Result<S, T>) {
                match item {
                    Ok(item) => {
                        self.indexes.push(true);
                        self.oks.push(item);
                    }
                    Err(item) => {
                        self.indexes.push(false);
                        self.errs.push(item);
                    }
                }
            }
        }
        impl<'a, S, SC: Push<&'a S> + Len, T, TC: Push<&'a T> + Len> Push<&'a Result<S, T>> for Results<SC, TC> {
            fn push(&mut self, item: &'a Result<S, T>) {
                match item {
                    Ok(item) => {
                        self.indexes.push(true);
                        self.oks.push(item);
                    }
                    Err(item) => {
                        self.indexes.push(false);
                        self.errs.push(item);
                    }
                }
            }
        }

        impl<SC: Clear, TC: Clear> Clear for Results<SC, TC> {
            fn clear(&mut self) {
                self.indexes.clear();
                self.oks.clear();
                self.errs.clear();
            }
        }

        impl<SC: HeapSize, TC: HeapSize> HeapSize for Results<SC, TC> {
            fn heap_size(&self) -> (usize, usize) {
                let (l0, c0) = self.oks.heap_size();
                let (l1, c1) = self.errs.heap_size();
                let (li, ci) = self.indexes.heap_size();
                (l0 + l1 + li, c0 + c1 + ci)
            }
        }

        #[cfg(test)]
        mod test {
            #[test]
            fn round_trip() {

                use crate::Columnable;
                use crate::common::{Index, Push, HeapSize, Len};

                let mut column: <Result<usize, usize> as Columnable>::Columns = Default::default();
                for i in 0..100 {
                    column.push(Ok::<usize, usize>(i));
                    column.push(Err::<usize, usize>(i));
                }

                assert_eq!(column.len(), 200);
                assert_eq!(column.heap_size(), (1656, 2112));

                for i in 0..100 {
                    assert_eq!(column.get(2*i+0), Ok(&i));
                    assert_eq!(column.get(2*i+1), Err(&i));
                }

                let mut column: <Result<usize, u8> as Columnable>::Columns = Default::default();
                for i in 0..100 {
                    column.push(Ok::<usize, u8>(i));
                    column.push(Err::<usize, u8>(i as u8));
                }

                assert_eq!(column.len(), 200);
                assert_eq!(column.heap_size(), (956, 1216));

                for i in 0..100 {
                    assert_eq!(column.get(2*i+0), Ok(&i));
                    assert_eq!(column.get(2*i+1), Err(&(i as u8)));
                }

                // Compare to the heap size of a `Vec<Option<usize>>`.
                let mut column: Vec<Result<usize, usize>> = Default::default();
                for i in 0..100 {
                    column.push(Ok::<usize, usize>(i));
                    column.push(Err::<usize, usize>(i));
                }
                assert_eq!(column.heap_size(), (3200, 4096));

            }
        }
    }

    pub mod option {

        use crate::{Clear, Columnable, Len, Index, IndexMut, Push, HeapSize};
        use super::BitsRank;

        impl<T: Columnable> Columnable for Option<T> {
            type Columns = Options<T::Columns>;
        }

        #[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
        pub struct Options<TC, CC=Vec<u64>, VC=Vec<u64>> {
            /// Uses two bits for each item, one to indicate the variant and one (amortized)
            /// to enable efficient rank determination.
            pub indexes: BitsRank<CC, VC>,
            pub somes: TC,
        }

        impl<T, CC, VC: Len> Len for Options<T, CC, VC> {
            #[inline(always)] fn len(&self) -> usize { self.indexes.len() }
        }

        impl<TC: Index, CC: std::ops::Index<usize, Output=u64>, VC: Len+std::ops::Index<usize, Output=u64>> Index for Options<TC, CC, VC> {
            type Ref<'a> = Option<TC::Ref<'a>> where TC: 'a, CC: 'a, VC: 'a;
            fn get(&self, index: usize) -> Self::Ref<'_> {
                if self.indexes.get(index) {
                    Some(self.somes.get(self.indexes.rank(index)))
                } else {
                    None
                }
            }
        }
        impl<TC: IndexMut, CC: std::ops::Index<usize, Output=u64>, VC: Len+std::ops::Index<usize, Output=u64>> IndexMut for Options<TC, CC, VC> {
            type IndexMut<'a> = Option<TC::IndexMut<'a>> where TC: 'a, CC: 'a, VC: 'a;
            fn get_mut(&mut self, index: usize) -> Self::IndexMut<'_> {
                if self.indexes.get(index) {
                    Some(self.somes.get_mut(self.indexes.rank(index)))
                } else {
                    None
                }
            }
        }

        impl<T, TC: Push<T> + Len> Push<Option<T>> for Options<TC> {
            fn push(&mut self, item: Option<T>) {
                match item {
                    Some(item) => {
                        self.indexes.push(true);
                        self.somes.push(item);
                    }
                    None => {
                        self.indexes.push(false);
                    }
                }
            }
        }
        impl<'a, T, TC: Push<&'a T> + Len> Push<&'a Option<T>> for Options<TC> {
            fn push(&mut self, item: &'a Option<T>) {
                match item {
                    Some(item) => {
                        self.indexes.push(true);
                        self.somes.push(item);
                    }
                    None => {
                        self.indexes.push(false);
                    }
                }
            }
        }

        impl<TC: Clear> Clear for Options<TC> {
            fn clear(&mut self) {
                self.indexes.clear();
                self.somes.clear();
            }
        }

        impl<TC: HeapSize> HeapSize for Options<TC> {
            fn heap_size(&self) -> (usize, usize) {
                let (l0, c0) = self.somes.heap_size();
                let (li, ci) = self.indexes.heap_size();
                (l0 + li, c0 + ci)
            }
        }

        #[cfg(test)]
        mod test {

            use crate::Columnable;
            use crate::common::{Index, HeapSize, Len};

            #[test]
            fn round_trip_some() {
                let store = Columnable::as_columns((0..100).map(Some));
                assert_eq!(store.len(), 100);
                assert!(store.iter().zip(0..100).all(|(a, b)| a == Some(&b)));
                assert_eq!(store.heap_size(), (424, 576));
            }

            #[test]
            fn round_trip_none() {
                let store = Columnable::as_columns((0..100).map(|_x| None::<i32>));
                assert_eq!(store.len(), 100);
                assert!(store.iter().zip(0..100).all(|(a, _b)| a == None));
                assert_eq!(store.heap_size(), (24, 64));
            }

            #[test]
            fn round_trip_mixed() {
                let store = Columnable::as_columns((0..100).map(|x| if x % 2 == 0 { Some(x) } else { None }));
                assert_eq!(store.len(), 100);
                assert!(store.iter().zip(0..100).all(|(a, b)| a == if b % 2 == 0 { Some(&b) } else { None }));
                assert_eq!(store.heap_size(), (224, 320));
            }
        }
    }
}

pub use lookback::{Repeats, Lookbacks};
/// Containers that can store either values, or offsets to prior values.
///
/// This has the potential to be more efficient than a list of `T` when many values repeat in
/// close proximity. Values must be equatable, and the degree of lookback can be configured.
pub mod lookback {

    use crate::{Options, Results, Push, Index, Len, HeapSize};

    /// A container that encodes repeated values with a `None` variant, at the cost of extra bits for every record.
    #[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
    pub struct Repeats<TC, const N: u8 = 255> {
        /// Some(x) encodes a value, and None indicates the prior `x` value.
        pub inner: Options<TC>,
    }

    impl<T: PartialEq, TC: Push<T> + Index, const N: u8> Push<T> for Repeats<TC, N>
    where
        for<'a> TC::Ref<'a> : PartialEq<T>,
    {
        fn push(&mut self, item: T) {
            // Look at the last `somes` value for a potential match.
            let insert: Option<T> = if self.inner.somes.last().map(|x| x.eq(&item)) == Some(true) {
                None
            } else {
                Some(item)
            };
            self.inner.push(insert);
        }
    }

    impl<TC: Len, const N: u8> Len for Repeats<TC, N> {
        #[inline(always)] fn len(&self) -> usize { self.inner.len() }
    }

    impl<TC: Index, const N: u8> Index for Repeats<TC, N> {
        type Ref<'a> = TC::Ref<'a> where TC: 'a;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref<'_> {
            match self.inner.get(index) {
                Some(item) => item,
                None => {
                    let pos = self.inner.indexes.rank(index) - 1;
                    self.inner.somes.get(pos)
                },
            }
        }
    }

    impl<TC: HeapSize, const N: u8> HeapSize for Repeats<TC, N> {
        fn heap_size(&self) -> (usize, usize) {
            self.inner.heap_size()
        }
    }

    #[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
    pub struct Lookbacks<TC, const N: u8 = 255> {
        /// Ok(x) encodes a value, and Err(y) indicates a value `y` back.
        pub inner: Results<TC, Vec<u8>>,
    }

    impl<T: PartialEq, TC: Push<T> + Index, const N: u8> Push<T> for Lookbacks<TC, N>
    where
        for<'a> TC::Ref<'a> : PartialEq<T>,
    {
        fn push(&mut self, item: T) {
            // Look backwards through (0 .. N) to look for a matching value.
            let oks_len = self.inner.oks.len();
            let find = (0u8 .. N).take(self.inner.oks.len()).find(|i| self.inner.oks.get(oks_len - (*i as usize) - 1) == item);
            let insert: Result<T, u8> = if let Some(back) = find { Err(back) } else { Ok(item) };
            self.inner.push(insert);
        }
    }

    impl<TC: Len, const N: u8> Len for Lookbacks<TC, N> {
        #[inline(always)] fn len(&self) -> usize { self.inner.len() }
    }

    impl<TC: Index, const N: u8> Index for Lookbacks<TC, N> {
        type Ref<'a> = TC::Ref<'a> where TC: 'a;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref<'_> {
            match self.inner.get(index) {
                Ok(item) => item,
                Err(back) => {
                    let pos = self.inner.indexes.rank(index) - 1;
                    self.inner.oks.get(pos - (*back as usize))
                },
            }
        }
    }

    impl<TC: HeapSize, const N: u8> HeapSize for Lookbacks<TC, N> {
        fn heap_size(&self) -> (usize, usize) {
            self.inner.heap_size()
        }
    }
}
