//! Types supporting flat / "columnar" layout for complex types.
//!
//! The intent is to re-layout `Vec<T>` types into vectors of reduced
//! complexity, repeatedly. One should be able to push and pop easily,
//! but indexing will be more complicated because we likely won't have
//! a real `T` lying around to return as a reference. Instead, we will
//! use Generic Associated Types (GATs) to provide alternate references.

// Re-export derive crate.
extern crate columnar_derive;
pub use columnar_derive::Columnar;

pub mod bytes;
pub mod adts;

/// A type that can be represented in columnar form.
pub trait Columnar : Sized {
    /// The type that stores the columnar representation.
    type Container: Push<Self> + Len + Clear + Default + bytes::AsBytes + for<'a> Push<&'a Self>;

    /// Converts a sequence of the type into columnar form.
    fn as_columns<'a, I>(selves: I) -> Self::Container where I: IntoIterator<Item =&'a Self>, Self: 'a {
        let mut columns: Self::Container = Default::default();
        for item in selves {
            columns.push(item);
        }
        columns
    }
    fn into_columns<I>(selves: I) -> Self::Container where I: IntoIterator<Item = Self> {
        let mut columns: Self::Container = Default::default();
        for item in selves {
            columns.push(item);
        }
        columns
    }
}

pub use common::{Clear, Len, Push, IndexMut, Index, IndexAs, HeapSize, Slice};
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
    impl<T> Len for Vec<T> {
        #[inline(always)] fn len(&self) -> usize { self.len() }
    }
    impl<T> Len for [T] {
        #[inline(always)] fn len(&self) -> usize { <[T]>::len(self) }
    }
    impl<T> Len for &[T] {
        #[inline(always)] fn len(&self) -> usize { <[T]>::len(self) }
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
    impl<T> Push<T> for Vec<T> {
        #[inline(always)] fn push(&mut self, item: T) { self.push(item) }
    }
    impl<'a, T: Clone> Push<&'a T> for Vec<T> {
        #[inline(always)] fn push(&mut self, item: &'a T) { self.push(item.clone()) }
    }
    impl<'a, T: Clone> Push<&'a [T]> for Vec<T> {
        #[inline(always)] fn push(&mut self, item: &'a [T]) { self.clone_from_slice(item) }
    }


    pub use index::{Index, IndexMut, IndexAs};
    /// Traits for accessing elements by `usize` indexes.
    ///
    /// There are several traits, with a core distinction being whether the returned reference depends on the lifetime of `&self`.
    /// For one trait `Index` the result does not depend on this lifetime.
    /// There is a third trait `IndexMut` that allows mutable access, that may be less commonly implemented.
    pub mod index {

        use crate::Len;
        use crate::common::IterOwn;

        /// A type that can be mutably accessed by `usize`.
        pub trait IndexMut {
            /// Type mutably referencing an indexed element.
            type IndexMut<'a> where Self: 'a;
            fn get_mut(& mut self, index: usize) -> Self::IndexMut<'_>;
            /// A reference to the last element, should one exist.
            #[inline(always)] fn last_mut(&mut self) -> Option<Self::IndexMut<'_>> where Self: Len {
                if self.is_empty() { None }
                else { Some(self.get_mut(self.len()-1)) }
            }
        }

        impl<'t, T: IndexMut + ?Sized> IndexMut for &'t mut T {
            type IndexMut<'a> = T::IndexMut<'a> where Self: 'a;
            #[inline(always)] fn get_mut(&mut self, index: usize) -> Self::IndexMut<'_> {
                T::get_mut(*self, index)
            }
        }
        impl<T> IndexMut for Vec<T> {
            type IndexMut<'a> = &'a mut T where Self: 'a;
            #[inline(always)] fn get_mut(&mut self, index: usize) -> Self::IndexMut<'_> { &mut self[index] }
        }
        impl<T> IndexMut for [T] {
            type IndexMut<'a> = &'a mut T where Self: 'a;
            #[inline(always)] fn get_mut(&mut self, index: usize) -> Self::IndexMut<'_> { &mut self[index] }
        }

        /// A type that can be accessed by `usize` but without borrowing `self`.
        ///
        /// This can be useful for types which include their own lifetimes, and
        /// which wish to express that their reference has the same lifetime.
        /// In the GAT `Index`, the `Ref<'_>` lifetime would be tied to `&self`.
        ///
        /// This trait may be challenging to implement for owning containers,
        /// for example `Vec<_>`, which would need their `Ref` type to depend 
        /// on the lifetime of the `&self` borrow in the `get()` function.
        pub trait Index {
            /// The type returned by the `get` method.
            ///
            /// Notably, this does not vary with lifetime, and will not depend on the lifetime of `&self`.
            type Ref;
            fn get(&self, index: usize) -> Self::Ref;
            #[inline(always)] fn last(&self) -> Option<Self::Ref> where Self: Len {
                if self.is_empty() { None }
                else { Some(self.get(self.len()-1)) }
            }
            fn iter(&self) -> IterOwn<&Self> {
                IterOwn {
                    index: 0,
                    slice: self,
                }
            }
            fn into_iter(self) -> IterOwn<Self> where Self: Sized {
                IterOwn {
                    index: 0,
                    slice: self,
                }
            }
        }

        // These implementations aim to reveal a longer lifetime, or to copy results to avoid a lifetime.
        impl<'a, T> Index for &'a [T] {
            type Ref = &'a T;
            #[inline(always)] fn get(&self, index: usize) -> Self::Ref { &self[index] }
        }
        impl<T: Copy> Index for [T] {
            type Ref = T;
            #[inline(always)] fn get(&self, index: usize) -> Self::Ref { self[index] }
        }
        impl<'a, T> Index for &'a Vec<T> {
            type Ref = &'a T;
            #[inline(always)] fn get(&self, index: usize) -> Self::Ref { &self[index] }
        }
        impl<T: Copy> Index for Vec<T> {
            type Ref = T;
            #[inline(always)] fn get(&self, index: usize) -> Self::Ref { self[index] }
        }


        /// Types that can be converted into another type by copying.
        ///
        /// We use this trait to unify the ability of `T` and `&T` to be converted into `T`.
        /// This is handy for copy types that we'd like to use, like `u8`, `u64` and `usize`.
        pub trait CopyAs<T> {
            fn copy_as(self) -> T;
        }
        impl<T: Copy> CopyAs<T> for &T {
            fn copy_as(self) -> T { *self }
        }
        impl<T> CopyAs<T> for T {
            fn copy_as(self) -> T { self }
        }

        pub trait IndexAs<T> {
            fn index_as(&self, index: usize) -> T;
            fn last(&self) -> Option<T> where Self: Len {
                if self.is_empty() { None }
                else { Some(self.index_as(self.len()-1)) }
            }
        }

        impl<T: Index, S> IndexAs<S> for T where T::Ref: CopyAs<S> {
            fn index_as(&self, index: usize) -> S { self.get(index).copy_as() }
        }
    }

    /// A type that can remove its contents and return to an empty state.
    ///
    /// Generally, this method does not release resources, and is used to make the container available for re-insertion.
    pub trait Clear {
        /// Clears `self`, without changing its capacity.
        fn clear(&mut self);
    }
    // Vectors can be cleared, unlike slices.
    impl<T> Clear for Vec<T> {
        #[inline(always)] fn clear(&mut self) { self.clear() }
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
    impl<T: HeapSize> HeapSize for [T] {
        fn heap_size(&self) -> (usize, usize) {
            let mut l = std::mem::size_of_val(self);
            let mut c = std::mem::size_of_val(self);
            for item in self.iter() {
                let (il, ic) = item.heap_size();
                l += il;
                c += ic;
            }
            (l, c)
        }
    }
    impl<T: HeapSize> HeapSize for Vec<T> {
        fn heap_size(&self) -> (usize, usize) {
            let mut l = std::mem::size_of::<T>() * self.len();
            let mut c = std::mem::size_of::<T>() * self.capacity();
            for item in (self[..]).iter() {
                let (il, ic) = item.heap_size();
                l += il;
                c += ic;
            }
            (l, c)
        }
    }

    /// A struct representing a slice of a range of values.
    ///
    /// The lower and upper bounds should be meaningfully set on construction.
    #[derive(Copy, Clone, Debug)]
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
        pub fn len(&self) -> usize { self.upper - self.lower }
    }

    impl<S: Index> PartialEq<[S::Ref]> for Slice<S> where S::Ref: PartialEq {
        fn eq(&self, other: &[S::Ref]) -> bool {
            if self.len() != other.len() { return false; }
            for i in 0 .. self.len() {
                if self.get(i) != other[i] { return false; }
            }
            true
        }
    }
    impl<S: Index> PartialEq<Vec<S::Ref>> for Slice<S> where S::Ref: PartialEq {
        fn eq(&self, other: &Vec<S::Ref>) -> bool {
            if self.len() != other.len() { return false; }
            for i in 0 .. self.len() {
                if self.get(i) != other[i] { return false; }
            }
            true
        }
    }

    impl<S> Len for Slice<S> {
        #[inline(always)] fn len(&self) -> usize { self.len() }
    }

    impl<S: Index> Index for Slice<S> {
        type Ref = S::Ref;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref {
            assert!(index < self.upper - self.lower);
            self.slice.get(self.lower + index)
        }
    }
    impl<'a, S> Index for &'a Slice<S>
    where
        &'a S : Index,
    {
        type Ref = <&'a S as Index>::Ref;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref {
            assert!(index < self.upper - self.lower);
            (&self.slice).get(self.lower + index)
        }
    }

    impl<S: IndexMut> IndexMut for Slice<S> {
        type IndexMut<'a> = S::IndexMut<'a> where S: 'a;
        #[inline(always)] fn get_mut(&mut self, index: usize) -> Self::IndexMut<'_> {
            assert!(index < self.upper - self.lower);
            self.slice.get_mut(self.lower + index)
        }
    }

    pub struct IterOwn<S> {
        index: usize,
        slice: S,
    }

    impl<S> IterOwn<S> {
        pub fn new(index: usize, slice: S) -> Self {
            Self { index, slice }
        }
    }

    impl<S: Index + Len> Iterator for IterOwn<S> {
        type Item = S::Ref;
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


/// Types that prefer to be represented by `Vec<T>`.
pub mod primitive {

    /// An implementation of opinions for types that want to use `Vec<T>`.
    macro_rules! implement_columnable {
        ($($index_type:ty),*) => { $(
            impl crate::Columnar for $index_type {
                type Container = Vec<$index_type>;
            }
            impl crate::HeapSize for $index_type { }
        )* }
    }

    implement_columnable!(u8, u16, u32, u64, u128, usize);
    implement_columnable!(i8, i16, i32, i64, i128, isize);
    implement_columnable!(f32, f64);

    pub use empty::Empties;
    /// A columnar store for `()`.
    mod empty {

        use crate::{Clear, Columnar, Len, IndexMut, Index, Push, HeapSize};

        #[derive(Default)]
        pub struct Empties { pub count: usize, pub empty: () }

        impl Len for Empties {
            fn len(&self) -> usize { self.count }
        }
        impl IndexMut for Empties {
            type IndexMut<'a> = &'a mut ();
            // TODO: panic if out of bounds?
            #[inline(always)] fn get_mut(&mut self, _index: usize) -> Self::IndexMut<'_> { &mut self.empty }
        }
        impl Index for Empties {
            type Ref = ();
            fn get(&self, _index: usize) -> Self::Ref { () }
        }
        impl<'a> Index for &'a Empties {
            type Ref = &'a ();
            fn get(&self, _index: usize) -> Self::Ref { &() }
        }
        impl Push<()> for Empties {
            // TODO: check for overflow?
            fn push(&mut self, _item: ()) { self.count += 1; }
        }
        impl Push<&()> for Empties {
            // TODO: check for overflow?
            fn push(&mut self, _item: &()) { self.count += 1; }
        }

        impl Columnar for () {
            type Container = Empties;
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

        use crate::{Clear, Len, Index, IndexAs, Push, HeapSize};

        impl crate::Columnar for bool {
            type Container = Bools;
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

        impl<VC: Len + IndexAs<u64>> Index for Bools<VC> {
            type Ref = bool;
            #[inline(always)] fn get(&self, index: usize) -> Self::Ref {
                let block = index / 64;
                let word = if block == self.values.len() {
                    self.last_word
                } else {
                    self.values.index_as(block)
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
        impl<'a, VC: Push<u64>> Push<&'a bool> for Bools<VC> {
            fn push(&mut self, bit: &'a bool) {
                self.push(*bit)
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

        impl crate::Columnar for std::time::Duration {
            type Container = Durations;
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
            type Ref = (SC::Ref, NC::Ref);
            #[inline(always)] fn get(&self, index: usize) -> Self::Ref {
                (self.seconds.get(index), self.nanoseconds.get(index))
            }
        }

        impl<SC: Push<u64>, NC: Push<u32>> Push<std::time::Duration> for Durations<SC, NC> {
            fn push(&mut self, item: std::time::Duration) {
                self.seconds.push(item.as_secs());
                self.nanoseconds.push(item.subsec_nanos());
            }
        }
        impl<'a, SC: Push<u64>, NC: Push<u32>> Push<&'a std::time::Duration> for Durations<SC, NC> {
            fn push(&mut self, item: &'a std::time::Duration) {
                self.push(*item)
            }
        }
        impl<'a, SC: Push<&'a u64>, NC: Push<&'a u32>> Push<(&'a u64, &'a u32)> for Durations<SC, NC> {
            fn push(&mut self, item: (&'a u64, &'a u32)) {
                self.seconds.push(item.0);
                self.nanoseconds.push(item.1);
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

    use super::{Clear, Columnar, Len, Index, IndexAs, Push, HeapSize};

    impl Columnar for String {
        type Container = Strings;
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

    impl<'a, BC: Len+IndexAs<usize>> Index for Strings<BC, &'a [u8]> {
        type Ref = &'a str;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref {
            let lower = if index == 0 { 0 } else { self.bounds.index_as(index - 1) };
            let upper = self.bounds.index_as(index);
            std::str::from_utf8(&self.values[lower .. upper]).unwrap()
        }
    }
    impl<'a, BC: Len+IndexAs<usize>> Index for &'a Strings<BC, Vec<u8>> {
        type Ref = &'a str;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref {
            let lower = if index == 0 { 0 } else { self.bounds.index_as(index - 1) };
            let upper = self.bounds.index_as(index);
            std::str::from_utf8(&self.values[lower .. upper]).unwrap()
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
    impl<BC: Push<usize>> Push<&str> for Strings<BC> {
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

    use super::{Clear, Columnar, Len, IndexMut, Index, IndexAs, Push, HeapSize, Slice};

    impl<T: Columnar> Columnar for Vec<T> {
        type Container = Vecs<T::Container>;
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

    impl<TC: Copy, BC: Len+IndexAs<usize>> Index for Vecs<TC, BC> {
        type Ref = Slice<TC>;
        #[inline(always)]
        fn get(&self, index: usize) -> Self::Ref {
            let lower = if index == 0 { 0 } else { self.bounds.index_as(index - 1) };
            let upper = self.bounds.index_as(index);
            Slice::new(lower, upper, self.values)
        }
    }
    impl<'a, TC, BC: Len+IndexAs<usize>> Index for &'a Vecs<TC, BC> {
        type Ref = Slice<&'a TC>;
        #[inline(always)]
        fn get(&self, index: usize) -> Self::Ref {
            let lower = if index == 0 { 0 } else { self.bounds.index_as(index - 1) };
            let upper = self.bounds.index_as(index);
            Slice::new(lower, upper, &self.values)
        }
    }
    impl<TC, BC: Len+IndexAs<usize>> IndexMut for Vecs<TC, BC> {
        type IndexMut<'a> = Slice<&'a mut TC> where TC: 'a, BC: 'a;

        #[inline(always)]
        fn get_mut(&mut self, index: usize) -> Self::IndexMut<'_> {
            let lower = if index == 0 { 0 } else { self.bounds.index_as(index - 1) };
            let upper = self.bounds.index_as(index);
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

    use super::{Clear, Columnar, Len, IndexMut, Index, Push, HeapSize};

    // Implementations for tuple types.
    // These are all macro based, because the implementations are very similar.
    // The macro requires two names, one for the store and one for pushable types.
    macro_rules! tuple_impl {
        ( $($name:ident,$name2:ident)+) => (
            impl<$($name: Columnar),*> Columnar for ($($name,)*) {
                type Container = ($($name::Container,)*);
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
                type Ref = ($($name::Ref,)*);
                fn get(&self, index: usize) -> Self::Ref {
                    let ($($name,)*) = self;
                    ($($name.get(index),)*)
                }
            }
            impl<'a, $($name),*> Index for &'a ($($name,)*) where $( &'a $name: Index),* {
                type Ref = ($(<&'a $name as Index>::Ref,)*);
                fn get(&self, index: usize) -> Self::Ref {
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

            use crate::Columnar;
            use crate::common::{Index, Push, HeapSize, Len};

            let mut column: <(usize, u8, String) as Columnar>::Container = Default::default();
            for i in 0..100 {
                column.push((i, i as u8, i.to_string()));
                column.push((i, i as u8, "".to_string()));
            }

            assert_eq!(column.len(), 200);
            assert_eq!(column.heap_size(), (3590, 4608));

            for i in 0..100 {
                assert_eq!((&column).get(2*i+0), (&i, &(i as u8), i.to_string().as_str()));
                assert_eq!((&column).get(2*i+1), (&i, &(i as u8), ""));
            }

            // Compare to the heap size of a `Vec<Option<usize>>`.
            let mut column: Vec<(usize, u8, String)> = Default::default();
            for i in 0..100 {
                column.push((i, i as u8, i.to_string()));
                column.push((i, i as u8, "".to_string()));
            }
            assert_eq!(column.heap_size(), (8190, 11040));

        }
    }
}

pub use sums::{rank_select::RankSelect, result::Results, option::Options};
/// Containers for enumerations ("sum types") that store variants separately.
///
/// The main work of these types is storing a discriminant and index efficiently,
/// as containers for each of the variant types can hold the actual data.
pub mod sums {

    /// Stores for maintaining discriminants, and associated sequential indexes.
    ///
    /// The sequential indexes are not explicitly maintained, but are supported
    /// by a `rank(index)` function that indicates how many of a certain variant
    /// precede the given index. While this could potentially be done with a scan
    /// of all preceding discriminants, the stores maintain running accumulations
    /// that make the operation constant time (using additional amortized memory).
    pub mod rank_select {

        use crate::primitive::Bools;
        use crate::{Len, Index, IndexAs, Push, Clear, HeapSize};

        /// A store for maintaining `Vec<bool>` with fast `rank` and `select` access.
        ///
        /// The design is to have `u64` running counts for each block of 1024 bits,
        /// which are roughly the size of a cache line. This is roughly 6% overhead,
        /// above the bits themselves, which seems pretty solid.
        #[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
        pub struct RankSelect<CC = Vec<u64>, VC = Vec<u64>> {
            /// Counts of the number of cumulative set (true) bits, *after* each block of 1024 bits.
            pub counts: CC,
            /// The bits themselves.
            pub values: Bools<VC>,
        }

        impl<CC, VC: Len + IndexAs<u64>> RankSelect<CC, VC> {
            #[inline]
            pub fn get(&self, index: usize) -> bool {
                Index::get(&self.values, index)
            }
        }
        impl<CC: Len + IndexAs<u64>, VC: Len + IndexAs<u64>> RankSelect<CC, VC> {
            /// The number of set bits *strictly* preceding `index`.
            ///
            /// This number is accumulated first by reading out of `self.counts` at the correct position,
            /// then by summing the ones in strictly prior `u64` entries, then by counting the ones in the
            /// masked `u64` in which the bit lives.
            pub fn rank(&self, index: usize) -> usize {
                let bit = index % 64;
                let block = index / 64;
                let chunk = block / 16;
                let mut count = if chunk > 0 { self.counts.index_as(chunk - 1) as usize } else { 0 };
                for pos in (16 * chunk) .. block {
                    count += self.values.values.index_as(pos).count_ones() as usize;
                }
                // TODO: Panic if out of bounds?
                let intra_word = if block == self.values.values.len() { self.values.last_word } else { self.values.values.index_as(block) };
                count += (intra_word & ((1 << bit) - 1)).count_ones() as usize;
                count
            }
            /// The index of the `rank`th set bit, should one exist.
            pub fn select(&self, rank: u64) -> Option<usize> {
                let mut chunk = 0;
                // Step one is to find the position in `counts` where we go from `rank` to `rank + 1`.
                // The position we are looking for is within that chunk of bits.
                // TODO: Binary search is likely better at many scales. Rust's binary search is .. not helpful with ties.
                while chunk < self.counts.len() && self.counts.index_as(chunk) <= rank {
                    chunk += 1;
                }
                let mut count = if chunk < self.counts.len() { self.counts.index_as(chunk) } else { 0 };
                // Step two is to find the position within that chunk where the `rank`th bit is.
                let mut block = 16 * chunk;
                while block < self.values.values.len() && count + (self.values.values.index_as(block).count_ones() as u64) <= rank {
                    count += self.values.values.index_as(block).count_ones() as u64;
                    block += 1;
                }
                // Step three is to search the last word for the location, or return `None` if we run out of bits.
                let last_bits = if block == self.values.values.len() { self.values.last_bits as usize } else { 64 };
                let last_word = if block == self.values.values.len() { self.values.last_word } else { self.values.values.index_as(block) };
                for shift in 0 .. last_bits {
                    if ((last_word >> shift) & 0x01 == 0x01) && count + 1 == rank {
                        return Some(64 * block + shift);
                    }
                    count += (last_word >> shift) & 0x01;
                }
                
                None
            }
        }

        impl<CC, VC: Len> RankSelect<CC, VC> {
            pub fn len(&self) -> usize {
                self.values.len()
            }
        }

        // This implementation probably only works for `Vec<u64>` and `Vec<u64>`, but we could fix that.
        // Partly, it's hard to name the `Index` flavor that allows one to get back a `u64`.
        impl<CC: Push<u64> + Len + IndexAs<u64>, VC: Push<u64> + Len + IndexAs<u64>> RankSelect<CC, VC> {
            #[inline]
            pub fn push(&mut self, bit: bool) {
                self.values.push(bit);
                while self.counts.len() < self.values.len() / 1024 {
                    let mut count = self.counts.last().unwrap_or(0);
                    let lower = 16 * self.counts.len();
                    let upper = lower + 16;
                    for i in lower .. upper {
                        count += self.values.values.index_as(i).count_ones() as u64;
                    }
                    self.counts.push(count);
                }
            }
        }
        impl<CC: Clear, VC: Clear> Clear for RankSelect<CC, VC> {
            fn clear(&mut self) {
                self.counts.clear();
                self.values.clear();
            }
        }
        impl<CC: HeapSize, VC: HeapSize> HeapSize for RankSelect<CC, VC> {
            fn heap_size(&self) -> (usize, usize) {
                let (l0, c0) = self.counts.heap_size();
                let (l1, c1) = self.values.heap_size();
                (l0 + l1, c0 + c1)
            }
        }
    }

    pub mod result {

        use crate::{Clear, Columnar, Len, IndexMut, Index, IndexAs, Push, HeapSize};
        use crate::RankSelect;

        impl<S: Columnar, T: Columnar> Columnar for Result<S, T> {
            type Container = Results<S::Container, T::Container>;
        }

        #[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
        pub struct Results<SC, TC, CC=Vec<u64>, VC=Vec<u64>> {
            /// Bits set to `true` correspond to `Ok` variants.
            pub indexes: RankSelect<CC, VC>,
            pub oks: SC,
            pub errs: TC,
        }

        impl<SC, TC, CC, VC: Len> Len for Results<SC, TC, CC, VC> {
            #[inline(always)] fn len(&self) -> usize { self.indexes.len() }
        }

        impl<SC, TC, CC, VC> Index for Results<SC, TC, CC, VC>
        where
            SC: Index,
            TC: Index,
            CC: IndexAs<u64> + Len,
            VC: IndexAs<u64> + Len,
        {
            type Ref = Result<SC::Ref, TC::Ref>;
            fn get(&self, index: usize) -> Self::Ref {
                if self.indexes.get(index) {
                    Ok(self.oks.get(self.indexes.rank(index)))
                } else {
                    Err(self.errs.get(index - self.indexes.rank(index)))
                }
            }
        }
        impl<'a, SC, TC, CC, VC> Index for &'a Results<SC, TC, CC, VC>
        where
            &'a SC: Index,
            &'a TC: Index,
            CC: IndexAs<u64> + Len,
            VC: IndexAs<u64> + Len,
        {
            type Ref = Result<<&'a SC as Index>::Ref, <&'a TC as Index>::Ref>;
            fn get(&self, index: usize) -> Self::Ref {
                if self.indexes.get(index) {
                    Ok((&self.oks).get(self.indexes.rank(index)))
                } else {
                    Err((&self.errs).get(index - self.indexes.rank(index)))
                }
            }
        }

        // NB: You are not allowed to change the variant, but can change its contents.
        impl<SC: IndexMut, TC: IndexMut, CC: IndexAs<u64> + Len, VC: IndexAs<u64> + Len> IndexMut for Results<SC, TC, CC, VC> {
            type IndexMut<'a> = Result<SC::IndexMut<'a>, TC::IndexMut<'a>> where SC: 'a, TC: 'a, CC: 'a, VC: 'a;
            fn get_mut(&mut self, index: usize) -> Self::IndexMut<'_> {
                if self.indexes.get(index) {
                    Ok(self.oks.get_mut(self.indexes.rank(index)))
                } else {
                    Err(self.errs.get_mut(index - self.indexes.rank(index)))
                }
            }
        }

        impl<S, SC: Push<S>, T, TC: Push<T>> Push<Result<S, T>> for Results<SC, TC> {
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
        impl<'a, S, SC: Push<&'a S>, T, TC: Push<&'a T>> Push<&'a Result<S, T>> for Results<SC, TC> {
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

                use crate::Columnar;
                use crate::common::{Index, Push, HeapSize, Len};

                let mut column: <Result<usize, usize> as Columnar>::Container = Default::default();
                for i in 0..100 {
                    column.push(Ok::<usize, usize>(i));
                    column.push(Err::<usize, usize>(i));
                }

                assert_eq!(column.len(), 200);
                assert_eq!(column.heap_size(), (1624, 2080));

                for i in 0..100 {
                    assert_eq!(column.get(2*i+0), Ok(i));
                    assert_eq!(column.get(2*i+1), Err(i));
                }

                let mut column: <Result<usize, u8> as Columnar>::Container = Default::default();
                for i in 0..100 {
                    column.push(Ok::<usize, u8>(i));
                    column.push(Err::<usize, u8>(i as u8));
                }

                assert_eq!(column.len(), 200);
                assert_eq!(column.heap_size(), (924, 1184));

                for i in 0..100 {
                    assert_eq!(column.get(2*i+0), Ok(i));
                    assert_eq!(column.get(2*i+1), Err(i as u8));
                }
            }
        }
    }

    pub mod option {

        use crate::{Clear, Columnar, Len, IndexMut, Index, IndexAs, Push, HeapSize};
        use crate::RankSelect;

        impl<T: Columnar> Columnar for Option<T> {
            type Container = Options<T::Container>;
        }

        #[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
        pub struct Options<TC, CC=Vec<u64>, VC=Vec<u64>> {
            /// Uses two bits for each item, one to indicate the variant and one (amortized)
            /// to enable efficient rank determination.
            pub indexes: RankSelect<CC, VC>,
            pub somes: TC,
        }

        impl<T, CC, VC: Len> Len for Options<T, CC, VC> {
            #[inline(always)] fn len(&self) -> usize { self.indexes.len() }
        }

        impl<TC: Index, CC: IndexAs<u64> + Len, VC: IndexAs<u64> + Len> Index for Options<TC, CC, VC> {
            type Ref = Option<TC::Ref>;
            fn get(&self, index: usize) -> Self::Ref {
                if self.indexes.get(index) {
                    Some(self.somes.get(self.indexes.rank(index)))
                } else {
                    None
                }
            }
        }
        impl<'a, TC, CC: IndexAs<u64> + Len, VC: IndexAs<u64> + Len> Index for &'a Options<TC, CC, VC>
        where &'a TC: Index 
        {
            type Ref = Option<<&'a TC as Index>::Ref>;
            fn get(&self, index: usize) -> Self::Ref {
                if self.indexes.get(index) {
                    Some((&self.somes).get(self.indexes.rank(index)))
                } else {
                    None
                }
            }
        }
        impl<TC: IndexMut, CC: IndexAs<u64> + Len, VC: IndexAs<u64> + Len> IndexMut for Options<TC, CC, VC> {
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

            use crate::Columnar;
            use crate::common::{Index, HeapSize, Len};
            use crate::Options;

            #[test]
            fn round_trip_some() {
                // Type annotation is important to avoid some inference overflow.
                let store: Options<Vec<i32>> = Columnar::into_columns((0..100).map(Some));
                assert_eq!(store.len(), 100);
                assert!((&store).iter().zip(0..100).all(|(a, b)| a == Some(&b)));
                assert_eq!(store.heap_size(), (408, 544));
            }

            #[test]
            fn round_trip_none() {
                let store = Columnar::into_columns((0..100).map(|_x| None::<i32>));
                assert_eq!(store.len(), 100);
                let foo = &store;
                assert!(foo.iter().zip(0..100).all(|(a, _b)| a == None));
                assert_eq!(store.heap_size(), (8, 32));
            }

            #[test]
            fn round_trip_mixed() {
                // Type annotation is important to avoid some inference overflow.
                let store: Options<Vec<i32>>  = Columnar::into_columns((0..100).map(|x| if x % 2 == 0 { Some(x) } else { None }));
                assert_eq!(store.len(), 100);
                assert!((&store).iter().zip(0..100).all(|(a, b)| a == if b % 2 == 0 { Some(&b) } else { None }));
                assert_eq!(store.heap_size(), (208, 288));
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

    impl<T: PartialEq, TC: Push<T> + Len, const N: u8> Push<T> for Repeats<TC, N>
    where
        for<'a> &'a TC: Index,
        for<'a> <&'a TC as Index>::Ref : PartialEq<T>,
    {
        fn push(&mut self, item: T) {
            // Look at the last `somes` value for a potential match.
            let insert: Option<T> = if (&self.inner.somes).last().map(|x| x.eq(&item)) == Some(true) {
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
        type Ref = TC::Ref;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref {
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
    pub struct Lookbacks<TC, VC = Vec<u8>, const N: u8 = 255> {
        /// Ok(x) encodes a value, and Err(y) indicates a value `y` back.
        pub inner: Results<TC, VC>,
    }

    impl<T: PartialEq, TC: Push<T> + Len, VC: Push<u8>, const N: u8> Push<T> for Lookbacks<TC, VC, N>
    where
        for<'a> &'a TC: Index,
        for<'a> <&'a TC as Index>::Ref : PartialEq<T>,
    {
        fn push(&mut self, item: T) {
            // Look backwards through (0 .. N) to look for a matching value.
            let oks_len = self.inner.oks.len();
            let find = (0u8 .. N).take(self.inner.oks.len()).find(|i| (&self.inner.oks).get(oks_len - (*i as usize) - 1) == item);
            let insert: Result<T, u8> = if let Some(back) = find { Err(back) } else { Ok(item) };
            self.inner.push(insert);
        }
    }

    impl<TC, VC, const N: u8> Len for Lookbacks<TC, VC, N> {
        #[inline(always)] fn len(&self) -> usize { self.inner.len() }
    }

    impl<TC: Index, VC: Index<Ref=u8>, const N: u8> Index for Lookbacks<TC, VC, N> {
        type Ref = TC::Ref;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref {
            match self.inner.get(index) {
                Ok(item) => item,
                Err(back) => {
                    let pos = self.inner.indexes.rank(index) - 1;
                    self.inner.oks.get(pos - (back as usize))
                },
            }
        }
    }
    impl<'a, TC, const N: u8> Index for &'a Lookbacks<TC, Vec<u8>, N> 
    where 
        &'a TC: Index,
    {
        type Ref = <&'a TC as Index>::Ref;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref {
            match (&self.inner).get(index) {
                Ok(item) => item,
                Err(back) => {
                    let pos = self.inner.indexes.rank(index) - 1;
                    (&self.inner.oks).get(pos - (*back as usize))
                },
            }
        }
    }

    impl<TC: HeapSize, VC: HeapSize, const N: u8> HeapSize for Lookbacks<TC, VC, N> {
        fn heap_size(&self) -> (usize, usize) {
            self.inner.heap_size()
        }
    }
}

/// Containers for `Vec<(K, V)>` that form columns by `K` keys.
mod maps {

    use crate::{Len, Push};
    use crate::Options;

    /// A container for `Vec<(K, V)>` items.
    ///
    /// Each inserted map is expected to have one `val` for any `key`.
    /// Each is stored with `None` variants for absent keys. As such,
    /// this type is not meant for large sparse key spaces.
    pub struct KeyMaps<CK, CV> {
        _keys: CK,
        vals: Vec<CV>,
    }

    impl<CK, CV: Len> Len for KeyMaps<CK, CV> {
        fn len(&self) -> usize {
            // This .. behaves badly if we have no keys.
            self.vals[0].len()
        }
    }

    // Should this implementation preserve the order of the key-val pairs?
    // That might want an associated `Vec<usize>` for each, to order the keys.
    // If they are all identical, it shouldn't take up any space, though.
    impl<K: PartialOrd, V, CV: Push<K>> Push<Vec<(K, V)>> for KeyMaps<Vec<K>, CV> {
        fn push(&mut self, _item: Vec<(K, V)>) {

        }
    }

    /// A container for `Vec<K>` items sliced by index.
    ///
    /// The container puts each `item[i]` element into the `i`th column.
    pub struct ListMaps<CV> {
        vals: Vec<Options<CV>>,
    }

    impl<CV> Default for ListMaps<CV> {
        fn default() -> Self {
            ListMaps { vals: Default::default() }
        }
    }

    impl<CV: Len> Len for ListMaps<CV> {
        fn len(&self) -> usize {
            self.vals[0].len()
        }
    }

    impl<V, CV: Push<V> + Len + Default> Push<Vec<V>> for ListMaps<CV> {
        fn push(&mut self, item: Vec<V>) {
            let mut item_len = item.len();
            let self_len = if self.vals.is_empty() { 0 } else { self.vals[0].len() };
            while self.vals.len() < item_len {
                let mut new_store: Options<CV> = Default::default();
                for _ in 0..self_len {
                    new_store.push(None);
                }
                self.vals.push(new_store);
            }
            for (store, i) in self.vals.iter_mut().zip(item) {
                store.push(Some(i));
            }
            while item_len < self.vals.len() {
                self.vals[item_len].push(None);
                item_len += 1;
            }
        }
    }

    #[cfg(test)]
    mod test {

        use crate::common::{Len, Push};
        use crate::{Results, Strings};

        #[test]
        fn round_trip_listmap() {

            // Each record is a list, of first homogeneous elements, and one heterogeneous.
            let records = (0 .. 1024).map(|i|
                vec![
                    Ok(i),
                    Err(format!("{:?}", i)),
                    if i % 2 == 0 { Ok(i) } else { Err(format!("{:?}", i)) },
                ]
            );

            // We'll stash all the records in the store, which expects them.
            let mut store: super::ListMaps<Results<Vec<i32>, Strings>> = Default::default();
            for record in records {
                store.push(record);
            }

            // Demonstrate type-safe restructuring.
            // We expect the first two columns to be homogenous, and the third to be mixed.
            let field0: Option<&[i32]> = if store.vals[0].somes.oks.len() == store.vals[0].len() {
                Some(&store.vals[0].somes.oks)
            } else { None };

            let field1: Option<&Strings> = if store.vals[1].somes.errs.len() == store.vals[1].len() {
                Some(&store.vals[1].somes.errs)
            } else { None };

            let field2: Option<&[i32]> = if store.vals[2].somes.oks.len() == store.vals[2].len() {
                Some(&store.vals[2].somes.oks)
            } else { None };

            assert!(field0.is_some());
            assert!(field1.is_some());
            assert!(field2.is_none());
        }
    }

}

/// Containers for `isize` and `usize` that adapt to the size of the data.
///
/// Similar structures could be used for containers of `u8`, `u16`, `u32`, and `u64`,
/// without losing their type information, if one didn't need the bespoke compression.
mod sizes {

    use crate::Push;
    use crate::Results;

    /// A four-variant container for integers of varying sizes.
    struct Sizes<C0, C1, C2, C3> {
        /// Four variants stored separately.
        inner: Results<Results<C0, C1>, Results<C2, C3>>,
    }

    impl<C0: Default, C1: Default, C2: Default, C3: Default> Default for Sizes<C0, C1, C2, C3> {
        fn default() -> Self {
            Sizes { inner: Default::default() }
        }
    }

    impl<C0: Push<u8>, C1: Push<u16>, C2: Push<u32>, C3: Push<u64>> Push<usize> for Sizes<C0, C1, C2, C3> {
        fn push(&mut self, item: usize) {
            if let Ok(item) = TryInto::<u8>::try_into(item) {
                self.inner.push(Ok(Ok(item)))
            } else if let Ok(item) = TryInto::<u16>::try_into(item) {
                self.inner.push(Ok(Err(item)))
            } else if let Ok(item) = TryInto::<u32>::try_into(item) {
                self.inner.push(Err(Ok(item)))
            } else if let Ok(item) = TryInto::<u64>::try_into(item) {
                self.inner.push(Err(Err(item)))
            } else {
                panic!("usize exceeds bounds of u64")
            }
        }
    }

    impl<C0: Push<i8>, C1: Push<i16>, C2: Push<i32>, C3: Push<i64>> Push<isize> for Sizes<C0, C1, C2, C3> {
        fn push(&mut self, item: isize) {
            if let Ok(item) = TryInto::<i8>::try_into(item) {
                self.inner.push(Ok(Ok(item)))
            } else if let Ok(item) = TryInto::<i16>::try_into(item) {
                self.inner.push(Ok(Err(item)))
            } else if let Ok(item) = TryInto::<i32>::try_into(item) {
                self.inner.push(Err(Ok(item)))
            } else if let Ok(item) = TryInto::<i64>::try_into(item) {
                self.inner.push(Err(Err(item)))
            } else {
                panic!("isize exceeds bounds of i64")
            }
        }
    }
}

/// Roaring bitmap (and similar) containers.
pub mod roaring {

    use crate::Results;

    /// A container for `bool` that uses techniques from Roaring bitmaps.
    ///
    /// These techniques are to block the bits into blocks of 2^16 bits,
    /// and to encode each block based on its density. Either a bitmap
    /// for dense blocks or a list of set bits for sparse blocks.
    ///
    /// Additionally, other representations encode runs of set bits.
    pub struct RoaringBits {
        _inner: Results<[u64; 1024], Vec<u16>>,
    }
}