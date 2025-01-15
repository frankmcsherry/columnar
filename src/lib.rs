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

pub mod adts;

/// A type that can be represented in columnar form.
///
/// For a running example, a type like `(A, Vec<B>)`.
pub trait Columnar : 'static {

    /// For each lifetime, a reference with that lifetime.
    ///
    /// As an example, `(&'a A, &'a [B])`.
    type Ref<'a>;
    /// Repopulates `self` from a reference.
    ///
    /// By default this just calls `into_owned()`, but it can be overridden.
    fn copy_from<'a>(&mut self, other: Self::Ref<'a>) where Self: Sized {
        *self = Self::into_owned(other);
    }
    /// Produce an instance of `Self` from `Self::Ref<'a>`.
    fn into_owned<'a>(other: Self::Ref<'a>) -> Self;

    /// The type that stores the columnar representation.
    ///
    /// The container must support pushing both `&Self` and `Self::Ref<'_>`.
    /// In our running example this might be `(Vec<A>, Vecs<Vec<B>>)`.
    type Container: Len + Clear + Default + for<'a> Push<&'a Self> + for<'a> Push<Self::Ref<'a>> + Container<Self>;

    /// Converts a sequence of the references to the type into columnar form.
    fn as_columns<'a, I>(selves: I) -> Self::Container where I: IntoIterator<Item =&'a Self>, Self: 'a {
        let mut columns: Self::Container = Default::default();
        for item in selves {
            columns.push(item);
        }
        columns
    }
    /// Converts a sequence of the type into columnar form.
    ///
    /// This consumes the owned `Self` types but uses them only by reference.
    /// Consider `as_columns()` instead if it is equally ergonomic.
    fn into_columns<I>(selves: I) -> Self::Container where I: IntoIterator<Item = Self>, Self: Sized {
        let mut columns: Self::Container = Default::default();
        for item in selves {
            columns.push(&item);
        }
        columns
    }
}

/// A container that can hold `C`, and provide its preferred references.
///
/// As an example, `(Vec<A>, Vecs<Vec<B>>)`.
pub trait Container<C: Columnar + ?Sized> {
    /// The type of a borrowed container.
    ///
    /// Corresponding to our example, `(&'a [A], Vecs<&'a [B], &'a [u64]>)`.
    type Borrowed<'a>: Copy + Len + AsBytes<'a> + FromBytes<'a> + Index<Ref = C::Ref<'a>> where Self: 'a;
    /// Converts a reference to the type to a borrowed variant.
    fn borrow<'a>(&'a self) -> Self::Borrowed<'a>;
}

pub use common::{Clear, Len, Push, IndexMut, Index, IndexAs, HeapSize, Slice, AsBytes, FromBytes};
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

        #[inline(always)]
        fn extend(&mut self, iter: impl IntoIterator<Item=T>) {
            std::iter::Extend::extend(self, iter)
        }
    }
    impl<'a, T: Clone> Push<&'a T> for Vec<T> {
        #[inline(always)] fn push(&mut self, item: &'a T) { self.push(item.clone()) }

        #[inline(always)]
        fn extend(&mut self, iter: impl IntoIterator<Item=&'a T>) {
            std::iter::Extend::extend(self, iter.into_iter().cloned())
        }
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
    // Vectors can be cleared.
    impl<T> Clear for Vec<T> {
        #[inline(always)] fn clear(&mut self) { self.clear() }
    }
    // Slice references can be cleared.
    impl<'a, T> Clear for &'a [T] {
        #[inline(always)] fn clear(&mut self) { *self = &[]; }
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
        pub fn new(lower: u64, upper: u64, slice: S) -> Self {
            let lower: usize = lower.try_into().expect("slice bounds must fit in `usize`");
            let upper: usize = upper.try_into().expect("slice bounds must fit in `usize`");
            Self { lower, upper, slice }
        }
        pub fn len(&self) -> usize { self.upper - self.lower }
    }

    impl<S: Index> PartialEq for Slice<S> where S::Ref: PartialEq {
        fn eq(&self, other: &Self) -> bool {
            if self.len() != other.len() { return false; }
            for i in 0 .. self.len() {
                if self.get(i) != other.get(i) { return false; }
            }
            true
        }
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

    impl<S: Index> Eq for Slice<S> where S::Ref: Eq { }

    impl<S: Index> PartialOrd for Slice<S> where S::Ref: PartialOrd {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            use std::cmp::Ordering;
            let len = std::cmp::min(self.len(), other.len());

            for i in 0 .. len {
                match self.get(i).partial_cmp(&other.get(i)) {
                    Some(Ordering::Equal) => (),
                    not_equal => return not_equal,
                }
            }

            self.len().partial_cmp(&other.len())
        }
    }

    impl<S: Index> Ord for Slice<S> where S::Ref: Ord + Eq {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            use std::cmp::Ordering;
            let len = std::cmp::min(self.len(), other.len());

            for i in 0 .. len {
                match self.get(i).cmp(&other.get(i)) {
                    Ordering::Equal => (),
                    not_equal => return not_equal,
                }
            }

            self.len().cmp(&other.len())
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

    /// A type that can be viewed as byte slices with lifetime `'a`.
    ///
    /// Implementors of this trait almost certainly reference the lifetime `'a` themselves.
    pub trait AsBytes<'a> {
        /// Presents `self` as a sequence of byte slices, with their required alignment.
        fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])>;
        /// The number of `u64` words required to record `self` as aligned bytes.
        fn length_in_words(&self) -> usize {
            self.as_bytes().map(|(_, x)| 1 + (x.len()/8) + if x.len() % 8 == 0 { 0 } else { 1 }).sum()
        }
    }

    /// A type that can be reconstituted from byte slices with lifetime `'a`.
    ///
    /// Implementors of this trait almost certainly reference the lifetime `'a` themselves,
    /// unless they actively deserialize the bytes (vs sit on the slices, as if zero-copy).
    pub trait FromBytes<'a> {
        /// Reconstructs `self` from a sequence of correctly aligned and sized bytes slices.
        ///
        /// The implementation is expected to consume the right number of items from the iterator,
        /// which may go on to be used by other implementations of `FromBytes`.
        fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self;
    }

}

/// Logic related to the transformation to and from bytes.
///
/// The methods here line up with the `AsBytes` and `FromBytes` traits.
pub mod bytes {
    /// A sequential byte layout for `AsBytes` and `FromBytes` implementors.
    ///
    /// The layout is aligned like a sequence of `u64`, where we repeatedly announce a length,
    /// and then follow it by that many bytes. We may need to follow this with padding bytes.
    pub mod serialization {

        /// Encodes a sequence of byte slices as their length followed by their bytes, aligned to 8 bytes.
        ///
        /// Each length will be exactly 8 bytes, and the bytes that follow are padded out to a multiple of 8 bytes.
        /// When reading the data, the length is in bytes, and one should consume those bytes and advance over padding.
        pub fn encode<'a>(store: &mut Vec<u64>, bytes: impl Iterator<Item=(u64, &'a [u8])>) {
            for (align, bytes) in bytes {
                assert!(align <= 8);
                store.push(bytes.len() as u64);
                let whole_words = 8 * (bytes.len() / 8);
                // We want to extend `store` by `bytes`, but `bytes` may not be `u64` aligned.
                // In the latter case, init `store` and cast and copy onto it as a byte slice.
                if let Ok(words) = bytemuck::try_cast_slice(&bytes[.. whole_words]) {
                    store.extend(words);
                }
                else {
                    let store_len = store.len();
                    store.resize(store_len + whole_words/8, 0);
                    let slice = bytemuck::try_cast_slice_mut(&mut store[store_len..]).expect("&[u64] should convert to &[u8]");
                    slice.copy_from_slice(&bytes[.. whole_words]);
                }
                let remaining_bytes = &bytes[whole_words..];
                if !remaining_bytes.is_empty() {
                    let mut remainder = 0u64;
                    let transmute: &mut [u8] = bytemuck::try_cast_slice_mut(std::slice::from_mut(&mut remainder)).expect("&[u64] should convert to &[u8]");
                    for (i, byte) in remaining_bytes.iter().enumerate() {
                        transmute[i] = *byte;
                    }
                    store.push(remainder);
                }
            }
        }

        /// Decodes a sequence of byte slices from their length followed by their bytes.
        ///
        /// This decoder matches the `encode` function above.
        /// In particular, it anticipates padding bytes when the length is not a multiple of eight.
        pub fn decode(store: &[u64]) -> Decoder<'_> {
            Decoder { store }
        }

        /// An iterator over byte slices, decoding from a sequence of lengths followed by bytes.
        pub struct Decoder<'a> {
            store: &'a [u64],
        }

        impl<'a> Iterator for Decoder<'a> {
            type Item = &'a [u8];
            fn next(&mut self) -> Option<Self::Item> {
                if let Some(length) = self.store.first() {
                    let length = *length as usize;
                    self.store = &self.store[1..];
                    let whole_words = if length % 8 == 0 { length / 8 } else { length / 8 + 1 };
                    let bytes: &[u8] = bytemuck::try_cast_slice(&self.store[..whole_words]).expect("&[u64] should convert to &[u8]");
                    self.store = &self.store[whole_words..];
                    Some(&bytes[..length])
                } else {
                    None
                }
            }
        }
    }


    #[cfg(test)]
    mod test {
        #[test]
        fn round_trip() {

            use crate::{Columnar, Container};
            use crate::common::{Push, HeapSize, Len, Index};
            use crate::{AsBytes, FromBytes};

            let mut column: <Result<u64, u64> as Columnar>::Container = Default::default();
            for i in 0..100u64 {
                column.push(Ok::<u64, u64>(i));
                column.push(Err::<u64, u64>(i));
            }

            assert_eq!(column.len(), 200);
            assert_eq!(column.heap_size(), (1624, 2080));

            for i in 0..100 {
                assert_eq!(column.get(2*i+0), Ok(i as u64));
                assert_eq!(column.get(2*i+1), Err(i as u64));
            }

            let column2 = crate::Results::<&[u64], &[u64], &[u64], &[u64], &u64>::from_bytes(&mut column.borrow().as_bytes().map(|(_, bytes)| bytes));
            for i in 0..100 {
                assert_eq!(column.get(2*i+0), column2.get(2*i+0).copied().map_err(|e| *e));
                assert_eq!(column.get(2*i+1), column2.get(2*i+1).copied().map_err(|e| *e));
            }

            let column3 = crate::Results::<&[u64], &[u64], &[u64], &[u64], &u64>::from_bytes(&mut column2.as_bytes().map(|(_, bytes)| bytes));
            for i in 0..100 {
                assert_eq!(column3.get(2*i+0), column2.get(2*i+0));
                assert_eq!(column3.get(2*i+1), column2.get(2*i+1));
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
                type Ref<'a> = &'a $index_type;
                fn into_owned<'a>(other: Self::Ref<'a>) -> Self { *other }

                type Container = Vec<$index_type>;
            }
            impl crate::Container<$index_type> for Vec<$index_type> {
                type Borrowed<'a> = &'a [$index_type];
                fn borrow<'a>(&'a self) -> Self::Borrowed<'a> { &self[..] }
            }

            impl crate::HeapSize for $index_type { }

            impl<'a> crate::AsBytes<'a> for &'a [$index_type] {
                fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
                    std::iter::once((std::mem::align_of::<$index_type>() as u64, bytemuck::cast_slice(&self[..])))
                }
            }
            impl<'a> crate::FromBytes<'a> for &'a [$index_type] {
                fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
                    // We use `unwrap()` here in order to panic with the `bytemuck` error, which may be informative.
                    bytemuck::try_cast_slice(bytes.next().expect("Iterator exhausted prematurely")).unwrap()
                }
            }
        )* }
    }

    implement_columnable!(u8, u16, u32, u64, u128);
    implement_columnable!(i8, i16, i32, i64, i128);
    implement_columnable!(f32, f64);

    pub use sizes::{Usizes, Isizes};
    /// Columnar stores for `usize` and `isize`, stored as 64 bits.
    mod sizes {

        use crate::{Clear, Columnar, Len, IndexMut, Index, IndexAs, Push, HeapSize};

        #[derive(Copy, Clone, Default)]
        pub struct Usizes<CV = Vec<u64>> { pub values: CV }

        impl Columnar for usize {
            type Ref<'a> = usize;
            fn into_owned<'a>(other: Self::Ref<'a>) -> Self { other }
            type Container = Usizes;
        }

        impl<CV: crate::Container<u64>> crate::Container<usize> for Usizes<CV> {
            type Borrowed<'a> = Usizes<CV::Borrowed<'a>> where CV: 'a;
            fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
                Usizes { values: self.values.borrow() }
            }
        }

        impl<CV: Len> Len for Usizes<CV> { fn len(&self) -> usize { self.values.len() }}
        impl IndexMut for Usizes {
            type IndexMut<'a> = &'a mut u64;
            #[inline(always)] fn get_mut(&mut self, index: usize) -> Self::IndexMut<'_> { &mut self.values[index] }
        }
        impl<CV: IndexAs<u64>> Index for Usizes<CV> {
            type Ref = usize;
            #[inline(always)] fn get(&self, index: usize) -> Self::Ref { self.values.index_as(index).try_into().expect("Usizes values should fit in `usize`") }
        }
        impl<'a, CV: IndexAs<u64>> Index for &'a Usizes<CV> {
            type Ref = usize;
            #[inline(always)] fn get(&self, index: usize) -> Self::Ref { self.values.index_as(index).try_into().expect("Usizes values should fit in `usize`") }
        }
        impl Push<usize> for Usizes {
            fn push(&mut self, item: usize) { self.values.push(item.try_into().expect("usize must fit in a u64")) }
        }
        impl Push<&usize> for Usizes {
            fn push(&mut self, item: &usize) { self.values.push((*item).try_into().expect("usize must fit in a u64")) }
        }
        impl<CV: Clear> Clear for Usizes<CV> { fn clear(&mut self) { self.values.clear() }}

        impl<CV: HeapSize> HeapSize for Usizes<CV> {
            fn heap_size(&self) -> (usize, usize) {
                self.values.heap_size()
            }
        }

        impl<'a, CV: crate::AsBytes<'a>> crate::AsBytes<'a> for crate::primitive::Usizes<CV> {
            fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
                self.values.as_bytes()
            }
        }

        impl<'a, CV: crate::FromBytes<'a>> crate::FromBytes<'a> for crate::primitive::Usizes<CV> {
            fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
                Self { values: CV::from_bytes(bytes) }
            }
        }


        #[derive(Copy, Clone, Default)]
        pub struct Isizes<CV = Vec<i64>> { pub values: CV }

        impl Columnar for isize {
            type Ref<'a> = isize;
            fn into_owned<'a>(other: Self::Ref<'a>) -> Self { other }
            type Container = Isizes;
        }

        impl<CV: crate::Container<i64>> crate::Container<isize> for Isizes<CV> {
            type Borrowed<'a> = Isizes<CV::Borrowed<'a>> where CV: 'a;
            fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
                Isizes { values: self.values.borrow() }
            }
        }

        impl<CV: Len> Len for Isizes<CV> { fn len(&self) -> usize { self.values.len() }}
        impl IndexMut for Isizes {
            type IndexMut<'a> = &'a mut i64;
            #[inline(always)] fn get_mut(&mut self, index: usize) -> Self::IndexMut<'_> { &mut self.values[index] }
        }
        impl<CV: IndexAs<i64>> Index for Isizes<CV> {
            type Ref = isize;
            #[inline(always)] fn get(&self, index: usize) -> Self::Ref { self.values.index_as(index).try_into().expect("Isizes values should fit in `isize`") }
        }
        impl<'a, CV: IndexAs<i64>> Index for &'a Isizes<CV> {
            type Ref = isize;
            #[inline(always)] fn get(&self, index: usize) -> Self::Ref { self.values.index_as(index).try_into().expect("Isizes values should fit in `isize`") }
        }
        impl Push<isize> for Isizes {
            fn push(&mut self, item: isize) { self.values.push(item.try_into().expect("isize must fit in a i64")) }
        }
        impl Push<&isize> for Isizes {
            fn push(&mut self, item: &isize) { self.values.push((*item).try_into().expect("isize must fit in a i64")) }
        }
        impl<CV: Clear> Clear for Isizes<CV> { fn clear(&mut self) { self.values.clear() }}

        impl<CV: HeapSize> HeapSize for Isizes<CV> {
            fn heap_size(&self) -> (usize, usize) {
                self.values.heap_size()
            }
        }

        impl<'a, CV: crate::AsBytes<'a>> crate::AsBytes<'a> for crate::primitive::Isizes<CV> {
            fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
                self.values.as_bytes()
            }
        }

        impl<'a, CV: crate::FromBytes<'a>> crate::FromBytes<'a> for crate::primitive::Isizes<CV> {
            fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
                Self { values: CV::from_bytes(bytes) }
            }
        }
    }

    pub use empty::Empties;
    /// A columnar store for `()`.
    mod empty {

        use crate::common::index::CopyAs;
        use crate::{Clear, Columnar, Len, IndexMut, Index, Push, HeapSize};

        #[derive(Copy, Clone, Debug, Default)]
        pub struct Empties<CC = u64> { pub count: CC, pub empty: () }

        impl Columnar for () {
            type Ref<'a> = ();
            fn into_owned<'a>(_other: Self::Ref<'a>) -> Self { () }
            type Container = Empties;
        }

        impl crate::Container<()> for Empties {
            type Borrowed<'a> = Empties<&'a u64>;
            fn borrow<'a>(&'a self) -> Self::Borrowed<'a> { Empties { count: &self.count, empty: () } }
        }

        impl<CC: CopyAs<u64> + Copy> Len for Empties<CC> {
            fn len(&self) -> usize { self.count.copy_as() as usize }
        }
        impl<CC> IndexMut for Empties<CC> {
            type IndexMut<'a> = &'a mut () where CC: 'a;
            // TODO: panic if out of bounds?
            #[inline(always)] fn get_mut(&mut self, _index: usize) -> Self::IndexMut<'_> { &mut self.empty }
        }
        impl<CC> Index for Empties<CC> {
            type Ref = ();
            fn get(&self, _index: usize) -> Self::Ref { () }
        }
        impl<'a, CC> Index for &'a Empties<CC> {
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

        impl HeapSize for Empties {
            fn heap_size(&self) -> (usize, usize) { (0, 0) }
        }
        impl Clear for Empties {
            fn clear(&mut self) { self.count = 0; }
        }

        impl<'a> crate::AsBytes<'a> for crate::primitive::Empties<&'a u64> {
            fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
                std::iter::once((8, bytemuck::cast_slice(std::slice::from_ref(self.count))))
            }
        }
        impl<'a> crate::FromBytes<'a> for crate::primitive::Empties<&'a u64> {
            fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
                Self { count: &bytemuck::try_cast_slice(bytes.next().expect("Iterator exhausted prematurely")).unwrap()[0], empty: () }
            }
        }
    }

    pub use boolean::Bools;
    /// A columnar store for `bool`.
    mod boolean {

        use crate::common::index::CopyAs;
        use crate::{Clear, Len, Index, IndexAs, Push, HeapSize};

        /// A store for maintaining `Vec<bool>`.
        #[derive(Copy, Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
        pub struct Bools<VC = Vec<u64>, WC = u64> {
            /// The bundles of bits that form complete `u64` values.
            pub values: VC,
            /// The work-in-progress bits that are not yet complete.
            pub last_word: WC,
            /// The number of set bits in `bits.last()`.
            pub last_bits: WC,
        }

        impl crate::Columnar for bool {
            type Ref<'a> = bool;
            fn into_owned<'a>(other: Self::Ref<'a>) -> Self { other }
            type Container = Bools;
        }

        impl<VC: crate::Container<u64>> crate::Container<bool> for Bools<VC> {
            type Borrowed<'a> = Bools<VC::Borrowed<'a>, &'a u64> where VC: 'a;
            fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
                Bools {
                    values: self.values.borrow(),
                    last_word: &self.last_word,
                    last_bits: &self.last_bits,
                }
            }
        }

        impl<'a, VC: crate::AsBytes<'a>> crate::AsBytes<'a> for crate::primitive::Bools<VC, &'a u64> {
            fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
                self.values.as_bytes()
                    .chain(std::iter::once((std::mem::align_of::<u64>() as u64, bytemuck::cast_slice(std::slice::from_ref(self.last_word)))))
                    .chain(std::iter::once((1, bytemuck::cast_slice(std::slice::from_ref(self.last_bits)))))
            }
        }

        impl<'a, VC: crate::FromBytes<'a>> crate::FromBytes<'a> for crate::primitive::Bools<VC, &'a u64> {
            fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
                let values = crate::FromBytes::from_bytes(bytes);
                let last_word = &bytemuck::try_cast_slice(bytes.next().expect("Iterator exhausted prematurely")).unwrap()[0];
                let last_bits = &bytemuck::try_cast_slice(bytes.next().expect("Iterator exhausted prematurely")).unwrap()[0];
                Self { values, last_word, last_bits }
            }
        }

        impl<VC: Len, WC: Copy + CopyAs<u64>> Len for Bools<VC, WC> {
            #[inline(always)] fn len(&self) -> usize { self.values.len() * 64 + (self.last_bits.copy_as() as usize) }
        }

        impl<VC: Len + IndexAs<u64>, WC: Copy + CopyAs<u64>> Index for Bools<VC, WC> {
            type Ref = bool;
            #[inline(always)] fn get(&self, index: usize) -> Self::Ref {
                let block = index / 64;
                let word = if block == self.values.len() {
                    self.last_word.copy_as()
                } else {
                    self.values.index_as(block)
                };
                let bit = index % 64;
                (word >> bit) & 1 == 1
            }
        }

        impl<VC: Len + IndexAs<u64>, WC: Copy + CopyAs<u64>> Index for &Bools<VC, WC> {
            type Ref = bool;
            #[inline(always)] fn get(&self, index: usize) -> Self::Ref {
                (*self).get(index)
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

        use std::time::Duration;
        use crate::{Len, Index, IndexAs, Push, Clear, HeapSize};

        // `std::time::Duration` is equivalent to `(u64, u32)`, corresponding to seconds and nanoseconds.
        #[derive(Copy, Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
        pub struct Durations<SC = Vec<u64>, NC = Vec<u32>> {
            pub seconds: SC,
            pub nanoseconds: NC,
        }

        impl crate::Columnar for Duration {
            type Ref<'a> = Duration;
            fn into_owned<'a>(other: Self::Ref<'a>) -> Self { other }
            type Container = Durations;
        }

        impl<SC: crate::Container<u64>, NC: crate::Container<u32>> crate::Container<Duration> for Durations<SC, NC> {
            type Borrowed<'a> = Durations<SC::Borrowed<'a>, NC::Borrowed<'a>> where SC: 'a, NC: 'a;
            fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
                Durations {
                    seconds: self.seconds.borrow(),
                    nanoseconds: self.nanoseconds.borrow(),
                }
            }
        }

        impl<'a, SC: crate::AsBytes<'a>, NC: crate::AsBytes<'a>> crate::AsBytes<'a> for crate::primitive::Durations<SC, NC> {
            fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
                self.seconds.as_bytes().chain(self.nanoseconds.as_bytes())
            }
        }
        impl<'a, SC: crate::FromBytes<'a>, NC: crate::FromBytes<'a>> crate::FromBytes<'a> for crate::primitive::Durations<SC, NC> {
            fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
                Self {
                    seconds: crate::FromBytes::from_bytes(bytes),
                    nanoseconds: crate::FromBytes::from_bytes(bytes),
                }
            }
        }

        impl<SC: Len, NC> Len for Durations<SC, NC> {
            #[inline(always)] fn len(&self) -> usize { self.seconds.len() }
        }

        impl<SC: IndexAs<u64>, NC: IndexAs<u32>> Index for Durations<SC, NC> {
            type Ref = Duration;
            #[inline(always)] fn get(&self, index: usize) -> Self::Ref {
                Duration::new(self.seconds.index_as(index), self.nanoseconds.index_as(index))
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

    /// A stand-in for `Vec<String>`.
    #[derive(Copy, Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
    pub struct Strings<BC = Vec<u64>, VC = Vec<u8>> {
        /// Bounds container; provides indexed access to offsets.
        pub bounds: BC,
        /// Values container; provides slice access to bytes.
        pub values: VC,
    }

    impl Columnar for String {
        type Ref<'a> = &'a str;
        fn copy_from<'a>(&mut self, other: Self::Ref<'a>) {
            self.clear();
            self.push_str(other);
        }
        fn into_owned<'a>(other: Self::Ref<'a>) -> Self { other.to_string() }
        type Container = Strings;
    }

    impl<'b, BC: crate::Container<u64>> crate::Container<String> for Strings<BC, &'b [u8]> {
        type Borrowed<'a> = Strings<BC::Borrowed<'a>, &'a [u8]> where BC: 'a, 'b: 'a;
        fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
            Strings {
                bounds: self.bounds.borrow(),
                values: self.values,
            }
        }
    }
    impl<BC: crate::Container<u64>> crate::Container<String> for Strings<BC, Vec<u8>> {
        type Borrowed<'a> = Strings<BC::Borrowed<'a>, &'a [u8]> where BC: 'a;
        fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
            Strings {
                bounds: self.bounds.borrow(),
                values: self.values.borrow(),
            }
        }
    }

    impl<'a, BC: crate::AsBytes<'a>, VC: crate::AsBytes<'a>> crate::AsBytes<'a> for Strings<BC, VC> {
        fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
            self.bounds.as_bytes().chain(self.values.as_bytes())
        }
    }
    impl<'a, BC: crate::FromBytes<'a>, VC: crate::FromBytes<'a>> crate::FromBytes<'a> for Strings<BC, VC> {
        fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
            Self {
                bounds: crate::FromBytes::from_bytes(bytes),
                values: crate::FromBytes::from_bytes(bytes),
            }
        }
    }

    impl<BC: Len, VC> Len for Strings<BC, VC> {
        #[inline(always)] fn len(&self) -> usize { self.bounds.len() }
    }

    impl<'a, BC: Len+IndexAs<u64>> Index for Strings<BC, &'a [u8]> {
        type Ref = &'a str;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref {
            let lower = if index == 0 { 0 } else { self.bounds.index_as(index - 1) };
            let upper = self.bounds.index_as(index);
            let lower: usize = lower.try_into().expect("bounds must fit in `usize`");
            let upper: usize = upper.try_into().expect("bounds must fit in `usize`");
            std::str::from_utf8(&self.values[lower .. upper]).expect("&[u8] must be valid utf8")
        }
    }
    impl<'a, BC: Len+IndexAs<u64>> Index for &'a Strings<BC, Vec<u8>> {
        type Ref = &'a str;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref {
            let lower = if index == 0 { 0 } else { self.bounds.index_as(index - 1) };
            let upper = self.bounds.index_as(index);
            let lower: usize = lower.try_into().expect("bounds must fit in `usize`");
            let upper: usize = upper.try_into().expect("bounds must fit in `usize`");
            std::str::from_utf8(&self.values[lower .. upper]).expect("&[u8] must be valid utf8")
        }
    }

    impl<BC: Push<u64>> Push<&String> for Strings<BC> {
        #[inline(always)] fn push(&mut self, item: &String) {
            self.values.extend_from_slice(item.as_bytes());
            self.bounds.push(self.values.len() as u64);
        }
    }
    impl<BC: Push<u64>> Push<&str> for Strings<BC> {
        fn push(&mut self, item: &str) {
            self.values.extend_from_slice(item.as_bytes());
            self.bounds.push(self.values.len() as u64);
        }
    }
    impl<BC: Clear, VC: Clear> Clear for Strings<BC, VC> {
        fn clear(&mut self) {
            self.bounds.clear();
            self.values.clear();
        }
    }
    impl<BC: HeapSize, VC: HeapSize> HeapSize for Strings<BC, VC> {
        fn heap_size(&self) -> (usize, usize) {
            let (l0, c0) = self.bounds.heap_size();
            let (l1, c1) = self.values.heap_size();
            (l0 + l1, c0 + c1)
        }
    }
}

pub use vector::Vecs;
pub mod vector {

    use super::{Clear, Columnar, Len, IndexMut, Index, IndexAs, Push, HeapSize, Slice};

    /// A stand-in for `Vec<Vec<T>>` for complex `T`.
    #[derive(Debug, Default, Copy, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    pub struct Vecs<TC, BC = Vec<u64>> {
        pub bounds: BC,
        pub values: TC,
    }

    impl<T: Columnar> Columnar for Vec<T> {
        type Ref<'a> = Slice<<T::Container as crate::Container<T>>::Borrowed<'a>> where T: 'a;
        fn copy_from<'a>(&mut self, other: Self::Ref<'a>) {
            self.truncate(other.len());
            let mut other_iter = other.into_iter();
            for (s, o) in self.iter_mut().zip(&mut other_iter) {
                T::copy_from(s, o);
            }
            for o in other_iter {
                self.push(T::into_owned(o));
            }
        }
        fn into_owned<'a>(other: Self::Ref<'a>) -> Self {
            other.into_iter().map(|x| T::into_owned(x)).collect()
        }
        type Container = Vecs<T::Container>;
    }

    impl<T: Columnar, const N: usize> Columnar for [T; N] {
        type Ref<'a> = Slice<<T::Container as crate::Container<T>>::Borrowed<'a>> where T: 'a;
        fn copy_from<'a>(&mut self, other: Self::Ref<'a>) {
            for (s, o) in self.iter_mut().zip(other.into_iter()) {
                T::copy_from(s, o);
            }
        }
        fn into_owned<'a>(other: Self::Ref<'a>) -> Self {
            let vec: Vec<_> = other.into_iter().map(|x| T::into_owned(x)).collect();
            match vec.try_into() {
                Ok(array) => array,
                Err(_) => panic!("wrong length"),
            }
        }
        type Container = Vecs<T::Container>;
    }

    impl<T: Columnar<Container = TC>, BC: crate::Container<u64>, TC: crate::Container<T>> crate::Container<Vec<T>> for Vecs<TC, BC> {
        type Borrowed<'a> = Vecs<TC::Borrowed<'a>, BC::Borrowed<'a>> where BC: 'a, TC: 'a, T: 'a;
        fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
            Vecs {
                bounds: self.bounds.borrow(),
                values: self.values.borrow(),
            }
        }
    }

    impl<T: Columnar<Container = TC>, BC: crate::Container<u64>, TC: crate::Container<T>, const N: usize> crate::Container<[T; N]> for Vecs<TC, BC> {
        type Borrowed<'a> = Vecs<TC::Borrowed<'a>, BC::Borrowed<'a>> where BC: 'a, TC: 'a, T: 'a;
        fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
            Vecs {
                bounds: self.bounds.borrow(),
                values: self.values.borrow(),
            }
        }
    }

    impl<'a, TC: crate::AsBytes<'a>, BC: crate::AsBytes<'a>> crate::AsBytes<'a> for Vecs<TC, BC> {
        fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
            self.bounds.as_bytes().chain(self.values.as_bytes())
        }
    }
    impl<'a, TC: crate::FromBytes<'a>, BC: crate::FromBytes<'a>> crate::FromBytes<'a> for Vecs<TC, BC> {
        fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
            Self {
                bounds: crate::FromBytes::from_bytes(bytes),
                values: crate::FromBytes::from_bytes(bytes),
            }
        }
    }

    impl<TC: Len> Vecs<TC> {
        pub fn push_iter<I>(&mut self, iter: I) where I: IntoIterator, TC: Push<I::Item> {
            self.values.extend(iter);
            self.bounds.push(self.values.len() as u64);
        }
    }

    impl<TC, BC: Len> Len for Vecs<TC, BC> {
        #[inline(always)] fn len(&self) -> usize { self.bounds.len() }
    }

    impl<TC: Copy, BC: Len+IndexAs<u64>> Index for Vecs<TC, BC> {
        type Ref = Slice<TC>;
        #[inline(always)]
        fn get(&self, index: usize) -> Self::Ref {
            let lower = if index == 0 { 0 } else { self.bounds.index_as(index - 1) };
            let upper = self.bounds.index_as(index);
            Slice::new(lower, upper, self.values)
        }
    }
    impl<'a, TC, BC: Len+IndexAs<u64>> Index for &'a Vecs<TC, BC> {
        type Ref = Slice<&'a TC>;
        #[inline(always)]
        fn get(&self, index: usize) -> Self::Ref {
            let lower = if index == 0 { 0 } else { self.bounds.index_as(index - 1) };
            let upper = self.bounds.index_as(index);
            Slice::new(lower, upper, &self.values)
        }
    }
    impl<TC, BC: Len+IndexAs<u64>> IndexMut for Vecs<TC, BC> {
        type IndexMut<'a> = Slice<&'a mut TC> where TC: 'a, BC: 'a;

        #[inline(always)]
        fn get_mut(&mut self, index: usize) -> Self::IndexMut<'_> {
            let lower = if index == 0 { 0 } else { self.bounds.index_as(index - 1) };
            let upper = self.bounds.index_as(index);
            Slice::new(lower, upper, &mut self.values)
        }
    }

    impl<TC: Push<TC2::Ref> + Len, TC2: Index> Push<Slice<TC2>> for Vecs<TC> {
        fn push(&mut self, item: Slice<TC2>) {
            self.values.extend(item.into_iter());
            self.bounds.push(self.values.len() as u64);
        }
    }
    impl<'a, T, TC: Push<&'a T> + Len> Push<&'a Vec<T>> for Vecs<TC> {
        fn push(&mut self, item: &'a Vec<T>) {
            self.push(&item[..]);
        }
    }
    impl<'a, T, TC: Push<&'a T> + Len, const N: usize> Push<&'a [T; N]> for Vecs<TC> {
        fn push(&mut self, item: &'a [T; N]) {
            self.push(&item[..]);
        }
    }
    impl<'a, T, TC: Push<&'a T> + Len> Push<&'a [T]> for Vecs<TC> {
        fn push(&mut self, item: &'a [T]) {
            self.values.extend(item.iter());
            self.bounds.push(self.values.len() as u64);
        }
    }
    impl<TC: Clear> Clear for Vecs<TC> {
        fn clear(&mut self) {
            self.bounds.clear();
            self.values.clear();
        }
    }

    impl<TC: HeapSize, BC: HeapSize> HeapSize for Vecs<TC, BC> {
        fn heap_size(&self) -> (usize, usize) {
            let (l0, c0) = self.bounds.heap_size();
            let (l1, c1) = self.values.heap_size();
            (l0 + l1, c0 + c1)
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
                type Ref<'a> = ($($name::Ref<'a>,)*) where $($name: 'a,)*;
                fn copy_from<'a>(&mut self, other: Self::Ref<'a>) {
                    let ($($name,)*) = self;
                    let ($($name2,)*) = other;
                    $(crate::Columnar::copy_from($name, $name2);)*
                }
                fn into_owned<'a>(other: Self::Ref<'a>) -> Self {
                    let ($($name2,)*) = other;
                    ($($name::into_owned($name2),)*)
                }
                type Container = ($($name::Container,)*);
            }
            impl<$($name: crate::Columnar, $name2: crate::Container<$name>,)*> crate::Container<($($name,)*)> for ($($name2,)*) {
                type Borrowed<'a> = ($($name2::Borrowed<'a>,)*) where $($name: 'a, $name2: 'a,)*;
                fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
                    let ($($name,)*) = self;
                    ($($name.borrow(),)*)
                }
            }

            #[allow(non_snake_case)]
            impl<'a, $($name: crate::AsBytes<'a>),*> crate::AsBytes<'a> for ($($name,)*) {
                fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
                    let ($($name,)*) = self;
                    let iter = None.into_iter();
                    $( let iter = iter.chain($name.as_bytes()); )*
                    iter
                }
            }
            impl<'a, $($name: crate::FromBytes<'a>),*> crate::FromBytes<'a> for ($($name,)*) {
                #[allow(non_snake_case)]
                fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
                    $(let $name = crate::FromBytes::from_bytes(bytes);)*
                    ($($name,)*)
                }
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

            let mut column: <(u64, u8, String) as Columnar>::Container = Default::default();
            for i in 0..100 {
                column.push((i, i as u8, &i.to_string()));
                column.push((i, i as u8, &"".to_string()));
            }

            assert_eq!(column.len(), 200);
            assert_eq!(column.heap_size(), (3590, 4608));

            for i in 0..100u64 {
                assert_eq!((&column).get((2*i+0) as usize), (&i, &(i as u8), i.to_string().as_str()));
                assert_eq!((&column).get((2*i+1) as usize), (&i, &(i as u8), ""));
            }

            // Compare to the heap size of a `Vec<Option<usize>>`.
            let mut column: Vec<(u64, u8, String)> = Default::default();
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
        use crate::common::index::CopyAs;
        use crate::{Len, Index, IndexAs, Push, Clear, HeapSize};

        /// A store for maintaining `Vec<bool>` with fast `rank` and `select` access.
        ///
        /// The design is to have `u64` running counts for each block of 1024 bits,
        /// which are roughly the size of a cache line. This is roughly 6% overhead,
        /// above the bits themselves, which seems pretty solid.
        #[derive(Copy, Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
        pub struct RankSelect<CC = Vec<u64>, VC = Vec<u64>, WC = u64> {
            /// Counts of the number of cumulative set (true) bits, *after* each block of 1024 bits.
            pub counts: CC,
            /// The bits themselves.
            pub values: Bools<VC, WC>,
        }

        impl<CC: crate::Container<u64>, VC: crate::Container<u64>> RankSelect<CC, VC> {
            pub fn borrow<'a>(&'a self) -> RankSelect<CC::Borrowed<'a>, VC::Borrowed<'a>, &'a u64> {
                use crate::Container;
                RankSelect {
                    counts: self.counts.borrow(),
                    values: self.values.borrow(),
                }
            }
        }

        impl<'a, CC: crate::AsBytes<'a>, VC: crate::AsBytes<'a>> crate::AsBytes<'a> for RankSelect<CC, VC, &'a u64> {
            fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
                self.counts.as_bytes().chain(self.values.as_bytes())
            }
        }
        impl<'a, CC: crate::FromBytes<'a>, VC: crate::FromBytes<'a>> crate::FromBytes<'a> for RankSelect<CC, VC, &'a u64> {
            fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
                Self {
                    counts: crate::FromBytes::from_bytes(bytes),
                    values: crate::FromBytes::from_bytes(bytes),
                }
            }
        }


        impl<CC, VC: Len + IndexAs<u64>, WC: Copy+CopyAs<u64>> RankSelect<CC, VC, WC> {
            #[inline]
            pub fn get(&self, index: usize) -> bool {
                Index::get(&self.values, index)
            }
        }
        impl<CC: Len + IndexAs<u64>, VC: Len + IndexAs<u64>, WC: Copy+CopyAs<u64>> RankSelect<CC, VC, WC> {
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
                let intra_word = if block == self.values.values.len() { self.values.last_word.copy_as() } else { self.values.values.index_as(block) };
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
                let last_bits = if block == self.values.values.len() { self.values.last_bits.copy_as() as usize } else { 64 };
                let last_word = if block == self.values.values.len() { self.values.last_word.copy_as() } else { self.values.values.index_as(block) };
                for shift in 0 .. last_bits {
                    if ((last_word >> shift) & 0x01 == 0x01) && count + 1 == rank {
                        return Some(64 * block + shift);
                    }
                    count += (last_word >> shift) & 0x01;
                }

                None
            }
        }

        impl<CC, VC: Len, WC: Copy + CopyAs<u64>> RankSelect<CC, VC, WC> {
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

        use crate::common::index::CopyAs;
        use crate::{Clear, Columnar, Len, IndexMut, Index, IndexAs, Push, HeapSize};
        use crate::RankSelect;

        #[derive(Copy, Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
        pub struct Results<SC, TC, CC=Vec<u64>, VC=Vec<u64>, WC=u64> {
            /// Bits set to `true` correspond to `Ok` variants.
            pub indexes: RankSelect<CC, VC, WC>,
            pub oks: SC,
            pub errs: TC,
        }

        impl<S: Columnar, T: Columnar> Columnar for Result<S, T> {
            type Ref<'a> = Result<S::Ref<'a>, T::Ref<'a>> where S: 'a, T: 'a;
            fn copy_from<'a>(&mut self, other: Self::Ref<'a>) {
                match (&mut *self, other) {
                    (Ok(x), Ok(y)) => x.copy_from(y),
                    (Err(x), Err(y)) => x.copy_from(y),
                    (_, other) => { *self = Self::into_owned(other); },
                }
            }
            fn into_owned<'a>(other: Self::Ref<'a>) -> Self {
                match other {
                    Ok(y) => Ok(S::into_owned(y)),
                    Err(y) => Err(T::into_owned(y)),
                }
            }
            type Container = Results<S::Container, T::Container>;
        }

        impl<S: Columnar, T: Columnar, SC: crate::Container<S>, TC: crate::Container<T>> crate::Container<Result<S, T>> for Results<SC, TC> {
            type Borrowed<'a> = Results<SC::Borrowed<'a>, TC::Borrowed<'a>, &'a [u64], &'a [u64], &'a u64> where SC: 'a, TC: 'a, S:'a, T: 'a;
            fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
                Results {
                    indexes: self.indexes.borrow(),
                    oks: self.oks.borrow(),
                    errs: self.errs.borrow(),
                }
            }
        }

        impl<'a, SC: crate::AsBytes<'a>, TC: crate::AsBytes<'a>, CC: crate::AsBytes<'a>, VC: crate::AsBytes<'a>> crate::AsBytes<'a> for Results<SC, TC, CC, VC, &'a u64> {
            fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
                self.indexes.as_bytes().chain(self.oks.as_bytes()).chain(self.errs.as_bytes())
            }
        }
        impl<'a, SC: crate::FromBytes<'a>, TC: crate::FromBytes<'a>, CC: crate::FromBytes<'a>, VC: crate::FromBytes<'a>> crate::FromBytes<'a> for Results<SC, TC, CC, VC, &'a u64> {
            fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
                Self {
                    indexes: crate::FromBytes::from_bytes(bytes),
                    oks: crate::FromBytes::from_bytes(bytes),
                    errs: crate::FromBytes::from_bytes(bytes),
                }
            }
        }

        impl<SC, TC, CC, VC: Len, WC: Copy+CopyAs<u64>> Len for Results<SC, TC, CC, VC, WC> {
            #[inline(always)] fn len(&self) -> usize { self.indexes.len() }
        }

        impl<SC, TC, CC, VC, WC> Index for Results<SC, TC, CC, VC, WC>
        where
            SC: Index,
            TC: Index,
            CC: IndexAs<u64> + Len,
            VC: IndexAs<u64> + Len,
            WC: Copy + CopyAs<u64>,
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
        impl<'a, SC, TC, CC, VC, WC> Index for &'a Results<SC, TC, CC, VC, WC>
        where
            &'a SC: Index,
            &'a TC: Index,
            CC: IndexAs<u64> + Len,
            VC: IndexAs<u64> + Len,
            WC: Copy + CopyAs<u64>,
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

                let mut column: <Result<u64, u64> as Columnar>::Container = Default::default();
                for i in 0..100 {
                    column.push(Ok::<u64, u64>(i));
                    column.push(Err::<u64, u64>(i));
                }

                assert_eq!(column.len(), 200);
                assert_eq!(column.heap_size(), (1624, 2080));

                for i in 0..100 {
                    assert_eq!(column.get(2*i+0), Ok(i as u64));
                    assert_eq!(column.get(2*i+1), Err(i as u64));
                }

                let mut column: <Result<u64, u8> as Columnar>::Container = Default::default();
                for i in 0..100 {
                    column.push(Ok::<u64, u8>(i as u64));
                    column.push(Err::<u64, u8>(i as u8));
                }

                assert_eq!(column.len(), 200);
                assert_eq!(column.heap_size(), (924, 1184));

                for i in 0..100 {
                    assert_eq!(column.get(2*i+0), Ok(i as u64));
                    assert_eq!(column.get(2*i+1), Err(i as u8));
                }
            }
        }
    }

    pub mod option {

        use crate::common::index::CopyAs;
        use crate::{Clear, Columnar, Len, IndexMut, Index, IndexAs, Push, HeapSize};
        use crate::RankSelect;

        #[derive(Copy, Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
        pub struct Options<TC, CC=Vec<u64>, VC=Vec<u64>, WC=u64> {
            /// Uses two bits for each item, one to indicate the variant and one (amortized)
            /// to enable efficient rank determination.
            pub indexes: RankSelect<CC, VC, WC>,
            pub somes: TC,
        }

        impl<T: Columnar> Columnar for Option<T> {
            type Ref<'a> = Option<T::Ref<'a>> where T: 'a;
            fn copy_from<'a>(&mut self, other: Self::Ref<'a>) {
                match (&mut *self, other) {
                    (Some(x), Some(y)) => { x.copy_from(y); }
                    (_, other) => { *self = Self::into_owned(other); }
                }
            }
            fn into_owned<'a>(other: Self::Ref<'a>) -> Self {
                other.map(|x| T::into_owned(x))
            }
            type Container = Options<T::Container>;
        }

        impl<T: Columnar, TC: crate::Container<T>> crate::Container<Option<T>> for Options<TC> {
            type Borrowed<'a> = Options<TC::Borrowed<'a>, &'a [u64], &'a [u64], &'a u64> where TC: 'a, T: 'a;
            fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
                Options {
                    indexes: self.indexes.borrow(),
                    somes: self.somes.borrow(),
                }
            }
        }

        impl<'a, TC: crate::AsBytes<'a>, CC: crate::AsBytes<'a>, VC: crate::AsBytes<'a>> crate::AsBytes<'a> for Options<TC, CC, VC, &'a u64> {
            fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
                self.indexes.as_bytes().chain(self.somes.as_bytes())
            }
        }

        impl <'a, TC: crate::FromBytes<'a>, CC: crate::FromBytes<'a>, VC: crate::FromBytes<'a>> crate::FromBytes<'a> for Options<TC, CC, VC, &'a u64> {
            fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
                Self {
                    indexes: crate::FromBytes::from_bytes(bytes),
                    somes: crate::FromBytes::from_bytes(bytes),
                }
            }
        }

        impl<T, CC, VC: Len, WC: Copy + CopyAs<u64>> Len for Options<T, CC, VC, WC> {
            #[inline(always)] fn len(&self) -> usize { self.indexes.len() }
        }

        impl<TC: Index, CC: IndexAs<u64> + Len, VC: IndexAs<u64> + Len, WC: Copy+CopyAs<u64>> Index for Options<TC, CC, VC, WC> {
            type Ref = Option<TC::Ref>;
            fn get(&self, index: usize) -> Self::Ref {
                if self.indexes.get(index) {
                    Some(self.somes.get(self.indexes.rank(index)))
                } else {
                    None
                }
            }
        }
        impl<'a, TC, CC: IndexAs<u64> + Len, VC: IndexAs<u64> + Len, WC: Copy+CopyAs<u64>> Index for &'a Options<TC, CC, VC, WC>
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

    impl<'a, V, CV: Push<&'a V> + Len + Default> Push<&'a Vec<V>> for ListMaps<CV> {
        fn push(&mut self, item: &'a Vec<V>) {
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
                store.push(&record);
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
