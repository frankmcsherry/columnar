//! Types that prefer to be represented by `Vec<T>`.

use std::num::Wrapping;

/// An implementation of opinions for types that want to use `Vec<T>`.
macro_rules! implement_columnable {
    ($($index_type:ty),*) => { $(
        impl crate::Columnar for $index_type {
            #[inline(always)]
            fn into_owned<'a>(other: crate::Ref<'a, Self>) -> Self { *other }

            type Container = Vec<$index_type>;
        }

        impl crate::HeapSize for $index_type { }

        impl<'a> crate::AsBytes<'a> for &'a [$index_type] {
            #[inline(always)]
            fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
                std::iter::once((std::mem::align_of::<$index_type>() as u64, bytemuck::cast_slice(&self[..])))
            }
        }
        impl<'a> crate::FromBytes<'a> for &'a [$index_type] {
            #[inline(always)]
            fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
                // We use `unwrap()` here in order to panic with the `bytemuck` error, which may be informative.
                bytemuck::try_cast_slice(bytes.next().expect("Iterator exhausted prematurely")).unwrap()
            }
        }
        impl<'a, const N: usize> crate::AsBytes<'a> for &'a [[$index_type; N]] {
            #[inline(always)]
            fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
                std::iter::once((std::mem::align_of::<$index_type>() as u64, bytemuck::cast_slice(&self[..])))
            }
        }
        impl<'a, const N: usize> crate::FromBytes<'a> for &'a [[$index_type; N]] {
            #[inline(always)]
            fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
                // We use `unwrap()` here in order to panic with the `bytemuck` error, which may be informative.
                bytemuck::try_cast_slice(bytes.next().expect("Iterator exhausted prematurely")).unwrap()
            }
        }
    )* }
}

implement_columnable!(u8, u16, u32, u64);
implement_columnable!(i8, i16, i32, i64);
implement_columnable!(f32, f64);
implement_columnable!(Wrapping<u8>, Wrapping<u16>, Wrapping<u32>, Wrapping<u64>);
implement_columnable!(Wrapping<i8>, Wrapping<i16>, Wrapping<i32>, Wrapping<i64>);

pub use sizes::{Usizes, Isizes};
/// Columnar stores for `usize` and `isize`, stored as 64 bits.
mod sizes {

    use crate::*;
    use crate::common::{BorrowIndexAs, PushIndexAs};

    #[derive(Copy, Clone, Default)]
    pub struct Usizes<CV = Vec<u64>> { pub values: CV }

    impl Columnar for usize {
        fn into_owned<'a>(other: crate::Ref<'a, Self>) -> Self { other }
        type Container = Usizes;
    }

    impl<CV: BorrowIndexAs<u64> + Len> Borrow for Usizes<CV> {
        type Ref<'a> = usize;
        type Borrowed<'a> = Usizes<CV::Borrowed<'a>> where CV: 'a;
        fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
            Usizes { values: self.values.borrow() }
        }
        #[inline(always)]
        fn reborrow<'b, 'a: 'b>(thing: Self::Borrowed<'a>) -> Self::Borrowed<'b> where CV: 'a {
            Usizes { values: CV::reborrow(thing.values) }
        }
        #[inline(always)]
        fn reborrow_ref<'b, 'a: 'b>(thing: Self::Ref<'a>) -> Self::Ref<'b> where Self: 'a { thing }
    }

    impl<CV: PushIndexAs<u64>> Container for Usizes<CV> {
        #[inline(always)]
        fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: std::ops::Range<usize>) {
            self.values.extend_from_self(other.values, range)
        }

        fn reserve_for<'a, I>(&mut self, selves: I) where Self: 'a, I: Iterator<Item = Self::Borrowed<'a>> + Clone {
            self.values.reserve_for(selves.map(|x| x.values))
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
    impl<CV: IndexAs<u64>> Index for &Usizes<CV> {
        type Ref = usize;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref { self.values.index_as(index).try_into().expect("Usizes values should fit in `usize`") }
    }
    impl<CV: for<'a> Push<&'a u64>> Push<usize> for Usizes<CV> {
        #[inline]
        fn push(&mut self, item: usize) { self.values.push(&item.try_into().expect("usize must fit in a u64")) }
    }
    impl Push<&usize> for Usizes {
        #[inline]
        fn push(&mut self, item: &usize) { self.values.push((*item).try_into().expect("usize must fit in a u64")) }
    }
    impl<CV: Clear> Clear for Usizes<CV> { fn clear(&mut self) { self.values.clear() }}

    impl<CV: HeapSize> HeapSize for Usizes<CV> {
        fn heap_size(&self) -> (usize, usize) {
            self.values.heap_size()
        }
    }

    impl<'a, CV: crate::AsBytes<'a>> crate::AsBytes<'a> for crate::primitive::Usizes<CV> {
        #[inline(always)]
        fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
            self.values.as_bytes()
        }
    }

    impl<'a, CV: crate::FromBytes<'a>> crate::FromBytes<'a> for crate::primitive::Usizes<CV> {
        #[inline(always)]
        fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
            Self { values: CV::from_bytes(bytes) }
        }
    }


    #[derive(Copy, Clone, Default)]
    pub struct Isizes<CV = Vec<i64>> { pub values: CV }

    impl Columnar for isize {
        fn into_owned<'a>(other: crate::Ref<'a, Self>) -> Self { other }
        type Container = Isizes;
    }

    impl<CV: BorrowIndexAs<i64>> Borrow for Isizes<CV> {
        type Ref<'a> = isize;
        type Borrowed<'a> = Isizes<CV::Borrowed<'a>> where CV: 'a;
        fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
            Isizes { values: self.values.borrow() }
        }
        #[inline(always)]
        fn reborrow<'b, 'a: 'b>(thing: Self::Borrowed<'a>) -> Self::Borrowed<'b> where CV: 'a {
            Isizes { values: CV::reborrow(thing.values) }
        }
        #[inline(always)]
        fn reborrow_ref<'b, 'a: 'b>(thing: Self::Ref<'a>) -> Self::Ref<'b> where Self: 'a { thing }
    }

    impl<CV: PushIndexAs<i64>> Container for Isizes<CV> {
        #[inline(always)]
        fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: std::ops::Range<usize>) {
            self.values.extend_from_self(other.values, range)
        }

        fn reserve_for<'a, I>(&mut self, selves: I) where Self: 'a, I: Iterator<Item = Self::Borrowed<'a>> + Clone {
            self.values.reserve_for(selves.map(|x| x.values))
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
    impl<CV: IndexAs<i64>> Index for &Isizes<CV> {
        type Ref = isize;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref { self.values.index_as(index).try_into().expect("Isizes values should fit in `isize`") }
    }
    impl<CV: for<'a> Push<&'a i64>> Push<isize> for Isizes<CV> {
        #[inline]
        fn push(&mut self, item: isize) { self.values.push(&item.try_into().expect("isize must fit in a i64")) }
    }
    impl Push<&isize> for Isizes {
        #[inline]
        fn push(&mut self, item: &isize) { self.values.push((*item).try_into().expect("isize must fit in a i64")) }
    }
    impl<CV: Clear> Clear for Isizes<CV> { fn clear(&mut self) { self.values.clear() }}

    impl<CV: HeapSize> HeapSize for Isizes<CV> {
        fn heap_size(&self) -> (usize, usize) {
            self.values.heap_size()
        }
    }

    impl<'a, CV: crate::AsBytes<'a>> crate::AsBytes<'a> for crate::primitive::Isizes<CV> {
        #[inline(always)]
        fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
            self.values.as_bytes()
        }
    }

    impl<'a, CV: crate::FromBytes<'a>> crate::FromBytes<'a> for crate::primitive::Isizes<CV> {
        #[inline(always)]
        fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
            Self { values: CV::from_bytes(bytes) }
        }
    }
}

pub use chars::{Chars};
/// Columnar store for `char`, stored as a `u32`.
mod chars {

    use crate::*;
    use crate::common::{BorrowIndexAs, PushIndexAs};

    type Encoded = u32;

    #[derive(Copy, Clone, Default)]
    pub struct Chars<CV = Vec<Encoded>> { pub values: CV }

    impl Columnar for char {
        fn into_owned<'a>(other: crate::Ref<'a, Self>) -> Self { other }
        type Container = Chars;
    }

    impl<CV: BorrowIndexAs<Encoded>> Borrow for Chars<CV> {
        type Ref<'a> = char;
        type Borrowed<'a> = Chars<CV::Borrowed<'a>> where CV: 'a;
        fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
            Chars { values: self.values.borrow() }
        }
        #[inline(always)]
        fn reborrow<'b, 'a: 'b>(thing: Self::Borrowed<'a>) -> Self::Borrowed<'b> where CV: 'a {
            Chars { values: CV::reborrow(thing.values) }
        }
        #[inline(always)]
        fn reborrow_ref<'b, 'a: 'b>(thing: Self::Ref<'a>) -> Self::Ref<'b> where Self: 'a { thing }
    }

    impl<CV: PushIndexAs<Encoded>> Container for Chars<CV> {
        #[inline(always)]
        fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: std::ops::Range<usize>) {
            self.values.extend_from_self(other.values, range)
        }

        fn reserve_for<'a, I>(&mut self, selves: I) where Self: 'a, I: Iterator<Item = Self::Borrowed<'a>> + Clone {
            self.values.reserve_for(selves.map(|x| x.values))
        }
    }

    impl<CV: Len> Len for Chars<CV> { fn len(&self) -> usize { self.values.len() }}
    impl<CV: IndexAs<Encoded>> Index for Chars<CV> {
        type Ref = char;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref { char::from_u32(self.values.index_as(index)).unwrap() }
    }
    impl<CV: IndexAs<Encoded>> Index for &Chars<CV> {
        type Ref = char;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref { char::from_u32(self.values.index_as(index)).unwrap() }
    }
    impl<CV: for<'a> Push<&'a Encoded>> Push<char> for Chars<CV> {
        #[inline]
        fn push(&mut self, item: char) { self.values.push(&u32::from(item)) }
    }
    impl Push<&char> for Chars {
        #[inline]
        fn push(&mut self, item: &char) { self.values.push(u32::from(*item)) }
    }
    impl<CV: Clear> Clear for Chars<CV> { fn clear(&mut self) { self.values.clear() }}

    impl<CV: HeapSize> HeapSize for Chars<CV> {
        fn heap_size(&self) -> (usize, usize) {
            self.values.heap_size()
        }
    }

    impl<'a, CV: crate::AsBytes<'a>> crate::AsBytes<'a> for Chars<CV> {
        #[inline(always)]
        fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
            self.values.as_bytes()
        }
    }

    impl<'a, CV: crate::FromBytes<'a>> crate::FromBytes<'a> for Chars<CV> {
        #[inline(always)]
        fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
            Self { values: CV::from_bytes(bytes) }
        }
    }
}

pub use larges::{U128s, I128s};
/// Columnar stores for `u128` and `i128`, stored as [u8; 16] bits.
mod larges {

    use crate::*;
    use crate::common::{BorrowIndexAs, PushIndexAs};

    type Encoded = [u8; 16];

    #[derive(Copy, Clone, Default)]
    pub struct U128s<CV = Vec<Encoded>> { pub values: CV }

    impl Columnar for u128 {
        fn into_owned<'a>(other: crate::Ref<'a, Self>) -> Self { other }
        type Container = U128s;
    }

    impl<CV: BorrowIndexAs<Encoded>> Borrow for U128s<CV> {
        type Ref<'a> = u128;
        type Borrowed<'a> = U128s<CV::Borrowed<'a>> where CV: 'a;
        fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
            U128s { values: self.values.borrow() }
        }
        #[inline(always)]
        fn reborrow<'b, 'a: 'b>(thing: Self::Borrowed<'a>) -> Self::Borrowed<'b> where CV: 'a {
            U128s { values: CV::reborrow(thing.values) }
        }
        #[inline(always)]
        fn reborrow_ref<'b, 'a: 'b>(thing: Self::Ref<'a>) -> Self::Ref<'b> where Self: 'a { thing }
    }

    impl<CV: PushIndexAs<Encoded>> Container for U128s<CV> {
        #[inline(always)]
        fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: std::ops::Range<usize>) {
            self.values.extend_from_self(other.values, range)
        }

        fn reserve_for<'a, I>(&mut self, selves: I) where Self: 'a, I: Iterator<Item = Self::Borrowed<'a>> + Clone {
            self.values.reserve_for(selves.map(|x| x.values))
        }
    }

    impl<CV: Len> Len for U128s<CV> { fn len(&self) -> usize { self.values.len() }}
    impl<CV: IndexAs<Encoded>> Index for U128s<CV> {
        type Ref = u128;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref { u128::from_le_bytes(self.values.index_as(index)) }
    }
    impl<CV: IndexAs<Encoded>> Index for &U128s<CV> {
        type Ref = u128;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref { u128::from_le_bytes(self.values.index_as(index)) }
    }
    impl<CV: for<'a> Push<&'a Encoded>> Push<u128> for U128s<CV> {
        #[inline]
        fn push(&mut self, item: u128) { self.values.push(&item.to_le_bytes()) }
    }
    impl Push<&u128> for U128s {
        #[inline]
        fn push(&mut self, item: &u128) { self.values.push(item.to_le_bytes()) }
    }
    impl<CV: Clear> Clear for U128s<CV> { fn clear(&mut self) { self.values.clear() }}

    impl<CV: HeapSize> HeapSize for U128s<CV> {
        fn heap_size(&self) -> (usize, usize) {
            self.values.heap_size()
        }
    }

    impl<'a, CV: crate::AsBytes<'a>> crate::AsBytes<'a> for U128s<CV> {
        #[inline(always)]
        fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
            self.values.as_bytes()
        }
    }

    impl<'a, CV: crate::FromBytes<'a>> crate::FromBytes<'a> for U128s<CV> {
        #[inline(always)]
        fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
            Self { values: CV::from_bytes(bytes) }
        }
    }

    #[derive(Copy, Clone, Default)]
    pub struct I128s<CV = Vec<Encoded>> { pub values: CV }

    impl Columnar for i128 {
        fn into_owned<'a>(other: crate::Ref<'a, Self>) -> Self { other }
        type Container = I128s;
    }

    impl<CV: BorrowIndexAs<Encoded>> Borrow for I128s<CV> {
        type Ref<'a> = i128;
        type Borrowed<'a> = I128s<CV::Borrowed<'a>> where CV: 'a;
        fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
            I128s { values: self.values.borrow() }
        }
        #[inline(always)]
        fn reborrow<'b, 'a: 'b>(thing: Self::Borrowed<'a>) -> Self::Borrowed<'b> where CV: 'a {
            I128s { values: CV::reborrow(thing.values) }
        }
        #[inline(always)]
        fn reborrow_ref<'b, 'a: 'b>(thing: Self::Ref<'a>) -> Self::Ref<'b> where Self: 'a { thing }
    }

    impl<CV: PushIndexAs<Encoded>> Container for I128s<CV> {
        #[inline(always)]
        fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: std::ops::Range<usize>) {
            self.values.extend_from_self(other.values, range)
        }

        fn reserve_for<'a, I>(&mut self, selves: I) where Self: 'a, I: Iterator<Item = Self::Borrowed<'a>> + Clone {
            self.values.reserve_for(selves.map(|x| x.values))
        }
    }

    impl<CV: Len> Len for I128s<CV> { fn len(&self) -> usize { self.values.len() }}
    impl<CV: IndexAs<Encoded>> Index for I128s<CV> {
        type Ref = i128;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref { i128::from_le_bytes(self.values.index_as(index)) }
    }
    impl<CV: IndexAs<Encoded>> Index for &I128s<CV> {
        type Ref = i128;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref { i128::from_le_bytes(self.values.index_as(index)) }
    }
    impl<CV: for<'a> Push<&'a Encoded>> Push<i128> for I128s<CV> {
        #[inline]
        fn push(&mut self, item: i128) { self.values.push(&item.to_le_bytes()) }
    }
    impl Push<&i128> for I128s {
        #[inline]
        fn push(&mut self, item: &i128) { self.values.push(item.to_le_bytes()) }
    }
    impl<CV: Clear> Clear for I128s<CV> { fn clear(&mut self) { self.values.clear() }}

    impl<CV: HeapSize> HeapSize for I128s<CV> {
        fn heap_size(&self) -> (usize, usize) {
            self.values.heap_size()
        }
    }

    impl<'a, CV: crate::AsBytes<'a>> crate::AsBytes<'a> for I128s<CV> {
        #[inline(always)]
        fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
            self.values.as_bytes()
        }
    }

    impl<'a, CV: crate::FromBytes<'a>> crate::FromBytes<'a> for I128s<CV> {
        #[inline(always)]
        fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
            Self { values: CV::from_bytes(bytes) }
        }
    }
}

/// Columnar stores for non-decreasing `u64`, stored in various ways.
///
/// The venerable `Vec<u64>` works as a general container for arbitrary offests,
/// but it can be non-optimal for various patterns of offset, including constant
/// inter-offset spacing, and relatively short runs (compared to a `RankSelect`).
pub mod offsets {

    pub use array::Fixeds;
    pub use stride::Strides;

    /// An offset container that encodes a constant spacing in its type.
    ///
    /// Any attempt to push any value will result in pushing the next value
    /// at the specified spacing. This type is only appropriate in certain
    /// contexts, for example when storing `[T; K]` array types, or having
    /// introspected a `Strides` and found it to be only one constant stride.
    mod array {

        use crate::{Container, Borrow, Index, Len, Push};
        use crate::common::index::CopyAs;

        /// An offset container that encodes a constant `K` spacing.
        #[derive(Copy, Clone, Debug, Default)]
        pub struct Fixeds<const K: u64, CC = u64> { pub count: CC }

        impl<const K: u64> Borrow for Fixeds<K> {
            type Ref<'a> = u64;
            type Borrowed<'a> = Fixeds<K, &'a u64>;
            #[inline(always)]
            fn borrow<'a>(&'a self) -> Self::Borrowed<'a> { Fixeds { count: &self.count } }
            #[inline(always)]
            fn reborrow<'b, 'a: 'b>(thing: Self::Borrowed<'a>) -> Self::Borrowed<'b> where Self: 'a {
                Fixeds { count: thing.count }
            }
            #[inline(always)]
            fn reborrow_ref<'b, 'a: 'b>(thing: Self::Ref<'a>) -> Self::Ref<'b> where Self: 'a { thing }
        }

        impl<const K: u64> Container for Fixeds<K> {
            #[inline(always)]
            fn extend_from_self(&mut self, _other: Self::Borrowed<'_>, range: std::ops::Range<usize>) {
                self.count += range.len() as u64;
            }

            fn reserve_for<'a, I>(&mut self, _selves: I) where Self: 'a, I: Iterator<Item = Self::Borrowed<'a>> + Clone { }
        }

        impl<const K: u64, CC: CopyAs<u64>> Len for Fixeds<K, CC> {
            #[inline(always)] fn len(&self) -> usize { self.count.copy_as() as usize }
        }

        impl<const K: u64, CC> Index for Fixeds<K, CC> {
            type Ref = u64;
            #[inline(always)]
            fn get(&self, index: usize) -> Self::Ref { (index as u64 + 1) * K }
        }
        impl<const K: u64, CC> Index for &Fixeds<K, CC> {
            type Ref = u64;
            #[inline(always)]
            fn get(&self, index: usize) -> Self::Ref { (index as u64 + 1) * K }
        }

        impl<'a, const K: u64, T> Push<T> for Fixeds<K> {
            // TODO: check for overflow?
            #[inline(always)]
            fn push(&mut self, _item: T) { self.count += 1; }
            #[inline(always)]
            fn extend(&mut self, iter: impl IntoIterator<Item=T>) {
                self.count += iter.into_iter().count() as u64;
            }
        }

        impl<const K: u64> crate::HeapSize for Fixeds<K> {
            #[inline(always)]
            fn heap_size(&self) -> (usize, usize) { (0, 0) }
        }
        impl<const K: u64> crate::Clear for Fixeds<K> {
            #[inline(always)]
            fn clear(&mut self) { self.count = 0; }
        }

        impl<'a, const K: u64> crate::AsBytes<'a> for Fixeds<K, &'a u64> {
            #[inline(always)]
            fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
                std::iter::once((8, bytemuck::cast_slice(std::slice::from_ref(self.count))))
            }
        }
        impl<'a, const K: u64> crate::FromBytes<'a> for Fixeds<K, &'a u64> {
            #[inline(always)]
            fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
                Self { count: &bytemuck::try_cast_slice(bytes.next().expect("Iterator exhausted prematurely")).unwrap()[0] }
            }
        }

        use super::Strides;
        impl<const K: u64, BC: Len, CC: CopyAs<u64>> std::convert::TryFrom<Strides<BC, CC>> for Fixeds<K, CC> {
            // On error we return the original.
            type Error = Strides<BC, CC>;
            fn try_from(item: Strides<BC, CC>) -> Result<Self, Self::Error> {
                if item.strided() == Some(K) { Ok( Self { count: item.length } ) } else { Err(item) }
            }
        }
    }

    /// An general offset container optimized for fixed inter-offset sizes.
    ///
    /// Although it can handle general offsets, it starts with the optimistic
    /// assumption that the offsets will be evenly spaced from zero, and while
    /// that holds it will maintain the stride and length. Should it stop being
    /// true, when a non-confirming offset is pushed, it will start to store
    /// the offsets in a general container.
    mod stride {

        use std::ops::Deref;
        use crate::{Container, Borrow, Index, Len, Push, Clear, AsBytes, FromBytes};
        use crate::common::index::CopyAs;

        /// The first two integers describe a stride pattern, [stride, length].
        ///
        /// If the length is zero the collection is empty. The first `item` pushed
        /// always becomes the first list element. The next element is the number of
        /// items at position `i` whose value is `item * (i+1)`. After this comes
        /// the remaining entries in the bounds container.
        #[derive(Copy, Clone, Debug, Default)]
        pub struct Strides<BC = Vec<u64>, CC = u64> {
            pub stride: CC,
            pub length: CC,
            pub bounds: BC,
        }

        impl Borrow for Strides {
            type Ref<'a> = u64;
            type Borrowed<'a> = Strides<&'a [u64], &'a u64>;

            #[inline(always)] fn borrow<'a>(&'a self) -> Self::Borrowed<'a> { Strides { stride: &self.stride, length: &self.length, bounds: &self.bounds[..] } }
            /// Reborrows the borrowed type to a shorter lifetime. See [`Columnar::reborrow`] for details.
            #[inline(always)] fn reborrow<'b, 'a: 'b>(item: Self::Borrowed<'a>) -> Self::Borrowed<'b> where Self: 'a {
                Strides { stride: item.stride, length: item.length, bounds: item.bounds }
            }
            /// Reborrows the borrowed type to a shorter lifetime. See [`Columnar::reborrow`] for details.
                #[inline(always)]fn reborrow_ref<'b, 'a: 'b>(item: Self::Ref<'a>) -> Self::Ref<'b> where Self: 'a { item }
        }

        impl Container for Strides {
            fn reserve_for<'a, I>(&mut self, selves: I) where Self: 'a, I: Iterator<Item = Self::Borrowed<'a>> + Clone {
                self.bounds.reserve_for(selves.map(|x| x.bounds))
            }
        }

        impl<'a> Push<&'a u64> for Strides { #[inline(always)] fn push(&mut self, item: &'a u64) { self.push(*item) } }
        impl Push<u64> for Strides { #[inline(always)] fn push(&mut self, item: u64) { self.push(item) } }
        impl Clear for Strides { #[inline(always)] fn clear(&mut self) { self.clear() } }

        impl<BC: Len, CC: CopyAs<u64>> Len for Strides<BC, CC> {
            #[inline(always)]
            fn len(&self) -> usize { self.length.copy_as() as usize + self.bounds.len() }
        }
        impl Index for Strides<&[u64], &u64> {
            type Ref = u64;
            #[inline(always)]
            fn get(&self, index: usize) -> Self::Ref {
                let index = index as u64;
                if index < *self.length { (index+1) * self.stride } else { self.bounds[(index - self.length) as usize] }
            }
        }

        impl<'a, BC: AsBytes<'a>> AsBytes<'a> for Strides<BC, &'a u64> {
            #[inline(always)]
            fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
                let stride = std::iter::once((8, bytemuck::cast_slice(std::slice::from_ref(self.stride))));
                let length = std::iter::once((8, bytemuck::cast_slice(std::slice::from_ref(self.length))));
                let bounds = self.bounds.as_bytes();
                crate::chain(stride, crate::chain(length, bounds))
            }
        }
        impl<'a, BC: FromBytes<'a>> FromBytes<'a> for Strides<BC, &'a u64> {
            #[inline(always)]
            fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
                let stride = &bytemuck::try_cast_slice(bytes.next().expect("Iterator exhausted prematurely")).unwrap()[0];
                let length = &bytemuck::try_cast_slice(bytes.next().expect("Iterator exhausted prematurely")).unwrap()[0];
                let bounds = BC::from_bytes(bytes);
                Self { stride, length, bounds }
            }
        }

        impl Strides {
            pub fn new(stride: u64, length: u64) -> Self {
                Self { stride, length, bounds: Vec::default() }
            }
            #[inline(always)]
            pub fn push(&mut self, item: u64) {
                if self.length == 0 {
                    self.stride = item;
                    self.length = 1;
                }
                else if !self.bounds.is_empty() {
                    self.bounds.push(item);
                }
                else if item == self.stride * (self.length + 1) {
                    self.length += 1;
                }
                else {
                    self.bounds.push(item);
                }
            }
            #[inline(always)]
            pub fn clear(&mut self) {
                self.stride = 0;
                self.length = 0;
                self.bounds.clear();
            }
        }

        impl<BC: Deref<Target=[u64]>, CC: CopyAs<u64>> Strides<BC, CC> {
            #[inline(always)]
            pub fn bounds(&self, index: usize) -> (usize, usize) {
                let stride = self.stride.copy_as();
                let length = self.length.copy_as();
                let index = index as u64;
                let lower = if index == 0 { 0 } else {
                    let index = index - 1;
                    if index < length { (index+1) * stride } else { self.bounds[(index - length) as usize] }
                } as usize;
                let upper = if index < length { (index+1) * stride } else { self.bounds[(index - length) as usize] } as usize;
                (lower, upper)
            }
        }
        impl<BC: Len, CC: CopyAs<u64>> Strides<BC, CC> {
            #[inline(always)] pub fn strided(&self) -> Option<u64> {
                if self.bounds.is_empty() {
                    Some(self.stride.copy_as())
                }
                else { None }
            }
        }
    }

    #[cfg(test)]
    mod test {
        #[test]
        fn round_trip() {

            use crate::common::{Index, Push, Len};
            use crate::{Borrow, Vecs};
            use crate::primitive::offsets::{Strides, Fixeds};

            let mut cols = Vecs::<Vec::<i32>, Strides>::default();
            for i in 0 .. 100 {
                cols.push(&[1i32, 2, i]);
            }

            let cols = Vecs {
                bounds: TryInto::<Fixeds<3>>::try_into(cols.bounds).unwrap(),
                values: cols.values,
            };

            assert_eq!(cols.borrow().len(), 100);
            for i in 0 .. 100 {
                assert_eq!(cols.borrow().get(i).len(), 3);
            }

            let mut cols = Vecs {
                bounds: Strides::new(3, cols.bounds.count),
                values: cols.values
            };

            cols.push(&[0, 0]);
            assert!(TryInto::<Fixeds<3>>::try_into(cols.bounds).is_err());
        }
    }
}

pub use empty::Empties;
/// A columnar store for `()`.
mod empty {

    use crate::common::index::CopyAs;
    use crate::{Clear, Columnar, Container, Len, IndexMut, Index, Push, HeapSize, Borrow};

    #[derive(Copy, Clone, Debug, Default)]
    pub struct Empties<CC = u64> { pub count: CC, pub empty: () }

    impl Columnar for () {
        #[inline(always)]
        fn into_owned<'a>(_other: crate::Ref<'a, Self>) -> Self { }
        type Container = Empties;
    }

    impl Borrow for Empties {
        type Ref<'a> = ();
        type Borrowed<'a> = Empties<&'a u64>;
        #[inline(always)]
        fn borrow<'a>(&'a self) -> Self::Borrowed<'a> { Empties { count: &self.count, empty: () } }
        #[inline(always)]
        fn reborrow<'b, 'a: 'b>(thing: Self::Borrowed<'a>) -> Self::Borrowed<'b> where Self: 'a {
            Empties { count: thing.count, empty: () }
        }
        #[inline(always)]
        fn reborrow_ref<'b, 'a: 'b>(thing: Self::Ref<'a>) -> Self::Ref<'b> where Self: 'a { thing }
    }

    impl Container for Empties {
        #[inline(always)]
        fn extend_from_self(&mut self, _other: Self::Borrowed<'_>, range: std::ops::Range<usize>) {
            self.count += range.len() as u64;
        }

        fn reserve_for<'a, I>(&mut self, _selves: I) where Self: 'a, I: Iterator<Item = Self::Borrowed<'a>> + Clone { }
    }

    impl<CC: CopyAs<u64>> Len for Empties<CC> {
        #[inline(always)] fn len(&self) -> usize { self.count.copy_as() as usize }
    }
    impl<CC> IndexMut for Empties<CC> {
        type IndexMut<'a> = &'a mut () where CC: 'a;
        // TODO: panic if out of bounds?
        #[inline(always)] fn get_mut(&mut self, _index: usize) -> Self::IndexMut<'_> { &mut self.empty }
    }
    impl<CC> Index for Empties<CC> {
        type Ref = ();
        #[inline(always)]
        fn get(&self, _index: usize) -> Self::Ref { }
    }
    impl<'a, CC> Index for &'a Empties<CC> {
        type Ref = &'a ();
        #[inline(always)]
        fn get(&self, _index: usize) -> Self::Ref { &() }
    }
    impl Push<()> for Empties {
        // TODO: check for overflow?
        #[inline(always)]
        fn push(&mut self, _item: ()) { self.count += 1; }
        #[inline(always)]
        fn extend(&mut self, iter: impl IntoIterator<Item=()>) {
            self.count += iter.into_iter().count() as u64;
        }
    }
    impl<'a> Push<&'a ()> for Empties {
        // TODO: check for overflow?
        #[inline(always)]
        fn push(&mut self, _item: &()) { self.count += 1; }
        #[inline(always)]
        fn extend(&mut self, iter: impl IntoIterator<Item=&'a ()>) {
            self.count += iter.into_iter().count() as u64;
        }
    }

    impl HeapSize for Empties {
        #[inline(always)]
        fn heap_size(&self) -> (usize, usize) { (0, 0) }
    }
    impl Clear for Empties {
        #[inline(always)]
        fn clear(&mut self) { self.count = 0; }
    }

    impl<'a> crate::AsBytes<'a> for crate::primitive::Empties<&'a u64> {
        #[inline(always)]
        fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
            std::iter::once((8, bytemuck::cast_slice(std::slice::from_ref(self.count))))
        }
    }
    impl<'a> crate::FromBytes<'a> for crate::primitive::Empties<&'a u64> {
        #[inline(always)]
        fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
            Self { count: &bytemuck::try_cast_slice(bytes.next().expect("Iterator exhausted prematurely")).unwrap()[0], empty: () }
        }
    }
}

pub use boolean::Bools;
/// A columnar store for `bool`.
mod boolean {

    use crate::common::index::CopyAs;
    use crate::{Container, Clear, Len, Index, IndexAs, Push, HeapSize, Borrow};

    /// A store for maintaining `Vec<bool>`.
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[derive(Copy, Clone, Debug, Default, PartialEq)]
    pub struct Bools<VC = Vec<u64>, WC = u64> {
        /// The bundles of bits that form complete `u64` values.
        pub values: VC,
        /// The work-in-progress bits that are not yet complete.
        pub last_word: WC,
        /// The number of set bits in `bits.last()`.
        pub last_bits: WC,
    }

    impl crate::Columnar for bool {
        #[inline(always)]
        fn into_owned<'a>(other: crate::Ref<'a, Self>) -> Self { other }
        type Container = Bools;
    }

    impl<VC: crate::common::BorrowIndexAs<u64>> Borrow for Bools<VC> {
        type Ref<'a> = bool;
        type Borrowed<'a> = Bools<VC::Borrowed<'a>, &'a u64> where VC: 'a;
        #[inline(always)]
        fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
            Bools {
                values: self.values.borrow(),
                last_word: &self.last_word,
                last_bits: &self.last_bits,
            }
        }
        #[inline(always)]
        fn reborrow<'b, 'a: 'b>(thing: Self::Borrowed<'a>) -> Self::Borrowed<'b> where VC: 'a {
            Bools {
                values: VC::reborrow(thing.values),
                last_word: thing.last_word,
                last_bits: thing.last_bits,
            }
        }
        #[inline(always)]
        fn reborrow_ref<'b, 'a: 'b>(thing: Self::Ref<'a>) -> Self::Ref<'b> where Self: 'a { thing }
    }

    impl<VC: crate::common::PushIndexAs<u64>> Container for Bools<VC> {
        // TODO: There is probably a smart way to implement `extend_from_slice`, but it isn't trivial due to alignment.

        fn reserve_for<'a, I>(&mut self, selves: I) where Self: 'a, I: Iterator<Item = Self::Borrowed<'a>> + Clone {
            self.values.reserve_for(selves.map(|x| x.values))
        }
    }

    impl<'a, VC: crate::AsBytes<'a>> crate::AsBytes<'a> for crate::primitive::Bools<VC, &'a u64> {
        #[inline(always)]
        fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
            let iter = self.values.as_bytes();
            let iter = crate::chain_one(iter, (std::mem::align_of::<u64>() as u64, bytemuck::cast_slice(std::slice::from_ref(self.last_word))));
            crate::chain_one(iter, (std::mem::align_of::<u64>() as u64, bytemuck::cast_slice(std::slice::from_ref(self.last_bits))))
        }
    }

    impl<'a, VC: crate::FromBytes<'a>> crate::FromBytes<'a> for crate::primitive::Bools<VC, &'a u64> {
        #[inline(always)]
        fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
            let values = crate::FromBytes::from_bytes(bytes);
            let last_word = &bytemuck::try_cast_slice(bytes.next().expect("Iterator exhausted prematurely")).unwrap()[0];
            let last_bits = &bytemuck::try_cast_slice(bytes.next().expect("Iterator exhausted prematurely")).unwrap()[0];
            Self { values, last_word, last_bits }
        }
    }

    impl<VC: Len, WC: CopyAs<u64>> Len for Bools<VC, WC> {
        #[inline(always)] fn len(&self) -> usize { self.values.len() * 64 + (self.last_bits.copy_as() as usize) }
    }

    impl<VC: Len + IndexAs<u64>, WC: CopyAs<u64>> Index for Bools<VC, WC> {
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

    impl<VC: Len + IndexAs<u64>, WC: CopyAs<u64>> Index for &Bools<VC, WC> {
        type Ref = bool;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref {
            (*self).get(index)
        }
    }

    impl<VC: for<'a> Push<&'a u64>> Push<bool> for Bools<VC> {
        #[inline]
        fn push(&mut self, bit: bool) {
            self.last_word |= (bit as u64) << self.last_bits;
            self.last_bits += 1;
            // If we have a fully formed word, commit it to `self.values`.
            if self.last_bits == 64 {
                self.values.push(&self.last_word);
                self.last_word = 0;
                self.last_bits = 0;
            }
        }
    }
    impl<'a, VC: for<'b> Push<&'b u64>> Push<&'a bool> for Bools<VC> {
        #[inline(always)]
        fn push(&mut self, bit: &'a bool) {
            self.push(*bit)
        }
    }


    impl<VC: Clear> Clear for Bools<VC> {
        #[inline(always)]
        fn clear(&mut self) {
            self.values.clear();
            self.last_word = 0;
            self.last_bits = 0;
        }
    }

    impl<VC: HeapSize> HeapSize for Bools<VC> {
        #[inline(always)]
        fn heap_size(&self) -> (usize, usize) {
            self.values.heap_size()
        }
    }
}

pub use duration::Durations;
/// A columnar store for `std::time::Duration`.
mod duration {

    use std::time::Duration;
    use crate::{Container, Len, Index, IndexAs, Push, Clear, HeapSize, Borrow};

    // `std::time::Duration` is equivalent to `(u64, u32)`, corresponding to seconds and nanoseconds.
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[derive(Copy, Clone, Debug, Default, PartialEq)]
    pub struct Durations<SC = Vec<u64>, NC = Vec<u32>> {
        pub seconds: SC,
        pub nanoseconds: NC,
    }

    impl crate::Columnar for Duration {
        #[inline(always)]
        fn into_owned<'a>(other: crate::Ref<'a, Self>) -> Self { other }
        type Container = Durations;
    }

    impl<SC: crate::common::BorrowIndexAs<u64>, NC: crate::common::BorrowIndexAs<u32>> Borrow for Durations<SC, NC> {
        type Ref<'a> = Duration;
        type Borrowed<'a> = Durations<SC::Borrowed<'a>, NC::Borrowed<'a>> where SC: 'a, NC: 'a;
        #[inline(always)]
        fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
            Durations {
                seconds: self.seconds.borrow(),
                nanoseconds: self.nanoseconds.borrow(),
            }
        }
        #[inline(always)]
        fn reborrow<'b, 'a: 'b>(thing: Self::Borrowed<'a>) -> Self::Borrowed<'b> where SC: 'a, NC: 'a {
            Durations {
                seconds: SC::reborrow(thing.seconds),
                nanoseconds: NC::reborrow(thing.nanoseconds),
            }
        }
        #[inline(always)]
        fn reborrow_ref<'b, 'a: 'b>(thing: Self::Ref<'a>) -> Self::Ref<'b> where Self: 'a { thing }
    }

    impl<SC: crate::common::PushIndexAs<u64>, NC: crate::common::PushIndexAs<u32>> Container for Durations<SC, NC> {
        #[inline(always)]
        fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: std::ops::Range<usize>) {
            self.seconds.extend_from_self(other.seconds, range.clone());
            self.nanoseconds.extend_from_self(other.nanoseconds, range);
        }

        fn reserve_for<'a, I>(&mut self, selves: I) where Self: 'a, I: Iterator<Item = Self::Borrowed<'a>> + Clone {
            self.seconds.reserve_for(selves.clone().map(|x| x.seconds));
            self.nanoseconds.reserve_for(selves.map(|x| x.nanoseconds));
        }
    }

    impl<'a, SC: crate::AsBytes<'a>, NC: crate::AsBytes<'a>> crate::AsBytes<'a> for crate::primitive::Durations<SC, NC> {
        #[inline(always)]
        fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
            crate::chain(self.seconds.as_bytes(), self.nanoseconds.as_bytes())
        }
    }
    impl<'a, SC: crate::FromBytes<'a>, NC: crate::FromBytes<'a>> crate::FromBytes<'a> for crate::primitive::Durations<SC, NC> {
        #[inline(always)]
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

    impl<SC: for<'a> Push<&'a u64>, NC: for<'a> Push<&'a u32>> Push<std::time::Duration> for Durations<SC, NC> {
        #[inline]
        fn push(&mut self, item: std::time::Duration) {
            self.seconds.push(&item.as_secs());
            self.nanoseconds.push(&item.subsec_nanos());
        }
    }
    impl<'a, SC: for<'b> Push<&'b u64>, NC: for<'b> Push<&'b u32>> Push<&'a std::time::Duration> for Durations<SC, NC> {
        #[inline]
        fn push(&mut self, item: &'a std::time::Duration) {
            self.push(*item)
        }
    }
    impl<'a, SC: Push<&'a u64>, NC: Push<&'a u32>> Push<(&'a u64, &'a u32)> for Durations<SC, NC> {
        #[inline]
        fn push(&mut self, item: (&'a u64, &'a u32)) {
            self.seconds.push(item.0);
            self.nanoseconds.push(item.1);
        }
    }

    impl<SC: Clear, NC: Clear> Clear for Durations<SC, NC> {
        #[inline(always)]
        fn clear(&mut self) {
            self.seconds.clear();
            self.nanoseconds.clear();
        }
    }

    impl<SC: HeapSize, NC: HeapSize> HeapSize for Durations<SC, NC> {
        #[inline(always)]
        fn heap_size(&self) -> (usize, usize) {
            let (l0, c0) = self.seconds.heap_size();
            let (l1, c1) = self.nanoseconds.heap_size();
            (l0 + l1, c0 + c1)
        }
    }
}

