//! Types that prefer to be represented by `Vec<T>`.
use alloc::{vec::Vec, string::String};

use core::num::Wrapping;

/// An implementation of opinions for types that want to use `Vec<T>`.
macro_rules! implement_columnable {
    ($($index_type:ty),*) => { $(
        impl crate::Columnar for $index_type {
            #[inline(always)]
            fn into_owned<'a>(other: crate::Ref<'a, Self>) -> Self { *other }

            type Container = Vec<$index_type>;
        }

        impl<'a> crate::AsBytes<'a> for &'a [$index_type] {
            const SLICE_COUNT: usize = 1;
            #[inline]
            fn get_byte_slice(&self, index: usize) -> (u64, &'a [u8]) {
                debug_assert!(index < Self::SLICE_COUNT);
                (core::mem::align_of::<$index_type>() as u64, bytemuck::cast_slice(&self[..]))
            }
        }
        impl<'a> crate::FromBytes<'a> for &'a [$index_type] {
            const SLICE_COUNT: usize = 1;
            #[inline(always)]
            fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
                // We use `unwrap()` here in order to panic with the `bytemuck` error, which may be informative.
                bytemuck::try_cast_slice(bytes.next().expect("Iterator exhausted prematurely")).unwrap()
            }
            #[inline(always)]
            fn from_store(store: &crate::bytes::indexed::DecodedStore<'a>, offset: &mut usize) -> Self {
                let (w, tail) = store.get(*offset);
                *offset += 1;
                let all: &[$index_type] = bytemuck::cast_slice(w);
                let trim = ((8 - tail as usize) % 8) / core::mem::size_of::<$index_type>();
                debug_assert!(trim <= all.len(), "from_store: trim {trim} exceeds slice length {}", all.len());
                all.get(..all.len().wrapping_sub(trim)).unwrap_or(&[])
            }
            fn element_sizes(sizes: &mut Vec<usize>) -> Result<(), String> {
                sizes.push(core::mem::size_of::<$index_type>());
                Ok(())
            }
        }
        impl<'a, const N: usize> crate::AsBytes<'a> for &'a [[$index_type; N]] {
            const SLICE_COUNT: usize = 1;
            #[inline]
            fn get_byte_slice(&self, index: usize) -> (u64, &'a [u8]) {
                debug_assert!(index < Self::SLICE_COUNT);
                (core::mem::align_of::<$index_type>() as u64, bytemuck::cast_slice(&self[..]))
            }
        }
        impl<'a, const N: usize> crate::FromBytes<'a> for &'a [[$index_type; N]] {
            const SLICE_COUNT: usize = 1;
            #[inline(always)]
            fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
                // We use `unwrap()` here in order to panic with the `bytemuck` error, which may be informative.
                bytemuck::try_cast_slice(bytes.next().expect("Iterator exhausted prematurely")).unwrap()
            }
            #[inline(always)]
            fn from_store(store: &crate::bytes::indexed::DecodedStore<'a>, offset: &mut usize) -> Self {
                let (w, tail) = store.get(*offset);
                *offset += 1;
                let all: &[[$index_type; N]] = bytemuck::cast_slice(w);
                let trim = ((8 - tail as usize) % 8) / (core::mem::size_of::<$index_type>() * N);
                debug_assert!(trim <= all.len(), "from_store: trim {trim} exceeds slice length {}", all.len());
                all.get(..all.len().wrapping_sub(trim)).unwrap_or(&[])
            }
            fn element_sizes(sizes: &mut Vec<usize>) -> Result<(), String> {
                sizes.push(core::mem::size_of::<$index_type>() * N);
                Ok(())
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
        fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: core::ops::Range<usize>) {
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
    impl<CV: IndexAs<u64> + Len> crate::Sequence for Usizes<CV>
    where
        Self: Copy,
    {
        type Ref = <Self as crate::Index>::Ref;
        type Iter = crate::common::IterOwn<Self>;
        #[inline(always)]
        fn seq_iter(self) -> Self::Iter {
            let len = crate::Len::len(&self);
            crate::common::IterOwn::with_range(self, 0..len)
        }
        #[inline(always)]
        fn seq_iter_range(self, range: core::ops::Range<usize>) -> Self::Iter {
            crate::common::IterOwn::with_range(self, range)
        }
    }
    impl<CV: IndexAs<u64>> Index for &Usizes<CV> {
        type Ref = usize;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref { self.values.index_as(index).try_into().expect("Usizes values should fit in `usize`") }
    }
    impl<'a, CV: IndexAs<u64> + Len> crate::Sequence for &'a Usizes<CV> {
        type Ref = <Self as crate::Index>::Ref;
        type Iter = crate::common::IterOwn<Self>;
        #[inline(always)]
        fn seq_iter(self) -> Self::Iter {
            let len = crate::Len::len(&self);
            crate::common::IterOwn::with_range(self, 0..len)
        }
        #[inline(always)]
        fn seq_iter_range(self, range: core::ops::Range<usize>) -> Self::Iter {
            crate::common::IterOwn::with_range(self, range)
        }
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

    impl<'a, CV: crate::AsBytes<'a>> crate::AsBytes<'a> for crate::primitive::Usizes<CV> {
        const SLICE_COUNT: usize = CV::SLICE_COUNT;
        #[inline]
        fn get_byte_slice(&self, index: usize) -> (u64, &'a [u8]) {
            self.values.get_byte_slice(index)
        }
    }

    impl<'a, CV: crate::FromBytes<'a>> crate::FromBytes<'a> for crate::primitive::Usizes<CV> {
        const SLICE_COUNT: usize = CV::SLICE_COUNT;
        #[inline(always)]
        fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
            Self { values: CV::from_bytes(bytes) }
        }
        #[inline(always)]
        fn from_store(store: &crate::bytes::indexed::DecodedStore<'a>, offset: &mut usize) -> Self {
            Self { values: CV::from_store(store, offset) }
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
        fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: core::ops::Range<usize>) {
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
    impl<CV: IndexAs<i64> + Len> crate::Sequence for Isizes<CV>
    where
        Self: Copy,
    {
        type Ref = <Self as crate::Index>::Ref;
        type Iter = crate::common::IterOwn<Self>;
        #[inline(always)]
        fn seq_iter(self) -> Self::Iter {
            let len = crate::Len::len(&self);
            crate::common::IterOwn::with_range(self, 0..len)
        }
        #[inline(always)]
        fn seq_iter_range(self, range: core::ops::Range<usize>) -> Self::Iter {
            crate::common::IterOwn::with_range(self, range)
        }
    }
    impl<CV: IndexAs<i64>> Index for &Isizes<CV> {
        type Ref = isize;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref { self.values.index_as(index).try_into().expect("Isizes values should fit in `isize`") }
    }
    impl<'a, CV: IndexAs<i64> + Len> crate::Sequence for &'a Isizes<CV> {
        type Ref = <Self as crate::Index>::Ref;
        type Iter = crate::common::IterOwn<Self>;
        #[inline(always)]
        fn seq_iter(self) -> Self::Iter {
            let len = crate::Len::len(&self);
            crate::common::IterOwn::with_range(self, 0..len)
        }
        #[inline(always)]
        fn seq_iter_range(self, range: core::ops::Range<usize>) -> Self::Iter {
            crate::common::IterOwn::with_range(self, range)
        }
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

    impl<'a, CV: crate::AsBytes<'a>> crate::AsBytes<'a> for crate::primitive::Isizes<CV> {
        const SLICE_COUNT: usize = CV::SLICE_COUNT;
        #[inline]
        fn get_byte_slice(&self, index: usize) -> (u64, &'a [u8]) {
            self.values.get_byte_slice(index)
        }
    }

    impl<'a, CV: crate::FromBytes<'a>> crate::FromBytes<'a> for crate::primitive::Isizes<CV> {
        const SLICE_COUNT: usize = CV::SLICE_COUNT;
        #[inline(always)]
        fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
            Self { values: CV::from_bytes(bytes) }
        }
        #[inline(always)]
        fn from_store(store: &crate::bytes::indexed::DecodedStore<'a>, offset: &mut usize) -> Self {
            Self { values: CV::from_store(store, offset) }
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
        fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: core::ops::Range<usize>) {
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
    impl<CV: IndexAs<Encoded> + Len> crate::Sequence for Chars<CV>
    where
        Self: Copy,
    {
        type Ref = <Self as crate::Index>::Ref;
        type Iter = crate::common::IterOwn<Self>;
        #[inline(always)]
        fn seq_iter(self) -> Self::Iter {
            let len = crate::Len::len(&self);
            crate::common::IterOwn::with_range(self, 0..len)
        }
        #[inline(always)]
        fn seq_iter_range(self, range: core::ops::Range<usize>) -> Self::Iter {
            crate::common::IterOwn::with_range(self, range)
        }
    }
    impl<CV: IndexAs<Encoded>> Index for &Chars<CV> {
        type Ref = char;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref { char::from_u32(self.values.index_as(index)).unwrap() }
    }
    impl<'a, CV: IndexAs<Encoded> + Len> crate::Sequence for &'a Chars<CV> {
        type Ref = <Self as crate::Index>::Ref;
        type Iter = crate::common::IterOwn<Self>;
        #[inline(always)]
        fn seq_iter(self) -> Self::Iter {
            let len = crate::Len::len(&self);
            crate::common::IterOwn::with_range(self, 0..len)
        }
        #[inline(always)]
        fn seq_iter_range(self, range: core::ops::Range<usize>) -> Self::Iter {
            crate::common::IterOwn::with_range(self, range)
        }
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

    impl<'a, CV: crate::AsBytes<'a>> crate::AsBytes<'a> for Chars<CV> {
        const SLICE_COUNT: usize = CV::SLICE_COUNT;
        #[inline]
        fn get_byte_slice(&self, index: usize) -> (u64, &'a [u8]) {
            self.values.get_byte_slice(index)
        }
    }

    impl<'a, CV: crate::FromBytes<'a>> crate::FromBytes<'a> for Chars<CV> {
        const SLICE_COUNT: usize = CV::SLICE_COUNT;
        #[inline(always)]
        fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
            Self { values: CV::from_bytes(bytes) }
        }
        #[inline(always)]
        fn from_store(store: &crate::bytes::indexed::DecodedStore<'a>, offset: &mut usize) -> Self {
            Self { values: CV::from_store(store, offset) }
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
        fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: core::ops::Range<usize>) {
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
    impl<CV: IndexAs<Encoded> + Len> crate::Sequence for U128s<CV>
    where
        Self: Copy,
    {
        type Ref = <Self as crate::Index>::Ref;
        type Iter = crate::common::IterOwn<Self>;
        #[inline(always)]
        fn seq_iter(self) -> Self::Iter {
            let len = crate::Len::len(&self);
            crate::common::IterOwn::with_range(self, 0..len)
        }
        #[inline(always)]
        fn seq_iter_range(self, range: core::ops::Range<usize>) -> Self::Iter {
            crate::common::IterOwn::with_range(self, range)
        }
    }
    impl<CV: IndexAs<Encoded>> Index for &U128s<CV> {
        type Ref = u128;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref { u128::from_le_bytes(self.values.index_as(index)) }
    }
    impl<'a, CV: IndexAs<Encoded> + Len> crate::Sequence for &'a U128s<CV> {
        type Ref = <Self as crate::Index>::Ref;
        type Iter = crate::common::IterOwn<Self>;
        #[inline(always)]
        fn seq_iter(self) -> Self::Iter {
            let len = crate::Len::len(&self);
            crate::common::IterOwn::with_range(self, 0..len)
        }
        #[inline(always)]
        fn seq_iter_range(self, range: core::ops::Range<usize>) -> Self::Iter {
            crate::common::IterOwn::with_range(self, range)
        }
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

    impl<'a, CV: crate::AsBytes<'a>> crate::AsBytes<'a> for U128s<CV> {
        const SLICE_COUNT: usize = CV::SLICE_COUNT;
        #[inline]
        fn get_byte_slice(&self, index: usize) -> (u64, &'a [u8]) {
            self.values.get_byte_slice(index)
        }
    }

    impl<'a, CV: crate::FromBytes<'a>> crate::FromBytes<'a> for U128s<CV> {
        const SLICE_COUNT: usize = CV::SLICE_COUNT;
        #[inline(always)]
        fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
            Self { values: CV::from_bytes(bytes) }
        }
        #[inline(always)]
        fn from_store(store: &crate::bytes::indexed::DecodedStore<'a>, offset: &mut usize) -> Self {
            Self { values: CV::from_store(store, offset) }
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
        fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: core::ops::Range<usize>) {
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
    impl<CV: IndexAs<Encoded> + Len> crate::Sequence for I128s<CV>
    where
        Self: Copy,
    {
        type Ref = <Self as crate::Index>::Ref;
        type Iter = crate::common::IterOwn<Self>;
        #[inline(always)]
        fn seq_iter(self) -> Self::Iter {
            let len = crate::Len::len(&self);
            crate::common::IterOwn::with_range(self, 0..len)
        }
        #[inline(always)]
        fn seq_iter_range(self, range: core::ops::Range<usize>) -> Self::Iter {
            crate::common::IterOwn::with_range(self, range)
        }
    }
    impl<CV: IndexAs<Encoded>> Index for &I128s<CV> {
        type Ref = i128;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref { i128::from_le_bytes(self.values.index_as(index)) }
    }
    impl<'a, CV: IndexAs<Encoded> + Len> crate::Sequence for &'a I128s<CV> {
        type Ref = <Self as crate::Index>::Ref;
        type Iter = crate::common::IterOwn<Self>;
        #[inline(always)]
        fn seq_iter(self) -> Self::Iter {
            let len = crate::Len::len(&self);
            crate::common::IterOwn::with_range(self, 0..len)
        }
        #[inline(always)]
        fn seq_iter_range(self, range: core::ops::Range<usize>) -> Self::Iter {
            crate::common::IterOwn::with_range(self, range)
        }
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

    impl<'a, CV: crate::AsBytes<'a>> crate::AsBytes<'a> for I128s<CV> {
        const SLICE_COUNT: usize = CV::SLICE_COUNT;
        #[inline]
        fn get_byte_slice(&self, index: usize) -> (u64, &'a [u8]) {
            self.values.get_byte_slice(index)
        }
    }

    impl<'a, CV: crate::FromBytes<'a>> crate::FromBytes<'a> for I128s<CV> {
        const SLICE_COUNT: usize = CV::SLICE_COUNT;
        #[inline(always)]
        fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
            Self { values: CV::from_bytes(bytes) }
        }
        #[inline(always)]
        fn from_store(store: &crate::bytes::indexed::DecodedStore<'a>, offset: &mut usize) -> Self {
            Self { values: CV::from_store(store, offset) }
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

        use alloc::{vec::Vec, string::String};
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
            fn extend_from_self(&mut self, _other: Self::Borrowed<'_>, range: core::ops::Range<usize>) {
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
        impl<const K: u64, CC: CopyAs<u64>> crate::Sequence for Fixeds<K, CC>
        where
            Self: Copy,
        {
            type Ref = <Self as crate::Index>::Ref;
            type Iter = crate::common::IterOwn<Self>;
            #[inline(always)]
            fn seq_iter(self) -> Self::Iter {
                let len = crate::Len::len(&self);
                crate::common::IterOwn::with_range(self, 0..len)
            }
            #[inline(always)]
            fn seq_iter_range(self, range: core::ops::Range<usize>) -> Self::Iter {
                crate::common::IterOwn::with_range(self, range)
            }
        }
        impl<'a, const K: u64, CC> Index for &'a Fixeds<K, CC> {
            type Ref = u64;
            #[inline(always)]
            fn get(&self, index: usize) -> Self::Ref { (index as u64 + 1) * K }
        }
        impl<'a, const K: u64, CC: CopyAs<u64>> crate::Sequence for &'a Fixeds<K, CC> {
            type Ref = <Self as crate::Index>::Ref;
            type Iter = crate::common::IterOwn<Self>;
            #[inline(always)]
            fn seq_iter(self) -> Self::Iter {
                let len = crate::Len::len(&self);
                crate::common::IterOwn::with_range(self, 0..len)
            }
            #[inline(always)]
            fn seq_iter_range(self, range: core::ops::Range<usize>) -> Self::Iter {
                crate::common::IterOwn::with_range(self, range)
            }
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

        impl<const K: u64> crate::Clear for Fixeds<K> {
            #[inline(always)]
            fn clear(&mut self) { self.count = 0; }
        }

        impl<'a, const K: u64> crate::AsBytes<'a> for Fixeds<K, &'a u64> {
            const SLICE_COUNT: usize = 1;
            #[inline]
            fn get_byte_slice(&self, index: usize) -> (u64, &'a [u8]) {
                debug_assert!(index < Self::SLICE_COUNT);
                (8, bytemuck::cast_slice(core::slice::from_ref(self.count)))
            }
        }
        impl<'a, const K: u64> crate::FromBytes<'a> for Fixeds<K, &'a u64> {
            const SLICE_COUNT: usize = 1;
            #[inline(always)]
            fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
                Self { count: &bytemuck::try_cast_slice(bytes.next().expect("Iterator exhausted prematurely")).unwrap()[0] }
            }
            #[inline(always)]
            fn from_store(store: &crate::bytes::indexed::DecodedStore<'a>, offset: &mut usize) -> Self {
                let (w, _) = store.get(*offset); *offset += 1;
                debug_assert!(!w.is_empty(), "Fixeds::from_store: empty count slice");
                Self { count: w.first().unwrap_or(&0) }
            }
            fn element_sizes(sizes: &mut Vec<usize>) -> Result<(), String> {
                sizes.push(8);
                Ok(())
            }
            fn validate(slices: &[(&[u64], u8)]) -> Result<(), String> {
                if slices.is_empty() || slices[0].0.is_empty() {
                    return Err("Fixeds: count slice must be non-empty".into());
                }
                Ok(())
            }
        }

        use super::Strides;
        impl<const K: u64> core::convert::TryFrom<Strides> for Fixeds<K> {
            type Error = Strides;
            fn try_from(item: Strides) -> Result<Self, Self::Error> {
                if item.strided() == Some(K) { Ok( Self { count: item.head[1] } ) } else { Err(item) }
            }
        }
        impl<'a, const K: u64> core::convert::TryFrom<Strides<&'a [u64], &'a [u64]>> for Fixeds<K, &'a u64> {
            type Error = Strides<&'a [u64], &'a [u64]>;
            fn try_from(item: Strides<&'a [u64], &'a [u64]>) -> Result<Self, Self::Error> {
                if item.strided() == Some(K) { Ok( Self { count: &item.head[1] } ) } else { Err(item) }
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

        use alloc::{vec::Vec, string::String};
        use core::ops::Deref;
        use crate::{Container, Borrow, Index, IndexAs, Len, Push, Clear, AsBytes, FromBytes};

        /// Columnar store for non-decreasing `u64` offsets with stride optimization.
        ///
        /// `head` holds `[stride, length]`: when the first `length` offsets follow a
        /// regular stride pattern (`(i+1) * stride`), they are stored implicitly.
        /// Remaining offsets go into `bounds`. In the owned form `head` is `[u64; 2]`;
        /// in the borrowed form it is `&[u64]` of length 2.
        #[derive(Copy, Clone, Debug, Default)]
        pub struct Strides<BC = Vec<u64>, HC = [u64; 2]> {
            pub head: HC,
            pub bounds: BC,
        }

        impl Borrow for Strides {
            type Ref<'a> = u64;
            type Borrowed<'a> = Strides<&'a [u64], &'a [u64]>;

            #[inline(always)] fn borrow<'a>(&'a self) -> Self::Borrowed<'a> { Strides { head: &self.head, bounds: &self.bounds[..] } }
            #[inline(always)] fn reborrow<'b, 'a: 'b>(item: Self::Borrowed<'a>) -> Self::Borrowed<'b> where Self: 'a {
                Strides { head: item.head, bounds: item.bounds }
            }
            #[inline(always)] fn reborrow_ref<'b, 'a: 'b>(item: Self::Ref<'a>) -> Self::Ref<'b> where Self: 'a { item }
        }

        impl Container for Strides {
            fn reserve_for<'a, I>(&mut self, selves: I) where Self: 'a, I: Iterator<Item = Self::Borrowed<'a>> + Clone {
                self.bounds.reserve_for(selves.map(|x| x.bounds))
            }
        }

        impl<'a> Push<&'a u64> for Strides { #[inline(always)] fn push(&mut self, item: &'a u64) { self.push(*item) } }
        impl Push<u64> for Strides { #[inline(always)] fn push(&mut self, item: u64) { self.push(item) } }
        impl Clear for Strides { #[inline(always)] fn clear(&mut self) { self.clear() } }

        impl<BC: Len, HC: IndexAs<u64>> Len for Strides<BC, HC> {
            #[inline(always)]
            fn len(&self) -> usize { self.head.index_as(1) as usize + self.bounds.len() }
        }
        impl<BC: IndexAs<u64>, HC: IndexAs<u64>> Index for Strides<BC, HC> {
            type Ref = u64;
            #[inline(always)]
            fn get(&self, index: usize) -> Self::Ref {
                let index = index as u64;
                let length = self.head.index_as(1);
                let stride = self.head.index_as(0);
                if index < length { (index+1) * stride } else { self.bounds.index_as((index - length) as usize) }
            }
        }
        impl<BC: IndexAs<u64> + Len, HC: IndexAs<u64>> crate::Sequence for Strides<BC, HC>
        where
            Self: Copy,
        {
            type Ref = <Self as crate::Index>::Ref;
            type Iter = crate::common::IterOwn<Self>;
            #[inline(always)]
            fn seq_iter(self) -> Self::Iter {
                let len = crate::Len::len(&self);
                crate::common::IterOwn::with_range(self, 0..len)
            }
            #[inline(always)]
            fn seq_iter_range(self, range: core::ops::Range<usize>) -> Self::Iter {
                crate::common::IterOwn::with_range(self, range)
            }
        }

        impl<'a, BC: AsBytes<'a>> AsBytes<'a> for Strides<BC, &'a [u64]> {
            const SLICE_COUNT: usize = 1 + BC::SLICE_COUNT;
            #[inline]
            fn get_byte_slice(&self, index: usize) -> (u64, &'a [u8]) {
                debug_assert!(index < Self::SLICE_COUNT);
                if index < 1 {
                    (8u64, bytemuck::cast_slice(self.head))
                } else {
                    self.bounds.get_byte_slice(index - 1)
                }
            }
        }
        impl<'a, BC: FromBytes<'a>> FromBytes<'a> for Strides<BC, &'a [u64]> {
            const SLICE_COUNT: usize = 1 + BC::SLICE_COUNT;
            #[inline(always)]
            fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
                let head: &[u64] = bytemuck::try_cast_slice(bytes.next().expect("Iterator exhausted prematurely")).unwrap();
                let bounds = BC::from_bytes(bytes);
                Self { head, bounds }
            }
            #[inline(always)]
            fn from_store(store: &crate::bytes::indexed::DecodedStore<'a>, offset: &mut usize) -> Self {
                let (head, _) = store.get(*offset); *offset += 1;
                debug_assert!(head.len() >= 2, "Strides::from_store: head slice too short (len {})", head.len());
                let bounds = BC::from_store(store, offset);
                Self { head, bounds }
            }
            fn element_sizes(sizes: &mut Vec<usize>) -> Result<(), String> {
                sizes.push(8); // head: [stride, length]
                BC::element_sizes(sizes)
            }
            fn validate(slices: &[(&[u64], u8)]) -> Result<(), String> {
                if slices.is_empty() || slices[0].0.len() < 2 {
                    return Err("Strides: head slice must have at least 2 elements (stride, length)".into());
                }
                BC::validate(&slices[1..])
            }
        }

        impl Strides {
            pub fn new(stride: u64, length: u64) -> Self {
                Self { head: [stride, length], bounds: Vec::default() }
            }
            #[inline(always)]
            pub fn push(&mut self, item: u64) {
                if self.head[1] == 0 {
                    self.head[0] = item;
                    self.head[1] = 1;
                }
                else if !self.bounds.is_empty() {
                    self.bounds.push(item);
                }
                else if item == self.head[0] * (self.head[1] + 1) {
                    self.head[1] += 1;
                }
                else {
                    self.bounds.push(item);
                }
            }
            /// Removes the last element, if non-empty.
            ///
            /// If empty, will trip a debug assert, but wrap in release.
            #[inline(always)]
            pub fn pop(&mut self) {
                debug_assert!(self.len() > 0);
                if self.bounds.is_empty() { self.head[1] -= 1; }
                else { self.bounds.pop(); }
            }
            #[inline(always)]
            pub fn clear(&mut self) {
                self.head = [0, 0];
                self.bounds.clear();
            }
        }

        impl<BC: Deref<Target=[u64]>, HC: IndexAs<u64>> Strides<BC, HC> {
            #[inline(always)]
            pub fn bounds(&self, index: usize) -> (usize, usize) {
                let stride = self.head.index_as(0);
                let length = self.head.index_as(1);
                let index = index as u64;
                let lower = if index == 0 { 0 } else {
                    let index = index - 1;
                    if index < length { (index+1) * stride } else { self.bounds[(index - length) as usize] }
                } as usize;
                let upper = if index < length { (index+1) * stride } else { self.bounds[(index - length) as usize] } as usize;
                (lower, upper)
            }
        }
        impl<BC: Len, HC: IndexAs<u64>> Strides<BC, HC> {
            #[inline(always)] pub fn strided(&self) -> Option<u64> {
                if self.bounds.is_empty() {
                    Some(self.head.index_as(0))
                }
                else { None }
            }
        }
    }

    #[cfg(test)]
    mod test {
        use alloc::vec::Vec;
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

    use alloc::{vec::Vec, string::String};
    use crate::common::index::CopyAs;
    use crate::{Clear, Columnar, Container, Len, IndexMut, Index, Push, Borrow};

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
        fn extend_from_self(&mut self, _other: Self::Borrowed<'_>, range: core::ops::Range<usize>) {
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
    impl<CC: CopyAs<u64>> crate::Sequence for Empties<CC>
    where
        Self: Copy,
    {
        type Ref = <Self as crate::Index>::Ref;
        type Iter = crate::common::IterOwn<Self>;
        #[inline(always)]
        fn seq_iter(self) -> Self::Iter {
            let len = crate::Len::len(&self);
            crate::common::IterOwn::with_range(self, 0..len)
        }
        #[inline(always)]
        fn seq_iter_range(self, range: core::ops::Range<usize>) -> Self::Iter {
            crate::common::IterOwn::with_range(self, range)
        }
    }
    impl<'a, CC> Index for &'a Empties<CC> {
        type Ref = &'a ();
        #[inline(always)]
        fn get(&self, _index: usize) -> Self::Ref { &() }
    }
    impl<'a, CC: CopyAs<u64>> crate::Sequence for &'a Empties<CC> {
        type Ref = <Self as crate::Index>::Ref;
        type Iter = crate::common::IterOwn<Self>;
        #[inline(always)]
        fn seq_iter(self) -> Self::Iter {
            let len = crate::Len::len(&self);
            crate::common::IterOwn::with_range(self, 0..len)
        }
        #[inline(always)]
        fn seq_iter_range(self, range: core::ops::Range<usize>) -> Self::Iter {
            crate::common::IterOwn::with_range(self, range)
        }
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

    impl Clear for Empties {
        #[inline(always)]
        fn clear(&mut self) { self.count = 0; }
    }

    impl<'a> crate::AsBytes<'a> for crate::primitive::Empties<&'a u64> {
        const SLICE_COUNT: usize = 1;
        #[inline]
        fn get_byte_slice(&self, index: usize) -> (u64, &'a [u8]) {
            debug_assert!(index < Self::SLICE_COUNT);
            (8, bytemuck::cast_slice(core::slice::from_ref(self.count)))
        }
    }
    impl<'a> crate::FromBytes<'a> for crate::primitive::Empties<&'a u64> {
        const SLICE_COUNT: usize = 1;
        #[inline(always)]
        fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
            Self { count: &bytemuck::try_cast_slice(bytes.next().expect("Iterator exhausted prematurely")).unwrap()[0], empty: () }
        }
        #[inline(always)]
        fn from_store(store: &crate::bytes::indexed::DecodedStore<'a>, offset: &mut usize) -> Self {
            let (w, _) = store.get(*offset); *offset += 1;
            debug_assert!(!w.is_empty(), "Empties::from_store: empty count slice");
            Self { count: w.first().unwrap_or(&0), empty: () }
        }
        fn element_sizes(sizes: &mut Vec<usize>) -> Result<(), String> {
            sizes.push(8);
            Ok(())
        }
        fn validate(slices: &[(&[u64], u8)]) -> Result<(), String> {
            if slices.is_empty() || slices[0].0.is_empty() {
                return Err("Empties: count slice must be non-empty".into());
            }
            Ok(())
        }
    }
}

pub use boolean::Bools;
/// A columnar store for `bool`.
mod boolean {

    use alloc::{vec::Vec, string::String};
    use crate::{Container, Clear, Len, Index, IndexAs, Push, Borrow};

    /// A store for maintaining `Vec<bool>`.
    ///
    /// Packed bits are stored in `values` as complete `u64` words. The `tail`
    /// holds `[last_word, last_bits]`: the partial word being filled and the
    /// count of valid bits in it. In the owned form `tail` is `[u64; 2]`;
    /// in the borrowed form it is `&[u64]` of length 2.
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[derive(Copy, Clone, Debug, Default, PartialEq)]
    pub struct Bools<VC = Vec<u64>, TC = [u64; 2]> {
        /// The bundles of bits that form complete `u64` values.
        pub values: VC,
        /// `[last_word, last_bits]`: the partial word and the number of valid bits in it.
        pub tail: TC,
    }

    impl crate::Columnar for bool {
        #[inline(always)]
        fn into_owned<'a>(other: crate::Ref<'a, Self>) -> Self { other }
        type Container = Bools;
    }

    impl<VC: crate::common::BorrowIndexAs<u64>> Borrow for Bools<VC> {
        type Ref<'a> = bool;
        type Borrowed<'a> = Bools<VC::Borrowed<'a>, &'a [u64]> where VC: 'a;
        #[inline(always)]
        fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
            Bools {
                values: self.values.borrow(),
                tail: &self.tail,
            }
        }
        #[inline(always)]
        fn reborrow<'b, 'a: 'b>(thing: Self::Borrowed<'a>) -> Self::Borrowed<'b> where VC: 'a {
            Bools {
                values: VC::reborrow(thing.values),
                tail: thing.tail,
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

    impl<'a, VC: crate::AsBytes<'a>> crate::AsBytes<'a> for crate::primitive::Bools<VC, &'a [u64]> {
        const SLICE_COUNT: usize = VC::SLICE_COUNT + 1;
        #[inline]
        fn get_byte_slice(&self, index: usize) -> (u64, &'a [u8]) {
            debug_assert!(index < Self::SLICE_COUNT);
            if index < VC::SLICE_COUNT {
                self.values.get_byte_slice(index)
            } else {
                (core::mem::align_of::<u64>() as u64, bytemuck::cast_slice(self.tail))
            }
        }
    }

    impl<'a, VC: crate::FromBytes<'a>> crate::FromBytes<'a> for crate::primitive::Bools<VC, &'a [u64]> {
        const SLICE_COUNT: usize = VC::SLICE_COUNT + 1;
        #[inline(always)]
        fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
            let values = crate::FromBytes::from_bytes(bytes);
            let tail: &[u64] = bytemuck::try_cast_slice(bytes.next().expect("Iterator exhausted prematurely")).unwrap();
            Self { values, tail }
        }
        #[inline(always)]
        fn from_store(store: &crate::bytes::indexed::DecodedStore<'a>, offset: &mut usize) -> Self {
            let values = VC::from_store(store, offset);
            let (tail, _) = store.get(*offset); *offset += 1;
            debug_assert!(tail.len() >= 2, "Bools::from_store: tail slice too short (len {})", tail.len());
            Self { values, tail }
        }
        fn element_sizes(sizes: &mut Vec<usize>) -> Result<(), String> {
            VC::element_sizes(sizes)?;
            sizes.push(8); // tail: [last_word, last_bits]
            Ok(())
        }
        fn validate(slices: &[(&[u64], u8)]) -> Result<(), String> {
            if slices.len() < Self::SLICE_COUNT {
                return Err(format!("Bools: expected {} slices but got {}", Self::SLICE_COUNT, slices.len()));
            }
            VC::validate(slices)?;
            let vc = VC::SLICE_COUNT;
            if slices[vc].0.len() < 2 {
                return Err("Bools: tail slice must have at least 2 elements (last_word, last_bits)".into());
            }
            Ok(())
        }
    }

    impl<VC: Len, TC: IndexAs<u64>> Len for Bools<VC, TC> {
        #[inline(always)] fn len(&self) -> usize { self.values.len() * 64 + (self.tail.index_as(1) as usize) }
    }

    impl<VC: Len + IndexAs<u64>, TC: IndexAs<u64>> Index for Bools<VC, TC> {
        type Ref = bool;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref {
            let block = index / 64;
            let word = if block == self.values.len() {
                self.tail.index_as(0)
            } else {
                self.values.index_as(block)
            };
            let bit = index % 64;
            (word >> bit) & 1 == 1
        }
    }
    impl<VC: Len + IndexAs<u64>, TC: IndexAs<u64>> crate::Sequence for Bools<VC, TC>
    where
        Self: Copy,
    {
        type Ref = <Self as crate::Index>::Ref;
        type Iter = crate::common::IterOwn<Self>;
        #[inline(always)]
        fn seq_iter(self) -> Self::Iter {
            let len = crate::Len::len(&self);
            crate::common::IterOwn::with_range(self, 0..len)
        }
        #[inline(always)]
        fn seq_iter_range(self, range: core::ops::Range<usize>) -> Self::Iter {
            crate::common::IterOwn::with_range(self, range)
        }
    }

    impl<VC: Len + IndexAs<u64>, TC: IndexAs<u64>> Index for &Bools<VC, TC> {
        type Ref = bool;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref {
            (*self).get(index)
        }
    }
    impl<'a, VC: Len + IndexAs<u64>, TC: IndexAs<u64>> crate::Sequence for &'a Bools<VC, TC> {
        type Ref = <Self as crate::Index>::Ref;
        type Iter = crate::common::IterOwn<Self>;
        #[inline(always)]
        fn seq_iter(self) -> Self::Iter {
            let len = crate::Len::len(&self);
            crate::common::IterOwn::with_range(self, 0..len)
        }
        #[inline(always)]
        fn seq_iter_range(self, range: core::ops::Range<usize>) -> Self::Iter {
            crate::common::IterOwn::with_range(self, range)
        }
    }

    impl<VC: for<'a> Push<&'a u64>> Push<bool> for Bools<VC> {
        #[inline]
        fn push(&mut self, bit: bool) {
            self.tail[0] |= (bit as u64) << self.tail[1];
            self.tail[1] += 1;
            // If we have a fully formed word, commit it to `self.values`.
            if self.tail[1] == 64 {
                self.values.push(&self.tail[0]);
                self.tail = [0, 0];
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
            self.tail = [0, 0];
        }
    }

}

pub use duration::Durations;
/// A columnar store for `core::time::Duration`.
mod duration {

    use alloc::vec::Vec;
    use core::time::Duration;
    use crate::{Container, Len, Index, IndexAs, Push, Clear, Borrow};

    // `core::time::Duration` is equivalent to `(u64, u32)`, corresponding to seconds and nanoseconds.
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
        fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: core::ops::Range<usize>) {
            self.seconds.extend_from_self(other.seconds, range.clone());
            self.nanoseconds.extend_from_self(other.nanoseconds, range);
        }

        fn reserve_for<'a, I>(&mut self, selves: I) where Self: 'a, I: Iterator<Item = Self::Borrowed<'a>> + Clone {
            self.seconds.reserve_for(selves.clone().map(|x| x.seconds));
            self.nanoseconds.reserve_for(selves.map(|x| x.nanoseconds));
        }
    }

    impl<'a, SC: crate::AsBytes<'a>, NC: crate::AsBytes<'a>> crate::AsBytes<'a> for crate::primitive::Durations<SC, NC> {
        const SLICE_COUNT: usize = SC::SLICE_COUNT + NC::SLICE_COUNT;
        #[inline]
        fn get_byte_slice(&self, index: usize) -> (u64, &'a [u8]) {
            debug_assert!(index < Self::SLICE_COUNT);
            if index < SC::SLICE_COUNT {
                self.seconds.get_byte_slice(index)
            } else {
                self.nanoseconds.get_byte_slice(index - SC::SLICE_COUNT)
            }
        }
    }
    impl<'a, SC: crate::FromBytes<'a>, NC: crate::FromBytes<'a>> crate::FromBytes<'a> for crate::primitive::Durations<SC, NC> {
        const SLICE_COUNT: usize = SC::SLICE_COUNT + NC::SLICE_COUNT;
        #[inline(always)]
        fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
            Self {
                seconds: crate::FromBytes::from_bytes(bytes),
                nanoseconds: crate::FromBytes::from_bytes(bytes),
            }
        }
        #[inline(always)]
        fn from_store(store: &crate::bytes::indexed::DecodedStore<'a>, offset: &mut usize) -> Self {
            Self {
                seconds: SC::from_store(store, offset),
                nanoseconds: NC::from_store(store, offset),
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
    impl<SC: IndexAs<u64> + Len, NC: IndexAs<u32>> crate::Sequence for Durations<SC, NC>
    where
        Self: Copy,
    {
        type Ref = <Self as crate::Index>::Ref;
        type Iter = crate::common::IterOwn<Self>;
        #[inline(always)]
        fn seq_iter(self) -> Self::Iter {
            let len = crate::Len::len(&self);
            crate::common::IterOwn::with_range(self, 0..len)
        }
        #[inline(always)]
        fn seq_iter_range(self, range: core::ops::Range<usize>) -> Self::Iter {
            crate::common::IterOwn::with_range(self, range)
        }
    }
    impl<SC: IndexAs<u64>, NC: IndexAs<u32>> Index for &Durations<SC, NC> {
        type Ref = Duration;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref {
            Duration::new(self.seconds.index_as(index), self.nanoseconds.index_as(index))
        }
    }
    impl<'a, SC: IndexAs<u64> + Len, NC: IndexAs<u32>> crate::Sequence for &'a Durations<SC, NC> {
        type Ref = <Self as crate::Index>::Ref;
        type Iter = crate::common::IterOwn<Self>;
        #[inline(always)]
        fn seq_iter(self) -> Self::Iter {
            let len = crate::Len::len(&self);
            crate::common::IterOwn::with_range(self, 0..len)
        }
        #[inline(always)]
        fn seq_iter_range(self, range: core::ops::Range<usize>) -> Self::Iter {
            crate::common::IterOwn::with_range(self, range)
        }
    }

    impl<SC: for<'a> Push<&'a u64>, NC: for<'a> Push<&'a u32>> Push<core::time::Duration> for Durations<SC, NC> {
        #[inline]
        fn push(&mut self, item: core::time::Duration) {
            self.seconds.push(&item.as_secs());
            self.nanoseconds.push(&item.subsec_nanos());
        }
    }
    impl<'a, SC: for<'b> Push<&'b u64>, NC: for<'b> Push<&'b u32>> Push<&'a core::time::Duration> for Durations<SC, NC> {
        #[inline]
        fn push(&mut self, item: &'a core::time::Duration) {
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

}

