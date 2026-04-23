use alloc::{vec::Vec, string::String};
use super::{Clear, Columnar, Container, Len, IndexMut, Index, IndexAs, Push, Slice, Borrow};

/// A stand-in for `Vec<Vec<T>>` for complex `T`.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Default, Copy, Clone, PartialEq)]
pub struct Vecs<TC, BC = Vec<u64>> {
    pub bounds: BC,
    pub values: TC,
}

impl<T: Columnar> Columnar for Vec<T> {
    #[inline(always)]
    fn copy_from<'a>(&mut self, other: crate::Ref<'a, Self>) {
        self.truncate(other.len());
        let mut other_iter = other.into_iter();
        for (s, o) in self.iter_mut().zip(&mut other_iter) {
            T::copy_from(s, o);
        }
        for o in other_iter {
            self.push(T::into_owned(o));
        }
    }
    #[inline(always)]
    fn into_owned<'a>(other: crate::Ref<'a, Self>) -> Self {
        other.into_iter().map(|x| T::into_owned(x)).collect()
    }
    type Container = Vecs<T::Container>;
}

impl<T: Columnar, const N: usize> Columnar for [T; N] {
    #[inline(always)]
    fn copy_from<'a>(&mut self, other: crate::Ref<'a, Self>) {
        for (s, o) in self.iter_mut().zip(other.into_iter()) {
            T::copy_from(s, o);
        }
    }
    #[inline(always)]
    fn into_owned<'a>(other: crate::Ref<'a, Self>) -> Self {
        let vec: Vec<_> = other.into_iter().map(|x| T::into_owned(x)).collect();
        match vec.try_into() {
            Ok(array) => array,
            Err(_) => panic!("wrong length"),
        }
    }
    type Container = Vecs<T::Container>;
}

impl<T: Columnar, const N: usize> Columnar for smallvec::SmallVec<[T; N]> {
    #[inline(always)]
    fn copy_from<'a>(&mut self, other: crate::Ref<'a, Self>) {
        self.truncate(other.len());
        let mut other_iter = other.into_iter();
        for (s, o) in self.iter_mut().zip(&mut other_iter) {
            T::copy_from(s, o);
        }
        for o in other_iter {
            self.push(T::into_owned(o));
        }
    }
    #[inline(always)]
    fn into_owned<'a>(other: crate::Ref<'a, Self>) -> Self {
        other.into_iter().map(|x| T::into_owned(x)).collect()
    }
    type Container = Vecs<T::Container>;
}

impl<BC: crate::common::BorrowIndexAs<u64>, TC: Container> Borrow for Vecs<TC, BC> {
    type Ref<'a> = Slice<TC::Borrowed<'a>> where TC: 'a;
    type Borrowed<'a> = Vecs<TC::Borrowed<'a>, BC::Borrowed<'a>> where BC: 'a, TC: 'a;
    #[inline(always)]
    fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
        Vecs {
            bounds: self.bounds.borrow(),
            values: self.values.borrow(),
        }
    }
    #[inline(always)]
    fn reborrow<'b, 'a: 'b>(thing: Self::Borrowed<'a>) -> Self::Borrowed<'b> where BC: 'a, TC: 'a {
        Vecs {
            bounds: BC::reborrow(thing.bounds),
            values: TC::reborrow(thing.values),
        }
    }
    #[inline(always)]
    fn reborrow_ref<'b, 'a: 'b>(thing: Self::Ref<'a>) -> Self::Ref<'b> where Self: 'a {
        thing.map(|x| TC::reborrow(x))
    }
}

impl<BC: crate::common::PushIndexAs<u64>, TC: Container> Container for Vecs<TC, BC> {
    #[inline(always)]
    fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: core::ops::Range<usize>) {
        if !range.is_empty() {
            // Imported bounds will be relative to this starting offset.
            let values_len = self.values.len() as u64;

            // Push all bytes that we can, all at once.
            let other_lower = if range.start == 0 { 0 } else { other.bounds.index_as(range.start-1) };
            let other_upper = other.bounds.index_as(range.end-1);
            self.values.extend_from_self(other.values, other_lower as usize .. other_upper as usize);

            // Each bound needs to be shifted by `values_len - other_lower`.
            if values_len == other_lower {
                self.bounds.extend_from_self(other.bounds, range);
            }
            else {
                for index in range {
                    let shifted = other.bounds.index_as(index) - other_lower + values_len;
                    self.bounds.push(&shifted)
                }
            }
        }
    }

    fn reserve_for<'a, I>(&mut self, selves: I) where Self: 'a, I: Iterator<Item = Self::Borrowed<'a>> + Clone {
        self.bounds.reserve_for(selves.clone().map(|x| x.bounds));
        self.values.reserve_for(selves.map(|x| x.values));
    }
}

impl<'a, TC: crate::AsBytes<'a>, BC: crate::AsBytes<'a>> crate::AsBytes<'a> for Vecs<TC, BC> {
    const SLICE_COUNT: usize = BC::SLICE_COUNT + TC::SLICE_COUNT;
    #[inline]
    fn get_byte_slice(&self, index: usize) -> (u64, &'a [u8]) {
        debug_assert!(index < Self::SLICE_COUNT);
        if index < BC::SLICE_COUNT {
            self.bounds.get_byte_slice(index)
        } else {
            self.values.get_byte_slice(index - BC::SLICE_COUNT)
        }
    }
}
impl<'a, TC: crate::FromBytes<'a>, BC: crate::FromBytes<'a>> crate::FromBytes<'a> for Vecs<TC, BC> {
    const SLICE_COUNT: usize = BC::SLICE_COUNT + TC::SLICE_COUNT;
    #[inline(always)]
    fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
        Self {
            bounds: crate::FromBytes::from_bytes(bytes),
            values: crate::FromBytes::from_bytes(bytes),
        }
    }
    #[inline(always)]
    fn from_store(store: &crate::bytes::indexed::DecodedStore<'a>, offset: &mut usize) -> Self {
        Self {
            bounds: BC::from_store(store, offset),
            values: TC::from_store(store, offset),
        }
    }
    fn element_sizes(sizes: &mut Vec<usize>) -> Result<(), String> {
        BC::element_sizes(sizes)?;
        TC::element_sizes(sizes)?;
        Ok(())
    }
}

impl<TC: Len> Vecs<TC> {
    #[inline]
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
impl<TC: Copy, BC: Len+IndexAs<u64>> crate::Sequence for Vecs<TC, BC>
where
    Self: Copy,
{
    type Ref = <Self as Index>::Ref;
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
impl<'a, TC, BC: Len+IndexAs<u64>> Index for &'a Vecs<TC, BC> {
    type Ref = Slice<&'a TC>;
    #[inline(always)]
    fn get(&self, index: usize) -> Self::Ref {
        let lower = if index == 0 { 0 } else { self.bounds.index_as(index - 1) };
        let upper = self.bounds.index_as(index);
        Slice::new(lower, upper, &self.values)
    }
}
impl<'a, TC, BC: Len+IndexAs<u64>> crate::Sequence for &'a Vecs<TC, BC> {
    type Ref = <Self as Index>::Ref;
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
impl<TC, BC: Len+IndexAs<u64>> IndexMut for Vecs<TC, BC> {
    type IndexMut<'a> = Slice<&'a mut TC> where TC: 'a, BC: 'a;

    #[inline(always)]
    fn get_mut(&mut self, index: usize) -> Self::IndexMut<'_> {
        let lower = if index == 0 { 0 } else { self.bounds.index_as(index - 1) };
        let upper = self.bounds.index_as(index);
        Slice::new(lower, upper, &mut self.values)
    }
}

impl<'a, TC: Container, BC: for<'b> Push<&'b u64>> Push<Slice<TC::Borrowed<'a>>> for Vecs<TC, BC> {
    #[inline]
    fn push(&mut self, item: Slice<TC::Borrowed<'a>>) {
        self.values.extend_from_self(item.slice, item.lower .. item.upper);
        self.bounds.push(&(self.values.len() as u64));
    }
}

impl<I: IntoIterator, TC: Push<I::Item> + Len, BC: for<'a> Push<&'a u64>> Push<I> for Vecs<TC, BC> {
    #[inline]
    fn push(&mut self, item: I) {
        self.values.extend(item);
        self.bounds.push(&(self.values.len() as u64));
    }
}

impl<TC: Clear, BC: Clear> Clear for Vecs<TC, BC> {
    #[inline(always)]
    fn clear(&mut self) {
        self.bounds.clear();
        self.values.clear();
    }
}


