//! Containers that can store either values, or offsets to prior values.
//!
//! This has the potential to be more efficient than a list of `T` when many values repeat in
//! close proximity. Values must be equatable, and the degree of lookback can be configured.

use crate::{Options, Results, Push, Index, Len, IndexAs, Borrow, Container, Clear};

/// A container that encodes repeated values with a `None` variant, at the cost of extra bits for every record.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Copy, Clone, Debug, Default, PartialEq)]
pub struct Repeats<TC, CC = Vec<u64>, VC = Vec<u64>, WC = [u64; 2], const N: u8 = 255> {
    /// Some(x) encodes a value, and None indicates the prior `x` value.
    pub inner: Options<TC, CC, VC, WC>,
}

impl<T: PartialEq, TC: Push<T> + Len, const N: u8> Push<T> for Repeats<TC, Vec<u64>, Vec<u64>, [u64; 2], N>
where
    for<'a> &'a TC: Index,
    for<'a> <&'a TC as Index>::Ref : PartialEq<T>,
{
    #[inline]
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

impl<TC, CC, VC: Len, WC: IndexAs<u64>, const N: u8> Len for Repeats<TC, CC, VC, WC, N> {
    #[inline(always)] fn len(&self) -> usize { self.inner.len() }
}

impl<TC: Index, CC: IndexAs<u64> + Len, VC: IndexAs<u64> + Len, WC: IndexAs<u64>, const N: u8> Index for Repeats<TC, CC, VC, WC, N> {
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

impl<TC: Clear, const N: u8> Clear for Repeats<TC, Vec<u64>, Vec<u64>, [u64; 2], N> {
    #[inline]
    fn clear(&mut self) {
        self.inner.clear();
    }
}

impl<TC: Borrow, const N: u8> Borrow for Repeats<TC, Vec<u64>, Vec<u64>, [u64; 2], N> {
    type Ref<'a> = TC::Ref<'a> where TC: 'a;
    type Borrowed<'a> = Repeats<TC::Borrowed<'a>, &'a [u64], &'a [u64], &'a [u64], N> where TC: 'a;
    #[inline]
    fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
        Repeats { inner: self.inner.borrow() }
    }
    #[inline(always)]
    fn reborrow<'b, 'a: 'b>(thing: Self::Borrowed<'a>) -> Self::Borrowed<'b> where TC: 'a {
        Repeats { inner: Options::<TC>::reborrow(thing.inner) }
    }
    #[inline(always)]
    fn reborrow_ref<'b, 'a: 'b>(thing: Self::Ref<'a>) -> Self::Ref<'b> where Self: 'a {
        TC::reborrow_ref(thing)
    }
}

impl<TC: Container, const N: u8> Container for Repeats<TC, Vec<u64>, Vec<u64>, [u64; 2], N>
where
    for<'a> TC::Ref<'a>: PartialEq,
    for<'a> &'a TC: Index,
    for<'a, 'b> <&'a TC as Index>::Ref: PartialEq<TC::Ref<'b>>,
{
    #[inline(always)]
    fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: std::ops::Range<usize>) {
        self.inner.extend_from_self(other.inner, range)
    }
    fn reserve_for<'a, I>(&mut self, selves: I)
    where
        Self: 'a,
        I: Iterator<Item = Self::Borrowed<'a>> + Clone,
    {
        self.inner.reserve_for(selves.map(|x| x.inner));
    }
}

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Lookbacks<TC, VC = Vec<u8>, const N: u8 = 255> {
    /// Ok(x) encodes a value, and Err(y) indicates a value `y` back.
    pub inner: Results<TC, VC>,
}

impl<T: PartialEq, TC: Push<T> + Len, VC: Push<u8>, const N: u8> Push<T> for Lookbacks<TC, VC, N>
where
    for<'a> &'a TC: Index,
    for<'a> <&'a TC as Index>::Ref : PartialEq<T>,
{
    #[inline]
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

