//! Containers that can store either values, or offsets to prior values.
//!
//! This has the potential to be more efficient than a list of `T` when many values repeat in
//! close proximity. Values must be equatable, and the degree of lookback can be configured.

use crate::{Options, Results, Push, Index, Len, HeapSize};

/// A container that encodes repeated values with a `None` variant, at the cost of extra bits for every record.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Repeats<TC, const N: u8 = 255> {
    /// Some(x) encodes a value, and None indicates the prior `x` value.
    pub inner: Options<TC>,
}

impl<T: PartialEq, TC: Push<T> + Len, const N: u8> Push<T> for Repeats<TC, N>
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

impl<TC: HeapSize, VC: HeapSize, const N: u8> HeapSize for Lookbacks<TC, VC, N> {
    fn heap_size(&self) -> (usize, usize) {
        self.inner.heap_size()
    }
}
