//! Containers that can store either values, or offsets to prior values.
//!
//! This has the potential to be more efficient than a list of `T` when many values repeat in
//! close proximity. Values must be equatable, and the degree of lookback can be configured.

use crate::{Options, Results, Push, Index, Len, Clear, Borrow, Container, IndexAs};

/// A container that encodes repeated values with a `None` variant, at the cost of extra bits for every record.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Copy, Clone, Debug, Default, PartialEq)]
pub struct Repeats<TC, CC=Vec<u64>, VC=Vec<u64>, WC=[u64; 2]> {
    /// Some(x) encodes a value, and None indicates the prior `x` value.
    pub inner: Options<TC, CC, VC, WC>,
}

impl<T: PartialEq, TC: Push<T> + Len> Push<T> for Repeats<TC>
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

impl<TC, CC, VC: Len, WC: IndexAs<u64>> Len for Repeats<TC, CC, VC, WC> {
    #[inline(always)] fn len(&self) -> usize { self.inner.len() }
}

impl<TC: Index, CC: IndexAs<u64> + Len, VC: IndexAs<u64> + Len, WC: IndexAs<u64>> Index for Repeats<TC, CC, VC, WC> {
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

impl<'a, TC> Index for &'a Repeats<TC>
where
    &'a TC: Index,
{
    type Ref = <&'a TC as Index>::Ref;
    #[inline(always)] fn get(&self, index: usize) -> Self::Ref {
        match (&self.inner).get(index) {
            Some(item) => item,
            None => {
                let pos = self.inner.indexes.rank(index) - 1;
                (&self.inner.somes).get(pos)
            },
        }
    }
}

impl<TC: Borrow> Borrow for Repeats<TC> {
    type Ref<'a> = TC::Ref<'a> where TC: 'a;
    type Borrowed<'a> = Repeats<TC::Borrowed<'a>, &'a [u64], &'a [u64], &'a [u64]> where TC: 'a;
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

impl<TC: Container> Container for Repeats<TC>
where
    for<'a> &'a TC: Index,
    for<'a> TC::Ref<'a>: PartialEq,
    for<'a, 'b> <&'a TC as Index>::Ref: PartialEq<TC::Ref<'b>>,
{
    fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: std::ops::Range<usize>) {
        if !range.is_empty() {
            // Push the first element, resolving any `None` to its actual value.
            self.push(other.get(range.start));
            // The remaining elements can be bulk-copied from the inner `Options`,
            // as any `None` now has a preceding `Some` to reference.
            if range.start + 1 < range.end {
                self.inner.extend_from_self(other.inner, range.start + 1 .. range.end);
            }
        }
    }

    fn reserve_for<'a, I>(&mut self, selves: I) where Self: 'a, I: Iterator<Item = Self::Borrowed<'a>> + Clone {
        self.inner.somes.reserve_for(selves.map(|x| x.inner.somes));
    }
}

impl<TC: Clear> Clear for Repeats<TC> {
    fn clear(&mut self) {
        self.inner.clear();
    }
}

impl<'a, TC: crate::AsBytes<'a>, CC: crate::AsBytes<'a>, VC: crate::AsBytes<'a>> crate::AsBytes<'a> for Repeats<TC, CC, VC, &'a [u64]> {
    #[inline(always)]
    fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
        self.inner.as_bytes()
    }
}

impl<'a, TC: crate::FromBytes<'a>, CC: crate::FromBytes<'a>, VC: crate::FromBytes<'a>> crate::FromBytes<'a> for Repeats<TC, CC, VC, &'a [u64]> {
    const SLICE_COUNT: usize = <Options<TC, CC, VC, &'a [u64]>>::SLICE_COUNT;
    #[inline(always)]
    fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
        Self { inner: crate::FromBytes::from_bytes(bytes) }
    }
    #[inline(always)]
    fn from_store(store: &crate::bytes::indexed::DecodedStore<'a>, offset: &mut usize) -> Self {
        Self { inner: crate::FromBytes::from_store(store, offset) }
    }
    fn element_sizes(sizes: &mut Vec<usize>) -> Result<(), String> {
        <Options<TC, CC, VC, &'a [u64]>>::element_sizes(sizes)
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

#[cfg(test)]
mod test {

    use crate::common::{Push, Index, Len, Clear};
    use crate::{Borrow, Container, AsBytes, FromBytes};
    use crate::bytes::stash::Stash;
    use super::Repeats;

    /// Helper to populate a `Repeats<Vec<u64>>` from a slice.
    fn repeats_from(values: &[u64]) -> Repeats<Vec<u64>> {
        let mut repeats: Repeats<Vec<u64>> = Default::default();
        for v in values {
            repeats.push(v);
        }
        repeats
    }

    #[test]
    fn push_and_index() {
        let repeats = repeats_from(&[1, 1, 2, 2, 1]);

        assert_eq!(repeats.len(), 5);
        assert_eq!((&repeats).get(0), 1);
        assert_eq!((&repeats).get(1), 1);
        assert_eq!((&repeats).get(2), 2);
        assert_eq!((&repeats).get(3), 2);
        assert_eq!((&repeats).get(4), 1);

        // Verify compression: only 3 distinct values stored (1, 2, 1).
        assert_eq!(repeats.inner.somes.len(), 3);
    }

    #[test]
    fn borrow_and_index() {
        let mut repeats: Repeats<Vec<u64>> = Default::default();
        for i in 0..50u64 {
            repeats.push(&i);
            repeats.push(&i); // repeat
        }

        assert_eq!(repeats.len(), 100);

        let borrowed = repeats.borrow();
        assert_eq!(borrowed.len(), 100);
        for i in 0..50u64 {
            assert_eq!(*borrowed.get(2 * i as usize), i);
            assert_eq!(*borrowed.get(2 * i as usize + 1), i);
        }
    }

    #[test]
    fn ref_index() {
        let repeats = repeats_from(&[10, 10, 20]);

        assert_eq!((&repeats).get(0), 10u64);
        assert_eq!((&repeats).get(1), 10u64);
        assert_eq!((&repeats).get(2), 20u64);
    }

    #[test]
    fn clear() {
        let mut repeats = repeats_from(&[1, 2]);
        assert_eq!(repeats.len(), 2);

        repeats.clear();
        assert_eq!(repeats.len(), 0);

        repeats.push(&3u64);
        assert_eq!(repeats.len(), 1);
        assert_eq!((&repeats).get(0), 3);
    }

    #[test]
    fn extend_from_self() {
        let repeats = repeats_from(&[1, 1, 2, 3, 3]);

        let mut dest: Repeats<Vec<u64>> = Default::default();
        dest.extend_from_self(repeats.borrow(), 1..4);
        assert_eq!(dest.len(), 3);
        assert_eq!(*dest.borrow().get(0), 1);
        assert_eq!(*dest.borrow().get(1), 2);
        assert_eq!(*dest.borrow().get(2), 3);
    }

    #[test]
    fn as_from_bytes() {
        let mut repeats: Repeats<Vec<u64>> = Default::default();
        for i in 0..100u64 {
            repeats.push(&i);
            repeats.push(&i);
        }

        let borrowed = repeats.borrow();
        let rebuilt = Repeats::<&[u64], &[u64], &[u64], &[u64]>::from_bytes(
            &mut borrowed.as_bytes().map(|(_, bytes)| bytes)
        );
        assert_eq!(rebuilt.len(), 200);
        for i in 0..100u64 {
            assert_eq!(*rebuilt.get(2 * i as usize), i);
            assert_eq!(*rebuilt.get(2 * i as usize + 1), i);
        }
    }

    #[test]
    fn from_store_round_trip() {
        let mut repeats: Repeats<Vec<u64>> = Default::default();
        for i in 0..50u64 {
            repeats.push(&i);
            repeats.push(&i);
        }

        let mut store = Vec::new();
        crate::bytes::indexed::encode(&mut store, &repeats.borrow());
        let ds = crate::bytes::indexed::DecodedStore::new(&store);
        let rebuilt = Repeats::<&[u64], &[u64], &[u64], &[u64]>::from_store(&ds, &mut 0);
        assert_eq!(rebuilt.len(), 100);
        for i in 0..50u64 {
            assert_eq!(*rebuilt.get(2 * i as usize), i);
            assert_eq!(*rebuilt.get(2 * i as usize + 1), i);
        }
    }

    #[test]
    fn validate_via_stash() {
        let repeats = repeats_from(&[1, 1, 2, 2, 3]);

        let mut bytes: Vec<u8> = Vec::new();
        crate::bytes::indexed::write(&mut bytes, &repeats.borrow()).unwrap();
        let stash: Stash<Repeats<Vec<u64>>, Vec<u8>> =
            Stash::try_from_bytes(bytes).expect("Repeats<Vec<u64>> should validate");
        let borrowed = stash.borrow();
        assert_eq!(borrowed.len(), 5);
        assert_eq!(*borrowed.get(0), 1);
        assert_eq!(*borrowed.get(1), 1);
        assert_eq!(*borrowed.get(2), 2);
        assert_eq!(*borrowed.get(3), 2);
        assert_eq!(*borrowed.get(4), 3);
    }

    #[test]
    fn all_repeats() {
        let mut repeats: Repeats<Vec<u64>> = Default::default();
        for _ in 0..100 {
            repeats.push(&42u64);
        }
        assert_eq!(repeats.len(), 100);
        // Only one distinct value stored.
        assert_eq!(repeats.inner.somes.len(), 1);

        let borrowed = repeats.borrow();
        for i in 0..100 {
            assert_eq!(*borrowed.get(i), 42);
        }
    }

    #[test]
    fn no_repeats() {
        let mut repeats: Repeats<Vec<u64>> = Default::default();
        for i in 0..100u64 {
            repeats.push(&i);
        }
        assert_eq!(repeats.len(), 100);
        // Every value is distinct.
        assert_eq!(repeats.inner.somes.len(), 100);

        let borrowed = repeats.borrow();
        for i in 0..100u64 {
            assert_eq!(*borrowed.get(i as usize), i);
        }
    }
}
