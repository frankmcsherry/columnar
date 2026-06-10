//! Bounds of contiguously stored lists, and containers thereof.
//!
//! A bounds container describes how a sequence of values is partitioned into
//! lists: list `i` occupies value positions `bounds(i).0 .. bounds(i).1`, and
//! the lists tile the values: the first list starts at zero, and each list
//! starts where its predecessor ends. The [`Bounds`] trait provides the read
//! side: the extent of each list ("select"), and the list containing each
//! value position ("rank").
//!
//! As *containers* ([`Index`], [`Push`], [`Container`]) these types present
//! the sequence of list *lengths*; any cumulative representation is a private
//! choice of the implementor. Lengths make the container contract uniform:
//! re-pushing the lengths of another container's lists reproduces those lists
//! rebased onto `self`, so the default `Container::extend_from_self` is
//! correct as-is, and implementors can specialize it (a `memcpy` of shifted
//! offsets, or bit concatenation) without consumers like `Vecs` doing offset
//! arithmetic on their behalf. Lengths also leave overlapping or disjoint
//! lists inexpressible, by design.

use alloc::{vec::Vec, string::String};
use crate::{Borrow, Container, Clear, Len, Index, IndexAs, Push, RankSelect};
use crate::common::index::CopyAs;

/// Extents of a sequence of contiguously stored lists.
///
/// List `i` occupies value positions `bounds(i).0 .. bounds(i).1`, where
/// `bounds(0).0 == 0` and `bounds(i+1).0 == bounds(i).1`.
pub trait Bounds: Len {
    /// The half-open extent `[lower, upper)` of list `index`.
    fn bounds(&self, index: usize) -> (u64, u64);
    /// The index of the list containing value position `offset`.
    ///
    /// Lists tile the value positions `0 .. self.total()`, so each such offset
    /// belongs to exactly one list (empty lists contain no positions). For
    /// `offset >= self.total()` the result is `self.len()`.
    ///
    /// The default implementation binary searches `bounds`; implementors with
    /// more direct access (strides divide, rank/select structures rank) should
    /// override it.
    fn rank(&self, offset: u64) -> usize {
        // Partition point: the least `index` with `bounds(index).1 > offset`.
        let mut lo = 0;
        let mut hi = self.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.bounds(mid).1 <= offset { lo = mid + 1; } else { hi = mid; }
        }
        lo
    }
    /// One past the last value position; equivalently, the sum of all list lengths.
    fn total(&self) -> u64 {
        if self.is_empty() { 0 } else { self.bounds(self.len() - 1).1 }
    }
}

impl<'b, B: Bounds + ?Sized> Bounds for &'b B {
    #[inline(always)] fn bounds(&self, index: usize) -> (u64, u64) { B::bounds(*self, index) }
    #[inline(always)] fn rank(&self, offset: u64) -> usize { B::rank(*self, offset) }
    #[inline(always)] fn total(&self) -> u64 { B::total(*self) }
}

/// Sorted `u64` sequences read as cumulative upper bounds.
///
/// These implementations support read paths over raw offset arrays, including
/// types from before the [`Bounds`] trait existed. They are read-only as
/// bounds: `Vec<u64>`'s `Push` and `Container` implementations mean literal
/// elements rather than list lengths, so `Vec<u64>` is deliberately *not* a
/// [`BoundsContainer`]. Use [`Uppers`] when building.
impl Bounds for [u64] {
    #[inline(always)]
    fn bounds(&self, index: usize) -> (u64, u64) {
        let lower = if index == 0 { 0 } else { self[index - 1] };
        (lower, self[index])
    }
    #[inline(always)]
    fn total(&self) -> u64 { self.last().copied().unwrap_or(0) }
}
impl Bounds for Vec<u64> {
    #[inline(always)] fn bounds(&self, index: usize) -> (u64, u64) { self[..].bounds(index) }
    #[inline(always)] fn rank(&self, offset: u64) -> usize { self[..].rank(offset) }
    #[inline(always)] fn total(&self) -> u64 { self[..].total() }
}

/// A composite trait for types whose borrowed form answers [`Bounds`] queries.
pub trait BorrowBounds: for<'a> Borrow<Borrowed<'a>: Bounds> {}
impl<C: for<'a> Borrow<Borrowed<'a>: Bounds>> BorrowBounds for C {}

/// A container of list lengths that supports [`Bounds`] queries: what `Vecs`
/// and `Strings` require of their bounds.
///
/// This trait is an explicit opt-in rather than a blanket implementation,
/// because it asserts a semantic commitment the supertraits cannot: that the
/// container's `Push`/`Index`/`extend_from_self` traffic in list *lengths*.
/// `Vec<u64>` satisfies every supertrait, but its pushes mean literal
/// elements, so it must not implement this trait.
pub trait BoundsContainer: Bounds + BorrowBounds + Container + for<'a> Push<&'a u64> {}

/// Cumulative upper bounds in a `u64` array: the default bounds container.
///
/// Stores the upper bound of each list; each lower bound is the preceding
/// upper bound (zero for the first list). As a container it presents list
/// lengths, per the [`Bounds`] contract; the cumulative form is its storage
/// strategy, and matches the serialized layout of a bare `u64` array.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Copy, Clone, Debug, Default, PartialEq)]
pub struct Uppers<BC = Vec<u64>> {
    /// The upper bound of each list, in order.
    pub uppers: BC,
}

impl<BC: Len> Len for Uppers<BC> {
    #[inline(always)] fn len(&self) -> usize { self.uppers.len() }
}

impl<BC: Len + IndexAs<u64>> Bounds for Uppers<BC> {
    #[inline(always)]
    fn bounds(&self, index: usize) -> (u64, u64) {
        let lower = if index == 0 { 0 } else { self.uppers.index_as(index - 1) };
        (lower, self.uppers.index_as(index))
    }
    #[inline(always)]
    fn total(&self) -> u64 {
        if self.uppers.is_empty() { 0 } else { self.uppers.index_as(self.uppers.len() - 1) }
    }
}

impl<BC: Len + IndexAs<u64>> Index for Uppers<BC> {
    type Ref = u64;
    /// The length of list `index`.
    #[inline(always)]
    fn get(&self, index: usize) -> Self::Ref {
        let (lower, upper) = self.bounds(index);
        upper - lower
    }
}

impl Push<u64> for Uppers {
    #[inline(always)]
    fn push(&mut self, item: u64) {
        let total = self.total();
        self.uppers.push(total + item);
    }
}
impl<'a> Push<&'a u64> for Uppers {
    #[inline(always)] fn push(&mut self, item: &'a u64) { self.push(*item) }
}
impl Clear for Uppers {
    #[inline(always)] fn clear(&mut self) { self.uppers.clear() }
}

impl Uppers {
    /// Removes the last list, if one exists.
    #[inline(always)]
    pub fn pop(&mut self) { self.uppers.pop(); }
}

impl Borrow for Uppers {
    type Ref<'a> = u64;
    type Borrowed<'a> = Uppers<&'a [u64]>;
    #[inline(always)] fn borrow<'a>(&'a self) -> Self::Borrowed<'a> { Uppers { uppers: &self.uppers[..] } }
    #[inline(always)] fn reborrow<'b, 'a: 'b>(item: Self::Borrowed<'a>) -> Self::Borrowed<'b> where Self: 'a {
        Uppers { uppers: item.uppers }
    }
    #[inline(always)] fn reborrow_ref<'b, 'a: 'b>(item: Self::Ref<'a>) -> Self::Ref<'b> where Self: 'a { item }
}

impl Container for Uppers {
    #[inline(always)]
    fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: core::ops::Range<usize>) {
        if !range.is_empty() {
            // Rebase `other`'s uppers so the first appended list starts at our total.
            let total = self.total();
            let other_lower = other.bounds(range.start).0;
            if total == other_lower {
                self.uppers.extend_from_slice(&other.uppers[range]);
            }
            else {
                self.uppers.reserve(range.len());
                for index in range {
                    self.uppers.push(other.uppers[index] - other_lower + total);
                }
            }
        }
    }
    fn reserve_for<'a, I>(&mut self, selves: I) where Self: 'a, I: Iterator<Item = Self::Borrowed<'a>> + Clone {
        self.uppers.reserve(selves.map(|x| x.uppers.len()).sum::<usize>());
    }
}

impl BoundsContainer for Uppers {}

impl<'a> crate::AsBytes<'a> for Uppers<&'a [u64]> {
    const SLICE_COUNT: usize = <&'a [u64] as crate::AsBytes<'a>>::SLICE_COUNT;
    #[inline]
    fn get_byte_slice(&self, index: usize) -> (u64, &'a [u8]) {
        self.uppers.get_byte_slice(index)
    }
}
impl<'a> crate::FromBytes<'a> for Uppers<&'a [u64]> {
    const SLICE_COUNT: usize = <&'a [u64] as crate::FromBytes<'a>>::SLICE_COUNT;
    #[inline(always)]
    fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
        Self { uppers: crate::FromBytes::from_bytes(bytes) }
    }
    #[inline(always)]
    fn from_store(store: &crate::bytes::indexed::DecodedStore<'a>, offset: &mut usize) -> Self {
        Self { uppers: crate::FromBytes::from_store(store, offset) }
    }
    fn element_sizes(sizes: &mut Vec<usize>) -> Result<(), String> {
        <&'a [u64]>::element_sizes(sizes)
    }
}

/// Implementations common to the two `RankSelect`-backed bounds containers.
///
/// Both store a list count (`NC`: `u64` owned, `&u64` borrowed) and a bit
/// vector (`RankSelect`); they differ in how lists are encoded as bits, which
/// is confined to their `Bounds`, `Push`, and `Container` implementations.
macro_rules! bitvec_bounds_common {
    ($name:ident) => {
        impl<CC, VC, NC: CopyAs<u64>, WC> Len for $name<CC, VC, NC, WC> {
            #[inline(always)] fn len(&self) -> usize { self.count.copy_as() as usize }
        }

        impl<CC: Len + IndexAs<u64>, VC: Len + IndexAs<u64>, NC: CopyAs<u64>, WC: IndexAs<u64>> Index for $name<CC, VC, NC, WC> {
            type Ref = u64;
            /// The length of list `index`.
            #[inline(always)]
            fn get(&self, index: usize) -> Self::Ref {
                let (lower, upper) = self.bounds(index);
                upper - lower
            }
        }

        impl<'a> Push<&'a u64> for $name {
            #[inline(always)] fn push(&mut self, item: &'a u64) { self.push(*item) }
        }

        impl Clear for $name {
            #[inline(always)]
            fn clear(&mut self) {
                self.count = 0;
                self.bits.clear();
            }
        }

        impl Borrow for $name {
            type Ref<'a> = u64;
            type Borrowed<'a> = $name<&'a [u64], &'a [u64], &'a u64, &'a [u64]>;
            #[inline(always)]
            fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
                $name { count: &self.count, bits: self.bits.borrow() }
            }
            #[inline(always)]
            fn reborrow<'b, 'a: 'b>(item: Self::Borrowed<'a>) -> Self::Borrowed<'b> where Self: 'a {
                $name { count: item.count, bits: RankSelect::<Vec<u64>, Vec<u64>>::reborrow(item.bits) }
            }
            #[inline(always)]
            fn reborrow_ref<'b, 'a: 'b>(item: Self::Ref<'a>) -> Self::Ref<'b> where Self: 'a { item }
        }

        impl BoundsContainer for $name {}

        impl<'a> crate::AsBytes<'a> for $name<&'a [u64], &'a [u64], &'a u64, &'a [u64]> {
            const SLICE_COUNT: usize = 1 + <RankSelect<&'a [u64], &'a [u64], &'a [u64]> as crate::AsBytes<'a>>::SLICE_COUNT;
            #[inline]
            fn get_byte_slice(&self, index: usize) -> (u64, &'a [u8]) {
                debug_assert!(index < Self::SLICE_COUNT);
                if index == 0 {
                    (8, bytemuck::cast_slice(core::slice::from_ref(self.count)))
                } else {
                    self.bits.get_byte_slice(index - 1)
                }
            }
        }
        impl<'a> crate::FromBytes<'a> for $name<&'a [u64], &'a [u64], &'a u64, &'a [u64]> {
            const SLICE_COUNT: usize = 1 + <RankSelect<&'a [u64], &'a [u64], &'a [u64]> as crate::FromBytes<'a>>::SLICE_COUNT;
            #[inline(always)]
            fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
                let count = &bytemuck::try_cast_slice(bytes.next().expect("Iterator exhausted prematurely")).unwrap()[0];
                Self { count, bits: crate::FromBytes::from_bytes(bytes) }
            }
            #[inline(always)]
            fn from_store(store: &crate::bytes::indexed::DecodedStore<'a>, offset: &mut usize) -> Self {
                let (w, _) = store.get(*offset); *offset += 1;
                let count = w.first().unwrap_or(&0);
                Self { count, bits: crate::FromBytes::from_store(store, offset) }
            }
            fn element_sizes(sizes: &mut Vec<usize>) -> Result<(), String> {
                sizes.push(8); // count
                <RankSelect<&'a [u64], &'a [u64], &'a [u64]>>::element_sizes(sizes)
            }
            fn validate(slices: &[(&[u64], u8)]) -> Result<(), String> {
                if slices.is_empty() || slices[0].0.is_empty() {
                    return Err(concat!(stringify!($name), ": count slice must be non-empty").into());
                }
                <RankSelect<&'a [u64], &'a [u64], &'a [u64]>>::validate(&slices[1..])
            }
        }
    }
}

/// Bounds for non-empty lists: one bit per value, set at each list's last value.
///
/// The `i`-th set bit marks the last value of list `i`, so `upper(i)` is
/// `select(i) + 1`, and the list containing value position `offset` is
/// exactly `rank(offset)`. Costs one bit per value. Pushing an empty list
/// is a contract violation (debug-asserted): use [`MaybeEmpty`] when lists
/// may be empty.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Copy, Clone, Debug, Default, PartialEq)]
pub struct NeverEmpty<CC = Vec<u64>, VC = Vec<u64>, NC = u64, WC = [u64; 2]> {
    /// The number of lists (set bits).
    pub count: NC,
    /// One bit per value, set at each list's last value.
    pub bits: RankSelect<CC, VC, WC>,
}

bitvec_bounds_common!(NeverEmpty);

impl<CC: Len + IndexAs<u64>, VC: Len + IndexAs<u64>, NC: CopyAs<u64>, WC: IndexAs<u64>> Bounds for NeverEmpty<CC, VC, NC, WC> {
    #[inline]
    fn bounds(&self, index: usize) -> (u64, u64) {
        // One select for the preceding end; the list's own end is the next
        // set bit, usually a word read away.
        if index == 0 {
            let upper = self.bits.select(0).expect("NeverEmpty: list index out of bounds") as u64 + 1;
            (0, upper)
        } else {
            let prev = self.bits.select(index as u64 - 1).expect("NeverEmpty: list index out of bounds");
            let next = self.bits.select_from(prev + 1, index as u64).expect("NeverEmpty: list index out of bounds");
            (prev as u64 + 1, next as u64 + 1)
        }
    }
    #[inline]
    fn rank(&self, offset: u64) -> usize {
        self.bits.rank(offset as usize)
    }
    #[inline]
    fn total(&self) -> u64 { self.bits.len() as u64 }
}

impl Push<u64> for NeverEmpty {
    /// Pushes a list of length `item`, which must be non-zero.
    #[inline]
    fn push(&mut self, item: u64) {
        debug_assert!(item > 0, "NeverEmpty requires non-empty lists; use MaybeEmpty for possibly-empty lists");
        let mut zeros = item.saturating_sub(1) as usize;
        while zeros >= 64 {
            self.bits.push_bits(0, 64);
            zeros -= 64;
        }
        self.bits.push_bits(1 << zeros, zeros + 1);
        self.count += 1;
    }
}

impl Container for NeverEmpty {
    #[inline]
    fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: core::ops::Range<usize>) {
        if !range.is_empty() {
            // The lists' bits are exactly their values' positions.
            let lower = other.bounds(range.start).0 as usize;
            let upper = other.bounds(range.end - 1).1 as usize;
            self.bits.extend_from_bits(&other.bits, lower .. upper);
            self.count += range.len() as u64;
        }
    }
    fn reserve_for<'a, I>(&mut self, selves: I) where Self: 'a, I: Iterator<Item = Self::Borrowed<'a>> + Clone {
        self.bits.values.values.reserve(selves.map(|x| x.bits.values.values.len()).sum::<usize>());
    }
}

/// Bounds for possibly-empty lists, in unary: each list contributes one zero
/// bit per value, then a one. Costs one bit per value plus one bit per list.
///
/// The `i`-th set bit sits at position `upper(i) + i`, so `upper(i)` is
/// `select(i) - i`, and the list containing value position `offset` is
/// `select_zero(offset) - offset` (the value's bit position, less the zeros
/// preceding it).
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Copy, Clone, Debug, Default, PartialEq)]
pub struct MaybeEmpty<CC = Vec<u64>, VC = Vec<u64>, NC = u64, WC = [u64; 2]> {
    /// The number of lists (set bits).
    pub count: NC,
    /// For each list in order, a zero per value and then a one.
    pub bits: RankSelect<CC, VC, WC>,
}

bitvec_bounds_common!(MaybeEmpty);

impl<CC: Len + IndexAs<u64>, VC: Len + IndexAs<u64>, NC: CopyAs<u64>, WC: IndexAs<u64>> Bounds for MaybeEmpty<CC, VC, NC, WC> {
    #[inline]
    fn bounds(&self, index: usize) -> (u64, u64) {
        // One select for the preceding end; the list's own end is the next
        // set bit, usually a word read away.
        let index = index as u64;
        if index == 0 {
            let upper = self.bits.select(0).expect("MaybeEmpty: list index out of bounds") as u64;
            (0, upper)
        } else {
            let prev = self.bits.select(index - 1).expect("MaybeEmpty: list index out of bounds");
            let next = self.bits.select_from(prev + 1, index).expect("MaybeEmpty: list index out of bounds");
            (prev as u64 + 1 - index, next as u64 - index)
        }
    }
    #[inline]
    fn rank(&self, offset: u64) -> usize {
        match self.bits.select_zero(offset) {
            Some(pos) => pos - offset as usize,
            None => self.len(),
        }
    }
    #[inline]
    fn total(&self) -> u64 { self.bits.len() as u64 - self.count.copy_as() }
}

impl Push<u64> for MaybeEmpty {
    /// Pushes a list of length `item`.
    #[inline]
    fn push(&mut self, item: u64) {
        let mut zeros = item as usize;
        while zeros >= 64 {
            self.bits.push_bits(0, 64);
            zeros -= 64;
        }
        self.bits.push_bits(1 << zeros, zeros + 1);
        self.count += 1;
    }
}

impl Container for MaybeEmpty {
    #[inline]
    fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: core::ops::Range<usize>) {
        if !range.is_empty() {
            // List `j` occupies bits `upper(j-1) + j .. upper(j) + j + 1`.
            let lower = other.bounds(range.start).0 as usize + range.start;
            let upper = other.bounds(range.end - 1).1 as usize + range.end;
            self.bits.extend_from_bits(&other.bits, lower .. upper);
            self.count += range.len() as u64;
        }
    }
    fn reserve_for<'a, I>(&mut self, selves: I) where Self: 'a, I: Iterator<Item = Self::Borrowed<'a>> + Clone {
        self.bits.values.values.reserve(selves.map(|x| x.bits.values.values.len()).sum::<usize>());
    }
}

#[cfg(test)]
mod test {

    use alloc::vec;
    use alloc::vec::Vec;
    use crate::{Borrow, Container, Index, Len, Push};
    use super::{Bounds, Uppers};

    #[test]
    fn uppers_lengths_and_bounds() {
        let mut uppers = Uppers::default();
        for length in [3u64, 0, 2, 0, 0, 5] {
            uppers.push(length);
        }
        assert_eq!(uppers.uppers, vec![3, 3, 5, 5, 5, 10]);
        assert_eq!(uppers.len(), 6);
        assert_eq!(uppers.total(), 10);
        // `get` reports lengths; `bounds` reports extents.
        assert_eq!((0..6).map(|i| uppers.get(i)).collect::<Vec<_>>(), vec![3, 0, 2, 0, 0, 5]);
        assert_eq!(uppers.bounds(0), (0, 3));
        assert_eq!(uppers.bounds(2), (3, 5));
        assert_eq!(uppers.bounds(5), (5, 10));
        // `rank` finds the list containing each position, skipping empty lists.
        assert_eq!(uppers.rank(0), 0);
        assert_eq!(uppers.rank(2), 0);
        assert_eq!(uppers.rank(3), 2);
        assert_eq!(uppers.rank(4), 2);
        assert_eq!(uppers.rank(5), 5);
        assert_eq!(uppers.rank(9), 5);
        assert_eq!(uppers.rank(10), 6);
    }

    #[test]
    fn uppers_extend_rebases() {
        let mut source = Uppers::default();
        for length in [1u64, 2, 3, 4] {
            source.push(length);
        }
        let mut target = Uppers::default();
        target.push(5u64);
        // Lists 1..3 of `source` (lengths 2, 3) appended after a list of length 5.
        target.extend_from_self(source.borrow(), 1..3);
        assert_eq!(target.uppers, vec![5, 7, 10]);
        // Aligned case: from a fresh target, lists 0..2 memcpy without shifting.
        let mut target = Uppers::default();
        target.extend_from_self(source.borrow(), 0..2);
        assert_eq!(target.uppers, vec![1, 3]);
    }

    #[test]
    fn vecs_extend_rebases() {
        use crate::Vecs;
        let mut source: Vecs<Vec<u8>> = Vecs::default();
        source.push([1u8, 2, 3]);
        source.push([4u8]);
        source.push([5u8, 6]);
        let mut target: Vecs<Vec<u8>> = Vecs::default();
        target.push([9u8]);
        target.extend_from_self(source.borrow(), 1..3);
        assert_eq!(target.values, vec![9, 4, 5, 6]);
        assert_eq!(target.bounds.uppers, vec![1, 2, 4]);
    }

    #[test]
    fn raw_vec_bounds_read_compat() {
        use crate::Vecs;
        // The pre-`Bounds` data shape: raw cumulative uppers. Reading still works;
        // writing requires `Uppers`, which shares the serialized layout.
        let cols: Vecs<Vec<u8>, Vec<u64>> = Vecs { bounds: vec![3, 3, 5], values: vec![1, 2, 3, 4, 5] };
        assert_eq!(cols.len(), 3);
        assert_eq!((&cols).get(1).len(), 0);
        let got: Vec<u8> = (&cols).get(2).into_iter().copied().collect();
        assert_eq!(got, vec![4, 5]);
    }

    #[test]
    fn never_empty_round_trip() {
        use crate::Vecs;
        use super::NeverEmpty;
        // Lengths 1..=37, totalling several BITS_PER_CHUNK chunks.
        let lists: Vec<Vec<u32>> = (0..400).map(|i| (0 .. (i % 37) + 1).collect()).collect();
        let mut cols = Vecs::<Vec<u32>, NeverEmpty>::default();
        for list in lists.iter() {
            cols.push(list);
        }
        assert_eq!(cols.len(), lists.len());
        let mut position = 0;
        for (index, list) in lists.iter().enumerate() {
            let got: Vec<u32> = cols.borrow().get(index).into_iter().copied().collect();
            assert_eq!(&got, list);
            assert_eq!(cols.bounds.get(index), list.len() as u64);
            for _ in list.iter() {
                assert_eq!(cols.bounds.rank(position), index);
                position += 1;
            }
        }
        assert_eq!(cols.bounds.total(), position);
        assert_eq!(cols.bounds.rank(position), lists.len());
    }

    #[test]
    fn maybe_empty_round_trip() {
        use crate::Vecs;
        use super::MaybeEmpty;
        // Lengths 0..=36, including many empty lists.
        let lists: Vec<Vec<u32>> = (0..400).map(|i| (0 .. (i * i) % 37).collect()).collect();
        let mut cols = Vecs::<Vec<u32>, MaybeEmpty>::default();
        for list in lists.iter() {
            cols.push(list);
        }
        assert_eq!(cols.len(), lists.len());
        let mut position = 0;
        for (index, list) in lists.iter().enumerate() {
            let got: Vec<u32> = cols.borrow().get(index).into_iter().copied().collect();
            assert_eq!(&got, list);
            assert_eq!(cols.bounds.get(index), list.len() as u64);
            for _ in list.iter() {
                assert_eq!(cols.bounds.rank(position), index);
                position += 1;
            }
        }
        assert_eq!(cols.bounds.total(), position);
        assert_eq!(cols.bounds.rank(position), lists.len());
    }

    #[test]
    fn bitvec_bounds_extend_matches_pushes() {
        use super::{NeverEmpty, MaybeEmpty};
        // Extending by a range must equal pushing each length, bit for bit,
        // across misaligned prefixes and range endpoints (exercising the
        // word splice and the count summaries).
        let lengths: Vec<u64> = (0..500u64).map(|i| (i * i) % 9).collect();
        let cases = [(0usize, 0usize, 500usize), (3, 2, 400), (1, 17, 18), (5, 499, 500), (2, 100, 100), (4, 0, 500)];

        let mut source = MaybeEmpty::default();
        for &length in lengths.iter() { source.push(length); }
        for (prefix, start, end) in cases {
            let mut extended = MaybeEmpty::default();
            let mut pushed = MaybeEmpty::default();
            for i in 0..prefix {
                extended.push((i % 4) as u64);
                pushed.push((i % 4) as u64);
            }
            extended.extend_from_self(source.borrow(), start..end);
            for i in start..end { pushed.push(lengths[i]); }
            assert_eq!(extended, pushed, "MaybeEmpty prefix {prefix} range {start}..{end}");
        }

        let mut source = NeverEmpty::default();
        for &length in lengths.iter() { source.push(length + 1); }
        for (prefix, start, end) in cases {
            let mut extended = NeverEmpty::default();
            let mut pushed = NeverEmpty::default();
            for i in 0..prefix {
                extended.push((i % 4) as u64 + 1);
                pushed.push((i % 4) as u64 + 1);
            }
            extended.extend_from_self(source.borrow(), start..end);
            for i in start..end { pushed.push(lengths[i] + 1); }
            assert_eq!(extended, pushed, "NeverEmpty prefix {prefix} range {start}..{end}");
        }
    }

    #[test]
    fn bitvec_bounds_bytes_round_trip() {
        use crate::{AsBytes, FromBytes};
        use super::MaybeEmpty;
        let mut maybe_empty = MaybeEmpty::default();
        for length in [3u64, 0, 2, 64, 0, 130] { maybe_empty.push(length); }
        let borrowed = maybe_empty.borrow();
        let slices: Vec<(u64, &[u8])> = borrowed.as_bytes().collect();
        let mut iter = slices.iter().map(|(_, bytes)| *bytes);
        let decoded = <MaybeEmpty<&[u64], &[u64], &u64, &[u64]> as FromBytes>::from_bytes(&mut iter);
        assert_eq!(decoded.len(), maybe_empty.len());
        for index in 0..maybe_empty.len() {
            assert_eq!(Bounds::bounds(&decoded, index), Bounds::bounds(&maybe_empty, index));
        }
    }

    #[test]
    fn raw_u64_read_side() {
        // `Vec<u64>` and `&[u64]` answer Bounds queries as cumulative uppers.
        let uppers: Vec<u64> = vec![3, 3, 5, 10];
        assert_eq!(uppers.bounds(1), (3, 3));
        assert_eq!(uppers.total(), 10);
        assert_eq!(uppers.rank(4), 2);
        assert_eq!((&uppers[..]).bounds(3), (5, 10));
    }
}
