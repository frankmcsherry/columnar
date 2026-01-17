//! Containers for enumerations ("sum types") that store variants separately.
//!
//! The main work of these types is storing a discriminant and index efficiently,
//! as containers for each of the variant types can hold the actual data.

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
    use crate::{Borrow, Len, Index, IndexAs, Push, Clear, HeapSize};

    /// A store for maintaining `Vec<bool>` with fast `rank` and `select` access.
    ///
    /// The design is to have `u64` running counts for each block of 1024 bits,
    /// which are roughly the size of a cache line. This is roughly 6% overhead,
    /// above the bits themselves, which seems pretty solid.
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[derive(Copy, Clone, Debug, Default, PartialEq)]
    pub struct RankSelect<CC = Vec<u64>, VC = Vec<u64>, WC = u64> {
        /// Counts of the number of cumulative set (true) bits, *after* each block of 1024 bits.
        pub counts: CC,
        /// The bits themselves.
        pub values: Bools<VC, WC>,
    }

    impl<CC: crate::common::BorrowIndexAs<u64>, VC: crate::common::BorrowIndexAs<u64>> RankSelect<CC, VC> {
        #[inline(always)]
        pub fn borrow<'a>(&'a self) -> RankSelect<CC::Borrowed<'a>, VC::Borrowed<'a>, &'a u64> {
            RankSelect {
                counts: self.counts.borrow(),
                values: self.values.borrow(),
            }
        }
        #[inline(always)]
        pub fn reborrow<'b, 'a: 'b>(thing: RankSelect<CC::Borrowed<'a>, VC::Borrowed<'a>, &'a u64>) -> RankSelect<CC::Borrowed<'b>, VC::Borrowed<'b>, &'b u64> {
            RankSelect {
                counts: CC::reborrow(thing.counts),
                values: Bools::<VC, u64>::reborrow(thing.values),
            }
        }
    }

    impl<'a, CC: crate::AsBytes<'a>, VC: crate::AsBytes<'a>> crate::AsBytes<'a> for RankSelect<CC, VC, &'a u64> {
        #[inline(always)]
        fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
            crate::chain(self.counts.as_bytes(), self.values.as_bytes())
        }
    }
    impl<'a, CC: crate::FromBytes<'a>, VC: crate::FromBytes<'a>> crate::FromBytes<'a> for RankSelect<CC, VC, &'a u64> {
        #[inline(always)]
        fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
            Self {
                counts: crate::FromBytes::from_bytes(bytes),
                values: crate::FromBytes::from_bytes(bytes),
            }
        }
    }


    impl<CC, VC: Len + IndexAs<u64>, WC: CopyAs<u64>> RankSelect<CC, VC, WC> {
        #[inline(always)]
        pub fn get(&self, index: usize) -> bool {
            Index::get(&self.values, index)
        }
    }
    impl<CC: Len + IndexAs<u64>, VC: Len + IndexAs<u64>, WC: CopyAs<u64>> RankSelect<CC, VC, WC> {
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

    impl<CC, VC: Len, WC: CopyAs<u64>> RankSelect<CC, VC, WC> {
        pub fn len(&self) -> usize {
            self.values.len()
        }
    }

    // This implementation probably only works for `Vec<u64>` and `Vec<u64>`, but we could fix that.
    // Partly, it's hard to name the `Index` flavor that allows one to get back a `u64`.
    impl<CC: for<'a> Push<&'a u64> + Len + IndexAs<u64>, VC: for<'a> Push<&'a u64> + Len + IndexAs<u64>> RankSelect<CC, VC> {
        #[inline]
        pub fn push(&mut self, bit: bool) {
            self.values.push(&bit);
            while self.counts.len() < self.values.len() / 1024 {
                let mut count = self.counts.last().unwrap_or(0);
                let lower = 16 * self.counts.len();
                let upper = lower + 16;
                for i in lower .. upper {
                    count += self.values.values.index_as(i).count_ones() as u64;
                }
                self.counts.push(&count);
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
    use crate::{Clear, Columnar, Container, Len, IndexMut, Index, IndexAs, Push, HeapSize, Borrow};
    use crate::RankSelect;

    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[derive(Copy, Clone, Debug, Default, PartialEq)]
    pub struct Results<SC, TC, CC=Vec<u64>, VC=Vec<u64>, WC=u64> {
        /// Bits set to `true` correspond to `Ok` variants.
        pub indexes: RankSelect<CC, VC, WC>,
        pub oks: SC,
        pub errs: TC,
    }

    impl<S: Columnar, T: Columnar> Columnar for Result<S, T> {
        fn copy_from<'a>(&mut self, other: crate::Ref<'a, Self>) {
            match (&mut *self, other) {
                (Ok(x), Ok(y)) => x.copy_from(y),
                (Err(x), Err(y)) => x.copy_from(y),
                (_, other) => { *self = Self::into_owned(other); },
            }
        }
        fn into_owned<'a>(other: crate::Ref<'a, Self>) -> Self {
            match other {
                Ok(y) => Ok(S::into_owned(y)),
                Err(y) => Err(T::into_owned(y)),
            }
        }
        type Container = Results<S::Container, T::Container>;
    }

    impl<SC: Borrow, TC: Borrow> Borrow for Results<SC, TC> {
        type Ref<'a> = Result<SC::Ref<'a>, TC::Ref<'a>> where SC: 'a, TC: 'a;
        type Borrowed<'a> = Results<SC::Borrowed<'a>, TC::Borrowed<'a>, &'a [u64], &'a [u64], &'a u64> where SC: 'a, TC: 'a;
        fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
            Results {
                indexes: self.indexes.borrow(),
                oks: self.oks.borrow(),
                errs: self.errs.borrow(),
            }
        }
        #[inline(always)]
        fn reborrow<'b, 'a: 'b>(thing: Self::Borrowed<'a>) -> Self::Borrowed<'b> where SC: 'a, TC: 'a {
            Results {
                indexes: RankSelect::<Vec<u64>, Vec<u64>>::reborrow(thing.indexes),
                oks: SC::reborrow(thing.oks),
                errs: TC::reborrow(thing.errs),
            }
        }
        #[inline(always)]
        fn reborrow_ref<'b, 'a: 'b>(thing: Self::Ref<'a>) -> Self::Ref<'b> where Self: 'a {
            match thing {
                Ok(y) => Ok(SC::reborrow_ref(y)),
                Err(y) => Err(TC::reborrow_ref(y)),
            }
        }
    }

    impl<SC: Container, TC: Container> Container for Results<SC, TC> {
        #[inline(always)]
        fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: std::ops::Range<usize>) {
            if !range.is_empty() {
                // Starting offsets of each variant in `other`.
                let oks_start = other.indexes.rank(range.start);
                let errs_start = range.start - oks_start;

                // Count the number of `Ok` and `Err` variants as we push, to determine the range.
                // TODO: This could probably be `popcnt` somehow.
                let mut oks = 0;
                for index in range.clone() {
                    let bit = other.indexes.get(index);
                    self.indexes.push(bit);
                    if bit { oks += 1; }
                }
                let errs = range.len() - oks;

                self.oks.extend_from_self(other.oks, oks_start .. oks_start + oks);
                self.errs.extend_from_self(other.errs, errs_start .. errs_start + errs);
            }
        }

        fn reserve_for<'a, I>(&mut self, selves: I) where Self: 'a, I: Iterator<Item = Self::Borrowed<'a>> + Clone {
            // TODO: reserve room in `self.indexes`.
            self.oks.reserve_for(selves.clone().map(|x| x.oks));
            self.errs.reserve_for(selves.map(|x| x.errs));
        }
    }

    impl<'a, SC: crate::AsBytes<'a>, TC: crate::AsBytes<'a>, CC: crate::AsBytes<'a>, VC: crate::AsBytes<'a>> crate::AsBytes<'a> for Results<SC, TC, CC, VC, &'a u64> {
        #[inline(always)]
        fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
            let iter = self.indexes.as_bytes();
            let iter = crate::chain(iter, self.oks.as_bytes());
            crate::chain(iter, self.errs.as_bytes())
        }
    }
    impl<'a, SC: crate::FromBytes<'a>, TC: crate::FromBytes<'a>, CC: crate::FromBytes<'a>, VC: crate::FromBytes<'a>> crate::FromBytes<'a> for Results<SC, TC, CC, VC, &'a u64> {
        #[inline(always)]
        fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
            Self {
                indexes: crate::FromBytes::from_bytes(bytes),
                oks: crate::FromBytes::from_bytes(bytes),
                errs: crate::FromBytes::from_bytes(bytes),
            }
        }
    }

    impl<SC, TC, CC, VC: Len, WC: CopyAs<u64>> Len for Results<SC, TC, CC, VC, WC> {
        #[inline(always)] fn len(&self) -> usize { self.indexes.len() }
    }

    impl<SC, TC, CC, VC, WC> Index for Results<SC, TC, CC, VC, WC>
    where
        SC: Index,
        TC: Index,
        CC: IndexAs<u64> + Len,
        VC: IndexAs<u64> + Len,
        WC: CopyAs<u64>,
    {
        type Ref = Result<SC::Ref, TC::Ref>;
        #[inline(always)]
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
        WC: CopyAs<u64>,
    {
        type Ref = Result<<&'a SC as Index>::Ref, <&'a TC as Index>::Ref>;
        #[inline(always)]
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
        #[inline(always)]
        fn get_mut(&mut self, index: usize) -> Self::IndexMut<'_> {
            if self.indexes.get(index) {
                Ok(self.oks.get_mut(self.indexes.rank(index)))
            } else {
                Err(self.errs.get_mut(index - self.indexes.rank(index)))
            }
        }
    }

    impl<S, SC: Push<S>, T, TC: Push<T>> Push<Result<S, T>> for Results<SC, TC> {
        #[inline]
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
        #[inline]
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

    impl<SC, TC, CC, VC, WC> Results<SC, TC, CC, VC, WC> {
        /// Returns ok values if no errors exist.
        pub fn unwrap(self) -> SC where TC: Len {
            assert!(self.errs.is_empty());
            self.oks
        }
        /// Returns error values if no oks exist.
        pub fn unwrap_err(self) -> TC where SC: Len {
            assert!(self.oks.is_empty());
            self.errs
        }
    }
    #[cfg(test)]
    mod test {
        #[test]
        fn round_trip() {

            use crate::common::{Index, Push, HeapSize, Len};

            let mut column: crate::ContainerOf<Result<u64, u64>> = Default::default();
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

            let mut column: crate::ContainerOf<Result<u64, u8>> = Default::default();
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
    use crate::{Clear, Columnar, Container, Len, IndexMut, Index, IndexAs, Push, HeapSize, Borrow};
    use crate::RankSelect;

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[derive(Copy, Clone, Debug, Default, PartialEq)]
    pub struct Options<TC, CC=Vec<u64>, VC=Vec<u64>, WC=u64> {
        /// Uses two bits for each item, one to indicate the variant and one (amortized)
        /// to enable efficient rank determination.
        pub indexes: RankSelect<CC, VC, WC>,
        pub somes: TC,
    }

    impl<T: Columnar> Columnar for Option<T> {
        fn copy_from<'a>(&mut self, other: crate::Ref<'a, Self>) {
            match (&mut *self, other) {
                (Some(x), Some(y)) => { x.copy_from(y); }
                (_, other) => { *self = Self::into_owned(other); }
            }
        }
        fn into_owned<'a>(other: crate::Ref<'a, Self>) -> Self {
            other.map(|x| T::into_owned(x))
        }
        type Container = Options<T::Container>;
    }

    impl<TC: Borrow> Borrow for Options<TC> {
        type Ref<'a> = Option<TC::Ref<'a>> where TC: 'a;
        type Borrowed<'a> = Options<TC::Borrowed<'a>, &'a [u64], &'a [u64], &'a u64> where TC: 'a;
        fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
            Options {
                indexes: self.indexes.borrow(),
                somes: self.somes.borrow(),
            }
        }
        #[inline(always)]
        fn reborrow<'b, 'a: 'b>(thing: Self::Borrowed<'a>) -> Self::Borrowed<'b> where TC: 'a {
            Options {
                indexes: RankSelect::<Vec<u64>, Vec<u64>>::reborrow(thing.indexes),
                somes: TC::reborrow(thing.somes),
            }
        }
        #[inline(always)]
        fn reborrow_ref<'b, 'a: 'b>(thing: Self::Ref<'a>) -> Self::Ref<'b> where Self: 'a {
            thing.map(TC::reborrow_ref)
        }
    }

    impl<TC: Container> Container for Options<TC> {
        #[inline(always)]
        fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: std::ops::Range<usize>) {
            if !range.is_empty() {
                // Starting offsets of `Some` variants in `other`.
                let somes_start = other.indexes.rank(range.start);

                // Count the number of `Some` variants as we push, to determine the range.
                // TODO: This could probably be `popcnt` somehow.
                let mut somes = 0;
                for index in range {
                    let bit = other.indexes.get(index);
                    self.indexes.push(bit);
                    if bit { somes += 1; }
                }

                self.somes.extend_from_self(other.somes, somes_start .. somes_start + somes);
            }
        }

        fn reserve_for<'a, I>(&mut self, selves: I) where Self: 'a, I: Iterator<Item = Self::Borrowed<'a>> + Clone {
            // TODO: reserve room in `self.indexes`.
            self.somes.reserve_for(selves.map(|x| x.somes));
        }
    }

    impl<'a, TC: crate::AsBytes<'a>, CC: crate::AsBytes<'a>, VC: crate::AsBytes<'a>> crate::AsBytes<'a> for Options<TC, CC, VC, &'a u64> {
        #[inline(always)]
        fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
            crate::chain(self.indexes.as_bytes(), self.somes.as_bytes())
        }
    }

    impl <'a, TC: crate::FromBytes<'a>, CC: crate::FromBytes<'a>, VC: crate::FromBytes<'a>> crate::FromBytes<'a> for Options<TC, CC, VC, &'a u64> {
        #[inline(always)]
        fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
            Self {
                indexes: crate::FromBytes::from_bytes(bytes),
                somes: crate::FromBytes::from_bytes(bytes),
            }
        }
    }

    impl<T, CC, VC: Len, WC: CopyAs<u64>> Len for Options<T, CC, VC, WC> {
        #[inline(always)] fn len(&self) -> usize { self.indexes.len() }
    }

    impl<TC: Index, CC: IndexAs<u64> + Len, VC: IndexAs<u64> + Len, WC: CopyAs<u64>> Index for Options<TC, CC, VC, WC> {
        type Ref = Option<TC::Ref>;
        #[inline(always)]
        fn get(&self, index: usize) -> Self::Ref {
            if self.indexes.get(index) {
                Some(self.somes.get(self.indexes.rank(index)))
            } else {
                None
            }
        }
    }
    impl<'a, TC, CC: IndexAs<u64> + Len, VC: IndexAs<u64> + Len, WC: CopyAs<u64>> Index for &'a Options<TC, CC, VC, WC>
    where &'a TC: Index
    {
        type Ref = Option<<&'a TC as Index>::Ref>;
        #[inline(always)]
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
        #[inline(always)]
        fn get_mut(&mut self, index: usize) -> Self::IndexMut<'_> {
            if self.indexes.get(index) {
                Some(self.somes.get_mut(self.indexes.rank(index)))
            } else {
                None
            }
        }
    }

    impl<T, TC: Push<T> + Len> Push<Option<T>> for Options<TC> {
        #[inline]
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
        #[inline]
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
            assert!((&store).index_iter().zip(0..100).all(|(a, b)| a == Some(&b)));
            assert_eq!(store.heap_size(), (408, 544));
        }

        #[test]
        fn round_trip_none() {
            let store = Columnar::into_columns((0..100).map(|_x| None::<i32>));
            assert_eq!(store.len(), 100);
            let foo = &store;
            assert!(foo.index_iter().zip(0..100).all(|(a, _b)| a == None));
            assert_eq!(store.heap_size(), (8, 32));
        }

        #[test]
        fn round_trip_mixed() {
            // Type annotation is important to avoid some inference overflow.
            let store: Options<Vec<i32>>  = Columnar::into_columns((0..100).map(|x| if x % 2 == 0 { Some(x) } else { None }));
            assert_eq!(store.len(), 100);
            assert!((&store).index_iter().zip(0..100).all(|(a, b)| a == if b % 2 == 0 { Some(&b) } else { None }));
            assert_eq!(store.heap_size(), (208, 288));
        }
    }
}
