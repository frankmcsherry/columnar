//! Logic related to the transformation to and from bytes.
//!
//! The methods here line up with the `AsBytes` and `FromBytes` traits.
//!
//! The encoding uses an index of byte offsets prepended to the data, enabling
//! random access to individual byte slices and `u64`-aligned decoding.
//!
//! The most reliable entry point to the read side of this functionality is the `Stash` type,
//! which can be formed from any type that implements `Deref<Target=[u8]>`. Doing so will check
//! `u64` alignment, copy the contents if misaligned, and perform some structural validation.

/// A binary encoding of sequences of byte slices.
///
/// The encoding starts with a sequence of n+1 offsets describing where to find the n slices in the bytes that follow.
/// Treating the offsets as a byte slice too, each offset indicates the location (in bytes) of the end of its slice.
/// Each byte slice can be found from a pair of adjacent offsets, where the first is rounded up to a multiple of eight.
/// This means that slices that are not multiples of eight bytes may leave unread bytes at their end, which is fine.
pub mod indexed {

    use crate::AsBytes;

    /// Encoded length in number of `u64` words required.
    pub fn length_in_words<'a, A>(bytes: &A) -> usize where A : AsBytes<'a> {
        1 + bytes.as_bytes().map(|(_align, bytes)| 1 + bytes.len().div_ceil(8)).sum::<usize>()
    }
    /// Encoded length in number of `u8` bytes required.
    pub fn length_in_bytes<'a, A>(bytes: &A) -> usize where A : AsBytes<'a> { 8 * length_in_words(bytes) }

    /// Encodes `item` into `u64` aligned words.
    ///
    /// The sequence of byte slices are appended, with padding to have each slice start `u64` aligned.
    /// The sequence is then pre-pended with as many byte offsets as there are slices in `item`, plus one.
    /// The byte offsets indicate where each slice ends, and by rounding up to `u64` alignemnt where the next slice begins.
    /// The first offset indicates where the list of offsets itself ends, and where the first slice begins.
    ///
    /// We will need to visit `as_bytes` three times to extract this information, so the method should be efficient and inlined.
    /// The first read writes the first offset, the second writes each other offset, and the third writes the bytes themselves.
    ///
    /// The offsets are zero-based, rather than based on `store.len()`.
    /// If you call the method with a non-empty `store` be careful decoding.
    pub fn encode<'a, A>(store: &mut Vec<u64>, iter: &A)
    where A : AsBytes<'a>,
    {
        // Read 1: Number of offsets we will record, equal to the number of slices plus one.
        // TODO: right-size `store` before first call to `push`.
        let offsets = 1 + iter.as_bytes().count();
        let offsets_end: u64 = TryInto::<u64>::try_into((offsets) * std::mem::size_of::<u64>()).unwrap();
        store.push(offsets_end);
        // Read 2: Establish each of the offsets based on lengths of byte slices.
        let mut position_bytes = offsets_end;
        for (align, bytes) in iter.as_bytes() {
            assert!(align <= 8);
            // Write length in bytes, but round up to words before updating `position_bytes`.
            let to_push: u64 = position_bytes + TryInto::<u64>::try_into(bytes.len()).unwrap();
            store.push(to_push);
            let round_len: u64 = ((bytes.len() + 7) & !7).try_into().unwrap();
            position_bytes += round_len;
        }
        // Read 3: Append each byte slice, with padding to align starts to `u64`.
        for (_align, bytes) in iter.as_bytes() {
            let whole_words = 8 * (bytes.len() / 8);
            // We want to extend `store` by `bytes`, but `bytes` may not be `u64` aligned.
            // In the latter case, init `store` and cast and copy onto it as a byte slice.
            if let Ok(words) = bytemuck::try_cast_slice(&bytes[.. whole_words]) {
                store.extend_from_slice(words);
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

    pub fn write<'a, A, W>(mut writer: W, iter: &A) -> std::io::Result<()>
    where
        A: AsBytes<'a>,
        W: std::io::Write,
    {
        // Read 1: Number of offsets we will record, equal to the number of slices plus one.
        let offsets = 1 + iter.as_bytes().count();
        let offsets_end: u64 = TryInto::<u64>::try_into((offsets) * std::mem::size_of::<u64>()).unwrap();
        writer.write_all(bytemuck::cast_slice(std::slice::from_ref(&offsets_end)))?;
        // Read 2: Establish each of the offsets based on lengths of byte slices.
        let mut position_bytes = offsets_end;
        for (align, bytes) in iter.as_bytes() {
            assert!(align <= 8);
            // Write length in bytes, but round up to words before updating `position_bytes`.
            let to_push: u64 = position_bytes + TryInto::<u64>::try_into(bytes.len()).unwrap();
            writer.write_all(bytemuck::cast_slice(std::slice::from_ref(&to_push)))?;
            let round_len: u64 = ((bytes.len() + 7) & !7).try_into().unwrap();
            position_bytes += round_len;
        }
        // Read 3: Append each byte slice, with padding to align starts to `u64`.
        for (_align, bytes) in iter.as_bytes() {
            writer.write_all(bytes)?;
            let padding = ((bytes.len() + 7) & !7) - bytes.len();
            if padding > 0 {
                writer.write_all(&[0u8;8][..padding])?;
            }
        }

        Ok(())
    }

    /// Decodes an encoded sequence of byte slices. Each result will be `u64` aligned.
    #[inline(always)]
    pub fn decode(store: &[u64]) -> impl Iterator<Item=&[u8]> {
        let slices = store[0] as usize / 8 - 1;
        let index = &store[..slices + 1];
        let last = index[slices] as usize;
        let bytes: &[u8] = &bytemuck::cast_slice(store)[..last];
        (0 .. slices).map(move |i| {
            let upper = (index[i + 1] as usize).min(last);
            let lower = (((index[i] as usize) + 7) & !7).min(upper);
            &bytes[lower .. upper]
        })
    }


    /// A zero-allocation view into indexed-encoded data, providing random access to individual slices.
    ///
    /// Constructed from `&[u64]` in O(1), this wraps the offset index and data region
    /// and provides `get(k)` to retrieve the k-th slice as `(&[u64], u8)`.
    /// Each access is independent — no iterator state — enabling LLVM to eliminate
    /// unused field lookups entirely.
    #[derive(Copy, Clone)]
    pub struct DecodedStore<'a> {
        /// The offset index: `index[0]` is the byte offset where data starts,
        /// `index[k+1]` is the byte offset where slice k ends.
        index: &'a [u64],
        /// The data region, pre-sliced to include only valid words.
        words: &'a [u64],
    }

    impl<'a> DecodedStore<'a> {
        /// Creates a decoded view of an indexed-encoded `&[u64]` store.
        ///
        /// This is O(1) — it just reads the first offset to locate the index
        /// and data regions. No allocation, no iteration.
        #[inline(always)]
        pub fn new(store: &'a [u64]) -> Self {
            let slices = store.first().copied().unwrap_or(0) as usize / 8;
            debug_assert!(slices <= store.len(), "DecodedStore::new: slice count {slices} exceeds store length {}", store.len());
            let index = store.get(..slices).unwrap_or(&[]);
            let last = index.last().copied().unwrap_or(0) as usize;
            let last_w = (last + 7) / 8;
            debug_assert!(last_w <= store.len(), "DecodedStore::new: last word offset {last_w} exceeds store length {}", store.len());
            let words = store.get(..last_w).unwrap_or(&[]);
            Self { index, words }
        }
        /// Returns the k-th slice as `(&[u64], u8)`.
        ///
        /// The `u8` is the number of valid trailing bytes in the last word
        /// (0 means all 8 are valid). Returns an empty slice for out-of-bounds access.
        #[inline(always)]
        pub fn get(&self, k: usize) -> (&'a [u64], u8) {
            debug_assert!(k + 1 < self.index.len(), "DecodedStore::get: index {k} out of bounds (len {})", self.index.len().saturating_sub(1));
            let upper = (*self.index.get(k + 1).unwrap_or(&0) as usize)
                .min(self.words.len() * 8);
            let lower = (((*self.index.get(k).unwrap_or(&0) as usize) + 7) & !7)
                .min(upper);
            let upper_w = ((upper + 7) / 8).min(self.words.len());
            let lower_w = (lower / 8).min(upper_w);
            let tail = (upper % 8) as u8;
            (self.words.get(lower_w..upper_w).unwrap_or(&[]), tail)
        }
        /// The number of slices in the store.
        #[inline(always)]
        pub fn len(&self) -> usize {
            self.index.len().saturating_sub(1)
        }
    }

    /// Validates the internal structure of indexed-encoded data.
    ///
    /// Checks that offsets are well-formed, in bounds, and that the slice count matches
    /// `expected_slices`. This is a building block for [`validate`]; prefer calling
    /// `validate` directly unless you need structural checks alone.
    pub fn validate_structure(store: &[u64], expected_slices: usize) -> Result<(), String> {
        if store.is_empty() {
            return Err("store is empty".into());
        }
        let first = store[0] as usize;
        if first % 8 != 0 {
            return Err(format!("first offset {} is not a multiple of 8", first));
        }
        let slices = first / 8 - 1;
        if slices + 1 > store.len() {
            return Err(format!("index requires {} words but store has {}", slices + 1, store.len()));
        }
        if slices != expected_slices {
            return Err(format!("expected {} slices but found {}", expected_slices, slices));
        }
        let store_bytes = store.len() * 8;
        let mut prev_upper = first;
        for i in 0..slices {
            let offset = store[i + 1] as usize;
            if offset > store_bytes {
                return Err(format!("slice {} offset {} exceeds store size {}", i, offset, store_bytes));
            }
            if offset < prev_upper {
                return Err(format!("slice {} offset {} precedes previous end {}", i, offset, prev_upper));
            }
            // Advance prev_upper to the aligned start of the next slice.
            prev_upper = (offset + 7) & !7;
        }
        Ok(())
    }

    /// Validates that `store` contains well-formed data compatible with type `T`.
    ///
    /// Checks both the internal structure of the encoding (offsets, slice count) and
    /// type-level compatibility (each slice's byte length is a multiple of its element
    /// size). Call this once at trust boundaries when receiving encoded data.
    ///
    /// The `from_store` decode path performs no further validation at access time:
    /// it will not panic on malformed data, but may return incorrect results.
    /// There is no undefined behavior in any case. Call this method once before
    /// using `from_store` to ensure the data is well-formed.
    ///
    /// ```ignore
    /// type B<'a> = <MyContainer as Borrow>::Borrowed<'a>;
    /// indexed::validate::<B>(&store)?;
    /// // Now safe to use the non-panicking path:
    /// let ds = indexed::DecodedStore::new(&store);
    /// let borrowed = B::from_store(&ds, &mut 0);
    /// ```
    pub fn validate<'a, T: crate::FromBytes<'a>>(store: &[u64]) -> Result<(), String> {
        validate_structure(store, T::SLICE_COUNT)?;
        let ds = DecodedStore::new(store);
        let slices: Vec<_> = (0..ds.len()).map(|i| ds.get(i)).collect();
        T::validate(&slices)
    }

    /// Decodes a specific byte slice by index. It will be `u64` aligned.
    #[inline(always)]
    pub fn decode_index(store: &[u64], index: u64) -> &[u8] {
        let index = index as usize;
        let bytes: &[u8] = bytemuck::cast_slice(store);
        let upper = (store[index + 1] as usize).min(bytes.len());
        let lower = (((store[index] as usize) + 7) & !7).min(upper);
        &bytes[lower .. upper]
    }

    #[cfg(test)]
    mod test {

        use crate::{Borrow, ContainerOf};
        use crate::common::Push;
        use crate::AsBytes;

        use super::{encode, decode};

        fn assert_roundtrip<'a, AB: AsBytes<'a>>(item: &AB) {
            let mut store = Vec::new();
            encode(&mut store, item);
            assert!(item.as_bytes().map(|x| x.1).eq(decode(&store)));
        }

        #[test]
        fn round_trip() {

            let mut column: ContainerOf<Result<u64, String>> = Default::default();
            for i in 0..10000u64 {
                column.push(&Ok::<u64, String>(i));
                column.push(&Err::<u64, String>(format!("{:?}", i)));
            }

            assert_roundtrip(&column.borrow());
        }

        #[test]
        fn validate_well_formed() {
            use crate::common::Push;

            let mut column: ContainerOf<(u64, u64, u64)> = Default::default();
            for i in 0..100u64 { column.push(&(i, i+1, i+2)); }
            let mut store = Vec::new();
            encode(&mut store, &column.borrow());

            type B<'a> = crate::BorrowedOf<'a, (u64, u64, u64)>;
            assert!(super::validate::<B>(&store).is_ok());

            // Wrong slice count should fail structural validation.
            assert!(super::validate_structure(&store, 5).is_err());
        }

        #[test]
        fn validate_mixed_types() {
            use crate::common::Push;

            let mut column: ContainerOf<(u64, String, Vec<u32>)> = Default::default();
            for i in 0..50u64 {
                column.push(&(i, format!("hello {i}"), vec![i as u32; i as usize]));
            }
            let mut store = Vec::new();
            encode(&mut store, &column.borrow());

            type B<'a> = crate::BorrowedOf<'a, (u64, String, Vec<u32>)>;
            assert!(super::validate::<B>(&store).is_ok());
        }
    }
}

/// A container of either typed columns, or serialized bytes that can be borrowed as the former.
pub mod stash {

    use crate::{Len, FromBytes};
    /// A container of either typed columns, or serialized bytes that can be borrowed as the former.
    ///
    /// When `B` dereferences to a byte slice, the container can be borrowed as if the container type `C`.
    /// This container inherents the readable properties of `C` through borrowing, but does not implement
    /// the traits itself.
    ///
    /// The container can be cleared and pushed into. When cleared it reverts to a typed variant, and when
    /// pushed into if the typed variant it will accept the item, and if not it will panic.
    ///
    /// The best ways to construct a `Stash` is with either the `Default` implementation to get an empty
    /// writeable version, or with the `try_from_bytes` method that attempts to install a type that dereferences
    /// to a byte slice in the `Bytes` variant, after validating some structural properties.
    ///
    /// One can form a `Stash` directly by loading the variants, which are public. Do so with care,
    /// as loading mis-aligned `B` into the `Bytes` variant can result in a run-time panic, and
    /// loading structurally invalid data into either the `Bytes` or `Align` variant can produce
    /// incorrect results at runtime (clamped index accesses, for example). The validation does not
    /// confirm that the internal structure of types are valid, for example that all vector bounds
    /// are in-bounds for their values, and these may result in panics at runtime for invalid data.
    #[derive(Clone)]
    pub enum Stash<C, B> {
        /// The typed variant of the container.
        Typed(C),
        /// The bytes variant of the container.
        Bytes(B),
        /// Relocated, aligned binary data, if `Bytes` doesn't work for some reason.
        ///
        /// Most commonly this works around misaligned binary data, but it can also be useful if the `B`
        /// type is a scarce resource that should be released.
        Align(Box<[u64]>),
    }

    impl<C: Default, B> Default for Stash<C, B> { fn default() -> Self { Self::Typed(Default::default()) } }

    impl<C: crate::ContainerBytes, B: std::ops::Deref<Target = [u8]>> Stash<C, B> {
        /// An analogue of `TryFrom` for any `B: Deref<Target=[u8]>`, avoiding coherence issues.
        ///
        /// This is the recommended way to form a `Stash`, as it performs certain structural validation
        /// steps that the stash will then skip in future borrowing and indexing operations. If the data
        /// are structurally invalid, e.g. the wrong framing header, the wrong number of slices for `C`,
        /// this will return an error. If this returns a `Stash` then all accesses that do not panic should
        /// be correct. The resulting `Stash` may still panic if the internal structure of the data
        /// are inconsistent, for example if any vector bounds are out-of-bounds for their values slice.
        ///
        /// There is no `unsafe` that is called through this type, and invalid data can result in panics
        /// or incorrect results, but not undefined behavior.
        ///
        /// # Example
        ///
        /// ```rust
        /// use columnar::{Columnar, Borrow, ContainerOf};
        /// use columnar::common::{Push, Index};
        /// use columnar::bytes::stash::Stash;
        ///
        /// // Build a typed container and populate it.
        /// let mut stash: Stash<ContainerOf<(u64, String)>, Vec<u8>> = Default::default();
        /// stash.push(&(0u64, format!("hello")));
        /// stash.push(&(1u64, format!("world")));
        ///
        /// // Serialize to bytes.
        /// let mut bytes: Vec<u8> = Vec::new();
        /// stash.into_bytes(&mut bytes);
        ///
        /// // Reconstruct from bytes, with validation.
        /// let stash: Stash<ContainerOf<(u64, String)>, Vec<u8>> =
        ///     Stash::try_from_bytes(bytes).expect("valid data");
        ///
        /// // Borrow and index into individual columns.
        /// let borrowed = stash.borrow();
        /// assert_eq!(*Index::get(&borrowed.0, 0), 0u64);
        /// assert_eq!(borrowed.1.get(1), "world");
        /// ```
        pub fn try_from_bytes(bytes: B) -> Result<Self, String> {
            use crate::bytes::indexed::validate;
            use crate::Borrow;
            if !(bytes.len() % 8 == 0) { return Err(format!("bytes.len() = {:?} not a multiple of 8", bytes.len())) }
            if let Ok(words) = bytemuck::try_cast_slice::<_, u64>(&bytes) {
                validate::<<C as Borrow>::Borrowed<'_>>(words)?;
                Ok(Self::Bytes(bytes))
            }
            else {
                // Re-locating bytes for alignment reasons.
                let mut alloc: Vec<u64> = vec![0; bytes.len() / 8];
                bytemuck::cast_slice_mut(&mut alloc[..]).copy_from_slice(&bytes[..]);
                validate::<<C as Borrow>::Borrowed<'_>>(&alloc)?;
                Ok(Self::Align(alloc.into()))
            }
        }
    }

    impl<C: crate::ContainerBytes, B: std::ops::Deref<Target=[u8]> + Clone + 'static> crate::Borrow for Stash<C, B> {

        type Ref<'a> = <C as crate::Borrow>::Ref<'a>;
        type Borrowed<'a> = <C as crate::Borrow>::Borrowed<'a>;

        #[inline(always)] fn borrow<'a>(&'a self) -> Self::Borrowed<'a> { self.borrow() }
        #[inline(always)] fn reborrow<'b, 'a: 'b>(item: Self::Borrowed<'a>) -> Self::Borrowed<'b> where Self: 'a { <C as crate::Borrow>::reborrow(item) }
        #[inline(always)] fn reborrow_ref<'b, 'a: 'b>(item: Self::Ref<'a>) -> Self::Ref<'b> where Self: 'a { <C as crate::Borrow>::reborrow_ref(item) }
    }

    impl<C: crate::ContainerBytes, B: std::ops::Deref<Target=[u8]>> Len for Stash<C, B> {
        #[inline(always)] fn len(&self) -> usize { self.borrow().len() }
    }

    impl<C: crate::ContainerBytes, B: std::ops::Deref<Target=[u8]>> Stash<C, B> {
        /// Borrows the contents, either from a typed container or by decoding serialized bytes.
        ///
        /// This method is relatively cheap but is not free.
        #[inline(always)] pub fn borrow<'a>(&'a self) -> <C as crate::Borrow>::Borrowed<'a> {
            match self {
                Stash::Typed(t) => t.borrow(),
                Stash::Bytes(b) => {
                    let store = crate::bytes::indexed::DecodedStore::new(bytemuck::cast_slice(b));
                    <C::Borrowed<'_> as FromBytes>::from_store(&store, &mut 0)
                },
                Stash::Align(a) => {
                    let store = crate::bytes::indexed::DecodedStore::new(a);
                    <C::Borrowed<'_> as FromBytes>::from_store(&store, &mut 0)
                },
            }
        }
        /// The number of bytes needed to write the contents using the [`indexed`] encoder.
        pub fn length_in_bytes(&self) -> usize {
            match self {
                // We'll need one u64 for the length, then the length rounded up to a multiple of 8.
                Stash::Typed(t) => crate::bytes::indexed::length_in_bytes(&t.borrow()),
                Stash::Bytes(b) => b.len(),
                Stash::Align(a) => 8 * a.len(),
            }
        }
        /// Write the contents into a `std::io::Write` using the [`indexed`] encoder.
        pub fn into_bytes<W: ::std::io::Write>(&self, writer: &mut W) {
            match self {
                Stash::Typed(t) => { crate::bytes::indexed::write(writer, &t.borrow()).unwrap() },
                Stash::Bytes(b) => writer.write_all(&b[..]).unwrap(),
                Stash::Align(a) => writer.write_all(bytemuck::cast_slice(&a[..])).unwrap(),
            }
        }
    }

    impl<T, C: crate::Container + crate::Push<T>, B> crate::Push<T> for Stash<C, B> {
        fn push(&mut self, item: T) {
            match self {
                Stash::Typed(t) => t.push(item),
                Stash::Bytes(_) | Stash::Align(_) => unimplemented!(),
            }
        }
    }

    impl<C: crate::Clear + Default, B> crate::Clear for Stash<C, B> {
        fn clear(&mut self) {
            match self {
                Stash::Typed(t) => t.clear(),
                Stash::Bytes(_) | Stash::Align(_) => {
                    *self = Stash::Typed(Default::default());
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::ContainerOf;

    #[test]
    fn round_trip() {

        use crate::common::{Push, HeapSize, Len, Index};
        use crate::{Borrow, AsBytes, FromBytes};

        let mut column: ContainerOf<Result<u64, u64>> = Default::default();
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

        // Test from_store round-trip.
        let mut store = Vec::new();
        crate::bytes::indexed::encode(&mut store, &column.borrow());
        let ds = crate::bytes::indexed::DecodedStore::new(&store);
        let column4 = crate::Results::<&[u64], &[u64], &[u64], &[u64], &u64>::from_store(&ds, &mut 0);
        for i in 0..100 {
            assert_eq!(column.get(2*i+0), column4.get(2*i+0).copied().map_err(|e| *e));
            assert_eq!(column.get(2*i+1), column4.get(2*i+1).copied().map_err(|e| *e));
        }
    }

    /// Test from_store for tuples.
    #[test]
    fn from_store_tuple() {
        use crate::common::{Push, Index};
        use crate::{Borrow, FromBytes, ContainerOf};

        let mut column: ContainerOf<(u64, String, Vec<u32>)> = Default::default();
        for i in 0..50u64 {
            column.push(&(i, format!("hello {i}"), vec![i as u32; i as usize]));
        }

        let mut store = Vec::new();
        crate::bytes::indexed::encode(&mut store, &column.borrow());
        let ds = crate::bytes::indexed::DecodedStore::new(&store);
        type Borrowed<'a> = crate::BorrowedOf<'a, (u64, String, Vec<u32>)>;
        let reconstructed = Borrowed::from_store(&ds, &mut 0);
        for i in 0..50 {
            let (a, b, _c) = reconstructed.get(i);
            assert_eq!(*a, i as u64);
            assert_eq!(b, &*format!("hello {i}"));
        }
    }

}
