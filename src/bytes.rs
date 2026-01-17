//! Logic related to the transformation to and from bytes.
//!
//! The methods here line up with the `AsBytes` and `FromBytes` traits.

use crate::AsBytes;

/// A coupled encode/decode pair for byte sequences.
pub trait EncodeDecode {
    /// Encoded length in number of `u64` words required.
    fn length_in_words<'a, A>(bytes: &A) -> usize where A : AsBytes<'a>;
    /// Encoded length in number of `u8` bytes required.
    ///
    /// This method should always be eight times `Self::length_in_words`, and is provided for convenience and clarity.
    fn length_in_bytes<'a, A>(bytes: &A) -> usize where A : AsBytes<'a> { 8 * Self::length_in_words(bytes) }
    /// Encodes `bytes` into a sequence of `u64`.
    fn encode<'a, A>(store: &mut Vec<u64>, bytes: &A) where A : AsBytes<'a>;
    /// Writes `bytes` in the encoded format to an arbitrary writer.
    fn write<'a, A, W: std::io::Write>(writer: W, bytes: &A) -> std::io::Result<()> where A : AsBytes<'a>;
    /// Decodes bytes from a sequence of `u64`.
    fn decode(store: &[u64]) -> impl Iterator<Item=&[u8]>;
}

/// A sequential byte layout for `AsBytes` and `FromBytes` implementors.
///
/// The layout is aligned like a sequence of `u64`, where we repeatedly announce a length,
/// and then follow it by that many bytes. We may need to follow this with padding bytes.
pub use serialization::Sequence;
mod serialization {

    use crate::AsBytes;

    /// Encodes and decodes bytes sequences, by prepending the length and appending the all sequences.
    pub struct Sequence;
    impl super::EncodeDecode for Sequence {
        fn length_in_words<'a, A>(bytes: &A) -> usize where A : AsBytes<'a> {
            // Each byte slice has one `u64` for the length, and then as many `u64`s as needed to hold all bytes.
            bytes.as_bytes().map(|(_align, bytes)| 1 + bytes.len().div_ceil(8)).sum()
        }
        fn encode<'a, A>(store: &mut Vec<u64>, bytes: &A) where A : AsBytes<'a> {
            encode(store, bytes.as_bytes())
        }
        fn write<'a, A, W: std::io::Write>(writer: W, bytes: &A) -> std::io::Result<()> where A : AsBytes<'a> {
            write(writer, bytes.as_bytes())
        }
        fn decode(store: &[u64]) -> impl Iterator<Item=&[u8]> {
            decode(store)
        }
    }

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

    /// Writes a sequence of byte slices as their length followed by their bytes, padded to 8 bytes.
    ///
    /// Each length will be exactly 8 bytes, and the bytes that follow are padded out to a multiple of 8 bytes.
    /// When reading the data, the length is in bytes, and one should consume those bytes and advance over padding.
    pub fn write<'a>(mut writer: impl std::io::Write, bytes: impl Iterator<Item=(u64, &'a [u8])>) -> std::io::Result<()> {
        // Columnar data is serialized as a sequence of `u64` values, with each `[u8]` slice
        // serialize as first its length in bytes, and then as many `u64` values as needed.
        // Padding should be added, but only for alignment; no specific values are required.
        for (align, bytes) in bytes {
            assert!(align <= 8);
            let length = u64::try_from(bytes.len()).unwrap();
            writer.write_all(bytemuck::cast_slice(std::slice::from_ref(&length)))?;
            writer.write_all(bytes)?;
            let padding = usize::try_from((8 - (length % 8)) % 8).unwrap();
            writer.write_all(&[0; 8][..padding])?;
        }
        Ok(())
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

/// A binary encoding of sequences of byte slices.
///
/// The encoding starts with a sequence of n+1 offsets describing where to find the n slices in the bytes that follow.
/// Treating the offsets as a byte slice too, the each offset indicates the location (in bytes) of the end of its slice.
/// Each byte slice can be found from a pair of adjacent offsets, where the first is rounded up to a multiple of eight.
pub use serialization_neu::Indexed;
pub mod serialization_neu {

    use crate::AsBytes;

    /// Encodes and decodes bytes sequences, using an index of offsets.
    pub struct Indexed;
    impl super::EncodeDecode for Indexed {
        fn length_in_words<'a, A>(bytes: &A) -> usize where A : AsBytes<'a> {
            1 + bytes.as_bytes().map(|(_align, bytes)| 1 + bytes.len().div_ceil(8)).sum::<usize>()
        }
        fn encode<'a, A>(store: &mut Vec<u64>, bytes: &A) where A : AsBytes<'a> {
            encode(store, bytes)
        }
        fn write<'a, A, W: std::io::Write>(writer: W, bytes: &A) -> std::io::Result<()> where A : AsBytes<'a> {
            write(writer, bytes)
        }
        fn decode(store: &[u64]) -> impl Iterator<Item=&[u8]> {
            decode(store)
        }
    }

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
    pub fn decode(store: &[u64]) -> impl Iterator<Item=&[u8]> {
        assert!(store[0] % 8 == 0);
        let slices = (store[0] / 8) - 1;
        (0 .. slices).map(|i| decode_index(store, i))
    }

    /// Decodes a specific byte slice by index. It will be `u64` aligned.
    #[inline(always)]
    pub fn decode_index(store: &[u64], index: u64) -> &[u8] {
        debug_assert!(index + 1 < store[0]/8);
        let index: usize = index.try_into().unwrap();
        let lower: usize = ((store[index] + 7) & !7).try_into().unwrap();
        let upper: usize = (store[index + 1]).try_into().unwrap();
        let bytes: &[u8] = bytemuck::try_cast_slice(store).expect("&[u64] should convert to &[u8]");
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
    }
}

/// A container of either typed columns, or serialized bytes that can be borrowed as the former.
pub mod stash {

    use crate::{Len, FromBytes};
    use crate::bytes::{EncodeDecode, Indexed};

    /// A container of either typed columns, or serialized bytes that can be borrowed as the former.
    ///
    /// When `B` dereferences to a byte slice, the container can be borrowed as if the container type `C`.
    /// This container inherents the readable properties of `C` through borrowing, but does not implement
    /// the traits itself.
    ///
    /// The container can be cleared and pushed into. When cleared it reverts to a typed variant, and when
    /// pushed into if the typed variant it will accept the item, and if not it will panic.
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
                Stash::Bytes(b) => <C::Borrowed<'_> as FromBytes>::from_bytes(&mut Indexed::decode(bytemuck::cast_slice(b))),
                Stash::Align(a) => <C::Borrowed<'_> as FromBytes>::from_bytes(&mut Indexed::decode(a)),
            }
        }
        /// The number of bytes needed to write the contents using the `Indexed` encoder.
        pub fn length_in_bytes(&self) -> usize {
            match self {
                // We'll need one u64 for the length, then the length rounded up to a multiple of 8.
                Stash::Typed(t) => 8 * Indexed::length_in_words(&t.borrow()),
                Stash::Bytes(b) => b.len(),
                Stash::Align(a) => 8 * a.len(),
            }
        }
        /// Write the contents into a `std::io::Write` using the `Indexed` encoder.
        pub fn into_bytes<W: ::std::io::Write>(&self, writer: &mut W) {
            match self {
                Stash::Typed(t) => { Indexed::write(writer, &t.borrow()).unwrap() },
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

    impl<C: crate::Container, B: std::ops::Deref<Target = [u8]>> From<B> for Stash<C, B> {
        fn from(bytes: B) -> Self {
            assert!(bytes.len() % 8 == 0);
            if bytemuck::try_cast_slice::<_, u64>(&bytes).is_ok() {
                Self::Bytes(bytes)
            }
            else {
                // Re-locating bytes for alignment reasons.
                let mut alloc: Vec<u64> = vec![0; bytes.len() / 8];
                bytemuck::cast_slice_mut(&mut alloc[..]).copy_from_slice(&bytes[..]);
                Self::Align(alloc.into())
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
    }
}
