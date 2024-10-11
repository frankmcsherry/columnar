/// Methods to convert containers to and from byte slices.

pub trait AsBytes {
    type Borrowed<'a>: FromBytes<'a>;
    /// Presents `self` as a sequence of byte slices, with their required alignment.
    fn as_bytes(&self) -> impl Iterator<Item=(u64, &[u8])>;
}
pub trait FromBytes<'a> : AsBytes {
    /// Reconstructs `self` from a sequence of correctly aligned and sized bytes slices.
    ///
    /// The implementation is expected to consume the right number of items from the iterator,
    /// which may go on to be used by other implementations of `FromBytes`.
    fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self;
}

macro_rules! implement_byteslices {
    ($($index_type:ty),*) => { $(
        impl AsBytes for Vec<$index_type> {
            type Borrowed<'a> = &'a [$index_type];
            fn as_bytes(&self) -> impl Iterator<Item=(u64, &[u8])> {
                std::iter::once((std::mem::align_of::<$index_type>() as u64, bytemuck::cast_slice(&self[..])))
            }
        }
        impl<'a> AsBytes for &'a [$index_type] {
            type Borrowed<'b> = &'b [$index_type];
            fn as_bytes(&self) -> impl Iterator<Item=(u64, &[u8])> {
                std::iter::once((std::mem::align_of::<$index_type>() as u64, bytemuck::cast_slice(&self[..])))
            }
        }
        impl<'a> FromBytes<'a> for &'a [$index_type] {
            fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
                bytemuck::try_cast_slice(bytes.next().unwrap()).unwrap()
            }
        }
    )* }
}

implement_byteslices!(u8, u16, u32, u64, u128);
implement_byteslices!(i8, i16, i32, i64, i128);
implement_byteslices!(f32, f64);
implement_byteslices!(());

use crate::{Strings, Vecs, Results, Options, RankSelect};

impl<BC: AsBytes, VC: AsBytes> AsBytes for Strings<BC, VC> {
    type Borrowed<'a> = Strings<BC::Borrowed<'a>, VC::Borrowed<'a>>;
    fn as_bytes(&self) -> impl Iterator<Item=(u64, &[u8])> {
        self.bounds.as_bytes().chain(self.values.as_bytes())
    }
}
impl<'a, BC: FromBytes<'a>, VC: FromBytes<'a>> FromBytes<'a> for Strings<BC, VC> {
    fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
        Self {
            bounds: FromBytes::from_bytes(bytes),
            values: FromBytes::from_bytes(bytes),
        }
    }
}

impl<TC: AsBytes, BC: AsBytes> AsBytes for Vecs<TC, BC> {
    type Borrowed<'a> = Vecs<TC::Borrowed<'a>, BC::Borrowed<'a>>;
    fn as_bytes(&self) -> impl Iterator<Item=(u64, &[u8])> {
        self.bounds.as_bytes().chain(self.values.as_bytes())
    }
}
impl<'a, TC: FromBytes<'a>, BC: FromBytes<'a>> FromBytes<'a> for Vecs<TC, BC> {
    fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
        Self {
            bounds: FromBytes::from_bytes(bytes),
            values: FromBytes::from_bytes(bytes),
        }
    }
}

macro_rules! tuple_impl {
    ( $($name:ident)+) => (
        impl<$($name: AsBytes),*> AsBytes for ($($name,)*) {
            type Borrowed<'a> = ($($name::Borrowed<'a>,)*);
            #[allow(non_snake_case)]
            fn as_bytes(&self) -> impl Iterator<Item=(u64, &[u8])> {
                let ($($name,)*) = self;
                let iter = None.into_iter();
                $( let iter = iter.chain($name.as_bytes()); )*
                iter
            }
        }
        impl<'a, $($name: FromBytes<'a>),*> FromBytes<'a> for ($($name,)*) {
            #[allow(non_snake_case)]
            fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
                $(let $name = FromBytes::from_bytes(bytes);)*
                ($($name,)*)
            }
        }
    )
}

tuple_impl!(A);
tuple_impl!(A B);
tuple_impl!(A B C);
tuple_impl!(A B C D);
tuple_impl!(A B C D E);
tuple_impl!(A B C D E F);
tuple_impl!(A B C D E F G);
tuple_impl!(A B C D E F G H);
tuple_impl!(A B C D E F G H I);
tuple_impl!(A B C D E F G H I J);

impl AsBytes for crate::primitive::Empties {
    type Borrowed<'a> = crate::primitive::Empties;
    fn as_bytes(&self) -> impl Iterator<Item=(u64, &[u8])> {
        std::iter::once((1, bytemuck::cast_slice(std::slice::from_ref(&self.count))))
    }
}
impl<'a> FromBytes<'a> for crate::primitive::Empties {
    fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
        Self { count: bytemuck::try_cast_slice(bytes.next().unwrap()).unwrap()[0], empty: () }
    }
}

impl<VC: AsBytes> AsBytes for crate::primitive::Bools<VC> {
    type Borrowed<'a> = crate::primitive::Bools<VC::Borrowed<'a>>;
    fn as_bytes(&self) -> impl Iterator<Item=(u64, &[u8])> {
        self.values.as_bytes()
        .chain(std::iter::once((std::mem::align_of::<u64>() as u64, bytemuck::cast_slice(std::slice::from_ref(&self.last_word)))))
        .chain(std::iter::once((1, bytemuck::cast_slice(std::slice::from_ref(&self.last_bits)))))
    }
}

impl<'a, VC: FromBytes<'a>> FromBytes<'a> for crate::primitive::Bools<VC> {
    fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
        let values = FromBytes::from_bytes(bytes);
        let last_word = bytemuck::try_cast_slice(bytes.next().unwrap()).unwrap()[0];
        let last_bits = bytemuck::try_cast_slice(bytes.next().unwrap()).unwrap()[0];
        Self { values, last_word, last_bits }
    }
}

impl<SC: AsBytes, NC: AsBytes> AsBytes for crate::primitive::Durations<SC, NC> {
    type Borrowed<'a> = crate::primitive::Durations<SC::Borrowed<'a>, NC::Borrowed<'a>>;
    fn as_bytes(&self) -> impl Iterator<Item=(u64, &[u8])> {
        self.seconds.as_bytes().chain(self.nanoseconds.as_bytes())
    }
}
impl<'a, SC: FromBytes<'a>, NC: FromBytes<'a>> FromBytes<'a> for crate::primitive::Durations<SC, NC> {
    fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
        Self {
            seconds: FromBytes::from_bytes(bytes),
            nanoseconds: FromBytes::from_bytes(bytes),
        }
    }
}

impl<CC: AsBytes, VC: AsBytes> AsBytes for RankSelect<CC, VC> {
    type Borrowed<'a> = RankSelect<CC::Borrowed<'a>, VC::Borrowed<'a>>;
    fn as_bytes(&self) -> impl Iterator<Item=(u64, &[u8])> {
        self.counts.as_bytes().chain(self.values.as_bytes())
    }
}
impl<'a, CC: FromBytes<'a>, VC: FromBytes<'a>> FromBytes<'a> for RankSelect<CC, VC> {
    fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
        Self {
            counts: FromBytes::from_bytes(bytes),
            values: FromBytes::from_bytes(bytes),
        }
    }
}

impl<SC: AsBytes, TC: AsBytes, CC: AsBytes, VC: AsBytes> AsBytes for Results<SC, TC, CC, VC> {
    type Borrowed<'a> = Results<SC::Borrowed<'a>, TC::Borrowed<'a>, CC::Borrowed<'a>, VC::Borrowed<'a>>;
    fn as_bytes(&self) -> impl Iterator<Item=(u64, &[u8])> {
        self.indexes.as_bytes().chain(self.oks.as_bytes()).chain(self.errs.as_bytes())
    }
}
impl<'a, SC: FromBytes<'a>, TC: FromBytes<'a>, CC: FromBytes<'a>, VC: FromBytes<'a>> FromBytes<'a> for Results<SC, TC, CC, VC> {
    fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
        Self {
            indexes: FromBytes::from_bytes(bytes),
            oks: FromBytes::from_bytes(bytes),
            errs: FromBytes::from_bytes(bytes),
        }
    }
}

impl <TC: AsBytes, CC: AsBytes, VC: AsBytes> AsBytes for Options<TC, CC, VC> {
    type Borrowed<'a> = Options<TC::Borrowed<'a>, CC::Borrowed<'a>, VC::Borrowed<'a>>;
    fn as_bytes(&self) -> impl Iterator<Item=(u64, &[u8])> {
        self.indexes.as_bytes().chain(self.somes.as_bytes())
    }
}

impl <'a, TC: FromBytes<'a>, CC: FromBytes<'a>, VC: FromBytes<'a>> FromBytes<'a> for Options<TC, CC, VC> {
    fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
        Self {
            indexes: FromBytes::from_bytes(bytes),
            somes: FromBytes::from_bytes(bytes),
        }
    }
}

/// A sequential byte layout for `AsBytes` and `FromBytes` implementors.
///
/// The layout is aligned like a sequence of `u64`, where we repeatedly announce a length,
/// and then follow it by that many bytes. We may need to follow this with padding bytes.
pub mod serialization {

    /// Encodes a sequence of byte slices as their length followed by their bytes, aligned to 8 bytes.
    ///
    /// Each length will be exactly 8 bytes, and the bytes that follow are padded out to a multiple of 8 bytes.
    /// When reading the data, the length is in bytes, and one should consume those bytes and advance over padding.
    pub fn encode<'a>(store: &mut Vec<u64>, bytes: impl Iterator<Item=(u64, &'a [u8])>) {
        for (align, bytes) in bytes {
            assert!(align <= 8);
            store.push(bytes.len() as u64);
            let whole_words = 8 * (bytes.len() / 8);
            store.extend(bytemuck::try_cast_slice(&bytes[.. whole_words]).unwrap());
            let remaining_bytes = &bytes[whole_words..];
            if !remaining_bytes.is_empty() {
                let mut remainder = [0u8; 8];
                for (i, byte) in remaining_bytes.iter().enumerate() {
                    remainder[i] = *byte;
                }
                store.push(bytemuck::try_cast_slice(&remainder).unwrap()[0]);
            }
        }
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
                let bytes: &[u8] = bytemuck::try_cast_slice(&self.store[..whole_words]).unwrap();
                self.store = &self.store[whole_words..];
                Some(&bytes[..length])
            } else {
                None
            }
        }
    }

    /// A wrapper for binary data that can be interpreted as a columnar container type `C`.
    ///
    /// The binary data must present as a `[u64]`, ensuring the appropriate alignment.
    pub struct ColumnarBytes<B, C> {
        bytes: B,
        phantom: std::marker::PhantomData<C>,
    }

    use crate::bytes::{AsBytes, FromBytes};

    impl<B, C> ColumnarBytes<B, C>
    where
        B: std::ops::Deref<Target = [u64]>,
        C: AsBytes,
    {
        /// Presents the binary data as a columnar wrapper for type `C`.
        pub fn decode(&self) -> C::Borrowed<'_> {
            FromBytes::from_bytes(&mut decode(&self.bytes))
        }
    }
}


#[cfg(test)]
mod test {
    #[test]
    fn round_trip() {

        use crate::Columnar;
        use crate::common::{Index, Push, HeapSize, Len};
        use crate::bytes::{AsBytes, FromBytes};

        let mut column: <Result<u64, u64> as Columnar>::Container = Default::default();
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

        let column2 = crate::Results::<&[u64], &[u64], &[u64], &[u64]>::from_bytes(&mut column.as_bytes().map(|(_, bytes)| bytes));
        for i in 0..100 {
            assert_eq!(column.get(2*i+0), column2.get(2*i+0).copied().map_err(|e| *e));
            assert_eq!(column.get(2*i+1), column2.get(2*i+1).copied().map_err(|e| *e));
        }

        let column3 = crate::Results::<&[u64], &[u64], &[u64], &[u64]>::from_bytes(&mut column2.as_bytes().map(|(_, bytes)| bytes));
        for i in 0..100 {
            assert_eq!(column3.get(2*i+0), column2.get(2*i+0));
            assert_eq!(column3.get(2*i+1), column2.get(2*i+1));
        }
    }
}
