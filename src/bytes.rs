/// Methods to convert containers to and from byte slices.

pub trait AsBytes {
    /// Presents `self` as a sequence of byte slices, with their required alignment.
    fn as_bytes(&self) -> impl Iterator<Item=(usize, &[u8])>;
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
            fn as_bytes(&self) -> impl Iterator<Item=(usize, &[u8])> {
                std::iter::once((std::mem::align_of::<$index_type>(), bytemuck::cast_slice(&self[..])))
            }
        }
        impl<'a> AsBytes for &'a [$index_type] {
            fn as_bytes(&self) -> impl Iterator<Item=(usize, &[u8])> {
                std::iter::once((std::mem::align_of::<$index_type>(), bytemuck::cast_slice(&self[..])))
            }
        }
        impl<'a> FromBytes<'a> for &'a [$index_type] {
            fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
                bytemuck::try_cast_slice(bytes.next().unwrap()).unwrap()
            }
        }
    )* }
}

implement_byteslices!(u8, u16, u32, u64, u128, usize);
implement_byteslices!(i8, i16, i32, i64, i128, isize);
implement_byteslices!(f32, f64);
implement_byteslices!(());

use crate::{ColumnString, ColumnVec, ColumnResult, ColumnOption, BitsRank};

impl<BC: AsBytes, VC: AsBytes> AsBytes for ColumnString<BC, VC> {
    fn as_bytes(&self) -> impl Iterator<Item=(usize, &[u8])> {
        self.bounds.as_bytes().chain(self.values.as_bytes())
    }
}
impl<'a, BC: FromBytes<'a>, VC: FromBytes<'a>> FromBytes<'a> for ColumnString<BC, VC> {
    fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
        Self {
            bounds: FromBytes::from_bytes(bytes),
            values: FromBytes::from_bytes(bytes),
        }
    }
}

impl<TC: AsBytes, BC: AsBytes> AsBytes for ColumnVec<TC, BC> {
    fn as_bytes(&self) -> impl Iterator<Item=(usize, &[u8])> {
        self.bounds.as_bytes().chain(self.values.as_bytes())
    }
}
impl<'a, TC: FromBytes<'a>, BC: FromBytes<'a>> FromBytes<'a> for ColumnVec<TC, BC> {
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
            #[allow(non_snake_case)]
            fn as_bytes(&self) -> impl Iterator<Item=(usize, &[u8])> {
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


impl<CC: AsBytes, VC: AsBytes> AsBytes for BitsRank<CC, VC> {
    fn as_bytes(&self) -> impl Iterator<Item=(usize, &[u8])> {
        self.counts.as_bytes().chain(self.values.as_bytes()).chain(std::iter::once((1, std::slice::from_ref(&self.last_bits))))
    }
}
impl<'a, CC: FromBytes<'a>, VC: FromBytes<'a>> FromBytes<'a> for BitsRank<CC, VC> {
    fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
        Self {
            counts: FromBytes::from_bytes(bytes),
            values: FromBytes::from_bytes(bytes),
            last_bits: bytes.next().unwrap()[0],
        }
    }
}

impl<SC: AsBytes, TC: AsBytes, CC: AsBytes, VC: AsBytes> AsBytes for ColumnResult<SC, TC, CC, VC> {
    fn as_bytes(&self) -> impl Iterator<Item=(usize, &[u8])> {
        self.indexes.as_bytes().chain(self.s_store.as_bytes()).chain(self.t_store.as_bytes())
    }
}
impl<'a, SC: FromBytes<'a>, TC: FromBytes<'a>, CC: FromBytes<'a>, VC: FromBytes<'a>> FromBytes<'a> for ColumnResult<SC, TC, CC, VC> {
    fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
        Self {
            indexes: FromBytes::from_bytes(bytes),
            s_store: FromBytes::from_bytes(bytes),
            t_store: FromBytes::from_bytes(bytes),
        }
    }
}

impl <TC: AsBytes, CC: AsBytes, VC: AsBytes> AsBytes for ColumnOption<TC, CC, VC> {
    fn as_bytes(&self) -> impl Iterator<Item=(usize, &[u8])> {
        self.indexes.as_bytes().chain(self.t_store.as_bytes())
    }
}

impl <'a, TC: FromBytes<'a>, CC: FromBytes<'a>, VC: FromBytes<'a>> FromBytes<'a> for ColumnOption<TC, CC, VC> {
    fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
        Self {
            indexes: FromBytes::from_bytes(bytes),
            t_store: FromBytes::from_bytes(bytes),
        }
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn round_trip() {

        use crate::Columnable;
        use crate::common::{Index, Push, HeapSize, Len};
        use crate::bytes::{AsBytes, FromBytes};

        let mut column: <Result<usize, usize> as Columnable>::Columns = Default::default();
        for i in 0..100 {
            column.push(Ok::<usize, usize>(i));
            column.push(Err::<usize, usize>(i));
        }

        assert_eq!(column.len(), 200);
        assert_eq!(column.heap_size(), (1656, 2112));

        for i in 0..100 {
            assert_eq!(column.get(2*i+0), Ok(&i));
            assert_eq!(column.get(2*i+1), Err(&i));
        }

        let column2 = crate::ColumnResult::<&[usize], &[usize], &[u64], &[u64]>::from_bytes(&mut column.as_bytes().map(|(_, bytes)| bytes));
        for i in 0..100 {
            assert_eq!(column.get(2*i+0), column2.get(2*i+0));
            assert_eq!(column.get(2*i+1), column2.get(2*i+1));
        }

        let column3 = crate::ColumnResult::<&[usize], &[usize], &[u64], &[u64]>::from_bytes(&mut column2.as_bytes().map(|(_, bytes)| bytes));
        for i in 0..100 {
            assert_eq!(column3.get(2*i+0), column2.get(2*i+0));
            assert_eq!(column3.get(2*i+1), column2.get(2*i+1));
        }
    }
}
