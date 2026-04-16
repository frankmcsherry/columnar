#![allow(non_snake_case)]

use alloc::{vec::Vec, string::String};
use crate::{Columnar, Container, Borrow, Len, Clear, Index, IndexMut, Push};

// Implementations for tuple types.
// These are all macro based, because the implementations are very similar.
// The macro requires two names, one for the store and one for pushable types.
macro_rules! tuple_impl {
    ( $cursor_name:ident; $($name:ident,$name2:ident,$idx:tt)+) => (

        pub struct $cursor_name<$($name,)*>($(pub $name,)*);

        impl<$($name: Iterator,)*> Iterator for $cursor_name<$($name,)*> {
            type Item = ($($name::Item,)*);
            #[inline(always)]
            fn next(&mut self) -> Option<Self::Item> {
                Some(($(self.$idx.next()?,)*))
            }
            #[inline(always)]
            fn size_hint(&self) -> (usize, Option<usize>) {
                self.0.size_hint()
            }
        }

        impl<$($name: ExactSizeIterator,)*> ExactSizeIterator for $cursor_name<$($name,)*> {}

        impl<$($name: Columnar),*> Columnar for ($($name,)*) {
            #[inline(always)]
            fn copy_from<'a>(&mut self, other: crate::Ref<'a, Self>) {
                let ($($name,)*) = self;
                let ($($name2,)*) = other;
                $(crate::Columnar::copy_from($name, $name2);)*
            }
            #[inline(always)]
            fn into_owned<'a>(other: crate::Ref<'a, Self>) -> Self {
                let ($($name2,)*) = other;
                ($($name::into_owned($name2),)*)
            }
            type Container = ($($name::Container,)*);
        }
        impl<$($name2: Borrow,)*> Borrow for ($($name2,)*) {
            type Ref<'a> = ($($name2::Ref<'a>,)*) where $($name2: 'a,)*;
            type Borrowed<'a> = ($($name2::Borrowed<'a>,)*) where $($name2: 'a,)*;
            #[inline(always)]
            fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
                let ($($name,)*) = self;
                ($($name.borrow(),)*)
            }
            #[inline(always)]
            fn reborrow<'b, 'a: 'b>(thing: Self::Borrowed<'a>) -> Self::Borrowed<'b> where $($name2: 'a,)* {
                let ($($name,)*) = thing;
                ($($name2::reborrow($name),)*)
            }
            #[inline(always)]
            fn reborrow_ref<'b, 'a: 'b>(thing: Self::Ref<'a>) -> Self::Ref<'b> where Self: 'a {
                let ($($name2,)*) = thing;
                ($($name2::reborrow_ref($name2),)*)
            }
        }
        impl<$($name2: Container,)*> Container for ($($name2,)*) {
            #[inline(always)]
            fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: core::ops::Range<usize>) {
                let ($($name,)*) = self;
                let ($($name2,)*) = other;
                $( $name.extend_from_self($name2, range.clone()); )*
            }

            fn reserve_for<'a, I>(&mut self, selves: I) where Self: 'a, I: Iterator<Item = Self::Borrowed<'a>> + Clone {
                let ($($name,)*) = self;
                $( $name.reserve_for(selves.clone().map(|x| x.$idx)); )*
            }
        }

        #[allow(non_snake_case)]
        impl<'a, $($name: crate::AsBytes<'a>),*> crate::AsBytes<'a> for ($($name,)*) {
            const SLICE_COUNT: usize = 0 $(+ $name::SLICE_COUNT)*;
            #[inline]
            fn get_byte_slice(&self, index: usize) -> (u64, &'a [u8]) {
                debug_assert!(index < Self::SLICE_COUNT);
                let ($($name,)*) = self;
                let mut _offset = 0;
                $(
                    if index < _offset + $name::SLICE_COUNT {
                        return $name.get_byte_slice(index - _offset);
                    }
                    _offset += $name::SLICE_COUNT;
                )*
                panic!("get_byte_slice: index out of bounds")
            }
        }
        impl<'a, $($name: crate::FromBytes<'a>),*> crate::FromBytes<'a> for ($($name,)*) {
            const SLICE_COUNT: usize = 0 $(+ $name::SLICE_COUNT)*;
            #[inline(always)]
            #[allow(non_snake_case)]
            fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
                $(let $name = crate::FromBytes::from_bytes(bytes);)*
                ($($name,)*)
            }
            #[inline(always)]
            #[allow(non_snake_case)]
            fn from_store(store: &crate::bytes::indexed::DecodedStore<'a>, offset: &mut usize) -> Self {
                $(let $name = $name::from_store(store, offset);)*
                ($($name,)*)
            }
            fn element_sizes(sizes: &mut Vec<usize>) -> Result<(), String> {
                $($name::element_sizes(sizes)?;)*
                Ok(())
            }
        }

        impl<$($name: Len),*> Len for ($($name,)*) {
            #[inline(always)]
            fn len(&self) -> usize {
                self.0.len()
            }
        }
        impl<$($name: Clear),*> Clear for ($($name,)*) {
            #[inline(always)]
            fn clear(&mut self) {
                let ($($name,)*) = self;
                $($name.clear();)*
            }
        }
        impl<$($name: Index),*> Index for ($($name,)*) {
            type Ref = ($($name::Ref,)*);
            type Cursor<'a> = $cursor_name<$($name::Cursor<'a>,)*> where Self: 'a;
            #[inline(always)]
            fn get(&self, index: usize) -> Self::Ref {
                let ($($name,)*) = self;
                ($($name.get(index),)*)
            }
            #[inline(always)]
            fn cursor(&self, range: core::ops::Range<usize>) -> Self::Cursor<'_> {
                $cursor_name($(self.$idx.cursor(range.clone()),)*)
            }
        }
        impl<'a, $($name),*> Index for &'a ($($name,)*) where $( &'a $name: Index),* {
            type Ref = ($(<&'a $name as Index>::Ref,)*);
            crate::common::impl_default_cursor!();
            #[inline(always)]
            fn get(&self, index: usize) -> Self::Ref {
                let ($($name,)*) = self;
                ($($name.get(index),)*)
            }
        }

        impl<$($name: IndexMut),*> IndexMut for ($($name,)*) {
            type IndexMut<'a> = ($($name::IndexMut<'a>,)*) where $($name: 'a),*;
            #[inline(always)]
            fn get_mut(&mut self, index: usize) -> Self::IndexMut<'_> {
                let ($($name,)*) = self;
                ($($name.get_mut(index),)*)
            }
        }
        impl<$($name2, $name: Push<$name2>),*> Push<($($name2,)*)> for ($($name,)*) {
            #[inline]
            fn push(&mut self, item: ($($name2,)*)) {
                let ($($name,)*) = self;
                let ($($name2,)*) = item;
                $($name.push($name2);)*
            }
        }
        impl<'a, $($name2, $name: Push<&'a $name2>),*> Push<&'a ($($name2,)*)> for ($($name,)*) {
            #[inline]
            fn push(&mut self, item: &'a ($($name2,)*)) {
                let ($($name,)*) = self;
                let ($($name2,)*) = item;
                $($name.push($name2);)*
            }
        }
    )
}

tuple_impl!(TupleCursor1; A,AA,0);
tuple_impl!(TupleCursor2; A,AA,0 B,BB,1);
tuple_impl!(TupleCursor3; A,AA,0 B,BB,1 C,CC,2);
tuple_impl!(TupleCursor4; A,AA,0 B,BB,1 C,CC,2 D,DD,3);
tuple_impl!(TupleCursor5; A,AA,0 B,BB,1 C,CC,2 D,DD,3 E,EE,4);
tuple_impl!(TupleCursor6; A,AA,0 B,BB,1 C,CC,2 D,DD,3 E,EE,4 F,FF,5);
tuple_impl!(TupleCursor7; A,AA,0 B,BB,1 C,CC,2 D,DD,3 E,EE,4 F,FF,5 G,GG,6);
tuple_impl!(TupleCursor8; A,AA,0 B,BB,1 C,CC,2 D,DD,3 E,EE,4 F,FF,5 G,GG,6 H,HH,7);
tuple_impl!(TupleCursor9; A,AA,0 B,BB,1 C,CC,2 D,DD,3 E,EE,4 F,FF,5 G,GG,6 H,HH,7 I,II,8);
tuple_impl!(TupleCursor10; A,AA,0 B,BB,1 C,CC,2 D,DD,3 E,EE,4 F,FF,5 G,GG,6 H,HH,7 I,II,8 J,JJ,9);

#[cfg(test)]
mod test {
    use alloc::string::{String, ToString};
    #[test]
    fn round_trip() {

        use crate::common::{Index, Push, Len};

        let mut column: crate::ContainerOf<(u64, u8, String)> = Default::default();
        for i in 0..100 {
            column.push((i, i as u8, &i.to_string()));
            column.push((i, i as u8, &"".to_string()));
        }

        assert_eq!(column.len(), 200);

        for i in 0..100u64 {
            assert_eq!((&column).get((2*i+0) as usize), (&i, &(i as u8), i.to_string().as_bytes()));
            assert_eq!((&column).get((2*i+1) as usize), (&i, &(i as u8), &b""[..]));
        }

    }

    #[test]
    fn cursor_composition() {
        use alloc::vec::Vec;
        use crate::common::{Index, Push};

        let mut column: crate::ContainerOf<(u64, u8)> = Default::default();
        for i in 0..100u64 {
            column.push((i, i as u8));
        }

        let cursor_values: Vec<_> = column.cursor(10..20).collect();
        let get_values: Vec<_> = (10..20).map(|i| column.get(i)).collect();
        assert_eq!(cursor_values, get_values);
    }
}
