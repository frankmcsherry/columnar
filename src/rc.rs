//! Implementations of traits for `Rc<T>`
use alloc::rc::Rc;

use crate::{Len, Borrow, AsBytes, FromBytes};

impl<T: Borrow> Borrow for Rc<T> {
    type Ref<'a> = T::Ref<'a> where T: 'a;
    type Borrowed<'a> = T::Borrowed<'a>;
    #[inline(always)] fn borrow<'a>(&'a self) -> Self::Borrowed<'a> { self.as_ref().borrow() }
    #[inline(always)] fn reborrow<'b, 'a: 'b>(item: Self::Borrowed<'a>) -> Self::Borrowed<'b> where Self: 'a { T::reborrow(item) }
    #[inline(always)] fn reborrow_ref<'b, 'a: 'b>(item: Self::Ref<'a>) -> Self::Ref<'b> where Self: 'a { T::reborrow_ref(item) }
}
impl<T: Len> Len for Rc<T> {
    #[inline(always)] fn len(&self) -> usize { self.as_ref().len() }
}
impl<'a, T: AsBytes<'a>> AsBytes<'a> for Rc<T> {
    const SLICE_COUNT: usize = T::SLICE_COUNT;
    #[inline] fn get_byte_slice(&self, index: usize) -> (u64, &'a [u8]) { self.as_ref().get_byte_slice(index) }
}
impl<'a, T: FromBytes<'a>> FromBytes<'a> for Rc<T> {
    const SLICE_COUNT: usize = T::SLICE_COUNT;
    #[inline(always)] fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self { Rc::new(T::from_bytes(bytes)) }
    #[inline(always)] fn from_store(store: &crate::bytes::indexed::DecodedStore<'a>, offset: &mut usize) -> Self { Rc::new(T::from_store(store, offset)) }
}

#[cfg(test)]
mod tests {
    use alloc::rc::Rc;
    use alloc::{vec, vec::Vec};
    use crate::{Borrow, Len, AsBytes, FromBytes};

    #[test]
    fn test_borrow() {
        let x = Rc::new(vec![1, 2, 3]);
        let y: &[i32] = x.borrow();
        assert_eq!(y, &[1, 2, 3]);
    }

    #[test]
    fn test_len() {
        let x = Rc::new(vec![1, 2, 3]);
        assert_eq!(x.len(), 3);
    }

    #[test]
    fn test_as_from_bytes() {
        let x = Rc::new(vec![1u8, 2, 3, 4, 5]);
        let bytes: Vec<_> = x.borrow().as_bytes().map(|(_, b)| b).collect();
        let y: Rc<&[u8]> = FromBytes::from_bytes(&mut bytes.into_iter());
        assert_eq!(*x, *y);
    }

    #[test]
    fn test_borrow_tuple() {
        let x = (vec![4,5,6,7,], Rc::new(vec![1, 2, 3]));
        let y: (&[i32], &[i32]) = x.borrow();
        assert_eq!(y, ([4,5,6,7].as_ref(), [1, 2, 3].as_ref()));
    }
}
