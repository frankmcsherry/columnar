//! Implementations of traits for `Rc<T>`
use std::rc::Rc;

use crate::{Len, Borrow, HeapSize, AsBytes, FromBytes};

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
impl<T: HeapSize> HeapSize for Rc<T> {
    fn heap_size(&self) -> (usize, usize) {
        let (l, c) = self.as_ref().heap_size();
        (l + std::mem::size_of::<Rc<T>>(), c + std::mem::size_of::<Rc<T>>())
    }
}
impl<'a, T: AsBytes<'a>> AsBytes<'a> for Rc<T> {
    #[inline(always)] fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> { self.as_ref().as_bytes() }
}
impl<'a, T: FromBytes<'a>> FromBytes<'a> for Rc<T> {
    #[inline(always)] fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self { Rc::new(T::from_bytes(bytes)) }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use crate::{Borrow, Len, HeapSize, AsBytes, FromBytes};

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
    fn test_heap_size() {
        let x = Rc::new(vec![1, 2, 3]);
        let (l, c) = x.heap_size();
        assert!(l > 0);
        assert!(c > 0);
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
