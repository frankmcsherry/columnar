//! Support for `Box<T>` where `T: Columnar`.
//!
//! The implementation defers to `T`'s implementation to store data. It reveals
//! the same reference type as `T`, wrapped in [`Boxed`].
//!
//! We need this wrapper to distinguish which [`Push`] implementation to use, otherwise
//! the implementations would conflict.

use crate::{AsBytes, Clear, Columnar, Container, FromBytes, HeapSize, Index, IndexMut, Len, Push, Ref};

impl<T: Columnar> Columnar for Box<T> {
    type Container = Boxed<T::Container>;
    #[inline(always)] fn copy_from<'a>(&mut self, other: Ref<'a, Self>) { self.as_mut().copy_from(other.0); }
    #[inline(always)] fn into_owned<'a>(other: Ref<'a, Self>) -> Self { T::into_owned(other.0).into() }
}

/// A newtype wrapper around `T` that implements `Deref` and `DerefMut`.
#[derive(Copy, Clone, Default)]
pub struct Boxed<T>(pub T);

impl<T> std::ops::Deref for Boxed<T> {
    type Target = T;
    #[inline(always)] fn deref(&self) -> &T { &self.0 }
}
impl<T> std::ops::DerefMut for Boxed<T> {
    #[inline(always)] fn deref_mut(&mut self) -> &mut T { &mut self.0 }
}
impl<C: Container> Container for Boxed<C> {
    type Ref<'a> = Boxed<C::Ref<'a>>;
    type Borrowed<'a> = Boxed<C::Borrowed<'a>>;
    #[inline(always)] fn borrow<'a>(&'a self) -> Self::Borrowed<'a> { Boxed(self.0.borrow()) }
    #[inline(always)] fn reborrow<'b, 'a: 'b>(item: Self::Borrowed<'a>) -> Self::Borrowed<'b> where Self: 'a { Boxed(C::reborrow(item.0)) }
    #[inline(always)] fn reborrow_ref<'b, 'a: 'b>(item: Self::Ref<'a>) -> Self::Ref<'b> where Self: 'a { Boxed(C::reborrow_ref(item.0)) }
    #[inline(always)] fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: std::ops::Range<usize>) { self.0.extend_from_self(other.0, range) }
    #[inline(always)] fn reserve_for<'a, I>(&mut self, selves: I) where Self: 'a, I: Iterator<Item = Self::Borrowed<'a>> + Clone { self.0.reserve_for(selves.map(|x| x.0)) }
}
impl<C: Len> Len for Boxed<C> {
    #[inline(always)] fn len(&self) -> usize { self.0.len() }
    #[inline(always)] fn is_empty(&self) -> bool { self.0.is_empty() }
}
impl<C: Clear> Clear for Boxed<C> {
    #[inline(always)] fn clear(&mut self) { self.0.clear() }
}
impl<'a, T: ?Sized, C: Container + Push<&'a T>> Push<&'a Box<T>> for Boxed<C> {
    #[inline(always)] fn push(&mut self, item: &'a Box<T>) { self.0.push(item.as_ref()) }
    #[inline(always)] fn extend(&mut self, iter: impl IntoIterator<Item=&'a Box<T>>) {
        self.0.extend(iter.into_iter().map(|x| x.as_ref()))
    }
}
impl<'a, C: Container> Push<Boxed<C::Ref<'a>>> for Boxed<C> {
    #[inline(always)] fn push(&mut self, item: Boxed<C::Ref<'_>>) { self.0.push(item.0) }
    #[inline(always)] fn extend(&mut self, iter: impl IntoIterator<Item=Boxed<C::Ref<'a>>>) {
        self.0.extend(iter.into_iter().map(|x| x.0))
    }
}
impl<'a, C: AsBytes<'a>> AsBytes<'a> for Boxed<C> {
    #[inline(always)] fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> { self.0.as_bytes() }
}
impl<'a, C: FromBytes<'a>> FromBytes<'a> for Boxed<C> {
    #[inline(always)] fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self { Self(C::from_bytes(bytes)) }
}
impl<C: Index> Index for Boxed<C> {
    type Ref = Boxed<C::Ref>;
    #[inline(always)] fn get(&self, index: usize) -> Self::Ref { Boxed(self.0.get(index)) }
}
impl<C: IndexMut> IndexMut for Boxed<C> {
    type IndexMut<'a> = Boxed<C::IndexMut<'a>> where Self: 'a;
    #[inline(always)] fn get_mut(&mut self, index: usize) -> Self::IndexMut<'_> { Boxed(self.0.get_mut(index)) }
}
impl<C: HeapSize> HeapSize for Boxed<C> {
    #[inline(always)] fn heap_size(&self) -> (usize, usize) { self.0.heap_size() }
}
