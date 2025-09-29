//! Types supporting flat / "columnar" layout for complex types.
//!
//! The intent is to re-layout `Vec<T>` types into vectors of reduced
//! complexity, repeatedly. One should be able to push and pop easily,
//! but indexing will be more complicated because we likely won't have
//! a real `T` lying around to return as a reference. Instead, we will
//! use Generic Associated Types (GATs) to provide alternate references.

// Re-export derive crate.
extern crate columnar_derive;
pub use columnar_derive::Columnar;

pub mod adts;
pub mod boxed;
mod arc;
mod rc;

pub use bytemuck;

/// A type that can be represented in columnar form.
///
/// For a running example, a type like `(A, Vec<B>)`.
pub trait Columnar : 'static {
    /// Repopulates `self` from a reference.
    ///
    /// By default this just calls `into_owned()`, but it can be overridden.
    fn copy_from<'a>(&mut self, other: Ref<'a, Self>) where Self: Sized {
        *self = Self::into_owned(other);
    }
    /// Produce an instance of `Self` from `Self::Ref<'a>`.
    fn into_owned<'a>(other: Ref<'a, Self>) -> Self;

    /// The type that stores the columnar representation.
    ///
    /// The container must support pushing both `&Self` and `Self::Ref<'_>`.
    /// In our running example this might be `(Vec<A>, Vecs<Vec<B>>)`.
    type Container: ContainerBytes + for<'a> Push<&'a Self>;

    /// Converts a sequence of the references to the type into columnar form.
    fn as_columns<'a, I>(selves: I) -> Self::Container where I: IntoIterator<Item=&'a Self>, Self: 'a {
        let mut columns: Self::Container = Default::default();
        for item in selves {
            columns.push(item);
        }
        columns
    }
    /// Converts a sequence of the type into columnar form.
    ///
    /// This consumes the owned `Self` types but uses them only by reference.
    /// Consider `as_columns()` instead if it is equally ergonomic.
    fn into_columns<I>(selves: I) -> Self::Container where I: IntoIterator<Item = Self>, Self: Sized {
        let mut columns: Self::Container = Default::default();
        for item in selves {
            columns.push(&item);
        }
        columns
    }
    /// Reborrows the reference type to a shorter lifetime.
    ///
    /// Implementations must not change the contents of the reference, and should mark
    /// the function as `#[inline(always)]`. It is no-op to overcome limitations
    /// of the borrow checker. In many cases, it is enough to return `thing` as-is.
    ///
    /// For example, when comparing two references `Ref<'a>` and `Ref<'b>`, we can
    /// reborrow both to a local lifetime to compare them. This allows us to keep the
    /// lifetimes `'a` and `'b` separate, while otherwise Rust would unify them.
    #[inline(always)] fn reborrow<'b, 'a: 'b>(thing: Ref<'a, Self>) -> Ref<'b, Self> {
        Self::Container::reborrow_ref(thing)
    }
}

/// The container type of columnar type `T`.
///
/// Equivalent to `<T as Columnar>::Container`.
pub type ContainerOf<T> = <T as Columnar>::Container;

/// For a lifetime, the reference type of columnar type `T`.
///
/// Equivalent to `<ContainerOf<T> as Borrow>::Ref<'a>`.
pub type Ref<'a, T> = <ContainerOf<T> as Borrow>::Ref<'a>;

/// A type that can be borrowed into a preferred reference type.
pub trait Borrow: Len + Clone + 'static {
    /// For each lifetime, a reference with that lifetime.
    ///
    /// As an example, `(&'a A, &'a [B])`.
    type Ref<'a> : Copy;
    /// The type of a borrowed container.
    ///
    /// Corresponding to our example, `(&'a [A], Vecs<&'a [B], &'a [u64]>)`.
    type Borrowed<'a>: Copy + Len + Index<Ref = Self::Ref<'a>> where Self: 'a;
    /// Converts a reference to the type to a borrowed variant.
    fn borrow<'a>(&'a self) -> Self::Borrowed<'a>;
    /// Reborrows the borrowed type to a shorter lifetime. See [`Columnar::reborrow`] for details.
    fn reborrow<'b, 'a: 'b>(item: Self::Borrowed<'a>) -> Self::Borrowed<'b> where Self: 'a;
    /// Reborrows the borrowed type to a shorter lifetime. See [`Columnar::reborrow`] for details.
    fn reborrow_ref<'b, 'a: 'b>(item: Self::Ref<'a>) -> Self::Ref<'b> where Self: 'a;
}


/// A container that can hold `C`, and provide its preferred references through [`Borrow`].
///
/// As an example, `(Vec<A>, Vecs<Vec<B>>)`.
pub trait Container : Borrow + Clear + for<'a> Push<Self::Ref<'a>> + Default + Send {
    /// Allocates an empty container that can be extended by `selves` without reallocation.
    ///
    /// This goal is optimistic, and some containers may struggle to size correctly, especially
    /// if they employ compression or other variable-sizing techniques that respond to the data
    /// and the order in which is it presented. Best effort, but still useful!
    fn with_capacity_for<'a, I>(selves: I) -> Self
    where
        Self: 'a,
        I: Iterator<Item = Self::Borrowed<'a>> + Clone
    {
        let mut output = Self::default();
        output.reserve_for(selves);
        output
    }

    // Ensure that `self` can extend from `selves` without reallocation.
    fn reserve_for<'a, I>(&mut self, selves: I)
    where
        Self: 'a,
        I: Iterator<Item = Self::Borrowed<'a>> + Clone;


    /// Extends `self` by a range in `other`.
    ///
    /// This method has a default implementation, but can and should be specialized when ranges can be copied.
    /// As an example, lists of lists are often backed by contiguous elements, all of which can be memcopied,
    /// with only the offsets into them (the bounds) to push either before or after (rather than during).
    #[inline(always)]
    fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: std::ops::Range<usize>) {
        self.extend(range.map(|i| other.get(i)))
    }
}

impl<T: Clone + 'static> Borrow for Vec<T> {
    type Ref<'a> = &'a T;
    type Borrowed<'a> = &'a [T];
    #[inline(always)] fn borrow<'a>(&'a self) -> Self::Borrowed<'a> { &self[..] }
    #[inline(always)] fn reborrow<'b, 'a: 'b>(item: Self::Borrowed<'a>) -> Self::Borrowed<'b> where Self: 'a { item }
    #[inline(always)] fn reborrow_ref<'b, 'a: 'b>(item: Self::Ref<'a>) -> Self::Ref<'b> where Self: 'a { item }
}

impl<T: Clone + Send + 'static> Container for Vec<T> {
    fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: std::ops::Range<usize>) {
        self.extend_from_slice(&other[range])
    }
    fn reserve_for<'a, I>(&mut self, selves: I) where Self: 'a, I: Iterator<Item = Self::Borrowed<'a>> + Clone {
        self.reserve(selves.map(|x| x.len()).sum::<usize>())
    }
}

/// A container that can also be viewed as and reconstituted from bytes.
pub trait ContainerBytes : Container + for<'a> Borrow<Borrowed<'a> : AsBytes<'a> + FromBytes<'a>> { }
impl<C: Container + for<'a> Borrow<Borrowed<'a> : AsBytes<'a> + FromBytes<'a>>> ContainerBytes for C { }

pub use common::{Clear, Len, Push, IndexMut, Index, IndexAs, HeapSize, Slice, AsBytes, FromBytes};
/// Common traits and types that are re-used throughout the module.
pub mod common {

    /// A type with a length.
    pub trait Len {
        /// The number of contained elements.
        fn len(&self) -> usize;
        /// Whether this contains no elements.
        fn is_empty(&self) -> bool {
            self.len() == 0
        }
    }
    impl<L: Len + ?Sized> Len for &L {
        #[inline(always)] fn len(&self) -> usize { L::len(*self) }
    }
    impl<L: Len + ?Sized> Len for &mut L {
        #[inline(always)] fn len(&self) -> usize { L::len(*self) }
    }
    impl<T> Len for Vec<T> {
        #[inline(always)] fn len(&self) -> usize { self.len() }
    }
    impl<T> Len for [T] {
        #[inline(always)] fn len(&self) -> usize { <[T]>::len(self) }
    }

    /// A type that can accept items of type `T`.
    pub trait Push<T> {
        /// Pushes an item onto `self`.
        fn push(&mut self, item: T);
        /// Pushes elements of an iterator onto `self`.
        #[inline(always)] fn extend(&mut self, iter: impl IntoIterator<Item=T>) {
            for item in iter {
                self.push(item);
            }
        }
    }
    impl<T> Push<T> for Vec<T> {
        #[inline(always)] fn push(&mut self, item: T) { self.push(item) }

        #[inline(always)]
        fn extend(&mut self, iter: impl IntoIterator<Item=T>) {
            std::iter::Extend::extend(self, iter)
        }
    }
    impl<'a, T: Clone> Push<&'a T> for Vec<T> {
        #[inline(always)] fn push(&mut self, item: &'a T) { self.push(item.clone()) }

        #[inline(always)]
        fn extend(&mut self, iter: impl IntoIterator<Item=&'a T>) {
            std::iter::Extend::extend(self, iter.into_iter().cloned())
        }
    }
    impl<'a, T: Clone> Push<&'a [T]> for Vec<T> {
        #[inline(always)] fn push(&mut self, item: &'a [T]) { self.clone_from_slice(item) }
    }


    pub use index::{Index, IndexMut, IndexAs};
    /// Traits for accessing elements by `usize` indexes.
    ///
    /// There are several traits, with a core distinction being whether the returned reference depends on the lifetime of `&self`.
    /// For one trait `Index` the result does not depend on this lifetime.
    /// There is a third trait `IndexMut` that allows mutable access, that may be less commonly implemented.
    pub mod index {

        use crate::Len;
        use crate::common::IterOwn;

        /// A type that can be mutably accessed by `usize`.
        pub trait IndexMut {
            /// Type mutably referencing an indexed element.
            type IndexMut<'a> where Self: 'a;
            fn get_mut(& mut self, index: usize) -> Self::IndexMut<'_>;
            /// A reference to the last element, should one exist.
            #[inline(always)] fn last_mut(&mut self) -> Option<Self::IndexMut<'_>> where Self: Len {
                if self.is_empty() { None }
                else { Some(self.get_mut(self.len()-1)) }
            }
        }

        impl<T: IndexMut + ?Sized> IndexMut for &mut T {
            type IndexMut<'a> = T::IndexMut<'a> where Self: 'a;
            #[inline(always)] fn get_mut(&mut self, index: usize) -> Self::IndexMut<'_> {
                T::get_mut(*self, index)
            }
        }
        impl<T> IndexMut for Vec<T> {
            type IndexMut<'a> = &'a mut T where Self: 'a;
            #[inline(always)] fn get_mut(&mut self, index: usize) -> Self::IndexMut<'_> { &mut self[index] }
        }
        impl<T> IndexMut for [T] {
            type IndexMut<'a> = &'a mut T where Self: 'a;
            #[inline(always)] fn get_mut(&mut self, index: usize) -> Self::IndexMut<'_> { &mut self[index] }
        }

        /// A type that can be accessed by `usize` but without borrowing `self`.
        ///
        /// This can be useful for types which include their own lifetimes, and
        /// which wish to express that their reference has the same lifetime.
        /// In the GAT `Index`, the `Ref<'_>` lifetime would be tied to `&self`.
        ///
        /// This trait may be challenging to implement for owning containers,
        /// for example `Vec<_>`, which would need their `Ref` type to depend
        /// on the lifetime of the `&self` borrow in the `get()` function.
        pub trait Index {
            /// The type returned by the `get` method.
            ///
            /// Notably, this does not vary with lifetime, and will not depend on the lifetime of `&self`.
            type Ref;
            fn get(&self, index: usize) -> Self::Ref;
            #[inline(always)] fn last(&self) -> Option<Self::Ref> where Self: Len {
                if self.is_empty() { None }
                else { Some(self.get(self.len()-1)) }
            }
            /// Converts `&self` into an iterator.
            ///
            /// This has an awkward name to avoid collision with `iter()`, which may also be implemented.
            #[inline(always)]
            fn index_iter(&self) -> IterOwn<&Self> {
                IterOwn {
                    index: 0,
                    slice: self,
                }
            }
            /// Converts `self` into an iterator.
            ///
            /// This has an awkward name to avoid collision with `into_iter()`, which may also be implemented.
            #[inline(always)]
            fn into_index_iter(self) -> IterOwn<Self> where Self: Sized {
                IterOwn {
                    index: 0,
                    slice: self,
                }
            }
        }

        // These implementations aim to reveal a longer lifetime, or to copy results to avoid a lifetime.
        impl<'a, T> Index for &'a [T] {
            type Ref = &'a T;
            #[inline(always)] fn get(&self, index: usize) -> Self::Ref { &self[index] }
        }
        impl<T: Copy> Index for [T] {
            type Ref = T;
            #[inline(always)] fn get(&self, index: usize) -> Self::Ref { self[index] }
        }
        impl<'a, T> Index for &'a Vec<T> {
            type Ref = &'a T;
            #[inline(always)] fn get(&self, index: usize) -> Self::Ref { &self[index] }
        }
        impl<T: Copy> Index for Vec<T> {
            type Ref = T;
            #[inline(always)] fn get(&self, index: usize) -> Self::Ref { self[index] }
        }


        /// Types that can be converted into another type by copying.
        ///
        /// We use this trait to unify the ability of `T` and `&T` to be converted into `T`.
        /// This is handy for copy types that we'd like to use, like `u8`, `u64` and `usize`.
        pub trait CopyAs<T> : Copy {
            fn copy_as(self) -> T;
        }
        impl<T: Copy> CopyAs<T> for &T {
            #[inline(always)] fn copy_as(self) -> T { *self }
        }
        impl<T: Copy> CopyAs<T> for T {
            #[inline(always)] fn copy_as(self) -> T { self }
        }

        pub trait IndexAs<T> {
            fn index_as(&self, index: usize) -> T;
            #[inline(always)] fn last(&self) -> Option<T> where Self: Len {
                if self.is_empty() { None }
                else { Some(self.index_as(self.len()-1)) }
            }
        }

        impl<T: Index, S> IndexAs<S> for T where T::Ref: CopyAs<S> {
            #[inline(always)] fn index_as(&self, index: usize) -> S { self.get(index).copy_as() }
        }

    }

    use crate::{Borrow, Container};
    use crate::common::index::CopyAs;
    /// A composite trait which captures the ability `Index<Ref = T>`.
    ///
    /// Implement `CopyAs<T>` for the reference type.
    pub trait BorrowIndexAs<T> : for<'a> Borrow<Ref<'a>: CopyAs<T>> { }
    impl<T, C: for<'a> Borrow<Ref<'a>: CopyAs<T>>> BorrowIndexAs<T> for C { }
    /// A composite trait which captures the ability `Push<&T>` and `Index<Ref = T>`.
    ///
    /// Implement `CopyAs<T>` for the reference type, and `Push<&'a T>` for the container.
    pub trait PushIndexAs<T> : BorrowIndexAs<T> + Container + for<'a> Push<&'a T> { }
    impl<T, C: BorrowIndexAs<T> + Container + for<'a> Push<&'a T>> PushIndexAs<T> for C { }

    /// A type that can remove its contents and return to an empty state.
    ///
    /// Generally, this method does not release resources, and is used to make the container available for re-insertion.
    pub trait Clear {
        /// Clears `self`, without changing its capacity.
        fn clear(&mut self);
    }
    // Vectors can be cleared.
    impl<T> Clear for Vec<T> {
        #[inline(always)] fn clear(&mut self) { self.clear() }
    }
    // Slice references can be cleared.
    impl<T> Clear for &[T] {
        #[inline(always)] fn clear(&mut self) { *self = &[]; }
    }

    pub trait HeapSize {
        /// Active (len) and allocated (cap) heap sizes in bytes.
        /// This should not include the size of `self` itself.
        fn heap_size(&self) -> (usize, usize) { (0, 0) }
    }

    impl HeapSize for String {
        fn heap_size(&self) -> (usize, usize) {
            (self.len(), self.capacity())
        }
    }
    impl<T: HeapSize> HeapSize for [T] {
        fn heap_size(&self) -> (usize, usize) {
            let mut l = std::mem::size_of_val(self);
            let mut c = std::mem::size_of_val(self);
            for item in self.iter() {
                let (il, ic) = item.heap_size();
                l += il;
                c += ic;
            }
            (l, c)
        }
    }
    impl<T: HeapSize> HeapSize for Vec<T> {
        fn heap_size(&self) -> (usize, usize) {
            let mut l = std::mem::size_of::<T>() * self.len();
            let mut c = std::mem::size_of::<T>() * self.capacity();
            for item in (self[..]).iter() {
                let (il, ic) = item.heap_size();
                l += il;
                c += ic;
            }
            (l, c)
        }
    }

    /// A struct representing a slice of a range of values.
    ///
    /// The lower and upper bounds should be meaningfully set on construction.
    #[derive(Copy, Clone, Debug)]
    pub struct Slice<S> {
        pub lower: usize,
        pub upper: usize,
        pub slice: S,
    }

    impl<S> std::hash::Hash for Slice<S> where Self: Index<Ref: std::hash::Hash> {
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            self.len().hash(state);
            for i in 0 .. self.len() {
                self.get(i).hash(state);
            }
        }
    }

    impl<S> Slice<S> {
        pub fn slice<R: std::ops::RangeBounds<usize>>(self, range: R) -> Self {
            use std::ops::Bound;
            let lower = match range.start_bound() {
                Bound::Included(s) => std::cmp::max(self.lower, *s),
                Bound::Excluded(s) => std::cmp::max(self.lower, *s+1),
                Bound::Unbounded => self.lower,
            };
            let upper = match range.end_bound() {
                Bound::Included(s) => std::cmp::min(self.upper, *s+1),
                Bound::Excluded(s) => std::cmp::min(self.upper, *s),
                Bound::Unbounded => self.upper,
            };
            assert!(lower <= upper);
            Self { lower, upper, slice: self.slice }
        }
        pub fn new(lower: u64, upper: u64, slice: S) -> Self {
            let lower: usize = lower.try_into().expect("slice bounds must fit in `usize`");
            let upper: usize = upper.try_into().expect("slice bounds must fit in `usize`");
            Self { lower, upper, slice }
        }
        pub fn len(&self) -> usize { self.upper - self.lower }
        /// Map the slice to another type.
        pub(crate) fn map<T>(self, f: impl Fn(S) -> T) -> Slice<T> {
            Slice {
                lower: self.lower,
                upper: self.upper,
                slice: f(self.slice),
            }
        }
    }

    impl<S: Index> PartialEq for Slice<S> where S::Ref: PartialEq {
        fn eq(&self, other: &Self) -> bool {
            if self.len() != other.len() { return false; }
            for i in 0 .. self.len() {
                if self.get(i) != other.get(i) { return false; }
            }
            true
        }
    }
    impl<S: Index> PartialEq<[S::Ref]> for Slice<S> where S::Ref: PartialEq {
        fn eq(&self, other: &[S::Ref]) -> bool {
            if self.len() != other.len() { return false; }
            for i in 0 .. self.len() {
                if self.get(i) != other[i] { return false; }
            }
            true
        }
    }
    impl<S: Index> PartialEq<Vec<S::Ref>> for Slice<S> where S::Ref: PartialEq {
        fn eq(&self, other: &Vec<S::Ref>) -> bool {
            if self.len() != other.len() { return false; }
            for i in 0 .. self.len() {
                if self.get(i) != other[i] { return false; }
            }
            true
        }
    }

    impl<S: Index> Eq for Slice<S> where S::Ref: Eq { }

    impl<S: Index> PartialOrd for Slice<S> where S::Ref: PartialOrd {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            use std::cmp::Ordering;
            let len = std::cmp::min(self.len(), other.len());

            for i in 0 .. len {
                match self.get(i).partial_cmp(&other.get(i)) {
                    Some(Ordering::Equal) => (),
                    not_equal => return not_equal,
                }
            }

            self.len().partial_cmp(&other.len())
        }
    }

    impl<S: Index> Ord for Slice<S> where S::Ref: Ord + Eq {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            use std::cmp::Ordering;
            let len = std::cmp::min(self.len(), other.len());

            for i in 0 .. len {
                match self.get(i).cmp(&other.get(i)) {
                    Ordering::Equal => (),
                    not_equal => return not_equal,
                }
            }

            self.len().cmp(&other.len())
        }
    }

    impl<S> Len for Slice<S> {
        #[inline(always)] fn len(&self) -> usize { self.len() }
    }

    impl<S: Index> Index for Slice<S> {
        type Ref = S::Ref;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref {
            assert!(index < self.upper - self.lower);
            self.slice.get(self.lower + index)
        }
    }
    impl<'a, S> Index for &'a Slice<S>
    where
        &'a S : Index,
    {
        type Ref = <&'a S as Index>::Ref;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref {
            assert!(index < self.upper - self.lower);
            (&self.slice).get(self.lower + index)
        }
    }

    impl<S: IndexMut> IndexMut for Slice<S> {
        type IndexMut<'a> = S::IndexMut<'a> where S: 'a;
        #[inline(always)] fn get_mut(&mut self, index: usize) -> Self::IndexMut<'_> {
            assert!(index < self.upper - self.lower);
            self.slice.get_mut(self.lower + index)
        }
    }

    impl<S: Index + Len> Slice<S> {
        /// Converts the slice into an iterator.
        ///
        /// This method exists rather than an `IntoIterator` implementation to avoid
        /// a conflicting implementation for pushing an `I: IntoIterator` into `Vecs`.
        pub fn into_iter(self) -> IterOwn<Slice<S>> {
            self.into_index_iter()
        }
    }

    impl<'a, T> Slice<&'a [T]> {
        pub fn as_slice(&self) -> &'a [T] {
            &self.slice[self.lower .. self.upper]
        }
    }

    pub struct IterOwn<S> {
        index: usize,
        slice: S,
    }

    impl<S> IterOwn<S> {
        pub fn new(index: usize, slice: S) -> Self {
            Self { index, slice }
        }
    }

    impl<S: Index + Len> Iterator for IterOwn<S> {
        type Item = S::Ref;
        #[inline(always)] fn next(&mut self) -> Option<Self::Item> {
            if self.index < self.slice.len() {
                let result = self.slice.get(self.index);
                self.index += 1;
                Some(result)
            } else {
                None
            }
        }
        #[inline(always)]
        fn size_hint(&self) -> (usize, Option<usize>) {
            (self.slice.len() - self.index, Some(self.slice.len() - self.index))
        }
    }

    impl<S: Index + Len> ExactSizeIterator for IterOwn<S> { }

    /// A type that can be viewed as byte slices with lifetime `'a`.
    ///
    /// Implementors of this trait almost certainly reference the lifetime `'a` themselves.
    pub trait AsBytes<'a> {
        /// Presents `self` as a sequence of byte slices, with their required alignment.
        fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])>;
    }

    /// A type that can be reconstituted from byte slices with lifetime `'a`.
    ///
    /// Implementors of this trait almost certainly reference the lifetime `'a` themselves,
    /// unless they actively deserialize the bytes (vs sit on the slices, as if zero-copy).
    pub trait FromBytes<'a> {
        /// Reconstructs `self` from a sequence of correctly aligned and sized bytes slices.
        ///
        /// The implementation is expected to consume the right number of items from the iterator,
        /// which may go on to be used by other implementations of `FromBytes`.
        ///
        /// The implementation should aim for only doing trivial work, as it backs calls like
        /// `borrow` for serialized containers.
        ///
        /// Implementations should almost always be marked as `#[inline(always)]` to ensure that
        /// they are inlined. A single non-inlined function on a tree of `from_bytes` calls
        /// can cause the performance to drop significantly.
        fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self;
    }

}

/// Logic related to the transformation to and from bytes.
///
/// The methods here line up with the `AsBytes` and `FromBytes` traits.
pub mod bytes {

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
}

/// Types that prefer to be represented by `Vec<T>`.
pub mod primitive {
    use std::num::Wrapping;

    /// An implementation of opinions for types that want to use `Vec<T>`.
    macro_rules! implement_columnable {
        ($($index_type:ty),*) => { $(
            impl crate::Columnar for $index_type {
                #[inline(always)]
                fn into_owned<'a>(other: crate::Ref<'a, Self>) -> Self { *other }

                type Container = Vec<$index_type>;
            }

            impl crate::HeapSize for $index_type { }

            impl<'a> crate::AsBytes<'a> for &'a [$index_type] {
                #[inline(always)]
                fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
                    std::iter::once((std::mem::align_of::<$index_type>() as u64, bytemuck::cast_slice(&self[..])))
                }
            }
            impl<'a> crate::FromBytes<'a> for &'a [$index_type] {
                #[inline(always)]
                fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
                    // We use `unwrap()` here in order to panic with the `bytemuck` error, which may be informative.
                    bytemuck::try_cast_slice(bytes.next().expect("Iterator exhausted prematurely")).unwrap()
                }
            }
            impl<'a, const N: usize> crate::AsBytes<'a> for &'a [[$index_type; N]] {
                #[inline(always)]
                fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
                    std::iter::once((std::mem::align_of::<$index_type>() as u64, bytemuck::cast_slice(&self[..])))
                }
            }
            impl<'a, const N: usize> crate::FromBytes<'a> for &'a [[$index_type; N]] {
                #[inline(always)]
                fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
                    // We use `unwrap()` here in order to panic with the `bytemuck` error, which may be informative.
                    bytemuck::try_cast_slice(bytes.next().expect("Iterator exhausted prematurely")).unwrap()
                }
            }
        )* }
    }

    implement_columnable!(u8, u16, u32, u64);
    implement_columnable!(i8, i16, i32, i64);
    implement_columnable!(f32, f64);
    implement_columnable!(Wrapping<u8>, Wrapping<u16>, Wrapping<u32>, Wrapping<u64>);
    implement_columnable!(Wrapping<i8>, Wrapping<i16>, Wrapping<i32>, Wrapping<i64>);

    pub use sizes::{Usizes, Isizes};
    /// Columnar stores for `usize` and `isize`, stored as 64 bits.
    mod sizes {

        use crate::*;
        use crate::common::{BorrowIndexAs, PushIndexAs};

        #[derive(Copy, Clone, Default)]
        pub struct Usizes<CV = Vec<u64>> { pub values: CV }

        impl Columnar for usize {
            fn into_owned<'a>(other: crate::Ref<'a, Self>) -> Self { other }
            type Container = Usizes;
        }

        impl<CV: BorrowIndexAs<u64> + Len> Borrow for Usizes<CV> {
            type Ref<'a> = usize;
            type Borrowed<'a> = Usizes<CV::Borrowed<'a>> where CV: 'a;
            fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
                Usizes { values: self.values.borrow() }
            }
            #[inline(always)]
            fn reborrow<'b, 'a: 'b>(thing: Self::Borrowed<'a>) -> Self::Borrowed<'b> where CV: 'a {
                Usizes { values: CV::reborrow(thing.values) }
            }
            #[inline(always)]
            fn reborrow_ref<'b, 'a: 'b>(thing: Self::Ref<'a>) -> Self::Ref<'b> where Self: 'a { thing }
        }

        impl<CV: PushIndexAs<u64>> Container for Usizes<CV> {
            #[inline(always)]
            fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: std::ops::Range<usize>) {
                self.values.extend_from_self(other.values, range)
            }

            fn reserve_for<'a, I>(&mut self, selves: I) where Self: 'a, I: Iterator<Item = Self::Borrowed<'a>> + Clone {
                self.values.reserve_for(selves.map(|x| x.values))
            }
        }

        impl<CV: Len> Len for Usizes<CV> { fn len(&self) -> usize { self.values.len() }}
        impl IndexMut for Usizes {
            type IndexMut<'a> = &'a mut u64;
            #[inline(always)] fn get_mut(&mut self, index: usize) -> Self::IndexMut<'_> { &mut self.values[index] }
        }
        impl<CV: IndexAs<u64>> Index for Usizes<CV> {
            type Ref = usize;
            #[inline(always)] fn get(&self, index: usize) -> Self::Ref { self.values.index_as(index).try_into().expect("Usizes values should fit in `usize`") }
        }
        impl<CV: IndexAs<u64>> Index for &Usizes<CV> {
            type Ref = usize;
            #[inline(always)] fn get(&self, index: usize) -> Self::Ref { self.values.index_as(index).try_into().expect("Usizes values should fit in `usize`") }
        }
        impl<CV: for<'a> Push<&'a u64>> Push<usize> for Usizes<CV> {
            #[inline]
            fn push(&mut self, item: usize) { self.values.push(&item.try_into().expect("usize must fit in a u64")) }
        }
        impl Push<&usize> for Usizes {
            #[inline]
            fn push(&mut self, item: &usize) { self.values.push((*item).try_into().expect("usize must fit in a u64")) }
        }
        impl<CV: Clear> Clear for Usizes<CV> { fn clear(&mut self) { self.values.clear() }}

        impl<CV: HeapSize> HeapSize for Usizes<CV> {
            fn heap_size(&self) -> (usize, usize) {
                self.values.heap_size()
            }
        }

        impl<'a, CV: crate::AsBytes<'a>> crate::AsBytes<'a> for crate::primitive::Usizes<CV> {
            #[inline(always)]
            fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
                self.values.as_bytes()
            }
        }

        impl<'a, CV: crate::FromBytes<'a>> crate::FromBytes<'a> for crate::primitive::Usizes<CV> {
            #[inline(always)]
            fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
                Self { values: CV::from_bytes(bytes) }
            }
        }


        #[derive(Copy, Clone, Default)]
        pub struct Isizes<CV = Vec<i64>> { pub values: CV }

        impl Columnar for isize {
            fn into_owned<'a>(other: crate::Ref<'a, Self>) -> Self { other }
            type Container = Isizes;
        }

        impl<CV: BorrowIndexAs<i64>> Borrow for Isizes<CV> {
            type Ref<'a> = isize;
            type Borrowed<'a> = Isizes<CV::Borrowed<'a>> where CV: 'a;
            fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
                Isizes { values: self.values.borrow() }
            }
            #[inline(always)]
            fn reborrow<'b, 'a: 'b>(thing: Self::Borrowed<'a>) -> Self::Borrowed<'b> where CV: 'a {
                Isizes { values: CV::reborrow(thing.values) }
            }
            #[inline(always)]
            fn reborrow_ref<'b, 'a: 'b>(thing: Self::Ref<'a>) -> Self::Ref<'b> where Self: 'a { thing }
        }

        impl<CV: PushIndexAs<i64>> Container for Isizes<CV> {
            #[inline(always)]
            fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: std::ops::Range<usize>) {
                self.values.extend_from_self(other.values, range)
            }

            fn reserve_for<'a, I>(&mut self, selves: I) where Self: 'a, I: Iterator<Item = Self::Borrowed<'a>> + Clone {
                self.values.reserve_for(selves.map(|x| x.values))
            }
        }

        impl<CV: Len> Len for Isizes<CV> { fn len(&self) -> usize { self.values.len() }}
        impl IndexMut for Isizes {
            type IndexMut<'a> = &'a mut i64;
            #[inline(always)] fn get_mut(&mut self, index: usize) -> Self::IndexMut<'_> { &mut self.values[index] }
        }
        impl<CV: IndexAs<i64>> Index for Isizes<CV> {
            type Ref = isize;
            #[inline(always)] fn get(&self, index: usize) -> Self::Ref { self.values.index_as(index).try_into().expect("Isizes values should fit in `isize`") }
        }
        impl<CV: IndexAs<i64>> Index for &Isizes<CV> {
            type Ref = isize;
            #[inline(always)] fn get(&self, index: usize) -> Self::Ref { self.values.index_as(index).try_into().expect("Isizes values should fit in `isize`") }
        }
        impl<CV: for<'a> Push<&'a i64>> Push<isize> for Isizes<CV> {
            #[inline]
            fn push(&mut self, item: isize) { self.values.push(&item.try_into().expect("isize must fit in a i64")) }
        }
        impl Push<&isize> for Isizes {
            #[inline]
            fn push(&mut self, item: &isize) { self.values.push((*item).try_into().expect("isize must fit in a i64")) }
        }
        impl<CV: Clear> Clear for Isizes<CV> { fn clear(&mut self) { self.values.clear() }}

        impl<CV: HeapSize> HeapSize for Isizes<CV> {
            fn heap_size(&self) -> (usize, usize) {
                self.values.heap_size()
            }
        }

        impl<'a, CV: crate::AsBytes<'a>> crate::AsBytes<'a> for crate::primitive::Isizes<CV> {
            #[inline(always)]
            fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
                self.values.as_bytes()
            }
        }

        impl<'a, CV: crate::FromBytes<'a>> crate::FromBytes<'a> for crate::primitive::Isizes<CV> {
            #[inline(always)]
            fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
                Self { values: CV::from_bytes(bytes) }
            }
        }
    }

    pub use chars::{Chars};
    /// Columnar store for `char`, stored as a `u32`.
    mod chars {

        use crate::*;
        use crate::common::{BorrowIndexAs, PushIndexAs};

        type Encoded = u32;

        #[derive(Copy, Clone, Default)]
        pub struct Chars<CV = Vec<Encoded>> { pub values: CV }

        impl Columnar for char {
            fn into_owned<'a>(other: crate::Ref<'a, Self>) -> Self { other }
            type Container = Chars;
        }

        impl<CV: BorrowIndexAs<Encoded>> Borrow for Chars<CV> {
            type Ref<'a> = char;
            type Borrowed<'a> = Chars<CV::Borrowed<'a>> where CV: 'a;
            fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
                Chars { values: self.values.borrow() }
            }
            #[inline(always)]
            fn reborrow<'b, 'a: 'b>(thing: Self::Borrowed<'a>) -> Self::Borrowed<'b> where CV: 'a {
                Chars { values: CV::reborrow(thing.values) }
            }
            #[inline(always)]
            fn reborrow_ref<'b, 'a: 'b>(thing: Self::Ref<'a>) -> Self::Ref<'b> where Self: 'a { thing }
        }

        impl<CV: PushIndexAs<Encoded>> Container for Chars<CV> {
            #[inline(always)]
            fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: std::ops::Range<usize>) {
                self.values.extend_from_self(other.values, range)
            }

            fn reserve_for<'a, I>(&mut self, selves: I) where Self: 'a, I: Iterator<Item = Self::Borrowed<'a>> + Clone {
                self.values.reserve_for(selves.map(|x| x.values))
            }
        }

        impl<CV: Len> Len for Chars<CV> { fn len(&self) -> usize { self.values.len() }}
        impl<CV: IndexAs<Encoded>> Index for Chars<CV> {
            type Ref = char;
            #[inline(always)] fn get(&self, index: usize) -> Self::Ref { char::from_u32(self.values.index_as(index)).unwrap() }
        }
        impl<CV: IndexAs<Encoded>> Index for &Chars<CV> {
            type Ref = char;
            #[inline(always)] fn get(&self, index: usize) -> Self::Ref { char::from_u32(self.values.index_as(index)).unwrap() }
        }
        impl<CV: for<'a> Push<&'a Encoded>> Push<char> for Chars<CV> {
            #[inline]
            fn push(&mut self, item: char) { self.values.push(&u32::from(item)) }
        }
        impl Push<&char> for Chars {
            #[inline]
            fn push(&mut self, item: &char) { self.values.push(u32::from(*item)) }
        }
        impl<CV: Clear> Clear for Chars<CV> { fn clear(&mut self) { self.values.clear() }}

        impl<CV: HeapSize> HeapSize for Chars<CV> {
            fn heap_size(&self) -> (usize, usize) {
                self.values.heap_size()
            }
        }

        impl<'a, CV: crate::AsBytes<'a>> crate::AsBytes<'a> for Chars<CV> {
            #[inline(always)]
            fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
                self.values.as_bytes()
            }
        }

        impl<'a, CV: crate::FromBytes<'a>> crate::FromBytes<'a> for Chars<CV> {
            #[inline(always)]
            fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
                Self { values: CV::from_bytes(bytes) }
            }
        }
    }

    pub use larges::{U128s, I128s};
    /// Columnar stores for `u128` and `i128`, stored as [u8; 16] bits.
    mod larges {

        use crate::*;
        use crate::common::{BorrowIndexAs, PushIndexAs};

        type Encoded = [u8; 16];

        #[derive(Copy, Clone, Default)]
        pub struct U128s<CV = Vec<Encoded>> { pub values: CV }

        impl Columnar for u128 {
            fn into_owned<'a>(other: crate::Ref<'a, Self>) -> Self { other }
            type Container = U128s;
        }

        impl<CV: BorrowIndexAs<Encoded>> Borrow for U128s<CV> {
            type Ref<'a> = u128;
            type Borrowed<'a> = U128s<CV::Borrowed<'a>> where CV: 'a;
            fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
                U128s { values: self.values.borrow() }
            }
            #[inline(always)]
            fn reborrow<'b, 'a: 'b>(thing: Self::Borrowed<'a>) -> Self::Borrowed<'b> where CV: 'a {
                U128s { values: CV::reborrow(thing.values) }
            }
            #[inline(always)]
            fn reborrow_ref<'b, 'a: 'b>(thing: Self::Ref<'a>) -> Self::Ref<'b> where Self: 'a { thing }
        }

        impl<CV: PushIndexAs<Encoded>> Container for U128s<CV> {
            #[inline(always)]
            fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: std::ops::Range<usize>) {
                self.values.extend_from_self(other.values, range)
            }

            fn reserve_for<'a, I>(&mut self, selves: I) where Self: 'a, I: Iterator<Item = Self::Borrowed<'a>> + Clone {
                self.values.reserve_for(selves.map(|x| x.values))
            }
        }

        impl<CV: Len> Len for U128s<CV> { fn len(&self) -> usize { self.values.len() }}
        impl<CV: IndexAs<Encoded>> Index for U128s<CV> {
            type Ref = u128;
            #[inline(always)] fn get(&self, index: usize) -> Self::Ref { u128::from_le_bytes(self.values.index_as(index)) }
        }
        impl<CV: IndexAs<Encoded>> Index for &U128s<CV> {
            type Ref = u128;
            #[inline(always)] fn get(&self, index: usize) -> Self::Ref { u128::from_le_bytes(self.values.index_as(index)) }
        }
        impl<CV: for<'a> Push<&'a Encoded>> Push<u128> for U128s<CV> {
            #[inline]
            fn push(&mut self, item: u128) { self.values.push(&item.to_le_bytes()) }
        }
        impl Push<&u128> for U128s {
            #[inline]
            fn push(&mut self, item: &u128) { self.values.push(item.to_le_bytes()) }
        }
        impl<CV: Clear> Clear for U128s<CV> { fn clear(&mut self) { self.values.clear() }}

        impl<CV: HeapSize> HeapSize for U128s<CV> {
            fn heap_size(&self) -> (usize, usize) {
                self.values.heap_size()
            }
        }

        impl<'a, CV: crate::AsBytes<'a>> crate::AsBytes<'a> for U128s<CV> {
            #[inline(always)]
            fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
                self.values.as_bytes()
            }
        }

        impl<'a, CV: crate::FromBytes<'a>> crate::FromBytes<'a> for U128s<CV> {
            #[inline(always)]
            fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
                Self { values: CV::from_bytes(bytes) }
            }
        }

        #[derive(Copy, Clone, Default)]
        pub struct I128s<CV = Vec<Encoded>> { pub values: CV }

        impl Columnar for i128 {
            fn into_owned<'a>(other: crate::Ref<'a, Self>) -> Self { other }
            type Container = I128s;
        }

        impl<CV: BorrowIndexAs<Encoded>> Borrow for I128s<CV> {
            type Ref<'a> = i128;
            type Borrowed<'a> = I128s<CV::Borrowed<'a>> where CV: 'a;
            fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
                I128s { values: self.values.borrow() }
            }
            #[inline(always)]
            fn reborrow<'b, 'a: 'b>(thing: Self::Borrowed<'a>) -> Self::Borrowed<'b> where CV: 'a {
                I128s { values: CV::reborrow(thing.values) }
            }
            #[inline(always)]
            fn reborrow_ref<'b, 'a: 'b>(thing: Self::Ref<'a>) -> Self::Ref<'b> where Self: 'a { thing }
        }

        impl<CV: PushIndexAs<Encoded>> Container for I128s<CV> {
            #[inline(always)]
            fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: std::ops::Range<usize>) {
                self.values.extend_from_self(other.values, range)
            }

            fn reserve_for<'a, I>(&mut self, selves: I) where Self: 'a, I: Iterator<Item = Self::Borrowed<'a>> + Clone {
                self.values.reserve_for(selves.map(|x| x.values))
            }
        }

        impl<CV: Len> Len for I128s<CV> { fn len(&self) -> usize { self.values.len() }}
        impl<CV: IndexAs<Encoded>> Index for I128s<CV> {
            type Ref = i128;
            #[inline(always)] fn get(&self, index: usize) -> Self::Ref { i128::from_le_bytes(self.values.index_as(index)) }
        }
        impl<CV: IndexAs<Encoded>> Index for &I128s<CV> {
            type Ref = i128;
            #[inline(always)] fn get(&self, index: usize) -> Self::Ref { i128::from_le_bytes(self.values.index_as(index)) }
        }
        impl<CV: for<'a> Push<&'a Encoded>> Push<i128> for I128s<CV> {
            #[inline]
            fn push(&mut self, item: i128) { self.values.push(&item.to_le_bytes()) }
        }
        impl Push<&i128> for I128s {
            #[inline]
            fn push(&mut self, item: &i128) { self.values.push(item.to_le_bytes()) }
        }
        impl<CV: Clear> Clear for I128s<CV> { fn clear(&mut self) { self.values.clear() }}

        impl<CV: HeapSize> HeapSize for I128s<CV> {
            fn heap_size(&self) -> (usize, usize) {
                self.values.heap_size()
            }
        }

        impl<'a, CV: crate::AsBytes<'a>> crate::AsBytes<'a> for I128s<CV> {
            #[inline(always)]
            fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
                self.values.as_bytes()
            }
        }

        impl<'a, CV: crate::FromBytes<'a>> crate::FromBytes<'a> for I128s<CV> {
            #[inline(always)]
            fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
                Self { values: CV::from_bytes(bytes) }
            }
        }
    }

    /// Columnar stores for non-decreasing `u64`, stored in various ways.
    ///
    /// The venerable `Vec<u64>` works as a general container for arbitrary offests,
    /// but it can be non-optimal for various patterns of offset, including constant
    /// inter-offset spacing, and relatively short runs (compared to a `RankSelect`).
    pub mod offsets {

        pub use array::Fixeds;
        pub use stride::Strides;

        /// An offset container that encodes a constant spacing in its type.
        ///
        /// Any attempt to push any value will result in pushing the next value
        /// at the specified spacing. This type is only appropriate in certain
        /// contexts, for example when storing `[T; K]` array types, or having
        /// introspected a `Strides` and found it to be only one constant stride.
        mod array {

            use crate::{Container, Borrow, Index, Len, Push};
            use crate::common::index::CopyAs;

            /// An offset container that encodes a constant `K` spacing.
            #[derive(Copy, Clone, Debug, Default)]
            pub struct Fixeds<const K: u64, CC = u64> { pub count: CC }

            impl<const K: u64> Borrow for Fixeds<K> {
                type Ref<'a> = u64;
                type Borrowed<'a> = Fixeds<K, &'a u64>;
                #[inline(always)]
                fn borrow<'a>(&'a self) -> Self::Borrowed<'a> { Fixeds { count: &self.count } }
                #[inline(always)]
                fn reborrow<'b, 'a: 'b>(thing: Self::Borrowed<'a>) -> Self::Borrowed<'b> where Self: 'a {
                    Fixeds { count: thing.count }
                }
                #[inline(always)]
                fn reborrow_ref<'b, 'a: 'b>(thing: Self::Ref<'a>) -> Self::Ref<'b> where Self: 'a { thing }
            }

            impl<const K: u64> Container for Fixeds<K> {
                #[inline(always)]
                fn extend_from_self(&mut self, _other: Self::Borrowed<'_>, range: std::ops::Range<usize>) {
                    self.count += range.len() as u64;
                }

                fn reserve_for<'a, I>(&mut self, _selves: I) where Self: 'a, I: Iterator<Item = Self::Borrowed<'a>> + Clone { }
            }

            impl<const K: u64, CC: CopyAs<u64>> Len for Fixeds<K, CC> {
                #[inline(always)] fn len(&self) -> usize { self.count.copy_as() as usize }
            }

            impl<const K: u64, CC> Index for Fixeds<K, CC> {
                type Ref = u64;
                #[inline(always)]
                fn get(&self, index: usize) -> Self::Ref { (index as u64 + 1) * K }
            }
            impl<'a, const K: u64, CC> Index for &'a Fixeds<K, CC> {
                type Ref = u64;
                #[inline(always)]
                fn get(&self, index: usize) -> Self::Ref { (index as u64 + 1) * K }
            }

            impl<'a, const K: u64, T> Push<T> for Fixeds<K> {
                // TODO: check for overflow?
                #[inline(always)]
                fn push(&mut self, _item: T) { self.count += 1; }
                #[inline(always)]
                fn extend(&mut self, iter: impl IntoIterator<Item=T>) {
                    self.count += iter.into_iter().count() as u64;
                }
            }

            impl<const K: u64> crate::HeapSize for Fixeds<K> {
                #[inline(always)]
                fn heap_size(&self) -> (usize, usize) { (0, 0) }
            }
            impl<const K: u64> crate::Clear for Fixeds<K> {
                #[inline(always)]
                fn clear(&mut self) { self.count = 0; }
            }

            impl<'a, const K: u64> crate::AsBytes<'a> for Fixeds<K, &'a u64> {
                #[inline(always)]
                fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
                    std::iter::once((8, bytemuck::cast_slice(std::slice::from_ref(self.count))))
                }
            }
            impl<'a, const K: u64> crate::FromBytes<'a> for Fixeds<K, &'a u64> {
                #[inline(always)]
                fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
                    Self { count: &bytemuck::try_cast_slice(bytes.next().expect("Iterator exhausted prematurely")).unwrap()[0] }
                }
            }

            use super::Strides;
            impl<const K: u64, BC: Len, CC: CopyAs<u64>> std::convert::TryFrom<Strides<BC, CC>> for Fixeds<K, CC> {
                // On error we return the original.
                type Error = Strides<BC, CC>;
                fn try_from(item: Strides<BC, CC>) -> Result<Self, Self::Error> {
                    if item.strided() == Some(K) { Ok( Self { count: item.length } ) } else { Err(item) }
                }
            }
        }

        /// An general offset container optimized for fixed inter-offset sizes.
        ///
        /// Although it can handle general offsets, it starts with the optimistic
        /// assumption that the offsets will be evenly spaced from zero, and while
        /// that holds it will maintain the stride and length. Should it stop being
        /// true, when a non-confirming offset is pushed, it will start to store
        /// the offsets in a general container.
        mod stride {

            use std::ops::Deref;
            use crate::{Container, Borrow, Index, Len, Push, Clear, AsBytes, FromBytes};
            use crate::common::index::CopyAs;

            /// The first two integers describe a stride pattern, [stride, length].
            ///
            /// If the length is zero the collection is empty. The first `item` pushed
            /// always becomes the first list element. The next element is the number of
            /// items at position `i` whose value is `item * (i+1)`. After this comes
            /// the remaining entries in the bounds container.
            #[derive(Copy, Clone, Debug, Default)]
            pub struct Strides<BC = Vec<u64>, CC = u64> {
                pub stride: CC,
                pub length: CC,
                pub bounds: BC,
            }

            impl Borrow for Strides {
                type Ref<'a> = u64;
                type Borrowed<'a> = Strides<&'a [u64], &'a u64>;

                #[inline(always)] fn borrow<'a>(&'a self) -> Self::Borrowed<'a> { Strides { stride: &self.stride, length: &self.length, bounds: &self.bounds[..] } }
                /// Reborrows the borrowed type to a shorter lifetime. See [`Columnar::reborrow`] for details.
                #[inline(always)] fn reborrow<'b, 'a: 'b>(item: Self::Borrowed<'a>) -> Self::Borrowed<'b> where Self: 'a {
                    Strides { stride: item.stride, length: item.length, bounds: item.bounds }
                }
                /// Reborrows the borrowed type to a shorter lifetime. See [`Columnar::reborrow`] for details.
                 #[inline(always)]fn reborrow_ref<'b, 'a: 'b>(item: Self::Ref<'a>) -> Self::Ref<'b> where Self: 'a { item }
            }

            impl Container for Strides {
                fn reserve_for<'a, I>(&mut self, selves: I) where Self: 'a, I: Iterator<Item = Self::Borrowed<'a>> + Clone {
                    self.bounds.reserve_for(selves.map(|x| x.bounds))
                }
            }

            impl<'a> Push<&'a u64> for Strides { #[inline(always)] fn push(&mut self, item: &'a u64) { self.push(*item) } }
            impl Push<u64> for Strides { #[inline(always)] fn push(&mut self, item: u64) { self.push(item) } }
            impl Clear for Strides { #[inline(always)] fn clear(&mut self) { self.clear() } }

            impl<BC: Len, CC: CopyAs<u64>> Len for Strides<BC, CC> {
                #[inline(always)]
                fn len(&self) -> usize { self.length.copy_as() as usize + self.bounds.len() }
            }
            impl Index for Strides<&[u64], &u64> {
                type Ref = u64;
                #[inline(always)]
                fn get(&self, index: usize) -> Self::Ref {
                    let index = index as u64;
                    if index < *self.length { (index+1) * self.stride } else { self.bounds[(index - self.length) as usize] }
                }
            }

            impl<'a, BC: AsBytes<'a>> AsBytes<'a> for Strides<BC, &'a u64> {
                #[inline(always)]
                fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
                    let stride = std::iter::once((8, bytemuck::cast_slice(std::slice::from_ref(self.stride))));
                    let length = std::iter::once((8, bytemuck::cast_slice(std::slice::from_ref(self.length))));
                    let bounds = self.bounds.as_bytes();
                    crate::chain(stride, crate::chain(length, bounds))
                }
            }
            impl<'a, BC: FromBytes<'a>> FromBytes<'a> for Strides<BC, &'a u64> {
                #[inline(always)]
                fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
                    let stride = &bytemuck::try_cast_slice(bytes.next().expect("Iterator exhausted prematurely")).unwrap()[0];
                    let length = &bytemuck::try_cast_slice(bytes.next().expect("Iterator exhausted prematurely")).unwrap()[0];
                    let bounds = BC::from_bytes(bytes);
                    Self { stride, length, bounds }
                }
            }

            impl Strides {
                pub fn new(stride: u64, length: u64) -> Self {
                    Self { stride, length, bounds: Vec::default() }
                }
                #[inline(always)]
                pub fn push(&mut self, item: u64) {
                    if self.length == 0 {
                        self.stride = item;
                        self.length = 1;
                    }
                    else if !self.bounds.is_empty() {
                        self.bounds.push(item);
                    }
                    else if item == self.stride * (self.length + 1) {
                        self.length += 1;
                    }
                    else {
                        self.bounds.push(item);
                    }
                }
                #[inline(always)]
                pub fn clear(&mut self) {
                    self.stride = 0;
                    self.length = 0;
                    self.bounds.clear();
                }
            }

            impl<BC: Deref<Target=[u64]>, CC: CopyAs<u64>> Strides<BC, CC> {
                #[inline(always)]
                pub fn bounds(&self, index: usize) -> (usize, usize) {
                    let stride = self.stride.copy_as();
                    let length = self.length.copy_as();
                    let index = index as u64;
                    let lower = if index == 0 { 0 } else {
                        let index = index - 1;
                        if index < length { (index+1) * stride } else { self.bounds[(index - length) as usize] }
                    } as usize;
                    let upper = if index < length { (index+1) * stride } else { self.bounds[(index - length) as usize] } as usize;
                    (lower, upper)
                }
            }
            impl<BC: Len, CC: CopyAs<u64>> Strides<BC, CC> {
                #[inline(always)] pub fn strided(&self) -> Option<u64> {
                    if self.bounds.is_empty() {
                        Some(self.stride.copy_as())
                    }
                    else { None }
                }
            }
        }

        #[cfg(test)]
        mod test {
            #[test]
            fn round_trip() {

                use crate::common::{Index, Push, Len};
                use crate::{Borrow, Vecs};
                use crate::primitive::offsets::{Strides, Fixeds};

                let mut cols = Vecs::<Vec::<i32>, Strides>::default();
                for i in 0 .. 100 {
                    cols.push(&[1i32, 2, i]);
                }

                let cols = Vecs {
                    bounds: TryInto::<Fixeds<3>>::try_into(cols.bounds).unwrap(),
                    values: cols.values,
                };

                assert_eq!(cols.borrow().len(), 100);
                for i in 0 .. 100 {
                    assert_eq!(cols.borrow().get(i).len(), 3);
                }

                let mut cols = Vecs {
                    bounds: Strides::new(3, cols.bounds.count),
                    values: cols.values
                };

                cols.push(&[0, 0]);
                assert!(TryInto::<Fixeds<3>>::try_into(cols.bounds).is_err());
            }
        }
    }

    pub use empty::Empties;
    /// A columnar store for `()`.
    mod empty {

        use crate::common::index::CopyAs;
        use crate::{Clear, Columnar, Container, Len, IndexMut, Index, Push, HeapSize, Borrow};

        #[derive(Copy, Clone, Debug, Default)]
        pub struct Empties<CC = u64> { pub count: CC, pub empty: () }

        impl Columnar for () {
            #[inline(always)]
            fn into_owned<'a>(_other: crate::Ref<'a, Self>) -> Self { }
            type Container = Empties;
        }

        impl Borrow for Empties {
            type Ref<'a> = ();
            type Borrowed<'a> = Empties<&'a u64>;
            #[inline(always)]
            fn borrow<'a>(&'a self) -> Self::Borrowed<'a> { Empties { count: &self.count, empty: () } }
            #[inline(always)]
            fn reborrow<'b, 'a: 'b>(thing: Self::Borrowed<'a>) -> Self::Borrowed<'b> where Self: 'a {
                Empties { count: thing.count, empty: () }
            }
            #[inline(always)]
            fn reborrow_ref<'b, 'a: 'b>(thing: Self::Ref<'a>) -> Self::Ref<'b> where Self: 'a { thing }
        }

        impl Container for Empties {
            #[inline(always)]
            fn extend_from_self(&mut self, _other: Self::Borrowed<'_>, range: std::ops::Range<usize>) {
                self.count += range.len() as u64;
            }

            fn reserve_for<'a, I>(&mut self, _selves: I) where Self: 'a, I: Iterator<Item = Self::Borrowed<'a>> + Clone { }
        }

        impl<CC: CopyAs<u64>> Len for Empties<CC> {
            #[inline(always)] fn len(&self) -> usize { self.count.copy_as() as usize }
        }
        impl<CC> IndexMut for Empties<CC> {
            type IndexMut<'a> = &'a mut () where CC: 'a;
            // TODO: panic if out of bounds?
            #[inline(always)] fn get_mut(&mut self, _index: usize) -> Self::IndexMut<'_> { &mut self.empty }
        }
        impl<CC> Index for Empties<CC> {
            type Ref = ();
            #[inline(always)]
            fn get(&self, _index: usize) -> Self::Ref { }
        }
        impl<'a, CC> Index for &'a Empties<CC> {
            type Ref = &'a ();
            #[inline(always)]
            fn get(&self, _index: usize) -> Self::Ref { &() }
        }
        impl Push<()> for Empties {
            // TODO: check for overflow?
            #[inline(always)]
            fn push(&mut self, _item: ()) { self.count += 1; }
            #[inline(always)]
            fn extend(&mut self, iter: impl IntoIterator<Item=()>) {
                self.count += iter.into_iter().count() as u64;
            }
        }
        impl<'a> Push<&'a ()> for Empties {
            // TODO: check for overflow?
            #[inline(always)]
            fn push(&mut self, _item: &()) { self.count += 1; }
            #[inline(always)]
            fn extend(&mut self, iter: impl IntoIterator<Item=&'a ()>) {
                self.count += iter.into_iter().count() as u64;
            }
        }

        impl HeapSize for Empties {
            #[inline(always)]
            fn heap_size(&self) -> (usize, usize) { (0, 0) }
        }
        impl Clear for Empties {
            #[inline(always)]
            fn clear(&mut self) { self.count = 0; }
        }

        impl<'a> crate::AsBytes<'a> for crate::primitive::Empties<&'a u64> {
            #[inline(always)]
            fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
                std::iter::once((8, bytemuck::cast_slice(std::slice::from_ref(self.count))))
            }
        }
        impl<'a> crate::FromBytes<'a> for crate::primitive::Empties<&'a u64> {
            #[inline(always)]
            fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
                Self { count: &bytemuck::try_cast_slice(bytes.next().expect("Iterator exhausted prematurely")).unwrap()[0], empty: () }
            }
        }
    }

    pub use boolean::Bools;
    /// A columnar store for `bool`.
    mod boolean {

        use crate::common::index::CopyAs;
        use crate::{Container, Clear, Len, Index, IndexAs, Push, HeapSize, Borrow};

        /// A store for maintaining `Vec<bool>`.
        #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
        #[derive(Copy, Clone, Debug, Default, PartialEq)]
        pub struct Bools<VC = Vec<u64>, WC = u64> {
            /// The bundles of bits that form complete `u64` values.
            pub values: VC,
            /// The work-in-progress bits that are not yet complete.
            pub last_word: WC,
            /// The number of set bits in `bits.last()`.
            pub last_bits: WC,
        }

        impl crate::Columnar for bool {
            #[inline(always)]
            fn into_owned<'a>(other: crate::Ref<'a, Self>) -> Self { other }
            type Container = Bools;
        }

        impl<VC: crate::common::BorrowIndexAs<u64>> Borrow for Bools<VC> {
            type Ref<'a> = bool;
            type Borrowed<'a> = Bools<VC::Borrowed<'a>, &'a u64> where VC: 'a;
            #[inline(always)]
            fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
                Bools {
                    values: self.values.borrow(),
                    last_word: &self.last_word,
                    last_bits: &self.last_bits,
                }
            }
            #[inline(always)]
            fn reborrow<'b, 'a: 'b>(thing: Self::Borrowed<'a>) -> Self::Borrowed<'b> where VC: 'a {
                Bools {
                    values: VC::reborrow(thing.values),
                    last_word: thing.last_word,
                    last_bits: thing.last_bits,
                }
            }
            #[inline(always)]
            fn reborrow_ref<'b, 'a: 'b>(thing: Self::Ref<'a>) -> Self::Ref<'b> where Self: 'a { thing }
        }

        impl<VC: crate::common::PushIndexAs<u64>> Container for Bools<VC> {
            // TODO: There is probably a smart way to implement `extend_from_slice`, but it isn't trivial due to alignment.

            fn reserve_for<'a, I>(&mut self, selves: I) where Self: 'a, I: Iterator<Item = Self::Borrowed<'a>> + Clone {
                self.values.reserve_for(selves.map(|x| x.values))
            }
        }

        impl<'a, VC: crate::AsBytes<'a>> crate::AsBytes<'a> for crate::primitive::Bools<VC, &'a u64> {
            #[inline(always)]
            fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
                let iter = self.values.as_bytes();
                let iter = crate::chain_one(iter, (std::mem::align_of::<u64>() as u64, bytemuck::cast_slice(std::slice::from_ref(self.last_word))));
                crate::chain_one(iter, (std::mem::align_of::<u64>() as u64, bytemuck::cast_slice(std::slice::from_ref(self.last_bits))))
            }
        }

        impl<'a, VC: crate::FromBytes<'a>> crate::FromBytes<'a> for crate::primitive::Bools<VC, &'a u64> {
            #[inline(always)]
            fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
                let values = crate::FromBytes::from_bytes(bytes);
                let last_word = &bytemuck::try_cast_slice(bytes.next().expect("Iterator exhausted prematurely")).unwrap()[0];
                let last_bits = &bytemuck::try_cast_slice(bytes.next().expect("Iterator exhausted prematurely")).unwrap()[0];
                Self { values, last_word, last_bits }
            }
        }

        impl<VC: Len, WC: CopyAs<u64>> Len for Bools<VC, WC> {
            #[inline(always)] fn len(&self) -> usize { self.values.len() * 64 + (self.last_bits.copy_as() as usize) }
        }

        impl<VC: Len + IndexAs<u64>, WC: CopyAs<u64>> Index for Bools<VC, WC> {
            type Ref = bool;
            #[inline(always)] fn get(&self, index: usize) -> Self::Ref {
                let block = index / 64;
                let word = if block == self.values.len() {
                    self.last_word.copy_as()
                } else {
                    self.values.index_as(block)
                };
                let bit = index % 64;
                (word >> bit) & 1 == 1
            }
        }

        impl<VC: Len + IndexAs<u64>, WC: CopyAs<u64>> Index for &Bools<VC, WC> {
            type Ref = bool;
            #[inline(always)] fn get(&self, index: usize) -> Self::Ref {
                (*self).get(index)
            }
        }

        impl<VC: for<'a> Push<&'a u64>> Push<bool> for Bools<VC> {
            #[inline]
            fn push(&mut self, bit: bool) {
                self.last_word |= (bit as u64) << self.last_bits;
                self.last_bits += 1;
                // If we have a fully formed word, commit it to `self.values`.
                if self.last_bits == 64 {
                    self.values.push(&self.last_word);
                    self.last_word = 0;
                    self.last_bits = 0;
                }
            }
        }
        impl<'a, VC: for<'b> Push<&'b u64>> Push<&'a bool> for Bools<VC> {
            #[inline(always)]
            fn push(&mut self, bit: &'a bool) {
                self.push(*bit)
            }
        }


        impl<VC: Clear> Clear for Bools<VC> {
            #[inline(always)]
            fn clear(&mut self) {
                self.values.clear();
                self.last_word = 0;
                self.last_bits = 0;
            }
        }

        impl<VC: HeapSize> HeapSize for Bools<VC> {
            #[inline(always)]
            fn heap_size(&self) -> (usize, usize) {
                self.values.heap_size()
            }
        }
    }

    pub use duration::Durations;
    /// A columnar store for `std::time::Duration`.
    mod duration {

        use std::time::Duration;
        use crate::{Container, Len, Index, IndexAs, Push, Clear, HeapSize, Borrow};

        // `std::time::Duration` is equivalent to `(u64, u32)`, corresponding to seconds and nanoseconds.
        #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
        #[derive(Copy, Clone, Debug, Default, PartialEq)]
        pub struct Durations<SC = Vec<u64>, NC = Vec<u32>> {
            pub seconds: SC,
            pub nanoseconds: NC,
        }

        impl crate::Columnar for Duration {
            #[inline(always)]
            fn into_owned<'a>(other: crate::Ref<'a, Self>) -> Self { other }
            type Container = Durations;
        }

        impl<SC: crate::common::BorrowIndexAs<u64>, NC: crate::common::BorrowIndexAs<u32>> Borrow for Durations<SC, NC> {
            type Ref<'a> = Duration;
            type Borrowed<'a> = Durations<SC::Borrowed<'a>, NC::Borrowed<'a>> where SC: 'a, NC: 'a;
            #[inline(always)]
            fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
                Durations {
                    seconds: self.seconds.borrow(),
                    nanoseconds: self.nanoseconds.borrow(),
                }
            }
            #[inline(always)]
            fn reborrow<'b, 'a: 'b>(thing: Self::Borrowed<'a>) -> Self::Borrowed<'b> where SC: 'a, NC: 'a {
                Durations {
                    seconds: SC::reborrow(thing.seconds),
                    nanoseconds: NC::reborrow(thing.nanoseconds),
                }
            }
            #[inline(always)]
            fn reborrow_ref<'b, 'a: 'b>(thing: Self::Ref<'a>) -> Self::Ref<'b> where Self: 'a { thing }
        }

        impl<SC: crate::common::PushIndexAs<u64>, NC: crate::common::PushIndexAs<u32>> Container for Durations<SC, NC> {
            #[inline(always)]
            fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: std::ops::Range<usize>) {
                self.seconds.extend_from_self(other.seconds, range.clone());
                self.nanoseconds.extend_from_self(other.nanoseconds, range);
            }

            fn reserve_for<'a, I>(&mut self, selves: I) where Self: 'a, I: Iterator<Item = Self::Borrowed<'a>> + Clone {
                self.seconds.reserve_for(selves.clone().map(|x| x.seconds));
                self.nanoseconds.reserve_for(selves.map(|x| x.nanoseconds));
            }
        }

        impl<'a, SC: crate::AsBytes<'a>, NC: crate::AsBytes<'a>> crate::AsBytes<'a> for crate::primitive::Durations<SC, NC> {
            #[inline(always)]
            fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
                crate::chain(self.seconds.as_bytes(), self.nanoseconds.as_bytes())
            }
        }
        impl<'a, SC: crate::FromBytes<'a>, NC: crate::FromBytes<'a>> crate::FromBytes<'a> for crate::primitive::Durations<SC, NC> {
            #[inline(always)]
            fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
                Self {
                    seconds: crate::FromBytes::from_bytes(bytes),
                    nanoseconds: crate::FromBytes::from_bytes(bytes),
                }
            }
        }

        impl<SC: Len, NC> Len for Durations<SC, NC> {
            #[inline(always)] fn len(&self) -> usize { self.seconds.len() }
        }

        impl<SC: IndexAs<u64>, NC: IndexAs<u32>> Index for Durations<SC, NC> {
            type Ref = Duration;
            #[inline(always)] fn get(&self, index: usize) -> Self::Ref {
                Duration::new(self.seconds.index_as(index), self.nanoseconds.index_as(index))
            }
        }

        impl<SC: for<'a> Push<&'a u64>, NC: for<'a> Push<&'a u32>> Push<std::time::Duration> for Durations<SC, NC> {
            #[inline]
            fn push(&mut self, item: std::time::Duration) {
                self.seconds.push(&item.as_secs());
                self.nanoseconds.push(&item.subsec_nanos());
            }
        }
        impl<'a, SC: for<'b> Push<&'b u64>, NC: for<'b> Push<&'b u32>> Push<&'a std::time::Duration> for Durations<SC, NC> {
            #[inline]
            fn push(&mut self, item: &'a std::time::Duration) {
                self.push(*item)
            }
        }
        impl<'a, SC: Push<&'a u64>, NC: Push<&'a u32>> Push<(&'a u64, &'a u32)> for Durations<SC, NC> {
            #[inline]
            fn push(&mut self, item: (&'a u64, &'a u32)) {
                self.seconds.push(item.0);
                self.nanoseconds.push(item.1);
            }
        }

        impl<SC: Clear, NC: Clear> Clear for Durations<SC, NC> {
            #[inline(always)]
            fn clear(&mut self) {
                self.seconds.clear();
                self.nanoseconds.clear();
            }
        }

        impl<SC: HeapSize, NC: HeapSize> HeapSize for Durations<SC, NC> {
            #[inline(always)]
            fn heap_size(&self) -> (usize, usize) {
                let (l0, c0) = self.seconds.heap_size();
                let (l1, c1) = self.nanoseconds.heap_size();
                (l0 + l1, c0 + c1)
            }
        }
    }
}

pub use string::Strings;
pub mod string {

    use super::{Clear, Columnar, Container, Len, Index, IndexAs, Push, HeapSize, Borrow};

    /// A stand-in for `Vec<String>`.
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[derive(Copy, Clone, Debug, Default, PartialEq)]
    pub struct Strings<BC = Vec<u64>, VC = Vec<u8>> {
        /// Bounds container; provides indexed access to offsets.
        pub bounds: BC,
        /// Values container; provides slice access to bytes.
        pub values: VC,
    }

    impl Columnar for String {
        #[inline(always)]
        fn copy_from<'a>(&mut self, other: crate::Ref<'a, Self>) {
            self.clear();
            self.push_str(other);
        }
        #[inline(always)]
        fn into_owned<'a>(other: crate::Ref<'a, Self>) -> Self { other.to_string() }
        type Container = Strings;
    }

    impl Columnar for Box<str> {
        #[inline(always)]
        fn copy_from<'a>(&mut self, other: crate::Ref<'a, Self>) {
            let mut s = String::from(std::mem::take(self));
            s.clear();
            s.push_str(other);
            *self = s.into_boxed_str();
        }
        #[inline(always)]
        fn into_owned<'a>(other: crate::Ref<'a, Self>) -> Self { Self::from(other) }
        type Container = Strings;
    }

    impl<BC: crate::common::BorrowIndexAs<u64>> Borrow for Strings<BC, Vec<u8>> {
        type Ref<'a> = &'a str;
        type Borrowed<'a> = Strings<BC::Borrowed<'a>, &'a [u8]> where BC: 'a;
        #[inline(always)]
        fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
            Strings {
                bounds: self.bounds.borrow(),
                values: self.values.borrow(),
            }
        }
        #[inline(always)]
        fn reborrow<'c, 'a: 'c>(thing: Self::Borrowed<'a>) -> Self::Borrowed<'c> where BC: 'a {
            Strings {
                bounds: BC::reborrow(thing.bounds),
                values: thing.values,
            }
        }
        #[inline(always)]
        fn reborrow_ref<'b, 'a: 'b>(thing: Self::Ref<'a>) -> Self::Ref<'b> where Self: 'a { thing }
    }

    impl<BC: crate::common::PushIndexAs<u64>> Container for Strings<BC, Vec<u8>> {
        #[inline(always)]
        fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: std::ops::Range<usize>) {
            if !range.is_empty() {
                // Imported bounds will be relative to this starting offset.
                let values_len = self.values.len() as u64;

                // Push all bytes that we can, all at once.
                let other_lower = if range.start == 0 { 0 } else { other.bounds.index_as(range.start-1) };
                let other_upper = other.bounds.index_as(range.end-1);
                self.values.extend_from_self(other.values, other_lower as usize .. other_upper as usize);

                // Each bound needs to be shifted by `values_len - other_lower`.
                if values_len == other_lower {
                    self.bounds.extend_from_self(other.bounds, range);
                }
                else {
                    for index in range {
                        let shifted = other.bounds.index_as(index) - other_lower + values_len;
                        self.bounds.push(&shifted)
                    }
                }
            }
        }

        fn reserve_for<'a, I>(&mut self, selves: I) where Self: 'a, I: Iterator<Item = Self::Borrowed<'a>> + Clone {
            self.bounds.reserve_for(selves.clone().map(|x| x.bounds));
            self.values.reserve_for(selves.map(|x| x.values));
        }

    }

    impl<'a, BC: crate::AsBytes<'a>, VC: crate::AsBytes<'a>> crate::AsBytes<'a> for Strings<BC, VC> {
        #[inline(always)]
        fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
            crate::chain(self.bounds.as_bytes(), self.values.as_bytes())
        }
    }
    impl<'a, BC: crate::FromBytes<'a>, VC: crate::FromBytes<'a>> crate::FromBytes<'a> for Strings<BC, VC> {
        #[inline(always)]
        fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
            Self {
                bounds: crate::FromBytes::from_bytes(bytes),
                values: crate::FromBytes::from_bytes(bytes),
            }
        }
    }

    impl<BC: Len, VC> Len for Strings<BC, VC> {
        #[inline(always)] fn len(&self) -> usize { self.bounds.len() }
    }

    impl<'a, BC: Len+IndexAs<u64>> Index for Strings<BC, &'a [u8]> {
        type Ref = &'a str;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref {
            let lower = if index == 0 { 0 } else { self.bounds.index_as(index - 1) };
            let upper = self.bounds.index_as(index);
            let lower: usize = lower.try_into().expect("bounds must fit in `usize`");
            let upper: usize = upper.try_into().expect("bounds must fit in `usize`");
            std::str::from_utf8(&self.values[lower .. upper]).expect("&[u8] must be valid utf8")
        }
    }
    impl<'a, BC: Len+IndexAs<u64>> Index for &'a Strings<BC, Vec<u8>> {
        type Ref = &'a str;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref {
            let lower = if index == 0 { 0 } else { self.bounds.index_as(index - 1) };
            let upper = self.bounds.index_as(index);
            let lower: usize = lower.try_into().expect("bounds must fit in `usize`");
            let upper: usize = upper.try_into().expect("bounds must fit in `usize`");
            std::str::from_utf8(&self.values[lower .. upper]).expect("&[u8] must be valid utf8")
        }
    }

    // This is a simpler implementation, but it leads to a performance regression
    // for Strings and str because it loses access to `Vec::extend_from_slice`.
    //
    // impl<BC: Push<u64>, D: std::fmt::Display> Push<D> for Strings<BC> {
    //     #[inline(always)]
    //     fn push(&mut self, item: D) {
    //         use std::io::Write;
    //         write!(self.values, "{}", item).unwrap();
    //         self.bounds.push(self.values.len() as u64);
    //     }
    // }

    impl<BC: for<'a> Push<&'a u64>> Push<&String> for Strings<BC> {
        #[inline(always)] fn push(&mut self, item: &String) {
            self.values.extend_from_slice(item.as_bytes());
            self.bounds.push(&(self.values.len() as u64));
        }
    }
    impl<BC: for<'a> Push<&'a u64>> Push<&str> for Strings<BC> {
        #[inline]
        fn push(&mut self, item: &str) {
            self.values.extend_from_slice(item.as_bytes());
            self.bounds.push(&(self.values.len() as u64));
        }
    }
    impl<BC: for<'a> Push<&'a u64>> Push<&Box<str>> for Strings<BC> {
        #[inline]
        fn push(&mut self, item: &Box<str>) {
            self.values.extend_from_slice(item.as_bytes());
            self.bounds.push(&(self.values.len() as u64));
        }
    }
    impl<'a, BC: for<'b> Push<&'b u64>> Push<std::fmt::Arguments<'a>> for Strings<BC> {
        #[inline]
        fn push(&mut self, item: std::fmt::Arguments<'a>) {
            use std::io::Write;
            self.values.write_fmt(item).expect("write_fmt failed");
            self.bounds.push(&(self.values.len() as u64));
        }
    }
    impl<'a, 'b, BC: for<'c> Push<&'c u64>> Push<&'b std::fmt::Arguments<'a>> for Strings<BC> {
        #[inline]
        fn push(&mut self, item: &'b std::fmt::Arguments<'a>) {
            use std::io::Write;
            self.values.write_fmt(*item).expect("write_fmt failed");
            self.bounds.push(&(self.values.len() as u64));
        }
    }
    impl<BC: Clear, VC: Clear> Clear for Strings<BC, VC> {
        #[inline(always)]
        fn clear(&mut self) {
            self.bounds.clear();
            self.values.clear();
        }
    }
    impl<BC: HeapSize, VC: HeapSize> HeapSize for Strings<BC, VC> {
        #[inline(always)]
        fn heap_size(&self) -> (usize, usize) {
            let (l0, c0) = self.bounds.heap_size();
            let (l1, c1) = self.values.heap_size();
            (l0 + l1, c0 + c1)
        }
    }
}

pub use vector::Vecs;
pub mod vector {

    use super::{Clear, Columnar, Container, Len, IndexMut, Index, IndexAs, Push, HeapSize, Slice, Borrow};

    /// A stand-in for `Vec<Vec<T>>` for complex `T`.
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[derive(Debug, Default, Copy, Clone, PartialEq)]
    pub struct Vecs<TC, BC = Vec<u64>> {
        pub bounds: BC,
        pub values: TC,
    }

    impl<T: Columnar> Columnar for Vec<T> {
        #[inline(always)]
        fn copy_from<'a>(&mut self, other: crate::Ref<'a, Self>) {
            self.truncate(other.len());
            let mut other_iter = other.into_iter();
            for (s, o) in self.iter_mut().zip(&mut other_iter) {
                T::copy_from(s, o);
            }
            for o in other_iter {
                self.push(T::into_owned(o));
            }
        }
        #[inline(always)]
        fn into_owned<'a>(other: crate::Ref<'a, Self>) -> Self {
            other.into_iter().map(|x| T::into_owned(x)).collect()
        }
        type Container = Vecs<T::Container>;
    }

    impl<T: Columnar, const N: usize> Columnar for [T; N] {
        #[inline(always)]
        fn copy_from<'a>(&mut self, other: crate::Ref<'a, Self>) {
            for (s, o) in self.iter_mut().zip(other.into_iter()) {
                T::copy_from(s, o);
            }
        }
        #[inline(always)]
        fn into_owned<'a>(other: crate::Ref<'a, Self>) -> Self {
            let vec: Vec<_> = other.into_iter().map(|x| T::into_owned(x)).collect();
            match vec.try_into() {
                Ok(array) => array,
                Err(_) => panic!("wrong length"),
            }
        }
        type Container = Vecs<T::Container>;
    }

    impl<T: Columnar, const N: usize> Columnar for smallvec::SmallVec<[T; N]> {
        #[inline(always)]
        fn copy_from<'a>(&mut self, other: crate::Ref<'a, Self>) {
            self.truncate(other.len());
            let mut other_iter = other.into_iter();
            for (s, o) in self.iter_mut().zip(&mut other_iter) {
                T::copy_from(s, o);
            }
            for o in other_iter {
                self.push(T::into_owned(o));
            }
        }
        #[inline(always)]
        fn into_owned<'a>(other: crate::Ref<'a, Self>) -> Self {
            other.into_iter().map(|x| T::into_owned(x)).collect()
        }
        type Container = Vecs<T::Container>;
    }

    impl<BC: crate::common::BorrowIndexAs<u64>, TC: Container> Borrow for Vecs<TC, BC> {
        type Ref<'a> = Slice<TC::Borrowed<'a>> where TC: 'a;
        type Borrowed<'a> = Vecs<TC::Borrowed<'a>, BC::Borrowed<'a>> where BC: 'a, TC: 'a;
        #[inline(always)]
        fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
            Vecs {
                bounds: self.bounds.borrow(),
                values: self.values.borrow(),
            }
        }
        #[inline(always)]
        fn reborrow<'b, 'a: 'b>(thing: Self::Borrowed<'a>) -> Self::Borrowed<'b> where BC: 'a, TC: 'a {
            Vecs {
                bounds: BC::reborrow(thing.bounds),
                values: TC::reborrow(thing.values),
            }
        }
        #[inline(always)]
        fn reborrow_ref<'b, 'a: 'b>(thing: Self::Ref<'a>) -> Self::Ref<'b> where Self: 'a {
            thing.map(|x| TC::reborrow(x))
        }
    }

    impl<BC: crate::common::PushIndexAs<u64>, TC: Container> Container for Vecs<TC, BC> {
        #[inline(always)]
        fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: std::ops::Range<usize>) {
            if !range.is_empty() {
                // Imported bounds will be relative to this starting offset.
                let values_len = self.values.len() as u64;

                // Push all bytes that we can, all at once.
                let other_lower = if range.start == 0 { 0 } else { other.bounds.index_as(range.start-1) };
                let other_upper = other.bounds.index_as(range.end-1);
                self.values.extend_from_self(other.values, other_lower as usize .. other_upper as usize);

                // Each bound needs to be shifted by `values_len - other_lower`.
                if values_len == other_lower {
                    self.bounds.extend_from_self(other.bounds, range);
                }
                else {
                    for index in range {
                        let shifted = other.bounds.index_as(index) - other_lower + values_len;
                        self.bounds.push(&shifted)
                    }
                }
            }
        }

        fn reserve_for<'a, I>(&mut self, selves: I) where Self: 'a, I: Iterator<Item = Self::Borrowed<'a>> + Clone {
            self.bounds.reserve_for(selves.clone().map(|x| x.bounds));
            self.values.reserve_for(selves.map(|x| x.values));
        }
    }

    impl<'a, TC: crate::AsBytes<'a>, BC: crate::AsBytes<'a>> crate::AsBytes<'a> for Vecs<TC, BC> {
        #[inline(always)]
        fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
            crate::chain(self.bounds.as_bytes(), self.values.as_bytes())
        }
    }
    impl<'a, TC: crate::FromBytes<'a>, BC: crate::FromBytes<'a>> crate::FromBytes<'a> for Vecs<TC, BC> {
        #[inline(always)]
        fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
            Self {
                bounds: crate::FromBytes::from_bytes(bytes),
                values: crate::FromBytes::from_bytes(bytes),
            }
        }
    }

    impl<TC: Len> Vecs<TC> {
        #[inline]
        pub fn push_iter<I>(&mut self, iter: I) where I: IntoIterator, TC: Push<I::Item> {
            self.values.extend(iter);
            self.bounds.push(self.values.len() as u64);
        }
    }

    impl<TC, BC: Len> Len for Vecs<TC, BC> {
        #[inline(always)] fn len(&self) -> usize { self.bounds.len() }
    }

    impl<TC: Copy, BC: Len+IndexAs<u64>> Index for Vecs<TC, BC> {
        type Ref = Slice<TC>;
        #[inline(always)]
        fn get(&self, index: usize) -> Self::Ref {
            let lower = if index == 0 { 0 } else { self.bounds.index_as(index - 1) };
            let upper = self.bounds.index_as(index);
            Slice::new(lower, upper, self.values)
        }
    }
    impl<'a, TC, BC: Len+IndexAs<u64>> Index for &'a Vecs<TC, BC> {
        type Ref = Slice<&'a TC>;
        #[inline(always)]
        fn get(&self, index: usize) -> Self::Ref {
            let lower = if index == 0 { 0 } else { self.bounds.index_as(index - 1) };
            let upper = self.bounds.index_as(index);
            Slice::new(lower, upper, &self.values)
        }
    }
    impl<TC, BC: Len+IndexAs<u64>> IndexMut for Vecs<TC, BC> {
        type IndexMut<'a> = Slice<&'a mut TC> where TC: 'a, BC: 'a;

        #[inline(always)]
        fn get_mut(&mut self, index: usize) -> Self::IndexMut<'_> {
            let lower = if index == 0 { 0 } else { self.bounds.index_as(index - 1) };
            let upper = self.bounds.index_as(index);
            Slice::new(lower, upper, &mut self.values)
        }
    }

    impl<'a, TC: Container, BC: for<'b> Push<&'b u64>> Push<Slice<TC::Borrowed<'a>>> for Vecs<TC, BC> {
        #[inline]
        fn push(&mut self, item: Slice<TC::Borrowed<'a>>) {
            self.values.extend_from_self(item.slice, item.lower .. item.upper);
            self.bounds.push(&(self.values.len() as u64));
        }
    }

    impl<I: IntoIterator, TC: Push<I::Item> + Len, BC: for<'a> Push<&'a u64>> Push<I> for Vecs<TC, BC> {
        #[inline]
        fn push(&mut self, item: I) {
            self.values.extend(item);
            self.bounds.push(&(self.values.len() as u64));
        }
    }

    impl<TC: Clear, BC: Clear> Clear for Vecs<TC, BC> {
        #[inline(always)]
        fn clear(&mut self) {
            self.bounds.clear();
            self.values.clear();
        }
    }

    impl<TC: HeapSize, BC: HeapSize> HeapSize for Vecs<TC, BC> {
        fn heap_size(&self) -> (usize, usize) {
            let (l0, c0) = self.bounds.heap_size();
            let (l1, c1) = self.values.heap_size();
            (l0 + l1, c0 + c1)
        }
    }
}

#[allow(non_snake_case)]
pub mod tuple {

    use crate::*;

    // Implementations for tuple types.
    // These are all macro based, because the implementations are very similar.
    // The macro requires two names, one for the store and one for pushable types.
    macro_rules! tuple_impl {
        ( $($name:ident,$name2:ident,$idx:tt)+) => (

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
                fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: std::ops::Range<usize>) {
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
                #[inline(always)]
                fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
                    let ($($name,)*) = self;
                    let iter = None.into_iter();
                    $( let iter = crate::chain(iter, $name.as_bytes()); )*
                    iter
                }
            }
            impl<'a, $($name: crate::FromBytes<'a>),*> crate::FromBytes<'a> for ($($name,)*) {
                #[inline(always)]
                #[allow(non_snake_case)]
                fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
                    $(let $name = crate::FromBytes::from_bytes(bytes);)*
                    ($($name,)*)
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
            impl<$($name: HeapSize),*> HeapSize for ($($name,)*) {
                #[inline(always)]
                fn heap_size(&self) -> (usize, usize) {
                    let ($($name,)*) = self;
                    let mut l = 0;
                    let mut c = 0;
                    $(let (l0, c0) = $name.heap_size(); l += l0; c += c0;)*
                    (l, c)
                }
            }
            impl<$($name: Index),*> Index for ($($name,)*) {
                type Ref = ($($name::Ref,)*);
                #[inline(always)]
                fn get(&self, index: usize) -> Self::Ref {
                    let ($($name,)*) = self;
                    ($($name.get(index),)*)
                }
            }
            impl<'a, $($name),*> Index for &'a ($($name,)*) where $( &'a $name: Index),* {
                type Ref = ($(<&'a $name as Index>::Ref,)*);
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

    tuple_impl!(A,AA,0);
    tuple_impl!(A,AA,0 B,BB,1);
    tuple_impl!(A,AA,0 B,BB,1 C,CC,2);
    tuple_impl!(A,AA,0 B,BB,1 C,CC,2 D,DD,3);
    tuple_impl!(A,AA,0 B,BB,1 C,CC,2 D,DD,3 E,EE,4);
    tuple_impl!(A,AA,0 B,BB,1 C,CC,2 D,DD,3 E,EE,4 F,FF,5);
    tuple_impl!(A,AA,0 B,BB,1 C,CC,2 D,DD,3 E,EE,4 F,FF,5 G,GG,6);
    tuple_impl!(A,AA,0 B,BB,1 C,CC,2 D,DD,3 E,EE,4 F,FF,5 G,GG,6 H,HH,7);
    tuple_impl!(A,AA,0 B,BB,1 C,CC,2 D,DD,3 E,EE,4 F,FF,5 G,GG,6 H,HH,7 I,II,8);
    tuple_impl!(A,AA,0 B,BB,1 C,CC,2 D,DD,3 E,EE,4 F,FF,5 G,GG,6 H,HH,7 I,II,8 J,JJ,9);

    #[cfg(test)]
    mod test {
        #[test]
        fn round_trip() {

            use crate::common::{Index, Push, HeapSize, Len};

            let mut column: crate::ContainerOf<(u64, u8, String)> = Default::default();
            for i in 0..100 {
                column.push((i, i as u8, &i.to_string()));
                column.push((i, i as u8, &"".to_string()));
            }

            assert_eq!(column.len(), 200);
            assert_eq!(column.heap_size(), (3590, 4608));

            for i in 0..100u64 {
                assert_eq!((&column).get((2*i+0) as usize), (&i, &(i as u8), i.to_string().as_str()));
                assert_eq!((&column).get((2*i+1) as usize), (&i, &(i as u8), ""));
            }

            // Compare to the heap size of a `Vec<Option<usize>>`.
            let mut column: Vec<(u64, u8, String)> = Default::default();
            for i in 0..100 {
                column.push((i, i as u8, i.to_string()));
                column.push((i, i as u8, "".to_string()));
            }
            // NB: Rust seems to change the capacities across versions (1.88 != 1.89),
            // so we just compare the allocated regions to avoid updating the MSRV.
            assert_eq!(column.heap_size().0, 8190);

        }
    }
}

pub use sums::{rank_select::RankSelect, result::Results, option::Options};
/// Containers for enumerations ("sum types") that store variants separately.
///
/// The main work of these types is storing a discriminant and index efficiently,
/// as containers for each of the variant types can hold the actual data.
pub mod sums {

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
}

pub use lookback::{Repeats, Lookbacks};
/// Containers that can store either values, or offsets to prior values.
///
/// This has the potential to be more efficient than a list of `T` when many values repeat in
/// close proximity. Values must be equatable, and the degree of lookback can be configured.
pub mod lookback {

    use crate::{Options, Results, Push, Index, Len, HeapSize};

    /// A container that encodes repeated values with a `None` variant, at the cost of extra bits for every record.
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[derive(Clone, Debug, Default, PartialEq)]
    pub struct Repeats<TC, const N: u8 = 255> {
        /// Some(x) encodes a value, and None indicates the prior `x` value.
        pub inner: Options<TC>,
    }

    impl<T: PartialEq, TC: Push<T> + Len, const N: u8> Push<T> for Repeats<TC, N>
    where
        for<'a> &'a TC: Index,
        for<'a> <&'a TC as Index>::Ref : PartialEq<T>,
    {
        #[inline]
        fn push(&mut self, item: T) {
            // Look at the last `somes` value for a potential match.
            let insert: Option<T> = if (&self.inner.somes).last().map(|x| x.eq(&item)) == Some(true) {
                None
            } else {
                Some(item)
            };
            self.inner.push(insert);
        }
    }

    impl<TC: Len, const N: u8> Len for Repeats<TC, N> {
        #[inline(always)] fn len(&self) -> usize { self.inner.len() }
    }

    impl<TC: Index, const N: u8> Index for Repeats<TC, N> {
        type Ref = TC::Ref;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref {
            match self.inner.get(index) {
                Some(item) => item,
                None => {
                    let pos = self.inner.indexes.rank(index) - 1;
                    self.inner.somes.get(pos)
                },
            }
        }
    }

    impl<TC: HeapSize, const N: u8> HeapSize for Repeats<TC, N> {
        fn heap_size(&self) -> (usize, usize) {
            self.inner.heap_size()
        }
    }

    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[derive(Clone, Debug, Default, PartialEq)]
    pub struct Lookbacks<TC, VC = Vec<u8>, const N: u8 = 255> {
        /// Ok(x) encodes a value, and Err(y) indicates a value `y` back.
        pub inner: Results<TC, VC>,
    }

    impl<T: PartialEq, TC: Push<T> + Len, VC: Push<u8>, const N: u8> Push<T> for Lookbacks<TC, VC, N>
    where
        for<'a> &'a TC: Index,
        for<'a> <&'a TC as Index>::Ref : PartialEq<T>,
    {
        #[inline]
        fn push(&mut self, item: T) {
            // Look backwards through (0 .. N) to look for a matching value.
            let oks_len = self.inner.oks.len();
            let find = (0u8 .. N).take(self.inner.oks.len()).find(|i| (&self.inner.oks).get(oks_len - (*i as usize) - 1) == item);
            let insert: Result<T, u8> = if let Some(back) = find { Err(back) } else { Ok(item) };
            self.inner.push(insert);
        }
    }

    impl<TC, VC, const N: u8> Len for Lookbacks<TC, VC, N> {
        #[inline(always)] fn len(&self) -> usize { self.inner.len() }
    }

    impl<TC: Index, VC: Index<Ref=u8>, const N: u8> Index for Lookbacks<TC, VC, N> {
        type Ref = TC::Ref;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref {
            match self.inner.get(index) {
                Ok(item) => item,
                Err(back) => {
                    let pos = self.inner.indexes.rank(index) - 1;
                    self.inner.oks.get(pos - (back as usize))
                },
            }
        }
    }
    impl<'a, TC, const N: u8> Index for &'a Lookbacks<TC, Vec<u8>, N>
    where
        &'a TC: Index,
    {
        type Ref = <&'a TC as Index>::Ref;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref {
            match (&self.inner).get(index) {
                Ok(item) => item,
                Err(back) => {
                    let pos = self.inner.indexes.rank(index) - 1;
                    (&self.inner.oks).get(pos - (*back as usize))
                },
            }
        }
    }

    impl<TC: HeapSize, VC: HeapSize, const N: u8> HeapSize for Lookbacks<TC, VC, N> {
        fn heap_size(&self) -> (usize, usize) {
            self.inner.heap_size()
        }
    }
}

/// Roaring bitmap (and similar) containers.
pub mod roaring {

    use crate::Results;

    /// A container for `bool` that uses techniques from Roaring bitmaps.
    ///
    /// These techniques are to block the bits into blocks of 2^16 bits,
    /// and to encode each block based on its density. Either a bitmap
    /// for dense blocks or a list of set bits for sparse blocks.
    ///
    /// Additionally, other representations encode runs of set bits.
    pub struct RoaringBits {
        _inner: Results<[u64; 1024], Vec<u16>>,
    }
}

pub use chain_mod::{chain, chain_one};

pub mod chain_mod {
    //! Chain iterators, or iterators and an item. Iterators that might improve inlining, at the
    //! expense of not providing iterator maker traits.

    /// Chain two iterators together. The result first iterates over `a`, then `b`, until both are
    /// exhausted.
    ///
    /// This addresses a quirk where deep iterators would not be optimized to their full potential.
    /// Here, functions are marked with `#[inline(always)]` to indicate that the compiler should
    /// try hard to inline the iterators.
    #[inline(always)]
    pub fn chain<A: IntoIterator, B: IntoIterator<Item=A::Item>>(a: A, b: B) -> Chain<A::IntoIter, B::IntoIter> {
        Chain { a: Some(a.into_iter()), b: Some(b.into_iter()) }
    }

    pub struct Chain<A, B> {
        a: Option<A>,
        b: Option<B>,
    }

    impl<A, B> Iterator for Chain<A, B>
    where
        A: Iterator,
        B: Iterator<Item=A::Item>,
    {
        type Item = A::Item;

        #[inline(always)]
        fn next(&mut self) -> Option<Self::Item> {
            if let Some(a) = self.a.as_mut() {
                let x = a.next();
                if x.is_none() {
                    self.a = None;
                } else {
                    return x;
                }
            }
            if let Some(b) = self.b.as_mut() {
                let x = b.next();
                if x.is_none() {
                    self.b = None;
                } else {
                    return x;
                }
            }
            None
        }

        #[inline]
        fn fold<Acc, F>(self, mut acc: Acc, mut f: F) -> Acc
        where
            F: FnMut(Acc, Self::Item) -> Acc,
        {
            if let Some(a) = self.a {
                acc = a.fold(acc, &mut f);
            }
            if let Some(b) = self.b {
                acc = b.fold(acc, f);
            }
            acc
        }
    }

    /// Chain a single item to an iterator. The resulting iterator first iterates over `a`,
    /// then `b`. The resulting iterator is marked as `#[inline(always)]`, which in some situations
    /// causes better inlining behavior with current Rust versions.
    #[inline(always)]
    pub fn chain_one<A: IntoIterator>(a: A, b: A::Item) -> ChainOne<A::IntoIter> {
        ChainOne { a: Some(a.into_iter()), b: Some(b) }
    }

    pub struct ChainOne<A: Iterator> {
        a: Option<A>,
        b: Option<A::Item>,
    }

    impl<A: Iterator> Iterator for ChainOne<A> {
        type Item = A::Item;

        #[inline(always)]
        fn next(&mut self) -> Option<Self::Item> {
            if let Some(a) = self.a.as_mut() {
                let x = a.next();
                if x.is_none() {
                    self.a = None;
                    self.b.take()
                } else {
                    x
                }
            } else {
                None
            }
        }
    }

    #[cfg(test)]
    mod test {
        use super::*;

        #[test]
        fn test_chain() {
            let a = [1, 2, 3];
            let b = [4, 5, 6];
            let mut chain = chain(a, b);
            assert_eq!(chain.next(), Some(1));
            assert_eq!(chain.next(), Some(2));
            assert_eq!(chain.next(), Some(3));
            assert_eq!(chain.next(), Some(4));
            assert_eq!(chain.next(), Some(5));
            assert_eq!(chain.next(), Some(6));
            assert_eq!(chain.next(), None);
        }

        #[test]
        fn test_chain_one() {
            let a = [1, 2, 3];
            let b = 4;
            let mut chain = chain_one(a, b);
            assert_eq!(chain.next(), Some(1));
            assert_eq!(chain.next(), Some(2));
            assert_eq!(chain.next(), Some(3));
            assert_eq!(chain.next(), Some(4));
            assert_eq!(chain.next(), None);
        }
    }
}
