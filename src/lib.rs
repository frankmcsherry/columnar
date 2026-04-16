//! Types supporting flat / "columnar" layout for complex types.
//!
//! The intent is to re-layout `Vec<T>` types into vectors of reduced
//! complexity, repeatedly. One should be able to push and pop easily,
//! but indexing will be more complicated because we likely won't have
//! a real `T` lying around to return as a reference. Instead, we will
//! use Generic Associated Types (GATs) to provide alternate references.

#![no_std]
#[macro_use]
extern crate alloc;
#[cfg(feature = "std")]
extern crate std;
use alloc::vec::Vec;

// Re-export derive crate.
extern crate columnar_derive;
pub use columnar_derive::Columnar;

pub mod adts;
pub mod boxed;
pub mod bytes;
pub mod lookback;
pub mod primitive;
pub mod string;
pub mod sums;
pub mod vector;
pub mod tuple;
mod arc;
mod rc;

pub use bytemuck;

pub use vector::Vecs;
pub use string::Strings;
pub use sums::{rank_select::RankSelect, result::Results, option::Options, discriminant::Discriminant};
pub use lookback::{Repeats, Lookbacks};

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

/// The borrowed container type of columnar type `T`.
///
/// Equivalent to `<<T as Columnar>::Container> as Borrow>::Borrowed<'a>`.
pub type BorrowedOf<'a, T> = <ContainerOf<T> as Borrow>::Borrowed<'a>;

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
    ///
    /// Implementations should most likely be marked `#[inline(always)]`.
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
    fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: core::ops::Range<usize>) {
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
    fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: core::ops::Range<usize>) {
        self.extend_from_slice(&other[range])
    }
    fn reserve_for<'a, I>(&mut self, selves: I) where Self: 'a, I: Iterator<Item = Self::Borrowed<'a>> + Clone {
        self.reserve(selves.map(|x| x.len()).sum::<usize>())
    }
}

/// A container that can also be viewed as and reconstituted from bytes.
pub trait ContainerBytes : Container + for<'a> Borrow<Borrowed<'a> : AsBytes<'a> + FromBytes<'a>> { }
impl<C: Container + for<'a> Borrow<Borrowed<'a> : AsBytes<'a> + FromBytes<'a>>> ContainerBytes for C { }

pub use common::{Clear, Len, Push, IndexMut, Index, IndexAs, Slice, AsBytes, FromBytes};
/// Common traits and types that are re-used throughout the module.
pub mod common {

    use alloc::{vec::Vec, string::String};

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
    impl<T, const N: usize> Len for [T; N] {
        #[inline(always)] fn len(&self) -> usize { N }
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
            core::iter::Extend::extend(self, iter)
        }
    }
    impl<'a, T: Clone> Push<&'a T> for Vec<T> {
        #[inline(always)] fn push(&mut self, item: &'a T) { self.push(item.clone()) }

        #[inline(always)]
        fn extend(&mut self, iter: impl IntoIterator<Item=&'a T>) {
            core::iter::Extend::extend(self, iter.into_iter().cloned())
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

        use alloc::vec::Vec;
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
        ///
        /// # Performance
        ///
        /// A call to `get(index)` will attempt to access member slices at `index`,
        /// and as these could panic the optimizer cannot eliminate them even if you
        /// do not then go on to examine the values. If you plan to access a field
        /// (for tuples or structs) or variant match (for enums) you should perform
        /// this before calling `get(index)` when able.
        pub trait Index {
            /// The type returned by the `get` method.
            ///
            /// This trait is most often implemented for lifetimed containers, and the `Ref` type
            /// will have a lifetime that depends on that of the containers, rather than `&self`.
            type Ref;
            /// Iterator type for sequential access over a range.
            ///
            /// Types with efficient sequential strategies (e.g. [`Repeats`](crate::Repeats))
            /// override this with a specialized iterator. Others use [`DefaultCursor`].
            type Cursor<'a>: Iterator<Item = Self::Ref> where Self: 'a;
            /// Returns the reference type for location `index`.
            ///
            /// Implementations should most likely be marked `#[inline(always)]`.
            /// If possible, avoid the potential to panic in these implementations,
            /// as this prevents Rust/LLVM from eliding the test even if the return
            /// value is not actually consumed.
            fn get(&self, index: usize) -> Self::Ref;
            /// Returns an iterator over `range` that may be more efficient than
            /// repeated `get()` calls.
            ///
            /// A cursor pre-validates the range once at creation time. Callers must
            /// ensure `range` is within bounds.
            fn cursor(&self, range: core::ops::Range<usize>) -> Self::Cursor<'_>;
            #[inline(always)] fn last(&self) -> Option<Self::Ref> where Self: Len {
                if self.is_empty() { None }
                else { Some(self.get(self.len()-1)) }
            }
            /// Returns a cursor over all elements.
            #[inline(always)]
            fn cursor_iter(&self) -> Self::Cursor<'_> where Self: Len {
                self.cursor(0..self.len())
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
            type Cursor<'b> = core::slice::Iter<'a, T> where Self: 'b;
            #[inline(always)] fn get(&self, index: usize) -> Self::Ref { &self[index] }
            #[inline(always)] fn cursor(&self, range: core::ops::Range<usize>) -> Self::Cursor<'_> {
                self[range].iter()
            }
        }
        impl<T: Copy> Index for [T] {
            type Ref = T;
            type Cursor<'a> = core::iter::Copied<core::slice::Iter<'a, T>> where Self: 'a;
            #[inline(always)] fn get(&self, index: usize) -> Self::Ref { self[index] }
            #[inline(always)] fn cursor(&self, range: core::ops::Range<usize>) -> Self::Cursor<'_> {
                self[range].iter().copied()
            }
        }
        impl<T: Copy, const N: usize> Index for [T; N] {
            type Ref = T;
            type Cursor<'a> = core::iter::Copied<core::slice::Iter<'a, T>> where Self: 'a;
            #[inline(always)] fn get(&self, index: usize) -> Self::Ref { self[index] }
            #[inline(always)] fn cursor(&self, range: core::ops::Range<usize>) -> Self::Cursor<'_> {
                self[range].iter().copied()
            }
        }
        impl<'a, T> Index for &'a Vec<T> {
            type Ref = &'a T;
            type Cursor<'b> = core::slice::Iter<'a, T> where Self: 'b;
            #[inline(always)] fn get(&self, index: usize) -> Self::Ref { &self[index] }
            #[inline(always)] fn cursor(&self, range: core::ops::Range<usize>) -> Self::Cursor<'_> {
                self[range].iter()
            }
        }
        impl<T: Copy> Index for Vec<T> {
            type Ref = T;
            type Cursor<'a> = core::iter::Copied<core::slice::Iter<'a, T>> where Self: 'a;
            #[inline(always)] fn get(&self, index: usize) -> Self::Ref { self[index] }
            #[inline(always)] fn cursor(&self, range: core::ops::Range<usize>) -> Self::Cursor<'_> {
                self[range].iter().copied()
            }
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

    /// A struct representing a slice of a range of values.
    ///
    /// The lower and upper bounds should be meaningfully set on construction.
    #[derive(Copy, Clone, Debug)]
    pub struct Slice<S> {
        pub lower: usize,
        pub upper: usize,
        pub slice: S,
    }

    impl<S> core::hash::Hash for Slice<S> where Self: Index<Ref: core::hash::Hash> {
        fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
            self.len().hash(state);
            for i in 0 .. self.len() {
                self.get(i).hash(state);
            }
        }
    }

    impl<S> Slice<S> {
        pub fn slice<R: core::ops::RangeBounds<usize>>(self, range: R) -> Self {
            use core::ops::Bound;
            let lower = match range.start_bound() {
                Bound::Included(s) => core::cmp::max(self.lower, *s),
                Bound::Excluded(s) => core::cmp::max(self.lower, *s+1),
                Bound::Unbounded => self.lower,
            };
            let upper = match range.end_bound() {
                Bound::Included(s) => core::cmp::min(self.upper, *s+1),
                Bound::Excluded(s) => core::cmp::min(self.upper, *s),
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
        fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
            use core::cmp::Ordering;
            let len = core::cmp::min(self.len(), other.len());

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
        fn cmp(&self, other: &Self) -> core::cmp::Ordering {
            use core::cmp::Ordering;
            let len = core::cmp::min(self.len(), other.len());

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
        type Cursor<'a> = S::Cursor<'a> where Self: 'a;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref {
            assert!(index < self.upper - self.lower);
            self.slice.get(self.lower + index)
        }
        #[inline(always)] fn cursor(&self, range: core::ops::Range<usize>) -> Self::Cursor<'_> {
            self.slice.cursor(self.lower + range.start .. self.lower + range.end)
        }
    }
    impl<'a, S> Index for &'a Slice<S>
    where
        &'a S : Index,
    {
        type Ref = <&'a S as Index>::Ref;
        type Cursor<'b> = <&'a S as Index>::Cursor<'b> where Self: 'b;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref {
            assert!(index < self.upper - self.lower);
            (&self.slice).get(self.lower + index)
        }
        #[inline(always)] fn cursor(&self, range: core::ops::Range<usize>) -> Self::Cursor<'_> {
            (&self.slice).cursor(self.lower + range.start .. self.lower + range.end)
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

    /// Cursor that delegates to [`Index::get`] for types without specialized iteration.
    ///
    /// Used as the default `Cursor` type for containers that do not benefit from
    /// a specialized sequential access strategy.
    pub struct DefaultCursor<'a, S: ?Sized> {
        slice: &'a S,
        cursor: usize,
        end: usize,
    }

    impl<'a, S: ?Sized> DefaultCursor<'a, S> {
        #[inline(always)]
        pub fn new(slice: &'a S, range: core::ops::Range<usize>) -> Self {
            DefaultCursor { slice, cursor: range.start, end: range.end }
        }
    }

    impl<'a, S: Index + ?Sized> Iterator for DefaultCursor<'a, S> {
        type Item = S::Ref;
        #[inline(always)]
        fn next(&mut self) -> Option<Self::Item> {
            if self.cursor < self.end {
                let result = self.slice.get(self.cursor);
                self.cursor += 1;
                Some(result)
            } else {
                None
            }
        }
        #[inline(always)]
        fn size_hint(&self) -> (usize, Option<usize>) {
            let remaining = self.end - self.cursor;
            (remaining, Some(remaining))
        }
    }

    impl<S: Index + ?Sized> ExactSizeIterator for DefaultCursor<'_, S> {}

    /// Generates the default `Cursor` and `cursor()` implementation for `Index`.
    ///
    /// Expands to a `type Cursor<'a> = DefaultCursor<'a, Self>` and a `cursor()`
    /// method that wraps `get()`. Use this in `Index` impls for types that do not
    /// benefit from specialized sequential access.
    macro_rules! impl_default_cursor {
        () => {
            type Cursor<'__cursor> = $crate::common::DefaultCursor<'__cursor, Self> where Self: '__cursor;
            #[inline]
            fn cursor(&self, range: core::ops::Range<usize>) -> Self::Cursor<'_> {
                $crate::common::DefaultCursor::new(self, range)
            }
        }
    }
    pub(crate) use impl_default_cursor;

    /// A type that can be viewed as byte slices with lifetime `'a`.
    ///
    /// Implementors of this trait almost certainly reference the lifetime `'a` themselves.
    pub trait AsBytes<'a> {
        /// The number of byte slices this type produces.
        const SLICE_COUNT: usize;
        /// Returns the `index`-th byte slice (alignment, data) by random access.
        ///
        /// Each composite type dispatches on compile-time-constant `SLICE_COUNT`
        /// boundaries, so LLVM can constant-fold the branch chain when the caller
        /// iterates `0..SLICE_COUNT`.
        fn get_byte_slice(&self, index: usize) -> (u64, &'a [u8]);
        /// Presents `self` as a sequence of byte slices, with their required alignment.
        ///
        /// The default implementation iterates `0..SLICE_COUNT` calling `get_byte_slice`.
        /// The return type is always `Map<Range<usize>, ...>` regardless of type complexity.
        #[inline]
        fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
            (0..Self::SLICE_COUNT).map(|i| self.get_byte_slice(i))
        }
    }

    /// A type that can be reconstituted from byte slices with lifetime `'a`.
    ///
    /// Implementors of this trait almost certainly reference the lifetime `'a` themselves,
    /// unless they actively deserialize the bytes (vs sit on the slices, as if zero-copy).
    ///
    /// # Overriding methods
    ///
    /// The only required method is [`from_bytes`](Self::from_bytes). However, the default
    /// implementation of [`from_store`](Self::from_store) falls back through `from_bytes`,
    /// which contains panicking operations that prevent LLVM from eliminating unused fields.
    /// Implementors should override `from_store` and [`element_sizes`](Self::element_sizes)
    /// to ensure optimal codegen. Missing overrides are functionally correct but silently
    /// degrade performance. The `#[derive(Columnar)]` macro generates all overrides
    /// automatically.
    pub trait FromBytes<'a> {
        /// The number of byte slices this type consumes when reconstructed.
        const SLICE_COUNT: usize;
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
        /// Reconstructs `self` from a [`DecodedStore`](crate::bytes::indexed::DecodedStore),
        /// using direct random access at a given offset.
        ///
        /// Each field indexes directly into the store at its compile-time-known offset,
        /// with no iterator state or sequential dependency. This enables LLVM to fully
        /// eliminate unused fields.
        #[inline(always)]
        fn from_store(store: &crate::bytes::indexed::DecodedStore<'a>, offset: &mut usize) -> Self where Self: Sized {
            // Default: decode each slice from the store and delegate to from_bytes.
            let start = *offset;
            *offset += Self::SLICE_COUNT;
            Self::from_bytes(&mut (start..*offset).map(|i| {
                let (w, tail) = store.get(i);
                let bytes: &[u8] = bytemuck::cast_slice(w);
                let len = if tail == 0 { bytes.len() } else { bytes.len() - (8 - tail as usize) };
                &bytes[..len]
            }))
        }
        /// Reports the element sizes (in bytes) for each slice this type consumes.
        ///
        /// Implementors should override this to report their actual element sizes.
        /// For example, `&[u32]` pushes `4`, while a tuple delegates to each field.
        /// The default returns `Err`, so that [`validate`](Self::validate) rejects
        /// data for types that have not implemented this method.
        fn element_sizes(sizes: &mut Vec<usize>) -> Result<(), String> {
            let _ = sizes;
            Err(format!("element_sizes not implemented for this type (SLICE_COUNT = {})", Self::SLICE_COUNT))
        }
        /// Validates that the given slices are compatible with this type.
        ///
        /// The input provides `(&[u64], u8)` pairs: each is a word slice
        /// and trailing byte count. This type consumes `Self::SLICE_COUNT` entries and checks
        /// that each slice's byte length is a multiple of its element size.
        ///
        /// Built from [`Self::element_sizes`]; generally should not need to be overridden.
        fn validate(slices: &[(&[u64], u8)]) -> Result<(), String> where Self: Sized {
            if slices.len() < Self::SLICE_COUNT {
                return Err(format!("expected {} slices but got {}", Self::SLICE_COUNT, slices.len()));
            }
            let mut sizes = Vec::new();
            Self::element_sizes(&mut sizes)?;
            for (i, elem_size) in sizes.iter().enumerate() {
                let (words, tail) = &slices[i];
                let byte_len = words.len() * 8 - ((8 - *tail as usize) % 8);
                if byte_len % elem_size != 0 {
                    return Err(format!(
                        "slice {} has {} bytes, not a multiple of element size {}",
                        i, byte_len, elem_size
                    ));
                }
            }
            Ok(())
        }
    }

}

/// Roaring bitmap (and similar) containers.
pub mod roaring {

    use alloc::vec::Vec;
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

