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
/// Equivalent to `<ContainerOf<T> as Container>::Ref<'a>`.
pub type Ref<'a, T> = <ContainerOf<T> as Container>::Ref<'a>;

/// A container that can hold `C`, and provide its preferred references.
///
/// As an example, `(Vec<A>, Vecs<Vec<B>>)`.
pub trait Container : Len + Clear + for<'a> Push<Self::Ref<'a>> + Clone + Default + Send + 'static {
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

impl<T: Clone + Send + 'static> Container for Vec<T> {
    type Ref<'a> = &'a T;
    type Borrowed<'a> = &'a [T];
    fn borrow<'a>(&'a self) -> Self::Borrowed<'a> { &self[..] }
    fn reborrow<'b, 'a: 'b>(item: Self::Borrowed<'a>) -> Self::Borrowed<'b> where Self: 'a { item }
    fn reborrow_ref<'b, 'a: 'b>(item: Self::Ref<'a>) -> Self::Ref<'b> where Self: 'a { item }
    fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: std::ops::Range<usize>) {
        self.extend_from_slice(&other[range])
    }
    fn reserve_for<'a, I>(&mut self, selves: I) where Self: 'a, I: Iterator<Item = Self::Borrowed<'a>> + Clone {
        self.reserve(selves.map(|x| x.len()).sum::<usize>())
    }
}

/// A container that can also be viewed as and reconstituted from bytes.
pub trait ContainerBytes : for<'a> Container<Borrowed<'a> : AsBytes<'a> + FromBytes<'a>> { }
impl<C: for<'a> Container<Borrowed<'a> : AsBytes<'a> + FromBytes<'a>>> ContainerBytes for C { }

pub use common::{Clear, Len, Push, IndexMut, Index, IndexAs, HeapSize, Slice, AsBytes, FromBytes};
pub mod common;
pub mod bytes;
pub mod primitive;

pub use string::Strings;
pub mod string;

pub use vector::Vecs;
pub mod vector;

pub mod tuple;

pub use sums::{rank_select::RankSelect, result::Results, option::Options};
pub mod sums;

pub use lookback::{Repeats, Lookbacks};
pub mod lookback;
mod maps;
mod sizes;
pub mod roaring;

pub use chain_mod::{chain, chain_one};
pub mod chain_mod;
