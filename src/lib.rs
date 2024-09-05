//! Types supporting flat / "columnar" layout for complex types.
//!
//! The intent is to re-layout `Vec<T>` types into vectors of reduced
//! complexity, repeatedly. One should be able to push and pop easily,
//! but indexing will be more complicated because we likely won't have
//! a real `T` lying around to return as a reference. Instead, we will
//! use Generic Associated Types (GATs) to provide alternate indexes.

/// A stand in for `Vec<T>` with a different layout
pub trait Columnar<T: ?Sized> : Len + Index {
    /// Pushes an owned item onto `self`.
    #[inline(always)] fn push(&mut self, item: T) where T: Sized { self.copy(&item) }
    /// Copy a reference to an item into `self`.
    fn copy(&mut self, item: &T);
    /// Copy a slice of items into `self`.
    ///
    /// This is an opportunity to provide a faster implementation if appropriate,
    /// and if not the default implementation copies each element in the slice.
    #[inline(always)] fn copy_slice(&mut self, slice: &[T]) where T: Sized {
        for item in slice.iter() {
            self.copy(item);
        }
    }
    /// Removes and returns the most recently pushed item.
    fn pop(&mut self) -> Option<T> where T: Sized { unimplemented!() }

    /// Removes all records of elements, but retains allocations.
    fn clear(&mut self);
    /// Active (len) and allocated (cap) heap sizes in bytes.
    /// This should not include the size of `self` itself.
    fn heap_size(&self) -> (usize, usize);

    fn iter(&self) -> IndexIter<'_, Self> {
        IndexIter {
            index: 0,
            slice: &self
        }
    }
}

/// A type that can be represented in columnar form.
pub trait Columnable {
    type Columns: Columnar<Self> + Default;
    /// Converts a vector of the type into columnar form.
    fn as_columns(selves: Vec<Self>) -> Self::Columns where Self: Sized {
        let mut columns: Self::Columns = Default::default();
        for item in selves {
            columns.push(item);
        }
        columns
    }
}

/// A type with a length.
pub trait Len {
    /// The number of contained elements.
    fn len(&self) -> usize;
    /// Whether this contains no elements.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A type that can accept items of type `T`.
pub trait Push<T> {
    /// Pushes an item onto `self`.
    fn push(&mut self, item: T);
}

/// A type that can be accessed by `usize`.
pub trait Index {
    /// Type referencing an indexed element.
    type Index<'a> where Self: 'a;
    fn index(&self, index: usize) -> Self::Index<'_>;
    /// A reference to the last element, should one exist.
    #[inline(always)] fn last(&self) -> Option<Self::Index<'_>> where Self: Len {
        if self.is_empty() { None }
        else { Some(self.index(self.len()-1)) }
    }
}

impl<'t, T: Index> Index for &'t T {
    type Index<'a> = T::Index<'t> where Self: 'a;
    #[inline(always)] fn index(&self, index: usize) -> Self::Index<'_> {
        T::index(*self, index)
    }
}

/// A type that can be mutably accessed by `usize`.
pub trait IndexMut : Index {
    /// Type mutably referencing an indexed element.
    type IndexMut<'a> where Self: 'a;
    fn index_mut(& mut self, index: usize) -> Self::IndexMut<'_>;
    /// A reference to the last element, should one exist.
    #[inline(always)] fn last_mut(&mut self) -> Option<Self::IndexMut<'_>> where Self: Len {
        if self.is_empty() { None }
        else { Some(self.index_mut(self.len()-1)) }
    }
}

/// A struct representing a slice of a range of values.
///
/// The lower and upper bounds should be meaningfully set on construction.
pub struct Slice<S> {
    lower: usize,
    upper: usize,
    slice: S,
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
    pub fn new(lower: usize, upper: usize, slice: S) -> Self {
        Self { lower, upper, slice }
    }
}

impl<S> Len for Slice<S> {
    fn len(&self) -> usize { self.upper - self.lower }
}

impl<S: Index> Index for Slice<S> {
    type Index<'a> = S::Index<'a> where S: 'a;
    #[inline(always)] fn index(&self, index: usize) -> Self::Index<'_> {
        assert!(index < self.upper - self.lower);
        self.slice.index(self.lower + index)
    }
}

impl<S: IndexMut> IndexMut for Slice<S> {
    type IndexMut<'a> = S::IndexMut<'a> where S: 'a;
    #[inline(always)] fn index_mut(&mut self, index: usize) -> Self::IndexMut<'_> {
        assert!(index < self.upper - self.lower);
        self.slice.index_mut(self.lower + index)
    }
}


impl<S: Index> Slice<S> {
    pub fn iter(&self) -> IndexIter<'_, Self> {
        IndexIter {
            index: 0,
            slice: self
        }
    }
}

impl<'a, S: Index> IntoIterator for &'a Slice<S> {
    type Item = S::Index<'a>;
    type IntoIter = IndexIter<'a, Slice<S>>;
    #[inline(always)] fn into_iter(self) -> Self::IntoIter {
        IndexIter {
            index: 0,
            slice: self
        }
    }
}

pub struct IndexIter<'a, S: ?Sized> {
    index: usize,
    slice: &'a S,
}

impl<'a, S: Index + Len> Iterator for IndexIter<'a, S> {
    type Item = S::Index<'a>;

    #[inline(always)] fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.slice.len() {
            let result = self.slice.index(self.index);
            self.index += 1;
            Some(result)
        } else {
            None
        }
    }
}

// All types that can be cloned can use `Vec`.
// Types that cannot be cloned should be able to use `Vec` once we decouple
// the `copy` and `push` behavior from the trait; they could be pushed, but
// not copied.
//
// Importantly, this implementation *allows* types to use `Vec`, but it does
// not cause them to implement `Columnable` which is where they would express
// an opinion about their preference for storage.
impl<T: Clone> Columnar<T> for Vec<T> {
    #[inline(always)] fn copy(&mut self, item: &T) { self.push(item.clone()); }
    #[inline(always)] fn copy_slice(&mut self, slice: &[T]) { self.extend_from_slice(slice); }
    #[inline(always)] fn pop(&mut self) -> Option<T> { self.pop() }
    #[inline(always)] fn clear(&mut self) { self.clear(); }
    fn heap_size(&self) -> (usize, usize) {
        (
            std::mem::size_of::<T>() * self.len(),
            std::mem::size_of::<T>() * self.capacity(),
        )
    }
}

impl<T> Len for Vec<T> {
    fn len(&self) -> usize { self.len() }
}
impl<T> Push<T> for Vec<T> {
    fn push(&mut self, item: T) { self.push(item) }
}
impl<'a, T: Clone> Push<&'a T> for Vec<T> {
    fn push(&mut self, item: &'a T) { self.push(item.clone()) }
}
impl<'a, T: Clone> Push<&'a [T]> for Vec<T> {
    fn push(&mut self, item: &'a [T]) { self.extend_from_slice(item) }
}
impl<T> Index for Vec<T> {
    type Index<'a> = &'a T where Self: 'a;
    fn index(&self, index: usize) -> Self::Index<'_> { &self[index] }
}
impl<T> IndexMut for Vec<T> {
    type IndexMut<'a> = &'a mut T where Self: 'a;
    fn index_mut(&mut self, index: usize) -> Self::IndexMut<'_> { &mut self[index] }
}
impl<'t, T> Len for &'t [T] {
    fn len(&self) -> usize { <[T]>::len(self) }
}
impl<'t, T> Len for &'t mut [T] {
    fn len(&self) -> usize { <[T]>::len(self) }
}
impl<'t, T> Index for &'t [T] {
    type Index<'a> = &'a T where Self: 'a;
    fn index(&self, index: usize) -> Self::Index<'_> { &self[index] }
}
impl<'t, T> Index for &'t mut [T] {
    type Index<'a> = &'a T where Self: 'a;
    fn index(&self, index: usize) -> Self::Index<'_> { &self[index] }
}
impl<'t, T> IndexMut for &'t mut [T] {
    type IndexMut<'a> = &'a mut T where Self: 'a;
    fn index_mut(&mut self, index: usize) -> Self::IndexMut<'_> { &mut self[index] }
}

/// Types that prefer to be represented by `Vec<T>`.
mod primitive {

    use super::{Columnar, Columnable, Len, Index, IndexMut};

    /// An implementation of opinions for types that want to use `Vec<T>`.
    macro_rules! implement_columnable {
        ($($index_type:ty),*) => { $(
            impl Columnable for $index_type {
                type Columns = Vec<$index_type>;
            }
        )* }
    }

    implement_columnable!(bool, char);
    implement_columnable!(u8, u16, u32, u64, u128, usize);
    implement_columnable!(i8, i16, i32, i64, i128, isize);
    implement_columnable!(f32, f64);
    implement_columnable!(std::time::Duration);

    #[derive(Default)]
    pub struct Empties { count: usize, empty: () }

    impl Len for Empties {
        fn len(&self) -> usize { self.count }
    }
    impl Index for Empties {
        type Index<'a> = &'a ();
        // TODO: panic if out of bounds?
        #[inline(always)] fn index(&self, _index: usize) -> Self::Index<'_> { &() }
    }
    impl IndexMut for Empties {
        type IndexMut<'a> = &'a mut ();
        // TODO: panic if out of bounds?
        #[inline(always)] fn index_mut(&mut self, _index: usize) -> Self::IndexMut<'_> { &mut self.empty }
    }

    impl Columnable for () {
        type Columns = Empties;
    }
    impl Columnar<()> for Empties {
        // TODO: check for overflow?
        #[inline(always)] fn copy(&mut self, _item: &()) { self.count += 1; }
        // TODO: check for overflow?
        #[inline(always)] fn copy_slice(&mut self, slice: &[()]) { self.count += slice.len(); }
        #[inline(always)] fn pop(&mut self) -> Option<()> { if self.count > 0 { self.count -= 1; Some(()) } else { None } }
        #[inline(always)] fn clear(&mut self) { self.count = 0; }
        fn heap_size(&self) -> (usize, usize) { (0, 0) }
    }
}

mod string {

    use super::{Columnar, Columnable, Len, Index, IndexMut};

    impl Columnable for String {
        type Columns = ColumnString;
    }

    /// A stand-in for `Vec<String>`.
    #[derive(Debug, Default)]
    pub struct ColumnString {
        bounds: Vec<usize>,
        values: Vec<u8>,
    }

    impl Len for ColumnString {
        #[inline(always)] fn len(&self) -> usize { self.bounds.len() - 1 }
    }
    impl Index for ColumnString {
        type Index<'a> = &'a [u8];

        fn index(&self, index: usize) -> Self::Index<'_> {
            let lower = self.bounds[index];
            let upper = self.bounds[index + 1];
            &self.values[lower .. upper]
        }
    }
    // Arguably safe, because we don't assume UTF-8, but also off-brand.
    impl IndexMut for ColumnString {
        type IndexMut<'a> = &'a mut [u8];

        fn index_mut(&mut self, index: usize) -> Self::IndexMut<'_> {
            let lower = self.bounds[index];
            let upper = self.bounds[index + 1];
            &mut self.values[lower .. upper]
        }
    }

    impl Columnar<String> for ColumnString {
        #[inline(always)]
        fn copy(&mut self, item: &String) {
            self.values.extend_from_slice(item.as_bytes());
            self.bounds.push(self.values.len());
        }
        fn pop(&mut self) -> Option<String> {
            if self.bounds.len() > 1 {
                self.bounds.pop();
                let start = *self.bounds.last().unwrap();
                let bytes = self.values.split_off(start);
                Some(String::from_utf8(bytes).expect("Invalid bytes encoded"))
            } else {
                None
            }
        }

        fn clear(&mut self) {
            self.bounds.clear();
            self.values.clear();
        }
        fn heap_size(&self) -> (usize, usize) {
            let bl = std::mem::size_of::<usize>() * self.bounds.len();
            let bc = std::mem::size_of::<usize>() * self.bounds.capacity();
            let vl = self.values.len();
            let vc = self.values.capacity();
            (bl + vl, bc + vc)
        }
    }
}

pub use vector::ColumnVec;
mod vector {

    use super::{Columnar, Columnable, Len, Index, IndexMut, Slice};

    impl<T: Columnable> Columnable for Vec<T> {
        type Columns = ColumnVec<T::Columns>;
    }

    /// A stand-in for `Vec<Vec<T>>` for complex `T`.
    #[derive(Debug, Clone)]
    pub struct ColumnVec<TC> {
        bounds: Vec<usize>,
        values: TC,
    }

    impl<TC: Default> Default for ColumnVec<TC> {
        fn default() -> Self {
            Self {
                bounds: vec![0],
                values: TC::default(),
            }
        }
    }

    impl<TC> Len for ColumnVec<TC> {
        #[inline(always)] fn len(&self) -> usize { self.bounds.len() - 1 }
    }

    impl<TC> Index for ColumnVec<TC> {
        type Index<'a> = Slice<&'a TC> where TC: 'a;

        #[inline(always)]
        fn index(&self, index: usize) -> Self::Index<'_> {
            Slice {
                lower: self.bounds[index],
                upper: self.bounds[index + 1],
                slice: &self.values,
            }
        }
    }
    impl<TC> IndexMut for ColumnVec<TC> {
        type IndexMut<'a> = Slice<&'a mut TC> where TC: 'a;

        #[inline(always)]
        fn index_mut(&mut self, index: usize) -> Self::IndexMut<'_> {
            Slice {
                lower: self.bounds[index],
                upper: self.bounds[index + 1],
                slice: &mut self.values,
            }
        }
    }

    impl<T, TC: Columnar<T>> Columnar<Vec<T>> for ColumnVec<TC> {
        #[inline(always)]
        fn copy(&mut self, item: &Vec<T>) {
            self.values.copy_slice(item);
            self.bounds.push(self.values.len());
        }
        fn pop(&mut self) -> Option<Vec<T>> {
            if self.bounds.len() > 1 {
                let last = self.bounds.pop().unwrap();
                let count = last - *self.bounds.last().unwrap();
                let mut result = Vec::with_capacity(count);
                for _ in 0 .. count {
                    result.push(self.values.pop().unwrap());
                }
                result.reverse();
                Some(result)
            } else {
                None
            }
        }

        fn clear(&mut self) {
            self.bounds.clear();
            self.values.clear();
        }

        fn heap_size(&self) -> (usize, usize) {
            let (inner_l, inner_c) = self.values.heap_size();
            (
                std::mem::size_of::<usize>() * self.bounds.len() + inner_l,
                std::mem::size_of::<usize>() * self.bounds.capacity() + inner_c,
            )
        }
    }
}

mod tuple {

    use super::{Columnar, Columnable, Len, Index, IndexMut};

    impl<S: Columnable, T: Columnable> Columnable for (S, T) {
        type Columns = (S::Columns, T::Columns);
    }

    impl<S: Columnable, T: Columnable, R: Columnable> Columnable for (S, T, R) {
        type Columns = (S::Columns, T::Columns, R::Columns);
    }

    impl<SC: Len, TC> Len for (SC, TC) {
        fn len(&self) -> usize { self.0.len() }
    }

    impl<SC: Index, TC: Index> Index for (SC, TC) {
        type Index<'a> = (SC::Index<'a>, TC::Index<'a>) where SC: 'a, TC: 'a;
        fn index(&self, index: usize) -> Self::Index<'_> {
            (self.0.index(index), self.1.index(index))
        }
    }
    impl<SC: IndexMut, TC: IndexMut> IndexMut for (SC, TC) {
        type IndexMut<'a> = (SC::IndexMut<'a>, TC::IndexMut<'a>) where SC: 'a, TC: 'a;
        fn index_mut(&mut self, index: usize) -> Self::IndexMut<'_> {
            (self.0.index_mut(index), self.1.index_mut(index))
        }
    }

    impl<S, SC: Columnar<S>, T, TC: Columnar<T>> Columnar<(S, T)> for (SC, TC) {
        #[inline(always)]
        fn copy(&mut self, item: &(S, T)) {
            self.0.copy(&item.0);
            self.1.copy(&item.1);
        }
        fn pop(&mut self) -> Option<(S, T)> {
            let s_item = self.0.pop();
            let t_item = self.1.pop();
            match (s_item, t_item) {
                (Some(s), Some(t)) => Some((s, t)),
                (None, None) => None,
                _ => panic!("invariant violated"),
            }
        }

        fn clear(&mut self) {
            self.0.clear();
            self.1.clear();
        }
        fn heap_size(&self) -> (usize, usize) {
            let (l0, c0) = self.0.heap_size();
            let (l1, c1) = self.1.heap_size();
            (l0 + l1, c0 + c1)
        }
    }

    impl<SC: Len, TC, RC> Len for (SC, TC, RC) {
        #[inline(always)] fn len(&self) -> usize { self.0.len() }
    }
    impl<SC: Index, TC: Index, RC: Index> Index for (SC, TC, RC) {
        type Index<'a> = (SC::Index<'a>, TC::Index<'a>, RC::Index<'a>) where SC: 'a, TC: 'a, RC: 'a;
        fn index(&self, index: usize) -> Self::Index<'_> {
            (self.0.index(index), self.1.index(index), self.2.index(index))
        }
    }
    impl<SC: IndexMut, TC: IndexMut, RC: IndexMut> IndexMut for (SC, TC, RC) {
        type IndexMut<'a> = (SC::IndexMut<'a>, TC::IndexMut<'a>, RC::IndexMut<'a>) where SC: 'a, TC: 'a, RC: 'a;
        fn index_mut(&mut self, index: usize) -> Self::IndexMut<'_> {
            (self.0.index_mut(index), self.1.index_mut(index), self.2.index_mut(index))
        }
    }

    impl<S, SC: Columnar<S>, T, TC: Columnar<T>, R, RC: Columnar<R>> Columnar<(S, T, R)> for (SC, TC, RC) {
        #[inline(always)]
        fn copy(&mut self, item: &(S, T, R)) {
            self.0.copy(&item.0);
            self.1.copy(&item.1);
            self.2.copy(&item.2);
        }
        fn pop(&mut self) -> Option<(S, T, R)> {
            let s_item = self.0.pop();
            let t_item = self.1.pop();
            let r_item = self.2.pop();
            match (s_item, t_item, r_item) {
                (Some(s), Some(t), Some(r)) => Some((s, t, r)),
                (None, None, None) => None,
                _ => panic!("invariant violated"),
            }
        }

        fn clear(&mut self) {
            self.0.clear();
            self.1.clear();
            self.2.clear();
        }
        fn heap_size(&self) -> (usize, usize) {
            let (l0, c0) = self.0.heap_size();
            let (l1, c1) = self.1.heap_size();
            let (l2, c2) = self.2.heap_size();
            (l0 + l1 + l2, c0 + c1 + c2)
        }
    }
}

mod result {

    use super::{Columnar, Columnable, Len, Index, IndexMut};

    impl<S: Columnable, T: Columnable> Columnable for Result<S, T> {
        type Columns = ColumnResult<S::Columns, T::Columns>;
    }

    pub struct ColumnResult<SC, TC> {
        /// This could be substantially more efficient as e.g. a `Vec<(u64, u64)>`,
        /// with one entry for each 64 items pushed, describing the cumulative sum
        /// of `Ok` variants (say) and a bitfield of the associated variants.
        indexes: Vec<Result<usize, usize>>,
        s_store: SC,
        t_store: TC,
    }

    impl<SC: Default, TC: Default> Default for ColumnResult<SC, TC> {
        fn default() -> Self {
            Self {
                indexes: Vec::default(),
                s_store: SC::default(),
                t_store: TC::default(),
            }
        }
    }

    impl<SC, TC> Len for ColumnResult<SC, TC> {
        #[inline(always)] fn len(&self) -> usize { self.indexes.len() }
    }

    impl<SC: Index, TC: Index> Index for ColumnResult<SC, TC> {
        type Index<'a> = Result<SC::Index<'a>, TC::Index<'a>> where SC: 'a, TC: 'a;
        fn index(&self, index: usize) -> Self::Index<'_> {
            match self.indexes[index] {
                Ok(i) => Ok(self.s_store.index(i)),
                Err(i) => Err(self.t_store.index(i)),
            }
        }
    }
    // NB: You are not allowed to change the variant, but can change its contents.
    impl<SC: IndexMut, TC: IndexMut> IndexMut for ColumnResult<SC, TC> {
        type IndexMut<'a> = Result<SC::IndexMut<'a>, TC::IndexMut<'a>> where SC: 'a, TC: 'a;
        fn index_mut(&mut self, index: usize) -> Self::IndexMut<'_> {
            match self.indexes[index] {
                Ok(i) => Ok(self.s_store.index_mut(i)),
                Err(i) => Err(self.t_store.index_mut(i)),
            }
        }
    }

    impl<S, SC: Columnar<S>, T, TC: Columnar<T>> Columnar<Result<S, T>> for ColumnResult<SC, TC> {
        #[inline(always)]
        fn copy(&mut self, item: &Result<S, T>) {
            match item {
                Ok(item) => {
                    self.indexes.push(Ok(self.s_store.len()));
                    self.s_store.copy(item);
                }
                Err(item) => {
                    self.indexes.push(Ok(self.t_store.len()));
                    self.t_store.copy(item);
                }
            }
        }
        fn pop(&mut self) -> Option<Result<S, T>> {
            self.indexes
                .pop()
                .map(|i| match i {
                    Ok(_) => Ok(self.s_store.pop().unwrap()),
                    Err(_)=> Err(self.t_store.pop().unwrap()),
                })
        }

        fn clear(&mut self) {
            self.indexes.clear();
            self.s_store.clear();
            self.t_store.clear();
        }

        fn heap_size(&self) -> (usize, usize) {
            let (l0, c0) = self.s_store.heap_size();
            let (l1, c1) = self.t_store.heap_size();
            let li = std::mem::size_of::<Result<usize, usize>>() * self.indexes.len();
            let ci = std::mem::size_of::<Result<usize, usize>>() * self.indexes.capacity();
            (l0 + l1 + li, c0 + c1 + ci)
        }
    }
}

mod option {

    use super::{Columnar, Columnable, Len, Index, IndexMut};

    impl<T: Columnable> Columnable for Option<T> {
        type Columns = ColumnOption<T::Columns>;
    }

    pub struct ColumnOption<TC> {
        /// This could be substantially more efficient as e.g. a `Vec<(u64, u64)>`,
        /// with one entry for each 64 items pushed, describing the cumulative sum
        /// of `Some` variants (say) and a bitfield of the associated variants.
        indexes: Vec<Option<usize>>,
        t_store: TC,
    }

    impl<TC: Default> Default for ColumnOption<TC> {
        fn default() -> Self {
            Self {
                indexes: Vec::default(),
                t_store: TC::default(),
            }
        }
    }

    impl<T> Len for ColumnOption<T> {
        #[inline(always)] fn len(&self) -> usize { self.indexes.len() }
    }

    impl<T: Index> Index for ColumnOption<T> {
        type Index<'a> = Option<T::Index<'a>> where T: 'a;
        fn index(&self, index: usize) -> Self::Index<'_> {
            match self.indexes[index] {
                Some(i) => Some(self.t_store.index(i)),
                None => None,
            }
        }
    }
    impl<T: IndexMut> IndexMut for ColumnOption<T> {
        type IndexMut<'a> = Option<T::IndexMut<'a>> where T: 'a;
        fn index_mut(&mut self, index: usize) -> Self::IndexMut<'_> {
            match self.indexes[index] {
                Some(i) => Some(self.t_store.index_mut(i)),
                None => None,
            }
        }
    }

    impl<T, TC: Columnar<T>> Columnar<Option<T>> for ColumnOption<TC> {
        #[inline(always)]
        fn copy(&mut self, item: &Option<T>) {
            match item {
                Some(item) => {
                    self.indexes.push(Some(self.t_store.len()));
                    self.t_store.copy(item);
                }
                None => {
                    self.indexes.push(None);
                }
            }
        }
        fn pop(&mut self) -> Option<Option<T>> {
            self.indexes
                .pop()
                .map(|i| match i {
                    Some(_) => Some(self.t_store.pop().unwrap()),
                    None => None,
                })
        }

        fn clear(&mut self) {
            self.indexes.clear();
            self.t_store.clear();
        }

        fn heap_size(&self) -> (usize, usize) {
            let (l0, c0) = self.t_store.heap_size();
            let li = std::mem::size_of::<Result<usize, usize>>() * self.indexes.len();
            let ci = std::mem::size_of::<Result<usize, usize>>() * self.indexes.capacity();
            (l0 + li, c0 + ci)
        }
    }
}
