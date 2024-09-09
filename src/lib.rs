//! Types supporting flat / "columnar" layout for complex types.
//!
//! The intent is to re-layout `Vec<T>` types into vectors of reduced
//! complexity, repeatedly. One should be able to push and pop easily,
//! but indexing will be more complicated because we likely won't have
//! a real `T` lying around to return as a reference. Instead, we will
//! use Generic Associated Types (GATs) to provide alternate references.

/// A type that can be represented in columnar form.
pub trait Columnable : Sized {
    /// The type that stores the columnar representation.
    type Columns: Push<Self> + Index + Clear + Default;
    /// Converts a sequence of the type into columnar form.
    fn as_columns<I>(selves: I) -> Self::Columns where I: Iterator<Item = Self>, Self: Sized {
        let mut columns: Self::Columns = Default::default();
        for item in selves {
            columns.push(item);
        }
        columns
    }
}

pub use common::{Clear, Len, Push, Index, IndexMut, HeapSize, Slice, IndexIter};
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

    impl<L: Len> Len for &L {
        #[inline(always)] fn len(&self) -> usize { L::len(*self) }
    }
    impl<L: Len> Len for &mut L {
        #[inline(always)] fn len(&self) -> usize { L::len(*self) }
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

    /// A type that can be accessed by `usize`.
    pub trait Index : Len {
        /// Type referencing an indexed element.
        type Ref<'a> where Self: 'a;
        fn get(&self, index: usize) -> Self::Ref<'_>;
        /// A reference to the last element, should one exist.
        #[inline(always)] fn last(&self) -> Option<Self::Ref<'_>> where Self: Len {
            if self.is_empty() { None }
            else { Some(self.get(self.len()-1)) }
        }
        /// An iterator over references to indexed elements.
        #[inline(always)] fn iter(&self) -> IndexIter<'_, Self> {
            IndexIter {
                index: 0,
                slice: self
            }
        }
    }

    impl<'t, T: Index> Index for &'t T {
        type Ref<'a> = T::Ref<'t> where Self: 'a;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref<'_> {
            T::get(*self, index)
        }
    }

    /// A type that can be mutably accessed by `usize`.
    pub trait IndexMut : Index {
        /// Type mutably referencing an indexed element.
        type IndexMut<'a> where Self: 'a;
        fn get_mut(& mut self, index: usize) -> Self::IndexMut<'_>;
        /// A reference to the last element, should one exist.
        #[inline(always)] fn last_mut(&mut self) -> Option<Self::IndexMut<'_>> where Self: Len {
            if self.is_empty() { None }
            else { Some(self.get_mut(self.len()-1)) }
        }
    }

    // These blanket implementations are unfortunate, in that they return lifetimes that depend
    // on the `&self` borrow, rather than the `'t` lifetime of the type itself. This is a result
    // of relying on the `T::index` and `T::index_mut` functions, which narrow the lifetime.
    impl<'t, T: Index> Index for &'t mut T {
        type Ref<'a> = T::Ref<'a> where Self: 'a;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref<'_> {
            T::get(self, index)
        }
    }
    impl<'t, T: IndexMut> IndexMut for &'t mut T {
        type IndexMut<'a> = T::IndexMut<'a> where Self: 'a;
        #[inline(always)] fn get_mut(&mut self, index: usize) -> Self::IndexMut<'_> {
            T::get_mut(self, index)
        }
    }

    pub trait Clear {
        /// Clears `self`, without changing its capacity.
        fn clear(&mut self);
    }

    pub trait HeapSize {
        /// Active (len) and allocated (cap) heap sizes in bytes.
        /// This should not include the size of `self` itself.
        fn heap_size(&self) -> (usize, usize);
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
        #[inline(always)] fn len(&self) -> usize { self.upper - self.lower }
    }

    impl<S: Index> Index for Slice<S> {
        type Ref<'a> = S::Ref<'a> where S: 'a;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref<'_> {
            assert!(index < self.upper - self.lower);
            self.slice.get(self.lower + index)
        }
    }

    impl<S: IndexMut> IndexMut for Slice<S> {
        type IndexMut<'a> = S::IndexMut<'a> where S: 'a;
        #[inline(always)] fn get_mut(&mut self, index: usize) -> Self::IndexMut<'_> {
            assert!(index < self.upper - self.lower);
            self.slice.get_mut(self.lower + index)
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
        type Item = S::Ref<'a>;
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

    impl<'a, S: ?Sized> IndexIter<'a, S> {
        pub fn new(index: usize, slice: &'a S) -> Self {
            Self { index, slice }
        }
    }

    impl<'a, S: Index + Len> Iterator for IndexIter<'a, S> {
        type Item = S::Ref<'a>;

        #[inline(always)] fn next(&mut self) -> Option<Self::Item> {
            if self.index < self.slice.len() {
                let result = self.slice.get(self.index);
                self.index += 1;
                Some(result)
            } else {
                None
            }
        }
    }
}

// The `Vec<T>` type is a fine form of storage that is not inherently columnar.
// This is ok, and we will still rely on it as a base case for many types.
// Simple types cannot be further reduced, and some complex types (e.g. `Rc<T>`)
// may have no other options but to use `Vec<T>`.
//
// Importantly, this implementation *allows* types to be stored in a  `Vec`, but 
// it does not implement `Columnable` for them nor express a preference for storage.

// This implementation does not chase down the heap contributions of the owned items.
// It *should*, to give a fair assessment vis-a-vis other representations, but it is
// annoying for vectors of types we neither control nor anticipate with `HeapSize`.
impl<T> HeapSize for Vec<T> {
    fn heap_size(&self) -> (usize, usize) {
        let l = std::mem::size_of::<T>() * self.len();
        let c = std::mem::size_of::<T>() * self.capacity();
        // for item in self.iter() {
        //     let (il, ic) = item.heap_size();
        //     l += il;
        //     c += ic;
        // }
        (l, c)
    }
}

impl<T> Len for Vec<T> {
    #[inline(always)] fn len(&self) -> usize { self.len() }
}
impl<T> Push<T> for Vec<T> {
    #[inline(always)] fn push(&mut self, item: T) { self.push(item) }
}
impl<'a, T: Clone> Push<&'a T> for Vec<T> {
    #[inline(always)] fn push(&mut self, item: &'a T) { self.push(item.clone()) }
}
impl<'a, T: Clone> Push<&'a [T]> for Vec<T> {
    #[inline(always)] fn push(&mut self, item: &'a [T]) { self.clone_from_slice(item) }
}
impl<T> Index for Vec<T> {
    type Ref<'a> = &'a T where Self: 'a;
    #[inline(always)] fn get(&self, index: usize) -> Self::Ref<'_> { &self[index] }
}
impl<T> IndexMut for Vec<T> {
    type IndexMut<'a> = &'a mut T where Self: 'a;
    #[inline(always)] fn get_mut(&mut self, index: usize) -> Self::IndexMut<'_> { &mut self[index] }
}
impl<'t, T> Len for &'t [T] {
    #[inline(always)] fn len(&self) -> usize { <[T]>::len(self) }
}
impl<'t, T> Len for &'t mut [T] {
    #[inline(always)] fn len(&self) -> usize { <[T]>::len(self) }
}
impl<'t, T> Index for &'t [T] {
    type Ref<'a> = &'a T where Self: 'a;
    #[inline(always)] fn get(&self, index: usize) -> Self::Ref<'_> { &self[index] }
}
impl<'t, T> Index for &'t mut [T] {
    type Ref<'a> = &'a T where Self: 'a;
    #[inline(always)] fn get(&self, index: usize) -> Self::Ref<'_> { &self[index] }
}
impl<'t, T> IndexMut for &'t mut [T] {
    type IndexMut<'a> = &'a mut T where Self: 'a;
    #[inline(always)] fn get_mut(&mut self, index: usize) -> Self::IndexMut<'_> { &mut self[index] }
}
impl<T> Clear for Vec<T> {
    #[inline(always)] fn clear(&mut self) { self.clear() }
}

/// Types that prefer to be represented by `Vec<T>`.
pub mod primitive {

    use super::{Clear, Columnable, Len, Index, IndexMut, Push, HeapSize};

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
        type Ref<'a> = &'a ();
        // TODO: panic if out of bounds?
        #[inline(always)] fn get(&self, _index: usize) -> Self::Ref<'_> { &() }
    }
    impl IndexMut for Empties {
        type IndexMut<'a> = &'a mut ();
        // TODO: panic if out of bounds?
        #[inline(always)] fn get_mut(&mut self, _index: usize) -> Self::IndexMut<'_> { &mut self.empty }
    }
    impl Push<()> for Empties {
        // TODO: check for overflow?
        fn push(&mut self, _item: ()) { self.count += 1; }
    }
    impl Push<&()> for Empties {
        // TODO: check for overflow?
        fn push(&mut self, _item: &()) { self.count += 1; }
    }

    impl Columnable for () {
        type Columns = Empties;
    }
    impl HeapSize for Empties {
        fn heap_size(&self) -> (usize, usize) { (0, 0) }
    }
    impl Clear for Empties {
        fn clear(&mut self) { self.count = 0; }
    }
}

pub use string::ColumnString;
pub mod string {

    use super::{Clear, Columnable, Len, Index, IndexMut, Push, HeapSize};

    impl Columnable for String {
        type Columns = ColumnString;
    }

    /// A stand-in for `Vec<String>`.
    #[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
    pub struct ColumnString {
        pub bounds: Vec<usize>,
        pub values: Vec<u8>,
    }

    impl Len for ColumnString {
        #[inline(always)] fn len(&self) -> usize { self.bounds.len() }
    }
    impl Index for ColumnString {
        // type Ref<'a> = &'a [u8];
        type Ref<'a> = &'a str;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref<'_> {
            let lower = if index == 0 { 0 } else { self.bounds[index - 1] };
            let upper = self.bounds[index];
            std::str::from_utf8(&self.values[lower .. upper]).unwrap()
        }
    }
    // Arguably safe, because we don't assume UTF-8, but also off-brand.
    impl IndexMut for ColumnString {
        type IndexMut<'a> = &'a mut [u8];
        #[inline(always)] fn get_mut(&mut self, index: usize) -> Self::IndexMut<'_> {
            let lower = if index == 0 { 0 } else { self.bounds[index] };
            let upper = self.bounds[index];
            &mut self.values[lower .. upper]
        }
    }

    impl Push<String> for ColumnString {
        #[inline(always)] fn push(&mut self, item: String) {
            self.values.extend_from_slice(item.as_bytes());
            self.bounds.push(self.values.len());
        }
    }
    impl Push<&String> for ColumnString {
        #[inline(always)] fn push(&mut self, item: &String) {
            self.values.extend_from_slice(item.as_bytes());
            self.bounds.push(self.values.len());
        }
    }
    impl Push<&str> for ColumnString {
        fn push(&mut self, item: &str) {
            self.values.extend_from_slice(item.as_bytes());
            self.bounds.push(self.values.len());
        }
    }
    impl Clear for ColumnString {
        fn clear(&mut self) {
            self.bounds.clear();
            self.values.clear();
        }
    }
    impl HeapSize for ColumnString {
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
pub mod vector {

    use super::{Clear, Columnable, Len, Index, IndexMut, Push, HeapSize, Slice};

    impl<T: Columnable> Columnable for Vec<T> {
        type Columns = ColumnVec<T::Columns>;
    }

    /// A stand-in for `Vec<Vec<T>>` for complex `T`.
    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    pub struct ColumnVec<TC> {
        pub bounds: Vec<usize>,
        pub values: TC,
    }

    impl<TC: Len> ColumnVec<TC> {
        pub fn push_iter<I>(&mut self, iter: I) where I: IntoIterator, TC: Push<I::Item> {
            self.values.extend(iter);
            self.bounds.push(self.values.len());
        }
    }
    
    impl<TC: Default> Default for ColumnVec<TC> {
        fn default() -> Self {
            Self {
                bounds: Vec::default(),
                values: TC::default(),
            }
        }
    }

    impl<TC> Len for ColumnVec<TC> {
        #[inline(always)] fn len(&self) -> usize { self.bounds.len() }
    }

    impl<TC> Index for ColumnVec<TC> {
        type Ref<'a> = Slice<&'a TC> where TC: 'a;

        #[inline(always)]
        fn get(&self, index: usize) -> Self::Ref<'_> {
            let lower = if index == 0 { 0 } else { self.bounds[index - 1] };
            let upper = self.bounds[index];
            Slice::new(lower, upper, &self.values)
        }
    }
    impl<TC> IndexMut for ColumnVec<TC> {
        type IndexMut<'a> = Slice<&'a mut TC> where TC: 'a;

        #[inline(always)]
        fn get_mut(&mut self, index: usize) -> Self::IndexMut<'_> {
            let lower = if index == 0 { 0 } else { self.bounds[index - 1] };
            let upper = self.bounds[index];
            Slice::new(lower, upper, &mut self.values)
        }
    }

    impl<T, TC: Push<T> + Len> Push<Vec<T>> for ColumnVec<TC> {
        fn push(&mut self, item: Vec<T>) {
            self.values.extend(item);
            self.bounds.push(self.values.len());
        }
    }
    impl<'a, T, TC: Push<&'a T> + Len> Push<&'a Vec<T>> for ColumnVec<TC> {
        fn push(&mut self, item: &'a Vec<T>) {
            self.push(&item[..]);
        }
    }
    impl<'a, T, TC: Push<&'a T> + Len> Push<&'a [T]> for ColumnVec<TC> {
        fn push(&mut self, item: &'a [T]) {
            self.values.extend(item.iter());
            self.bounds.push(self.values.len());
        }
    }
    impl<TC: Clear> Clear for ColumnVec<TC> {
        fn clear(&mut self) {
            self.bounds.clear();
            self.values.clear();
        }
    }

    impl<TC: HeapSize> HeapSize for ColumnVec<TC> {
        fn heap_size(&self) -> (usize, usize) {
            let (inner_l, inner_c) = self.values.heap_size();
            (
                std::mem::size_of::<usize>() * self.bounds.len() + inner_l,
                std::mem::size_of::<usize>() * self.bounds.capacity() + inner_c,
            )
        }
    }
}

#[allow(non_snake_case)]
pub mod tuple {

    use super::{Clear, Columnable, Len, Index, IndexMut, Push, HeapSize};

    // Implementations for tuple types.
    // These are all macro based, because the implementations are very similar.
    // The macro requires two names, one for the store and one for pushable types.
    macro_rules! tuple_impl {
        ( $($name:ident,$name2:ident)+) => (
            impl<$($name: Columnable),*> Columnable for ($($name,)*) {
                type Columns = ($($name::Columns,)*);
            }
            impl<$($name: Len),*> Len for ($($name,)*) {
                fn len(&self) -> usize {
                    self.0.len()
                }
            }
            impl<$($name: Clear),*> Clear for ($($name,)*) {
                fn clear(&mut self) {
                    let ($($name,)*) = self;
                    $($name.clear();)*
                }
            }
            impl<$($name: HeapSize),*> HeapSize for ($($name,)*) {
                fn heap_size(&self) -> (usize, usize) {
                    let ($($name,)*) = self;
                    let mut l = 0;
                    let mut c = 0;
                    $(let (l0, c0) = $name.heap_size(); l += l0; c += c0;)*
                    (l, c)
                }
            }
            impl<$($name: Index),*> Index for ($($name,)*) {
                type Ref<'a> = ($($name::Ref<'a>,)*) where $($name: 'a),*;
                fn get(&self, index: usize) -> Self::Ref<'_> {
                    let ($($name,)*) = self;
                    ($($name.get(index),)*)
                }
            }
            impl<$($name: IndexMut),*> IndexMut for ($($name,)*) {
                type IndexMut<'a> = ($($name::IndexMut<'a>,)*) where $($name: 'a),*;
                fn get_mut(&mut self, index: usize) -> Self::IndexMut<'_> {
                    let ($($name,)*) = self;
                    ($($name.get_mut(index),)*)
                }
            }
            impl<$($name2, $name: Push<$name2>),*> Push<($($name2,)*)> for ($($name,)*) {
                fn push(&mut self, item: ($($name2,)*)) {
                    let ($($name,)*) = self;
                    let ($($name2,)*) = item;
                    $($name.push($name2);)*
                }
            }
            impl<'a, $($name2, $name: Push<&'a $name2>),*> Push<&'a ($($name2,)*)> for ($($name,)*) {
                fn push(&mut self, item: &'a ($($name2,)*)) {
                    let ($($name,)*) = self;
                    let ($($name2,)*) = item;
                    $($name.push($name2);)*
                }
            }
        )
    }

    tuple_impl!(A,AA);
    tuple_impl!(A,AA B,BB);
    tuple_impl!(A,AA B,BB C,CC);
    tuple_impl!(A,AA B,BB C,CC D,DD);
    tuple_impl!(A,AA B,BB C,CC D,DD E,EE);
    tuple_impl!(A,AA B,BB C,CC D,DD E,EE F,FF);
    tuple_impl!(A,AA B,BB C,CC D,DD E,EE F,FF G,GG);
    tuple_impl!(A,AA B,BB C,CC D,DD E,EE F,FF G,GG H,HH);
    tuple_impl!(A,AA B,BB C,CC D,DD E,EE F,FF G,GG H,HH I,II);
    tuple_impl!(A,AA B,BB C,CC D,DD E,EE F,FF G,GG H,HH I,II J,JJ);

    #[cfg(test)]
    mod test {
        #[test]
        fn round_trip() {

            use crate::Columnable;
            use crate::common::{Index, Push, HeapSize, Len};

            let mut column: <(usize, u8, String) as Columnable>::Columns = Default::default();
            for i in 0..100 {
                column.push((i, i as u8, i.to_string()));
                column.push((i, i as u8, "".to_string()));
            }

            assert_eq!(column.len(), 200);
            assert_eq!(column.heap_size(), (3590, 4608));

            for i in 0..100 {
                assert_eq!(column.get(2*i+0), (&i, &(i as u8), i.to_string().as_str()));
                assert_eq!(column.get(2*i+1), (&i, &(i as u8), ""));
            }

            // Compare to the heap size of a `Vec<Option<usize>>`.
            let mut column: Vec<(usize, u8, String)> = Default::default();
            for i in 0..100 {
                column.push((i, i as u8, i.to_string()));
                column.push((i, i as u8, "".to_string()));
            }
            assert_eq!(column.heap_size(), (8000, 10240));

        }
    }
}

pub use sums::{BitsRank, result::ColumnResult, option::ColumnOption};
/// Containers for enumerations ("sum types") that store variants separately.
///
/// The main work of these types is storing a discriminant and index efficiently,
/// as containers for each of the variant types can hold the actual data.
pub mod sums {

    /// A store for maintaining `Vec<bool>` with fast rank access.
    /// This is not "succinct" in the technical sense, but it has
    /// similar goals.
    #[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
    pub struct BitsRank {
        /// Counts of the number of cumulative set (true) bits,
        /// *after* each block of 64 bits.
        counts: Vec<u64>,
        /// The bits themselves.
        bits: Vec<u64>,
        /// The number of set bits in `bits.last()`.
        last_bits: usize,
    }

    impl BitsRank {
        #[inline] fn push(&mut self, bit: bool) {
            if self.last_bits == 64 {
                let last_count = self.counts.last().copied().unwrap_or(0);
                self.counts.push(last_count + (self.bits.last().unwrap().count_ones() as u64));
                self.last_bits = 0;
            }
            if self.last_bits == 0 {
                self.bits.push(0);
            }
            *self.bits.last_mut().unwrap() |= (bit as u64) << self.last_bits;
            self.last_bits += 1;
        }
        #[inline] pub fn get(&self, index: usize) -> bool {
            let block = index / 64;
            let bit = index % 64;
            (self.bits[block] >> bit) & 1 == 1
        }
        /// The number of set bits *strictly* preceding `index`.
        pub fn rank(&self, index: usize) -> usize {
            let block = index / 64;
            let bit = index % 64;
            let inter_count = if block == 0 { 0 } else { self.counts.get(block-1).copied().unwrap_or(0) } as usize;
            let intra_count = (self.bits[block] & ((1 << bit) - 1)).count_ones() as usize;
            inter_count + intra_count
        }
        fn len(&self) -> usize {
            self.counts.len() * 64 + self.last_bits
        }
        fn clear(&mut self) {
            self.counts.clear();
            self.bits.clear();
            self.last_bits = 0;
        }
        fn heap_size(&self) -> (usize, usize) {
            let cl = std::mem::size_of::<u64>() * self.counts.len();
            let bl = std::mem::size_of::<u64>() * self.bits.len();
            let cc = std::mem::size_of::<u64>() * self.counts.capacity();
            let bc = std::mem::size_of::<u64>() * self.bits.capacity();
            (cl + bl, cc + bc)
        }
    }

    pub mod result {

        use super::super::{Clear, Columnable, Len, Index, IndexMut, Push, HeapSize};
        use super::BitsRank;

        impl<S: Columnable, T: Columnable> Columnable for Result<S, T> {
            type Columns = ColumnResult<S::Columns, T::Columns>;
        }

        #[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
        pub struct ColumnResult<SC, TC> {
            /// Uses two bits for each item, one to indicate the variant and one (amortized)
            /// to enable efficient rank determination.
            pub indexes: BitsRank,
            pub s_store: SC,
            pub t_store: TC,
        }

        impl<SC, TC> Len for ColumnResult<SC, TC> {
            #[inline(always)] fn len(&self) -> usize { self.indexes.len() }
        }

        impl<SC: Index, TC: Index> Index for ColumnResult<SC, TC> {
            type Ref<'a> = Result<SC::Ref<'a>, TC::Ref<'a>> where SC: 'a, TC: 'a;
            fn get(&self, index: usize) -> Self::Ref<'_> {
                if self.indexes.get(index) {
                    Ok(self.s_store.get(self.indexes.rank(index)))
                } else {
                    Err(self.t_store.get(index - self.indexes.rank(index)))
                }
            }
        }
        // NB: You are not allowed to change the variant, but can change its contents.
        impl<SC: IndexMut, TC: IndexMut> IndexMut for ColumnResult<SC, TC> {
            type IndexMut<'a> = Result<SC::IndexMut<'a>, TC::IndexMut<'a>> where SC: 'a, TC: 'a;
            fn get_mut(&mut self, index: usize) -> Self::IndexMut<'_> {
                if self.indexes.get(index) {
                    Ok(self.s_store.get_mut(self.indexes.rank(index)))
                } else {
                    Err(self.t_store.get_mut(index - self.indexes.rank(index)))
                }
            }
        }

        impl<S, SC: Push<S> + Len, T, TC: Push<T> + Len> Push<Result<S, T>> for ColumnResult<SC, TC> {
            fn push(&mut self, item: Result<S, T>) {
                match item {
                    Ok(item) => {
                        self.indexes.push(true);
                        self.s_store.push(item);
                    }
                    Err(item) => {
                        self.indexes.push(false);
                        self.t_store.push(item);
                    }
                }
            }
        }
        impl<'a, S, SC: Push<&'a S> + Len, T, TC: Push<&'a T> + Len> Push<&'a Result<S, T>> for ColumnResult<SC, TC> {
            fn push(&mut self, item: &'a Result<S, T>) {
                match item {
                    Ok(item) => {
                        self.indexes.push(true);
                        self.s_store.push(item);
                    }
                    Err(item) => {
                        self.indexes.push(false);
                        self.t_store.push(item);
                    }
                }
            }
        }

        impl<SC: Clear, TC: Clear> Clear for ColumnResult<SC, TC> {
            fn clear(&mut self) {
                self.indexes.clear();
                self.s_store.clear();
                self.t_store.clear();
            }
        }

        impl<SC: HeapSize, TC: HeapSize> HeapSize for ColumnResult<SC, TC> {
            fn heap_size(&self) -> (usize, usize) {
                let (l0, c0) = self.s_store.heap_size();
                let (l1, c1) = self.t_store.heap_size();
                let (li, ci) = self.indexes.heap_size();
                (l0 + l1 + li, c0 + c1 + ci)
            }
        }

        #[cfg(test)]
        mod test {
            #[test]
            fn round_trip() {

                use crate::Columnable;
                use crate::common::{Index, Push, HeapSize, Len};

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

                let mut column: <Result<usize, u8> as Columnable>::Columns = Default::default();
                for i in 0..100 {
                    column.push(Ok::<usize, u8>(i));
                    column.push(Err::<usize, u8>(i as u8));
                }

                assert_eq!(column.len(), 200);
                assert_eq!(column.heap_size(), (956, 1216));

                for i in 0..100 {
                    assert_eq!(column.get(2*i+0), Ok(&i));
                    assert_eq!(column.get(2*i+1), Err(&(i as u8)));
                }

                // Compare to the heap size of a `Vec<Option<usize>>`.
                let mut column: Vec<Result<usize, usize>> = Default::default();
                for i in 0..100 {
                    column.push(Ok::<usize, usize>(i));
                    column.push(Err::<usize, usize>(i));
                }
                assert_eq!(column.heap_size(), (3200, 4096));

            }
        }
    }

    pub mod option {

        use super::super::{Clear, Columnable, Len, Index, IndexMut, Push, HeapSize};
        use super::BitsRank;

        impl<T: Columnable> Columnable for Option<T> {
            type Columns = ColumnOption<T::Columns>;
        }

        #[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
        pub struct ColumnOption<TC> {
            /// Uses two bits for each item, one to indicate the variant and one (amortized)
            /// to enable efficient rank determination.
            pub indexes: BitsRank,
            pub t_store: TC,
        }

        impl<T> Len for ColumnOption<T> {
            #[inline(always)] fn len(&self) -> usize { self.indexes.len() }
        }

        impl<T: Index> Index for ColumnOption<T> {
            type Ref<'a> = Option<T::Ref<'a>> where T: 'a;
            fn get(&self, index: usize) -> Self::Ref<'_> {
                if self.indexes.get(index) {
                    Some(self.t_store.get(self.indexes.rank(index)))
                } else {
                    None
                }
            }
        }
        impl<T: IndexMut> IndexMut for ColumnOption<T> {
            type IndexMut<'a> = Option<T::IndexMut<'a>> where T: 'a;
            fn get_mut(&mut self, index: usize) -> Self::IndexMut<'_> {
                if self.indexes.get(index) {
                    Some(self.t_store.get_mut(self.indexes.rank(index)))
                } else {
                    None
                }
            }
        }

        impl<T, TC: Push<T> + Len> Push<Option<T>> for ColumnOption<TC> {
            fn push(&mut self, item: Option<T>) {
                match item {
                    Some(item) => {
                        self.indexes.push(true);
                        self.t_store.push(item);
                    }
                    None => {
                        self.indexes.push(false);
                    }
                }
            }
        }
        impl<'a, T, TC: Push<&'a T> + Len> Push<&'a Option<T>> for ColumnOption<TC> {
            fn push(&mut self, item: &'a Option<T>) {
                match item {
                    Some(item) => {
                        self.indexes.push(true);
                        self.t_store.push(item);
                    }
                    None => {
                        self.indexes.push(false);
                    }
                }
            }
        }

        impl<TC: Clear> Clear for ColumnOption<TC> {
            fn clear(&mut self) {
                self.indexes.clear();
                self.t_store.clear();
            }
        }

        impl<TC: HeapSize> HeapSize for ColumnOption<TC> {
            fn heap_size(&self) -> (usize, usize) {
                let (l0, c0) = self.t_store.heap_size();
                let (li, ci) = self.indexes.heap_size();
                (l0 + li, c0 + ci)
            }
        }

        #[cfg(test)]
        mod test {

            use crate::Columnable;
            use crate::common::{Index, HeapSize, Len};

            #[test]
            fn round_trip_some() {
                let store = Columnable::as_columns((0..100).map(Some));
                assert_eq!(store.len(), 100);
                assert!(store.iter().zip(0..100).all(|(a, b)| a == Some(&b)));
                assert_eq!(store.heap_size(), (424, 576));
            }

            #[test]
            fn round_trip_none() {
                let store = Columnable::as_columns((0..100).map(|_x| None::<i32>));
                assert_eq!(store.len(), 100);
                assert!(store.iter().zip(0..100).all(|(a, _b)| a == None));
                assert_eq!(store.heap_size(), (24, 64));
            }

            #[test]
            fn round_trip_mixed() {
                let store = Columnable::as_columns((0..100).map(|x| if x % 2 == 0 { Some(x) } else { None }));
                assert_eq!(store.len(), 100);
                assert!(store.iter().zip(0..100).all(|(a, b)| a == if b % 2 == 0 { Some(&b) } else { None }));
                assert_eq!(store.heap_size(), (224, 320));
            }
        }
    }
}

pub use lookback::{ColumnRepeats, ColumnLookback};
/// A container that can store either values, or offsets to prior values.
///
/// This has the potential to be more efficient than a list of `T` when many values repeat in
/// close proximity. Values must be equateable, and the degree of lookback can be configured.
pub mod lookback {

    use crate::{ColumnOption, ColumnResult, Push, Index, Len, HeapSize};

    /// A container that encodes repeated values with a `None` variant, at the cost of a few extra bits for every record.
    #[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
    pub struct ColumnRepeats<TC, const N: u8 = 255> {
        /// Some(x) encodes a value, and None indicates the prior `x` value.
        pub inner: ColumnOption<TC>,
    }

    impl<T: PartialEq, TC: Push<T> + Index, const N: u8> Push<T> for ColumnRepeats<TC, N> 
    where 
        for<'a> TC::Ref<'a> : PartialEq<T>,
    {
        fn push(&mut self, item: T) {
            // Look at the last `s_store` value for a potential match.
            let insert: Option<T> = if self.inner.t_store.last().map(|x| x.eq(&item)) == Some(true) {
                None
            } else {
                Some(item)
            };
            self.inner.push(insert);
        }
    }

    impl<TC: Len, const N: u8> Len for ColumnRepeats<TC, N> {
        #[inline(always)] fn len(&self) -> usize { self.inner.len() }
    }
    
    impl<TC: Index, const N: u8> Index for ColumnRepeats<TC, N> {
        type Ref<'a> = TC::Ref<'a> where TC: 'a;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref<'_> { 
            match self.inner.get(index) {
                Some(item) => item,
                None => { 
                    let t_store_pos = self.inner.indexes.rank(index) - 1;
                    self.inner.t_store.get(t_store_pos)
                },
            }
        }
    }

    impl<TC: HeapSize, const N: u8> HeapSize for ColumnRepeats<TC, N> {
        fn heap_size(&self) -> (usize, usize) {
            self.inner.heap_size()
        }
    }

    #[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
    pub struct ColumnLookback<TC, const N: u8 = 255> {
        /// Ok(x) encodes a value, and Err(y) indicates a value `y` back.
        pub inner: ColumnResult<TC, Vec<u8>>,
    }

    impl<T: PartialEq, TC: Push<T> + Index, const N: u8> Push<T> for ColumnLookback<TC, N> 
    where 
        for<'a> TC::Ref<'a> : PartialEq<T>,
    {
        fn push(&mut self, item: T) {
            // Look backwards through (0 .. N) to look for a matching value.
            let s_store_len = self.inner.s_store.len();
            let find = (0u8 .. N).take(self.inner.s_store.len()).find(|i| self.inner.s_store.get(s_store_len - (*i as usize) - 1) == item);
            let insert: Result<T, u8> = if let Some(back) = find { Err(back) } else { Ok(item) };
            self.inner.push(insert);
        }
    }

    impl<TC: Len, const N: u8> Len for ColumnLookback<TC, N> {
        #[inline(always)] fn len(&self) -> usize { self.inner.len() }
    }
    
    impl<TC: Index, const N: u8> Index for ColumnLookback<TC, N> {
        type Ref<'a> = TC::Ref<'a> where TC: 'a;
        #[inline(always)] fn get(&self, index: usize) -> Self::Ref<'_> { 
            match self.inner.get(index) {
                Ok(item) => item,
                Err(back) => { 
                    let s_store_pos = self.inner.indexes.rank(index) - 1;
                    self.inner.s_store.get(s_store_pos - (*back as usize))
                },
            }
        }
    }

    impl<TC: HeapSize, const N: u8> HeapSize for ColumnLookback<TC, N> {
        fn heap_size(&self) -> (usize, usize) {
            self.inner.heap_size()
        }
    }
}