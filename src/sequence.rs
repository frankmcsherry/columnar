//! Sequential iteration over `Borrowed` views.
//!
//! `Sequence` is the fast-path companion to [`Index`](crate::Index). It is
//! implemented on `Copy` borrowed types (`C::Borrowed<'a>`) so the iterator
//! can be constructed by value and hold inner-`'a` references directly,
//! avoiding any reference to the borrowed shell itself.
//!
//! Trivial impls use [`crate::common::IterOwn`] as the iterator type; types
//! with faster sequential strategies (e.g. [`crate::Repeats`]) override with
//! a specialized iterator that maintains incremental state.

use core::ops::Range;

/// A borrowed view that can yield its elements sequentially.
///
/// `Sequence` is implemented on `Copy` borrowed types. The iterator is
/// constructed by consuming `self` by value; its state owns the inner `'a`
/// references directly, so the lifetime of the returned iterator is tied
/// to the borrowed data rather than to any outer `&Borrowed` shell.
pub trait Sequence: Copy {
    /// Element reference type; fixed by the borrowed type's inner lifetime.
    type Ref;
    /// Concrete iterator type that yields `Self::Ref` items.
    type Iter: Iterator<Item = Self::Ref>;
    /// Iterates over all elements.
    fn seq_iter(self) -> Self::Iter;
    /// Iterates over a sub-range.
    fn seq_iter_range(self, range: Range<usize>) -> Self::Iter;
    /// Internal-iteration convenience. Defaults to the external iterator.
    /// Specialized impls may override with a true stack-only loop.
    #[inline(always)]
    fn for_each_in(self, range: Range<usize>, mut f: impl FnMut(Self::Ref)) {
        for x in self.seq_iter_range(range) {
            f(x);
        }
    }
}

#[cfg(test)]
mod test {
    use super::Sequence;
    use alloc::vec;
    use alloc::vec::Vec;

    #[test]
    fn slice_seq_iter_parity() {
        let data: &[u64] = &[1, 2, 3, 4, 5];
        let via_seq: Vec<&u64> = data.seq_iter().collect();
        let via_get: Vec<&u64> = (0..data.len()).map(|i| &data[i]).collect();
        assert_eq!(via_seq, via_get);
    }

    #[test]
    fn slice_seq_iter_range() {
        let data: &[u64] = &[10, 20, 30, 40, 50];
        let got: Vec<&u64> = data.seq_iter_range(1..4).collect();
        assert_eq!(got, vec![&20, &30, &40]);
    }

    #[test]
    fn vec_ref_seq_iter_parity() {
        let v: Vec<u64> = vec![7, 8, 9];
        let via_seq: Vec<&u64> = (&v).seq_iter().collect();
        assert_eq!(via_seq, vec![&7u64, &8, &9]);
    }
}
