//! Containers for `isize` and `usize` that adapt to the size of the data.
//!
//! Similar structures could be used for containers of `u8`, `u16`, `u32`, and `u64`,
//! without losing their type information, if one didn't need the bespoke compression.

use crate::Push;
use crate::Results;

/// A four-variant container for integers of varying sizes.
#[allow(dead_code)]
struct Sizes<C0, C1, C2, C3> {
    /// Four variants stored separately.
    inner: Results<Results<C0, C1>, Results<C2, C3>>,
}

impl<C0: Default, C1: Default, C2: Default, C3: Default> Default for Sizes<C0, C1, C2, C3> {
    fn default() -> Self {
        Sizes { inner: Default::default() }
    }
}

impl<C0: Push<u8>, C1: Push<u16>, C2: Push<u32>, C3: Push<u64>> Push<usize> for Sizes<C0, C1, C2, C3> {
    #[inline]
    fn push(&mut self, item: usize) {
        if let Ok(item) = TryInto::<u8>::try_into(item) {
            self.inner.push(Ok(Ok(item)))
        } else if let Ok(item) = TryInto::<u16>::try_into(item) {
            self.inner.push(Ok(Err(item)))
        } else if let Ok(item) = TryInto::<u32>::try_into(item) {
            self.inner.push(Err(Ok(item)))
        } else if let Ok(item) = TryInto::<u64>::try_into(item) {
            self.inner.push(Err(Err(item)))
        } else {
            panic!("usize exceeds bounds of u64")
        }
    }
}

impl<C0: Push<i8>, C1: Push<i16>, C2: Push<i32>, C3: Push<i64>> Push<isize> for Sizes<C0, C1, C2, C3> {
    #[inline]
    fn push(&mut self, item: isize) {
        if let Ok(item) = TryInto::<i8>::try_into(item) {
            self.inner.push(Ok(Ok(item)))
        } else if let Ok(item) = TryInto::<i16>::try_into(item) {
            self.inner.push(Ok(Err(item)))
        } else if let Ok(item) = TryInto::<i32>::try_into(item) {
            self.inner.push(Err(Ok(item)))
        } else if let Ok(item) = TryInto::<i64>::try_into(item) {
            self.inner.push(Err(Err(item)))
        } else {
            panic!("isize exceeds bounds of i64")
        }
    }
}
