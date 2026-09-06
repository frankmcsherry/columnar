//! In-place construction of a container's next element.
//!
//! Pushing a `String` into a `Strings` column costs one copy more than needed: the
//! string is formatted into its own allocation, then copied into the column. The
//! [`Append`] trait lets a caller open the next element, write into it directly
//! (for example with `write!`), and commit it by dropping the handle.
//!
//! Variable-length elements (`Strings`, `Vecs`) grow at the end of their value
//! storage and record their bound on commit. Fixed-width elements (`Vec<T>`) are
//! pushed as `T::default()` and then mutated through the handle.

use alloc::vec::Vec;
use core::ops::{Deref, DerefMut};

use crate::{Len, Push, Strings, Vecs};

/// A handle onto an element under construction.
///
/// Dropping the handle commits the element; [`abort`](Self::abort) discards it.
pub trait Appender {
    /// Discards the element under construction, leaving the container as it was
    /// before the element was opened.
    fn abort(self);
}

/// A container whose next element can be built in place.
pub trait Append {
    /// A handle onto the element under construction.
    type Appender<'a>: Appender where Self: 'a;

    /// Starts a new element and returns a handle to build it.
    ///
    /// Named `appender` rather than `append` because `Vec::append` is an inherent
    /// method and would shadow a trait method of the same name.
    fn appender(&mut self) -> Self::Appender<'_>;

    /// Builds a new element inside `logic`, committing it when `logic` returns.
    #[inline]
    fn append_with<R>(&mut self, logic: impl FnOnce(&mut Self::Appender<'_>) -> R) -> R {
        let mut appender = self.appender();
        logic(&mut appender)
    }

    /// Builds a new element inside `logic`, committing it on `Ok` and discarding it on `Err`.
    #[inline]
    fn try_append_with<R, E>(&mut self, logic: impl FnOnce(&mut Self::Appender<'_>) -> Result<R, E>) -> Result<R, E> {
        let mut appender = self.appender();
        match logic(&mut appender) {
            Ok(result) => Ok(result),
            Err(error) => {
                appender.abort();
                Err(error)
            }
        }
    }
}

/// A container that can be shortened to a prefix of its elements.
pub trait Truncate {
    /// Keeps the first `len` elements and drops the rest. No effect if `len >= self.len()`.
    fn truncate(&mut self, len: usize);
}

impl<T> Truncate for Vec<T> {
    #[inline(always)]
    fn truncate(&mut self, len: usize) { Vec::truncate(self, len) }
}

impl Truncate for Strings<Vec<u64>, Vec<u8>> {
    #[inline]
    fn truncate(&mut self, len: usize) {
        if len < self.bounds.len() {
            self.bounds.truncate(len);
            let end = self.bounds.last().copied().unwrap_or(0);
            self.values.truncate(end.try_into().expect("bounds must fit in `usize`"));
        }
    }
}

impl<TC: Truncate> Truncate for Vecs<TC, Vec<u64>> {
    #[inline]
    fn truncate(&mut self, len: usize) {
        if len < self.bounds.len() {
            self.bounds.truncate(len);
            let end = self.bounds.last().copied().unwrap_or(0);
            self.values.truncate(end.try_into().expect("bounds must fit in `usize`"));
        }
    }
}

/// Handle onto the last element of a `Vec<T>`.
///
/// The element is pushed as `T::default()` when the handle is created and can be
/// mutated through `DerefMut`. Dropping the handle leaves the element in place.
pub struct VecAppender<'a, T> {
    vec: &'a mut Vec<T>,
}

impl<T> Appender for VecAppender<'_, T> {
    #[inline]
    fn abort(self) {
        self.vec.pop();
    }
}

impl<T> Deref for VecAppender<'_, T> {
    type Target = T;
    #[inline(always)]
    fn deref(&self) -> &T {
        // `append` pushes an element before handing out the handle, so `last` is present.
        self.vec.last().expect("VecAppender over an empty vector")
    }
}

impl<T> DerefMut for VecAppender<'_, T> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut T {
        self.vec.last_mut().expect("VecAppender over an empty vector")
    }
}

impl<T: Default> Append for Vec<T> {
    type Appender<'a> = VecAppender<'a, T> where T: 'a;
    #[inline]
    fn appender(&mut self) -> Self::Appender<'_> {
        self.push(T::default());
        VecAppender { vec: self }
    }
}

/// Handle onto the string under construction at the end of a `Strings` column.
///
/// Bytes written through `fmt::Write`, [`push_str`](Self::push_str), or
/// [`push_bytes`](Self::push_bytes) extend the element. Dropping the handle records
/// the element's bound; [`abort`](Appender::abort) discards the written bytes instead.
pub struct StringsAppender<'a, BC: for<'b> Push<&'b u64>> {
    strings: &'a mut Strings<BC>,
    /// Length of `values` when the element was opened, for `abort`.
    start: usize,
    /// Cleared by `abort` so that `Drop` does not record a bound.
    live: bool,
}

impl<BC: for<'b> Push<&'b u64>> StringsAppender<'_, BC> {
    /// Appends `s` to the element under construction.
    #[inline(always)]
    pub fn push_str(&mut self, s: &str) {
        self.strings.values.extend_from_slice(s.as_bytes());
    }
    /// Appends raw bytes to the element under construction.
    ///
    /// `Strings` does not validate UTF-8 on write, so the caller is responsible for
    /// keeping the element valid if it is later read as `str`.
    #[inline(always)]
    pub fn push_bytes(&mut self, bytes: &[u8]) {
        self.strings.values.extend_from_slice(bytes);
    }
}

impl<BC: for<'b> Push<&'b u64>> Appender for StringsAppender<'_, BC> {
    #[inline]
    fn abort(mut self) {
        self.live = false;
        self.strings.values.truncate(self.start);
    }
}

impl<BC: for<'b> Push<&'b u64>> core::fmt::Write for StringsAppender<'_, BC> {
    #[inline(always)]
    fn write_str(&mut self, s: &str) -> core::fmt::Result {
        self.push_str(s);
        Ok(())
    }
}

impl<BC: for<'b> Push<&'b u64>> Drop for StringsAppender<'_, BC> {
    #[inline]
    fn drop(&mut self) {
        if self.live {
            self.strings.bounds.push(&(self.strings.values.len() as u64));
        }
    }
}

impl<BC: for<'b> Push<&'b u64> + 'static> Append for Strings<BC> {
    type Appender<'a> = StringsAppender<'a, BC>;
    #[inline]
    fn appender(&mut self) -> Self::Appender<'_> {
        let start = self.values.len();
        StringsAppender { strings: self, start, live: true }
    }
}

/// Handle onto the inner sequence under construction at the end of a `Vecs` column.
///
/// The handle dereferences to the values container, so elements are added with its
/// usual `push`, or built in place with its own `appender`. Dropping the handle
/// records the sequence's bound; [`abort`](Appender::abort) removes the added
/// elements instead, which is why the values container must support [`Truncate`].
pub struct VecsAppender<'a, TC: Len, BC: for<'b> Push<&'b u64>> {
    vecs: &'a mut Vecs<TC, BC>,
    /// Length of `values` when the sequence was opened, for `abort`.
    start: usize,
    /// Cleared by `abort` so that `Drop` does not record a bound.
    live: bool,
}

impl<TC: Truncate + Len, BC: for<'b> Push<&'b u64>> Appender for VecsAppender<'_, TC, BC> {
    #[inline]
    fn abort(mut self) {
        self.live = false;
        self.vecs.values.truncate(self.start);
    }
}

impl<TC: Len, BC: for<'b> Push<&'b u64>> Deref for VecsAppender<'_, TC, BC> {
    type Target = TC;
    #[inline(always)]
    fn deref(&self) -> &TC { &self.vecs.values }
}

impl<TC: Len, BC: for<'b> Push<&'b u64>> DerefMut for VecsAppender<'_, TC, BC> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut TC { &mut self.vecs.values }
}

impl<TC: Len, BC: for<'b> Push<&'b u64>> Drop for VecsAppender<'_, TC, BC> {
    #[inline]
    fn drop(&mut self) {
        if self.live {
            self.vecs.bounds.push(&(self.vecs.values.len() as u64));
        }
    }
}

impl<TC: Truncate + Len + 'static, BC: for<'b> Push<&'b u64> + 'static> Append for Vecs<TC, BC> {
    type Appender<'a> = VecsAppender<'a, TC, BC>;
    #[inline]
    fn appender(&mut self) -> Self::Appender<'_> {
        let start = self.values.len();
        VecsAppender { vecs: self, start, live: true }
    }
}

macro_rules! tuple_append {
    ( $($name:ident),* ) => (
        impl<$($name: Append),*> Append for ($($name,)*) {
            type Appender<'a> = ($($name::Appender<'a>,)*) where $($name: 'a),*;
            #[inline]
            fn appender(&mut self) -> Self::Appender<'_> {
                #[allow(non_snake_case)]
                let ($($name,)*) = self;
                ($($name.appender(),)*)
            }
        }
        impl<$($name: Appender),*> Appender for ($($name,)*) {
            #[inline]
            fn abort(self) {
                #[allow(non_snake_case)]
                let ($($name,)*) = self;
                $($name.abort();)*
            }
        }
        impl<$($name: Truncate),*> Truncate for ($($name,)*) {
            #[inline]
            fn truncate(&mut self, len: usize) {
                #[allow(non_snake_case)]
                let ($($name,)*) = self;
                $($name.truncate(len);)*
            }
        }
    )
}

tuple_append!(A);
tuple_append!(A, B);
tuple_append!(A, B, C);
tuple_append!(A, B, C, D);
tuple_append!(A, B, C, D, E);
tuple_append!(A, B, C, D, E, F);
tuple_append!(A, B, C, D, E, F, G);
tuple_append!(A, B, C, D, E, F, G, H);
tuple_append!(A, B, C, D, E, F, G, H, I);
tuple_append!(A, B, C, D, E, F, G, H, I, J);

#[cfg(test)]
mod test {
    use alloc::vec::Vec;
    use core::fmt::Write;
    use crate::{Index, Len, Push, Strings, Vecs};
    use super::{Append, Appender, Truncate};

    #[test]
    fn strings_append() {
        let mut strings: Strings = Default::default();
        strings.push("before");
        {
            let mut a = strings.appender();
            write!(a, "{}-{}", 1, 2).unwrap();
            a.push_str("!");
        }
        strings.push("after");
        assert_eq!(strings.len(), 3);
        assert_eq!((&strings).get(0), b"before");
        assert_eq!((&strings).get(1), b"1-2!");
        assert_eq!((&strings).get(2), b"after");
    }

    #[test]
    fn strings_abort() {
        let mut strings: Strings = Default::default();
        strings.push("before");
        {
            let mut a = strings.appender();
            write!(a, "discarded").unwrap();
            a.abort();
        }
        strings.push("after");
        assert_eq!(strings.len(), 2);
        assert_eq!((&strings).get(0), b"before");
        assert_eq!((&strings).get(1), b"after");
        assert_eq!(strings.values.len(), "before".len() + "after".len());
    }

    #[test]
    fn strings_append_with() {
        let mut strings: Strings = Default::default();
        let written = strings.append_with(|a| { write!(a, "{:03}", 7).unwrap(); 3 });
        assert_eq!(written, 3);
        assert_eq!((&strings).get(0), b"007");
    }

    #[test]
    fn vecs_nested_append() {
        let mut vecs: Vecs<Strings> = Default::default();
        for r in 0..3 {
            let mut row = vecs.appender();
            for c in 0..r {
                write!(row.appender(), "{}-{}", r, c).unwrap();
            }
        }
        assert_eq!(vecs.len(), 3);
        assert_eq!((&vecs).get(0).len(), 0);
        assert_eq!((&vecs).get(1).len(), 1);
        assert_eq!((&vecs).get(2).len(), 2);
        assert_eq!((&vecs).get(2).get(1), b"2-1");
    }

    #[test]
    fn vecs_abort() {
        let mut vecs: Vecs<Strings> = Default::default();
        vecs.push(["a", "b"]);
        {
            let mut row = vecs.appender();
            row.push("x");
            write!(row.appender(), "y").unwrap();
            row.abort();
        }
        vecs.push(["c"]);
        assert_eq!(vecs.len(), 2);
        assert_eq!(vecs.values.len(), 3);
        assert_eq!(vecs.values.values.len(), 3);
        assert_eq!((&vecs).get(1).get(0), b"c");
    }

    #[test]
    fn vec_default_append() {
        let mut vec: Vec<u64> = Vec::new();
        *vec.appender() = 5;
        {
            let mut a = vec.appender();
            *a += 7;
            a.abort();
        }
        vec.append_with(|a| **a = 9);
        assert_eq!(vec, [5, 9]);
    }

    #[test]
    fn tuple_append() {
        let mut pair: (Vec<u64>, Strings) = Default::default();
        {
            let (mut n, mut s) = pair.appender();
            *n = 42;
            write!(s, "{}", 42).unwrap();
        }
        assert_eq!(pair.len(), 1);
        assert_eq!((&pair).get(0), (&42, &b"42"[..]));
    }

    #[test]
    fn try_append_with() {
        let mut vecs: Vecs<(Vec<u64>, Strings)> = Default::default();
        let ok: Result<(), ()> = vecs.try_append_with(|row| {
            let (mut n, mut s) = row.appender();
            *n = 1;
            write!(s, "one").unwrap();
            Ok(())
        });
        assert_eq!(ok, Ok(()));
        let err: Result<(), &str> = vecs.try_append_with(|row| {
            let (mut n, mut s) = row.appender();
            *n = 2;
            write!(s, "two").unwrap();
            drop((n, s));
            row.push((&3, "three"));
            Err("changed my mind")
        });
        assert_eq!(err, Err("changed my mind"));
        assert_eq!(vecs.len(), 1);
        assert_eq!(vecs.values.len(), 1);
        assert_eq!(vecs.values.1.values.len(), 3);
        assert_eq!((&vecs).get(0).get(0), (&1, &b"one"[..]));
    }

    #[test]
    fn tuple_abort() {
        let mut pair: (Vec<u64>, Strings) = Default::default();
        let mut a = pair.appender();
        *a.0 = 1;
        write!(a.1, "x").unwrap();
        a.abort();
        assert_eq!(pair.len(), 0);
        assert_eq!(pair.1.values.len(), 0);
    }

    #[test]
    fn truncate() {
        let mut vecs: Vecs<Strings> = Default::default();
        vecs.push(["a", "bb"]);
        vecs.push(["ccc"]);
        vecs.push(["dddd", "e"]);
        vecs.truncate(5);
        assert_eq!(vecs.len(), 3);
        vecs.truncate(1);
        assert_eq!(vecs.len(), 1);
        assert_eq!(vecs.values.len(), 2);
        assert_eq!(vecs.values.values.len(), 3);
        vecs.truncate(0);
        assert_eq!(vecs.values.values.len(), 0);
    }
}
