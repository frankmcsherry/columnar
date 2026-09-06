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

use crate::{Index, Len, Push, Strings, Vecs};
use crate::boxed::Boxed;
use crate::lookback::{Lookbacks, Repeats};
use crate::primitive::{Bools, Chars, Durations, Empties, I128s, Isizes, U128s, Usizes};
use crate::primitive::offsets::{Fixeds, Strides};
use crate::{Discriminant, Options, RankSelect, Results};

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
            let end = <[u64]>::last(&self.bounds).copied().unwrap_or(0);
            self.values.truncate(end.try_into().expect("bounds must fit in `usize`"));
        }
    }
}

impl<TC: Truncate> Truncate for Vecs<TC, Vec<u64>> {
    #[inline]
    fn truncate(&mut self, len: usize) {
        if len < self.bounds.len() {
            self.bounds.truncate(len);
            let end = <[u64]>::last(&self.bounds).copied().unwrap_or(0);
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

/// Handle that builds a value of `T` and pushes it into `C` on drop.
///
/// Fixed-width containers whose storage differs from their element type (for
/// example `Usizes`, which stores `u64`) gain nothing from writing in place, so
/// they build the value first and push it once. `Deref` exposes the value.
pub struct ValueAppender<'a, T, C: Push<T>> {
    container: &'a mut C,
    /// Taken by `Drop` to push, or by `abort` to discard.
    value: Option<T>,
}

impl<'a, T, C: Push<T>> ValueAppender<'a, T, C> {
    /// Opens an element starting from `value`.
    #[inline]
    pub fn new(container: &'a mut C, value: T) -> Self {
        Self { container, value: Some(value) }
    }
}

impl<T, C: Push<T>> Deref for ValueAppender<'_, T, C> {
    type Target = T;
    #[inline(always)]
    fn deref(&self) -> &T { self.value.as_ref().expect("ValueAppender value already taken") }
}

impl<T, C: Push<T>> DerefMut for ValueAppender<'_, T, C> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut T { self.value.as_mut().expect("ValueAppender value already taken") }
}

impl<T, C: Push<T>> Drop for ValueAppender<'_, T, C> {
    #[inline]
    fn drop(&mut self) {
        if let Some(value) = self.value.take() {
            self.container.push(value);
        }
    }
}

impl<T, C: Push<T>> Appender for ValueAppender<'_, T, C> {
    #[inline]
    fn abort(mut self) {
        self.value = None;
    }
}

/// Implements `Append` through a `ValueAppender` seeded with a starting value.
macro_rules! value_append {
    ($container:ty, $value:ty, $start:expr) => {
        impl Append for $container {
            type Appender<'a> = ValueAppender<'a, $value, Self>;
            #[inline]
            fn appender(&mut self) -> Self::Appender<'_> {
                ValueAppender::new(self, $start)
            }
        }
    };
}

value_append!(Usizes, usize, 0);
value_append!(Isizes, isize, 0);
value_append!(Chars, char, '\0');
value_append!(U128s, u128, 0);
value_append!(I128s, i128, 0);
value_append!(Durations, core::time::Duration, core::time::Duration::ZERO);
value_append!(Bools, bool, false);
value_append!(Empties, (), ());
value_append!(Strides, u64, 0);

impl<const K: u64> Append for Fixeds<K> {
    type Appender<'a> = ValueAppender<'a, (), Self>;
    #[inline]
    fn appender(&mut self) -> Self::Appender<'_> {
        ValueAppender::new(self, ())
    }
}

/// Implements `Truncate` for containers that wrap a single `values` field.
macro_rules! values_truncate {
    ($container:ident) => {
        impl<CV: Truncate> Truncate for $container<CV> {
            #[inline(always)]
            fn truncate(&mut self, len: usize) { self.values.truncate(len) }
        }
    };
}

values_truncate!(Usizes);
values_truncate!(Isizes);
values_truncate!(Chars);
values_truncate!(U128s);
values_truncate!(I128s);

impl<SC: Truncate, NC: Truncate> Truncate for Durations<SC, NC> {
    #[inline]
    fn truncate(&mut self, len: usize) {
        self.seconds.truncate(len);
        self.nanoseconds.truncate(len);
    }
}

impl Truncate for Empties {
    #[inline(always)]
    fn truncate(&mut self, len: usize) { self.count = self.count.min(len as u64) }
}

impl<const K: u64> Truncate for Fixeds<K> {
    #[inline(always)]
    fn truncate(&mut self, len: usize) { self.count = self.count.min(len as u64) }
}

impl Truncate for Strides {
    #[inline]
    fn truncate(&mut self, len: usize) {
        let strided = self.head[1] as usize;
        if len <= strided {
            // The kept prefix lies entirely within the implicit stride pattern.
            self.head[1] = len as u64;
            self.bounds.clear();
        } else {
            self.bounds.truncate(len - strided);
        }
    }
}

impl Truncate for Bools {
    #[inline]
    fn truncate(&mut self, len: usize) {
        if len < self.len() {
            let words = len / 64;
            let bits = len % 64;
            // `bits < 64`, so the shift cannot overflow.
            let mask = (1u64 << bits) - 1;
            let word = if words < self.values.len() { self.values[words] } else { self.tail[0] };
            self.values.truncate(words);
            self.tail = [word & mask, bits as u64];
        }
    }
}

/// Handle for one variant of a two-way sum, recording the variant bit once the
/// inner element commits.
///
/// Dereferences to the inner handle. Dropping commits the inner element and then
/// pushes `bit`; [`abort`](Appender::abort) discards the inner element and pushes
/// nothing.
pub struct VariantAppender<'a, A: Appender> {
    /// Taken by `Drop` to commit, or by `abort` to discard.
    inner: Option<A>,
    indexes: &'a mut RankSelect,
    bit: bool,
}

impl<A: Appender> Deref for VariantAppender<'_, A> {
    type Target = A;
    #[inline(always)]
    fn deref(&self) -> &A { self.inner.as_ref().expect("VariantAppender inner already taken") }
}

impl<A: Appender> DerefMut for VariantAppender<'_, A> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut A { self.inner.as_mut().expect("VariantAppender inner already taken") }
}

impl<A: Appender> Drop for VariantAppender<'_, A> {
    #[inline]
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            // The inner element must commit before its variant is recorded, so
            // that a panic while committing leaves the columns consistent.
            drop(inner);
            self.indexes.push(self.bit);
        }
    }
}

impl<A: Appender> Appender for VariantAppender<'_, A> {
    #[inline]
    fn abort(mut self) {
        if let Some(inner) = self.inner.take() {
            inner.abort();
        }
    }
}

/// Handle onto the next element of an `Options` column.
///
/// The element exists only once a variant is chosen with [`some`](Self::some)
/// or [`none`](Self::none). Dropping the handle without choosing adds nothing.
pub struct OptionsAppender<'a, TC> {
    options: &'a mut Options<TC>,
}

impl<'a, TC: Append> OptionsAppender<'a, TC> {
    /// Chooses `Some` and returns a handle onto the inner element.
    #[inline]
    pub fn some(self) -> VariantAppender<'a, TC::Appender<'a>> {
        let Options { indexes, somes } = self.options;
        VariantAppender { inner: Some(somes.appender()), indexes, bit: true }
    }
    /// Chooses `None`.
    #[inline]
    pub fn none(self) {
        self.options.indexes.push(false);
    }
}

impl<TC> Appender for OptionsAppender<'_, TC> {
    #[inline(always)]
    fn abort(self) { }
}

impl<TC: Append> Append for Options<TC> {
    type Appender<'a> = OptionsAppender<'a, TC> where TC: 'a;
    #[inline]
    fn appender(&mut self) -> Self::Appender<'_> {
        OptionsAppender { options: self }
    }
}

impl<TC: Truncate + Len> Truncate for Options<TC> {
    #[inline]
    fn truncate(&mut self, len: usize) {
        if len < self.len() {
            let somes = self.indexes.rank(len);
            self.somes.truncate(somes);
            self.indexes.truncate(len);
        }
    }
}

/// Handle onto the next element of a `Results` column.
///
/// The element exists only once a variant is chosen with [`ok`](Self::ok) or
/// [`err`](Self::err). Dropping the handle without choosing adds nothing.
pub struct ResultsAppender<'a, SC, TC> {
    results: &'a mut Results<SC, TC>,
}

impl<'a, SC: Append, TC: Append> ResultsAppender<'a, SC, TC> {
    /// Chooses `Ok` and returns a handle onto the inner element.
    #[inline]
    pub fn ok(self) -> VariantAppender<'a, SC::Appender<'a>> {
        let Results { indexes, oks, .. } = self.results;
        VariantAppender { inner: Some(oks.appender()), indexes, bit: true }
    }
    /// Chooses `Err` and returns a handle onto the inner element.
    #[inline]
    pub fn err(self) -> VariantAppender<'a, TC::Appender<'a>> {
        let Results { indexes, errs, .. } = self.results;
        VariantAppender { inner: Some(errs.appender()), indexes, bit: false }
    }
}

impl<SC, TC> Appender for ResultsAppender<'_, SC, TC> {
    #[inline(always)]
    fn abort(self) { }
}

impl<SC: Append, TC: Append> Append for Results<SC, TC> {
    type Appender<'a> = ResultsAppender<'a, SC, TC> where SC: 'a, TC: 'a;
    #[inline]
    fn appender(&mut self) -> Self::Appender<'_> {
        ResultsAppender { results: self }
    }
}

impl<SC: Truncate + Len, TC: Truncate + Len> Truncate for Results<SC, TC> {
    #[inline]
    fn truncate(&mut self, len: usize) {
        if len < self.len() {
            let oks = self.indexes.rank(len);
            self.oks.truncate(oks);
            self.errs.truncate(len - oks);
            self.indexes.truncate(len);
        }
    }
}

/// Handle for one variant of a derived enum, recording its discriminant and
/// offset once the inner element commits.
///
/// Dereferences to the inner handle. Dropping commits the inner element and then
/// records the variant; [`abort`](Appender::abort) discards the inner element
/// and records nothing.
pub struct DiscriminantAppender<'a, A: Appender> {
    /// Taken by `Drop` to commit, or by `abort` to discard.
    inner: Option<A>,
    indexes: &'a mut Discriminant,
    variant: u8,
    offset: u64,
}

impl<'a, A: Appender> DiscriminantAppender<'a, A> {
    /// Wraps `inner`, which builds the element at `offset` in the container for `variant`.
    #[inline]
    pub fn new(inner: A, indexes: &'a mut Discriminant, variant: u8, offset: u64) -> Self {
        Self { inner: Some(inner), indexes, variant, offset }
    }
}

impl<A: Appender> Deref for DiscriminantAppender<'_, A> {
    type Target = A;
    #[inline(always)]
    fn deref(&self) -> &A { self.inner.as_ref().expect("DiscriminantAppender inner already taken") }
}

impl<A: Appender> DerefMut for DiscriminantAppender<'_, A> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut A { self.inner.as_mut().expect("DiscriminantAppender inner already taken") }
}

impl<A: Appender> Drop for DiscriminantAppender<'_, A> {
    #[inline]
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            drop(inner);
            self.indexes.push(self.variant, self.offset);
        }
    }
}

impl<A: Appender> Appender for DiscriminantAppender<'_, A> {
    #[inline]
    fn abort(mut self) {
        if let Some(inner) = self.inner.take() {
            inner.abort();
        }
    }
}

impl Truncate for Discriminant {
    #[inline]
    fn truncate(&mut self, len: usize) {
        if self.is_heterogeneous() {
            self.variant.truncate(len);
            self.offset.truncate(len);
        } else if self.offset.len() >= 2 && (self.offset[1] as usize) > len {
            self.offset[1] = len as u64;
        }
    }
}

/// Handle onto the next element of a `Repeats` column.
///
/// Dereferences to the values container, in which exactly one element must be
/// built (for example with its `appender`). Dropping compares that element with
/// its predecessor and records either the value or a repeat marker. Dropping
/// without building an element adds nothing.
pub struct RepeatsAppender<'a, TC: Truncate + Len>
where
    for<'b> &'b TC: Index,
    for<'b> <&'b TC as Index>::Ref: PartialEq,
{
    repeats: &'a mut Repeats<TC>,
    /// Length of the values container when the element was opened.
    start: usize,
    /// Cleared by `abort` so that `Drop` records nothing.
    live: bool,
}

impl<TC: Truncate + Len> Deref for RepeatsAppender<'_, TC>
where
    for<'b> &'b TC: Index,
    for<'b> <&'b TC as Index>::Ref: PartialEq,
{
    type Target = TC;
    #[inline(always)]
    fn deref(&self) -> &TC { &self.repeats.inner.somes }
}

impl<TC: Truncate + Len> DerefMut for RepeatsAppender<'_, TC>
where
    for<'b> &'b TC: Index,
    for<'b> <&'b TC as Index>::Ref: PartialEq,
{
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut TC { &mut self.repeats.inner.somes }
}

impl<TC: Truncate + Len> Drop for RepeatsAppender<'_, TC>
where
    for<'b> &'b TC: Index,
    for<'b> <&'b TC as Index>::Ref: PartialEq,
{
    fn drop(&mut self) {
        if !self.live { return; }
        let somes = &mut self.repeats.inner.somes;
        let added = somes.len() - self.start;
        if added == 0 { return; }
        assert_eq!(added, 1, "RepeatsAppender: exactly one element may be built");
        let repeat = self.start > 0 && (&*somes).get(self.start - 1) == (&*somes).get(self.start);
        if repeat {
            somes.truncate(self.start);
            self.repeats.inner.indexes.push(false);
        } else {
            self.repeats.inner.indexes.push(true);
        }
    }
}

impl<TC: Truncate + Len> Appender for RepeatsAppender<'_, TC>
where
    for<'b> &'b TC: Index,
    for<'b> <&'b TC as Index>::Ref: PartialEq,
{
    #[inline]
    fn abort(mut self) {
        self.live = false;
        self.repeats.inner.somes.truncate(self.start);
    }
}

impl<TC: Truncate + Len> Append for Repeats<TC>
where
    for<'b> &'b TC: Index,
    for<'b> <&'b TC as Index>::Ref: PartialEq,
{
    type Appender<'a> = RepeatsAppender<'a, TC> where TC: 'a;
    #[inline]
    fn appender(&mut self) -> Self::Appender<'_> {
        let start = self.inner.somes.len();
        RepeatsAppender::<'_, TC> { repeats: self, start, live: true }
    }
}

impl<TC: Truncate + Len> Truncate for Repeats<TC> {
    #[inline(always)]
    fn truncate(&mut self, len: usize) { self.inner.truncate(len) }
}

/// Handle onto the next element of a `Lookbacks` column.
///
/// Dereferences to the values container, in which exactly one element must be
/// built (for example with its `appender`). Dropping searches the previous `N`
/// values for a match and records either the value or a lookback offset.
/// Dropping without building an element adds nothing.
pub struct LookbacksAppender<'a, TC: Truncate + Len, const N: u8>
where
    for<'b> &'b TC: Index,
    for<'b> <&'b TC as Index>::Ref: PartialEq,
{
    lookbacks: &'a mut Lookbacks<TC, Vec<u8>, Vec<u64>, Vec<u64>, [u64; 2], N>,
    /// Length of the values container when the element was opened.
    start: usize,
    /// Cleared by `abort` so that `Drop` records nothing.
    live: bool,
}

impl<TC: Truncate + Len, const N: u8> Deref for LookbacksAppender<'_, TC, N>
where
    for<'b> &'b TC: Index,
    for<'b> <&'b TC as Index>::Ref: PartialEq,
{
    type Target = TC;
    #[inline(always)]
    fn deref(&self) -> &TC { &self.lookbacks.inner.oks }
}

impl<TC: Truncate + Len, const N: u8> DerefMut for LookbacksAppender<'_, TC, N>
where
    for<'b> &'b TC: Index,
    for<'b> <&'b TC as Index>::Ref: PartialEq,
{
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut TC { &mut self.lookbacks.inner.oks }
}

impl<TC: Truncate + Len, const N: u8> Drop for LookbacksAppender<'_, TC, N>
where
    for<'b> &'b TC: Index,
    for<'b> <&'b TC as Index>::Ref: PartialEq,
{
    fn drop(&mut self) {
        if !self.live { return; }
        let inner = &mut self.lookbacks.inner;
        let added = inner.oks.len() - self.start;
        if added == 0 { return; }
        assert_eq!(added, 1, "LookbacksAppender: exactly one element may be built");
        let start = self.start;
        let oks = &inner.oks;
        let found = (0u8 .. N).take(start).find(|back| oks.get(start - (*back as usize) - 1) == oks.get(start));
        if let Some(back) = found {
            inner.oks.truncate(start);
            inner.indexes.push(false);
            inner.errs.push(back);
        } else {
            inner.indexes.push(true);
        }
    }
}

impl<TC: Truncate + Len, const N: u8> Appender for LookbacksAppender<'_, TC, N>
where
    for<'b> &'b TC: Index,
    for<'b> <&'b TC as Index>::Ref: PartialEq,
{
    #[inline]
    fn abort(mut self) {
        self.live = false;
        self.lookbacks.inner.oks.truncate(self.start);
    }
}

impl<TC: Truncate + Len, const N: u8> Append for Lookbacks<TC, Vec<u8>, Vec<u64>, Vec<u64>, [u64; 2], N>
where
    for<'b> &'b TC: Index,
    for<'b> <&'b TC as Index>::Ref: PartialEq,
{
    type Appender<'a> = LookbacksAppender<'a, TC, N> where TC: 'a;
    #[inline]
    fn appender(&mut self) -> Self::Appender<'_> {
        let start = self.inner.oks.len();
        LookbacksAppender::<'_, TC, N> { lookbacks: self, start, live: true }
    }
}

impl<TC: Truncate + Len, const N: u8> Truncate for Lookbacks<TC, Vec<u8>, Vec<u64>, Vec<u64>, [u64; 2], N> {
    #[inline(always)]
    fn truncate(&mut self, len: usize) { self.inner.truncate(len) }
}

impl<C: Append> Append for Boxed<C> {
    type Appender<'a> = C::Appender<'a> where C: 'a;
    #[inline(always)]
    fn appender(&mut self) -> Self::Appender<'_> { self.0.appender() }
}

impl<C: Truncate> Truncate for Boxed<C> {
    #[inline(always)]
    fn truncate(&mut self, len: usize) { self.0.truncate(len) }
}

#[cfg(test)]
mod test {
    use alloc::vec::Vec;
    use core::fmt::Write;
    use crate::{Index, Len, Push, Strings, Vecs, Options, Results, Repeats, Lookbacks, RankSelect, Discriminant};
    use crate::boxed::Boxed;
    use crate::primitive::{Bools, Chars, Durations, Empties, Usizes};
    use crate::primitive::offsets::{Fixeds, Strides};
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
    fn value_appenders() {
        let mut usizes: Usizes = Default::default();
        *usizes.appender() = 7;
        usizes.appender().abort();
        assert_eq!(usizes.len(), 1);
        assert_eq!(usizes.get(0), 7);

        let mut chars: Chars = Default::default();
        *chars.appender() = 'x';
        assert_eq!(chars.get(0), 'x');

        let mut durations: Durations = Default::default();
        *durations.appender() = core::time::Duration::from_millis(1500);
        assert_eq!(durations.get(0), core::time::Duration::from_millis(1500));

        let mut bools: Bools = Default::default();
        for i in 0..100 { *bools.appender() = i % 3 == 0; }
        assert_eq!(bools.len(), 100);
        assert!(bools.get(99));
        assert!(!bools.get(98));

        let mut empties: Empties = Default::default();
        empties.appender();
        empties.appender();
        assert_eq!(empties.len(), 2);

        let mut fixeds: Fixeds<3> = Default::default();
        fixeds.appender();
        assert_eq!(fixeds.get(0), 3);

        let mut strides: Strides = Default::default();
        *strides.appender() = 4;
        *strides.appender() = 8;
        *strides.appender() = 9;
        assert_eq!(strides.len(), 3);
        assert_eq!(strides.get(2), 9);
    }

    #[test]
    fn primitive_truncate() {
        let mut bools: Bools = Default::default();
        for i in 0..200 { bools.push(i % 2 == 0); }
        bools.truncate(130);
        assert_eq!(bools.len(), 130);
        assert!(bools.get(128));
        bools.push(true);
        assert!(bools.get(130));
        assert!(!bools.get(129));
        bools.truncate(0);
        assert_eq!(bools.len(), 0);

        let mut strides: Strides = Default::default();
        for i in 1..=5 { strides.push(4 * i); }
        strides.push(100);
        strides.truncate(3);
        assert_eq!(strides.len(), 3);
        assert_eq!(strides.get(2), 12);
        assert!(strides.bounds.is_empty());

        let mut rs: RankSelect = Default::default();
        for i in 0..3000 { rs.push(i % 7 == 0); }
        rs.truncate(2500);
        assert_eq!(rs.len(), 2500);
        assert_eq!(rs.counts.len(), 2);
        assert_eq!(rs.rank(2500), (0..2500).filter(|i| i % 7 == 0).count());

        let mut fixeds: Fixeds<2> = Default::default();
        fixeds.push(()); fixeds.push(()); fixeds.push(());
        fixeds.truncate(1);
        assert_eq!(fixeds.len(), 1);

        let mut disc: Discriminant = Default::default();
        disc.push(1, 0); disc.push(1, 1);
        disc.truncate(1);
        assert_eq!(disc.len(), 1);
        assert_eq!(disc.homogeneous(), Some(1));
        disc.push(0, 0); disc.push(1, 1);
        disc.truncate(2);
        assert_eq!(disc.len(), 2);
        assert_eq!(disc.get(1), (0, 0));
    }

    #[test]
    fn options_append() {
        let mut options: Options<Strings> = Default::default();
        write!(options.appender().some(), "{}", 1).unwrap();
        options.appender().none();
        options.appender();
        {
            let mut a = options.appender().some();
            write!(a, "discarded").unwrap();
            a.abort();
        }
        write!(options.appender().some(), "{}", 2).unwrap();
        assert_eq!(options.len(), 3);
        assert_eq!(options.somes.values.len(), 2);
        assert_eq!((&options).get(0), Some(&b"1"[..]));
        assert_eq!((&options).get(1), None);
        assert_eq!((&options).get(2), Some(&b"2"[..]));
    }

    #[test]
    fn results_append_and_truncate() {
        let mut results: Results<Vec<u64>, Strings> = Default::default();
        **results.appender().ok() = 1;
        write!(results.appender().err(), "e{}", 1).unwrap();
        **results.appender().ok() = 2;
        assert_eq!(results.len(), 3);
        assert_eq!((&results).get(1), Err(&b"e1"[..]));
        assert_eq!((&results).get(2), Ok(&2));
        results.truncate(2);
        assert_eq!(results.len(), 2);
        assert_eq!(results.oks.len(), 1);
        assert_eq!(results.errs.len(), 1);
        **results.appender().ok() = 3;
        assert_eq!((&results).get(2), Ok(&3));

        let mut options: Options<Vec<u64>> = Default::default();
        for i in 0..10u64 { options.push(if i % 2 == 0 { Some(i) } else { None }); }
        options.truncate(5);
        assert_eq!(options.len(), 5);
        assert_eq!(options.somes.len(), 3);
        assert_eq!(options.get(4), Some(4));
    }

    #[test]
    fn repeats_append() {
        let mut repeats: Repeats<Strings> = Default::default();
        for s in ["a", "a", "b", "b", "b", "a"] {
            let mut r = repeats.appender();
            write!(r.appender(), "{}", s).unwrap();
        }
        assert_eq!(repeats.len(), 6);
        assert_eq!(repeats.inner.somes.len(), 3);
        assert_eq!((&repeats).get(1), b"a");
        assert_eq!((&repeats).get(4), b"b");
        assert_eq!((&repeats).get(5), b"a");
        {
            let mut r = repeats.appender();
            write!(r.appender(), "gone").unwrap();
            r.abort();
        }
        repeats.appender();
        assert_eq!(repeats.len(), 6);
        assert_eq!(repeats.inner.somes.len(), 3);
        repeats.truncate(2);
        assert_eq!(repeats.len(), 2);
        assert_eq!(repeats.inner.somes.len(), 1);
    }

    #[test]
    fn lookbacks_append() {
        let mut lookbacks: Lookbacks<Strings> = Default::default();
        for s in ["a", "b", "a", "c", "b", "b"] {
            let mut l = lookbacks.appender();
            write!(l.appender(), "{}", s).unwrap();
        }
        assert_eq!(lookbacks.len(), 6);
        assert_eq!(lookbacks.inner.oks.len(), 3);
        assert_eq!(lookbacks.inner.errs, [1, 1, 1]);
        for (i, s) in ["a", "b", "a", "c", "b", "b"].iter().enumerate() {
            assert_eq!((&lookbacks).get(i), s.as_bytes());
        }
        lookbacks.truncate(3);
        assert_eq!(lookbacks.len(), 3);
        assert_eq!(lookbacks.inner.oks.len(), 2);
        assert_eq!(lookbacks.inner.errs.len(), 1);
    }

    #[test]
    fn boxed_append() {
        let mut boxed: Boxed<Strings> = Default::default();
        write!(boxed.appender(), "x").unwrap();
        assert_eq!(boxed.len(), 1);
        boxed.truncate(0);
        assert_eq!(boxed.len(), 0);
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
