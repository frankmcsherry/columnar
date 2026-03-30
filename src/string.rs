use alloc::{vec::Vec, string::String, string::ToString, boxed::Box};
use super::{Clear, Columnar, Container, Len, Index, IndexAs, Push, Borrow};

/// A stand-in for `Vec<String>`.
///
/// The reference type for `Strings` is `&[u8]` rather than `&str` to remove utf8 validation
/// from the critical path of reads. You get to make the call about whether and how you'd like
/// to manage this validation. The `copy_from` and `into_owned` methods panic on invalid data.
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
        self.push_str(core::str::from_utf8(other).expect("invalid utf8 in Strings column"));
    }
    #[inline(always)]
    fn into_owned<'a>(other: crate::Ref<'a, Self>) -> Self {
        core::str::from_utf8(other).expect("invalid utf8 in Strings column").to_string()
    }
    type Container = Strings;
}

impl Columnar for Box<str> {
    #[inline(always)]
    fn copy_from<'a>(&mut self, other: crate::Ref<'a, Self>) {
        let mut s = String::from(core::mem::take(self));
        s.clear();
        s.push_str(core::str::from_utf8(other).expect("invalid utf8 in Strings column"));
        *self = s.into_boxed_str();
    }
    #[inline(always)]
    fn into_owned<'a>(other: crate::Ref<'a, Self>) -> Self {
        Self::from(core::str::from_utf8(other).expect("invalid utf8 in Strings column"))
    }
    type Container = Strings;
}

impl<BC: crate::common::BorrowIndexAs<u64>> Borrow for Strings<BC, Vec<u8>> {
    type Ref<'a> = &'a [u8];
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
    fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: core::ops::Range<usize>) {
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
    const SLICE_COUNT: usize = BC::SLICE_COUNT + VC::SLICE_COUNT;
    #[inline]
    fn get_byte_slice(&self, index: usize) -> (u64, &'a [u8]) {
        if index < BC::SLICE_COUNT {
            self.bounds.get_byte_slice(index)
        } else {
            self.values.get_byte_slice(index - BC::SLICE_COUNT)
        }
    }
}
impl<'a, BC: crate::FromBytes<'a>, VC: crate::FromBytes<'a>> crate::FromBytes<'a> for Strings<BC, VC> {
    const SLICE_COUNT: usize = BC::SLICE_COUNT + VC::SLICE_COUNT;
    #[inline(always)]
    fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
        Self {
            bounds: crate::FromBytes::from_bytes(bytes),
            values: crate::FromBytes::from_bytes(bytes),
        }
    }
    #[inline(always)]
    fn from_store(store: &crate::bytes::indexed::DecodedStore<'a>, offset: &mut usize) -> Self {
        Self {
            bounds: BC::from_store(store, offset),
            values: VC::from_store(store, offset),
        }
    }
    fn element_sizes(sizes: &mut Vec<usize>) -> Result<(), String> {
        BC::element_sizes(sizes)?;
        VC::element_sizes(sizes)?;
        Ok(())
    }
}

impl<BC: Len, VC> Len for Strings<BC, VC> {
    #[inline(always)] fn len(&self) -> usize { self.bounds.len() }
}

impl<'a, BC: Len+IndexAs<u64>> Strings<BC, &'a [u8]> {
    /// Returns the `index`-th string as `&str`, validating UTF-8.
    ///
    /// This is a convenience wrapper around [`Index::get`] (which returns `&[u8]`)
    /// for callers who need `&str`. The UTF-8 validation has a measurable cost;
    /// use `get` directly if you can work with `&[u8]`.
    #[inline(always)]
    pub fn get_str(&self, index: usize) -> &'a str {
        core::str::from_utf8(self.get(index)).expect("invalid utf8 in Strings column")
    }
}

impl<'a, BC: Len+IndexAs<u64>> Index for Strings<BC, &'a [u8]> {
    type Ref = &'a [u8];
    #[inline(always)] fn get(&self, index: usize) -> Self::Ref {
        let lower = if index == 0 { 0 } else { self.bounds.index_as(index - 1) };
        let upper = self.bounds.index_as(index);
        let lower: usize = lower.try_into().expect("bounds must fit in `usize`");
        let upper: usize = upper.try_into().expect("bounds must fit in `usize`");
        &self.values[lower .. upper]
    }
}
impl<'a, BC: Len+IndexAs<u64>> Index for &'a Strings<BC, Vec<u8>> {
    type Ref = &'a [u8];
    #[inline(always)] fn get(&self, index: usize) -> Self::Ref {
        let lower = if index == 0 { 0 } else { self.bounds.index_as(index - 1) };
        let upper = self.bounds.index_as(index);
        let lower: usize = lower.try_into().expect("bounds must fit in `usize`");
        let upper: usize = upper.try_into().expect("bounds must fit in `usize`");
        &self.values[lower .. upper]
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

impl<BC: for<'a> Push<&'a u64>> Push<&[u8]> for Strings<BC> {
    #[inline(always)] fn push(&mut self, item: &[u8]) {
        self.values.extend_from_slice(item);
        self.bounds.push(&(self.values.len() as u64));
    }
}
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
impl<'a, BC: for<'b> Push<&'b u64>> Push<core::fmt::Arguments<'a>> for Strings<BC> {
    #[inline]
    fn push(&mut self, item: core::fmt::Arguments<'a>) {
        // Use core::fmt::Write via a wrapper to avoid requiring std::io::Write.
        struct VecWriter<'a>(&'a mut alloc::vec::Vec<u8>);
        impl core::fmt::Write for VecWriter<'_> {
            fn write_str(&mut self, s: &str) -> core::fmt::Result {
                self.0.extend_from_slice(s.as_bytes());
                Ok(())
            }
        }
        core::fmt::Write::write_fmt(&mut VecWriter(&mut self.values), item).expect("write_fmt failed");
        self.bounds.push(&(self.values.len() as u64));
    }
}
impl<'a, 'b, BC: for<'c> Push<&'c u64>> Push<&'b core::fmt::Arguments<'a>> for Strings<BC> {
    #[inline]
    fn push(&mut self, item: &'b core::fmt::Arguments<'a>) {
        self.push(*item);
    }
}
impl<BC: Clear, VC: Clear> Clear for Strings<BC, VC> {
    #[inline(always)]
    fn clear(&mut self) {
        self.bounds.clear();
        self.values.clear();
    }
}

