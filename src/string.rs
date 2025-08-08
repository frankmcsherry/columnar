use super::{Clear, Columnar, Container, Len, Index, IndexAs, Push, HeapSize};

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

impl<BC: crate::common::PushIndexAs<u64>> Container for Strings<BC, Vec<u8>> {
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
