extern crate collections;

use std::default::Default;
use std::mem;
use std::mem::{transmute, size_of};
use std::iter::Zip;
use std::vec::MoveItems;
// use std::rc::Rc;

pub trait ColumnarEncode<T> : 'static
{
    // takes a list of buffers to populate, a count, and what is essentially a cloneable iterator.
    // produces a list of Vec<u8>s that should decode to the supplied contents.
    fn encode<'a, K: Iterator<&'a T>>(buffers: &mut Vec<Vec<u8>>, count: uint, iterator: || -> K);
}

pub trait ColumnarDecode<T, K: Iterator<T>> : 'static+Default
{
    // takes a list of Vec<u8>s, which are consumed and result in some
    // type of iterator (it depends on T) which will show you the values.
    // the hint is present to avoid fighting with type inference.
    fn decode(buffers: &mut Vec<Vec<u8>>, count: uint, hint: &T) -> K;
}

#[inline]
unsafe fn to_typed_vec<T>(mut vector: Vec<u8>) -> Vec<T>
{
    let rawbyt: *mut u8 = vector.as_mut_ptr();

    let length = vector.len() / size_of::<T>();
    let rawptr: *mut T = transmute(rawbyt);
    mem::forget(vector);

    Vec::from_raw_parts(rawptr, length, length)
}

#[inline]
unsafe fn to_bytes_vec<T>(mut vector: Vec<T>) -> Vec<u8>
{
    let rawbyt: *mut T = vector.as_mut_ptr();

    let length = vector.len() * size_of::<T>();
    let rawptr: *mut u8 = transmute(rawbyt);
    mem::forget(vector);

    Vec::from_raw_parts(rawptr, length, length)
}

// Columnar encode and decode implementations for uint
impl ColumnarEncode<uint> for uint
{
    #[inline(always)]
    fn encode<'a, K: Iterator<&'a uint>>(writers: &mut Vec<Vec<u8>>, count: uint, iterator: || -> K)
    {
        encode_batched_test(writers, count, iterator());
    }
}

impl ColumnarDecode<uint, MoveItems<uint>> for uint
{
    #[inline(always)]
    fn decode(buffers: &mut Vec<Vec<u8>>, _count: uint, _hint: &uint) -> MoveItems<uint>
    {
        let reader = buffers.pop().expect("missing buffer");
        let buffer = unsafe { to_typed_vec(reader) };

        buffer.into_iter()
    }
}


// Columnar encode and decode implementations for pairs
impl<T1, T2> ColumnarEncode<(T1,T2)> for (T1, T2)
where T1: ColumnarEncode<T1>,
      T2: ColumnarEncode<T2>,
{
    #[inline(always)]
    fn encode<'a, K: Iterator<&'a (T1, T2)>>(writers: &mut Vec<Vec<u8>>, count: uint, iterator: || -> K)
    {
        ColumnarEncode::encode(writers, count, || iterator().map(|&(ref a, _)| a));
        ColumnarEncode::encode(writers, count, || iterator().map(|&(_, ref b)| b));
    }
}
impl<T1, T2, K1, K2> ColumnarDecode<(T1, T2), Zip<K1, K2>> for (T1, T2)
where T1: ColumnarDecode<T1, K1>,
      T2: ColumnarDecode<T2, K2>,
      K1: Iterator<T1>,
      K2: Iterator<T2>,
{
    #[inline(always)]
    fn decode(buffers: &mut Vec<Vec<u8>>, count: uint, _hint: &(T1, T2)) -> Zip<K1, K2>
    {
        let iter1: K1 = ColumnarDecode::decode(buffers, count, &Default::default());
        let iter2: K2 = ColumnarDecode::decode(buffers, count, &Default::default());

        iter1.zip(iter2)
    }
}


// Columnar encode and decode implementations for vectors
impl<T: ColumnarEncode<T>> ColumnarEncode<Vec<T>> for Vec<T>
{
    #[inline(always)]
    fn encode<'a, K: Iterator<&'a Vec<T>>>(writers: &mut Vec<Vec<u8>>, count: uint, iterator: || -> K)
    {
        let mut total = 0u;
        let mut counts = Vec::with_capacity(count);
        for x in iterator()
        {
            total += x.len();
            counts.push(x.len());
        }

        ColumnarEncode::encode(writers, count, || counts.iter());
        ColumnarEncode::encode(writers, total, || iterator().flat_map(|x| x.iter()));
    }
}
impl<T:ColumnarDecode<T, K>, K: Iterator<T>> ColumnarDecode<Vec<T>, VectorIterator<T, K>> for Vec<T>
{
    #[inline(always)]
    fn decode(buffers: &mut Vec<Vec<u8>>, _count: uint, _hint: &Vec<T>) -> VectorIterator<T, K>
    {
        let reader = buffers.pop().expect("missing reader");
        let counts = unsafe { to_typed_vec(reader) };

        let mut total = 0u;
        for i in counts.iter() { total += *i; }

        let iterator = ColumnarDecode::decode(buffers, total, &Default::default());

        VectorIterator { iter: iterator, counts: counts, finger: 0u }
    }
}


pub struct VectorIterator<T, K: Iterator<T>>
{
    iter:   K,
    counts: Vec<uint>,
    finger: uint,
}

impl<T:'static, K: Iterator<T>> Iterator<Vec<T>> for VectorIterator<T, K>
{
    #[inline(always)]
    fn next(&mut self) -> Option<Vec<T>>
    {
        if self.finger < self.counts.len()
        {
            let mut result = Vec::with_capacity(self.counts[self.finger]);
            for _ in range(0, self.counts[self.finger])
            {
                result.push(self.iter.next().expect("ran out of data"));
            }

            self.finger += 1;

            Some(result)
        }
        else
        {
            None
        }
    }
}


#[inline]
fn encode_batched_test<'a, T:Copy+'static, K: Iterator<&'a T>>(writers: &mut Vec<Vec<u8>>, count: uint, mut iterator: K)
{
    let mut vector = Vec::with_capacity(count);
    for i in iterator
    {
        vector.push(*i);
    }

    writers.push(unsafe { to_bytes_vec(vector) });
}
