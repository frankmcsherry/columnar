use std::mem;
use std::mem::{transmute, size_of, swap};

pub trait ColumnarVec<T>
{
    fn push(&mut self, T);
    fn pop(&mut self) -> Option<T>;

    fn encode(&mut self, &mut Vec<Vec<u8>>);
    fn decode(&mut self, &mut Vec<Vec<u8>>);

    fn new() -> Self;
}


impl ColumnarVec<uint> for Vec<uint>
{
    #[inline(always)]
    fn push(&mut self, data: uint) { self.push(data); }

    #[inline(always)]
    fn pop(&mut self) -> Option<uint> { self.pop() }

    fn encode(&mut self, buffers: &mut Vec<Vec<u8>>)
    {
        let mut x = Vec::new();
        swap(self, &mut x);
        buffers.push(unsafe { to_bytes_vec(x) });
    }

    fn decode(&mut self, buffers: &mut Vec<Vec<u8>>)
    {
        if self.len() > 0 { panic!("decoding into non-empty ColumnarVec!"); }
        *self = unsafe { to_typed_vec(buffers.pop().unwrap()) };
    }

    fn new() -> Vec<uint> { Vec::new() }
}

/*
impl<T:Copy> ColumnarVec<T> for Vec<T>
{
    #[inline(always)]
    fn push(&mut self, data: T) { self.push(data); }

    #[inline(always)]
    fn pop(&mut self) -> Option<T> { self.pop() }

    fn encode(&mut self, buffers: &mut Vec<Vec<u8>>)
    {
        let mut x = Vec::new();
        swap(self, &mut x);
        buffers.push(unsafe { to_bytes_vec(x) });
    }

    fn decode(&mut self, buffers: &mut Vec<Vec<u8>>)
    {
        if self.len() > 0 { panic!("setting non-empty buffer!"); }
        *self = unsafe { to_typed_vec(buffers.pop().unwrap()) };
    }

    fn new() -> Vec<T> { Vec::new() }
}
*/

impl<T1, R1, T2, R2> ColumnarVec<(T1, T2)> for (R1, R2)
where R1: ColumnarVec<T1>,
R2: ColumnarVec<T2>,
{
    #[inline(always)]
    fn push(&mut self, (x, y): (T1, T2))
    {
        self.mut0().push(x);
        self.mut1().push(y);
    }

    #[inline(always)]
    fn pop(&mut self) -> Option<(T1, T2)>
    {
        self.mut0().pop().map(|x| (x, self.mut1().pop().unwrap()))        
    }

    fn encode(&mut self, buffers: &mut Vec<Vec<u8>>)
    {
        self.mut0().encode(buffers);
        self.mut1().encode(buffers);
    }
    fn decode(&mut self, buffers: &mut Vec<Vec<u8>>)
    {
        self.mut1().decode(buffers);
        self.mut0().decode(buffers);
    }

    fn new() -> (R1, R2) { (ColumnarVec::<T1>::new(), ColumnarVec::<T2>::new()) }
}


impl<T, R: ColumnarVec<T>> ColumnarVec<Option<T>> for (Vec<u8>, R)
{
    #[inline(always)]
    fn push(&mut self, option: Option<T>)
    {
        let &(ref mut lens, ref mut rest) = self;

        match option
        {
            Some(record) => { lens.push(1); rest.push(record); },
            None         => { lens.push(0); },
        }
    }

    #[inline(always)]
    fn pop(&mut self) -> Option<Option<T>>
    {
        let &(ref mut lens, ref mut rest) = self;

        if let Some(count) = lens.pop()
        {
            if count > 0 { Some(Some(rest.pop().unwrap())) }
            else         { Some(None) }
        }
        else { None }
    }

    fn encode(&mut self, buffers: &mut Vec<Vec<u8>>)
    {
        let mut empty = Vec::new();
        swap(self.mut0(), &mut empty);
        buffers.push(empty);
        self.mut1().encode(buffers);
    }

    fn decode(&mut self, buffers: &mut Vec<Vec<u8>>)
    {
        self.mut1().decode(buffers);
        *self.mut0() = buffers.pop().unwrap();
    }

    fn new() -> (Vec<u8>, R) { (Vec::new(), ColumnarVec::<T>::new()) }
}

impl<T, R1: ColumnarVec<uint>, R2: ColumnarVec<T>> ColumnarVec<Vec<T>> for (R1, R2, Vec<Vec<T>>)
{
    #[inline(always)]
    fn push(&mut self, mut vector: Vec<T>)
    {
        self.mut0().push(vector.len());
        while let Some(record) = vector.pop() { self.mut1().push(record); }
        self.mut2().push(vector);
    }

    #[inline(always)]
    fn pop(&mut self) -> Option<Vec<T>>
    {
        if let Some(count) = self.mut0().pop()
        {
            let mut vector = self.mut2().pop().unwrap_or(Vec::new());
            for _ in range(0, count) { vector.push(self.mut1().pop().unwrap()); }
            Some(vector)
        }
        else { None }
    }

    fn encode(&mut self, buffers: &mut Vec<Vec<u8>>)
    {
        self.mut0().encode(buffers);
        self.mut1().encode(buffers);
    }

    fn decode(&mut self, buffers: &mut Vec<Vec<u8>>)
    {
        self.mut1().decode(buffers);
        self.mut0().decode(buffers);
    }

    fn new() -> (R1, R2, Vec<Vec<T>>) { (ColumnarVec::<uint>::new(), ColumnarVec::<T>::new(), Vec::new()) }
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
