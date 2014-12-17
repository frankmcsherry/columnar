use std::mem;
use std::mem::{transmute, size_of, replace};
use std::default::Default;

// this trait defines a "prefered implementation" of ColumnarVec for a type T.
// because multiple types may implement, for example, ColumnarVec<(uint, uint)>.
pub trait Columnar<R: ColumnarVec<Self>> { }

pub trait ColumnarVec<T> : Default
{
    fn push(&mut self, T);
    fn pop(&mut self) -> Option<T>;

    fn encode(&mut self, &mut Vec<Vec<u8>>);
    fn decode(&mut self, &mut Vec<Vec<u8>>);
}

// implementations defining default implementors of ColumnarVec.
impl Columnar<Vec<i64>> for i64 { }
impl Columnar<Vec<i32>> for i32 { }
impl Columnar<Vec<i16>> for i16 { }

impl Columnar<Vec<u64>> for u64 { }
impl Columnar<Vec<u32>> for u32 { }
impl Columnar<Vec<u16>> for u16 { }
impl Columnar<Vec<u8>> for u8 { }

impl Columnar<Vec<uint>> for uint { }
impl Columnar<Vec<int>> for int { }

impl<T1: Columnar<R1>, R1: ColumnarVec<T1>, T2: Columnar<R2>, R2: ColumnarVec<T2>> Columnar<(R1, R2)> for (T1, T2) { }

impl<T: Columnar<R>, R: ColumnarVec<T>> Columnar<(Vec<u8>, R)> for Option<T> { }
impl<T: Columnar<R>, R: ColumnarVec<T>> Columnar<(Vec<uint>, R, Vec<Vec<T>>)> for Vec<T> { }




impl<T:Copy> ColumnarVec<T> for Vec<T>
{
    #[inline(always)]
    fn push(&mut self, data: T) { self.push(data); }

    #[inline(always)]
    fn pop(&mut self) -> Option<T> { self.pop() }

    fn encode(&mut self, buffers: &mut Vec<Vec<u8>>)
    {
        buffers.push(unsafe { to_bytes_vec(replace(self, Vec::new())) });
    }

    fn decode(&mut self, buffers: &mut Vec<Vec<u8>>)
    {
        if self.len() > 0 { panic!("calling decode from a non-empty ColumnarVec"); }
        *self = unsafe { to_typed_vec(buffers.pop().unwrap()) };
    }
}

impl<T1, R1, T2, R2> ColumnarVec<(T1, T2)> for (R1, R2)
where R1: ColumnarVec<T1>,
R2: ColumnarVec<T2>,
{
    #[inline(always)]
    fn push(&mut self, (x, y): (T1, T2))
    {
        self.0.push(x);
        self.1.push(y);
    }

    #[inline(always)]
    fn pop(&mut self) -> Option<(T1, T2)>
    {
        self.0.pop().map(|x| (x, self.1.pop().unwrap()))
    }

    fn encode(&mut self, buffers: &mut Vec<Vec<u8>>)
    {
        self.0.encode(buffers);
        self.1.encode(buffers);
    }
    fn decode(&mut self, buffers: &mut Vec<Vec<u8>>)
    {
        self.1.decode(buffers);
        self.0.decode(buffers);
    }
}

impl<T, R: ColumnarVec<T>> ColumnarVec<Option<T>> for (Vec<u8>, R)
{
    #[inline(always)]
    fn push(&mut self, option: Option<T>)
    {
        match option
        {
            Some(record) => { self.0.push(1); self.1.push(record); },
            None         => { self.0.push(0); },
        }
    }

    #[inline(always)]
    fn pop(&mut self) -> Option<Option<T>>
    {
        if let Some(count) = self.0.pop()
        {
            if count > 0 { Some(Some(self.1.pop().unwrap())) }
            else         { Some(None) }
        }
        else { None }
    }

    fn encode(&mut self, buffers: &mut Vec<Vec<u8>>)
    {
        buffers.push(replace(&mut self.0, Vec::new()));
        self.1.encode(buffers);
    }

    fn decode(&mut self, buffers: &mut Vec<Vec<u8>>)
    {
        self.1.decode(buffers);
        self.0 = buffers.pop().unwrap();
    }
}

impl<T, R1: ColumnarVec<uint>, R2: ColumnarVec<T>> ColumnarVec<Vec<T>> for (R1, R2, Vec<Vec<T>>)
{
    #[inline(always)]
    fn push(&mut self, mut vector: Vec<T>)
    {
        self.0.push(vector.len());
        while let Some(record) = vector.pop() { self.1.push(record); }
        self.2.push(vector);
    }

    #[inline(always)]
    fn pop(&mut self) -> Option<Vec<T>>
    {
        if let Some(count) = self.0.pop()
        {
            let mut vector = self.2.pop().unwrap_or(Vec::new());
            for _ in range(0, count) { vector.push(self.1.pop().unwrap()); }
            Some(vector)
        }
        else { None }
    }

    fn encode(&mut self, buffers: &mut Vec<Vec<u8>>)
    {
        self.0.encode(buffers);
        self.1.encode(buffers);
    }

    fn decode(&mut self, buffers: &mut Vec<Vec<u8>>)
    {
        self.1.decode(buffers);
        self.0.decode(buffers);
    }
}

unsafe fn to_typed_vec<T>(mut vector: Vec<u8>) -> Vec<T>
{
    let rawbyt: *mut u8 = vector.as_mut_ptr();

    let length = vector.len() / size_of::<T>();
    let rawptr: *mut T = transmute(rawbyt);
    mem::forget(vector);

    Vec::from_raw_parts(rawptr, length, length)
}

unsafe fn to_bytes_vec<T>(mut vector: Vec<T>) -> Vec<u8>
{
    let rawbyt: *mut T = vector.as_mut_ptr();

    let length = vector.len() * size_of::<T>();
    let rawptr: *mut u8 = transmute(rawbyt);
    mem::forget(vector);

    Vec::from_raw_parts(rawptr, length, length)
}



// tests!
#[test]
fn test_uint()
{
    _test_columnarization(1024, |i| i);
}

#[test]
fn test_uint_uint_uint()
{
    _test_columnarization(1024, |i| (i, (i+1, i-1)));
}

#[test]
fn test_vec_vec_uint()
{
    _test_columnarization(128, |_| vec![vec![0u, 1u], vec![1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2]]);
}

#[test]
fn test_option_uint()
{
    _test_columnarization(1024, |i| if i % 2 == 0 { Some(i) } else { None });
}


// bounces some elements back and forth between columnar stacks, encoding/decoding ...
fn _test_columnarization<T: Columnar<R>+Eq+PartialEq, R: ColumnarVec<T>>(number: uint, element: |uint|:'static -> T)
{
    let mut stack1: R = Default::default();
    let mut stack2: R = Default::default();

    let mut buffers = Vec::new();

    for index in range(0, number) { stack1.push(element(index)); }
    stack1.encode(&mut buffers);

    for _ in range(0u, 10)
    {
        // decode, move, encode
        stack1.decode(&mut buffers);
        while let Some(record) = stack1.pop() { stack2.push(record); }
        stack2.encode(&mut buffers);

        // decode, move, encode
        stack2.decode(&mut buffers);
        while let Some(record) = stack2.pop() { stack1.push(record); }
        stack1.encode(&mut buffers);
    }

    stack1.decode(&mut buffers);
    for index in range(0, number)
    {
        if let Some(record) = stack1.pop()
        {
            // elements popped in reverse order from insert
            if record.ne(&element(number - index - 1))
            {
                panic!("un-equal elements found");
            }
        }
        else
        {
            panic!("Too few elements pop()d.");
        }
    }
}
