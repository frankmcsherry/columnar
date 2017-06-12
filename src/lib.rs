extern crate byteorder;

use std::mem;
use std::default::Default;
use std::string::String;
use std::io::{Read, Write, Result};

use byteorder::{ReadBytesExt, WriteBytesExt, LittleEndian};

// this trait defines a "prefered implementation" of ColumnarStack for a type T,
// because multiple types may implement, for example, ColumnarStack<(u64, u64)>.
pub trait Columnar : Sized + 'static {
    type Stack: ColumnarStack<Self>;
}

// this trait defines a push/pop interface backed by easily serialized columnar storage.
pub trait ColumnarStack<T> : Default {
    fn push(&mut self, T);
    fn pop(&mut self) -> Option<T>;

    fn encode<W: Write>(&mut self, &mut W) -> Result<()>;
    fn decode<R: Read>(&mut self, &mut R) -> Result<()>;
}

// implementations defining default implementors of ColumnarStack.
impl Columnar for i64 { type Stack = Vec<i64>; }
impl Columnar for i32 { type Stack = Vec<i32>; }
impl Columnar for i16 { type Stack = Vec<i16>; }

impl Columnar for u64 { type Stack = Vec<u64>; }
impl Columnar for u32 { type Stack = Vec<u32>; }
impl Columnar for u16 { type Stack = Vec<u16>; }
impl Columnar for u8  { type Stack = Vec<u8>;  }

impl Columnar for usize { type Stack = Vec<usize>; }
impl Columnar for isize { type Stack = Vec<isize>; }

impl Columnar for f32 { type Stack = Vec<f32>; }
impl Columnar for f64 { type Stack = Vec<f64>; }

impl Columnar for String { type Stack = (Vec<u64>, Vec<u8>, Vec<Vec<u8>>); }

impl<T1: Columnar, T2: Columnar> Columnar for (T1, T2) {
    type Stack = (T1::Stack, T2::Stack);
}
impl<T1: Columnar, T2: Columnar, T3: Columnar> Columnar for (T1, T2, T3) {
    type Stack = (T1::Stack, T2::Stack, T3::Stack);
}
impl<T1: Columnar, T2: Columnar, T3: Columnar, T4: Columnar> Columnar for (T1, T2, T3, T4) {
    type Stack = (T1::Stack, T2::Stack, T3::Stack, T4::Stack);
}

impl<T: Columnar> Columnar for Option<T> { type Stack = (Vec<u8>, T::Stack); }
impl<T: Columnar+'static> Columnar for Vec<T> { type Stack = (Vec<u64>, T::Stack, Vec<Vec<T>>); }

impl Columnar for () { type Stack = u64; }

impl ColumnarStack<()> for u64 {
    #[inline(always)] fn push(&mut self, _empty: ()) {
        *self += 1;
    }
    #[inline(always)] fn pop(&mut self) -> Option<()> {
        if *self > 0 { *self -= 1; Some(()) }
        else         { None }
    }

    fn encode<W: Write>(&mut self, writer: &mut W) -> Result<()> {
        try!(writer.write_u64::<LittleEndian>(*self));
        Ok(())
    }
    fn decode<R: Read>(&mut self, reader: &mut R) -> Result<()> {
        *self = try!(reader.read_u64::<LittleEndian>());
        Ok(())
    }
}

// implementations of specific ColumnarQueues.
impl<T:Copy+'static> ColumnarStack<T> for Vec<T> {
    #[inline(always)] fn push(&mut self, data: T) { self.push(data); }
    #[inline(always)] fn pop(&mut self) -> Option<T> { self.pop() }

    fn encode<W: Write>(&mut self, writer: &mut W) -> Result<()> {
        try!(writer.write_typed_vec(self));
        self.clear();
        Ok(())
    }
    fn decode<R: Read>(&mut self, reader: &mut R) -> Result<()> {
        self.clear();
        try!(reader.read_typed_vec(self));
        Ok(())
    }
}

impl ColumnarStack<String> for (Vec<u64>, Vec<u8>, Vec<Vec<u8>>) {
    #[inline(always)]
    fn push(&mut self, string: String) {
        let mut vector = string.into_bytes();
        self.0.push(vector.len() as u64);
        while let Some(record) = vector.pop() { self.1.push(record); }
        self.2.push(vector);
    }

    #[inline(always)]
    fn pop(&mut self) -> Option<String> {
        self.0.pop().map(|count| {
            let mut vector = self.2.pop().unwrap_or_default();
            for _ in 0..count { vector.push(self.1.pop().unwrap()); }
            unsafe { String::from_utf8_unchecked(vector) }
        })
    }

    fn encode<W: Write>(&mut self, writer: &mut W) -> Result<()> {
        try!(self.0.encode(writer));
        try!(self.1.encode(writer));
        Ok(())
    }
    fn decode<R: Read>(&mut self, reader: &mut R) -> Result<()> {
        try!(self.0.decode(reader));
        try!(self.1.decode(reader));
        Ok(())
    }
}

impl<T1, T2, R1: ColumnarStack<T1>, R2: ColumnarStack<T2>> ColumnarStack<(T1, T2)> for (R1, R2) {
    #[inline(always)]
    fn push(&mut self, (x, y): (T1, T2)) {
        self.0.push(x);
        self.1.push(y);
    }

    #[inline(always)]
    fn pop(&mut self) -> Option<(T1, T2)> {
        // self.0.pop().map(|x| (x, self.1.pop().unwrap()))
        match (self.0.pop(), self.1.pop()) {
            (Some(x), Some(y)) => Some((x, y)),
            (None, None)       => None,
            _                  => { println!("error in pair de-columnarization"); None },
        }
    }

    fn encode<W: Write>(&mut self, writer: &mut W) -> Result<()> {
        try!(self.0.encode(writer));
        try!(self.1.encode(writer));
        Ok(())
    }
    fn decode<R: Read>(&mut self, reader: &mut R) -> Result<()> {
        try!(self.0.decode(reader));
        try!(self.1.decode(reader));
        Ok(())
    }
}

impl<T1, T2, T3, R1: ColumnarStack<T1>, R2: ColumnarStack<T2>, R3: ColumnarStack<T3>> ColumnarStack<(T1, T2, T3)> for (R1, R2, R3) {
    #[inline(always)]
    fn push(&mut self, (x, y, z): (T1, T2, T3)) {
        self.0.push(x);
        self.1.push(y);
        self.2.push(z);
    }

    #[inline(always)]
    fn pop(&mut self) -> Option<(T1, T2, T3)> {
        self.0.pop().map(|x| (x, self.1.pop().unwrap(), self.2.pop().unwrap()))
    }

    fn encode<W: Write>(&mut self, writer: &mut W) -> Result<()> {
        try!(self.0.encode(writer));
        try!(self.1.encode(writer));
        try!(self.2.encode(writer));
        Ok(())
    }
    fn decode<R: Read>(&mut self, reader: &mut R) -> Result<()> {
        try!(self.0.decode(reader));
        try!(self.1.decode(reader));
        try!(self.2.decode(reader));
        Ok(())
    }
}

impl<T1, T2, T3, T4,
     R1: ColumnarStack<T1>,
     R2: ColumnarStack<T2>,
     R3: ColumnarStack<T3>,
     R4: ColumnarStack<T4>>
 ColumnarStack<(T1, T2, T3, T4)> for (R1, R2, R3, R4) {
    #[inline(always)]
    fn push(&mut self, (x, y, z, w): (T1, T2, T3, T4)) {
        self.0.push(x);
        self.1.push(y);
        self.2.push(z);
        self.3.push(w);
    }

    #[inline(always)]
    fn pop(&mut self) -> Option<(T1, T2, T3, T4)> {
        self.0.pop().map(|x| (x, self.1.pop().unwrap(), self.2.pop().unwrap(), self.3.pop().unwrap()))
    }
    fn encode<W: Write>(&mut self, writer: &mut W) -> Result<()> {
        try!(self.0.encode(writer));
        try!(self.1.encode(writer));
        try!(self.2.encode(writer));
        try!(self.3.encode(writer));
        Ok(())
    }
    fn decode<R: Read>(&mut self, reader: &mut R) -> Result<()> {
        try!(self.0.decode(reader));
        try!(self.1.decode(reader));
        try!(self.2.decode(reader));
        try!(self.3.decode(reader));
        Ok(())
    }
}

impl<T, S: ColumnarStack<T>> ColumnarStack<Option<T>> for (Vec<u8>, S) {
    #[inline(always)]
    fn push(&mut self, option: Option<T>) {
        match option {
            Some(record) => { self.0.push(1); self.1.push(record); },
            None         => { self.0.push(0); },
        }
    }

    #[inline(always)]
    fn pop(&mut self) -> Option<Option<T>> {
        self.0.pop().map(|count| {
            if count > 0 { Some(self.1.pop().unwrap()) }
            else         { None }
        })
    }

    fn encode<W: Write>(&mut self, writer: &mut W) -> Result<()> {
        try!(writer.write_typed_vec(&mut self.0));
        try!(self.1.encode(writer));
        Ok(())
    }
    fn decode<R: Read>(&mut self, reader: &mut R) -> Result<()> {
        try!(reader.read_typed_vec(&mut self.0));
        try!(self.1.decode(reader));
        Ok(())
    }
}

impl<T:'static, R1: ColumnarStack<u64>, R2: ColumnarStack<T>> ColumnarStack<Vec<T>> for (R1, R2, Vec<Vec<T>>) {
    #[inline(always)]
    fn push(&mut self, mut vector: Vec<T>) {
        self.0.push(vector.len() as u64);
        while let Some(record) = vector.pop() { self.1.push(record); }
        self.2.push(vector);
    }

    #[inline(always)]
    fn pop(&mut self) -> Option<Vec<T>> {
        self.0.pop().map(|count| {
            let mut vector = self.2.pop().unwrap_or_default();
            for _ in 0..count { vector.push(self.1.pop().unwrap()); }
            vector
        })
    }

    fn encode<W: Write>(&mut self, writer: &mut W) -> Result<()> {
        try!(self.0.encode(writer));
        try!(self.1.encode(writer));
        Ok(())
    }
    fn decode<R: Read>(&mut self, reader: &mut R) -> Result<()> {
        try!(self.0.decode(reader));
        try!(self.1.decode(reader));
        Ok(())
    }
}

trait ColumnarWriteExt {
    fn write_typed_vec<T: Copy>(&mut self, vector: &mut Vec<T>) -> Result<()>;
}

impl<W: Write> ColumnarWriteExt for W {
    fn write_typed_vec<T: Copy>(&mut self, vector: &mut Vec<T>) -> Result<()> {
        try!(self.write_u64::<LittleEndian>(vector.len() as u64));
        try!(self.write_all(unsafe { typed_as_byte_slice(&mut vector[..]) }));
        Ok(())
    }
}

trait ColumnarReadExt {
    fn read_typed_vec<T: Copy>(&mut self, vector: &mut Vec<T>) -> Result<()>;
}

impl<R: Read> ColumnarReadExt for R {
    fn read_typed_vec<T: Copy>(&mut self, vector: &mut Vec<T>) -> Result<()> {
        vector.clear();

        let len = try!(self.read_u64::<LittleEndian>()) as usize;
        vector.reserve(len);
        unsafe { vector.set_len(len); }

        let slice = unsafe { typed_as_byte_slice(&mut vector[..]) };
        let mut read = 0;
        while read < slice.len() {
            let just_read = try!(self.read(&mut slice[read..]));
            read += just_read;
        }

        Ok(())
    }
}

unsafe fn typed_as_byte_slice<T>(slice: &mut [T]) -> &mut [u8] {
    std::slice::from_raw_parts_mut(slice.as_mut_ptr() as *mut u8, slice.len() * mem::size_of::<T>())
}
