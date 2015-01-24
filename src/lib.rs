extern crate test;

use std::mem;
use std::mem::{transmute, size_of, replace};
use std::default::Default;
use std::string::String;
use std::io::{IoResult, MemReader, MemWriter};

use test::Bencher;

// this trait defines a "prefered implementation" of ColumnarVec for a type T.
// because multiple types may implement, for example, ColumnarVec<(uint, uint)>.
pub trait Columnar<R: ColumnarVec<Self>> { }

pub trait ColumnarVec<T> : Default {
    fn push(&mut self, T);
    fn pop(&mut self) -> Option<T>;

    fn encode(&mut self, &mut Vec<Vec<u8>>);
    fn decode(&mut self, &mut Vec<Vec<u8>>);

    // default implementation of writing to a Writer via encode
    // note: writes out data but does *not* clear the ColumnarVec
    fn write<W: Writer>(&mut self, writer: &mut W) -> IoResult<()> {
        let mut bytes = Vec::new();
        self.encode(&mut bytes);

        for vec in bytes.iter() {
            try!(writer.write_le_u64(vec.len() as u64));
            try!(writer.write(vec.as_slice()));
        }

        // re-install bytes
        self.decode(&mut bytes);
        return Ok(());
    }

    // default implementation of reading from a Reader via decode
    // note: will overwrite any existing data.
    fn read<R: Reader>(&mut self, reader: &mut R) -> IoResult<()> {
        // read bytes to load into
        let mut bytes = Vec::new();
        self.encode(&mut bytes);

        for vec in bytes.iter_mut() {
            vec.clear();
            let veclen = try!(reader.read_le_u64());
            try!(reader.push(veclen as usize, vec));
        }

        self.decode(&mut bytes);
        return Ok(());
    }
}

// implementations defining default implementors of ColumnarVec.
impl Columnar<Vec<i64>> for i64 { }
impl Columnar<Vec<i32>> for i32 { }
impl Columnar<Vec<i16>> for i16 { }

impl Columnar<Vec<u64>> for u64 { }
impl Columnar<Vec<u32>> for u32 { }
impl Columnar<Vec<u16>> for u16 { }
impl Columnar<Vec<u8>> for u8 { }

impl Columnar<Vec<usize>> for usize { }
impl Columnar<Vec<isize>> for isize { }

impl Columnar<(Vec<u64>, Vec<u8>, Vec<Vec<u8>>)> for String { }

impl<T1: Columnar<R1>, R1: ColumnarVec<T1>, T2: Columnar<R2>, R2: ColumnarVec<T2>> Columnar<(R1, R2)> for (T1, T2) { }

impl<T: Columnar<R>, R: ColumnarVec<T>> Columnar<(Vec<u8>, R)> for Option<T> { }
impl<T: Columnar<R>, R: ColumnarVec<T>> Columnar<(Vec<u64>, R, Vec<Vec<T>>)> for Vec<T> { }




impl<T:Copy> ColumnarVec<T> for Vec<T> {
    #[inline(always)]
    fn push(&mut self, data: T) { self.push(data); }

    #[inline(always)]
    fn pop(&mut self) -> Option<T> { self.pop() }

    fn encode(&mut self, buffers: &mut Vec<Vec<u8>>) {
        buffers.push(unsafe { to_bytes_vec(replace(self, Vec::new())) });
    }

    fn decode(&mut self, buffers: &mut Vec<Vec<u8>>) {
        if self.len() > 0 { panic!("calling decode from a non-empty ColumnarVec"); }
        *self = unsafe { to_typed_vec(buffers.pop().unwrap()) };
    }
}


impl ColumnarVec<String> for (Vec<u64>, Vec<u8>, Vec<Vec<u8>>) {
    #[inline(always)]
    fn push(&mut self, string: String) {
        let mut vector = string.into_bytes();

        self.0.push(vector.len() as u64);
        while let Some(record) = vector.pop() { self.1.push(record); }
        self.2.push(vector);
    }

    #[inline(always)]
    fn pop(&mut self) -> Option<String> {

        if let Some(count) = self.0.pop() {
            let mut vector = self.2.pop().unwrap_or(Vec::new());
            // if vector.capacity() == 0 { println!("empty string!"); }
            for _ in (0..count) { vector.push(self.1.pop().unwrap()); }
            Some(unsafe { String::from_utf8_unchecked(vector) })
        }
        else { None }
    }

    fn encode(&mut self, buffers: &mut Vec<Vec<u8>>) {
        self.0.encode(buffers);
        self.1.encode(buffers);
    }

    fn decode(&mut self, buffers: &mut Vec<Vec<u8>>) {
        self.1.decode(buffers);
        self.0.decode(buffers);
    }
}



impl<T1, R1, T2, R2> ColumnarVec<(T1, T2)> for (R1, R2)
where R1: ColumnarVec<T1>,
      R2: ColumnarVec<T2>, {
    #[inline(always)]
    fn push(&mut self, (x, y): (T1, T2)) {
        self.0.push(x);
        self.1.push(y);
    }

    #[inline(always)]
    fn pop(&mut self) -> Option<(T1, T2)> {
        self.0.pop().map(|x| (x, self.1.pop().unwrap()))
    }

    fn encode(&mut self, buffers: &mut Vec<Vec<u8>>) {
        self.0.encode(buffers);
        self.1.encode(buffers);
    }
    fn decode(&mut self, buffers: &mut Vec<Vec<u8>>) {
        self.1.decode(buffers);
        self.0.decode(buffers);
    }
}

impl<T, R: ColumnarVec<T>> ColumnarVec<Option<T>> for (Vec<u8>, R) {
    #[inline(always)]
    fn push(&mut self, option: Option<T>) {
        match option {
            Some(record) => { self.0.push(1); self.1.push(record); },
            None         => { self.0.push(0); },
        }
    }

    #[inline(always)]
    fn pop(&mut self) -> Option<Option<T>> {
        if let Some(count) = self.0.pop() {
            if count > 0 { Some(Some(self.1.pop().unwrap())) }
            else         { Some(None) }
        }
        else { None }
    }

    fn encode(&mut self, buffers: &mut Vec<Vec<u8>>) {
        buffers.push(replace(&mut self.0, Vec::new()));
        self.1.encode(buffers);
    }

    fn decode(&mut self, buffers: &mut Vec<Vec<u8>>) {
        self.1.decode(buffers);
        self.0 = buffers.pop().unwrap();
    }
}

impl<T, R1: ColumnarVec<u64>, R2: ColumnarVec<T>> ColumnarVec<Vec<T>> for (R1, R2, Vec<Vec<T>>) {
    #[inline(always)]
    fn push(&mut self, mut vector: Vec<T>) {
        self.0.push(vector.len() as u64);
        while let Some(record) = vector.pop() { self.1.push(record); }
        self.2.push(vector);
    }

    #[inline(always)]
    fn pop(&mut self) -> Option<Vec<T>> {
        if let Some(count) = self.0.pop() {
            let mut vector = self.2.pop().unwrap_or(Vec::new());
            // if vector.capacity() == 0 { println!("zero capacity"); }
            for _ in (0..count) { vector.push(self.1.pop().unwrap()); }
            Some(vector)
        }
        else { None }
    }

    fn encode(&mut self, buffers: &mut Vec<Vec<u8>>) {
        self.0.encode(buffers);
        self.1.encode(buffers);
    }

    fn decode(&mut self, buffers: &mut Vec<Vec<u8>>) {
        self.1.decode(buffers);
        self.0.decode(buffers);
    }
}

unsafe fn to_typed_vec<T>(mut vector: Vec<u8>) -> Vec<T> {
    let rawbyt: *mut u8 = vector.as_mut_ptr();

    let length = vector.len() / size_of::<T>();
    let rawptr: *mut T = transmute(rawbyt);
    mem::forget(vector);

    Vec::from_raw_parts(rawptr, length, length)
}

unsafe fn to_bytes_vec<T>(mut vector: Vec<T>) -> Vec<u8> {
    let rawbyt: *mut T = vector.as_mut_ptr();

    let length = vector.len() * size_of::<T>();
    let rawptr: *mut u8 = transmute(rawbyt);
    mem::forget(vector);

    Vec::from_raw_parts(rawptr, length, length)
}



// tests!
#[bench] fn u64(bencher: &mut Bencher) { _bench_enc_dec(bencher, (0..1024u64).collect()); }
#[bench] fn mem_u64(bencher: &mut Bencher) { _bench_mem_wr(bencher, (0..1024u64).collect()); }

#[bench] fn u64_x3(bencher: &mut Bencher) { _bench_enc_dec(bencher, (0..1024u64).map(|i| (i, (i+1, i-1))).collect()); }
#[bench] fn mem_uu64_x3(bencher: &mut Bencher) { _bench_mem_wr(bencher, (0..1024u64).map(|i| (i, (i+1, i-1))).collect()); }


#[bench]
fn vec_vec_u64(bencher: &mut Bencher) {
    _bench_enc_dec(bencher, (0..128).map(|_| vec![vec![0u64, 1u64], vec![1, 2, 1, 1, 2]]).collect());
}
#[bench]
fn mem_vec_vec_u64(bencher: &mut Bencher) {
    _bench_mem_wr(bencher, (0..128).map(|_| vec![vec![0u64, 1u64], vec![1, 2, 1, 1, 2]]).collect());
}

#[bench]
fn option_u64(bencher: &mut Bencher) {
    _bench_enc_dec(bencher, (0..1024u64).map(|i| if i % 2 == 0 { Some(i as u64) } else { None }).collect());
}

#[bench]
fn mem_option_u64(bencher: &mut Bencher) {
    _bench_mem_wr(bencher, (0..1024u64).map(|i| if i % 2 == 0 { Some(i as u64) } else { None }).collect());
}

#[bench]
fn u64_vec_string_u64(bencher: &mut Bencher) {
    let data: Vec<(u64,Vec<_>)> = (0..128u64).map(|i| (i, (0..5u64).map(|j| (format!("number: {}", i + j), i as u64 + 10)).collect())).collect();
    _bench_enc_dec(bencher, data);
}

#[bench]
fn mem_u64_vec_string_u64(bencher: &mut Bencher) {
    let data: Vec<(u64,Vec<_>)> = (0..128u64).map(|i| (i, (0..5u64).map(|j| (format!("number: {}", i + j), i as u64 + 10)).collect())).collect();
    _bench_mem_wr(bencher, data);
}


// bounces some elements back and forth between columnar stacks, encoding/decoding ...
fn _bench_enc_dec<T: Columnar<R>+Eq+PartialEq+Clone, R: ColumnarVec<T>>(bencher: &mut Bencher, mut elements: Vec<T>) {
    let mut stack1: R = Default::default();
    let mut stack2: R = Default::default();

    let mut buffers = Vec::new();

    for index in (0..elements.len()) { stack1.push(elements[index].clone()); }
    stack1.encode(&mut buffers);

    let mut bytes = 0;
    for buffer in buffers.iter() { bytes += 2 * buffer.len() as u64; }
    bencher.bytes = bytes;

    bencher.iter(|| {
        // decode, move, encode
        stack1.decode(&mut buffers);
        while let Some(record) = stack1.pop() { stack2.push(record); }
        stack2.encode(&mut buffers);

        // decode, move, encode
        stack2.decode(&mut buffers);
        while let Some(record) = stack2.pop() { stack1.push(record); }
        stack1.encode(&mut buffers);
    });

    stack1.decode(&mut buffers);
    while let Some(element) = elements.pop() {
        if let Some(record) = stack1.pop() {
            if record.ne(&element) {
                panic!("un-equal elements found");
            }
        }
        else {
            panic!("Too few elements pop()d.");
        }
    }
}

fn _bench_mem_wr<T, R>(bencher: &mut Bencher, mut elements: Vec<T>)
where T: Columnar<R>,
      R: ColumnarVec<T>, {
    let mut stack: R = Default::default();
    let mut bytes = 0;

    while let Some(record) = elements.pop() { stack.push(record); }
    let mut buffers = Vec::new();
    stack.encode(&mut buffers);
    for buffer in buffers.iter() { bytes += buffer.len() as u64; }
    stack.decode(&mut buffers);
    while let Some(record) = stack.pop() { elements.push(record); }

    bencher.bytes = bytes;

    // memory for re-use by the MemWriter/MemReader.
    let mut buffer = Vec::with_capacity(bytes as usize);

    bencher.iter(|| {
        buffer.clear();
        let mut writer = MemWriter::from_vec(replace(&mut buffer, Vec::new()));

        while let Some(record) = elements.pop() { stack.push(record); }
        stack.write(&mut writer).ok().expect("write error");

        let mut reader = MemReader::new(writer.into_inner());

        stack.read(&mut reader).ok().expect("read error");
        while let Some(record) = stack.pop() { elements.push(record); }

        let mut local = reader.into_inner();
        buffer = replace(&mut local, Vec::new());
    });
}
