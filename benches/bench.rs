#![feature(test)]

extern crate columnar;
extern crate test;

use columnar::*;
use test::Bencher;

use std::default::Default;
use std::io::Read;

#[bench] fn u64(bencher: &mut Bencher) { _bench_enc_dec(bencher, (0..1024u64).collect()); }
#[bench] fn u64_x3(bencher: &mut Bencher) { _bench_enc_dec(bencher, (0..1024u64).map(|i| (i, (i+1, i-1))).collect()); }
#[bench] fn vec_vec_u64(bencher: &mut Bencher) {
    _bench_enc_dec(bencher, vec![vec![vec![0u64, 1u64], vec![1, 2, 1, 1, 2]]; 128]);
}
#[bench] fn option_u64(bencher: &mut Bencher) {
    _bench_enc_dec(bencher, (0..1024u64).map(|i| if i % 2 == 0 { Some(i as u64) } else { None }).collect());
}
#[bench] fn u64_vec_string_u64(bencher: &mut Bencher) {
    let data: Vec<(u64,Vec<_>)> = (0..128u64).map(|i| (i, (0..5u64).map(|j| (format!("number: {}", i + j), i as u64 + 10)).collect()))
                                             .collect();
    _bench_enc_dec(bencher, data);
}

// bounces some elements back and forth between columnar stacks, encoding/decoding ...
fn _bench_enc_dec<T: Columnar+Eq+PartialEq+Clone>(bencher: &mut Bencher, mut elements: Vec<T>) {
    let mut stack1: T::Stack = Default::default();
    let mut stack2: T::Stack = Default::default();

    let mut buffers = Vec::with_capacity(1 << 20);

    for element in &elements { stack1.push(element.clone()); }
    stack1.encode(&mut buffers).unwrap();

    bencher.bytes = (buffers.len() as u64) * 2;

    bencher.iter(|| {
        // decode, move, encode
        stack1.decode(&mut &buffers[..]).unwrap();
        while let Some(record) = stack1.pop() { stack2.push(record); }
        buffers.clear();
        stack2.encode(&mut buffers).unwrap();

        // decode, move, encode
        stack2.decode(&mut &buffers[..]).unwrap();
        while let Some(record) = stack2.pop() { stack1.push(record); }
        buffers.clear();
        stack1.encode(&mut buffers).unwrap();
    });

    stack1.decode(&mut &buffers[..]).unwrap();

    while let Some(element) = elements.pop() {
        if let Some(record) = stack1.pop() {
            if record.ne(&element) {
                panic!("un-equal elements found.");
            }
        }
        else {
            panic!("Too few elements pop()d.");
        }
    }
    if let Some(_) = stack1.pop() {
        panic!("Too many elements pop()d.");
    }
}
