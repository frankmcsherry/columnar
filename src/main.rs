extern crate time;
extern crate test;
extern crate core;

use columnar::{ColumnarEncode, ColumnarDecode};
mod columnar;

fn main()
{
    bench_encode_decode_verify(1024, ((0u, (3u, 4u)), (vec![vec![0u, 1u], vec![1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2]])));
    bench_encode_decode_verify(1024, (0u, (3u, 4u)));
}

fn bench_encode_decode_verify<T:Clone+ColumnarEncode<T>+ColumnarDecode<T, K>+Eq+PartialEq, K:Iterator<T>>(number: uint, record: T)
{
    let source = Vec::from_elem(number, record.clone());

    for _ in range(0u, 10)
    {
        let     start = time::precise_time_ns();
        let mut bytes = 0u;

        let mut buffers1 = Vec::new();
        let mut buffers2 = Vec::new();

        while time::precise_time_ns() - start < 1000000000
        {
            // encode data into buffers
            ColumnarEncode::encode(&mut buffers1, number, || source.iter());

            for buffer in buffers1.iter() { bytes += buffer.len(); }

            // revers buffers
            while let Some(buffer) = buffers1.pop() { buffers2.push(buffer); }

            for (_index, _element) in ColumnarDecode::decode(&mut buffers2, number, &record).enumerate()
            {
                if source[_index].ne(&_element) { println!("encode/decode error!"); }
            }
        }

        println!("Encoding/decoding/validating at {}GB/s", bytes as f64 / (time::precise_time_ns() - start) as f64)
    }
}
