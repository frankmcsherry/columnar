use bencher::{benchmark_group, benchmark_main, Bencher};

extern crate columnar;

use columnar::{Columnar, Index, Len};
use columnar::Strings;

pub enum Op {
    Add,    // binary
    Neg,    // unary
    Len,    // unary
    Fmt,    // unary
}

impl Op {
    fn eval(&self, dataz: &[Result<i32, String>]) -> Result<i32, String> {
        match self {
            Op::Add => { 
                let a = dataz[dataz.len()-2].as_ref().ok().unwrap();
                let b = dataz[dataz.len()-1].as_ref().ok().unwrap();
                Ok(a + b)
            },
            Op::Neg => {
                let a = dataz[dataz.len()-1].as_ref().ok().unwrap();
                Ok(-a)
            },
            Op::Len => {
                let a = dataz[dataz.len()-1].as_ref().err().unwrap();
                Ok(a.len() as i32)
            },
            Op::Fmt => {
                let a = dataz[dataz.len()-1].as_ref().ok().unwrap();
                Err(format!("{:?}", a))
            },
        }
    }

    fn eval_vec(&self, dataz: &[Result<Vec<i32>, Strings>]) -> Result<Vec<i32>, Strings> {
        match self {
            Op::Add => { 
                let aa: &Vec<i32> = &dataz[dataz.len()-2].as_ref().ok().unwrap();
                let bb: &Vec<i32> = &dataz[dataz.len()-1].as_ref().ok().unwrap();
                let mut result = Vec::with_capacity(aa.len());
                for (a, b) in aa.iter().zip(bb.iter()) {
                    result.push(a + b);
                }
                Ok(result)
            },
            Op::Neg => {
                let aa: &Vec<i32> = &dataz[dataz.len()-1].as_ref().ok().unwrap();
                let mut result = Vec::with_capacity(aa.len());
                for a in aa.iter() {
                    result.push(-a);
                }
                Ok(result)
            },
            Op::Len => {
                let aa = &dataz[dataz.len()-1].as_ref().err().unwrap();
                let mut result = Vec::with_capacity(aa.len());
                for a in aa.into_iter() {
                    result.push(a.len() as i32);
                }
                Ok(result)
            },
            Op::Fmt => {
                let aa: &Vec<i32> = &dataz[dataz.len()-1].as_ref().ok().unwrap();
                let mut result = Strings::default();
                for a in aa.iter() {
                    use columnar::Push;
                    result.push(&format!("{:?}", a));
                }
                Err(result)
            },
        }
    }
}

fn bench_ops_rows(bencher: &mut Bencher) {

    let prog = vec![Op::Add, Op::Neg, Op::Add];
    let mut rows = Vec::with_capacity(1024);
    for i in 0 .. (rows.capacity() as i32) {
        rows.push(vec![Err("hello".to_string()), Ok(i), Ok(i), Ok(i)]);
    }

    bencher.iter(|| {
        for row in rows.iter_mut() {
            row.truncate(4);
            for op in prog.iter() {
                row.push(op.eval(&row[..]));
            }
        }
        bencher::black_box(&rows);
    });
}

fn bench_ops_rows_compiled(bencher: &mut Bencher) {

    // let prog = vec![Op::Add, Op::Neg, Op::Add];
    let mut rows = Vec::with_capacity(1024);
    for i in 0 .. (rows.capacity() as i32) {
        rows.push(vec![Err("hello".to_string()), Ok(i), Ok(i), Ok(i)]);
    }

    bencher.iter(|| {
        for row in rows.iter_mut() {
            row.truncate(4);
            row.push(Ok(*row[row.len()-2].as_ref().unwrap() + *row[row.len()-1].as_ref().unwrap()));
            row.push(Ok(- *row[row.len()-1].as_ref().unwrap()));
            row.push(Ok(*row[row.len()-2].as_ref().unwrap() + *row[row.len()-1].as_ref().unwrap()));
        }
        bencher::black_box(&rows);
    });
}

fn bench_ops_rows_compiled2(bencher: &mut Bencher) {

    // let prog = vec![Op::Add, Op::Neg, Op::Add];
    let mut rows = Vec::with_capacity(1024);
    for i in 0 .. (rows.capacity() as i32) {
        rows.push(vec![i, i, i]);
    }

    bencher.iter(|| {
        for row in rows.iter_mut() {
            row.truncate(4);
            row.push(row[row.len()-2] + row[row.len()-1]);
            row.push(- row[row.len()-1]);
            row.push(row[row.len()-2] + row[row.len()-1]);
        }
        bencher::black_box(&rows);
    });
}

fn bench_ops_cols(bencher: &mut Bencher) {

    let prog = vec![Op::Add, Op::Neg, Op::Add];
    let mut rows = Vec::with_capacity(1024);
    for i in 0 .. (rows.capacity() as i32) {
        rows.push((Err::<i32, String>("hello".to_string()), Ok::<i32, String>(i), Ok::<i32, String>(i), Ok::<i32, String>(i)));
    }
    
    let cols = Columnar::into_columns(rows.into_iter());
    let mut cols = vec![Err(cols.0.errs), Ok(cols.1.oks), Ok(cols.2.oks), Ok(cols.3.oks)];

    bencher.iter(|| {
        for op in prog.iter() {
            cols.push(op.eval_vec(&cols));
        }
        bencher::black_box(&cols);
        cols.truncate(4);
    });
}

benchmark_group!(
    cols,
    bench_ops_cols,
);

benchmark_group!(
    rows,
    bench_ops_rows,
    bench_ops_rows_compiled,
    bench_ops_rows_compiled2,
);

benchmark_main!(cols, rows);