use bencher::{benchmark_group, benchmark_main, Bencher};

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
                let result = aa.iter().zip(bb.iter()).map(|(a,b)| a + b).collect();
                Ok(result)
            },
            Op::Neg => {
                let aa: &Vec<i32> = &dataz[dataz.len()-1].as_ref().ok().unwrap();
                let result = aa.iter().map(|a| -a).collect();
                Ok(result)
            },
            Op::Len => {
                let aa = &dataz[dataz.len()-1].as_ref().err().unwrap();
                let mut result = Vec::with_capacity(aa.len());
                for a in aa.into_index_iter() {
                    result.push(a.len() as i32);
                }
                Ok(result)
            },
            Op::Fmt => {
                let aa: &Vec<i32> = &dataz[dataz.len()-1].as_ref().ok().unwrap();
                let mut result = Strings::default();
                for a in aa.iter() {
                    use columnar::Push;
                    result.push(&format_args!("{:?}", a));
                }
                Err(result)
            },
        }
    }
}

fn add_rows(bencher: &mut Bencher) { _bench_rows(bencher, &[Op::Add]); }
fn add_cols(bencher: &mut Bencher) { _bench_cols(bencher, &[Op::Add]); }
fn neg_rows(bencher: &mut Bencher) { _bench_rows(bencher, &[Op::Neg]); }
fn neg_cols(bencher: &mut Bencher) { _bench_cols(bencher, &[Op::Neg]); }
fn fmt_rows(bencher: &mut Bencher) { _bench_rows(bencher, &[Op::Fmt]); }
fn fmt_cols(bencher: &mut Bencher) { _bench_cols(bencher, &[Op::Fmt]); }
fn anf_rows(bencher: &mut Bencher) { _bench_rows(bencher, &[Op::Add, Op::Neg, Op::Fmt]); }
fn anf_cols(bencher: &mut Bencher) { _bench_cols(bencher, &[Op::Add, Op::Neg, Op::Fmt]); }

fn _bench_rows(bencher: &mut Bencher, prog: &[Op]) {

    let mut rows = Vec::with_capacity(1024);
    for i in 0 .. (rows.capacity() as i32) {
        rows.push(vec![Err("hello".to_string()), Ok(i), Ok(i), Ok(i)]);
    }
    let len = 4;

    bencher.iter(|| {
        for row in rows.iter_mut() {
            row.truncate(len);
            for op in prog.iter() {
                row.push(op.eval(&row[..]));
            }
        }
        bencher::black_box(&rows);
    });
}

fn _bench_cols(bencher: &mut Bencher, prog: &[Op]) {

    let mut rows = Vec::with_capacity(1024);
    for i in 0 .. (rows.capacity() as i32) {
        rows.push((Err::<i32, String>("hello".to_string()), Ok::<i32, String>(i), Ok::<i32, String>(i), Ok::<i32, String>(i)));
    }
    
    let cols = Columnar::into_columns(rows.into_iter());
    let mut cols = vec![Err(cols.0.errs), Ok(cols.1.oks), Ok(cols.2.oks), Ok(cols.3.oks)];
    let len = cols.len();

    bencher.iter(|| {
        for op in prog.iter() {
            cols.push(op.eval_vec(&cols));
        }
        bencher::black_box(&cols);
        cols.truncate(len);
    });
}

benchmark_group!(
    cols,
    add_cols,
    neg_cols,
    fmt_cols,
    anf_cols,
);

benchmark_group!(
    rows,
    add_rows,
    neg_rows,
    fmt_rows,
    anf_rows,
);

benchmark_main!(cols, rows);
