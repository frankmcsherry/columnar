use bencher::{benchmark_group, benchmark_main, Bencher};

use columnar::{Columnar, Index};
use columnar::Strings;

#[derive(Debug)]
pub enum Op {
    Add,    // binary
    Neg,    // unary
    Len,    // unary
    Fmt,    // unary
    Lit(i32),
}

type Val = Result<i32, String>;
type Row = Vec<Val>;
type Col = Result<Vec<i32>, Strings>;

impl Op {
    #[inline(always)]
    fn arity(&self) -> usize {
        match self {
            Op::Lit(_) => 0,
            Op::Neg | Op::Len | Op::Fmt => 1,
            Op::Add => 2,
        }
    }

    #[inline(always)]
    fn eval(&self, dataz: &[Val]) -> Val {
        match (self, dataz) {
            (Op::Add, [.., Ok(a), Ok(b)]) => { Ok(a + b) },
            (Op::Neg, [.., Ok(a)]) => { Ok(-a) },
            (Op::Len, [.., Err(a)]) => { Ok(a.len() as i32) },
            (Op::Fmt, [.., Ok(a)]) => { Err(format!("{:?}", a)) },
            (Op::Lit(lit), [..]) => Ok(*lit),
            _ => panic!("terrible argument!"),
        }
    }

    fn eval_vec(&self, dataz: &[Col]) -> Col {
        match (self, dataz) {
            (Op::Add, [.., Ok(aa), Ok(bb)]) => {
                Ok(aa.iter().zip(bb).map(|(a,b)| a + b).collect())
            },
            (Op::Neg, [.., Ok(aa)]) => {
                Ok(aa.iter().map(|a| -a).collect())
            },
            (Op::Len, [.., Err(aa)]) => {
                Ok(aa.into_index_iter().map(|a| a.len() as i32).collect())
            },
            (Op::Fmt, [.., Ok(aa)]) => {
                let mut result = Strings::default();
                for a in aa.iter() {
                    use columnar::Push;
                    result.push(&format_args!("{:?}", a));
                }
                Err(result)
            },
            (Op::Lit(lit), [.., x]) => {
                use columnar::Len;
                let len = match x { Ok(x) => x.len(), Err(x) => x.len() };
                Ok(std::iter::repeat(*lit).take(len).collect())
            }
            _ => panic!("terrible argument!"),
        }
    }
}

fn add_rows(bencher: &mut Bencher) { _bench_rows(bencher, &[Op::Lit(0), Op::Add]); }
fn add_cols(bencher: &mut Bencher) { _bench_cols(bencher, &[Op::Lit(0), Op::Add]); }
fn neg_rows(bencher: &mut Bencher) { _bench_rows(bencher, &[Op::Neg]); }
fn neg_cols(bencher: &mut Bencher) { _bench_cols(bencher, &[Op::Neg]); }
fn fmt_rows(bencher: &mut Bencher) { _bench_rows(bencher, &[Op::Fmt, Op::Len]); }
fn fmt_cols(bencher: &mut Bencher) { _bench_cols(bencher, &[Op::Fmt, Op::Len]); }
fn ana_rows(bencher: &mut Bencher) { _bench_rows(bencher, &[Op::Lit(0), Op::Lit(0), Op::Add, Op::Neg, Op::Add]); }
fn ana_cols(bencher: &mut Bencher) { _bench_cols(bencher, &[Op::Lit(0), Op::Lit(0), Op::Add, Op::Neg, Op::Add]); }
fn anf_rows(bencher: &mut Bencher) { _bench_rows(bencher, &[Op::Lit(0), Op::Add, Op::Neg, Op::Fmt, Op::Len]); }
fn anf_cols(bencher: &mut Bencher) { _bench_cols(bencher, &[Op::Lit(0), Op::Add, Op::Neg, Op::Fmt, Op::Len]); }

fn _bench_rows(bencher: &mut Bencher, prog: &[Op]) {

    let mut rows: Vec<Row> = Vec::with_capacity(1024);
    for i in 0 .. (rows.capacity() as i32) {
        rows.push(vec![Ok(i), Ok(i+1), Ok(i+2)]);
    }
    bencher.iter(|| {
        for row in rows.iter_mut() {
            for op in prog.iter() {
                let val = op.eval(&row[..]);
                for _ in 0 .. op.arity() { row.pop(); }
                row.push(val);
            }
        }
        bencher::black_box(&rows);
    });
}

fn _bench_cols(bencher: &mut Bencher, prog: &[Op]) {

    let mut rows = Vec::with_capacity(1024);
    for i in 0 .. (rows.capacity() as i32) {
        rows.push((i, i+1, i+2));
    }

    let cols = Columnar::into_columns(rows.into_iter());
    let mut cols: Vec<Col> = vec![Ok(cols.0), Ok(cols.1), Ok(cols.2)];
    let len = cols.len();

    bencher.iter(|| {
        for op in prog.iter() {
            let vals = op.eval_vec(&cols);
            for _ in 0 .. op.arity() { cols.pop(); }
            cols.push(vals);
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
    ana_cols,
    anf_cols,
);

benchmark_group!(
    rows,
    add_rows,
    neg_rows,
    fmt_rows,
    ana_rows,
    anf_rows,
);

benchmark_main!(cols, rows);
