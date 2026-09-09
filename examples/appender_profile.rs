//! Fills a derived container either through `Append` handles or by pushing
//! owned records, for profiling the appender code paths.
//!
//! Usage: `appender_profile [append|push|both] [rounds]`.

use std::fmt::Write;
use std::time::Instant;

use columnar::{Append, Clear, Columnar, ContainerOf, Len};

#[derive(Columnar, Debug, Clone)]
pub enum Kind {
    Plain,
    Numbered(u32),
    Located { x: u64, y: u64 },
}

#[derive(Columnar, Debug, Clone)]
pub struct Record {
    pub id: u64,
    pub name: String,
    pub tags: Vec<String>,
    pub kind: Kind,
    pub score: Option<f64>,
}

const RECORDS: usize = 20_000;

fn fill_append(columns: &mut ContainerOf<Record>) {
    for i in 0..RECORDS as u64 {
        let mut record = columns.appender();
        *record.id = i;
        write!(record.name, "record-{}", i).unwrap();
        for t in 0..(i % 4) {
            write!(record.tags.appender(), "tag-{}-{}", i, t).unwrap();
        }
        match i % 3 {
            0 => { record.kind.Plain(); }
            1 => { **record.kind.Numbered() = i as u32; }
            _ => {
                let mut located = record.kind.Located();
                *located.0 = i;
                *located.1 = i * 2;
            }
        }
        if i % 2 == 0 {
            **record.score.some() = i as f64 * 0.5;
        } else {
            record.score.none();
        }
    }
}

fn build_owned() -> Vec<Record> {
    (0..RECORDS as u64).map(|i| Record {
        id: i,
        name: format!("record-{}", i),
        tags: (0..(i % 4)).map(|t| format!("tag-{}-{}", i, t)).collect(),
        kind: match i % 3 {
            0 => Kind::Plain,
            1 => Kind::Numbered(i as u32),
            _ => Kind::Located { x: i, y: i * 2 },
        },
        score: if i % 2 == 0 { Some(i as f64 * 0.5) } else { None },
    }).collect()
}

fn fill_push(columns: &mut ContainerOf<Record>) {
    use columnar::Push;
    for record in build_owned() {
        columns.push(&record);
    }
}


fn main() {
    let mode = std::env::args().nth(1).unwrap_or_else(|| "both".to_string());
    let rounds: usize = std::env::args().nth(2).and_then(|r| r.parse().ok()).unwrap_or(200);

    let mut columns: ContainerOf<Record> = Default::default();

    if mode == "append" || mode == "both" {
        let start = Instant::now();
        for _ in 0..rounds {
            columns.clear();
            fill_append(&mut columns);
        }
        let elapsed = start.elapsed();
        println!("append: {} records in {:?} ({:.1} ns/record)", columns.len(), elapsed, elapsed.as_nanos() as f64 / (rounds * RECORDS) as f64);
    }
    if mode == "push" || mode == "both" {
        let start = Instant::now();
        for _ in 0..rounds {
            columns.clear();
            fill_push(&mut columns);
        }
        let elapsed = start.elapsed();
        println!("push:   {} records in {:?} ({:.1} ns/record)", columns.len(), elapsed, elapsed.as_nanos() as f64 / (rounds * RECORDS) as f64);
    }
}
