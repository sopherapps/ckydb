use ckydb::{self, Controller};
use criterion::{black_box, criterion_group, criterion_main, Criterion};

// Setting
fn setting_benchmark(c: &mut Criterion) {
    let mut db = ckydb::connect("db", 4.0, 60.0).unwrap();
    c.bench_function("set hello world", |b| {
        b.iter(|| db.set(black_box("hello"), black_box("world")))
    });
}

// Updating
fn updating_benchmark(c: &mut Criterion) {
    let mut db = ckydb::connect("db", 4.0, 60.0).unwrap();
    db.set("hello", "world").expect("set hello");
    c.bench_function("update hello to foo", |b| {
        b.iter(|| db.set(black_box("hello"), black_box("foo")))
    });
}

// Deleting
fn deleting_benchmark(c: &mut Criterion) {
    let mut db = ckydb::connect("db", 4.0, 60.0).unwrap();
    db.set("hello", "world").expect("set hello");
    c.bench_function("delete hello world", |b| {
        b.iter(|| db.delete(black_box("hello")))
    });
}

// Clearing
fn clearing_benchmark(c: &mut Criterion) {
    let mut db = ckydb::connect("db", 4.0, 60.0).unwrap();
    db.set("hello", "world").expect("set hello");
    c.bench_function("clear", |b| b.iter(|| db.clear()));
}

criterion_group!(
    benches,
    setting_benchmark,
    updating_benchmark,
    deleting_benchmark,
    clearing_benchmark
);
criterion_main!(benches);
