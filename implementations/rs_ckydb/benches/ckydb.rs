use ckydb::{self, Controller};
use criterion::{black_box, criterion_group, criterion_main, Criterion};

const RECORDS: [(&str, &str); 7] = [
    ("hey", "English"),
    ("hi", "English"),
    ("salut", "French"),
    ("bonjour", "French"),
    ("hola", "Spanish"),
    ("oi", "Portuguese"),
    ("mulimuta", "Runyoro"),
];

const UPDATES: [(&str, &str); 5] = [
    ("hey", "Jane"),
    ("hi", "John"),
    ("hola", "Santos"),
    ("oi", "Ronaldo"),
    ("mulimuta", "Aliguma"),
];

// Setting
fn setting_benchmark(c: &mut Criterion) {
    let mut db = ckydb::connect("db", 4.0, 60.0).unwrap();
    for (k, v) in RECORDS {
        c.bench_function(&format!("set {} {}", k, v), |b| {
            b.iter(|| db.set(black_box(k), black_box(v)))
        });
    }
}

// Updating
fn updating_benchmark(c: &mut Criterion) {
    let mut db = ckydb::connect("db", 4.0, 60.0).unwrap();
    for (k, v) in RECORDS {
        db.set(k, v).expect(&format!("set {}", k));
    }
    for (k, v) in UPDATES {
        c.bench_function(&format!("update {} to {}", k, v), |b| {
            b.iter(|| db.set(black_box(k), black_box(v)))
        });
    }
}

// Getting
fn getting_benchmark(c: &mut Criterion) {
    let mut db = ckydb::connect("db", 4.0, 60.0).unwrap();
    for (k, v) in RECORDS {
        db.set(k, v).expect(&format!("set {}", k));
    }
    for (k, _) in RECORDS {
        c.bench_function(&format!("get {}", k), |b| b.iter(|| db.get(black_box(k))));
    }
}

// Deleting
fn deleting_benchmark(c: &mut Criterion) {
    let mut db = ckydb::connect("db", 4.0, 60.0).unwrap();
    for (k, v) in RECORDS {
        db.set(k, v).expect(&format!("set {}", k));
    }

    for (k, _) in RECORDS {
        c.bench_function(&format!("delete {}", k), |b| {
            b.iter(|| db.delete(black_box(k)))
        });
    }
}

// Clearing
fn clearing_benchmark(c: &mut Criterion) {
    let mut db = ckydb::connect("db", 4.0, 60.0).unwrap();
    for (k, v) in RECORDS {
        db.set(k, v).expect(&format!("set {}", k));
    }

    c.bench_function("clear", |b| b.iter(|| db.clear()));
}

criterion_group!(
    benches,
    setting_benchmark,
    updating_benchmark,
    getting_benchmark,
    deleting_benchmark,
    clearing_benchmark
);
criterion_main!(benches);
