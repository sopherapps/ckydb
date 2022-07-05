import os

import pytest

import ckydb

_db_folder = os.path.join(os.path.dirname(os.path.dirname(__file__)), "db")
_records = [
    ("hey", "English"),
    ("hi", "English"),
    ("salut", "French"),
    ("bonjour", "French"),
    ("hola", "Spanish"),
    ("oi", "Portuguese"),
    ("mulimuta", "Runyoro"),
]
_updates = [
    ("hey", "Jane"),
    ("hi", "John"),
    ("hola", "Juan"),
    ("oi", "Ronaldo"),
    ("mulimuta", "Aliguma"),
]
_updates_args = [(k, v, n) for ((k, v), (_, n)) in zip(_records, _updates)]


@pytest.mark.parametrize("k, v", _records)
def test_benchmark_set(benchmark, k, v):
    """Benchmarks the ckydb.set operation"""
    with ckydb.connect(_db_folder) as db:
        benchmark(db.set, k, v)


@pytest.mark.parametrize("k, v", _records)
def test_benchmark_get(benchmark, k, v):
    """Benchmarks the ckydb.get operation"""
    with ckydb.connect(_db_folder) as db:
        db.set(k, v)
        result = benchmark(db.get, k)
        assert result == v


@pytest.mark.parametrize("key, value, new_value", _updates_args)
def test_benchmark_update(benchmark, key, value, new_value):
    """Benchmarks the ckydb.set update operation"""
    with ckydb.connect(_db_folder) as db:
        db.set(key, value)
        benchmark(db.set, key, new_value)


@pytest.mark.parametrize("k, v", _records)
def test_benchmark_delete(benchmark, k, v):
    """Benchmarks the ckydb.delete operation"""
    with ckydb.connect(_db_folder) as db:
        benchmark.pedantic(db.delete, setup=lambda: _load_db(db), args=(k,), iterations=1, rounds=100)


def test_benchmark_clear(benchmark):
    """Benchmarks the ckydb.clear operation"""
    with ckydb.connect(_db_folder) as db:
        benchmark.pedantic(db.clear, setup=lambda: _load_db(db), iterations=1, rounds=100)


def _load_db(db: ckydb.Ckydb):
    """Loads the database with dummy data"""
    for (k, v) in _records:
        db.set(k, v)
