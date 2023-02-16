use std::path::PathBuf;

use criterion::{criterion_group, criterion_main, BatchSize::SmallInput, Criterion};
use kvs::{BitcaskEngine, KvsEngine, Result};
use rand::{seq::IteratorRandom, thread_rng};
use tempfile::TempDir;
use walkdir::WalkDir;

fn get_dir_size(path: PathBuf) -> Result<u64> {
    let entries = WalkDir::new(path).into_iter();
    let len: walkdir::Result<u64> = entries
        .map(|res| {
            res.and_then(|entry| entry.metadata())
                .map(|metadata| metadata.len())
        })
        .sum();
    Ok(len.unwrap())
}

fn write_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("write");
    let mut rng = &mut thread_rng();
    let range = (1..10000).choose_multiple(&mut rng, 100000).to_vec();
    group.bench_function("kvs", |b| {
        b.iter_batched(
            || {
                let temp_dir =
                    TempDir::new().expect("unable to create temporary working directory");
                let store = BitcaskEngine::open(temp_dir.path()).expect("unable to init KvStore");
                (store, temp_dir.into_path())
            },
            |(store, _)| {
                for i in &range {
                    store
                        .set(format!("key{}", i), format!("value{}", i))
                        .expect("unable to write KvStore");
                }
                // let size = get_dir_size(path).unwrap();
                // println!("dir size: {}", size);
            },
            SmallInput,
        )
    });
    group.bench_function("sled", |b| {
        b.iter_batched(
            || {
                let temp_dir =
                    TempDir::new().expect("unable to create temporary working directory");
                let store = sled::open(temp_dir.path()).expect("unable to init SledKvsEngine");
                store
            },
            |store| {
                for i in &range {
                    store
                        .insert(format!("key{}", i), format!("value{}", i).as_bytes())
                        .expect("unable to write SledKvsEngine");
                }
            },
            SmallInput,
        )
    });
    group.finish()
}

fn read_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("read");
    let mut rng = &mut thread_rng();
    let write_key_range = (1..10000).choose_multiple(&mut rng, 100000).to_vec();
    let read_range = write_key_range.iter().choose_multiple(&mut rng, 10000);
    group.bench_function("kvs", |b| {
        b.iter_batched(
            || {
                let temp_dir =
                    TempDir::new().expect("unable to create temporary working directory");
                let store = BitcaskEngine::open(temp_dir.path()).expect("unable to init KvStore");
                for i in &write_key_range {
                    store
                        .set(format!("key{}", i), format!("value{}", i))
                        .expect("unable to write KvStore");
                }
                store.flush().unwrap();
                store
            },
            |store| {
                for i in &read_range {
                    assert_eq!(
                        format!("value{}", i),
                        store
                            .get(format!("key{}", i))
                            .expect("unable to read KvStore")
                            .unwrap()
                    );
                }
                // report dir
            },
            SmallInput,
        )
    });
    group.bench_function("sled", |b| {
        b.iter_batched(
            || {
                let temp_dir =
                    TempDir::new().expect("unable to create temporary working directory");
                let store = sled::open(temp_dir.path()).expect("unable to init SledKvsEngine");
                for i in &write_key_range {
                    store
                        .insert(format!("key{}", i), format!("value{}", i).as_bytes())
                        .expect("unable to write SledKvsEngine");
                }
                store
            },
            |store| {
                for i in &read_range {
                    assert_eq!(
                        format!("value{}", i),
                        String::from_utf8(
                            store
                                .get(format!("key{}", i))
                                .expect("unable to read SledKvsEngine")
                                .unwrap()
                                .to_vec()
                        )
                        .unwrap()
                    );
                }
            },
            SmallInput,
        )
    });
    group.finish()
}

criterion_group!(benches, write_benchmark, read_benchmark);
criterion_main!(benches);
