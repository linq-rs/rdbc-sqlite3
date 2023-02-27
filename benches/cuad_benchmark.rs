use criterion::Criterion;
use criterion::{criterion_group, criterion_main};

// This is a struct that tells Criterion.rs to use the "futures" crate's current-thread executor
use criterion::async_executor::FuturesExecutor;
use rdbc_rs::{ArgName, ArgValue, Database, Preparable};

use std::time::Duration;
use std::{
    fs::{create_dir_all, remove_dir_all},
    path::PathBuf,
};

use rdbc_sqlite3::*;

#[allow(dead_code)]
async fn prepare_benchmark() -> rdbc_rs::Database {
    _ = register_sqlite3();

    let path: PathBuf = ".test".into();

    if path.exists() {
        remove_dir_all(path.to_owned()).unwrap();
    }

    create_dir_all(path.to_owned()).unwrap();

    let path = path.join("test.db");

    let path = format!("file:{}", path.to_string_lossy());

    let mut db: Database = rdbc_rs::open("sqlite3", &path).unwrap();

    // let mut db = rdbc_rs::open("sqlite3", "file:memdb_commit?mode=memory&cache=shared").unwrap();

    let mut stmt = db
        .prepare("CREATE TABLE t(x INTEGER PRIMARY KEY ASC, y TEXT, z NUMERIC);")
        .await
        .unwrap();

    stmt.execute(vec![]).await.unwrap();

    db
}

async fn insert_one_row(mut db: rdbc_rs::Database) {
    let mut stmt = db.prepare("INSERT INTO t(y,z) VALUES(?,?);").await.unwrap();

    _ = stmt
        .execute(vec![
            rdbc_rs::Argument {
                name: ArgName::Offset(1),
                value: ArgValue::String("hello world".to_owned()),
            },
            rdbc_rs::Argument {
                name: ArgName::Offset(2),
                value: ArgValue::String("7.82910138827292".to_owned()),
            },
        ])
        .await
        .unwrap();
}

fn insert_benchmark(c: &mut Criterion) {
    let db = async_std::task::block_on(async { prepare_benchmark().await });

    let mut group = c.benchmark_group("cuad");

    group.measurement_time(Duration::from_secs(10));

    group.bench_function("insert benchmark", |b| {
        b.to_async(FuturesExecutor)
            .iter(|| insert_one_row(db.clone()));
    });

    group.finish();
}

criterion_group!(benches, insert_benchmark);

criterion_main!(benches);
