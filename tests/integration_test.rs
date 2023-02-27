use std::{
    fs::{create_dir_all, remove_dir_all},
    path::PathBuf,
};

use rdbc_sqlite3::*;

use rdbc_rs::*;

#[allow(dead_code)]
fn create_test_dir() -> PathBuf {
    let path: PathBuf = ".test".into();

    if path.exists() {
        remove_dir_all(path.to_owned()).unwrap();
    }

    create_dir_all(path.to_owned()).unwrap();

    path
}

#[async_std::test]
async fn test_create_table() {
    _ = pretty_env_logger::try_init();
    _ = register_sqlite3();

    let mut db: Database = open("sqlite3", ":memory:").unwrap();

    let mut stmt = db
        .prepare("CREATE TABLE t(x INTEGER PRIMARY KEY ASC, y TEXT, z NUMERIC);")
        .await
        .unwrap();

    stmt.execute(vec![]).await.unwrap();
}

#[async_std::test]
async fn test_stmt() {
    _ = pretty_env_logger::try_init();
    _ = register_sqlite3();

    // let path = create_test_dir().join("test.db");

    // let path = format!("file:{}", path.to_string_lossy());

    // let mut db = open("sqlite3", &path).unwrap();

    let mut db: Database = open("sqlite3", "file:memdb_stmt?mode=memory&cache=shared").unwrap();

    let mut stmt = db
        .prepare("CREATE TABLE t(x INTEGER PRIMARY KEY ASC, y TEXT, z NUMERIC);")
        .await
        .unwrap();

    stmt.execute(vec![]).await.unwrap();

    let mut stmt = db.prepare("INSERT INTO t(y,z) VALUES(?,?);").await.unwrap();

    assert_eq!(stmt.num_input().await.unwrap(), Some(2));

    let result = stmt
        .execute(vec![
            rdbc_rs::Argument {
                name: rdbc_rs::ArgName::Offset(1),
                value: driver::ArgValue::String("hello world".to_owned()),
            },
            rdbc_rs::Argument {
                name: rdbc_rs::ArgName::Offset(2),
                value: driver::ArgValue::String("7.82910138827292".to_owned()),
            },
        ])
        .await
        .unwrap();

    assert_eq!(result.last_insert_id, 1);
    assert_eq!(result.raws_affected, 1);

    let mut stmt = db
        .prepare("SELECT * FROM t ORDER BY x ASC LIMIT 0,1")
        .await
        .unwrap();

    assert_eq!(stmt.num_input().await.unwrap(), Some(0));

    let mut rows = stmt.query(vec![]).await.unwrap();

    let columns = rows.colunms().await.unwrap();

    assert_eq!(
        columns.clone(),
        vec![
            Column {
                column_index: 0,
                column_name: "x".to_owned(),
                column_decltype: "INTEGER".to_owned(),
                column_decltype_len: Some(8),
            },
            Column {
                column_index: 1,
                column_name: "y".to_owned(),
                column_decltype: "TEXT".to_owned(),
                column_decltype_len: None,
            },
            Column {
                column_index: 2,
                column_name: "z".to_owned(),
                column_decltype: "NUMERIC".to_owned(),
                column_decltype_len: None,
            }
        ]
    );

    assert!(rows.next().await.unwrap());

    let id = rows.get("x", driver::ColumnType::I64).await.unwrap();

    assert_eq!(id, Some(rdbc_rs::ArgValue::I64(1)));

    let y = rows.get("y", driver::ColumnType::String).await.unwrap();

    assert_eq!(y, Some(rdbc_rs::ArgValue::String("hello world".to_owned())));

    let z = rows.get("z", driver::ColumnType::String).await.unwrap();

    assert_eq!(
        z,
        Some(rdbc_rs::ArgValue::String("7.82910138827292".to_owned()))
    );

    assert!(!rows.next().await.unwrap());
}

#[async_std::test]
async fn test_stmt_named_args() {
    _ = pretty_env_logger::try_init();
    _ = register_sqlite3();

    // let path = create_test_dir().join("test.db");

    // let path = format!("file:{}", path.to_string_lossy());

    // let mut db = open("sqlite3", &path).unwrap();

    let mut db: Database = open(
        "sqlite3",
        "file:memdb_stmt_named_args?mode=memory&cache=shared",
    )
    .unwrap();

    let mut stmt = db
        .prepare("CREATE TABLE t(x INTEGER PRIMARY KEY ASC, y TEXT, z NUMERIC);")
        .await
        .unwrap();

    stmt.execute(vec![]).await.unwrap();

    let mut stmt = db
        .prepare("INSERT INTO t(y,z) VALUES(:y,:z);")
        .await
        .unwrap();

    assert_eq!(stmt.num_input().await.unwrap(), Some(2));

    let result = stmt
        .execute(vec![
            rdbc_rs::Argument {
                name: rdbc_rs::ArgName::String(":y".to_owned()),
                value: driver::ArgValue::String("hello world".to_owned()),
            },
            rdbc_rs::Argument {
                name: rdbc_rs::ArgName::String(":z".to_owned()),
                value: driver::ArgValue::String("7.82910138827292".to_owned()),
            },
        ])
        .await
        .unwrap();

    assert_eq!(result.last_insert_id, 1);
    assert_eq!(result.raws_affected, 1);

    let mut stmt = db.prepare("SELECT * FROM t").await.unwrap();

    assert_eq!(stmt.num_input().await.unwrap(), Some(0));

    let mut rows = stmt.query(vec![]).await.unwrap();

    assert!(rows.next().await.unwrap());

    let id = rows.get(0, driver::ColumnType::I64).await.unwrap();

    assert_eq!(id, Some(rdbc_rs::ArgValue::I64(1)));

    let y = rows.get(1, driver::ColumnType::String).await.unwrap();

    assert_eq!(y, Some(rdbc_rs::ArgValue::String("hello world".to_owned())));

    let z = rows.get(2, driver::ColumnType::String).await.unwrap();

    assert_eq!(
        z,
        Some(rdbc_rs::ArgValue::String("7.82910138827292".to_owned()))
    );

    assert!(!rows.next().await.unwrap());
}

#[async_std::test]
async fn test_multi_stmt() {
    _ = pretty_env_logger::try_init();
    _ = register_sqlite3();

    let mut db: Database =
        open("sqlite3", "file:test_multi_stmt?mode=memory&cache=shared").unwrap();

    let mut stmt = db
        .prepare("CREATE TABLE t(x INTEGER PRIMARY KEY ASC, y TEXT, z NUMERIC);")
        .await
        .unwrap();

    stmt.execute(vec![]).await.unwrap();

    let mut stmt = db
        .prepare("INSERT INTO t(y,z) VALUES(@y,:z);")
        .await
        .unwrap();

    let mut stmt2 = db.prepare("SELECT * FROM t").await.unwrap();

    let result = stmt
        .execute(vec![
            rdbc_rs::Argument {
                name: rdbc_rs::ArgName::String("@y".to_owned()),
                value: driver::ArgValue::String("hello world".to_owned()),
            },
            rdbc_rs::Argument {
                name: rdbc_rs::ArgName::String(":z".to_owned()),
                value: driver::ArgValue::String("7.82910138827292".to_owned()),
            },
        ])
        .await
        .unwrap();

    assert_eq!(result.last_insert_id, 1);
    assert_eq!(result.raws_affected, 1);

    let mut rows = stmt2.query(vec![]).await.unwrap();

    assert!(rows.next().await.unwrap());

    let id = rows
        .get(rdbc_rs::ArgName::Offset(0), driver::ColumnType::I64)
        .await
        .unwrap();

    assert_eq!(id.unwrap(), rdbc_rs::ArgValue::I64(1));

    let y = rows
        .get(rdbc_rs::ArgName::Offset(1), driver::ColumnType::String)
        .await
        .unwrap();

    assert_eq!(
        y.unwrap(),
        rdbc_rs::ArgValue::String("hello world".to_owned())
    );

    let z = rows
        .get(rdbc_rs::ArgName::Offset(2), driver::ColumnType::String)
        .await
        .unwrap();

    assert_eq!(
        z.unwrap(),
        rdbc_rs::ArgValue::String("7.82910138827292".to_owned())
    );

    assert!(!rows.next().await.unwrap());
}

#[async_std::test]
async fn test_tx_commit_data() {
    _ = pretty_env_logger::try_init();
    _ = register_sqlite3();

    // let path = create_test_dir().join("test.db");

    // let path = format!("file:{}", path.to_string_lossy());

    // let mut db = open("sqlite3", &path).unwrap();

    let mut db: Database = open("sqlite3", "file:memdb_commit?mode=memory&cache=shared").unwrap();

    {
        let mut tx = db.begin().await.unwrap();

        let mut stmt = tx
            .prepare("CREATE TABLE t(x INTEGER PRIMARY KEY ASC, y TEXT, z NUMERIC);")
            .await
            .unwrap();

        stmt.execute(vec![]).await.unwrap();

        let mut stmt = tx.prepare("INSERT INTO t(y,z) VALUES(?,?);").await.unwrap();

        assert_eq!(stmt.num_input().await.unwrap(), Some(2));

        let result = stmt
            .execute(vec![
                rdbc_rs::Argument {
                    name: rdbc_rs::ArgName::Offset(1),
                    value: rdbc_rs::ArgValue::String("hello world".to_owned()),
                },
                rdbc_rs::Argument {
                    name: rdbc_rs::ArgName::Offset(2),
                    value: rdbc_rs::ArgValue::String("7.82910138827292".to_owned()),
                },
            ])
            .await
            .unwrap();

        assert_eq!(result.last_insert_id, 1);
        assert_eq!(result.raws_affected, 1);

        let mut stmt = tx.prepare("SELECT * FROM t").await.unwrap();

        assert_eq!(stmt.num_input().await.unwrap(), Some(0));

        let mut rows = stmt.query(vec![]).await.unwrap();

        let columns = rows.colunms().await.unwrap();

        assert_eq!(
            columns,
            vec![
                Column {
                    column_index: 0,
                    column_name: "x".to_owned(),
                    column_decltype: "INTEGER".to_owned(),
                    column_decltype_len: Some(8),
                },
                Column {
                    column_index: 1,
                    column_name: "y".to_owned(),
                    column_decltype: "TEXT".to_owned(),
                    column_decltype_len: None,
                },
                Column {
                    column_index: 2,
                    column_name: "z".to_owned(),
                    column_decltype: "NUMERIC".to_owned(),
                    column_decltype_len: None,
                }
            ]
        );

        assert!(rows.next().await.unwrap());

        let id = rows
            .get(rdbc_rs::ArgName::Offset(0), driver::ColumnType::I64)
            .await
            .unwrap();

        assert_eq!(id.unwrap(), rdbc_rs::ArgValue::I64(1));

        let y = rows
            .get(rdbc_rs::ArgName::Offset(1), driver::ColumnType::String)
            .await
            .unwrap();

        assert_eq!(
            y.unwrap(),
            rdbc_rs::ArgValue::String("hello world".to_owned())
        );

        let z = rows
            .get(rdbc_rs::ArgName::Offset(2), driver::ColumnType::String)
            .await
            .unwrap();

        assert_eq!(
            z.unwrap(),
            rdbc_rs::ArgValue::String("7.82910138827292".to_owned())
        );

        assert!(!rows.next().await.unwrap());

        tx.commit().await.unwrap();
    }

    let mut stmt = db.prepare("SELECT * FROM t").await.unwrap();

    assert_eq!(stmt.num_input().await.unwrap(), Some(0));

    let mut rows = stmt.query(vec![]).await.unwrap();

    let columns = rows.colunms().await.unwrap();

    assert_eq!(
        columns,
        vec![
            Column {
                column_index: 0,
                column_name: "x".to_owned(),
                column_decltype: "INTEGER".to_owned(),
                column_decltype_len: Some(8),
            },
            Column {
                column_index: 1,
                column_name: "y".to_owned(),
                column_decltype: "TEXT".to_owned(),
                column_decltype_len: None,
            },
            Column {
                column_index: 2,
                column_name: "z".to_owned(),
                column_decltype: "NUMERIC".to_owned(),
                column_decltype_len: None,
            }
        ]
    );

    assert!(rows.next().await.unwrap());

    let id = rows
        .get(rdbc_rs::ArgName::Offset(0), driver::ColumnType::I64)
        .await
        .unwrap();

    assert_eq!(id.unwrap(), rdbc_rs::ArgValue::I64(1));

    let y = rows
        .get(rdbc_rs::ArgName::Offset(1), driver::ColumnType::String)
        .await
        .unwrap();

    assert_eq!(
        y.unwrap(),
        rdbc_rs::ArgValue::String("hello world".to_owned())
    );

    let z = rows
        .get(rdbc_rs::ArgName::Offset(2), driver::ColumnType::String)
        .await
        .unwrap();

    assert_eq!(
        z.unwrap(),
        rdbc_rs::ArgValue::String("7.82910138827292".to_owned())
    );
}

#[async_std::test]
async fn test_tx_rollback_data() {
    _ = pretty_env_logger::try_init();
    _ = register_sqlite3();

    // let path = create_test_dir().join("test.db");

    // let path = format!("file:{}", path.to_string_lossy());

    // let mut db = open("sqlite3", &path).unwrap();

    let mut db: Database = open("sqlite3", "file:memdb_rollback?mode=memory&cache=shared").unwrap();

    {
        let mut tx = db.begin().await.unwrap();

        let mut stmt = tx
            .prepare("CREATE TABLE t(x INTEGER PRIMARY KEY ASC, y TEXT, z NUMERIC);")
            .await
            .unwrap();

        stmt.execute(vec![]).await.unwrap();

        let mut stmt = tx.prepare("INSERT INTO t(y,z) VALUES(?,?);").await.unwrap();

        assert_eq!(stmt.num_input().await.unwrap(), Some(2));

        let result = stmt
            .execute(vec![
                rdbc_rs::Argument {
                    name: rdbc_rs::ArgName::Offset(1),
                    value: rdbc_rs::ArgValue::String("hello world".to_owned()),
                },
                rdbc_rs::Argument {
                    name: rdbc_rs::ArgName::Offset(2),
                    value: rdbc_rs::ArgValue::String("7.82910138827292".to_owned()),
                },
            ])
            .await
            .unwrap();

        assert_eq!(result.last_insert_id, 1);
        assert_eq!(result.raws_affected, 1);

        let mut stmt = tx.prepare("SELECT * FROM t").await.unwrap();

        assert_eq!(stmt.num_input().await.unwrap(), Some(0));

        let mut rows = stmt.query(vec![]).await.unwrap();

        let columns = rows.colunms().await.unwrap();

        assert_eq!(
            columns,
            vec![
                Column {
                    column_index: 0,
                    column_name: "x".to_owned(),
                    column_decltype: "INTEGER".to_owned(),
                    column_decltype_len: Some(8),
                },
                Column {
                    column_index: 1,
                    column_name: "y".to_owned(),
                    column_decltype: "TEXT".to_owned(),
                    column_decltype_len: None,
                },
                Column {
                    column_index: 2,
                    column_name: "z".to_owned(),
                    column_decltype: "NUMERIC".to_owned(),
                    column_decltype_len: None,
                }
            ]
        );

        assert!(rows.next().await.unwrap());

        let id = rows
            .get(rdbc_rs::ArgName::Offset(0), driver::ColumnType::I64)
            .await
            .unwrap();

        assert_eq!(id.unwrap(), rdbc_rs::ArgValue::I64(1));

        let y = rows
            .get(rdbc_rs::ArgName::Offset(1), driver::ColumnType::String)
            .await
            .unwrap();

        assert_eq!(
            y.unwrap(),
            rdbc_rs::ArgValue::String("hello world".to_owned())
        );

        let z = rows
            .get(rdbc_rs::ArgName::Offset(2), driver::ColumnType::String)
            .await
            .unwrap();

        assert_eq!(
            z.unwrap(),
            rdbc_rs::ArgValue::String("7.82910138827292".to_owned())
        );

        assert!(!rows.next().await.unwrap());
    }

    // no such table: t
    assert!(db.prepare("SELECT * FROM t").await.is_err());
}
