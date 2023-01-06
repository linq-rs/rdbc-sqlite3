/// ! sqlite3 c api wrapper mod
///
use std::{
    ffi::{c_void, CStr, CString},
    os::raw::c_char,
    ptr::{null, null_mut},
    slice::from_raw_parts,
};

use super::error;

use sqlite3_sys::*;

use anyhow::{Ok, Result};

use rdbc::driver::{self, callback::BoxedCallback, RDBCError};

pub fn colunm_decltype(
    stmt: *mut sqlite3_stmt,
    i: i32,
) -> (driver::ColumnType, String, Option<u64>) {
    let decltype = unsafe { CStr::from_ptr(sqlite3_column_decltype(stmt, i)) }.to_string_lossy();

    match decltype.as_ref() {
        "INT" | "INTEGER" | "TINYINT" | "SMALLINT" | "MEDIUMINT" | "BIGINT"
        | "UNSIGNED BIG INT" | "INT2" | "INT8" => {
            (driver::ColumnType::I64, decltype.to_string(), Some(8))
        }
        "CHARACTER(20)"
        | "VARCHAR(255)"
        | "VARYING CHARACTER(255)"
        | "NCHAR(55)"
        | "NATIVE CHARACTER(70)"
        | "NVARCHAR(100)"
        | "TEXT"
        | "CLOB" => (driver::ColumnType::String, decltype.to_string(), None),
        "BLOB" => (driver::ColumnType::Bytes, decltype.to_string(), None),
        "REAL" | "DOUBLE" | "DOUBLE PRECISION" | "FLOAT" => {
            (driver::ColumnType::F64, decltype.to_string(), Some(8))
        }
        _ => (driver::ColumnType::String, decltype.to_string(), None),
    }
}

pub fn stmt_sql(stmt: *mut sqlite3_stmt) -> String {
    unsafe {
        CStr::from_ptr(sqlite3_expanded_sql(stmt))
            .to_string_lossy()
            .to_owned()
            .to_string()
    }
}

pub fn stmt_original_sql(stmt: *mut sqlite3_stmt) -> String {
    unsafe {
        CStr::from_ptr(sqlite3_sql(stmt))
            .to_string_lossy()
            .to_owned()
            .to_string()
    }
}

pub struct Sqlite3Driver {}

impl driver::Driver for Sqlite3Driver {
    fn open(&mut self, name: &str) -> Result<Box<dyn driver::Connection>> {
        let conn = Connection::new(name)?;

        Ok(Box::new(conn))
    }
}

/// sqlite connection object
pub struct Connection {
    db: *mut sqlite3,
    _id: String,
}

unsafe impl Send for Connection {}

impl Connection {
    fn new(name: &str) -> Result<Self> {
        unsafe {
            assert!(
                sqlite3_threadsafe() != 0,
                "Sqlite3 must be compiled in thread safe mode."
            );
        }

        let mut db = std::ptr::null_mut();

        let flags =
            SQLITE_OPEN_URI | SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE | SQLITE_OPEN_NOMUTEX;

        log::trace!("open sqlite3 database: {} {:X}", name, flags);

        let c_name = CString::new(name)?;

        unsafe {
            let r = sqlite3_open_v2(c_name.as_ptr(), &mut db, flags, std::ptr::null());

            if r != SQLITE_OK {
                let e = if db.is_null() {
                    error::native_error(r, format!("open sqlite {} failure", name))
                } else {
                    let e = error::db_native_error(db, r);

                    let r = sqlite3_close(db); // ignore result .

                    // debug output
                    if r != SQLITE_OK {
                        log::error!("close sqlite3 conn failed: code({})", r);
                    }

                    e
                };

                return Err(e);
            } else {
                log::trace!("create connection {:?}", db);

                let c_str = CString::new("PRAGMA foreign_keys = ON;").unwrap();

                let rc = sqlite3_exec(
                    db,
                    c_str.as_ptr(),
                    None,
                    null_mut::<c_void>(),
                    null_mut::<*mut i8>(),
                );

                if rc != SQLITE_OK {
                    return Err(error::db_native_error(db, rc));
                }

                return Ok(Self {
                    db,
                    _id: format!("{:?}", db),
                });
            }
        }
    }

    fn _begin(&mut self) -> anyhow::Result<Box<dyn driver::Transaction>> {
        let rc = unsafe {
            let c_str = CString::new("BEGIN").unwrap();

            sqlite3_exec(
                self.db,
                c_str.as_ptr(),
                None,
                null_mut::<c_void>(),
                null_mut::<*mut i8>(),
            )
        };

        if rc != SQLITE_OK {
            return Err(error::db_native_error(self.db, rc));
        }

        Ok(Box::new(Transaction {
            conn: Connection {
                db: self.db,
                _id: self._id.clone(),
            },
            finished: false,
            id: uuid::Uuid::new_v4().to_string(), // Use the randomly generated uuid as tx id
        }))
    }

    fn _prepare(&mut self, query: String) -> Result<Box<dyn driver::Statement>> {
        let sqlite3_query = CString::new(query.clone())?;

        let mut stmt = null_mut();

        let rc = unsafe {
            sqlite3_prepare_v2(
                self.db,
                sqlite3_query.as_ptr(),
                sqlite3_query.as_bytes().len() as i32,
                &mut stmt,
                null_mut::<*const c_char>(),
            )
        };

        if rc != SQLITE_OK {
            return Err(error::error_with_sql(self.db, rc, &query));
        }

        // If the input text contains no SQL (if the input is an empty string or a comment) then *ppStmt is set to NULL.
        if stmt.is_null() {
            return Err(anyhow::anyhow!("invalid input sql {}", query));
        }

        Ok(Box::new(Statement {
            db: self.db,
            stmt,
            id: format!("{:?}", stmt),
        }))
    }
}

impl driver::Connection for Connection {
    fn conn_status(&self) -> driver::ConnStatus {
        driver::ConnStatus::Connected
    }

    fn id(&self) -> &str {
        &self._id
    }

    fn begin(&mut self, callback: BoxedCallback<Box<dyn driver::Transaction>>) {
        callback.invoke(self._begin())
    }

    fn prepare(&mut self, query: String, callback: BoxedCallback<Box<dyn driver::Statement>>) {
        callback.invoke(self._prepare(query))
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if !self.db.is_null() {
            log::trace!("drop connection {:?}", self.db);

            let r = unsafe { sqlite3_close(self.db) }; // ignore result .

            self.db = std::ptr::null_mut(); // set db ptr to null to preventing twice drop

            // debug output
            if r != SQLITE_OK {
                log::error!("close sqlite3 conn failed: code({})", r);
            }
        }
    }
}

pub struct Statement {
    db: *mut sqlite3,
    stmt: *mut sqlite3_stmt,
    pub id: String,
}

unsafe impl Send for Statement {}

fn get_bind_index(stmt: *mut sqlite3_stmt, pos: driver::ArgName) -> anyhow::Result<i32> {
    let index = match &pos {
        driver::ArgName::Offset(index) => *index as i32,
        driver::ArgName::String(name) => {
            let c_named = CString::new(name.as_str())?;
            unsafe { sqlite3_bind_parameter_index(stmt, c_named.as_ptr()) }
        }
    };

    if index == 0 {
        return Err(anyhow::format_err!(
            "arg name({:?}) not found, {}",
            pos,
            stmt_original_sql(stmt),
        ));
    }

    return Ok(index);
}

impl Statement {
    unsafe fn bind_args(&mut self, args: Vec<rdbc::driver::Argument>) -> anyhow::Result<()> {
        // sqlite3_clear_bindings(self.stmt);
        sqlite3_reset(self.stmt);

        log::trace!("execute sql {} with args {:?}", stmt_sql(self.stmt), args);

        for arg in args {
            let index = get_bind_index(self.stmt, arg.name)?;

            let rc = match arg.value {
                driver::ArgValue::Bytes(bytes) => {
                    let ptr = bytes.as_ptr();
                    let len = bytes.len();
                    sqlite3_bind_blob(
                        self.stmt,
                        index,
                        ptr as *const c_void,
                        len as i32,
                        Some(std::mem::transmute(SQLITE_TRANSIENT as usize)),
                    )
                }
                driver::ArgValue::F64(f64) => sqlite3_bind_double(self.stmt, index, f64),

                driver::ArgValue::I64(i64) => sqlite3_bind_int64(self.stmt, index, i64),

                driver::ArgValue::String(str) => {
                    let str = CString::new(str)?;

                    let ptr = str.as_ptr();
                    let len = str.as_bytes().len() as i32;

                    sqlite3_bind_text(
                        self.stmt,
                        index,
                        ptr,
                        len,
                        Some(std::mem::transmute(SQLITE_TRANSIENT as usize)),
                    )
                }

                driver::ArgValue::Null => SQLITE_OK,
            };

            if rc != SQLITE_OK {
                return Err(error::db_native_error(self.db, rc));
            }
        }

        Ok(())
    }

    fn _query(&mut self, args: Vec<rdbc::Argument>) -> Result<Box<dyn driver::Rows>> {
        unsafe { self.bind_args(args) }?;

        return Ok(Box::new(Rows {
            db: self.db,
            stmt: self.stmt,
            columns: None,
            has_next: false,
            id: uuid::Uuid::new_v4().to_string(),
        }));
    }
}

impl driver::Statement for Statement {
    fn execute(&mut self, args: Vec<rdbc::Argument>, callback: BoxedCallback<driver::ExecResult>) {
        let exec = || {
            unsafe { self.bind_args(args) }?;

            let rc = unsafe { sqlite3_step(self.stmt) };

            // unsafe { sqlite3_reset(self.stmt) };

            match rc {
                SQLITE_DONE => {
                    let last_insert_id = unsafe { sqlite3_last_insert_rowid(self.db) } as u64;
                    let raws_affected = unsafe { sqlite3_changes(self.db) } as u64;

                    return Ok(driver::ExecResult {
                        last_insert_id,
                        raws_affected,
                    });
                }
                SQLITE_ROW => {
                    return Err(anyhow::Error::new(driver::RDBCError::UnexpectRows));
                }
                _ => {
                    return Err(error::db_native_error(self.db, rc));
                }
            };
        };

        callback.invoke(exec())
    }

    fn num_input(&self, callback: BoxedCallback<Option<usize>>) {
        callback.invoke(Ok(Some(unsafe {
            sqlite3_bind_parameter_count(self.stmt) as usize
        })))
    }

    fn query(&mut self, args: Vec<rdbc::Argument>, callback: BoxedCallback<Box<dyn driver::Rows>>) {
        callback.invoke(self._query(args))
    }
}

impl Drop for Statement {
    fn drop(&mut self) {
        if !self.stmt.is_null() {
            log::trace!("drop stmt: {}", stmt_sql(self.stmt));
            unsafe { sqlite3_finalize(self.stmt) };
            self.stmt = null_mut();
        }
    }
}

pub struct Transaction {
    conn: Connection,
    finished: bool,
    pub id: String,
}

impl Transaction {
    fn _rollback(&self) -> anyhow::Result<()> {
        let rc = unsafe {
            let c_str = CString::new("ROLLBACK").unwrap();

            sqlite3_exec(
                self.conn.db,
                c_str.as_ptr(),
                None,
                null_mut::<c_void>(),
                null_mut::<*mut i8>(),
            )
        };

        if rc != SQLITE_OK {
            return Err(error::error_with_sql(self.conn.db, rc, "ROLLBACK"));
        }

        Ok(())
    }
}

impl driver::Transaction for Transaction {
    fn commit(&mut self, callback: BoxedCallback<()>) {
        let mut invoke = || {
            let rc = unsafe {
                let c_str = CString::new("COMMIT").unwrap();

                sqlite3_exec(
                    self.conn.db,
                    c_str.as_ptr(),
                    None,
                    null_mut::<c_void>(),
                    null_mut::<*mut i8>(),
                )
            };

            self.finished = true;

            if rc != SQLITE_OK {
                Err(error::error_with_sql(self.conn.db, rc, "COMMIT"))
            } else {
                Ok(())
            }
        };

        callback.invoke(invoke());
    }

    fn prepare(&mut self, query: String, callback: BoxedCallback<Box<dyn driver::Statement>>) {
        use driver::Connection;

        self.conn.prepare(query, callback)
    }

    fn rollback(&mut self, callback: BoxedCallback<()>) {
        let mut invoke = || {
            self.finished = true;

            self._rollback()
        };

        callback.invoke(invoke())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        // default to rollback all stmt .
        if !self.finished {
            _ = self._rollback();
            self.finished = true;
        }

        self.conn.db = null_mut();
    }
}

pub struct Rows {
    db: *mut sqlite3,
    stmt: *mut sqlite3_stmt,
    columns: Option<Vec<driver::Column>>,
    has_next: bool,
    pub id: String,
}

impl Rows {
    fn _columns(&mut self) -> Result<&Vec<driver::Column>> {
        if self.columns.is_none() {
            let mut columns = vec![];

            unsafe {
                let count = sqlite3_column_count(self.stmt);

                for i in 0..count {
                    let name = sqlite3_column_name(self.stmt, i);

                    let (_, decltype, len) = colunm_decltype(self.stmt, i);

                    columns.push(driver::Column {
                        column_index: i as u64,
                        column_name: CStr::from_ptr(name).to_string_lossy().to_string(),
                        column_decltype: decltype,
                        column_decltype_len: len,
                    })
                }
            };

            self.columns = Some(columns);
        }

        Ok(self.columns.as_ref().unwrap())
    }

    fn _get(
        &mut self,
        name: driver::ArgName,
        column_type: driver::ColumnType,
    ) -> Result<Option<driver::ArgValue>> {
        log::trace!(
            "{} :get column({:?},{:?})",
            stmt_sql(self.stmt),
            name,
            column_type
        );

        let index = match name {
            driver::ArgName::Offset(index) => index as i32,
            driver::ArgName::String(name) => {
                let columns = self._columns()?;

                let col = columns
                    .iter()
                    .find(|column| column.column_name.to_uppercase() == name.to_uppercase())
                    .map(|c| c.column_index as i32);

                if let Some(index) = col {
                    index
                } else {
                    return Ok(None);
                }
            }
        };

        let max_index = unsafe { sqlite3_column_count(self.stmt) };

        if index >= max_index {
            return Err(anyhow::Error::new(RDBCError::OutOfRange(index as u64)));
        }

        if !self.has_next {
            return Err(anyhow::Error::new(RDBCError::NextDataError));
        }

        let value = unsafe {
            match column_type {
                driver::ColumnType::Bytes => {
                    let len = sqlite3_column_bytes(self.stmt, index);
                    let data = sqlite3_column_blob(self.stmt, index) as *const u8;
                    let data = from_raw_parts(data, len as usize).to_owned();

                    driver::ArgValue::Bytes(data)
                }
                driver::ColumnType::I64 => {
                    driver::ArgValue::I64(sqlite3_column_int64(self.stmt, index))
                }
                driver::ColumnType::F64 => {
                    driver::ArgValue::F64(sqlite3_column_double(self.stmt, index))
                }
                driver::ColumnType::String => {
                    let data = sqlite3_column_text(self.stmt, index) as *const i8;

                    if data != null() {
                        driver::ArgValue::String(CStr::from_ptr(data).to_string_lossy().to_string())
                    } else {
                        driver::ArgValue::String("".to_owned())
                    }
                }
                driver::ColumnType::Null => driver::ArgValue::Null,
            }
        };

        Ok(Some(value))
    }

    fn _next(&mut self) -> Result<bool> {
        match unsafe { sqlite3_step(self.stmt) } {
            SQLITE_DONE => {
                self.has_next = false;
                Ok(false)
            }

            SQLITE_ROW => {
                self.has_next = true;
                Ok(true)
            }

            rc => {
                self.has_next = false;
                Err(error::db_native_error(self.db, rc))
            }
        }
    }
}

unsafe impl Send for Rows {}

impl driver::Rows for Rows {
    fn colunms(&mut self, callback: BoxedCallback<Vec<driver::Column>>) {
        callback.invoke(self._columns().map(|c| c.clone()))
    }

    fn next(&mut self, callback: BoxedCallback<bool>) {
        callback.invoke(self._next())
    }

    fn get(
        &mut self,
        name: driver::ArgName,
        column_type: driver::ColumnType,
        callback: BoxedCallback<Option<driver::ArgValue>>,
    ) {
        callback.invoke(self._get(name, column_type))
    }
}

impl Drop for Rows {
    fn drop(&mut self) {
        log::trace!("reset stmt {}", stmt_sql(self.stmt));
        unsafe { sqlite3_reset(self.stmt) };
    }
}
