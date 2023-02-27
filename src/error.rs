use std::os::raw::{c_char, c_int};

use rdbc_rs::driver::RDBCError;
use sqlite3_sys::*;

pub fn native_error(code: i32, message: String) -> anyhow::Error {
    anyhow::format_err!(RDBCError::NativeError(code, message))
}

pub fn db_native_error(db: *mut sqlite3, code: c_int) -> anyhow::Error {
    let errmsg = unsafe { errmsg_to_string(sqlite3_errmsg(db)) };

    native_error(code, errmsg)
}

pub fn error_with_sql(db: *mut sqlite3, code: c_int, sql: &str) -> anyhow::Error {
    let errmsg = unsafe { errmsg_to_string(sqlite3_errmsg(db)) };

    native_error(code, format!("{}, with SQL {}", errmsg, sql))
}

unsafe fn errmsg_to_string(errmsg: *const c_char) -> String {
    std::ffi::CStr::from_ptr(errmsg)
        .to_string_lossy()
        .into_owned()
}
