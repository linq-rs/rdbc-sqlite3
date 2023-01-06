pub mod error;
pub mod sqlite3;

pub use sqlite3::Sqlite3Driver;

pub fn register_sqlite3() -> anyhow::Result<()> {
    return rdbc::register("sqlite3", Sqlite3Driver {});
}
