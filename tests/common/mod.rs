//! Fixtures shared by the integration tests.
//!
//! Each file in `tests/` is its own crate, so shared helpers have to live in a
//! subdirectory module like this one — `tests/common.rs` would be compiled as a
//! test binary of its own and report zero tests.
//!
//! This module is compiled separately into *every* test binary that declares it,
//! so a helper only one of them uses looks dead in the others. Hence the allow.

#![allow(dead_code)]

use std::sync::Arc;

use datafusion::{error::Result, prelude::SessionContext};
use ducktails::table::LogTable;
use tempfile::{TempDir, tempdir};

/// Two log files with 3 + 2 lines, and a glob matching both.
///
/// The returned `TempDir` must be kept alive for the duration of the test — it
/// deletes the directory when dropped.
pub fn two_log_files() -> (TempDir, String) {
    let dir = tempdir().expect("tempdir");
    std::fs::write(dir.path().join("a.log"), "one\ntwo\nthree\n").expect("write a.log");
    std::fs::write(dir.path().join("b.log"), "four\nfive\n").expect("write b.log");
    let pattern = dir.path().join("*.log").to_string_lossy().into_owned();
    (dir, pattern)
}

/// One log file of logfmt lines, and a glob matching it.
///
/// Deliberately uneven: the keys differ per line, one value is quoted and
/// contains a space, and the last line is not logfmt at all — so a parser stage
/// has to cope with a line it cannot read.
pub fn logfmt_log_file() -> (TempDir, String) {
    let dir = tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("app.log"),
        concat!(
            "level=info msg=started\n",
            "level=error msg=\"went wrong\" code=500\n",
            "not logfmt at all\n",
        ),
    )
    .expect("write app.log");
    let pattern = dir.path().join("*.log").to_string_lossy().into_owned();
    (dir, pattern)
}

/// A context with the glob registered as `logs`, for driving `LogTable` from SQL.
///
/// Only the SQL tests need this: a plan built by `ducktails::plan` carries its
/// own `TableSource`, so it needs no registration.
pub fn ctx_with(pattern: String) -> Result<SessionContext> {
    let ctx = SessionContext::new();
    ctx.register_table("logs", Arc::new(LogTable::new(pattern)))?;
    Ok(ctx)
}
