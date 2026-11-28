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

/// Logfmt lines whose values are longer than a view can inline, so the labels
/// they produce take the by-reference path instead of the inline one.
///
/// The second line's value has escapes to resolve, so its bytes do not exist
/// anywhere in the line and cannot be referenced at all.
pub fn long_logfmt_log_file() -> (TempDir, String) {
    let dir = tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("app.log"),
        concat!(
            "level=info msg=\"a value well past twelve bytes\"\n",
            "level=warn msg=\"he said \\\"twelve bytes or more\\\" plainly\"\n",
        ),
    )
    .expect("write app.log");
    let pattern = dir.path().join("*.log").to_string_lossy().into_owned();
    (dir, pattern)
}

/// Logfmt lines covering every label value type a filter can compare, plus the
/// two cases Loki treats specially: a label that is *missing* from a line, and
/// one that is present but *unparseable* as the type being compared.
pub fn typed_logfmt_log_file() -> (TempDir, String) {
    let dir = tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("app.log"),
        concat!(
            "level=error code=500 latency=2h size=11GB\n",
            "level=info code=200 latency=30m size=1MB\n",
            // `code` cannot be read as a number, and the other labels are absent.
            "level=warn code=abc\n",
            // No labels at all.
            "not logfmt at all\n",
        ),
    )
    .expect("write app.log");
    let pattern = dir.path().join("*.log").to_string_lossy().into_owned();
    (dir, pattern)
}

/// Lines carrying RFC3339 timestamps, deliberately out of order in the file so
/// that sorting by `timestamp` proves the timestamp was read from the line
/// rather than assigned by position.
pub fn timestamped_log_file() -> (TempDir, String) {
    let dir = tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("app.log"),
        concat!(
            "2026-11-27T10:00:02Z level=info msg=third\n",
            "2026-11-27T10:00:00Z level=error msg=first\n",
            // An offset rather than Z, to pin that it is honoured: 11:00:01+01:00
            // is 10:00:01Z, so this sorts second and not last.
            "2026-11-27T11:00:01+01:00 level=warn msg=second\n",
            // No timestamp at all: stamped with ingest time, so it sorts last.
            "no timestamp here\n",
        ),
    )
    .expect("write app.log");
    let pattern = dir.path().join("*.log").to_string_lossy().into_owned();
    (dir, pattern)
}

/// Two files of timestamped logfmt lines, for metric queries.
///
/// Evaluated at `2026-11-27T10:04:00Z`, the lines fall either side of the
/// interesting boundaries: `a.log` has one line exactly five minutes old, one
/// exactly four, one at the instant itself, and one an hour before.
pub fn metric_log_files() -> (TempDir, String) {
    let dir = tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("a.log"),
        concat!(
            "2026-11-27T10:00:00Z level=info msg=a\n",
            "2026-11-27T10:02:00Z level=error msg=b\n",
            "2026-11-27T10:04:00Z level=error msg=c\n",
            "2026-11-27T09:00:00Z level=info msg=old\n",
        ),
    )
    .expect("write a.log");
    std::fs::write(
        dir.path().join("b.log"),
        "2026-11-27T10:03:00Z level=error msg=d\n",
    )
    .expect("write b.log");
    let pattern = dir.path().join("*.log").to_string_lossy().into_owned();
    (dir, pattern)
}
