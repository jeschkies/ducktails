//! Integration tests for `LogTable`, the DataFusion `TableProvider` backing
//! text log files. SQL is the test harness because to be independent of logical planning; the CLI won't support SQL.

use std::sync::Arc;

use datafusion::{assert_batches_eq, error::Result, prelude::SessionContext};
use ducktails::table::LogTable;
use tempfile::{TempDir, tempdir};

/// Two log files with 3 + 2 lines. Returned `TempDir` must be kept alive for
/// the duration of the test — it deletes itself when dropped.
fn two_log_files() -> (TempDir, String) {
    let dir = tempdir().expect("tempdir");
    std::fs::write(dir.path().join("a.log"), "one\ntwo\nthree\n").expect("write a.log");
    std::fs::write(dir.path().join("b.log"), "four\nfive\n").expect("write b.log");
    let pattern = dir.path().join("*.log").to_string_lossy().into_owned();
    (dir, pattern)
}

fn ctx_with(pattern: String) -> Result<SessionContext> {
    let ctx = SessionContext::new();
    ctx.register_table("logs", Arc::new(LogTable::new(pattern)))?;
    Ok(ctx)
}

#[tokio::test]
async fn counts_lines_across_a_glob() -> Result<()> {
    let (_dir, pattern) = two_log_files();
    let ctx = ctx_with(pattern)?;

    let batches = ctx
        .sql("SELECT count(*) as n FROM logs")
        .await?
        .collect()
        .await?;

    assert_batches_eq!(&["+---+", "| n |", "+---+", "| 5 |", "+---+",], &batches);
    Ok(())
}

#[tokio::test]
async fn projects_line_content_deterministically() -> Result<()> {
    let (_dir, pattern) = two_log_files();
    let ctx = ctx_with(pattern)?;

    let batches = ctx.sql("SELECT line FROM logs").await?.collect().await?;

    assert_batches_eq!(
        &[
            "+-------+",
            "| line  |",
            "+-------+",
            "| one   |",
            "| two   |",
            "| three |",
            "| four  |",
            "| five  |",
            "+-------+",
        ],
        &batches
    );

    Ok(())
}
