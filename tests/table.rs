//! Integration tests for `LogTable`, the DataFusion `TableProvider` backing
//! text log files. SQL is the harness here so these stay independent of logical
//! planning; the CLI itself will never support SQL.

mod common;

use common::{ctx_with, two_log_files};
use datafusion::{assert_batches_eq, error::Result};

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
