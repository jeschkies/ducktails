//! Integration tests for `LogTable`, the DataFusion `TableProvider` backing
//! text log files. SQL is the harness here so these stay independent of logical
//! planning; the CLI itself will never support SQL.

mod common;

use common::{ctx_with, logfmt_log_file, two_log_files};
use datafusion::arrow::array::AsArray;
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

/// The point of `Utf8View` for `line`: the file's bytes are one block that every
/// line views into, instead of a copy per line.
///
/// Uses the logfmt fixture because its lines are longer than 12 bytes. A shorter
/// line is stored *inline* in its own 16-byte view and never reaches the block,
/// so a fixture of short lines would pass this test for the wrong reason.
#[tokio::test]
async fn lines_view_a_single_copy_of_the_file() -> Result<()> {
    let (dir, pattern) = logfmt_log_file();
    let size = std::fs::metadata(dir.path().join("app.log"))
        .expect("fixture exists")
        .len();
    let ctx = ctx_with(pattern)?;

    let batches = ctx.sql("SELECT line FROM logs").await?.collect().await?;
    let lines = batches[0].column(0).as_string_view();

    let buffers = lines.data_buffers();
    assert_eq!(buffers.len(), 1, "expected one block per file");
    assert_eq!(
        buffers[0].len() as u64,
        size,
        "the block should be the file itself, verbatim"
    );
    Ok(())
}
