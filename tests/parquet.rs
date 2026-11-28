//! A Parquet catalog, reached through a `file://` URL.
//!
//! The fixture is written with Arrow's own writer rather than by the engine, so
//! these tests read a file ducktails did not produce — including a `timestamp`
//! in milliseconds and a `line` as `Utf8`, neither of which is the shape the
//! engine works in.

mod common;

use std::sync::Arc;

use chrono::{DateTime, Utc};
use datafusion::arrow::array::{RecordBatch, StringArray, TimestampMillisecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::error::Result;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::prelude::SessionContext;
use ducktails::catalog::Catalog;
use ducktails::parser::Parser;
use ducktails::plan::plan;
use tempfile::{TempDir, tempdir};

/// 2026-11-27T10:00:00Z, in milliseconds.
const T: i64 = 1_795_773_600_000;

/// A Parquet file of log rows, and a `file://` URL for the directory.
///
/// `service` is neither `timestamp` nor `line`, so the catalog exposes it as a
/// stream label without being told to.
fn parquet_logs() -> (TempDir, String) {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("logs.parquet");

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("line", DataType::Utf8, false),
        Field::new("service", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(TimestampMillisecondArray::from(vec![
                T,
                T + 60_000,
                T + 120_000,
            ])),
            Arc::new(StringArray::from(vec![
                "level=info msg=started",
                "level=error msg=\"went wrong\" code=500",
                "level=info msg=done",
            ])),
            Arc::new(StringArray::from(vec!["api", "api", "worker"])),
        ],
    )
    .expect("batch");

    let file = std::fs::File::create(&path).expect("create");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
    writer.write(&batch).expect("write");
    writer.close().expect("close");

    let url = format!("file://{}", dir.path().to_string_lossy());
    (dir, url)
}

async fn run(url: &str, query: &str, columns: &[&str]) -> Result<Vec<RecordBatch>> {
    let ctx = SessionContext::new();
    let catalog = Catalog::open(url, &ctx.state()).await?;
    let ast = Parser::parse(query).expect("test query must parse");
    let now: DateTime<Utc> = "2026-11-27T10:02:00Z".parse().expect("instant");

    ctx.execute_logical_plan(plan(&catalog, &ast, now)?)
        .await?
        .sort_by(vec![datafusion::prelude::col("timestamp")])?
        .select_columns(columns)?
        .collect()
        .await
}

/// The whole pipeline over Parquet: the line filter reads the mapped `line`
/// column, which was `Utf8` in the file and is `Utf8View` by the time a filter
/// sees it.
#[tokio::test]
async fn a_line_filter_reads_a_parquet_line() -> Result<()> {
    let (_dir, url) = parquet_logs();

    datafusion::assert_batches_eq!(
        &[
            "+---------------------------------------+",
            "| line                                  |",
            "+---------------------------------------+",
            "| level=error msg=\"went wrong\" code=500 |",
            "+---------------------------------------+",
        ],
        &run(&url, r#"{} |= "error""#, &["line"]).await?
    );
    Ok(())
}

/// A column that is not `timestamp` or `line` is a stream label, so the selector
/// matches on it — and this is the case worth having, because it compiles to a
/// column predicate Parquet can push down.
#[tokio::test]
async fn a_selector_matches_a_parquet_column() -> Result<()> {
    let (_dir, url) = parquet_logs();

    datafusion::assert_batches_eq!(
        &[
            "+---------------------+",
            "| line                |",
            "+---------------------+",
            "| level=info msg=done |",
            "+---------------------+",
        ],
        &run(&url, r#"{service="worker"}"#, &["line"]).await?
    );
    Ok(())
}

/// `| logfmt` works over Parquet too: the catalog supplies an empty `labels`
/// column for a parser stage to fill.
#[tokio::test]
async fn logfmt_and_a_label_filter_over_parquet() -> Result<()> {
    let (_dir, url) = parquet_logs();

    datafusion::assert_batches_eq!(
        &[
            "+---------------------------------------+",
            "| line                                  |",
            "+---------------------------------------+",
            "| level=error msg=\"went wrong\" code=500 |",
            "+---------------------------------------+",
        ],
        &run(&url, r#"{} | logfmt | level="error""#, &["line"]).await?
    );
    Ok(())
}

/// A Parquet source without the columns the engine needs says so.
#[tokio::test]
async fn a_parquet_source_needs_a_line_column() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("other.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new("note", DataType::Utf8, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(vec!["nope"]))],
    )
    .expect("batch");
    let file = std::fs::File::create(&path).expect("create");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
    writer.write(&batch).expect("write");
    writer.close().expect("close");

    let url = format!("file://{}", dir.path().to_string_lossy());
    let err = run(&url, "{}", &["line"])
        .await
        .expect_err("no line column");

    assert!(err.to_string().contains("timestamp"), "got {err}");
}
