//! End-to-end: LogQL text → AST → `LogicalPlan` → executed rows.
//!
//! `sort_by(timestamp)` throughout, because `timestamp` is the monotonic line
//! index (§6) and the only column that preserves file order across partitions.

mod common;

use common::two_log_files;
use datafusion::arrow::array::AsArray;
use datafusion::{assert_batches_eq, error::Result, logical_expr::col, prelude::SessionContext};
use ducktails::{parser::Parser, plan::plan};

/// Runs `query` over `pattern` and returns the `line` column in file order.
async fn lines(pattern: &str, query: &str) -> Result<Vec<datafusion::arrow::array::RecordBatch>> {
    let ast = Parser::parse(query).expect("test query must parse");
    let logical_plan = plan(pattern, &ast)?;

    SessionContext::new()
        .execute_logical_plan(logical_plan)
        .await?
        .sort_by(vec![col("timestamp")])?
        .select_columns(&["line"])?
        .collect()
        .await
}

#[tokio::test]
async fn empty_selector_returns_every_line_from_every_file() -> Result<()> {
    let (_dir, pattern) = two_log_files();

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
        &lines(&pattern, "{}").await?
    );
    Ok(())
}

#[tokio::test]
async fn filter_filename_and_lines() -> Result<()> {
    let (_dir, pattern) = two_log_files();

    assert_batches_eq!(
        &[
            "+-------+",
            "| line  |",
            "+-------+",
            "| one   |",
            "| three |",
            "+-------+",
        ],
        &lines(&pattern, r#"{filename=~".*/a\\.log"} |~ "one|three""#).await?
    );
    Ok(())
}

/// Loki fully anchors label matcher regexes: "the regex expression must match
/// against the *entire* string". So a bare `a.log` must NOT match `/tmp/…/a.log`.
#[tokio::test]
async fn label_matcher_regex_is_fully_anchored() -> Result<()> {
    let (_dir, pattern) = two_log_files();

    let matched = lines(&pattern, r#"{filename=~"a\\.log"}"#).await?;
    assert_eq!(
        matched.iter().map(|b| b.num_rows()).sum::<usize>(),
        0,
        "an unanchored pattern must not match a full path"
    );
    Ok(())
}

/// All four line filters, in one pipeline, so their order and their mapping onto
/// `contains` / `RegexMatch` are both pinned.
#[tokio::test]
async fn every_line_filter_operator() -> Result<()> {
    let (_dir, pattern) = two_log_files();

    let cases = [
        (r#"{} |= "o""#, vec!["one", "two", "four"]),
        (r#"{} != "o""#, vec!["three", "five"]),
        (r#"{} |~ "^f""#, vec!["four", "five"]),
        (r#"{} !~ "^f""#, vec!["one", "two", "three"]),
        // Stages compose in source order: `|= "o"` keeps one/two/four, then
        // `!= "t"` drops two.
        (r#"{} |= "o" != "t""#, vec!["one", "four"]),
    ];

    for (query, want) in cases {
        let batches = lines(&pattern, query).await?;
        let got: Vec<String> = batches
            .iter()
            .flat_map(|b| {
                let column = b.column(0).as_string::<i32>();
                (0..b.num_rows())
                    .map(|i| column.value(i).to_string())
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(got, want, "query: {query}");
    }
    Ok(())
}

/// A label we cannot resolve yet must be reported, not silently return nothing.
#[tokio::test]
async fn unavailable_label_is_an_error() {
    let (_dir, pattern) = two_log_files();

    let ast = Parser::parse(r#"{job="app"}"#).expect("should parse");
    let err = plan(&pattern, &ast).expect_err("job is not available yet");

    assert!(err.to_string().contains("job"), "got {err}");
}

/// A source that matches no files is a different failure from a query that
/// matches no lines — the former exits non-zero, the latter returns empty.
#[tokio::test]
async fn a_glob_matching_no_files_is_an_error() {
    let (dir, _pattern) = two_log_files();
    let pattern = dir.path().join("*.txt").to_string_lossy().into_owned();

    let err = lines(&pattern, "{}")
        .await
        .expect_err("no files match the glob");

    assert!(err.to_string().contains("no files match"), "got {err}");
}
