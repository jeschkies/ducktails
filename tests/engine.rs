//! End-to-end: LogQL text → AST → `LogicalPlan` → executed rows.
//!
//! `sort_by(timestamp)` throughout, because `timestamp` is the monotonic line
//! index (§6) and the only column that preserves file order across partitions.

mod common;

use common::{logfmt_log_file, long_logfmt_log_file, two_log_files, typed_logfmt_log_file};
use datafusion::arrow::array::{Array, AsArray};
use datafusion::{assert_batches_eq, error::Result, logical_expr::col, prelude::SessionContext};
use ducktails::{parser::Parser, plan::plan};

/// Runs `query` over `pattern` and returns `columns` in file order.
async fn run(
    pattern: &str,
    query: &str,
    columns: &[&str],
) -> Result<Vec<datafusion::arrow::array::RecordBatch>> {
    let ast = Parser::parse(query).expect("test query must parse");
    let logical_plan = plan(pattern, &ast)?;

    SessionContext::new()
        .execute_logical_plan(logical_plan)
        .await?
        .sort_by(vec![col("timestamp")])?
        .select_columns(columns)?
        .collect()
        .await
}

/// Runs `query` over `pattern` and returns the `line` column in file order.
async fn lines(pattern: &str, query: &str) -> Result<Vec<datafusion::arrow::array::RecordBatch>> {
    run(pattern, query, &["line"]).await
}

/// The first column of `batches` as plain strings, for expectations that read
/// better as a list than as a table.
fn to_strings(batches: &[datafusion::arrow::array::RecordBatch]) -> Vec<String> {
    batches
        .iter()
        .flat_map(|b| {
            let column = b.column(0).as_string_view();
            (0..b.num_rows())
                .map(|i| column.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
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
        let got = to_strings(&lines(&pattern, query).await?);
        assert_eq!(got, want, "query: {query}");
    }
    Ok(())
}

/// `| logfmt` is the first stage that *writes* to `labels` rather than filtering
/// rows, so this pins the `List<Struct<key,value>>` representation from §1 all
/// the way through the UDF, `array_concat` and the `Projection`.
#[tokio::test]
async fn logfmt_stage_extracts_labels() -> Result<()> {
    let (_dir, pattern) = logfmt_log_file();

    assert_batches_eq!(
        &[
            "+---------------------------------------+--------------------------------------------------------------------------------------+",
            "| line                                  | labels                                                                               |",
            "+---------------------------------------+--------------------------------------------------------------------------------------+",
            "| level=info msg=started                | [{key: level, value: info}, {key: msg, value: started}]                              |",
            "| level=error msg=\"went wrong\" code=500 | [{key: level, value: error}, {key: msg, value: went wrong}, {key: code, value: 500}] |",
            "| not logfmt at all                     | []                                                                                   |",
            "+---------------------------------------+--------------------------------------------------------------------------------------+",
        ],
        &run(&pattern, r#"{} | logfmt"#, &["line", "labels"]).await?
    );
    Ok(())
}

/// Without a parser stage `labels` stays empty — a plain text file carries none.
#[tokio::test]
async fn labels_are_empty_without_a_parser_stage() -> Result<()> {
    let (_dir, pattern) = logfmt_log_file();

    let batches = run(&pattern, "{}", &["labels"]).await?;
    let labels = batches[0].column(0).as_list::<i32>();
    assert!(
        (0..labels.len()).all(|i| labels.value(i).is_empty()),
        "expected no labels before a parser stage, got {:?}",
        labels
    );
    Ok(())
}

/// A line filter after `| logfmt` still tests the raw line, not the labels.
#[tokio::test]
async fn a_line_filter_after_logfmt_still_reads_the_line() -> Result<()> {
    let (_dir, pattern) = logfmt_log_file();

    assert_batches_eq!(
        &[
            "+---------------------------------------+",
            "| line                                  |",
            "+---------------------------------------+",
            "| level=error msg=\"went wrong\" code=500 |",
            "+---------------------------------------+",
        ],
        &lines(&pattern, r#"{} | logfmt |= "code=500""#).await?
    );
    Ok(())
}

/// A value too long to inline must be a *view into the line*, not a copy of it.
///
/// Nothing else in this suite reaches that path: a value of 12 bytes or fewer
/// lives inside its own 16-byte view, and every fixture above stays under that.
/// Keys are always short enough to inline, so the `key` column registers the
/// line block and then never points into it.
#[tokio::test]
async fn a_long_label_value_views_the_line_instead_of_copying_it() -> Result<()> {
    let (dir, pattern) = long_logfmt_log_file();
    let size = std::fs::metadata(dir.path().join("app.log"))
        .expect("fixture exists")
        .len();

    let batches = run(&pattern, "{} | logfmt", &["labels"]).await?;
    let entries = batches[0].column(0).as_list::<i32>().values().as_struct();
    let values = entries.column(1).as_string_view();

    assert!(
        (0..values.len()).any(|i| values.value(i) == "a value well past twelve bytes"),
        "the long value must survive the round trip"
    );
    assert!(
        values.data_buffers().iter().any(|b| b.len() as u64 == size),
        "expected a block that is the file itself, got blocks of {:?} for a {size} byte file",
        values
            .data_buffers()
            .iter()
            .map(|b| b.len())
            .collect::<Vec<_>>()
    );
    Ok(())
}

/// An escaped value is the one case that must copy: its unescaped bytes appear
/// nowhere in the line, so there is nothing to point at.
#[tokio::test]
async fn an_escaped_label_value_is_unescaped_and_kept() -> Result<()> {
    let (_dir, pattern) = long_logfmt_log_file();

    let batches = run(&pattern, "{} | logfmt", &["labels"]).await?;
    let entries = batches[0].column(0).as_list::<i32>().values().as_struct();
    let values = entries.column(1).as_string_view();

    let got: Vec<&str> = (0..values.len()).map(|i| values.value(i)).collect();
    assert!(
        got.contains(&r#"he said "twelve bytes or more" plainly"#),
        "got {got:?}"
    );
    Ok(())
}

/// `line` is `Utf8View`, and every stage must read it as one.
///
/// A UDF or predicate declaring `Utf8` would still *plan*: DataFusion inserts a
/// cast, which copies every line and silently undoes the view. Nothing else in
/// this suite would notice, so the plan itself is the assertion.
#[tokio::test]
async fn the_pipeline_reads_the_line_view_without_casting_it() -> Result<()> {
    let (_dir, pattern) = logfmt_log_file();

    let ast = Parser::parse(r#"{} | logfmt |= "code=500""#).expect("test query must parse");
    let optimized = SessionContext::new()
        .execute_logical_plan(plan(&pattern, &ast)?)
        .await?
        .logical_plan()
        .display_indent()
        .to_string();

    assert!(
        !optimized.contains("CAST"),
        "a cast copies the line bytes:\n{optimized}"
    );
    Ok(())
}

/// Label filters, and in particular the two cases where Loki is deliberately
/// asymmetric: a *missing* label reads as `""` for a string filter but drops the
/// line for a typed one, while an *unparseable* one keeps the line.
#[tokio::test]
async fn label_filters_follow_loki_on_missing_and_unparseable_values() -> Result<()> {
    let (_dir, pattern) = typed_logfmt_log_file();

    const ERROR: &str = "level=error code=500 latency=2h size=11GB";
    const INFO: &str = "level=info code=200 latency=30m size=1MB";
    const WARN: &str = "level=warn code=abc";
    const PLAIN: &str = "not logfmt at all";

    let cases = [
        (r#"{} | logfmt | level="error""#, vec![ERROR]),
        // `!=` keeps the line with no `level` at all: absent reads as `""`.
        (r#"{} | logfmt | level!="error""#, vec![INFO, WARN, PLAIN]),
        // Fully anchored, so `warn` does not match `warning` and `arn` matches
        // nothing.
        (r#"{} | logfmt | level=~"warn|info""#, vec![INFO, WARN]),
        (r#"{} | logfmt | level=~"arn""#, vec![]),
        // `code=abc` is present but not a number: Loki keeps the line and flags
        // it. `PLAIN` has no `code`, so it is dropped.
        (r#"{} | logfmt | code>=500"#, vec![ERROR, WARN]),
        (r#"{} | logfmt | code<300"#, vec![INFO, WARN]),
        // Durations and byte sizes compare in their own units, not as strings:
        // `30m` < `1h30m` < `2h`, and `1MB` < `10GB` < `11GB`.
        (r#"{} | logfmt | latency>1h30m"#, vec![ERROR]),
        (r#"{} | logfmt | latency<=1h30m"#, vec![INFO]),
        (r#"{} | logfmt | size>=10GB"#, vec![ERROR]),
        // Stages compose, so a second filter narrows the first.
        (r#"{} | logfmt | code>=200 | level="info""#, vec![INFO]),
    ];

    for (query, want) in cases {
        let got = to_strings(&lines(&pattern, query).await?);
        assert_eq!(got, want, "query: {query}");
    }
    Ok(())
}

/// The grammar is looser than the semantics: it parses `| level>="error"`, and
/// the planner is where that has to be refused.
#[tokio::test]
async fn a_label_filter_cannot_order_a_string() {
    let (_dir, pattern) = typed_logfmt_log_file();

    let ast = Parser::parse(r#"{} | logfmt | level>="error""#).expect("should parse");
    let err = plan(&pattern, &ast).expect_err("ordering a string is not meaningful");

    assert!(err.to_string().contains("level"), "got {err}");
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
