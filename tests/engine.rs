//! End-to-end: LogQL text → AST → `LogicalPlan` → executed rows.
//!
//! `sort_by(timestamp)` throughout, because `timestamp` is the monotonic line
//! index (§6) and the only column that preserves file order across partitions.

mod common;

use chrono::{DateTime, Utc};
use common::{
    logfmt_log_file, long_logfmt_log_file, metric_log_files, two_log_files, typed_logfmt_log_file,
};
use datafusion::arrow::array::{Array, AsArray};
use datafusion::{assert_batches_eq, error::Result, logical_expr::col, prelude::SessionContext};
use ducktails::{
    catalog::Catalog,
    parser::Parser,
    plan::plan,
    query::{Direction, Query},
};

/// Runs `query` over `pattern` and returns `columns` in file order.
async fn run(
    pattern: &str,
    query: &str,
    columns: &[&str],
) -> Result<Vec<datafusion::arrow::array::RecordBatch>> {
    let ast = Parser::parse(query).expect("test query must parse");
    let logical_plan = plan(&Catalog::files(pattern), &Query::new(ast))?;

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
        .execute_logical_plan(plan(&Catalog::files(&pattern), &Query::new(ast))?)
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
    let err = plan(&Catalog::files(&pattern), &Query::new(ast))
        .expect_err("ordering a string is not meaningful");

    assert!(err.to_string().contains("level"), "got {err}");
}

/// A label filter resolves a column first, so `filename` — which the catalog
/// materialises — works in the pipeline and not only in the selector.
#[tokio::test]
async fn a_label_filter_reaches_a_catalog_label() -> Result<()> {
    let (_dir, pattern) = two_log_files();

    // `a.log` holds one/two/three; the filter keeps exactly those.
    let got = to_strings(&lines(&pattern, r#"{} | filename=~".*/a\.log""#).await?);
    assert_eq!(got, vec!["one", "two", "three"]);

    // And the negation keeps the other file's lines.
    let got = to_strings(&lines(&pattern, r#"{} | filename!~".*/a\.log""#).await?);
    assert_eq!(got, vec!["four", "five"]);
    Ok(())
}

/// A label we cannot resolve yet must be reported, not silently return nothing.
#[tokio::test]
async fn unavailable_label_is_an_error() {
    let (_dir, pattern) = two_log_files();

    let ast = Parser::parse(r#"{job="app"}"#).expect("should parse");
    let err =
        plan(&Catalog::files(&pattern), &Query::new(ast)).expect_err("job is not available yet");

    let message = err.to_string();
    assert!(message.contains("job"), "got {message}");
    // The message names the labels this source does have.
    assert!(message.contains("filename"), "got {message}");
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

/// The instant the metric tests evaluate at. Fixed, so the windows are too.
const NOW: &str = "2026-11-27T10:04:00Z";

/// Runs a metric query at `NOW` and returns its `value` column, one row per
/// series, ordered by series so the expectations are stable.
async fn samples(pattern: &str, query: &str) -> Result<Vec<datafusion::arrow::array::RecordBatch>> {
    let ast = Parser::parse(query).expect("test query must parse");
    let now: DateTime<Utc> = NOW.parse().expect("instant must parse");

    SessionContext::new()
        .execute_logical_plan(plan(
            &Catalog::files(pattern),
            &Query::new(ast).with_window(None, Some(now)),
        )?)
        .await?
        .sort_by(vec![col("filename")])?
        .select_columns(&["value"])?
        .collect()
        .await
}

/// `count_over_time` counts the lines whose own timestamp falls in the window,
/// which is `(now - range, now]` — half-open at the start, as Loki's is.
#[tokio::test]
async fn count_over_time_counts_the_lines_in_the_window() -> Result<()> {
    let (_dir, pattern) = metric_log_files();

    // `a.log` has lines at 09:00, 10:00, 10:02 and 10:04; `b.log` one at 10:03.
    let cases = [
        // Five minutes back reaches 09:59, so everything but the 09:00 line.
        ("count_over_time({}[5m])", vec![3, 1]),
        // Four minutes back lands *exactly* on the 10:00 line, which the open
        // bound excludes.
        ("count_over_time({}[4m])", vec![2, 1]),
        // Wide enough for every line in both files.
        ("count_over_time({}[2h])", vec![4, 1]),
        // Narrow enough to catch only the line at the instant itself.
        ("count_over_time({}[1m])", vec![1]),
    ];

    for (query, want) in cases {
        let batches = samples(&pattern, query).await?;
        let got: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                let column = b
                    .column(0)
                    .as_primitive::<datafusion::arrow::datatypes::Int64Type>();
                (0..b.num_rows())
                    .map(|i| column.value(i))
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(got, want, "query: {query}");
    }
    Ok(())
}

/// The pipeline runs before the aggregation, so filters narrow what is counted.
#[tokio::test]
async fn a_range_aggregation_counts_what_the_pipeline_kept() -> Result<()> {
    let (_dir, pattern) = metric_log_files();

    for query in [
        r#"count_over_time({} |= "error" [5m])"#,
        r#"count_over_time({} | logfmt | level="error" [5m])"#,
    ] {
        assert_batches_eq!(
            &[
                "+-------+",
                "| value |",
                "+-------+",
                "| 2     |",
                "| 1     |",
                "+-------+",
            ],
            &samples(&pattern, query).await?
        );
    }
    Ok(())
}

/// `bytes_over_time` measures volume rather than lines, and the two rate
/// functions are those same sums over the window in seconds.
#[tokio::test]
async fn the_byte_and_rate_variants() -> Result<()> {
    let (_dir, pattern) = metric_log_files();

    // The three lines of `a.log` in the window are 37 + 38 + 38 bytes; `b.log`'s
    // single line is 38.
    assert_batches_eq!(
        &[
            "+-------+",
            "| value |",
            "+-------+",
            "| 113   |",
            "| 38    |",
            "+-------+",
        ],
        &samples(&pattern, "bytes_over_time({}[5m])").await?
    );

    // 3 lines and 1 line over 300 seconds.
    assert_batches_eq!(
        &[
            "+-----------------------+",
            "| value                 |",
            "+-----------------------+",
            "| 0.01                  |",
            "| 0.0033333333333333335 |",
            "+-----------------------+",
        ],
        &samples(&pattern, "rate({}[5m])").await?
    );
    Ok(())
}

/// An unknown function is a usage error, and says which name it did not know.
#[test]
fn an_unknown_function_is_reported() {
    let err = Parser::parse("coun_over_time({}[5m])").expect_err("should not parse");

    assert_eq!(err.to_string(), "Unknown function: coun_over_time");
}

/// Runs a log query and returns the lines *in the order the plan produced them*
/// — unlike `lines`, which re-sorts, because here the order is what is on trial.
async fn ordered_lines(
    pattern: &str,
    query: &str,
    configure: impl FnOnce(Query) -> Query,
) -> Result<Vec<String>> {
    let ast = Parser::parse(query).expect("test query must parse");
    let query = configure(Query::new(ast));

    let batches = SessionContext::new()
        .execute_logical_plan(plan(&Catalog::files(pattern), &query)?)
        .await?
        .select_columns(&["line"])?
        .collect()
        .await?;

    Ok(to_strings(&batches))
}

/// The window is `(start, end]`, so a line exactly on `start` is excluded and one
/// exactly on `end` is kept.
#[tokio::test]
async fn a_window_bounds_a_log_query() -> Result<()> {
    let (_dir, pattern) = metric_log_files();
    let at = |t: &str| Some(t.parse::<DateTime<Utc>>().expect("instant"));

    let got = ordered_lines(&pattern, "{}", |query| {
        query.with_window(at("2026-11-27T10:02:00Z"), at("2026-11-27T10:03:00Z"))
    })
    .await?;

    // 10:02 sits on the open start bound and is dropped; 10:03 is kept.
    assert_eq!(got, vec!["2026-11-27T10:03:00Z level=error msg=d"]);
    Ok(())
}

/// A limit means "the newest n", which is only meaningful because the plan
/// orders the lines itself.
#[tokio::test]
async fn a_limit_takes_the_newest_lines() -> Result<()> {
    let (_dir, pattern) = metric_log_files();

    let got = ordered_lines(&pattern, "{}", |query| query.with_limit(Some(2))).await?;

    assert_eq!(
        got,
        vec![
            "2026-11-27T10:04:00Z level=error msg=c",
            "2026-11-27T10:03:00Z level=error msg=d",
        ]
    );
    Ok(())
}

/// Forward reverses that: the oldest lines, oldest first.
#[tokio::test]
async fn forward_takes_the_oldest_lines() -> Result<()> {
    let (_dir, pattern) = metric_log_files();

    let got = ordered_lines(&pattern, "{}", |query| {
        query.with_limit(Some(2)).with_direction(Direction::Forward)
    })
    .await?;

    assert_eq!(
        got,
        vec![
            "2026-11-27T09:00:00Z level=info msg=old",
            "2026-11-27T10:00:00Z level=info msg=a",
        ]
    );
    Ok(())
}

/// A metric query has no instant to look back from unless one is given.
#[tokio::test]
async fn a_metric_query_needs_an_instant() {
    let (_dir, pattern) = metric_log_files();
    let ast = Parser::parse("count_over_time({}[5m])").expect("should parse");

    let err =
        plan(&Catalog::files(&pattern), &Query::new(ast)).expect_err("no instant to evaluate at");

    assert!(err.to_string().contains("instant"), "got {err}");
}
