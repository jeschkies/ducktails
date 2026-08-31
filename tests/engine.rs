use datafusion::{assert_batches_eq, error::Result, logical_expr::col, prelude::SessionContext};
use ducktails::{parser::Parser, plan::plan};
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

#[tokio::test]
async fn filter_filename_and_lines() -> Result<()> {
    let ctx = SessionContext::new();
    let (_dir, pattern) = two_log_files();

    let query = Parser::parse(r#"{filename=~".*/a\\.log"} |~ "one|three" "#)
        .expect("test query must parse");
    let logical_plan = plan(&pattern, &query)?;

    let results = ctx
        .execute_logical_plan(logical_plan)
        .await?
        .select_columns(&["line"])?
        .sort_by(vec![col("line")])?
        .collect()
        .await?;

    assert_batches_eq!(
        &[
            "+-------+",
            "| line  |",
            "+-------+",
            "| one   |",
            "| three |",
            "+-------+",
        ],
        &results
    );
    Ok(())
}
