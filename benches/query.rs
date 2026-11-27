//! Times the query pipeline and counts what it allocates.
//!
//! Single threaded, one partition, on purpose: `AllocProfiler` attributes
//! allocations made on the benchmark's own thread, so a multi-threaded runtime
//! would quietly drop everything DataFusion does on its workers — the numbers
//! would look better than they are.
//!
//! The `line` and `labels` columns are `Utf8View` over the file's own bytes, so
//! "AllocatedBytes" is the claim under test: parsing a logfmt line should cost
//! views, not copies.

use std::fmt::Write as _;

use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::SessionConfig;
use ducktails::parser::Parser;
use ducktails::plan::plan;
use ducktails::table::read_source;
use tempfile::{TempDir, tempdir};

/// Enough lines to dwarf the fixed cost of planning, small enough to iterate on.
const LINES: usize = 20_000;

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    divan::main();
}

/// A logfmt file of `LINES` lines, a tenth of them errors so the filters below
/// have something to reject.
///
/// Values straddle the 12-byte inline limit deliberately: `level` and `status`
/// fit inside a view, `msg` and `caller` do not, so both paths are exercised.
fn logfmt_file() -> (TempDir, String) {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("app.log");

    let mut contents = String::with_capacity(LINES * 128);
    for i in 0..LINES {
        let level = if i % 10 == 0 { "error" } else { "info" };
        writeln!(
            contents,
            r#"level={level} caller=handler.go:{i} msg="request {i} completed in a reasonable time" status=200 duration=12ms"#
        )
        .expect("writing to a String cannot fail");
    }
    std::fs::write(&path, contents).expect("write app.log");

    (dir, path.to_string_lossy().into_owned())
}

/// Parse, plan, execute, collect — everything the CLI does bar printing.
async fn run(pattern: &str, query: &str) -> Result<usize> {
    let ast = Parser::parse(query).expect("benchmark query must parse");
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    let batches = ctx
        .execute_logical_plan(plan(pattern, &ast)?)
        .await?
        .collect()
        .await?;
    Ok(batches.iter().map(|b| b.num_rows()).sum())
}

/// File to `RecordBatch`, with no query on top: this is where `Utf8View` is
/// supposed to save a copy of the whole file.
#[divan::bench]
fn ingest(bencher: divan::Bencher) {
    let (_dir, path) = logfmt_file();

    bencher.bench_local(|| divan::black_box(read_source(&path).expect("reads")));
}

/// The pipeline, one stage at a time. Each query adds exactly one stage to the
/// one above it, so the deltas say what a stage costs.
#[divan::bench(args = [
    "{}",
    r#"{} |= "error""#,
    "{} | logfmt",
    r#"{} | logfmt | level="error""#,
    r#"{} | logfmt | status>=500"#,
    r#"{} | logfmt | duration>10ms"#,
])]
fn query(bencher: divan::Bencher, query: &str) {
    let (_dir, pattern) = logfmt_file();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime");

    bencher.bench_local(|| divan::black_box(runtime.block_on(run(&pattern, query)).expect("runs")));
}
