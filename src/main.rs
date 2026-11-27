use clap::error::ErrorKind;
use clap::{CommandFactory, Parser as ClapParser};
use datafusion::arrow::util::pretty::print_batches;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{LogicalPlan, col};
use ducktails::parser::Parser;
use ducktails::plan::plan;

/// Like DuckDB but for logs.
///
/// Source first, query second, following `duckdb FILENAME SQL`.
#[derive(ClapParser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Log file or glob to read; use /dev/stdin for a pipe
    #[arg(value_name = "FILENAME", value_parser = parse_source)]
    source: String,

    /// LogQL query, e.g. '{job="app"} |= "error"'
    #[arg(value_name = "LOGQL")]
    query: String,
}

/// Rejects a FILENAME we already know we cannot use, before anything is opened.
///
/// Runs during argument parsing, so a bad value is reported by clap in the same
/// format as a bad LOGQL — see `Cli::command().error(..)` below.
fn parse_source(s: &str) -> Result<String, String> {
    if s.trim().is_empty() {
        return Err("must not be empty".to_string());
    }

    // Deliberately no existence check: `/var/log/*.log` is a valid source and
    // not a path that exists, and `/dev/stdin` is not a regular file. Whatever
    // resolves the glob is the only thing that can report "no such file".
    Ok(s.to_string())
}

/// Runs the plan and prints the matching lines.
///
/// Separated from `main` so the body can use `?`: everything in here is a
/// *runtime* failure (a missing file, an invalid regex), which exits 1 —
/// unlike a malformed argument, which clap reports and exits 2.
async fn run(logical_plan: LogicalPlan) -> Result<()> {
    let ctx = SessionContext::new();
    let results = ctx
        .execute_logical_plan(logical_plan)
        .await?
        // Only `line` is meaningful output; `timestamp` is line order (§6) and
        // `labels` is empty until a pipeline stage fills it.
        .select_columns(&["line"])?
        .sort_by(vec![col("timestamp")])?
        .collect()
        .await?;

    print_batches(&results)?;
    Ok(())
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // A bad query or an unsupported label is a usage error: clap's format, exit 2.
    let query = match Parser::parse(&cli.query) {
        Ok(expr) => expr,
        Err(e) => Cli::command().error(ErrorKind::ValueValidation, e).exit(),
    };
    let logical_plan = match plan(&cli.source, &query) {
        Ok(p) => p,
        Err(e) => Cli::command().error(ErrorKind::ValueValidation, e).exit(),
    };

    if let Err(e) = run(logical_plan).await {
        eprintln!("ducktails: {e}");
        std::process::exit(1);
    }
}
