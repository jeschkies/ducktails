use chrono::{DateTime, Utc};
use clap::error::ErrorKind;
use clap::{CommandFactory, Parser as ClapParser, ValueEnum};
use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::datatypes::TimestampNanosecondType;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{LogicalPlan, col};
use ducktails::catalog::Catalog;
use ducktails::output::{Format, ResultKind, write_result};
use ducktails::parser::{Expr, Parser};
use ducktails::plan::{plan, plan_newest};
use ducktails::table::is_label;

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

    /// Instant to evaluate a metric query at, RFC3339
    ///
    /// Defaults to the newest line in the data rather than to wall-clock now,
    /// which is where this parts ways with `logcli instant-query --now`: a file
    /// can be arbitrarily old, and "now" would then select nothing.
    #[arg(long, value_name = "RFC3339")]
    now: Option<DateTime<Utc>>,

    /// How to render the result
    #[arg(short, long, value_enum, default_value_t = Output::Table)]
    output: Output,
}

/// The formats `--output` accepts. A separate enum from `output::Format` so the
/// library keeps its distance from clap.
#[derive(Debug, Clone, Copy, ValueEnum)]
enum Output {
    /// A table to read in a terminal
    Table,
    /// Prometheus' HTTP API response shape, which Loki's API mirrors
    Json,
}

impl From<Output> for Format {
    fn from(output: Output) -> Self {
        match output {
            Output::Table => Format::Table,
            Output::Json => Format::Json,
        }
    }
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
async fn run(
    ctx: &SessionContext,
    logical_plan: LogicalPlan,
    cli: &Cli,
    kind: ResultKind,
) -> Result<()> {
    let frame = ctx.execute_logical_plan(logical_plan).await?;

    let results = match kind {
        ResultKind::Samples => {
            // Ordered by the series key — whatever this catalog calls its labels
            // — so repeated runs agree. A catalog with none yields one series,
            // and `sort_by` wants at least one expression.
            let series: Vec<_> = frame
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .filter(|name| is_label(name))
                .map(col)
                .collect();

            match series.is_empty() {
                true => frame.collect().await?,
                false => frame.sort_by(series)?.collect().await?,
            }
        }
        ResultKind::Logs => {
            let frame = frame.sort_by(vec![col("timestamp")])?;
            match cli.output {
                // A table of log lines is read by a person, and a column of
                // identical paths beside every line is just noise.
                Output::Table => frame.select_columns(&["line"])?,
                // The JSON shape needs the stream labels and the instant, so it
                // gets every column.
                Output::Json => frame,
            }
            .collect()
            .await?
        }
    };

    write_result(
        &mut std::io::stdout().lock(),
        cli.output.into(),
        kind,
        &results,
    )
}

/// The newest timestamp in `source`, for a metric query given no `--now`.
///
/// An empty source has no newest line, so wall-clock now stands in; the query
/// then selects nothing, which is the honest answer.
async fn newest_timestamp(ctx: &SessionContext, catalog: &Catalog) -> Result<DateTime<Utc>> {
    let batches = ctx
        .execute_logical_plan(plan_newest(catalog)?)
        .await?
        .collect()
        .await?;

    let newest = batches
        .first()
        .map(|batch| batch.column(0).as_primitive::<TimestampNanosecondType>())
        .filter(|column| !column.is_empty() && !column.is_null(0))
        .map(|column| column.value(0));

    Ok(newest
        .map(DateTime::from_timestamp_nanos)
        .unwrap_or_else(Utc::now))
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // A bad query or an unsupported label is a usage error: clap's format, exit 2.
    let query = match Parser::parse(&cli.query) {
        Ok(expr) => expr,
        Err(e) => Cli::command().error(ErrorKind::ValueValidation, e).exit(),
    };
    // Opening the catalog may list an object store or read file metadata, which
    // is why it is the one step before planning that can fail at runtime.
    let ctx = SessionContext::new();
    let catalog = match Catalog::open(&cli.source, &ctx.state()).await {
        Ok(catalog) => catalog,
        Err(e) => {
            eprintln!("ducktails: {e}");
            std::process::exit(1);
        }
    };

    // Resolving the default instant reads the data, so it is only worth doing for
    // a query that has an instant at all.
    let kind = match query {
        Expr::Log(_) => ResultKind::Logs,
        Expr::Metric(_) => ResultKind::Samples,
    };
    let metric = kind == ResultKind::Samples;
    let now = match cli.now {
        Some(now) => now,
        None if metric => match newest_timestamp(&ctx, &catalog).await {
            Ok(now) => now,
            Err(e) => {
                eprintln!("ducktails: {e}");
                std::process::exit(1);
            }
        },
        None => Utc::now(),
    };

    let logical_plan = match plan(&catalog, &query, now) {
        Ok(p) => p,
        Err(e) => Cli::command().error(ErrorKind::ValueValidation, e).exit(),
    };

    if let Err(e) = run(&ctx, logical_plan, &cli, kind).await {
        eprintln!("ducktails: {e}");
        std::process::exit(1);
    }
}
