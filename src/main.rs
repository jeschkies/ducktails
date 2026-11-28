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
use ducktails::query::{Direction, Query};
use ducktails::server;
use ducktails::table::is_label;

/// Like DuckDB but for logs.
///
/// Subcommands rather than DuckDB's bare `FILENAME SQL`, because serving is not
/// a query — `logcli query` is the closer model, and the point of serving is
/// Loki compatibility.
#[derive(ClapParser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(clap::Subcommand, Debug)]
enum Command {
    /// Run a LogQL query and print the result
    Query(QueryArgs),
    /// Serve a subset of Loki's HTTP API
    Serve(ServeArgs),
}

#[derive(clap::Args, Debug)]
struct QueryArgs {
    /// Log file or glob to read; use /dev/stdin for a pipe
    #[arg(value_name = "FILENAME", value_parser = parse_source)]
    source: String,

    /// LogQL query, e.g. '{job="app"} |= "error"'
    #[arg(value_name = "LOGQL")]
    query: String,

    /// Start of the query window, exclusive, RFC3339
    ///
    /// Unset reads from the beginning of the data, which is what pointing at a
    /// file usually means — unlike `logcli`, whose `--since=1h` default suits a
    /// store that holds live data.
    #[arg(long, value_name = "RFC3339")]
    from: Option<DateTime<Utc>>,

    /// End of the query window, inclusive, and the instant a metric query is
    /// evaluated at, RFC3339
    ///
    /// Unset reads to the end of the data. For a metric query that resolves to
    /// the newest line rather than to wall-clock now, which is where this parts
    /// ways with `logcli instant-query --now`: a file can be arbitrarily old,
    /// and "now" would then select nothing.
    #[arg(long, visible_alias = "now", value_name = "RFC3339")]
    to: Option<DateTime<Utc>>,

    /// Maximum number of lines to return
    #[arg(long, default_value_t = 100)]
    limit: usize,

    /// Return the oldest lines first instead of the newest
    #[arg(long)]
    forward: bool,

    /// How to render the result
    #[arg(short, long, value_enum, default_value_t = Output::Table)]
    output: Output,
}

#[derive(clap::Args, Debug)]
struct ServeArgs {
    /// Log file or glob to read; use /dev/stdin for a pipe
    #[arg(value_name = "FILENAME", value_parser = parse_source)]
    source: String,

    /// Address to listen on; 3100 is Loki's, so a Grafana datasource needs no
    /// special casing
    #[arg(long, default_value = "0.0.0.0:3100")]
    listen: String,
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
    args: &QueryArgs,
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
            // The plan already ordered and cut these, so there is nothing to do
            // here but choose the columns.
            match args.output {
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
        args.output.into(),
        kind,
        &results,
    )
}

/// The newest timestamp in `source`, for a metric query given no `--to`.
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

    let result = match &cli.command {
        Command::Query(args) => run_query(args).await,
        Command::Serve(args) => serve(args).await,
    };

    if let Err(e) = result {
        eprintln!("ducktails: {e}");
        std::process::exit(1);
    }
}

async fn run_query(args: &QueryArgs) -> Result<()> {
    // A bad query or an unsupported label is a usage error: clap's format, exit 2.
    let expr = match Parser::parse(&args.query) {
        Ok(expr) => expr,
        Err(e) => Cli::command().error(ErrorKind::ValueValidation, e).exit(),
    };

    // Opening the catalog may list an object store or read file metadata, which
    // is why it is the one step before planning that can fail at runtime.
    let ctx = SessionContext::new();
    let catalog = Catalog::open(&args.source, &ctx.state()).await?;

    let kind = match expr {
        Expr::Log(_) => ResultKind::Logs,
        Expr::Metric(_) => ResultKind::Samples,
    };

    // Only a metric query needs a concrete instant, and resolving the default
    // reads the data, so it is not worth doing for a log query.
    let end = match (args.to, kind) {
        (Some(to), _) => Some(to),
        (None, ResultKind::Samples) => Some(newest_timestamp(&ctx, &catalog).await?),
        (None, ResultKind::Logs) => None,
    };

    let query = Query::new(expr)
        .with_window(args.from, end)
        .with_limit(Some(args.limit))
        .with_direction(match args.forward {
            true => Direction::Forward,
            false => Direction::Backward,
        });

    let logical_plan = match plan(&catalog, &query) {
        Ok(logical_plan) => logical_plan,
        Err(e) => Cli::command().error(ErrorKind::ValueValidation, e).exit(),
    };

    run(&ctx, logical_plan, args, kind).await
}

async fn serve(args: &ServeArgs) -> Result<()> {
    let ctx = SessionContext::new();
    let catalog = Catalog::open(&args.source, &ctx.state()).await?;

    server::serve(&args.listen, server::Api::new(ctx, catalog)).await
}
