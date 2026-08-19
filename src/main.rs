mod error;
mod parser;
mod scanner;

use crate::parser::Parser;
use clap::error::ErrorKind;
use clap::{CommandFactory, Parser as ClapParser};

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

    // TODO(human)
    Ok(s.to_string())
}

fn main() {
    let cli = Cli::parse();

    match Parser::parse(&cli.query) {
        Ok(expr) => {
            println!("source: {}", cli.source);
            println!("query:  {expr:?}");
        }
        Err(e) => Cli::command().error(ErrorKind::ValueValidation, e).exit(),
    }
}
