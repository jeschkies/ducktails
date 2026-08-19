mod error;
mod parser;
mod scanner;

use crate::parser::Parser;
use clap::error::ErrorKind;
use clap::{CommandFactory, Parser as ClapParser};

/// Like DuckDB but for logs.
#[derive(ClapParser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Name of the person to greet
    #[arg(short, long)]
    query: String,
}

fn main() {
    let args = Cli::parse();

    match Parser::parse(&args.query) {
        Ok(expr) => println!("{expr:?}"),
        Err(e) => Cli::command().error(ErrorKind::ValueValidation, e).exit(),
    }
}
