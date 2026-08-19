mod error;
mod parser;
mod scanner;

use crate::parser::Parser;
use clap::Parser as ClapParser;

/// Simple program to greet a person
#[derive(ClapParser, Debug)]
#[command(version, about, long_about = None)]
struct CLI {
    /// Name of the person to greet
    #[arg(short, long)]
    query: String,
}

fn main() {
    let args = CLI::parse();

    let expr = Parser::parse(args.query.as_str()).unwrap();
    println!("{:?}", expr)
}
