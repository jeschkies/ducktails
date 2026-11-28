//! LogQL over Apache DataFusion. Like DuckDB but for logs.
//!
//! The library is everything except argv, stdout and exit codes — those live in
//! `main.rs`.

pub mod catalog;
pub mod error;
pub mod output;
pub mod parser;
pub mod plan;
pub mod query;
pub mod scanner;
pub mod table;
pub mod udf;
pub mod units;
