//! LogQL over Apache DataFusion. Like DuckDB but for logs.
//!
//! The library is everything except argv, stdout and exit codes — those live in
//! `main.rs`. Design decisions and their reasoning are in `DESIGN.md`.

pub mod error;
pub mod parser;
pub mod plan;
pub mod scanner;
pub mod table;
