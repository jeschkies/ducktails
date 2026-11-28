//! A request to the engine: what to run, over which window, and how much of it.
//!
//! Loki splits this across two API endpoints — `/query` takes a `time`,
//! `/query_range` takes `start` and `end` — and both reduce to the same thing: a
//! window with an upper bound. A metric query is evaluated *at* that bound, so
//! `end` is both the end of the window and the instant a range selector looks
//! back from.

use chrono::{DateTime, Utc};

use crate::parser::Expr;

/// Which end of the window a log query returns first.
///
/// Backward by default, as Loki is: the newest lines are the ones a reader wants
/// when a limit cuts the rest off.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Direction {
    #[default]
    Backward,
    Forward,
}

#[derive(Debug, Clone)]
pub struct Query {
    pub expr: Expr,
    /// Exclusive lower bound. `None` reads from the beginning of the data, which
    /// is what somebody pointing at a file means by no argument at all.
    pub start: Option<DateTime<Utc>>,
    /// Inclusive upper bound, and the instant a metric query is evaluated at.
    /// `None` reads to the end of the data; a metric query needs one, so the
    /// caller resolves it first.
    pub end: Option<DateTime<Utc>>,
    /// How many lines to return. `None` is all of them — the API layer applies
    /// Loki's default of 100, the same way `logcli` applies its own.
    pub limit: Option<usize>,
    pub direction: Direction,
}

impl Query {
    /// The whole of the data, newest first.
    pub fn new(expr: Expr) -> Self {
        Self {
            expr,
            start: None,
            end: None,
            limit: None,
            direction: Direction::default(),
        }
    }

    pub fn with_window(mut self, start: Option<DateTime<Utc>>, end: Option<DateTime<Utc>>) -> Self {
        self.start = start;
        self.end = end;
        self
    }

    pub fn with_limit(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
    }

    pub fn with_direction(mut self, direction: Direction) -> Self {
        self.direction = direction;
        self
    }
}
