//! LogQL AST → DataFusion `LogicalPlan`.
//!
//! Shaped like DataFusion's own SQL frontend: `SqlToRel` turns a SQL AST into a
//! `LogicalPlan` against a `TableSource`, and `SessionContext::execute_logical_plan`
//! runs it. Nothing here consults a catalog or executes anything, so this stage
//! is synchronous and testable by asserting on plan shape.

use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::TableReference;
use datafusion::datasource::provider_as_source;
use datafusion::error::{DataFusionError, Result};
use datafusion::functions::expr_fn::{coalesce, contains};
use datafusion::functions_nested::expr_fn::array_concat;
use datafusion::logical_expr::{
    Expr as DfExpr, LogicalPlan, LogicalPlanBuilder, Operator, binary_expr, col, lit, not,
    try_cast, when,
};

use crate::parser::{
    Expr, LabelFilter, LabelFilterOp, LabelValue, LineFilter, LineFilterOp, MatchOp, Matcher,
    Selector, Stage,
};
use crate::table::LogTable;
use crate::udf::{label_bytes, label_duration, label_value, logfmt_parse};

/// The column holding the raw log line — what line filters apply to.
const LINE: &str = "line";

/// Monotonic line index (§6), carried through projections to preserve order.
const TIMESTAMP: &str = "timestamp";

/// `List<Struct<key, value>>` — appended to by parser stages, read by label filters.
const LABELS: &str = "labels";

/// The only label available before a pipeline stage populates `labels`, per
/// DESIGN.md §2. A plain text file carries no labels of its own.
const FILENAME: &str = "filename";

/// Plans `query` over the files matched by the `source` glob.
///
/// `source` doubles as the table name, so `EXPLAIN` output names the glob —
/// the same read as DuckDB's `SELECT * FROM '/var/log/*.log'`.
pub fn plan(source: &str, query: &Expr) -> Result<LogicalPlan> {
    let Expr::Log(log_query) = query;

    let table = provider_as_source(Arc::new(LogTable::new(source)));
    let mut builder = LogicalPlanBuilder::scan(TableReference::bare(source), table, None)?;

    // A selector is a conjunction, so it collapses into a single `Filter`.
    if let Some(predicate) = selector_predicate(&log_query.selector)? {
        builder = builder.filter(predicate)?;
    }

    // Each pipeline stage is its own node: order is semantic (§3), and a later
    // `| json` will read the output of the filters above it.
    for stage in &log_query.pipeline {
        builder = match stage {
            Stage::LabelFilter(filter) => builder.filter(label_predicate(filter)?)?,
            Stage::Line(filter) => builder.filter(line_predicate(filter))?,
            // A parser stage is a `Projection`, not a `Filter`: it rewrites
            // `labels` and leaves the row count alone.
            //
            // `labels` accumulates rather than being replaced, so chained parser
            // stages each see the previous stage's output — as Loki does. Not yet
            // implemented: Loki suffixes a colliding key with `_extracted`, so
            // duplicates currently survive and precedence is undefined.
            Stage::Logfmt => builder.project(vec![
                col(TIMESTAMP),
                col(LINE),
                array_concat(vec![col(LABELS), logfmt_parse().call(vec![col(LINE)])]).alias(LABELS),
                col(FILENAME),
            ])?,
        };
    }

    builder.build()
}

/// A label filter reads `labels`, so every variant starts from `label_value`.
///
/// Loki's semantics here are not uniform, and the difference is deliberate:
///
/// - a **string** filter reads a missing label as `""`, so `| level!="error"`
///   keeps a line that has no `level` at all;
/// - a **numeric, duration or bytes** filter drops a line whose label is
///   missing, but *keeps* one whose label is present and unparseable, tagging it
///   `__error__="LabelFilterErr"` so the user sees the bad data rather than
///   silently losing the line. We have no `__error__` label, so such a line is
///   kept unflagged: the rows match Loki, the explanation does not.
fn label_predicate(filter: &LabelFilter) -> Result<DfExpr> {
    let value = label_value().call(vec![col(LABELS), lit(filter.label.clone())]);

    match &filter.value {
        LabelValue::String(text) => string_label_predicate(value, filter, text),
        LabelValue::Number(number) => {
            let parsed = try_cast(value.clone(), DataType::Float64);
            numeric_label_predicate(value, parsed, filter, lit(*number))
        }
        LabelValue::Duration(duration) => {
            let nanos = i64::try_from(duration.as_nanos()).map_err(|_| {
                DataFusionError::Plan(format!(
                    "label filter on {:?}: {duration:?} is too long to compare",
                    filter.label
                ))
            })?;
            let parsed = label_duration().call(vec![value.clone()]);
            numeric_label_predicate(value, parsed, filter, lit(nanos))
        }
        LabelValue::Bytes(bytes) => {
            let parsed = label_bytes().call(vec![value.clone()]);
            numeric_label_predicate(value, parsed, filter, lit(*bytes))
        }
    }
}

fn string_label_predicate(value: DfExpr, filter: &LabelFilter, text: &str) -> Result<DfExpr> {
    // A missing label compares as the empty string, which is what makes `!=`
    // keep a line that lacks the label entirely.
    let value = coalesce(vec![value, lit("")]);

    Ok(match filter.op {
        LabelFilterOp::Eq => value.eq(lit(text)),
        LabelFilterOp::Neq => value.not_eq(lit(text)),
        // Fully anchored, like a selector matcher and unlike a line filter.
        LabelFilterOp::Re => binary_expr(value, Operator::RegexMatch, lit(format!("^(?:{text})$"))),
        LabelFilterOp::Nre => {
            binary_expr(value, Operator::RegexNotMatch, lit(format!("^(?:{text})$")))
        }
        op => {
            return Err(DataFusionError::Plan(format!(
                "label filter on {:?}: {op:?} needs a number, duration or byte size, not a string",
                filter.label
            )));
        }
    })
}

/// The three-way outcome a numeric, duration or bytes filter has: `value` tells
/// absent from present, `parsed` tells parsed from unparseable.
fn numeric_label_predicate(
    value: DfExpr,
    parsed: DfExpr,
    filter: &LabelFilter,
    literal: DfExpr,
) -> Result<DfExpr> {
    let op = match filter.op {
        LabelFilterOp::Eq => Operator::Eq,
        LabelFilterOp::Neq => Operator::NotEq,
        LabelFilterOp::Gt => Operator::Gt,
        LabelFilterOp::Gte => Operator::GtEq,
        LabelFilterOp::Lt => Operator::Lt,
        LabelFilterOp::Lte => Operator::LtEq,
        op => {
            return Err(DataFusionError::Plan(format!(
                "label filter on {:?}: {op:?} needs a string on the right",
                filter.label
            )));
        }
    };

    when(value.is_null(), lit(false))
        .when(parsed.clone().is_null(), lit(true))
        .otherwise(binary_expr(parsed, op, literal))
}

/// ANDs the matchers together, as Loki does. `None` for `{}`, which selects
/// everything rather than nothing.
fn selector_predicate(selector: &Selector) -> Result<Option<DfExpr>> {
    selector
        .matchers
        .iter()
        .try_fold(None, |acc: Option<DfExpr>, matcher| {
            let next = matcher_predicate(matcher)?;
            Ok(Some(match acc {
                Some(predicate) => predicate.and(next),
                None => next,
            }))
        })
}

fn matcher_predicate(matcher: &Matcher) -> Result<DfExpr> {
    if matcher.name != FILENAME {
        return Err(DataFusionError::NotImplemented(format!(
            "label {:?}: only `{FILENAME}` is available until a pipeline stage populates labels",
            matcher.name
        )));
    }

    let column = col(FILENAME);
    let value = lit(matcher.value.clone());

    Ok(match matcher.op {
        MatchOp::Eq => column.eq(value),
        MatchOp::Neq => column.not_eq(value),
        MatchOp::Re => binary_expr(
            column,
            Operator::RegexMatch,
            lit(format!("^(?:{})$", matcher.value)),
        ),
        MatchOp::Nre => binary_expr(
            column,
            Operator::RegexNotMatch,
            lit(format!("^(?:{})$", matcher.value)),
        ),
    })
}

/// A line filter tests the raw line, so every variant reads the `line` column.
fn line_predicate(filter: &LineFilter) -> DfExpr {
    let column = col(LINE);
    let value = lit(filter.value.clone());

    match filter.op {
        // `|=` and `!=` are substring tests, not equality.
        LineFilterOp::Contains => contains(column, value),
        LineFilterOp::NotContains => not(contains(column, value)),
        // Loki does not anchor line filter regexes: "the `|~` and `!~` regex
        // operators are not fully anchored". `RegexMatch` is unanchored too, so
        // this maps across directly — unlike the label matcher case above.
        LineFilterOp::Re => binary_expr(column, Operator::RegexMatch, value),
        LineFilterOp::Nre => binary_expr(column, Operator::RegexNotMatch, value),
    }
}
