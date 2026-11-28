//! LogQL AST → DataFusion `LogicalPlan`.
//!
//! Shaped like DataFusion's own SQL frontend: `SqlToRel` turns a SQL AST into a
//! `LogicalPlan` against a `TableSource`, and `SessionContext::execute_logical_plan`
//! runs it. Nothing here consults a catalog or executes anything, so this stage
//! is synchronous and testable by asserting on plan shape.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::functions::expr_fn::{coalesce, contains, octet_length};
use datafusion::functions_aggregate::expr_fn::{count, max, sum};
use datafusion::functions_nested::expr_fn::array_concat;
use datafusion::logical_expr::{
    Expr as DfExpr, LogicalPlan, LogicalPlanBuilder, Operator, binary_expr, col, lit, not,
    try_cast, when,
};

use crate::catalog::Catalog;
use crate::parser::{
    Expr, LabelFilter, LabelFilterOp, LabelValue, LineFilter, LineFilterOp, LogQuery, MatchOp,
    Matcher, RangeAggregation, RangeOp, Selector, Stage,
};
use crate::table::is_label;
use crate::udf::{label_bytes, label_duration, label_value, logfmt_parse};

/// The column holding the raw log line — what line filters apply to.
const LINE: &str = "line";

/// Monotonic line index (§6), carried through projections to preserve order.
const TIMESTAMP: &str = "timestamp";

/// `List<Struct<key, value>>` — appended to by parser stages, read by label filters.
const LABELS: &str = "labels";

/// The sample a metric query produces, one per series.
const VALUE: &str = "value";

/// Plans `query` over the files matched by the `source` glob.
///
/// `source` doubles as the table name, so `EXPLAIN` output names the glob —
/// the same read as DuckDB's `SELECT * FROM '/var/log/*.log'`.
/// `now` is the instant a metric query is evaluated at, as Loki's instant query
/// API takes a `time` parameter. A log query ignores it, until it grows a query
/// window of its own.
pub fn plan(catalog: &Catalog, query: &Expr, now: DateTime<Utc>) -> Result<LogicalPlan> {
    match query {
        Expr::Log(log_query) => log_builder(catalog, log_query)?.build(),
        Expr::Metric(aggregation) => plan_range_aggregation(catalog, aggregation, now),
    }
}

/// Everything a metric query shares with a log query: the scan, the selector and
/// the pipeline. Hands back the builder so an aggregation can keep building.
fn log_builder(catalog: &Catalog, log_query: &LogQuery) -> Result<LogicalPlanBuilder> {
    let mut builder = catalog.scan()?;

    // What a label means is whatever the catalog exposes, so every matcher and
    // filter below resolves against this schema.
    let schema = Arc::new(builder.schema().as_ref().clone());

    // A selector is a conjunction, so it collapses into a single `Filter`.
    if let Some(predicate) = selector_predicate(&log_query.selector, &schema)? {
        builder = builder.filter(predicate)?;
    }

    // Each pipeline stage is its own node: order is semantic (§3), and a later
    // `| json` will read the output of the filters above it.
    for stage in &log_query.pipeline {
        builder = match stage {
            Stage::LabelFilter(filter) => builder.filter(label_predicate(filter, &schema)?)?,
            Stage::Line(filter) => builder.filter(line_predicate(filter))?,
            // A parser stage is a `Projection`, not a `Filter`: it rewrites
            // `labels` and leaves the row count alone.
            //
            // `labels` accumulates rather than being replaced, so chained parser
            // stages each see the previous stage's output — as Loki does. Not yet
            // implemented: Loki suffixes a colliding key with `_extracted`, so
            // duplicates currently survive and precedence is undefined.
            Stage::Logfmt => builder.project(
                vec![
                    col(TIMESTAMP),
                    col(LINE),
                    array_concat(vec![col(LABELS), logfmt_parse().call(vec![col(LINE)])])
                        .alias(LABELS),
                ]
                .into_iter()
                .chain(label_columns(&schema))
                .collect::<Vec<_>>(),
            )?,
        };
    }

    Ok(builder)
}

/// Plans `max(timestamp)` over the catalog.
///
/// The default instant to evaluate a metric query at. Loki's API defaults `time`
/// to wall-clock now, which works because Loki holds recent data; a file can be
/// arbitrarily old, so the newest line in the data is the useful default here.
pub fn plan_newest(catalog: &Catalog) -> Result<LogicalPlan> {
    catalog
        .scan()?
        .aggregate(
            Vec::<DfExpr>::new(),
            vec![max(col(TIMESTAMP)).alias(TIMESTAMP)],
        )?
        .build()
}

/// A timestamp literal of the same type as the `timestamp` column.
fn instant(nanos: i64) -> DfExpr {
    lit(ScalarValue::TimestampNanosecond(Some(nanos), None))
}

/// Plans an instant query: one sample per series over `(now - range, now]`.
///
/// Half-open at the start, as Loki is — a line exactly `range` old belongs to the
/// previous window.
///
/// The series key is `filename`, the only stream identity a text file has, and
/// exactly Loki's whenever the pipeline extracts no labels. Once `sum by (…)`
/// exists its grouping replaces this.
fn plan_range_aggregation(
    catalog: &Catalog,
    aggregation: &RangeAggregation,
    now: DateTime<Utc>,
) -> Result<LogicalPlan> {
    let end = now.timestamp_nanos_opt().ok_or_else(|| {
        DataFusionError::Plan(format!(
            "{now} is outside the range of nanosecond timestamps"
        ))
    })?;
    let start = i64::try_from(aggregation.range.as_nanos())
        .ok()
        .and_then(|range| end.checked_sub(range))
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "a range of {:?} reaches outside the range of nanosecond timestamps",
                aggregation.range
            ))
        })?;

    // `count(1)` counts lines; the byte variants measure log volume, so they sum
    // the lines' own lengths.
    let value = match aggregation.op {
        RangeOp::CountOverTime | RangeOp::Rate => count(lit(1)),
        RangeOp::BytesOverTime | RangeOp::BytesRate => sum(octet_length(col(LINE))),
    };

    // The rates are those same sums over the window, so they differ only here.
    let sample = match aggregation.op {
        RangeOp::CountOverTime | RangeOp::BytesOverTime => col(VALUE),
        RangeOp::Rate | RangeOp::BytesRate => col(VALUE) / lit(aggregation.range.as_secs_f64()),
    };

    let builder = log_builder(catalog, &aggregation.query)?;
    // One series per label set, which for a catalog whose labels are columns is
    // exactly Loki's — bar the labels a parser stage extracted, which stay out of
    // the key until there is a canonical form to group them by.
    let series = label_columns(builder.schema());

    builder
        .filter(
            col(TIMESTAMP)
                .gt(instant(start))
                .and(col(TIMESTAMP).lt_eq(instant(end))),
        )?
        .aggregate(series.clone(), vec![value.alias(VALUE)])?
        .project(
            series
                .into_iter()
                .chain([instant(end).alias(TIMESTAMP), sample.alias(VALUE)])
                .collect::<Vec<_>>(),
        )?
        .build()
}

/// The label columns of `schema`, in schema order.
///
/// These are a catalog's stream labels — the series key of a metric query, and
/// the labels a selector can match on.
fn label_columns(schema: &DFSchema) -> Vec<DfExpr> {
    schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .filter(|name| is_label(name))
        .map(col)
        .collect()
}

fn is_label_column(name: &str, schema: &DFSchema) -> bool {
    is_label(name) && schema.has_column_with_unqualified_name(name)
}

/// A label is a column when the catalog materialises it, and an entry in the
/// `labels` list otherwise.
///
/// The column form is the one worth having: it sits in a `Filter` above the
/// scan, so a source can push it down. A `label_value` call is opaque by
/// comparison and can only run once rows exist.
fn label_expr(name: &str, schema: &DFSchema) -> DfExpr {
    match is_label_column(name, schema) {
        true => col(name),
        false => label_value().call(vec![col(LABELS), lit(name)]),
    }
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
fn label_predicate(filter: &LabelFilter, schema: &DFSchema) -> Result<DfExpr> {
    let value = label_expr(&filter.label, schema);

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
fn selector_predicate(selector: &Selector, schema: &DFSchema) -> Result<Option<DfExpr>> {
    selector
        .matchers
        .iter()
        .try_fold(None, |acc: Option<DfExpr>, matcher| {
            let next = matcher_predicate(matcher, schema)?;
            Ok(Some(match acc {
                Some(predicate) => predicate.and(next),
                None => next,
            }))
        })
}

/// A selector runs before any pipeline stage, so the only labels it can match on
/// are the ones the catalog itself carries. An unknown name is a mistake worth
/// reporting rather than a matcher that silently selects nothing.
fn matcher_predicate(matcher: &Matcher, schema: &DFSchema) -> Result<DfExpr> {
    if !is_label_column(&matcher.name, schema) {
        let available: Vec<String> = label_columns(schema)
            .iter()
            .map(|label| label.to_string())
            .collect();
        return Err(DataFusionError::Plan(format!(
            "label {:?}: this source has no such label, only {}",
            matcher.name,
            available.join(", ")
        )));
    }

    let column = col(&matcher.name);
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
