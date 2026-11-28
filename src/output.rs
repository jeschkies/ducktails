//! Rendering a result set: a table to read, or JSON for something else to read.
//!
//! Writes to an `impl Write` rather than to stdout, so a test can capture the
//! bytes — `main.rs` is still the only thing that knows what stdout is.

use std::io::Write;

use datafusion::arrow::array::{Array, AsArray, RecordBatch};
use datafusion::arrow::datatypes::{Schema, TimestampNanosecondType};
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::error::{DataFusionError, Result};
use serde_json::{Value, json};

use crate::table::is_label;

/// Which kind of query produced the rows, which decides the JSON shape: Loki
/// answers a log query with `streams` and a metric query with `vector`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultKind {
    Logs,
    Samples,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Format {
    #[default]
    Table,
    /// Prometheus' HTTP API response shape, which Loki's own API mirrors.
    Json,
}

pub fn write_result(
    out: &mut impl Write,
    format: Format,
    kind: ResultKind,
    batches: &[RecordBatch],
) -> Result<()> {
    match format {
        Format::Table => writeln!(out, "{}", pretty_format_batches(batches)?),
        Format::Json => {
            let body = serde_json::to_string_pretty(&json_result(kind, batches)?)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            writeln!(out, "{body}")
        }
    }
    .map_err(|e| DataFusionError::IoError(e).context("writing the result"))
}

/// The indices of the label columns, in schema order.
fn label_columns(schema: &Schema) -> Vec<usize> {
    (0..schema.fields().len())
        .filter(|&i| is_label(schema.field(i).name()))
        .collect()
}

/// Nanoseconds since the epoch from the `timestamp` column of `batch`.
fn timestamps(batch: &RecordBatch) -> Result<&dyn Array> {
    batch
        .column_by_name("timestamp")
        .map(|column| column.as_ref())
        .ok_or_else(|| DataFusionError::Internal("a result has no timestamp column".into()))
}

/// One row's labels, as a JSON object.
fn labels_of(batch: &RecordBatch, columns: &[usize], row: usize) -> Result<Value> {
    let options = FormatOptions::default();
    let mut labels = serde_json::Map::new();

    for &column in columns {
        let array = batch.column(column);
        if array.is_null(row) {
            continue;
        }
        let text = ArrayFormatter::try_new(array.as_ref(), &options)?
            .value(row)
            .to_string();
        labels.insert(batch.schema().field(column).name().clone(), text.into());
    }

    Ok(Value::Object(labels))
}

/// The Prometheus/Loki response body for a result set.
///
/// A `Value` rather than a string: the HTTP API returns it as JSON directly, and
/// only the terminal wants it pretty-printed.
pub fn json_result(kind: ResultKind, batches: &[RecordBatch]) -> Result<Value> {
    let result = match kind {
        ResultKind::Samples => vector_result(batches)?,
        ResultKind::Logs => streams_result(batches)?,
    };
    let result_type = match kind {
        ResultKind::Samples => "vector",
        ResultKind::Logs => "streams",
    };

    Ok(json!({
        "status": "success",
        "data": { "resultType": result_type, "result": result },
    }))
}

/// A metric query is already one row per series, so each row is one sample.
///
/// Prometheus writes the instant as seconds — a number — and the sample as a
/// string, which is why the value goes through a formatter rather than a cast.
fn vector_result(batches: &[RecordBatch]) -> Result<Vec<Value>> {
    let options = FormatOptions::default();
    let mut samples = Vec::new();

    for batch in batches {
        let columns = label_columns(&batch.schema());
        let instants = timestamps(batch)?.as_primitive::<TimestampNanosecondType>();
        let values = batch
            .column_by_name("value")
            .ok_or_else(|| DataFusionError::Internal("a sample has no value column".into()))?;
        let values = ArrayFormatter::try_new(values.as_ref(), &options)?;

        for row in 0..batch.num_rows() {
            samples.push(json!({
                "metric": labels_of(batch, &columns, row)?,
                "value": [seconds(instants.value(row)), values.value(row).to_string()],
            }));
        }
    }

    Ok(samples)
}

/// Prometheus renders a whole second as an integer and anything finer as a
/// fraction, so this does too rather than always printing `.0`.
fn seconds(nanos: i64) -> Value {
    match nanos % 1_000_000_000 {
        0 => json!(nanos / 1_000_000_000),
        _ => json!(nanos as f64 / 1e9),
    }
}

/// A log query is one row per line, so lines are grouped back into the stream
/// they came from. Loki's entries are `[<nanoseconds as a string>, <line>]`.
///
/// Streams keep the order they first appear in, and a linear scan finds them:
/// a query spans a handful of streams, not thousands.
fn streams_result(batches: &[RecordBatch]) -> Result<Vec<Value>> {
    let mut streams: Vec<(Value, Vec<Value>)> = Vec::new();

    for batch in batches {
        let columns = label_columns(&batch.schema());
        let instants = timestamps(batch)?.as_primitive::<TimestampNanosecondType>();
        let lines = batch
            .column_by_name("line")
            .ok_or_else(|| DataFusionError::Internal("a log result has no line column".into()))?
            .as_string_view();

        for row in 0..batch.num_rows() {
            let labels = labels_of(batch, &columns, row)?;
            let entry = json!([instants.value(row).to_string(), lines.value(row)]);

            match streams.iter_mut().find(|(stream, _)| *stream == labels) {
                Some((_, entries)) => entries.push(entry),
                None => streams.push((labels, vec![entry])),
            }
        }
    }

    Ok(streams
        .into_iter()
        .map(|(stream, values)| json!({ "stream": stream, "values": values }))
        .collect())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{
        Int64Array, StringArray, StringViewArray, TimestampNanosecondArray,
    };
    use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};

    use super::*;

    /// 2026-11-27T10:04:00Z and one minute later.
    const T: i64 = 1_795_773_840_000_000_000;

    fn schema(fields: Vec<Field>) -> Arc<Schema> {
        Arc::new(Schema::new(fields))
    }

    fn timestamp_field() -> Field {
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )
    }

    fn rendered(format: Format, kind: ResultKind, batch: RecordBatch) -> String {
        let mut out = Vec::new();
        write_result(&mut out, format, kind, &[batch]).expect("writes");
        String::from_utf8(out).expect("utf8")
    }

    #[test]
    fn samples_render_as_a_prometheus_vector() {
        let batch = RecordBatch::try_new(
            schema(vec![
                Field::new("filename", DataType::Utf8, false),
                timestamp_field(),
                Field::new("value", DataType::Int64, true),
            ]),
            vec![
                Arc::new(StringArray::from(vec!["a.log"])),
                Arc::new(TimestampNanosecondArray::from(vec![T])),
                Arc::new(Int64Array::from(vec![3])),
            ],
        )
        .expect("batch");

        let json = rendered(Format::Json, ResultKind::Samples, batch);
        let got: Value = serde_json::from_str(&json).expect("valid json");

        assert_eq!(
            got,
            json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [{
                        "metric": { "filename": "a.log" },
                        // Seconds as a number, the sample as a string.
                        "value": [1_795_773_840_i64, "3"],
                    }],
                },
            })
        );
    }

    /// Loki groups a stream's entries together, so two lines of one file are one
    /// stream with two values rather than two streams.
    #[test]
    fn logs_render_as_loki_streams() {
        let batch = RecordBatch::try_new(
            schema(vec![
                timestamp_field(),
                Field::new("line", DataType::Utf8View, false),
                Field::new("filename", DataType::Utf8, false),
            ]),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![T, T + 60_000_000_000])),
                Arc::new(StringViewArray::from(vec!["first", "second"])),
                Arc::new(StringArray::from(vec!["a.log", "a.log"])),
            ],
        )
        .expect("batch");

        let json = rendered(Format::Json, ResultKind::Logs, batch);
        let got: Value = serde_json::from_str(&json).expect("valid json");

        assert_eq!(
            got,
            json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [{
                        "stream": { "filename": "a.log" },
                        // Nanoseconds, as a string, and the line beside it.
                        "values": [
                            ["1795773840000000000", "first"],
                            ["1795773900000000000", "second"],
                        ],
                    }],
                },
            })
        );
    }

    /// An instant that is not a whole second keeps its fraction, as Prometheus
    /// renders one.
    #[test]
    fn a_fractional_instant_is_a_fraction() {
        assert_eq!(seconds(1_795_773_840_000_000_000), json!(1_795_773_840_i64));
        assert_eq!(seconds(1_795_773_840_500_000_000), json!(1_795_773_840.5));
    }

    #[test]
    fn a_table_is_rendered_for_reading() {
        let batch = RecordBatch::try_new(
            schema(vec![Field::new("line", DataType::Utf8View, false)]),
            vec![Arc::new(StringViewArray::from(vec!["hello"]))],
        )
        .expect("batch");

        let table = rendered(Format::Table, ResultKind::Logs, batch);
        assert!(table.contains("| line  |"), "got {table}");
        assert!(table.contains("| hello |"), "got {table}");
    }
}
