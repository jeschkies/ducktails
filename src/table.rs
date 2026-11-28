use async_trait::async_trait;
use chrono::{DateTime, Utc};
use glob::glob;
use std::{fs::read_to_string, path::PathBuf, sync::Arc};

use datafusion::{
    arrow::{
        array::{
            ListBuilder, RecordBatch, StringBuilder, StringViewBuilder, StructBuilder,
            TimestampNanosecondBuilder,
        },
        buffer::Buffer,
        datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit},
    },
    catalog::{Session, TableProvider},
    datasource::TableType,
    error::{DataFusionError, Result},
    logical_expr::Expr,
    physical_plan::ExecutionPlan,
};
use datafusion_datasource::memory::MemorySourceConfig;

/// `Utf8View`, so a key or value can *point at* the line it was parsed out of
/// instead of being copied out of it — see `logfmt_parse`.
/// The columns every catalog gives a fixed meaning to. Anything else it exposes
/// is a stream label: `filename` for a file, `service`/`env` for a column-mapped
/// table, whatever a Loki catalog advertises.
///
/// `value` only appears in a metric result, but it is reserved everywhere so the
/// planner and the output layer agree on the same set.
pub const CANONICAL: [&str; 4] = ["timestamp", "line", "labels", "value"];

/// Whether a column of that name is a label rather than one of the fixed ones.
pub fn is_label(name: &str) -> bool {
    !CANONICAL.contains(&name)
}

pub fn label_fields() -> Fields {
    Fields::from(vec![
        Field::new("key", DataType::Utf8View, false),
        Field::new("value", DataType::Utf8View, true),
    ])
}

/// The type of the `labels` column: `List<Struct<key, value>>`, per DESIGN.md §1.
///
/// Shared so the schema and anything producing labels — `logfmt_parse`, later
/// `json_parse` — agree by construction. The inner field's name (`item`) and
/// nullability are part of the `DataType`, so a mismatch here is a runtime
/// schema error at `RecordBatch::try_new`, not a compile error.
pub fn labels_type() -> DataType {
    DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(label_fields()),
        true,
    )))
}

pub fn log_schema() -> SchemaRef {
    let schema = Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        // `Utf8View`, so a line is a 16-byte view into the file's own bytes
        // rather than a copy of them — see `read_source`.
        Field::new("line", DataType::Utf8View, false),
        Field::new("labels", labels_type(), true),
        Field::new("filename", DataType::Utf8, false),
    ]);

    // Wrap in Arc for DataFusion APIs
    Arc::new(schema)
}

/// Nanoseconds since the epoch from an RFC3339 timestamp at the *start* of
/// `line`, if there is one.
///
/// Only the start, and only RFC3339. lambda-promtail declares a regex and a Go
/// layout per log source rather than guessing at the format, which is the shape
/// a `--timestamp-format` flag should grow into; until then, guessing exactly one
/// well-specified format is the honest subset.
///
/// No timezone handling is needed: the result is an absolute instant, so an
/// offset in the text is already accounted for.
fn line_timestamp(line: &str) -> Option<i64> {
    // An RFC3339 timestamp contains no spaces, so the first field is all of it.
    let field = line.split([' ', '\t']).next()?;
    DateTime::parse_from_rfc3339(field)
        .ok()?
        .timestamp_nanos_opt()
}

pub fn read_source(path: &str) -> Result<RecordBatch> {
    let schema = log_schema();

    let mut ts_builder = TimestampNanosecondBuilder::new();
    let mut line_builder = StringViewBuilder::new();
    let mut labels_builder = ListBuilder::new(StructBuilder::from_fields(label_fields(), 0));
    let mut filename_builder = StringBuilder::new();

    let contents = read_to_string(path)
        .map_err(|e| DataFusionError::from(e).context(format!("reading {path}")))?;

    // A view's offset and length are `u32`, so one block cannot exceed 4 GiB.
    // Checked rather than left to `append_block`'s assert, which would abort the
    // process instead of failing the query.
    if contents.len() >= u32::MAX as usize {
        return Err(DataFusionError::Execution(format!(
            "{path} is {} bytes: a single file must stay under 4 GiB",
            contents.len()
        )));
    }

    // Locate the lines first: `Buffer::from_vec` takes ownership of the bytes, so
    // this is the last chance to look at them as a `str`. Subtracting the
    // pointers is sound because every line is a slice of this one allocation.
    //
    // The timestamp is read here for the same reason — it comes from the text,
    // not from the view.
    let ingested_at = Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let lines: Vec<(u32, u32, i64)> = contents
        .lines()
        .enumerate()
        .map(|(i, line)| {
            let offset = line.as_ptr() as usize - contents.as_ptr() as usize;
            // Ingest time when the line carries none, as lambda-promtail does.
            // The `+ i` keeps those lines in file order, which is the only order
            // they have.
            let timestamp = line_timestamp(line).unwrap_or(ingested_at + i as i64);
            (offset as u32, line.len() as u32, timestamp)
        })
        .collect();

    // The file's bytes become the single block every `line` view points into, so
    // the contents live in memory once rather than once per line. The block index
    // comes back from `append_block` — it is not necessarily 0.
    let block = line_builder.append_block(Buffer::from_vec(contents.into_bytes()));

    for (offset, len, timestamp) in &lines {
        ts_builder.append_value(*timestamp);
        line_builder.try_append_view(block, *offset, *len)?;
        labels_builder.append_null();
        filename_builder.append_value(path); // TODO: can save memory here
    }

    // Finish array construction and create batch
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(ts_builder.finish()),
            Arc::new(line_builder.finish()),
            Arc::new(labels_builder.finish()),
            Arc::new(filename_builder.finish()),
        ],
    )?)
}

/// A DataFusion table backed by log files matched from a shell-style glob.
///
/// Each `scan` re-resolves the glob, so files that appear between queries show
/// up on the next one — matching how log rotation actually works.
#[derive(Debug)]
pub struct LogTable {
    schema: SchemaRef,
    pattern: String,
}

impl LogTable {
    pub fn new(pattern: impl Into<String>) -> Self {
        Self {
            schema: log_schema(),
            pattern: pattern.into(),
        }
    }

    fn resolve_files(&self) -> Result<Vec<PathBuf>> {
        let mut paths = glob(&self.pattern)
            .map_err(|e| {
                DataFusionError::Execution(format!("invalid glob {:?}: {e}", self.pattern))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| {
                DataFusionError::from(std::io::Error::from(e))
                    .context(format!("resolving {}", self.pattern))
            })?;

        if paths.is_empty() {
            return Err(DataFusionError::Execution(format!(
                "no files match {:?}",
                self.pattern
            )));
        }

        paths.sort();
        Ok(paths)
    }
}

#[async_trait]
impl TableProvider for LogTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let files = self.resolve_files()?;
        let batches = files
            .iter()
            .map(|p| read_source(&p.to_string_lossy()))
            .collect::<Result<Vec<_>>>()?;

        Ok(MemorySourceConfig::try_new_exec(
            &[batches],
            self.schema(),
            projection.cloned(),
        )?)
    }
}
