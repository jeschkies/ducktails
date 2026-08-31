use async_trait::async_trait;
use chrono::Utc;
use glob::glob;
use std::{fs::read_to_string, path::PathBuf, sync::Arc};

use datafusion::{
    arrow::{
        array::{
            ListBuilder, RecordBatch, StringBuilder, StructBuilder, TimestampNanosecondBuilder,
        },
        datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit},
    },
    catalog::{Session, TableProvider},
    datasource::TableType,
    error::{DataFusionError, Result},
    logical_expr::Expr,
    physical_plan::ExecutionPlan,
};
use datafusion_datasource::memory::MemorySourceConfig;

fn label_fields() -> Fields {
    Fields::from(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, true),
    ])
}

pub fn log_schema() -> SchemaRef {
    let labels_pair_struct = DataType::Struct(label_fields());

    let schema = Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("line", DataType::Utf8, false),
        Field::new(
            "labels",
            DataType::List(Arc::new(Field::new("item", labels_pair_struct, true))),
            true,
        ),
        Field::new("filename", DataType::Utf8, false),
    ]);

    // Wrap in Arc for DataFusion APIs
    Arc::new(schema)
}

pub fn read_source(path: &str) -> Result<RecordBatch> {
    let schema = log_schema();

    let mut ts_builder = TimestampNanosecondBuilder::new();
    let mut line_builder = StringBuilder::new();
    let mut labels_builder = ListBuilder::new(StructBuilder::from_fields(label_fields(), 0));
    let mut filename_builder = StringBuilder::new();

    let ingested_at = Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let contents = read_to_string(path)
        .map_err(|e| DataFusionError::from(e).context(format!("reading {path}")))?;
    for (i, line) in contents.lines().enumerate() {
        ts_builder.append_value(ingested_at + i as i64); // TODO: infer ts 
        line_builder.append_value(line);
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
