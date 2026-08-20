use chrono::Utc;
use std::{fs::read_to_string, sync::Arc};

// This is a stub for a provider table provider in the future.
use datafusion::{
    arrow::{
        array::{
            ListBuilder, RecordBatch, StringBuilder, StructBuilder, TimestampNanosecondBuilder,
        },
        datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit},
    },
    error::{DataFusionError, Result},
};

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

    // TODO: support glob
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_source() {}
}
