//! Writes a small Parquet file, for trying the Parquet catalog by hand.
//!
//! `cargo run --example write_parquet -- /tmp/pq/logs.parquet`

use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, StringArray, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::parquet::arrow::ArrowWriter;

fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("usage: write_parquet <path>");

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("line", DataType::Utf8, false),
        Field::new("service", DataType::Utf8, false),
    ]));

    // 2026-11-27T10:00:00Z onwards, a minute apart.
    let start = 1_795_773_600_000_000;
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(TimestampMicrosecondArray::from(vec![
                start,
                start + 60_000_000,
                start + 120_000_000,
            ])),
            Arc::new(StringArray::from(vec![
                "level=info msg=started",
                "level=error msg=\"went wrong\" code=500",
                "level=info msg=done",
            ])),
            Arc::new(StringArray::from(vec!["api", "api", "worker"])),
        ],
    )
    .expect("batch");

    let file = std::fs::File::create(&path).expect("create");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
    writer.write(&batch).expect("write");
    writer.close().expect("close");
    println!("wrote {path}");
}
