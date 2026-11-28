//! Where lines come from.
//!
//! Every catalog presents the same three columns — `timestamp`, `line` and
//! `labels` — plus one column per stream label, so the planner resolves a label
//! the same way whatever it is reading. A catalog that is natively a table gets
//! there with a projection; one with its own query language does the translation
//! inside its `TableProvider`.
//!
//! Opening a catalog may list an object store or read file metadata, so it is
//! async. Planning is not: `plan()` builds a `LogicalPlan` and touches nothing.

use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::catalog::TableProvider;
use datafusion::common::{ScalarValue, TableReference};
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::provider_as_source;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{Expr as DfExpr, LogicalPlanBuilder, cast, col, lit};

use crate::table::{LogTable, is_label, labels_type};

/// The columns a catalog must supply itself. Everything else it exposes is a
/// stream label.
const TIMESTAMP: &str = "timestamp";
const LINE: &str = "line";
const LABELS: &str = "labels";

/// A resolved source: a name for `EXPLAIN` output and something to scan.
pub struct Catalog {
    /// Doubles as the table name, so a plan names the source the user gave —
    /// the same read as DuckDB's `SELECT * FROM '/var/log/*.log'`.
    name: String,
    kind: Kind,
}

enum Kind {
    /// Text log files from a path or glob, one line per row.
    Files(Arc<LogTable>),
    /// A Parquet dataset that already has `timestamp` and `line` columns.
    Parquet(Arc<ListingTable>),
}

impl Catalog {
    /// Text log files, which need nothing resolved.
    pub fn files(pattern: impl Into<String>) -> Self {
        let pattern = pattern.into();
        Self {
            kind: Kind::Files(Arc::new(LogTable::new(pattern.clone()))),
            name: pattern,
        }
    }

    /// Resolves `source` to a catalog.
    ///
    /// A URI selects a Parquet dataset and its storage — `file://` locally,
    /// `s3://` and friends once their object stores are registered. Anything
    /// else is a path or glob of text logs, which keeps the bare-path case
    /// working as it always has.
    pub async fn open(source: &str, state: &SessionState) -> Result<Self> {
        match source.split_once("://") {
            Some((scheme, _)) => Self::parquet(source, scheme, state).await,
            None => Ok(Self::files(source)),
        }
    }

    async fn parquet(source: &str, scheme: &str, state: &SessionState) -> Result<Self> {
        let url = ListingTableUrl::parse(source).map_err(|e| {
            DataFusionError::Plan(format!("{source:?} is not a usable {scheme} URL: {e}"))
        })?;

        let options = ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_file_extension(ParquetFormat::default().get_ext());
        let schema = options.infer_schema(state, &url).await?;

        let table = ListingTable::try_new(
            ListingTableConfig::new(url)
                .with_listing_options(options)
                .with_schema(schema),
        )?;

        Ok(Self {
            name: source.to_string(),
            kind: Kind::Parquet(Arc::new(table)),
        })
    }

    /// The bottom of every plan: a scan, plus whatever it takes to present the
    /// canonical columns.
    pub fn scan(&self) -> Result<LogicalPlanBuilder> {
        let reference = TableReference::bare(self.name.clone());

        match &self.kind {
            Kind::Files(table) => {
                let source = provider_as_source(Arc::clone(table) as Arc<_>);
                LogicalPlanBuilder::scan(reference, source, None)
            }
            Kind::Parquet(table) => {
                let mapping = parquet_mapping(table.schema().as_ref())?;
                let source = provider_as_source(Arc::clone(table) as Arc<_>);
                LogicalPlanBuilder::scan(reference, source, None)?.project(mapping)
            }
        }
    }
}

/// Maps a Parquet schema onto the canonical columns.
///
/// A projection rather than a rewrite inside the provider: the optimizer already
/// pushes filters *through* a projection, rewriting the column references on the
/// way, so predicate and projection pushdown into Parquet still happen.
///
/// `timestamp` and `line` have to exist. Everything else becomes a label, which
/// is the convention until a catalogs file can say otherwise.
fn parquet_mapping(schema: &datafusion::arrow::datatypes::Schema) -> Result<Vec<DfExpr>> {
    for required in [TIMESTAMP, LINE] {
        if schema.field_with_name(required).is_err() {
            return Err(DataFusionError::Plan(format!(
                "a Parquet source needs a {required:?} column; this one has {}",
                schema
                    .fields()
                    .iter()
                    .map(|field| field.name().as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
    }

    // Cast rather than require an exact type: Parquet keeps timestamps in
    // whatever unit it was written with, and a `line` is as likely to be `Utf8`
    // as `Utf8View`. Casting here means the rest of the engine — including
    // `logfmt_parse`, which takes `Utf8View` — sees one shape, and no cast
    // appears in the middle of the plan.
    let mut mapping = vec![
        cast(
            col(TIMESTAMP),
            crate::table::log_schema().field(0).data_type().clone(),
        )
        .alias(TIMESTAMP),
        cast(col(LINE), DataType::Utf8View).alias(LINE),
    ];

    // Nothing has extracted labels yet, so the column starts empty — exactly as
    // it does for a text file before a parser stage.
    mapping.push(lit(ScalarValue::try_from(&labels_type())?).alias(LABELS));

    mapping.extend(
        schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .filter(|name| is_label(name))
            .map(col),
    );

    Ok(mapping)
}
