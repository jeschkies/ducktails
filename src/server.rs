//! A subset of Loki's HTTP API — enough for Grafana's Loki datasource.
//!
//! Only what Grafana needs to browse logs: readiness, the build info it gates
//! features on, the label endpoints behind the label browser, and the two query
//! endpoints. Live tailing and the Logs Drilldown endpoints are out of scope.
//!
//! The response bodies come from `output`, which already speaks Loki's shapes,
//! so this module is routing, parameter parsing and error mapping.

use std::sync::Arc;

use axum::Router;
use axum::extract::{Form, Path, Query as UrlQuery, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use chrono::{DateTime, TimeZone, Utc};
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::catalog::Catalog;
use crate::output::{ResultKind, json_result};
use crate::parser::{Expr, Parser};
use crate::plan::{label_names, plan, plan_label_values};
use crate::query::{Direction, Query};

/// Everything a request needs: one catalog, opened once, and one session.
///
/// Both are shared rather than per-request — `SessionContext` is built for
/// concurrent use, and re-opening a catalog would re-list the storage behind it
/// on every call.
pub struct Api {
    catalog: Catalog,
    ctx: SessionContext,
}

impl Api {
    pub fn new(ctx: SessionContext, catalog: Catalog) -> Self {
        Self { catalog, ctx }
    }
}

/// Serves until the process ends.
pub async fn serve(listen: &str, api: Api) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .map_err(|e| DataFusionError::IoError(e).context(format!("listening on {listen}")))?;

    eprintln!("ducktails: listening on {listen}");

    axum::serve(listener, router(Arc::new(api)))
        .await
        .map_err(|e| DataFusionError::IoError(e).context("serving"))
}

/// The routes, separate from the listener so a test can call them directly.
pub fn router(api: Arc<Api>) -> Router {
    Router::new()
        .route("/ready", get(ready))
        .route("/loki/api/v1/status/buildinfo", get(buildinfo))
        .route("/loki/api/v1/labels", get(labels))
        .route("/loki/api/v1/label/{name}/values", get(label_values))
        // Grafana sends queries as GET or, when they are long, as a form POST.
        .route("/loki/api/v1/query", get(query_get).post(query_post))
        .route(
            "/loki/api/v1/query_range",
            get(query_range_get).post(query_range_post),
        )
        .with_state(api)
}

/// A failed request, rendered as Loki renders one: a status and a plain-text
/// body, which is what Grafana surfaces to the user.
struct ApiError(StatusCode, String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, self.1).into_response()
    }
}

impl ApiError {
    fn bad_request(message: impl std::fmt::Display) -> Self {
        Self(StatusCode::BAD_REQUEST, message.to_string())
    }
}

impl From<DataFusionError> for ApiError {
    fn from(e: DataFusionError) -> Self {
        // A plan error is the caller's fault — an unknown label, a filter that
        // cannot be ordered — while the rest is ours.
        let status = match e {
            DataFusionError::Plan(_) | DataFusionError::SchemaError(_, _) => {
                StatusCode::BAD_REQUEST
            }
            DataFusionError::NotImplemented(_) => StatusCode::NOT_IMPLEMENTED,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Self(status, e.to_string())
    }
}

impl From<datafusion::arrow::error::ArrowError> for ApiError {
    fn from(e: datafusion::arrow::error::ArrowError) -> Self {
        Self(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
    }
}

type ApiResult = std::result::Result<axum::Json<Value>, ApiError>;

async fn ready() -> &'static str {
    "ready\n"
}

/// Grafana reads this to decide which features to enable, so it reports a Loki
/// version. The other fields say what is actually answering.
async fn buildinfo() -> axum::Json<Value> {
    axum::Json(json!({
        "version": "3.0.0",
        "revision": concat!("ducktails ", env!("CARGO_PKG_VERSION")),
        "branch": "ducktails",
        "buildUser": "ducktails",
        "buildDate": "",
        "goVersion": "",
    }))
}

/// Parameters shared by the label endpoints: a window, which we honour, and
/// `query`, a selector to narrow the answer by, which we do not yet.
#[derive(Debug, Deserialize)]
struct LabelParams {
    start: Option<String>,
    end: Option<String>,
}

async fn labels(
    State(api): State<Arc<Api>>,
    UrlQuery(_params): UrlQuery<LabelParams>,
) -> ApiResult {
    let names = label_names(&api.catalog)?;
    Ok(axum::Json(json!({ "status": "success", "data": names })))
}

async fn label_values(
    State(api): State<Arc<Api>>,
    Path(name): Path<String>,
    UrlQuery(params): UrlQuery<LabelParams>,
) -> ApiResult {
    let start = optional_instant(params.start.as_deref())?;
    let end = optional_instant(params.end.as_deref())?;

    let batches = api
        .ctx
        .execute_logical_plan(plan_label_values(&api.catalog, &name, start, end)?)
        .await?
        .collect()
        .await?;

    // One column of whatever type the label is, rendered as Loki renders every
    // label value: a string.
    let options = FormatOptions::default();
    let mut values = Vec::new();
    for batch in &batches {
        let column = ArrayFormatter::try_new(batch.column(0).as_ref(), &options)?;
        for row in 0..batch.num_rows() {
            values.push(column.value(row).to_string());
        }
    }

    Ok(axum::Json(json!({ "status": "success", "data": values })))
}

/// `/query`: an instant query, `time` being the instant.
#[derive(Debug, Deserialize)]
struct InstantParams {
    query: String,
    time: Option<String>,
    limit: Option<usize>,
    direction: Option<String>,
}

/// `/query_range`: a window, and a `step` that only matters once metric queries
/// are answered over a grid of instants.
#[derive(Debug, Deserialize)]
struct RangeParams {
    query: String,
    start: Option<String>,
    end: Option<String>,
    #[allow(dead_code)]
    step: Option<String>,
    limit: Option<usize>,
    direction: Option<String>,
}

async fn query_get(state: State<Arc<Api>>, UrlQuery(params): UrlQuery<InstantParams>) -> ApiResult {
    instant_query(state, params).await
}

async fn query_post(state: State<Arc<Api>>, Form(params): Form<InstantParams>) -> ApiResult {
    instant_query(state, params).await
}

async fn query_range_get(
    state: State<Arc<Api>>,
    UrlQuery(params): UrlQuery<RangeParams>,
) -> ApiResult {
    range_query(state, params).await
}

async fn query_range_post(state: State<Arc<Api>>, Form(params): Form<RangeParams>) -> ApiResult {
    range_query(state, params).await
}

async fn instant_query(State(api): State<Arc<Api>>, params: InstantParams) -> ApiResult {
    let expr = parse(&params.query)?;

    // Loki defaults `time` to now, and so does this: a server is asked about a
    // moment, where the CLI is pointed at a file whose newest line is the useful
    // default.
    let at = optional_instant(params.time.as_deref())?.unwrap_or_else(Utc::now);

    let query = Query::new(expr)
        .with_window(None, Some(at))
        .with_limit(params.limit)
        .with_direction(direction(params.direction.as_deref())?);

    answer(&api, query).await
}

async fn range_query(State(api): State<Arc<Api>>, params: RangeParams) -> ApiResult {
    let expr = parse(&params.query)?;

    if matches!(expr, Expr::Metric(_)) {
        return Err(ApiError(
            StatusCode::NOT_IMPLEMENTED,
            "a metric query over a range needs a sample per step, which is not built yet; \
             use /loki/api/v1/query for a single instant"
                .to_string(),
        ));
    }

    // Loki's defaults: the last hour, ending now.
    let end = optional_instant(params.end.as_deref())?.unwrap_or_else(Utc::now);
    let start = optional_instant(params.start.as_deref())?
        .unwrap_or_else(|| end - chrono::Duration::hours(1));

    let query = Query::new(expr)
        .with_window(Some(start), Some(end))
        .with_limit(Some(params.limit.unwrap_or(100)))
        .with_direction(direction(params.direction.as_deref())?);

    answer(&api, query).await
}

async fn answer(api: &Api, query: Query) -> ApiResult {
    let kind = match query.expr {
        Expr::Log(_) => ResultKind::Logs,
        Expr::Metric(_) => ResultKind::Samples,
    };

    let batches = api
        .ctx
        .execute_logical_plan(plan(&api.catalog, &query)?)
        .await?
        .collect()
        .await?;

    Ok(axum::Json(json_result(kind, &batches)?))
}

fn parse(query: &str) -> std::result::Result<Expr, ApiError> {
    Parser::parse(query).map_err(ApiError::bad_request)
}

fn direction(value: Option<&str>) -> std::result::Result<Direction, ApiError> {
    match value {
        None | Some("backward") => Ok(Direction::Backward),
        Some("forward") => Ok(Direction::Forward),
        Some(other) => Err(ApiError::bad_request(format!(
            "direction {other:?}: expected \"forward\" or \"backward\""
        ))),
    }
}

fn optional_instant(value: Option<&str>) -> std::result::Result<Option<DateTime<Utc>>, ApiError> {
    value
        .filter(|text| !text.is_empty())
        .map(instant)
        .transpose()
}

/// Loki's timestamp rule, which Grafana relies on: a bare integer is
/// nanoseconds, a decimal is seconds, and anything else is RFC3339.
fn instant(value: &str) -> std::result::Result<DateTime<Utc>, ApiError> {
    if let Ok(nanos) = value.parse::<i64>() {
        return Ok(DateTime::from_timestamp_nanos(nanos));
    }

    if let Ok(seconds) = value.parse::<f64>() {
        let nanos = (seconds * 1e9).round() as i64;
        return Ok(DateTime::from_timestamp_nanos(nanos));
    }

    value
        .parse::<DateTime<Utc>>()
        .or_else(|_| {
            // A date with no offset is not RFC3339, but Loki accepts it.
            chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S")
                .map(|naive| Utc.from_utc_datetime(&naive))
        })
        .map_err(|e| ApiError::bad_request(format!("time {value:?}: {e}")))
}
