//! The Loki HTTP API subset, exercised through the router.
//!
//! `oneshot` rather than a listener and a client: the routing, parameter parsing
//! and response shapes are what matter, and a socket adds nothing but ports to
//! clash over.

mod common;

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use common::metric_log_files;
use datafusion::prelude::SessionContext;
use ducktails::catalog::Catalog;
use ducktails::server::{Api, router};
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

/// Sends one request and returns the status and the body.
///
/// The body comes back as a `Value` where it is JSON and as a string otherwise —
/// an error body is plain text, as Loki's is.
async fn request(uri: &str) -> (StatusCode, Result<Value, String>) {
    let (_dir, pattern) = metric_log_files();
    let ctx = SessionContext::new();
    let catalog = Catalog::files(&pattern);
    let app = router(Arc::new(Api::new(ctx, catalog)));

    let response = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let text = String::from_utf8_lossy(&bytes).into_owned();

    (status, serde_json::from_str(&text).map_err(|_| text))
}

#[tokio::test]
async fn ready_and_buildinfo_answer() {
    let (status, body) = request("/ready").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.unwrap_err().trim(), "ready");

    // Grafana gates features on the version, so it has to look like Loki's.
    let (status, body) = request("/loki/api/v1/status/buildinfo").await;
    assert_eq!(status, StatusCode::OK);
    let body = body.expect("json");
    assert!(body["version"].is_string(), "got {body}");
    assert!(
        body["revision"].as_str().unwrap().contains("ducktails"),
        "got {body}"
    );
}

/// The label browser asks for names first, then values. `filename` is the one
/// label a file catalog has, and its values are the files the glob matched.
#[tokio::test]
async fn the_label_endpoints_answer() {
    let (status, body) = request("/loki/api/v1/labels").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.expect("json")["data"], serde_json::json!(["filename"]));

    let (status, body) = request("/loki/api/v1/label/filename/values").await;
    assert_eq!(status, StatusCode::OK);
    let values = body.expect("json");
    let values = values["data"].as_array().expect("array");
    assert_eq!(values.len(), 2, "two files matched the glob: {values:?}");
    assert!(
        values[0].as_str().unwrap().ends_with("a.log"),
        "sorted, so a.log first: {values:?}"
    );

    // A label the catalog does not have is the caller's mistake.
    let (status, _) = request("/loki/api/v1/label/nope/values").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// A log query over a range: Loki's `streams`, newest first, cut to the limit.
#[tokio::test]
async fn query_range_answers_a_log_query() {
    let (status, body) = request(
        "/loki/api/v1/query_range?query=%7B%7D%20%7C%3D%20%22error%22\
         &start=2026-11-27T09:00:00Z&end=2026-11-27T10:05:00Z&limit=2",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let body = body.expect("json");
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"]["resultType"], "streams");

    // Two files hold error lines, so two streams; the limit cut the third line.
    let streams = body["data"]["result"].as_array().expect("array");
    let entries: usize = streams
        .iter()
        .map(|stream| stream["values"].as_array().expect("values").len())
        .sum();
    assert_eq!(entries, 2, "limit=2: {body}");
}

/// An instant metric query is answered; a *ranged* one is not, and says so
/// rather than returning something wrong.
#[tokio::test]
async fn metric_queries_over_an_instant_and_a_range() {
    let (status, body) = request(
        "/loki/api/v1/query?query=count_over_time(%7B%7D%5B5m%5D)&time=2026-11-27T10:04:00Z",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let body = body.expect("json");
    assert_eq!(body["data"]["resultType"], "vector");
    assert_eq!(body["data"]["result"].as_array().expect("array").len(), 2);

    let (status, message) = request(
        "/loki/api/v1/query_range?query=count_over_time(%7B%7D%5B5m%5D)\
         &start=2026-11-27T09:00:00Z&end=2026-11-27T10:05:00Z&step=60",
    )
    .await;

    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    assert!(message.unwrap_err().contains("step"), "should say why");
}

/// A malformed query is a 400 with the parser's own message.
#[tokio::test]
async fn a_bad_query_is_a_bad_request() {
    let (status, message) = request("/loki/api/v1/query_range?query=%7Bfoo%3D%7D").await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        message.unwrap_err().contains("Unexpected token"),
        "should quote the parser"
    );
}
