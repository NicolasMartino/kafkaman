#![cfg(all(feature = "axum", feature = "rdkafka"))]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::{Method, Request, StatusCode};
use kafkaman::axum::{resync_router, ResyncState};
use kafkaman::recovery::{Outcome, SourceProgress};
use kafkaman::sqlx::ResyncProgress;
use serde_json::{json, Value};
use tower::ServiceExt;

async fn call(app: axum::Router, method: Method, path: &str, body: Value) -> (StatusCode, Value) {
    let response = app
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap_or_else(|_| Request::new(Body::empty())),
        )
        .await
        .unwrap_or_else(|never| match never {});
    let status = response.status();
    let body = to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap_or_default();
    (status, serde_json::from_slice(&body).unwrap_or(Value::Null))
}

#[tokio::test]
async fn reports_progress_refuses_overlap_and_cancels() {
    let app = resync_router(ResyncState::new(|_, reporter| async move {
        reporter.record(Outcome {
            source: "profiles".to_owned(),
            progress: SourceProgress::Resync(ResyncProgress {
                enqueued: 2,
                skipped: 1,
                last_key: Some("profile-3".to_owned()),
            }),
        });
        std::future::pending::<()>().await;
        Ok(())
    }));
    let (status, started) = call(app.clone(), Method::POST, "/resync", json!({})).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/resync",
        json!({"start_after":"profile-1"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let id = started["run_id"].as_str().unwrap_or_default();
    let path = format!("/resync/{id}");
    let (status, _) = call(app.clone(), Method::POST, "/resync", json!({})).await;
    assert_eq!(status, StatusCode::CONFLICT);

    let progress = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let (_, current) = call(app.clone(), Method::GET, &path, Value::Null).await;
            if current["progress"]
                .as_array()
                .is_some_and(|rows| !rows.is_empty())
            {
                break current;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or(Value::Null);
    assert_eq!(progress["progress"][0]["enqueued"], 2);
    assert_eq!(progress["progress"][0]["skipped"], 1);
    assert_eq!(progress["progress"][0]["last_key"], "profile-3");

    let (status, cancelled) = call(app.clone(), Method::DELETE, &path, Value::Null).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(cancelled["state"], "cancelled");
    let (status, _) = call(app.clone(), Method::GET, &path, Value::Null).await;
    assert_eq!(status, StatusCode::OK);
    let (status, second) = call(app.clone(), Method::POST, "/resync", json!({})).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let second_path = format!("/resync/{}", second["run_id"].as_str().unwrap_or_default());
    call(app, Method::DELETE, &second_path, Value::Null).await;
}

#[tokio::test]
async fn passes_the_resume_cursor_and_reports_failure() {
    let seen = Arc::new(Mutex::new(None));
    let app = resync_router(ResyncState::new({
        let seen = Arc::clone(&seen);
        move |request, _| {
            let seen = Arc::clone(&seen);
            async move {
                *seen.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = request.start_after;
                Err("source failed".to_owned())
            }
        }
    }));
    let (status, started) = call(
        app.clone(),
        Method::POST,
        "/resync",
        json!({"source":"profiles", "start_after":"profile-42"}),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let path = format!("/resync/{}", started["run_id"].as_str().unwrap_or_default());
    let finished = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let (_, current) = call(app.clone(), Method::GET, &path, Value::Null).await;
            if current["state"] == "failed" {
                break current;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or(Value::Null);
    assert_eq!(finished["error"], "source failed");
    assert_eq!(
        seen.lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_deref(),
        Some("profile-42")
    );
}

#[tokio::test]
async fn reports_a_finished_run() {
    let app = resync_router(ResyncState::new(|_, reporter| async move {
        reporter.record(Outcome {
            source: "profiles".to_owned(),
            progress: SourceProgress::Resync(ResyncProgress {
                enqueued: 1,
                skipped: 0,
                last_key: Some("profile-1".to_owned()),
            }),
        });
        Ok(())
    }));
    let (status, started) = call(app.clone(), Method::POST, "/resync", json!({})).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let path = format!("/resync/{}", started["run_id"].as_str().unwrap_or_default());
    let finished = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let (_, current) = call(app.clone(), Method::GET, &path, Value::Null).await;
            if current["state"] == "finished" {
                break current;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or(Value::Null);
    assert_eq!(finished["progress"][0]["last_key"], "profile-1");
}
