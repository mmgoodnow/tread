use axum::{
    Form, Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde_json::{Value, json};
use sqlx::SqlitePool;

use crate::{
    clients::webhook::{
        arr_event, generic_media_event, overseerr_request_from_payload, rtorrent_event,
        rtorrent_payload_from_form,
    },
    core::model::EventSource,
    db::{ingest_event, upsert_media_request},
    telemetry,
};

#[derive(Clone)]
pub struct AppState {
    pub pool: SqlitePool,
}

pub fn router(pool: SqlitePool) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics))
        .route("/webhooks/overseerr", post(overseerr_webhook))
        .route("/webhooks/sonarr", post(sonarr_webhook))
        .route("/webhooks/radarr", post(radarr_webhook))
        .route("/webhooks/tautulli", post(tautulli_webhook))
        .route("/webhooks/rtorrent", post(rtorrent_webhook))
        .with_state(AppState { pool })
}

async fn healthz() -> impl IntoResponse {
    Json(json!({ "ok": true }))
}

async fn metrics(State(state): State<AppState>) -> Result<Response, ApiError> {
    let body = telemetry::render_metrics(&state.pool).await?;
    Ok((
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
        .into_response())
}

async fn overseerr_webhook(
    State(state): State<AppState>,
    Json(payload): Json<Value>,
) -> Result<impl IntoResponse, ApiError> {
    if let Some(request) = overseerr_request_from_payload(&payload) {
        let id = upsert_media_request(&state.pool, request).await?;
        let event = generic_media_event(EventSource::Overseerr, "request_submitted", payload);
        ingest_event(&state.pool, event).await?;
        return Ok((
            StatusCode::ACCEPTED,
            Json(json!({ "media_request_id": id })),
        ));
    }

    let event = generic_media_event(EventSource::Overseerr, "notification", payload);
    let outcome = ingest_event(&state.pool, event).await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({ "media_request_id": outcome.map(|value| value.media_request_id) })),
    ))
}

async fn sonarr_webhook(
    State(state): State<AppState>,
    Json(payload): Json<Value>,
) -> Result<impl IntoResponse, ApiError> {
    let outcome = ingest_event(&state.pool, arr_event(EventSource::Sonarr, payload)).await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({ "media_request_id": outcome.map(|value| value.media_request_id) })),
    ))
}

async fn radarr_webhook(
    State(state): State<AppState>,
    Json(payload): Json<Value>,
) -> Result<impl IntoResponse, ApiError> {
    let outcome = ingest_event(&state.pool, arr_event(EventSource::Radarr, payload)).await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({ "media_request_id": outcome.map(|value| value.media_request_id) })),
    ))
}

async fn tautulli_webhook(
    State(state): State<AppState>,
    Json(payload): Json<Value>,
) -> Result<impl IntoResponse, ApiError> {
    let outcome = ingest_event(
        &state.pool,
        generic_media_event(EventSource::Tautulli, "recently_added", payload),
    )
    .await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({ "media_request_id": outcome.map(|value| value.media_request_id) })),
    ))
}

async fn rtorrent_webhook(
    State(state): State<AppState>,
    body: RtorrentBody,
) -> Result<impl IntoResponse, ApiError> {
    let outcome = ingest_event(&state.pool, rtorrent_event(body.into_value())).await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({ "media_request_id": outcome.map(|value| value.media_request_id) })),
    ))
}

enum RtorrentBody {
    Json(Value),
    Form(std::collections::HashMap<String, String>),
}

impl axum::extract::FromRequest<AppState> for RtorrentBody {
    type Rejection = Response;

    async fn from_request(
        req: axum::extract::Request,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let content_type = req
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string();

        if content_type.starts_with("application/x-www-form-urlencoded") {
            let Form(form) =
                Form::<std::collections::HashMap<String, String>>::from_request(req, state)
                    .await
                    .map_err(IntoResponse::into_response)?;
            return Ok(Self::Form(form));
        }

        let Json(payload) = Json::<Value>::from_request(req, state)
            .await
            .map_err(IntoResponse::into_response)?;
        Ok(Self::Json(payload))
    }
}

impl RtorrentBody {
    fn into_value(self) -> Value {
        match self {
            Self::Json(payload) => payload,
            Self::Form(form) => rtorrent_payload_from_form(form),
        }
    }
}

#[derive(Debug)]
pub struct ApiError(anyhow::Error);

impl<E> From<E> for ApiError
where
    E: Into<anyhow::Error>,
{
    fn from(error: E) -> Self {
        Self(error.into())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        tracing::error!(error = ?self.0, "request failed");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "internal server error" })),
        )
            .into_response()
    }
}
