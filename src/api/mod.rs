use axum::{
    Form, Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{Row, SqlitePool};

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
        .route("/api/software-delay/recent", get(recent_software_delay))
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

#[derive(Debug, Deserialize)]
pub struct RecentSoftwareDelayQuery {
    #[serde(default = "default_recent_limit")]
    pub limit: i64,
    #[serde(default)]
    pub measurable_only: bool,
}

#[derive(Debug, Serialize)]
pub struct RecentSoftwareDelayRow {
    pub item_id: i64,
    pub media_request_id: i64,
    pub overseerr_request_id: Option<i64>,
    pub media_type: String,
    pub title: String,
    pub display_title: String,
    pub season_number: Option<i64>,
    pub episode_number: Option<i64>,
    pub requested_at: String,
    pub download_finished_at: Option<String>,
    pub arr_imported_at: Option<String>,
    pub plex_available_at: Option<String>,
    pub notification_sent_at: Option<String>,
    pub download_finished_to_arr_import_seconds: Option<f64>,
    pub arr_import_to_plex_available_seconds: Option<f64>,
    pub plex_available_to_notification_seconds: Option<f64>,
    pub known_software_delay_seconds: f64,
    pub total_software_delay_seconds: f64,
    pub observed_stage_count: i64,
    pub expected_stage_count: i64,
    pub lifecycle_complete: bool,
    pub missing_stages: Vec<&'static str>,
}

fn default_recent_limit() -> i64 {
    25
}

async fn recent_software_delay(
    State(state): State<AppState>,
    Query(query): Query<RecentSoftwareDelayQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let limit = query.limit.clamp(1, 200);
    let rows =
        recent_software_delay_rows_with_options(&state.pool, limit, query.measurable_only).await?;
    Ok(Json(json!({ "rows": rows })))
}

pub async fn recent_software_delay_rows(
    pool: &SqlitePool,
    limit: i64,
) -> anyhow::Result<Vec<RecentSoftwareDelayRow>> {
    recent_software_delay_rows_with_options(pool, limit, false).await
}

pub async fn recent_software_delay_rows_with_options(
    pool: &SqlitePool,
    limit: i64,
    measurable_only: bool,
) -> anyhow::Result<Vec<RecentSoftwareDelayRow>> {
    let rows = sqlx::query(
        r#"
        WITH lifecycle AS (
            SELECT
                mri.id AS item_id,
                mr.id AS media_request_id,
                mr.overseerr_request_id,
                mri.media_type,
                COALESCE(mri.title, mr.title) AS title,
                mri.season_number,
                mri.episode_number,
                mri.requested_at,
                mri.download_finished_at,
                COALESCE(mri.radarr_imported_at, mri.sonarr_imported_at) AS arr_imported_at,
                mri.plex_available_at,
                mri.overseerr_notification_sent_at AS notification_sent_at
            FROM media_request_items mri
            JOIN media_requests mr ON mr.id = mri.media_request_id
            WHERE (
                mri.download_finished_at IS NOT NULL
                OR mri.radarr_imported_at IS NOT NULL
                OR mri.sonarr_imported_at IS NOT NULL
                OR mri.plex_available_at IS NOT NULL
                OR mri.overseerr_notification_sent_at IS NOT NULL
            )
              AND NOT (
                mri.season_number IS NULL
                AND mri.episode_number IS NULL
                AND EXISTS (
                    SELECT 1
                    FROM media_request_items child
                    WHERE child.media_request_id = mri.media_request_id
                      AND (
                        child.season_number IS NOT NULL
                        OR child.episode_number IS NOT NULL
                      )
                      AND (
                        child.download_finished_at IS NOT NULL
                        OR child.radarr_imported_at IS NOT NULL
                        OR child.sonarr_imported_at IS NOT NULL
                        OR child.plex_available_at IS NOT NULL
                        OR child.overseerr_notification_sent_at IS NOT NULL
                      )
                )
              )
        ),
        measured AS (
            SELECT
            *,
            CASE
                WHEN download_finished_at IS NOT NULL
                 AND arr_imported_at IS NOT NULL
                 AND julianday(arr_imported_at) >= julianday(download_finished_at)
                THEN (julianday(arr_imported_at) - julianday(download_finished_at)) * 86400.0
            END AS download_finished_to_arr_import_seconds,
            CASE
                WHEN arr_imported_at IS NOT NULL
                 AND plex_available_at IS NOT NULL
                 AND ((julianday(plex_available_at) - julianday(arr_imported_at)) * 86400.0) >= -1.0
                THEN (julianday(plex_available_at) - julianday(arr_imported_at)) * 86400.0
            END AS arr_import_to_plex_available_seconds,
            CASE
                WHEN plex_available_at IS NOT NULL
                 AND notification_sent_at IS NOT NULL
                 AND julianday(notification_sent_at) >= julianday(plex_available_at)
                THEN (julianday(notification_sent_at) - julianday(plex_available_at)) * 86400.0
            END AS plex_available_to_notification_seconds
            FROM lifecycle
        )
        SELECT *
        FROM measured
        WHERE ? = 0
           OR download_finished_to_arr_import_seconds IS NOT NULL
           OR arr_import_to_plex_available_seconds IS NOT NULL
           OR plex_available_to_notification_seconds IS NOT NULL
        ORDER BY COALESCE(notification_sent_at, plex_available_at, arr_imported_at, download_finished_at, requested_at) DESC
        LIMIT ?
        "#,
    )
    .bind(if measurable_only { 1_i64 } else { 0_i64 })
    .bind(limit.clamp(1, 200))
    .fetch_all(pool)
    .await?;

    let rows = rows
        .into_iter()
        .map(|row| {
            let download_finished_to_arr_import_seconds = row
                .get::<Option<f64>, _>("download_finished_to_arr_import_seconds")
                .map(clean_seconds);
            let arr_import_to_plex_available_seconds = row
                .get::<Option<f64>, _>("arr_import_to_plex_available_seconds")
                .map(clean_seconds);
            let plex_available_to_notification_seconds = row
                .get::<Option<f64>, _>("plex_available_to_notification_seconds")
                .map(clean_seconds);
            let total_software_delay_seconds = [
                download_finished_to_arr_import_seconds,
                arr_import_to_plex_available_seconds,
                plex_available_to_notification_seconds,
            ]
            .into_iter()
            .flatten()
            .sum();
            let download_finished_at: Option<String> = row.get("download_finished_at");
            let arr_imported_at: Option<String> = row.get("arr_imported_at");
            let plex_available_at: Option<String> = row.get("plex_available_at");
            let notification_sent_at: Option<String> = row.get("notification_sent_at");
            let stages = [
                ("download_finished", download_finished_at.is_some()),
                ("arr_imported", arr_imported_at.is_some()),
                ("plex_available", plex_available_at.is_some()),
                ("notification_sent", notification_sent_at.is_some()),
            ];
            let expected_stage_count = stages.len() as i64;
            let missing_stages = stages
                .into_iter()
                .filter_map(|(stage, present)| (!present).then_some(stage))
                .collect::<Vec<_>>();
            let observed_stage_count = expected_stage_count - missing_stages.len() as i64;
            let title: String = row.get("title");
            let season_number = row.get("season_number");
            let episode_number = row.get("episode_number");

            RecentSoftwareDelayRow {
                item_id: row.get("item_id"),
                media_request_id: row.get("media_request_id"),
                overseerr_request_id: row.get("overseerr_request_id"),
                media_type: row.get("media_type"),
                display_title: software_delay_display_title(&title, season_number, episode_number),
                title,
                season_number,
                episode_number,
                requested_at: row.get("requested_at"),
                download_finished_at,
                arr_imported_at,
                plex_available_at,
                notification_sent_at,
                download_finished_to_arr_import_seconds,
                arr_import_to_plex_available_seconds,
                plex_available_to_notification_seconds,
                known_software_delay_seconds: total_software_delay_seconds,
                total_software_delay_seconds,
                observed_stage_count,
                expected_stage_count,
                lifecycle_complete: missing_stages.is_empty(),
                missing_stages,
            }
        })
        .collect();

    Ok(rows)
}

fn software_delay_display_title(
    title: &str,
    season_number: Option<i64>,
    episode_number: Option<i64>,
) -> String {
    match (season_number, episode_number) {
        (Some(season), Some(episode)) => format!("{title} S{season:02}E{episode:02}"),
        (Some(season), None) => format!("{title} S{season:02}"),
        _ => title.to_string(),
    }
}

fn clean_seconds(value: f64) -> f64 {
    value.max(0.0).round()
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
