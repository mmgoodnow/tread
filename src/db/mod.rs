use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{Row, SqlitePool, sqlite::SqlitePoolOptions};

use crate::core::{
    correlation,
    model::{EventIngest, IncomingRequest, MatchOutcome, MediaIdentity, MediaType},
};

pub async fn connect(database_url: &str) -> anyhow::Result<SqlitePool> {
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await?;
    sqlx::migrate!("./migrations").run(&pool).await?;
    Ok(pool)
}

pub async fn upsert_media_request(
    pool: &SqlitePool,
    request: IncomingRequest,
) -> anyhow::Result<i64> {
    let media_type = request.identity.media_type.as_str();
    let requested_at = request.requested_at.to_rfc3339();

    let row = sqlx::query(
        r#"
        INSERT INTO media_requests (
            overseerr_request_id, media_type, tmdb_id, tvdb_id, imdb_id, title, year,
            season_number, episode_number, requested_by, requested_at, match_confidence, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1.0, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
        ON CONFLICT(overseerr_request_id) DO UPDATE SET
            media_type = excluded.media_type,
            tmdb_id = COALESCE(excluded.tmdb_id, media_requests.tmdb_id),
            tvdb_id = COALESCE(excluded.tvdb_id, media_requests.tvdb_id),
            imdb_id = COALESCE(excluded.imdb_id, media_requests.imdb_id),
            title = excluded.title,
            year = COALESCE(excluded.year, media_requests.year),
            season_number = COALESCE(excluded.season_number, media_requests.season_number),
            episode_number = COALESCE(excluded.episode_number, media_requests.episode_number),
            requested_by = COALESCE(excluded.requested_by, media_requests.requested_by),
            requested_at = excluded.requested_at,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        RETURNING id
        "#,
    )
    .bind(request.overseerr_request_id)
    .bind(media_type)
    .bind(request.identity.tmdb_id)
    .bind(request.identity.tvdb_id)
    .bind(request.identity.imdb_id)
    .bind(request.title)
    .bind(request.identity.year)
    .bind(request.identity.season_number)
    .bind(request.identity.episode_number)
    .bind(request.requested_by)
    .bind(requested_at)
    .fetch_one(pool)
    .await?;

    Ok(row.get("id"))
}

pub async fn ingest_event(
    pool: &SqlitePool,
    event: EventIngest,
) -> anyhow::Result<Option<MatchOutcome>> {
    let match_outcome = match &event.identity {
        Some(identity) => find_match(pool, identity).await?,
        None => None,
    };

    if let Some(outcome) = match_outcome {
        apply_lifecycle_timestamp(pool, outcome.media_request_id, &event, outcome.confidence)
            .await?;
    }

    let payload = serde_json::to_string(&event.payload_json)?;
    let observed_at = event.observed_at.to_rfc3339();

    sqlx::query(
        r#"
        INSERT INTO events (source, event_type, media_request_id, external_id, payload_json, observed_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(source, event_type, external_id) DO UPDATE SET
            media_request_id = COALESCE(excluded.media_request_id, events.media_request_id),
            payload_json = excluded.payload_json,
            observed_at = excluded.observed_at
        "#,
    )
    .bind(event.source.as_str())
    .bind(&event.event_type)
    .bind(match_outcome.map(|outcome| outcome.media_request_id))
    .bind(&event.external_id)
    .bind(payload)
    .bind(observed_at)
    .execute(pool)
    .await?;

    Ok(match_outcome)
}

async fn apply_lifecycle_timestamp(
    pool: &SqlitePool,
    media_request_id: i64,
    event: &EventIngest,
    confidence: f64,
) -> anyhow::Result<()> {
    let observed_at = event.observed_at.to_rfc3339();
    let stage = lifecycle_column(event.source.as_str(), &event.event_type);

    if let Some(column) = stage {
        let sql = format!(
            "UPDATE media_requests SET {column} = COALESCE({column}, ?), match_confidence = MAX(match_confidence, ?), updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?"
        );
        sqlx::query(&sql)
            .bind(observed_at)
            .bind(confidence)
            .bind(media_request_id)
            .execute(pool)
            .await?;
    }

    Ok(())
}

fn lifecycle_column(source: &str, event_type: &str) -> Option<&'static str> {
    let normalized = event_type.to_ascii_lowercase();
    match (source, normalized.as_str()) {
        ("tautulli", "recently_added") | ("plex", "recently_added") | (_, "plex_available") => {
            Some("plex_available_at")
        }
        ("sonarr", "grab") | ("sonarr", "download") | ("sonarr", "episodegrabbed") => {
            Some("sonarr_grabbed_at")
        }
        ("sonarr", "import") | ("sonarr", "download_import") | ("sonarr", "episodefiledeleted") => {
            Some("sonarr_imported_at")
        }
        ("radarr", "grab") | ("radarr", "download") | ("radarr", "moviegrabbed") => {
            Some("radarr_grabbed_at")
        }
        ("radarr", "import") | ("radarr", "download_import") | ("radarr", "moviedownloaded") => {
            Some("radarr_imported_at")
        }
        ("torrent", "download_started") => Some("download_started_at"),
        ("torrent", "download_finished") => Some("download_finished_at"),
        ("overseerr", "notification") | ("overseerr", "email_sent") => {
            Some("overseerr_notification_sent_at")
        }
        _ => None,
    }
}

pub async fn find_match(
    pool: &SqlitePool,
    identity: &MediaIdentity,
) -> anyhow::Result<Option<MatchOutcome>> {
    if let Some(outcome) = find_by_id(pool, identity, "tmdb_id", identity.tmdb_id, 1.0).await? {
        return Ok(Some(outcome));
    }
    if let Some(outcome) = find_by_id(pool, identity, "tvdb_id", identity.tvdb_id, 0.95).await? {
        return Ok(Some(outcome));
    }
    if let Some(imdb_id) = &identity.imdb_id {
        let row = sqlx::query("SELECT id FROM media_requests WHERE media_type = ? AND imdb_id = ? ORDER BY requested_at DESC LIMIT 1")
            .bind(identity.media_type.as_str())
            .bind(imdb_id)
            .fetch_optional(pool)
            .await?;
        if let Some(row) = row {
            return Ok(Some(MatchOutcome {
                media_request_id: row.get("id"),
                confidence: 0.9,
            }));
        }
    }

    if identity.title.is_some() && identity.year.is_some() {
        let rows = sqlx::query(
            "SELECT id, media_type, tmdb_id, tvdb_id, imdb_id, title, year, season_number, episode_number FROM media_requests WHERE media_type = ? AND year = ?",
        )
        .bind(identity.media_type.as_str())
        .bind(identity.year)
        .fetch_all(pool)
        .await?;

        let candidates = rows
            .into_iter()
            .filter_map(|row| {
                let media_type: String = row.get("media_type");
                let request_identity = MediaIdentity {
                    media_type: MediaType::try_from(media_type.as_str()).ok()?,
                    tmdb_id: row.get("tmdb_id"),
                    tvdb_id: row.get("tvdb_id"),
                    imdb_id: row.get("imdb_id"),
                    title: row.get("title"),
                    year: row.get("year"),
                    season_number: row.get("season_number"),
                    episode_number: row.get("episode_number"),
                };
                Some((row.get::<i64, _>("id"), request_identity))
            })
            .collect::<Vec<_>>();

        return Ok(correlation::best_match(
            identity,
            candidates.iter().map(|(id, request)| (*id, request)),
        ));
    }

    Ok(None)
}

async fn find_by_id(
    pool: &SqlitePool,
    identity: &MediaIdentity,
    column: &str,
    value: Option<i64>,
    confidence: f64,
) -> anyhow::Result<Option<MatchOutcome>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let sql = format!(
        "SELECT id FROM media_requests WHERE media_type = ? AND {column} = ? ORDER BY requested_at DESC LIMIT 1"
    );
    let row = sqlx::query(&sql)
        .bind(identity.media_type.as_str())
        .bind(value)
        .fetch_optional(pool)
        .await?;

    Ok(row.map(|row| MatchOutcome {
        media_request_id: row.get("id"),
        confidence,
    }))
}

pub fn parse_datetime_or_now(value: Option<&Value>) -> DateTime<Utc> {
    value
        .and_then(Value::as_str)
        .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(Utc::now)
}
