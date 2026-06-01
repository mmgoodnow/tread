use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{Row, SqlitePool, sqlite::SqlitePoolOptions};

use crate::core::{
    correlation,
    model::{
        AvailabilityClass, EventIngest, IncomingRequest, MatchOutcome, MediaIdentity,
        MediaRequestItemInput, MediaType,
    },
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
    .bind(request.identity.imdb_id.clone())
    .bind(request.title.clone())
    .bind(request.identity.year)
    .bind(request.identity.season_number)
    .bind(request.identity.episode_number)
    .bind(request.requested_by.clone())
    .bind(requested_at)
    .fetch_one(pool)
    .await?;

    let media_request_id = row.get("id");
    upsert_request_items(pool, media_request_id, &request).await?;

    Ok(media_request_id)
}

async fn upsert_request_items(
    pool: &SqlitePool,
    media_request_id: i64,
    request: &IncomingRequest,
) -> anyhow::Result<()> {
    let items = if request.items.is_empty() {
        vec![MediaRequestItemInput {
            season_number: request.identity.season_number,
            episode_number: request.identity.episode_number,
            title: Some(request.title.clone()),
            air_date: None,
            availability_class: if request.identity.media_type == MediaType::Movie {
                AvailabilityClass::Existing
            } else {
                AvailabilityClass::Unknown
            },
        }]
    } else {
        request.items.clone()
    };

    for item in items {
        sqlx::query(
            r#"
            INSERT INTO media_request_items (
                media_request_id, media_type, season_number, episode_number, title, air_date,
                requested_at, availability_class, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            ON CONFLICT(
                media_request_id,
                COALESCE(season_number, -1),
                COALESCE(episode_number, -1)
            ) DO UPDATE SET
                title = COALESCE(excluded.title, media_request_items.title),
                air_date = COALESCE(excluded.air_date, media_request_items.air_date),
                availability_class = CASE
                    WHEN media_request_items.availability_class = 'unknown'
                    THEN excluded.availability_class
                    ELSE media_request_items.availability_class
                END,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            "#,
        )
        .bind(media_request_id)
        .bind(request.identity.media_type.as_str())
        .bind(item.season_number)
        .bind(item.episode_number)
        .bind(item.title)
        .bind(item.air_date.map(|date| date.to_rfc3339()))
        .bind(request.requested_at.to_rfc3339())
        .bind(item.availability_class.as_str())
        .execute(pool)
        .await?;
    }

    Ok(())
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
        apply_lifecycle_timestamp(pool, outcome, &event).await?;
    }

    let payload = serde_json::to_string(&event.payload_json)?;
    let observed_at = event.observed_at.to_rfc3339();

    sqlx::query(
        r#"
        INSERT INTO events (
            source, event_type, media_request_id, media_request_item_id,
            external_id, payload_json, observed_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source, event_type, external_id) DO UPDATE SET
            media_request_id = COALESCE(excluded.media_request_id, events.media_request_id),
            media_request_item_id = COALESCE(excluded.media_request_item_id, events.media_request_item_id),
            payload_json = excluded.payload_json,
            observed_at = excluded.observed_at
        "#,
    )
    .bind(event.source.as_str())
    .bind(&event.event_type)
    .bind(match_outcome.map(|outcome| outcome.media_request_id))
    .bind(match_outcome.and_then(|outcome| outcome.media_request_item_id))
    .bind(&event.external_id)
    .bind(payload)
    .bind(observed_at)
    .execute(pool)
    .await?;

    Ok(match_outcome)
}

async fn apply_lifecycle_timestamp(
    pool: &SqlitePool,
    outcome: MatchOutcome,
    event: &EventIngest,
) -> anyhow::Result<()> {
    let observed_at = event.observed_at.to_rfc3339();
    let stage = lifecycle_column(event.source.as_str(), &event.event_type);

    if let Some(column) = stage {
        if let Some(media_request_item_id) = outcome.media_request_item_id {
            let sql = format!(
                "UPDATE media_request_items SET {column} = COALESCE({column}, ?), match_confidence = MAX(match_confidence, ?), updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?"
            );
            sqlx::query(&sql)
                .bind(&observed_at)
                .bind(outcome.confidence)
                .bind(media_request_item_id)
                .execute(pool)
                .await?;
        }

        let sql = format!(
            "UPDATE media_requests SET {column} = COALESCE({column}, ?), match_confidence = MAX(match_confidence, ?), updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?"
        );
        sqlx::query(&sql)
            .bind(observed_at)
            .bind(outcome.confidence)
            .bind(outcome.media_request_id)
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
            let media_request_id = row.get("id");
            return Ok(Some(MatchOutcome {
                media_request_id,
                media_request_item_id: find_item_for_request(pool, media_request_id, identity)
                    .await?,
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

        let mut outcome = correlation::best_match(
            identity,
            candidates.iter().map(|(id, request)| (*id, request)),
        );
        if let Some(outcome) = &mut outcome {
            outcome.media_request_item_id =
                find_item_for_request(pool, outcome.media_request_id, identity).await?;
        }
        return Ok(outcome);
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

    if let Some(row) = row {
        let media_request_id = row.get("id");
        return Ok(Some(MatchOutcome {
            media_request_id,
            media_request_item_id: find_item_for_request(pool, media_request_id, identity).await?,
            confidence,
        }));
    }

    Ok(None)
}

async fn find_item_for_request(
    pool: &SqlitePool,
    media_request_id: i64,
    identity: &MediaIdentity,
) -> anyhow::Result<Option<i64>> {
    if let Some(episode_number) = identity.episode_number {
        let row = sqlx::query(
            r#"
            SELECT id FROM media_request_items
            WHERE media_request_id = ?
              AND (season_number = ? OR season_number IS NULL)
              AND episode_number = ?
            ORDER BY season_number IS NULL, id
            LIMIT 1
            "#,
        )
        .bind(media_request_id)
        .bind(identity.season_number)
        .bind(episode_number)
        .fetch_optional(pool)
        .await?;
        if let Some(row) = row {
            return Ok(Some(row.get("id")));
        }
    }

    if let Some(season_number) = identity.season_number {
        let row = sqlx::query(
            r#"
            SELECT id FROM media_request_items
            WHERE media_request_id = ?
              AND season_number = ?
              AND episode_number IS NULL
            ORDER BY id
            LIMIT 1
            "#,
        )
        .bind(media_request_id)
        .bind(season_number)
        .fetch_optional(pool)
        .await?;
        if let Some(row) = row {
            return Ok(Some(row.get("id")));
        }
    }

    let row = sqlx::query(
        r#"
        SELECT id FROM media_request_items
        WHERE media_request_id = ?
        ORDER BY
          CASE
            WHEN season_number IS NULL AND episode_number IS NULL THEN 0
            ELSE 1
          END,
          id
        LIMIT 1
        "#,
    )
    .bind(media_request_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|row| row.get("id")))
}

pub fn parse_datetime_or_now(value: Option<&Value>) -> DateTime<Utc> {
    value
        .and_then(Value::as_str)
        .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(Utc::now)
}
