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
            title = CASE
                WHEN excluded.tmdb_id IS NOT NULL
                    AND excluded.title = CAST(excluded.tmdb_id AS TEXT)
                    AND media_requests.title IS NOT NULL
                    AND media_requests.title != excluded.title
                THEN media_requests.title
                WHEN excluded.tvdb_id IS NOT NULL
                    AND excluded.title = CAST(excluded.tvdb_id AS TEXT)
                    AND media_requests.title IS NOT NULL
                    AND media_requests.title != excluded.title
                THEN media_requests.title
                WHEN excluded.title LIKE '% tmdb:%'
                    AND media_requests.title IS NOT NULL
                THEN media_requests.title
                WHEN excluded.title LIKE '% tvdb:%'
                    AND media_requests.title IS NOT NULL
                THEN media_requests.title
                WHEN excluded.title LIKE '% imdb:%'
                    AND media_requests.title IS NOT NULL
                THEN media_requests.title
                ELSE excluded.title
            END,
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
    reconcile_unmatched_events(pool, media_request_id, &request.identity).await?;

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
        update_request_identity_from_event(pool, outcome.media_request_id, &event).await?;
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
    for column in lifecycle_columns(event.source.as_str(), &event.event_type) {
        apply_lifecycle_column(
            pool,
            outcome.media_request_id,
            outcome.media_request_item_id,
            column,
            &event.observed_at.to_rfc3339(),
            outcome.confidence,
        )
        .await?;
    }

    Ok(())
}

async fn update_request_identity_from_event(
    pool: &SqlitePool,
    media_request_id: i64,
    event: &EventIngest,
) -> anyhow::Result<()> {
    let Some(identity) = &event.identity else {
        return Ok(());
    };

    sqlx::query(
        r#"
        UPDATE media_requests
        SET tmdb_id = COALESCE(tmdb_id, ?),
            tvdb_id = COALESCE(tvdb_id, ?),
            imdb_id = COALESCE(imdb_id, ?),
            title = CASE
                WHEN ? IS NULL THEN title
                WHEN title IS NULL THEN ?
                WHEN tmdb_id IS NOT NULL AND title = CAST(tmdb_id AS TEXT) THEN ?
                WHEN tvdb_id IS NOT NULL AND title = CAST(tvdb_id AS TEXT) THEN ?
                WHEN title LIKE '% tmdb:%' THEN ?
                WHEN title LIKE '% tvdb:%' THEN ?
                WHEN title LIKE '% imdb:%' THEN ?
                ELSE title
            END,
            year = COALESCE(year, ?),
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        WHERE id = ?
        "#,
    )
    .bind(identity.tmdb_id)
    .bind(identity.tvdb_id)
    .bind(identity.imdb_id.clone())
    .bind(identity.title.clone())
    .bind(identity.title.clone())
    .bind(identity.title.clone())
    .bind(identity.title.clone())
    .bind(identity.title.clone())
    .bind(identity.title.clone())
    .bind(identity.title.clone())
    .bind(identity.year)
    .bind(media_request_id)
    .execute(pool)
    .await?;

    Ok(())
}

async fn apply_lifecycle_column(
    pool: &SqlitePool,
    media_request_id: i64,
    media_request_item_id: Option<i64>,
    column: &str,
    observed_at: &str,
    confidence: f64,
) -> anyhow::Result<()> {
    match media_request_item_id {
        Some(media_request_item_id) => {
            let sql = format!(
                "UPDATE media_request_items SET {column} = CASE WHEN {column} IS NULL OR (julianday(?) IS NOT NULL AND julianday({column}) IS NOT NULL AND julianday(?) < julianday({column})) THEN ? ELSE {column} END, match_confidence = MAX(match_confidence, ?), updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?"
            );
            sqlx::query(&sql)
                .bind(observed_at)
                .bind(observed_at)
                .bind(observed_at)
                .bind(confidence)
                .bind(media_request_item_id)
                .execute(pool)
                .await?;
        }
        None => {
            let sql = format!(
                "UPDATE media_request_items SET {column} = CASE WHEN {column} IS NULL OR (julianday(?) IS NOT NULL AND julianday({column}) IS NOT NULL AND julianday(?) < julianday({column})) THEN ? ELSE {column} END, match_confidence = MAX(match_confidence, ?), updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE media_request_id = ? AND season_number IS NULL AND episode_number IS NULL"
            );
            sqlx::query(&sql)
                .bind(observed_at)
                .bind(observed_at)
                .bind(observed_at)
                .bind(confidence)
                .bind(media_request_id)
                .execute(pool)
                .await?;
        }
    }

    let sql = format!(
        "UPDATE media_requests SET {column} = CASE WHEN {column} IS NULL OR (julianday(?) IS NOT NULL AND julianday({column}) IS NOT NULL AND julianday(?) < julianday({column})) THEN ? ELSE {column} END, match_confidence = MAX(match_confidence, ?), updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?"
    );
    sqlx::query(&sql)
        .bind(observed_at)
        .bind(observed_at)
        .bind(observed_at)
        .bind(confidence)
        .bind(media_request_id)
        .execute(pool)
        .await?;

    Ok(())
}

async fn reconcile_unmatched_events(
    pool: &SqlitePool,
    media_request_id: i64,
    identity: &MediaIdentity,
) -> anyhow::Result<()> {
    let media_request_item_id = find_item_for_request(pool, media_request_id, identity).await?;

    if let Some(tmdb_id) = identity.tmdb_id {
        reconcile_unmatched_integer_events(
            pool,
            media_request_id,
            media_request_item_id,
            &[
                "tmdbId",
                "tmdb_id",
                "movie.tmdbId",
                "series.tmdbId",
                "media.tmdbId",
                "request.media.tmdbId",
            ],
            tmdb_id,
            1.0,
        )
        .await?;
    }

    if let Some(tvdb_id) = identity.tvdb_id {
        reconcile_unmatched_integer_events(
            pool,
            media_request_id,
            media_request_item_id,
            &[
                "tvdbId",
                "tvdb_id",
                "movie.tvdbId",
                "series.tvdbId",
                "media.tvdbId",
                "request.media.tvdbId",
            ],
            tvdb_id,
            0.95,
        )
        .await?;
    }

    if let Some(imdb_id) = &identity.imdb_id {
        reconcile_unmatched_text_events(
            pool,
            media_request_id,
            media_request_item_id,
            &[
                "imdbId",
                "imdb_id",
                "movie.imdbId",
                "series.imdbId",
                "media.imdbId",
                "request.media.imdbId",
            ],
            imdb_id,
            0.9,
        )
        .await?;
    }

    if let Some(title) = &identity.title {
        reconcile_unmatched_events_by_normalized_title(
            pool,
            media_request_id,
            media_request_item_id,
            identity,
            title,
            0.6,
        )
        .await?;
    }

    Ok(())
}

async fn reconcile_unmatched_integer_events(
    pool: &SqlitePool,
    media_request_id: i64,
    media_request_item_id: Option<i64>,
    json_paths: &[&str],
    value: i64,
    confidence: f64,
) -> anyhow::Result<()> {
    for path in json_paths {
        reconcile_unmatched_events_by_integer_json_value(
            pool,
            media_request_id,
            media_request_item_id,
            path,
            value,
            confidence,
        )
        .await?;
    }
    Ok(())
}

async fn reconcile_unmatched_text_events(
    pool: &SqlitePool,
    media_request_id: i64,
    media_request_item_id: Option<i64>,
    json_paths: &[&str],
    value: &str,
    confidence: f64,
) -> anyhow::Result<()> {
    for path in json_paths {
        reconcile_unmatched_events_by_text_json_value(
            pool,
            media_request_id,
            media_request_item_id,
            path,
            value,
            confidence,
        )
        .await?;
    }
    Ok(())
}

async fn reconcile_unmatched_events_by_integer_json_value(
    pool: &SqlitePool,
    media_request_id: i64,
    media_request_item_id: Option<i64>,
    json_path: &str,
    value: i64,
    confidence: f64,
) -> anyhow::Result<()> {
    let path = format!("$.{json_path}");
    let rows = sqlx::query(
        r#"
        SELECT id, source, event_type, observed_at
        FROM events
        WHERE media_request_id IS NULL
          AND CAST(json_extract(payload_json, ?) AS INTEGER) = ?
        "#,
    )
    .bind(&path)
    .bind(value)
    .fetch_all(pool)
    .await?;

    reconcile_unmatched_event_rows(
        pool,
        media_request_id,
        media_request_item_id,
        confidence,
        rows,
    )
    .await
}

async fn reconcile_unmatched_events_by_text_json_value(
    pool: &SqlitePool,
    media_request_id: i64,
    media_request_item_id: Option<i64>,
    json_path: &str,
    value: &str,
    confidence: f64,
) -> anyhow::Result<()> {
    let path = format!("$.{json_path}");
    let rows = sqlx::query(
        r#"
        SELECT id, source, event_type, observed_at
        FROM events
        WHERE media_request_id IS NULL
          AND json_extract(payload_json, ?) = ?
        "#,
    )
    .bind(&path)
    .bind(value)
    .fetch_all(pool)
    .await?;

    reconcile_unmatched_event_rows(
        pool,
        media_request_id,
        media_request_item_id,
        confidence,
        rows,
    )
    .await
}

async fn reconcile_unmatched_events_by_normalized_title(
    pool: &SqlitePool,
    media_request_id: i64,
    media_request_item_id: Option<i64>,
    identity: &MediaIdentity,
    title: &str,
    confidence: f64,
) -> anyhow::Result<()> {
    let normalized_title = correlation::normalize_title(title);
    if normalized_title.is_empty() {
        return Ok(());
    }

    let rows = sqlx::query(
        r#"
        SELECT id, source, event_type, observed_at, payload_json
        FROM events
        WHERE media_request_id IS NULL
        "#,
    )
    .fetch_all(pool)
    .await?;

    let rows = rows
        .into_iter()
        .filter_map(|row| {
            let payload = row.get::<String, _>("payload_json");
            let payload = serde_json::from_str::<Value>(&payload).ok()?;

            let event_media_type = text_from_payload(&payload, &["media_type"])
                .or_else(|| text_from_payload(&payload, &["mediaType"]))
                .and_then(|value| MediaType::try_from(value.as_str()).ok());
            if event_media_type.is_some_and(|media_type| media_type != identity.media_type) {
                return None;
            }

            let event_title = text_from_payload(&payload, &["title"])
                .or_else(|| text_from_payload(&payload, &["grandparent_title"]))
                .or_else(|| text_from_payload(&payload, &["request", "title"]))
                .or_else(|| text_from_payload(&payload, &["media", "title"]))?;
            if correlation::normalize_title(&event_title) != normalized_title {
                return None;
            }

            Some(row)
        })
        .collect::<Vec<_>>();

    reconcile_unmatched_event_rows(
        pool,
        media_request_id,
        media_request_item_id,
        confidence,
        rows,
    )
    .await
}

fn text_from_payload(value: &Value, path: &[&str]) -> Option<String> {
    let mut cursor = value;
    for segment in path {
        cursor = cursor.get(*segment)?;
    }
    cursor
        .as_str()
        .map(ToString::to_string)
        .or_else(|| cursor.as_i64().map(|n| n.to_string()))
}

async fn reconcile_unmatched_event_rows(
    pool: &SqlitePool,
    media_request_id: i64,
    media_request_item_id: Option<i64>,
    confidence: f64,
    rows: Vec<sqlx::sqlite::SqliteRow>,
) -> anyhow::Result<()> {
    for row in rows {
        let event_id = row.get::<i64, _>("id");
        let source = row.get::<String, _>("source");
        let event_type = row.get::<String, _>("event_type");
        let observed_at = row.get::<String, _>("observed_at");

        for column in lifecycle_columns(&source, &event_type) {
            apply_lifecycle_column(
                pool,
                media_request_id,
                media_request_item_id,
                column,
                &observed_at,
                confidence,
            )
            .await?;
        }

        sqlx::query(
            r#"
            UPDATE events
            SET media_request_id = ?,
                media_request_item_id = COALESCE(media_request_item_id, ?)
            WHERE id = ?
            "#,
        )
        .bind(media_request_id)
        .bind(media_request_item_id)
        .bind(event_id)
        .execute(pool)
        .await?;
    }

    Ok(())
}

fn lifecycle_columns(source: &str, event_type: &str) -> Vec<&'static str> {
    let normalized = event_type.to_ascii_lowercase();
    match (source, normalized.as_str()) {
        ("tautulli", "recently_added") | ("plex", "recently_added") | (_, "plex_available") => {
            vec!["plex_available_at"]
        }
        ("sonarr", "grab") | ("sonarr", "episodegrabbed") => {
            vec!["sonarr_grabbed_at", "download_started_at"]
        }
        ("sonarr", "download") | ("sonarr", "import") | ("sonarr", "download_import") => {
            vec!["sonarr_imported_at", "download_finished_at"]
        }
        ("radarr", "grab") | ("radarr", "moviegrabbed") => {
            vec!["radarr_grabbed_at", "download_started_at"]
        }
        ("radarr", "download")
        | ("radarr", "import")
        | ("radarr", "download_import")
        | ("radarr", "moviedownloaded") => {
            vec!["radarr_imported_at", "download_finished_at"]
        }
        ("torrent", "download_started") => vec!["download_started_at"],
        ("torrent", "download_finished") => vec!["download_finished_at"],
        ("overseerr", "notification") | ("overseerr", "email_sent") => {
            vec!["overseerr_notification_sent_at"]
        }
        _ => Vec::new(),
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
            return Ok(Some(*outcome));
        }
    }

    if let Some(title) = &identity.title {
        let rows = sqlx::query(
            "SELECT id, media_type, tmdb_id, tvdb_id, imdb_id, title, year, season_number, episode_number FROM media_requests WHERE media_type = ?",
        )
        .bind(identity.media_type.as_str())
        .fetch_all(pool)
        .await?;

        let normalized_title = correlation::normalize_title(title);
        let mut outcome = rows
            .into_iter()
            .filter_map(|row| {
                let request_title = row.get::<Option<String>, _>("title")?;
                if correlation::normalize_title(&request_title) != normalized_title {
                    return None;
                }

                Some(MatchOutcome {
                    media_request_id: row.get("id"),
                    media_request_item_id: None,
                    confidence: 0.6,
                })
            })
            .max_by(|a, b| a.confidence.total_cmp(&b.confidence));

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
