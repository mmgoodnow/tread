use chrono::{TimeZone, Utc};
use serde_json::json;
use sqlx::Row;
use tread::{
    clients::webhook::generic_media_event,
    core::model::{
        AvailabilityClass, EventSource, IncomingRequest, MediaIdentity, MediaRequestItemInput,
        MediaType,
    },
    db::{connect, ingest_event, upsert_media_request},
    telemetry::render_metrics,
};

#[tokio::test]
async fn request_upsert_reconciles_prior_unmatched_radarr_grab() {
    let dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!("sqlite://{}?mode=rwc", dir.path().join("test.db").display());
    let pool = connect(&database_url).await.expect("db connect");

    let event = generic_media_event(
        EventSource::Radarr,
        "grab",
        json!({
            "eventType": "Grab",
            "movie": {
                "tmdbId": 10843,
                "imdbId": "tt0088680",
                "title": "After Hours",
                "year": 1985
            },
            "downloadId": "download-1",
            "observed_at": "2026-06-02T02:51:26Z"
        }),
    );
    ingest_event(&pool, event).await.expect("event ingest");

    upsert_media_request(
        &pool,
        IncomingRequest {
            overseerr_request_id: Some(546),
            identity: MediaIdentity {
                media_type: MediaType::Movie,
                tmdb_id: Some(10843),
                tvdb_id: None,
                imdb_id: Some("tt0088680".to_string()),
                title: Some("After Hours".to_string()),
                year: Some(1985),
                season_number: None,
                episode_number: None,
            },
            items: vec![MediaRequestItemInput {
                season_number: None,
                episode_number: None,
                title: None,
                air_date: None,
                availability_class: AvailabilityClass::Existing,
            }],
            title: "After Hours".to_string(),
            requested_by: Some("user".to_string()),
            requested_at: Utc.with_ymd_and_hms(2026, 6, 2, 2, 51, 18).unwrap(),
        },
    )
    .await
    .expect("request insert");

    let row = sqlx::query(
        "SELECT radarr_grabbed_at FROM media_requests WHERE overseerr_request_id = 546",
    )
    .fetch_one(&pool)
    .await
    .expect("request row");
    assert_eq!(
        row.get::<Option<String>, _>("radarr_grabbed_at").as_deref(),
        Some("2026-06-02T02:51:26+00:00")
    );

    let metrics = render_metrics(&pool).await.expect("metrics render");
    assert!(
        metrics.contains("media_request_events_total{event_type=\"Grab\",source=\"radarr\"} 1")
    );
    assert!(metrics.contains("media_request_lifecycle_inflight{stage=\"download_started\"} 1"));
    assert!(metrics.contains("media_requests_total{media_type=\"movie\"} 1"));
}

#[tokio::test]
async fn generic_event_reads_overseerr_request_media_identity() {
    let dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!("sqlite://{}?mode=rwc", dir.path().join("test.db").display());
    let pool = connect(&database_url).await.expect("db connect");

    upsert_media_request(
        &pool,
        IncomingRequest {
            overseerr_request_id: Some(548),
            identity: MediaIdentity {
                media_type: MediaType::Movie,
                tmdb_id: Some(11368),
                tvdb_id: None,
                imdb_id: Some("tt0086979".to_string()),
                title: Some("Blood Simple".to_string()),
                year: Some(1985),
                season_number: None,
                episode_number: None,
            },
            items: vec![MediaRequestItemInput {
                season_number: None,
                episode_number: None,
                title: None,
                air_date: None,
                availability_class: AvailabilityClass::Existing,
            }],
            title: "Blood Simple".to_string(),
            requested_by: Some("user".to_string()),
            requested_at: Utc.with_ymd_and_hms(2026, 6, 2, 4, 44, 59).unwrap(),
        },
    )
    .await
    .expect("request insert");

    let event = generic_media_event(
        EventSource::Overseerr,
        "request_submitted",
        json!({
            "event_type": "notification",
            "notification_type": "MEDIA_AUTO_APPROVED",
            "request": {
                "id": "548",
                "type": "movie",
                "title": "Blood Simple (1985)",
                "media": {
                    "mediaType": "movie",
                    "tmdbId": "11368",
                    "imdbId": "tt0086979",
                    "title": "Blood Simple (1985)"
                }
            }
        }),
    );
    ingest_event(&pool, event).await.expect("event ingest");

    let row = sqlx::query(
        "SELECT overseerr_notification_sent_at FROM media_requests WHERE overseerr_request_id = 548",
    )
    .fetch_one(&pool)
    .await
    .expect("request row");

    assert!(
        row.get::<Option<String>, _>("overseerr_notification_sent_at")
            .is_some()
    );
}

#[tokio::test]
async fn metrics_include_correlated_request_to_plex_duration() {
    let dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!("sqlite://{}?mode=rwc", dir.path().join("test.db").display());
    let pool = connect(&database_url).await.expect("db connect");

    upsert_media_request(
        &pool,
        IncomingRequest {
            overseerr_request_id: Some(1001),
            identity: MediaIdentity {
                media_type: MediaType::Movie,
                tmdb_id: Some(550),
                tvdb_id: None,
                imdb_id: Some("tt0137523".to_string()),
                title: Some("Fight Club".to_string()),
                year: Some(1999),
                season_number: None,
                episode_number: None,
            },
            items: vec![MediaRequestItemInput {
                season_number: None,
                episode_number: None,
                title: None,
                air_date: None,
                availability_class: AvailabilityClass::Existing,
            }],
            title: "Fight Club".to_string(),
            requested_by: Some("user".to_string()),
            requested_at: Utc.with_ymd_and_hms(2026, 5, 31, 20, 0, 0).unwrap(),
        },
    )
    .await
    .expect("request insert");

    let event = generic_media_event(
        EventSource::Tautulli,
        "recently_added",
        json!({
            "event_type": "recently_added",
            "external_id": "plex-rating-key-1",
            "media_type": "movie",
            "tmdb_id": 550,
            "imdb_id": "tt0137523",
            "title": "Fight Club",
            "year": 1999,
            "observed_at": "2026-05-31T20:05:00Z"
        }),
    );
    ingest_event(&pool, event).await.expect("event ingest");

    let metrics = render_metrics(&pool).await.expect("metrics render");
    assert!(metrics.contains("media_requests_total{media_type=\"movie\"} 1"));
    assert!(metrics.contains(
        "media_request_events_total{event_type=\"recently_added\",source=\"tautulli\"} 1"
    ));
    assert!(metrics.contains(
        "media_request_to_plex_available_seconds_sum{media_type=\"movie\",source=\"tautulli\"} 300"
    ));
}

#[tokio::test]
async fn future_airing_items_use_air_date_latency() {
    let dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!("sqlite://{}?mode=rwc", dir.path().join("test.db").display());
    let pool = connect(&database_url).await.expect("db connect");

    upsert_media_request(
        &pool,
        IncomingRequest {
            overseerr_request_id: Some(2001),
            identity: MediaIdentity {
                media_type: MediaType::Series,
                tmdb_id: Some(123),
                tvdb_id: Some(456),
                imdb_id: None,
                title: Some("Weekly Show".to_string()),
                year: Some(2026),
                season_number: Some(3),
                episode_number: None,
            },
            items: vec![MediaRequestItemInput {
                season_number: Some(3),
                episode_number: Some(1),
                title: Some("Premiere".to_string()),
                air_date: Some(Utc.with_ymd_and_hms(2026, 6, 7, 20, 0, 0).unwrap()),
                availability_class: AvailabilityClass::FutureAiring,
            }],
            title: "Weekly Show".to_string(),
            requested_by: Some("user".to_string()),
            requested_at: Utc.with_ymd_and_hms(2026, 5, 31, 20, 0, 0).unwrap(),
        },
    )
    .await
    .expect("request insert");

    let event = generic_media_event(
        EventSource::Tautulli,
        "recently_added",
        json!({
            "event_type": "recently_added",
            "external_id": "plex-rating-key-2",
            "media_type": "series",
            "tmdb_id": 123,
            "tvdb_id": 456,
            "title": "Weekly Show",
            "year": 2026,
            "season_number": 3,
            "episode_number": 1,
            "observed_at": "2026-06-07T21:00:00Z"
        }),
    );
    ingest_event(&pool, event).await.expect("event ingest");

    let metrics = render_metrics(&pool).await.expect("metrics render");
    assert!(
        metrics
            .contains("media_episode_air_to_plex_available_seconds_sum{source=\"tautulli\"} 3600")
    );
    assert!(!metrics.contains(
        "media_request_to_plex_available_seconds_sum{media_type=\"series\",source=\"tautulli\"} 608400"
    ));
}
