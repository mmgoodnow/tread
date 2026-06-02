use chrono::{TimeZone, Utc};
use serde_json::json;
use sqlx::Row;
use tread::{
    clients::webhook::{
        arr_event, generic_media_event, rtorrent_event, rtorrent_payload_from_form,
    },
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
        "SELECT radarr_grabbed_at, download_started_at FROM media_requests WHERE overseerr_request_id = 546",
    )
    .fetch_one(&pool)
    .await
    .expect("request row");
    assert_eq!(
        row.get::<Option<String>, _>("radarr_grabbed_at").as_deref(),
        Some("2026-06-02T02:51:26+00:00")
    );
    assert_eq!(
        row.get::<Option<String>, _>("download_started_at")
            .as_deref(),
        Some("2026-06-02T02:51:26+00:00")
    );

    let metrics = render_metrics(&pool).await.expect("metrics render");
    assert!(
        metrics.contains("media_request_events_total{event_type=\"Grab\",source=\"radarr\"} 1")
    );
    assert!(metrics.contains("media_request_lifecycle_inflight{stage=\"download_started\"} 0"));
    assert!(metrics.contains("media_requests_total{media_type=\"movie\"} 1"));
}

#[tokio::test]
async fn radarr_download_marks_download_finished() {
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

    let event = arr_event(
        EventSource::Radarr,
        json!({
            "eventType": "Download",
            "movie": {
                "tmdbId": 11368,
                "imdbId": "tt0086979",
                "title": "Blood Simple",
                "year": 1985
            },
            "downloadId": "download-1",
            "observed_at": "2026-06-02T04:53:48Z"
        }),
    );
    ingest_event(&pool, event).await.expect("event ingest");

    let row = sqlx::query(
        "SELECT radarr_imported_at, download_finished_at FROM media_requests WHERE overseerr_request_id = 548",
    )
    .fetch_one(&pool)
    .await
    .expect("request row");

    let radarr_imported_at = row
        .get::<Option<String>, _>("radarr_imported_at")
        .expect("radarr import timestamp");
    let download_finished_at = row
        .get::<Option<String>, _>("download_finished_at")
        .expect("download finish timestamp");
    assert_eq!(radarr_imported_at, download_finished_at);
}

#[tokio::test]
async fn request_upsert_does_not_overwrite_real_title_with_numeric_fallback() {
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

    upsert_media_request(
        &pool,
        IncomingRequest {
            overseerr_request_id: Some(548),
            identity: MediaIdentity {
                media_type: MediaType::Movie,
                tmdb_id: Some(11368),
                tvdb_id: None,
                imdb_id: None,
                title: Some("11368".to_string()),
                year: None,
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
            title: "11368".to_string(),
            requested_by: None,
            requested_at: Utc.with_ymd_and_hms(2026, 6, 2, 4, 44, 59).unwrap(),
        },
    )
    .await
    .expect("fallback request upsert");

    let row =
        sqlx::query("SELECT title, year FROM media_requests WHERE overseerr_request_id = 548")
            .fetch_one(&pool)
            .await
            .expect("request row");
    assert_eq!(row.get::<String, _>("title"), "Blood Simple");
    assert_eq!(row.get::<Option<i64>, _>("year"), Some(1985));
}

#[tokio::test]
async fn title_only_plex_match_updates_generic_movie_item() {
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
                imdb_id: None,
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
        EventSource::Tautulli,
        "recently_added",
        json!({
            "event_type": "recently_added",
            "external_id": "plex-rating-key-3",
            "media_type": "movie",
            "title": "Blood Simple",
            "year": 1984,
            "added_at": 1780376028,
            "observed_at": "2026-06-02T05:13:42Z"
        }),
    );
    ingest_event(&pool, event).await.expect("event ingest");

    let row = sqlx::query(
        r#"
        SELECT mri.plex_available_at
        FROM media_request_items mri
        JOIN media_requests mr ON mr.id = mri.media_request_id
        WHERE mr.overseerr_request_id = 548
        "#,
    )
    .fetch_one(&pool)
    .await
    .expect("item row");
    assert_eq!(
        row.get::<Option<String>, _>("plex_available_at").as_deref(),
        Some("2026-06-02T04:53:48+00:00")
    );

    let metrics = render_metrics(&pool).await.expect("metrics render");
    assert!(metrics.contains(
        "media_request_to_plex_available_seconds_sum{media_type=\"movie\",source=\"tautulli\"} 529"
    ));
}

#[tokio::test]
async fn rtorrent_form_hook_marks_download_finished_by_title_fallback() {
    let dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!("sqlite://{}?mode=rwc", dir.path().join("test.db").display());
    let pool = connect(&database_url).await.expect("db connect");

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

    let payload = rtorrent_payload_from_form(
        [
            ("info_hash".to_string(), "abc123".to_string()),
            (
                "base_path".to_string(),
                "/downloads/After.Hours.1985.1080p".to_string(),
            ),
            ("complete".to_string(), "1".to_string()),
            (
                "observed_at".to_string(),
                "2026-06-02T03:00:05Z".to_string(),
            ),
        ]
        .into_iter()
        .collect(),
    );
    ingest_event(&pool, rtorrent_event(payload))
        .await
        .expect("rtorrent event ingest");

    let row = sqlx::query(
        "SELECT download_finished_at FROM media_requests WHERE overseerr_request_id = 546",
    )
    .fetch_one(&pool)
    .await
    .expect("request row");
    assert_eq!(
        row.get::<Option<String>, _>("download_finished_at")
            .as_deref(),
        Some("2026-06-02T03:00:05+00:00")
    );

    let metrics = render_metrics(&pool).await.expect("metrics render");
    assert!(metrics.contains(
        "media_request_to_download_finished_seconds_sum{download_client=\"rtorrent\",media_type=\"movie\"} 527"
    ));
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
            "added_at": 1780257900,
            "observed_at": "2026-05-31T20:10:00Z"
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
