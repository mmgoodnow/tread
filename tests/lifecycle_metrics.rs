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
                identifiers: Vec::new(),
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
async fn recent_software_delay_rows_break_down_avoidable_lag() {
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
                identifiers: Vec::new(),
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

    sqlx::query(
        r#"
        UPDATE media_request_items
        SET download_finished_at = '2026-06-02T04:53:00+00:00',
            radarr_imported_at = '2026-06-02T04:54:30+00:00',
            plex_available_at = '2026-06-02T04:55:00+00:00',
            overseerr_notification_sent_at = '2026-06-02T04:55:45+00:00'
        WHERE media_request_id = (SELECT id FROM media_requests WHERE overseerr_request_id = 548)
        "#,
    )
    .execute(&pool)
    .await
    .expect("item update");

    let rows = tread::api::recent_software_delay_rows(&pool, 10)
        .await
        .expect("delay rows");
    let row = rows.first().expect("delay row");

    assert_eq!(row.title, "Blood Simple");
    assert_eq!(row.download_finished_to_arr_import_seconds, Some(90.0));
    assert_eq!(row.arr_import_to_plex_available_seconds, Some(30.0));
    assert_eq!(row.plex_available_to_notification_seconds, Some(45.0));
    assert_eq!(row.known_software_delay_seconds, 165.0);
    assert_eq!(row.total_software_delay_seconds, 165.0);
    assert_eq!(row.observed_stage_count, 4);
    assert_eq!(row.expected_stage_count, 4);
    assert!(row.lifecycle_complete);
    assert!(row.missing_stages.is_empty());
}

#[tokio::test]
async fn recent_software_delay_rows_skip_parent_when_specific_items_exist() {
    let dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!("sqlite://{}?mode=rwc", dir.path().join("test.db").display());
    let pool = connect(&database_url).await.expect("db connect");

    upsert_media_request(
        &pool,
        IncomingRequest {
            overseerr_request_id: Some(552),
            identity: MediaIdentity {
                media_type: MediaType::Series,
                tmdb_id: Some(250758),
                tvdb_id: None,
                imdb_id: None,
                title: Some("Rafa".to_string()),
                year: Some(2026),
                season_number: None,
                episode_number: None,
                identifiers: Vec::new(),
            },
            items: vec![
                MediaRequestItemInput {
                    season_number: None,
                    episode_number: None,
                    title: None,
                    air_date: None,
                    availability_class: AvailabilityClass::Existing,
                },
                MediaRequestItemInput {
                    season_number: Some(1),
                    episode_number: None,
                    title: None,
                    air_date: None,
                    availability_class: AvailabilityClass::Existing,
                },
            ],
            title: "Rafa".to_string(),
            requested_by: Some("user".to_string()),
            requested_at: Utc.with_ymd_and_hms(2026, 6, 7, 7, 19, 42).unwrap(),
        },
    )
    .await
    .expect("request insert");

    sqlx::query(
        r#"
        UPDATE media_request_items
        SET download_finished_at = '2026-06-07T07:42:32+00:00',
            sonarr_imported_at = '2026-06-07T07:42:32+00:00',
            plex_available_at = '2026-06-07T07:42:37+00:00',
            overseerr_notification_sent_at = '2026-06-07T10:15:47+00:00'
        WHERE media_request_id = (SELECT id FROM media_requests WHERE overseerr_request_id = 552)
        "#,
    )
    .execute(&pool)
    .await
    .expect("item update");

    let rows = tread::api::recent_software_delay_rows(&pool, 10)
        .await
        .expect("delay rows");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].title, "Rafa");
    assert_eq!(rows[0].season_number, Some(1));
    assert_eq!(rows[0].episode_number, None);
}

#[tokio::test]
async fn recent_software_delay_rows_expose_missing_lifecycle_stages() {
    let dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!("sqlite://{}?mode=rwc", dir.path().join("test.db").display());
    let pool = connect(&database_url).await.expect("db connect");

    upsert_media_request(
        &pool,
        IncomingRequest {
            overseerr_request_id: Some(549),
            identity: MediaIdentity {
                media_type: MediaType::Movie,
                tmdb_id: Some(123),
                tvdb_id: None,
                imdb_id: None,
                title: Some("Partial Movie".to_string()),
                year: Some(2026),
                season_number: None,
                episode_number: None,
                identifiers: Vec::new(),
            },
            items: vec![MediaRequestItemInput {
                season_number: None,
                episode_number: None,
                title: None,
                air_date: None,
                availability_class: AvailabilityClass::Existing,
            }],
            title: "Partial Movie".to_string(),
            requested_by: None,
            requested_at: Utc.with_ymd_and_hms(2026, 6, 2, 4, 44, 59).unwrap(),
        },
    )
    .await
    .expect("request insert");

    sqlx::query(
        r#"
        UPDATE media_request_items
        SET download_finished_at = '2026-06-02T04:53:00+00:00',
            plex_available_at = '2026-06-02T04:55:00+00:00'
        WHERE media_request_id = (SELECT id FROM media_requests WHERE overseerr_request_id = 549)
        "#,
    )
    .execute(&pool)
    .await
    .expect("item update");

    let rows = tread::api::recent_software_delay_rows(&pool, 10)
        .await
        .expect("delay rows");
    let row = rows.first().expect("delay row");

    assert_eq!(row.title, "Partial Movie");
    assert_eq!(row.observed_stage_count, 2);
    assert_eq!(row.expected_stage_count, 4);
    assert!(!row.lifecycle_complete);
    assert_eq!(
        row.missing_stages,
        vec!["arr_imported", "notification_sent"]
    );
    assert_eq!(row.known_software_delay_seconds, 0.0);
}

#[tokio::test]
async fn recent_software_delay_rows_clamp_subsecond_arr_to_plex_skew() {
    let dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!("sqlite://{}?mode=rwc", dir.path().join("test.db").display());
    let pool = connect(&database_url).await.expect("db connect");

    upsert_media_request(
        &pool,
        IncomingRequest {
            overseerr_request_id: Some(550),
            identity: MediaIdentity {
                media_type: MediaType::Movie,
                tmdb_id: Some(11368),
                tvdb_id: None,
                imdb_id: None,
                title: Some("Nearly Same Instant".to_string()),
                year: Some(2026),
                season_number: None,
                episode_number: None,
                identifiers: Vec::new(),
            },
            items: vec![MediaRequestItemInput {
                season_number: None,
                episode_number: None,
                title: None,
                air_date: None,
                availability_class: AvailabilityClass::Existing,
            }],
            title: "Nearly Same Instant".to_string(),
            requested_by: None,
            requested_at: Utc.with_ymd_and_hms(2026, 6, 2, 4, 44, 59).unwrap(),
        },
    )
    .await
    .expect("request insert");

    sqlx::query(
        r#"
        UPDATE media_request_items
        SET download_finished_at = '2026-06-02T04:53:48+00:00',
            radarr_imported_at = '2026-06-02T04:53:48.047606700+00:00',
            plex_available_at = '2026-06-02T04:53:48+00:00'
        WHERE media_request_id = (SELECT id FROM media_requests WHERE overseerr_request_id = 550)
        "#,
    )
    .execute(&pool)
    .await
    .expect("item update");

    let rows = tread::api::recent_software_delay_rows(&pool, 10)
        .await
        .expect("delay rows");
    let row = rows.first().expect("delay row");

    assert_eq!(row.arr_import_to_plex_available_seconds, Some(0.0));
}

#[tokio::test]
async fn episode_events_for_season_request_do_not_share_lifecycle_columns() {
    let dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!("sqlite://{}?mode=rwc", dir.path().join("test.db").display());
    let pool = connect(&database_url).await.expect("db connect");

    upsert_media_request(
        &pool,
        IncomingRequest {
            overseerr_request_id: Some(551),
            identity: MediaIdentity {
                media_type: MediaType::Series,
                tmdb_id: Some(12345),
                tvdb_id: Some(67890),
                imdb_id: None,
                title: Some("MasterChef".to_string()),
                year: Some(2010),
                season_number: Some(16),
                episode_number: None,
                identifiers: Vec::new(),
            },
            items: vec![MediaRequestItemInput {
                season_number: Some(16),
                episode_number: None,
                title: None,
                air_date: None,
                availability_class: AvailabilityClass::Unknown,
            }],
            title: "MasterChef".to_string(),
            requested_by: Some("user".to_string()),
            requested_at: Utc.with_ymd_and_hms(2026, 5, 1, 0, 0, 0).unwrap(),
        },
    )
    .await
    .expect("request insert");

    ingest_event(
        &pool,
        generic_media_event(
            EventSource::Tautulli,
            "recently_added",
            json!({
                "event_type": "recently_added",
                "media_type": "episode",
                "grandparent_title": "MasterChef",
                "grandparent_guids": ["tmdb://12345", "tvdb://67890"],
                "parent_media_index": "16",
                "media_index": "5",
                "rating_key": "episode-5",
                "added_at": "2026-05-21T07:27:21Z"
            }),
        ),
    )
    .await
    .expect("episode 5 tautulli ingest");

    ingest_event(
        &pool,
        arr_event(
            EventSource::Sonarr,
            json!({
                "eventType": "Download",
                "series": {
                    "title": "MasterChef",
                    "tvdbId": 67890,
                    "tmdbId": 12345
                },
                "episodes": [{
                    "seasonNumber": 16,
                    "episodeNumber": 8,
                    "title": "World Cup Cookoff"
                }],
                "episodeFile": {
                    "id": 67627,
                    "dateAdded": "2026-06-04T07:15:22.6277335Z",
                    "relativePath": "Season 16/MasterChef - S16E08.mkv"
                },
                "downloadId": "download-8",
                "downloadClient": "rTorrent",
                "observed_at": "2026-06-04T07:15:23Z"
            }),
        ),
    )
    .await
    .expect("episode 8 sonarr ingest");

    ingest_event(
        &pool,
        generic_media_event(
            EventSource::Tautulli,
            "recently_added",
            json!({
                "event_type": "recently_added",
                "media_type": "episode",
                "grandparent_title": "MasterChef",
                "grandparent_guids": ["tmdb://12345", "tvdb://67890"],
                "parent_media_index": "16",
                "media_index": "8",
                "rating_key": "episode-8",
                "added_at": "2026-06-04T07:15:27Z"
            }),
        ),
    )
    .await
    .expect("episode 8 tautulli ingest");

    let rows = tread::api::recent_software_delay_rows(&pool, 10)
        .await
        .expect("delay rows");
    let season_row = sqlx::query(
        r#"
        SELECT sonarr_imported_at, plex_available_at
        FROM media_request_items
        WHERE season_number = 16
          AND episode_number IS NULL
        "#,
    )
    .fetch_one(&pool)
    .await
    .expect("season row");
    assert_eq!(
        season_row
            .get::<Option<String>, _>("sonarr_imported_at")
            .as_deref(),
        None
    );
    assert_eq!(
        season_row
            .get::<Option<String>, _>("plex_available_at")
            .as_deref(),
        None
    );

    let episode_8_row = rows
        .iter()
        .find(|row| row.season_number == Some(16) && row.episode_number == Some(8))
        .expect("episode 8 row");
    assert_eq!(
        episode_8_row.arr_import_to_plex_available_seconds,
        Some(4.0)
    );
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
                identifiers: Vec::new(),
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
async fn earlier_rtorrent_finish_replaces_later_radarr_generic_finish() {
    let dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!("sqlite://{}?mode=rwc", dir.path().join("test.db").display());
    let pool = connect(&database_url).await.expect("db connect");

    upsert_media_request(
        &pool,
        IncomingRequest {
            overseerr_request_id: Some(549),
            identity: MediaIdentity {
                media_type: MediaType::Movie,
                tmdb_id: Some(10388),
                tvdb_id: None,
                imdb_id: None,
                title: Some("The Limey (1999)".to_string()),
                year: Some(1999),
                season_number: None,
                episode_number: None,
                identifiers: Vec::new(),
            },
            items: vec![MediaRequestItemInput {
                season_number: None,
                episode_number: None,
                title: None,
                air_date: None,
                availability_class: AvailabilityClass::Existing,
            }],
            title: "The Limey (1999)".to_string(),
            requested_by: Some("user".to_string()),
            requested_at: Utc.with_ymd_and_hms(2026, 6, 2, 6, 4, 9).unwrap(),
        },
    )
    .await
    .expect("request insert");

    ingest_event(
        &pool,
        arr_event(
            EventSource::Radarr,
            json!({
                "eventType": "Download",
                "movie": {
                    "tmdbId": 10388,
                    "title": "The Limey",
                    "year": 1999
                },
                "downloadId": "download-1",
                "observed_at": "2026-06-02T06:22:19Z"
            }),
        ),
    )
    .await
    .expect("radarr event ingest");

    let payload = rtorrent_payload_from_form(
        [
            ("info_hash".to_string(), "abc123".to_string()),
            (
                "base_path".to_string(),
                "/downloads/The.Limey.1999.1080p.mkv".to_string(),
            ),
            ("complete".to_string(), "1".to_string()),
            (
                "observed_at".to_string(),
                "2026-06-02T06:20:50Z".to_string(),
            ),
        ]
        .into_iter()
        .collect(),
    );
    ingest_event(&pool, rtorrent_event(payload))
        .await
        .expect("rtorrent event ingest");

    let row = sqlx::query(
        "SELECT radarr_imported_at, download_finished_at FROM media_requests WHERE overseerr_request_id = 549",
    )
    .fetch_one(&pool)
    .await
    .expect("request row");
    assert_eq!(
        row.get::<Option<String>, _>("radarr_imported_at")
            .as_deref(),
        Some("2026-06-02T06:22:19+00:00")
    );
    assert_eq!(
        row.get::<Option<String>, _>("download_finished_at")
            .as_deref(),
        Some("2026-06-02T06:20:50+00:00")
    );
}

#[tokio::test]
async fn metrics_include_post_download_and_notification_lag() {
    let dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!("sqlite://{}?mode=rwc", dir.path().join("test.db").display());
    let pool = connect(&database_url).await.expect("db connect");

    upsert_media_request(
        &pool,
        IncomingRequest {
            overseerr_request_id: Some(549),
            identity: MediaIdentity {
                media_type: MediaType::Movie,
                tmdb_id: Some(10388),
                tvdb_id: None,
                imdb_id: None,
                title: Some("The Limey (1999)".to_string()),
                year: Some(1999),
                season_number: None,
                episode_number: None,
                identifiers: Vec::new(),
            },
            items: vec![MediaRequestItemInput {
                season_number: None,
                episode_number: None,
                title: None,
                air_date: None,
                availability_class: AvailabilityClass::Existing,
            }],
            title: "The Limey (1999)".to_string(),
            requested_by: Some("user".to_string()),
            requested_at: Utc.with_ymd_and_hms(2026, 6, 2, 6, 4, 9).unwrap(),
        },
    )
    .await
    .expect("request insert");

    sqlx::query(
        r#"
        UPDATE media_request_items
        SET download_finished_at = '2026-06-02T06:20:50+00:00',
            radarr_imported_at = '2026-06-02T06:22:20+00:00',
            plex_available_at = '2026-06-02T06:22:30+00:00',
            overseerr_notification_sent_at = '2026-06-02T06:23:00+00:00'
        WHERE media_request_id = (
            SELECT id FROM media_requests WHERE overseerr_request_id = 549
        )
        "#,
    )
    .execute(&pool)
    .await
    .expect("item update");

    let metrics = render_metrics(&pool).await.expect("metrics render");

    assert!(metrics.contains(
        "media_request_download_finished_to_arr_import_seconds_sum{arr=\"radarr\",media_type=\"movie\"} 90"
    ));
    assert!(metrics.contains(
        "media_request_plex_available_to_overseerr_notification_seconds_sum{media_type=\"movie\"} 30"
    ));
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
                identifiers: Vec::new(),
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
                identifiers: Vec::new(),
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
                identifiers: Vec::new(),
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
                identifiers: Vec::new(),
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
async fn learned_plex_show_rating_key_matches_later_tautulli_events() {
    let dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!("sqlite://{}?mode=rwc", dir.path().join("test.db").display());
    let pool = connect(&database_url).await.expect("db connect");

    upsert_media_request(
        &pool,
        IncomingRequest {
            overseerr_request_id: Some(550),
            identity: MediaIdentity {
                media_type: MediaType::Series,
                tmdb_id: Some(278174),
                tvdb_id: Some(457154),
                imdb_id: Some("tt35006947".to_string()),
                title: Some("Girl Rules".to_string()),
                year: Some(2026),
                season_number: Some(1),
                episode_number: None,
                identifiers: Vec::new(),
            },
            items: vec![MediaRequestItemInput {
                season_number: Some(1),
                episode_number: None,
                title: None,
                air_date: None,
                availability_class: AvailabilityClass::Unknown,
            }],
            title: "Girl Rules".to_string(),
            requested_by: Some("user".to_string()),
            requested_at: Utc.with_ymd_and_hms(2026, 6, 2, 12, 0, 0).unwrap(),
        },
    )
    .await
    .expect("request insert");

    ingest_event(
        &pool,
        generic_media_event(
            EventSource::Tautulli,
            "recently_added",
            json!({
                "event_type": "recently_added",
                "media_type": "episode",
                "title": "Final Girls",
                "grandparent_title": "Girl Rules",
                "parent_title": "Season 1",
                "grandparent_rating_key": "110001",
                "grandparent_guids": ["imdb://tt35006947", "tmdb://278174", "tvdb://457154"],
                "rating_key": "117598",
                "added_at": "2026-06-02T12:18:02Z"
            }),
        ),
    )
    .await
    .expect("first tautulli ingest");

    let identifier = sqlx::query(
        r#"
        SELECT media_request_id
        FROM media_request_identifiers
        WHERE namespace = 'plex_show_rating_key'
          AND value = '110001'
        "#,
    )
    .fetch_one(&pool)
    .await
    .expect("identifier row");
    assert!(identifier.get::<i64, _>("media_request_id") > 0);

    ingest_event(
        &pool,
        generic_media_event(
            EventSource::Tautulli,
            "recently_added",
            json!({
                "event_type": "recently_added",
                "media_type": "episode",
                "title": "Episode 2",
                "grandparent_title": "Unhelpful Title",
                "parent_title": "Season 1",
                "grandparent_rating_key": "110001",
                "rating_key": "117599",
                "added_at": "2026-06-02T12:25:00Z"
            }),
        ),
    )
    .await
    .expect("second tautulli ingest");

    let matched_count = sqlx::query(
        r#"
        SELECT COUNT(*) AS matched_count
        FROM events
        WHERE source = 'tautulli'
          AND media_request_id IS NOT NULL
        "#,
    )
    .fetch_one(&pool)
    .await
    .expect("events row")
    .get::<i64, _>("matched_count");

    assert_eq!(matched_count, 2);
}

#[tokio::test]
async fn series_title_match_does_not_fall_back_to_wrong_requested_season() {
    let dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!("sqlite://{}?mode=rwc", dir.path().join("test.db").display());
    let pool = connect(&database_url).await.expect("db connect");

    upsert_media_request(
        &pool,
        IncomingRequest {
            overseerr_request_id: Some(551),
            identity: MediaIdentity {
                media_type: MediaType::Series,
                tmdb_id: Some(111110),
                tvdb_id: Some(392276),
                imdb_id: Some("tt0388629".to_string()),
                title: Some("One Piece (2023)".to_string()),
                year: Some(2023),
                season_number: Some(1),
                episode_number: None,
                identifiers: Vec::new(),
            },
            items: vec![MediaRequestItemInput {
                season_number: Some(1),
                episode_number: None,
                title: None,
                air_date: None,
                availability_class: AvailabilityClass::Unknown,
            }],
            title: "One Piece (2023)".to_string(),
            requested_by: Some("user".to_string()),
            requested_at: Utc.with_ymd_and_hms(2026, 6, 7, 15, 0, 0).unwrap(),
        },
    )
    .await
    .expect("request insert");

    let outcome = ingest_event(
        &pool,
        generic_media_event(
            EventSource::Tautulli,
            "recently_added",
            json!({
                "event_type": "recently_added",
                "media_type": "episode",
                "title": "Episode 10",
                "grandparent_title": "One Piece",
                "parent_title": "Season 23",
                "grandparent_rating_key": "92442",
                "grandparent_guids": ["imdb://tt0388629", "tmdb://37854", "tvdb://81797"],
                "rating_key": "117843",
                "added_at": "2026-05-24T16:17:39Z"
            }),
        ),
    )
    .await
    .expect("tautulli ingest");

    assert!(outcome.is_none());

    let row = sqlx::query(
        r#"
        SELECT mri.plex_available_at, e.media_request_id
        FROM media_request_items mri
        CROSS JOIN events e
        WHERE mri.media_request_id = (
            SELECT id FROM media_requests WHERE overseerr_request_id = 551
        )
        "#,
    )
    .fetch_one(&pool)
    .await
    .expect("row");

    assert_eq!(
        row.get::<Option<String>, _>("plex_available_at").as_deref(),
        None
    );
    assert_eq!(row.get::<Option<i64>, _>("media_request_id"), None);
}

#[tokio::test]
async fn request_upsert_reconciles_prior_unmatched_tautulli_by_normalized_title() {
    let dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!("sqlite://{}?mode=rwc", dir.path().join("test.db").display());
    let pool = connect(&database_url).await.expect("db connect");

    let event = generic_media_event(
        EventSource::Tautulli,
        "recently_added",
        json!({
            "event_type": "recently_added",
            "external_id": "plex-rating-key-limey",
            "media_type": "movie",
            "title": "The Limey",
            "year": 1999,
            "added_at": 1780381340,
            "observed_at": "2026-06-02T06:22:20Z"
        }),
    );
    ingest_event(&pool, event).await.expect("event ingest");

    upsert_media_request(
        &pool,
        IncomingRequest {
            overseerr_request_id: Some(549),
            identity: MediaIdentity {
                media_type: MediaType::Movie,
                tmdb_id: Some(10388),
                tvdb_id: None,
                imdb_id: None,
                title: Some("The Limey (1999)".to_string()),
                year: Some(1999),
                season_number: None,
                episode_number: None,
                identifiers: Vec::new(),
            },
            items: vec![MediaRequestItemInput {
                season_number: None,
                episode_number: None,
                title: None,
                air_date: None,
                availability_class: AvailabilityClass::Existing,
            }],
            title: "The Limey (1999)".to_string(),
            requested_by: Some("user".to_string()),
            requested_at: Utc.with_ymd_and_hms(2026, 6, 2, 6, 4, 9).unwrap(),
        },
    )
    .await
    .expect("request insert");

    let row = sqlx::query(
        "SELECT plex_available_at FROM media_requests WHERE overseerr_request_id = 549",
    )
    .fetch_one(&pool)
    .await
    .expect("request row");
    assert_eq!(
        row.get::<Option<String>, _>("plex_available_at").as_deref(),
        Some("2026-06-02T06:22:20+00:00")
    );
}

#[tokio::test]
async fn overseerr_auto_approved_does_not_count_as_available_notification() {
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
                identifiers: Vec::new(),
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

    assert_eq!(
        row.get::<Option<String>, _>("overseerr_notification_sent_at"),
        None
    );
}

#[tokio::test]
async fn overseerr_media_available_sets_available_notification_timestamp() {
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
                identifiers: Vec::new(),
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
        "notification",
        json!({
            "notification_type": "MEDIA_AVAILABLE",
            "request": {
                "id": "548",
                "type": "movie",
                "title": "Blood Simple (1985)",
                "createdAt": "2026-06-02T04:44:59Z",
                "media": {
                    "mediaType": "movie",
                    "tmdbId": "11368",
                    "imdbId": "tt0086979",
                    "title": "Blood Simple (1985)"
                }
            },
            "media": {
                "mediaType": "movie",
                "tmdbId": "11368",
                "imdbId": "tt0086979",
                "title": "Blood Simple (1985)"
            },
            "subject": "Blood Simple (1985)",
            "message": "Movie is now available.",
            "observed_at": "2026-06-02T04:53:49Z"
        }),
    );
    ingest_event(&pool, event).await.expect("event ingest");

    let row = sqlx::query(
        "SELECT overseerr_notification_sent_at FROM media_requests WHERE overseerr_request_id = 548",
    )
    .fetch_one(&pool)
    .await
    .expect("request row");

    assert_eq!(
        row.get::<Option<String>, _>("overseerr_notification_sent_at")
            .as_deref(),
        Some("2026-06-02T04:53:49+00:00")
    );
}

#[tokio::test]
async fn overseerr_media_available_replaces_pre_plex_notification_timestamp() {
    let dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!("sqlite://{}?mode=rwc", dir.path().join("test.db").display());
    let pool = connect(&database_url).await.expect("db connect");

    let media_request_id = upsert_media_request(
        &pool,
        IncomingRequest {
            overseerr_request_id: Some(549),
            identity: MediaIdentity {
                media_type: MediaType::Movie,
                tmdb_id: Some(10388),
                tvdb_id: None,
                imdb_id: None,
                title: Some("The Limey (1999)".to_string()),
                year: Some(1999),
                season_number: None,
                episode_number: None,
                identifiers: Vec::new(),
            },
            items: vec![MediaRequestItemInput {
                season_number: None,
                episode_number: None,
                title: None,
                air_date: None,
                availability_class: AvailabilityClass::Existing,
            }],
            title: "The Limey (1999)".to_string(),
            requested_by: Some("user".to_string()),
            requested_at: Utc.with_ymd_and_hms(2026, 6, 2, 6, 4, 9).unwrap(),
        },
    )
    .await
    .expect("request insert");

    sqlx::query(
        r#"
        UPDATE media_requests
        SET plex_available_at = '2026-06-02T06:22:20+00:00',
            overseerr_notification_sent_at = '2026-06-02T06:04:09+00:00'
        WHERE id = ?
        "#,
    )
    .bind(media_request_id)
    .execute(&pool)
    .await
    .expect("request update");
    sqlx::query(
        r#"
        UPDATE media_request_items
        SET plex_available_at = '2026-06-02T06:22:20+00:00',
            overseerr_notification_sent_at = '2026-06-02T06:04:09+00:00'
        WHERE media_request_id = ?
        "#,
    )
    .bind(media_request_id)
    .execute(&pool)
    .await
    .expect("item update");

    let event = generic_media_event(
        EventSource::Overseerr,
        "notification",
        json!({
            "notification_type": "MEDIA_AVAILABLE",
            "request": {
                "id": "549",
                "type": "movie",
                "title": "The Limey (1999)",
                "media": {
                    "mediaType": "movie",
                    "tmdbId": "10388",
                    "title": "The Limey (1999)"
                }
            },
            "observed_at": "2026-06-02T11:02:57Z"
        }),
    );
    ingest_event(&pool, event).await.expect("event ingest");

    let row = sqlx::query(
        r#"
        SELECT mr.overseerr_notification_sent_at AS request_notification,
               mri.overseerr_notification_sent_at AS item_notification
        FROM media_requests mr
        JOIN media_request_items mri ON mri.media_request_id = mr.id
        WHERE mr.id = ?
        "#,
    )
    .bind(media_request_id)
    .fetch_one(&pool)
    .await
    .expect("request row");

    assert_eq!(
        row.get::<Option<String>, _>("request_notification")
            .as_deref(),
        Some("2026-06-02T11:02:57+00:00")
    );
    assert_eq!(
        row.get::<Option<String>, _>("item_notification").as_deref(),
        Some("2026-06-02T11:02:57+00:00")
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
                identifiers: Vec::new(),
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
                identifiers: Vec::new(),
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

#[tokio::test]
async fn sonarr_grab_reports_episode_air_to_download_start_latency() {
    let dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!("sqlite://{}?mode=rwc", dir.path().join("test.db").display());
    let pool = connect(&database_url).await.expect("db connect");

    upsert_media_request(
        &pool,
        IncomingRequest {
            overseerr_request_id: Some(456),
            identity: MediaIdentity {
                media_type: MediaType::Series,
                tmdb_id: Some(278174),
                tvdb_id: Some(457154),
                imdb_id: None,
                title: Some("Girl Rules".to_string()),
                year: None,
                season_number: Some(1),
                episode_number: None,
                identifiers: Vec::new(),
            },
            items: vec![MediaRequestItemInput {
                season_number: Some(1),
                episode_number: None,
                title: None,
                air_date: None,
                availability_class: AvailabilityClass::Unknown,
            }],
            title: "Girl Rules".to_string(),
            requested_by: Some("user".to_string()),
            requested_at: Utc.with_ymd_and_hms(2026, 3, 9, 15, 10, 14).unwrap(),
        },
    )
    .await
    .expect("request insert");

    ingest_event(
        &pool,
        arr_event(
            EventSource::Sonarr,
            json!({
                "eventType": "Grab",
                "series": {
                    "title": "Girl Rules",
                    "tvdbId": 457154,
                    "tmdbId": 278174,
                    "type": "standard"
                },
                "episodes": [{
                    "seasonNumber": 1,
                    "episodeNumber": 12,
                    "airDateUtc": "2026-06-01T13:30:00Z"
                }],
                "downloadId": "download-1",
                "observed_at": "2026-06-02T12:15:02Z"
            }),
        ),
    )
    .await
    .expect("sonarr grab ingest");

    let metrics = render_metrics(&pool).await.expect("metrics render");
    assert!(
        metrics
            .contains("media_episode_air_to_download_started_seconds_sum{source=\"sonarr\"} 81902")
    );
}
