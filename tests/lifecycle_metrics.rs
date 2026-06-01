use chrono::{TimeZone, Utc};
use serde_json::json;
use tread::{
    clients::webhook::generic_media_event,
    core::model::{EventSource, IncomingRequest, MediaIdentity, MediaType},
    db::{connect, ingest_event, upsert_media_request},
    telemetry::render_metrics,
};

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
