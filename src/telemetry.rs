use prometheus::{
    Encoder, GaugeVec, HistogramOpts, HistogramVec, IntCounterVec, Opts, Registry, TextEncoder,
};
use sqlx::{Row, SqlitePool};

const REQUEST_LIFECYCLE_BUCKETS: &[f64] = &[
    1.0,
    5.0,
    10.0,
    30.0,
    60.0,
    120.0,
    300.0,
    600.0,
    900.0,
    1_800.0,
    3_600.0,
    7_200.0,
    14_400.0,
    28_800.0,
    86_400.0,
    172_800.0,
    604_800.0,
    2_592_000.0,
];

const EPISODE_AIR_BUCKETS: &[f64] = &[
    300.0,
    600.0,
    1_800.0,
    3_600.0,
    7_200.0,
    14_400.0,
    28_800.0,
    86_400.0,
    172_800.0,
    604_800.0,
    1_209_600.0,
    2_592_000.0,
    7_776_000.0,
];

pub async fn render_metrics(pool: &SqlitePool) -> anyhow::Result<String> {
    let registry = Registry::new();

    let request_to_plex = HistogramVec::new(
        HistogramOpts::new(
            "media_request_to_plex_available_seconds",
            "Seconds from request submission to Plex availability.",
        )
        .buckets(REQUEST_LIFECYCLE_BUCKETS.to_vec()),
        &["media_type", "source"],
    )?;
    let request_to_download_started = HistogramVec::new(
        HistogramOpts::new(
            "media_request_to_download_started_seconds",
            "Seconds from request submission to BitTorrent download start.",
        )
        .buckets(REQUEST_LIFECYCLE_BUCKETS.to_vec()),
        &["media_type", "download_client"],
    )?;
    let request_to_download_finished = HistogramVec::new(
        HistogramOpts::new(
            "media_request_to_download_finished_seconds",
            "Seconds from request submission to BitTorrent download completion.",
        )
        .buckets(REQUEST_LIFECYCLE_BUCKETS.to_vec()),
        &["media_type", "download_client"],
    )?;
    let request_to_notification = HistogramVec::new(
        HistogramOpts::new(
            "media_request_to_overseerr_notification_seconds",
            "Seconds from request submission to Overseerr notification or email.",
        )
        .buckets(REQUEST_LIFECYCLE_BUCKETS.to_vec()),
        &["media_type", "notification_type"],
    )?;
    let request_to_first_available = HistogramVec::new(
        HistogramOpts::new(
            "media_request_to_first_available_seconds",
            "Seconds from request submission to the first requested item becoming available.",
        )
        .buckets(REQUEST_LIFECYCLE_BUCKETS.to_vec()),
        &["media_type", "availability_class"],
    )?;
    let item_to_plex = HistogramVec::new(
        HistogramOpts::new(
            "media_request_item_to_plex_available_seconds",
            "Seconds from item request submission to Plex availability.",
        )
        .buckets(REQUEST_LIFECYCLE_BUCKETS.to_vec()),
        &["media_type", "availability_class", "source"],
    )?;
    let episode_air_to_plex = HistogramVec::new(
        HistogramOpts::new(
            "media_episode_air_to_plex_available_seconds",
            "Seconds from episode air date to Plex availability for future-airing items.",
        )
        .buckets(EPISODE_AIR_BUCKETS.to_vec()),
        &["source"],
    )?;

    let requests_total = IntCounterVec::new(
        Opts::new("media_requests_total", "Total tracked media requests."),
        &["media_type"],
    )?;
    let events_total = IntCounterVec::new(
        Opts::new(
            "media_request_events_total",
            "Total lifecycle events observed.",
        ),
        &["source", "event_type"],
    )?;
    let failures_total = IntCounterVec::new(
        Opts::new(
            "media_request_lifecycle_failures_total",
            "Lifecycle failures detected by the correlator.",
        ),
        &["stage", "reason"],
    )?;
    let inflight = GaugeVec::new(
        Opts::new(
            "media_request_lifecycle_inflight",
            "Requests submitted but not yet completed at a lifecycle stage.",
        ),
        &["stage"],
    )?;
    let unmatched = GaugeVec::new(
        Opts::new(
            "media_request_lifecycle_unmatched_events",
            "Lifecycle events without a matching media request.",
        ),
        &["source"],
    )?;

    registry.register(Box::new(request_to_plex.clone()))?;
    registry.register(Box::new(request_to_download_started.clone()))?;
    registry.register(Box::new(request_to_download_finished.clone()))?;
    registry.register(Box::new(request_to_notification.clone()))?;
    registry.register(Box::new(request_to_first_available.clone()))?;
    registry.register(Box::new(item_to_plex.clone()))?;
    registry.register(Box::new(episode_air_to_plex.clone()))?;
    registry.register(Box::new(requests_total.clone()))?;
    registry.register(Box::new(events_total.clone()))?;
    registry.register(Box::new(failures_total.clone()))?;
    registry.register(Box::new(inflight.clone()))?;
    registry.register(Box::new(unmatched.clone()))?;

    let request_rows = sqlx::query(
        r#"
        SELECT media_type, COUNT(*) AS count
        FROM media_requests
        GROUP BY media_type
        "#,
    )
    .fetch_all(pool)
    .await?;

    for row in request_rows {
        let media_type: String = row.get("media_type");
        let count: i64 = row.get("count");
        requests_total
            .with_label_values(&[&media_type])
            .inc_by(count.try_into()?);
    }

    let rows = sqlx::query(
        r#"
        SELECT media_type, availability_class, requested_at, air_date,
               download_started_at, download_finished_at,
               plex_available_at, overseerr_notification_sent_at
        FROM media_request_items
        "#,
    )
    .fetch_all(pool)
    .await?;

    let mut awaiting_download_start = 0.0;
    let mut awaiting_download_finish = 0.0;
    let mut awaiting_plex = 0.0;
    let mut awaiting_notification = 0.0;

    for row in rows {
        let media_type: String = row.get("media_type");
        let availability_class: String = row.get("availability_class");
        let requested_at: String = row.get("requested_at");
        let plex_available_at: Option<String> = row.get("plex_available_at");

        if availability_class == "future_airing" {
            let air_date: Option<String> = row.get("air_date");
            if let Some(air_date) = air_date {
                observe_duration(
                    &episode_air_to_plex,
                    &["tautulli"],
                    &air_date,
                    plex_available_at.clone(),
                )?;
            }
        } else {
            observe_duration(
                &request_to_plex,
                &[&media_type, "tautulli"],
                &requested_at,
                plex_available_at.clone(),
            )?;
            observe_duration(
                &item_to_plex,
                &[&media_type, &availability_class, "tautulli"],
                &requested_at,
                plex_available_at.clone(),
            )?;
        }
        observe_duration(
            &request_to_download_started,
            &[&media_type, "rtorrent"],
            &requested_at,
            row.get("download_started_at"),
        )?;
        observe_duration(
            &request_to_download_finished,
            &[&media_type, "rtorrent"],
            &requested_at,
            row.get("download_finished_at"),
        )?;
        observe_duration(
            &request_to_notification,
            &[&media_type, "best_effort"],
            &requested_at,
            row.get("overseerr_notification_sent_at"),
        )?;

        if row
            .get::<Option<String>, _>("download_started_at")
            .is_none()
        {
            awaiting_download_start += 1.0;
        }
        if row
            .get::<Option<String>, _>("download_finished_at")
            .is_none()
        {
            awaiting_download_finish += 1.0;
        }
        if plex_available_at.is_none() {
            awaiting_plex += 1.0;
        }
        if row
            .get::<Option<String>, _>("overseerr_notification_sent_at")
            .is_none()
        {
            awaiting_notification += 1.0;
        }
    }

    for row in sqlx::query(
        r#"
        SELECT mr.media_type, mri.availability_class, mr.requested_at,
               MIN(mri.plex_available_at) AS first_available_at
        FROM media_requests mr
        JOIN media_request_items mri ON mri.media_request_id = mr.id
        WHERE mri.plex_available_at IS NOT NULL
          AND mri.availability_class != 'future_airing'
        GROUP BY mr.id, mr.media_type, mri.availability_class, mr.requested_at
        "#,
    )
    .fetch_all(pool)
    .await?
    {
        let media_type: String = row.get("media_type");
        let availability_class: String = row.get("availability_class");
        let requested_at: String = row.get("requested_at");
        observe_duration(
            &request_to_first_available,
            &[&media_type, &availability_class],
            &requested_at,
            row.get("first_available_at"),
        )?;
    }

    inflight
        .with_label_values(&["download_started"])
        .set(awaiting_download_start);
    inflight
        .with_label_values(&["download_finished"])
        .set(awaiting_download_finish);
    inflight
        .with_label_values(&["plex_available"])
        .set(awaiting_plex);
    inflight
        .with_label_values(&["overseerr_notification"])
        .set(awaiting_notification);

    for row in sqlx::query(
        "SELECT source, event_type, COUNT(*) AS count FROM events GROUP BY source, event_type",
    )
    .fetch_all(pool)
    .await?
    {
        let source: String = row.get("source");
        let event_type: String = row.get("event_type");
        let count: i64 = row.get("count");
        events_total
            .with_label_values(&[&source, &event_type])
            .inc_by(count.try_into()?);
    }

    for row in sqlx::query("SELECT source, COUNT(*) AS count FROM events WHERE media_request_id IS NULL GROUP BY source")
        .fetch_all(pool)
        .await?
    {
        let source: String = row.get("source");
        let count: i64 = row.get("count");
        unmatched.with_label_values(&[&source]).set(count as f64);
    }

    failures_total
        .with_label_values(&["correlation", "unmatched_event"])
        .inc_by(unmatched_total(pool).await?.try_into()?);

    let encoder = TextEncoder::new();
    let mut buffer = Vec::new();
    encoder.encode(&registry.gather(), &mut buffer)?;
    Ok(String::from_utf8(buffer)?)
}

fn observe_duration(
    histogram: &HistogramVec,
    labels: &[&str],
    requested_at: &str,
    completed_at: Option<String>,
) -> anyhow::Result<()> {
    let Some(completed_at) = completed_at else {
        return Ok(());
    };
    let requested_at = chrono::DateTime::parse_from_rfc3339(requested_at)?;
    let completed_at = chrono::DateTime::parse_from_rfc3339(&completed_at)?;
    let seconds = completed_at
        .signed_duration_since(requested_at)
        .num_seconds();
    if seconds >= 0 {
        histogram.with_label_values(labels).observe(seconds as f64);
    }
    Ok(())
}

async fn unmatched_total(pool: &SqlitePool) -> anyhow::Result<u64> {
    let row = sqlx::query("SELECT COUNT(*) AS count FROM events WHERE media_request_id IS NULL")
        .fetch_one(pool)
        .await?;
    let count: i64 = row.get("count");
    Ok(count.try_into()?)
}
