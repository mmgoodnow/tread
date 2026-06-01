CREATE TABLE IF NOT EXISTS media_request_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    media_request_id INTEGER NOT NULL REFERENCES media_requests(id) ON DELETE CASCADE,
    media_type TEXT NOT NULL CHECK (media_type IN ('movie', 'series')),
    season_number INTEGER,
    episode_number INTEGER,
    title TEXT,
    air_date TEXT,
    requested_at TEXT NOT NULL,
    availability_class TEXT NOT NULL DEFAULT 'unknown' CHECK (availability_class IN ('existing', 'future_airing', 'unknown')),
    download_started_at TEXT,
    download_finished_at TEXT,
    sonarr_grabbed_at TEXT,
    sonarr_imported_at TEXT,
    radarr_grabbed_at TEXT,
    radarr_imported_at TEXT,
    plex_available_at TEXT,
    overseerr_notification_sent_at TEXT,
    match_confidence REAL NOT NULL DEFAULT 1.0,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_media_request_items_unique_part
    ON media_request_items(
        media_request_id,
        COALESCE(season_number, -1),
        COALESCE(episode_number, -1)
    );

CREATE INDEX IF NOT EXISTS idx_media_request_items_request
    ON media_request_items(media_request_id);

CREATE INDEX IF NOT EXISTS idx_media_request_items_lifecycle
    ON media_request_items(media_type, availability_class, requested_at);

INSERT OR IGNORE INTO media_request_items (
    media_request_id, media_type, season_number, episode_number, title, requested_at,
    availability_class, download_started_at, download_finished_at, sonarr_grabbed_at,
    sonarr_imported_at, radarr_grabbed_at, radarr_imported_at, plex_available_at,
    overseerr_notification_sent_at, match_confidence, created_at, updated_at
)
SELECT
    id, media_type, season_number, episode_number, title, requested_at,
    'unknown', download_started_at, download_finished_at, sonarr_grabbed_at,
    sonarr_imported_at, radarr_grabbed_at, radarr_imported_at, plex_available_at,
    overseerr_notification_sent_at, match_confidence, created_at, updated_at
FROM media_requests;

ALTER TABLE events ADD COLUMN media_request_item_id INTEGER REFERENCES media_request_items(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_events_media_request_item_id
    ON events(media_request_item_id);
