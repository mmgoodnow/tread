CREATE TABLE IF NOT EXISTS media_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    overseerr_request_id INTEGER UNIQUE,
    media_type TEXT NOT NULL CHECK (media_type IN ('movie', 'series')),
    tmdb_id INTEGER,
    tvdb_id INTEGER,
    imdb_id TEXT,
    title TEXT NOT NULL,
    year INTEGER,
    season_number INTEGER,
    episode_number INTEGER,
    requested_by TEXT,
    requested_at TEXT NOT NULL,
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

CREATE INDEX IF NOT EXISTS idx_media_requests_tmdb
    ON media_requests(media_type, tmdb_id)
    WHERE tmdb_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_media_requests_tvdb
    ON media_requests(media_type, tvdb_id)
    WHERE tvdb_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_media_requests_imdb
    ON media_requests(media_type, imdb_id)
    WHERE imdb_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_media_requests_title_year
    ON media_requests(media_type, title, year);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL CHECK (source IN ('overseerr', 'sonarr', 'radarr', 'torrent', 'plex', 'tautulli')),
    event_type TEXT NOT NULL,
    media_request_id INTEGER REFERENCES media_requests(id) ON DELETE SET NULL,
    external_id TEXT,
    payload_json TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    UNIQUE (source, event_type, external_id)
);

CREATE INDEX IF NOT EXISTS idx_events_media_request_id
    ON events(media_request_id);

CREATE INDEX IF NOT EXISTS idx_events_source_event_type
    ON events(source, event_type);

CREATE INDEX IF NOT EXISTS idx_events_unmatched_source
    ON events(source)
    WHERE media_request_id IS NULL;
