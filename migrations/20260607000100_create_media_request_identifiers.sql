CREATE TABLE IF NOT EXISTS media_request_identifiers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    namespace TEXT NOT NULL,
    value TEXT NOT NULL,
    media_type TEXT NOT NULL CHECK (media_type IN ('movie', 'series')),
    media_request_id INTEGER NOT NULL REFERENCES media_requests(id) ON DELETE CASCADE,
    media_request_item_id INTEGER REFERENCES media_request_items(id) ON DELETE SET NULL,
    confidence REAL NOT NULL DEFAULT 1.0,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    UNIQUE(namespace, value, media_type)
);

CREATE INDEX IF NOT EXISTS idx_media_request_identifiers_request
    ON media_request_identifiers(media_request_id);

CREATE INDEX IF NOT EXISTS idx_media_request_identifiers_lookup
    ON media_request_identifiers(namespace, value, media_type);
