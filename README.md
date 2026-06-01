# tread

Small Rust service for observing media request lifecycle latency across Overseerr, Arr apps, qBittorrent, and Plex/Tautulli.

Prometheus is intentionally not the source of truth. `tread` stores request and raw event history in SQLite, then computes low-cardinality Prometheus metrics from that durable state at `/metrics`.

## Current slice

Implemented:

- Axum HTTP service with `/healthz` and `/metrics`
- SQLite schema and migrations for `media_requests`, `media_request_items`, and `events`
- Item-level lifecycle rows for movies, seasons, and episodes
- Idempotent Overseerr request ingestion from webhook or API polling
- Tautulli recently-added webhook/poll ingestion for request-to-Plex timing
- Sonarr/Radarr webhook ingestion for grab/import lifecycle timestamps
- qBittorrent HTTP polling for started/finished download events
- Low-cardinality Prometheus counters, gauges, and histograms
- `configure` subcommand for writing local environment files

Not implemented yet:

- Reliable Overseerr notification/email tracking beyond best-effort webhook/event ingestion

## Run locally

```sh
cargo run -- serve
```

The default database is `sqlite://data/tread.db?mode=rwc`.

Create a local env file:

```sh
cargo run -- configure \
  --output .env.local \
  --overseerr-url http://overseerr.local:5055/ \
  --overseerr-api-key "$OVERSEERR_API_KEY" \
  --tautulli-url http://tautulli.local:8181/ \
  --tautulli-api-key "$TAUTULLI_API_KEY" \
  --qbittorrent-url http://qbittorrent.local:8080/ \
  --qbittorrent-username "$QBITTORRENT_USERNAME" \
  --qbittorrent-password "$QBITTORRENT_PASSWORD"
```

Load it with your shell or Docker Compose before starting the service.

## Webhooks

Configure these URLs in the source apps when available:

- `POST /webhooks/overseerr`
- `POST /webhooks/sonarr`
- `POST /webhooks/radarr`
- `POST /webhooks/tautulli`

Raw payloads are stored in the `events` table for debugging. Correlation prefers stable IDs in this order:

1. TMDB ID
2. TVDB ID
3. IMDB ID
4. media type + normalized title + year
5. season/episode when available

Torrent names should only be used as a later fallback.

The qBittorrent poller uses only HTTP API calls. It infers media type from category/tags (`radarr`, `sonarr`, `movies`, or `tv`) and then uses the torrent name as a fallback title because qBittorrent does not carry stable TMDB/TVDB/IMDB IDs. If that inference is too weak for your setup, keep the poller disabled until we add a stronger mapping source.

## Active-airing TV

TV requests are tracked as parent requests plus child `media_request_items`. Movies get one item. TV requests create season or episode items when that detail is present.

For active-airing episodes, request-submitted latency is the wrong operational clock because the media did not exist yet. Mark those items as `future_airing` with an `air_date`; `tread` then records `media_episode_air_to_plex_available_seconds` from air date to Plex availability instead of counting the wait from the original request date. Existing or unknown items still contribute to request-to-Plex and item-to-Plex metrics.

## Metrics

Exposed histograms:

- `media_request_to_plex_available_seconds{media_type,source}`
- `media_request_to_first_available_seconds{media_type,availability_class}`
- `media_request_item_to_plex_available_seconds{media_type,availability_class,source}`
- `media_episode_air_to_plex_available_seconds{source}`
- `media_request_to_download_started_seconds{media_type,download_client}`
- `media_request_to_download_finished_seconds{media_type,download_client}`
- `media_request_to_overseerr_notification_seconds{media_type,notification_type}`

Counters:

- `media_requests_total{media_type}`
- `media_request_events_total{source,event_type}`
- `media_request_lifecycle_failures_total{stage,reason}`

Gauges:

- `media_request_lifecycle_inflight{stage}`
- `media_request_lifecycle_unmatched_events{source}`

No titles, usernames, emails, request IDs, torrent hashes, or media IDs are used as metric labels.

## Docker Compose

```sh
cp .env.example .env
docker compose up --build
```
