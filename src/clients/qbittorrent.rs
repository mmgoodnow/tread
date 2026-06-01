use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use sqlx::SqlitePool;
use url::Url;

use crate::{
    clients::webhook::generic_media_event,
    config::QbittorrentSettings,
    core::model::{EventSource, MediaType},
    db::ingest_event,
};

#[derive(Clone)]
pub struct QbittorrentClient {
    http: Client,
    base_url: Url,
    username: String,
    password: String,
}

impl QbittorrentClient {
    pub fn from_settings(settings: &QbittorrentSettings) -> Option<Self> {
        Some(Self {
            http: Client::builder().cookie_store(true).build().ok()?,
            base_url: settings.base_url.clone()?,
            username: settings.username.clone()?,
            password: settings.password.clone()?,
        })
    }

    pub async fn poll_torrents(&self, pool: &SqlitePool) -> anyhow::Result<usize> {
        self.login().await?;
        let torrents: Vec<TorrentInfo> = self
            .http
            .get(self.base_url.join("api/v2/torrents/info")?)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        let mut ingested = 0;
        for torrent in torrents {
            let Some(media_type) = infer_media_type(&torrent) else {
                continue;
            };

            let started = json!({
                "event_type": "download_started",
                "external_id": torrent.hash,
                "media_type": media_type.as_str(),
                "title": torrent.name,
                "download_client": "qbittorrent",
                "observed_at": unix_to_rfc3339(torrent.added_on),
                "raw": torrent,
            });
            ingest_event(
                pool,
                generic_media_event(EventSource::Torrent, "download_started", started),
            )
            .await?;
            ingested += 1;

            if torrent.completion_on.unwrap_or(-1) > 0 || torrent.progress >= 1.0 {
                let finished = json!({
                    "event_type": "download_finished",
                    "external_id": format!("{}:finished", torrent.hash),
                    "media_type": media_type.as_str(),
                    "title": torrent.name,
                    "download_client": "qbittorrent",
                    "observed_at": unix_to_rfc3339(torrent.completion_on.unwrap_or_default()),
                    "raw": torrent,
                });
                ingest_event(
                    pool,
                    generic_media_event(EventSource::Torrent, "download_finished", finished),
                )
                .await?;
                ingested += 1;
            }
        }

        Ok(ingested)
    }

    async fn login(&self) -> anyhow::Result<()> {
        self.http
            .post(self.base_url.join("api/v2/auth/login")?)
            .form(&[
                ("username", self.username.as_str()),
                ("password", self.password.as_str()),
            ])
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }
}

fn infer_media_type(torrent: &TorrentInfo) -> Option<MediaType> {
    let category = torrent.category.to_ascii_lowercase();
    let tags = torrent.tags.to_ascii_lowercase();
    if category.contains("radarr") || tags.contains("radarr") || category == "movies" {
        Some(MediaType::Movie)
    } else if category.contains("sonarr") || tags.contains("sonarr") || category == "tv" {
        Some(MediaType::Series)
    } else {
        None
    }
}

fn unix_to_rfc3339(timestamp: i64) -> String {
    chrono::DateTime::from_timestamp(timestamp.max(0), 0)
        .unwrap_or_else(chrono::Utc::now)
        .to_rfc3339()
}

#[derive(Debug, Clone, Deserialize, serde::Serialize)]
struct TorrentInfo {
    hash: String,
    name: String,
    #[serde(default)]
    category: String,
    #[serde(default)]
    tags: String,
    #[serde(default)]
    progress: f64,
    #[serde(default)]
    added_on: i64,
    completion_on: Option<i64>,
}
