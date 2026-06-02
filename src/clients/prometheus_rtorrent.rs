use chrono::Utc;
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use sqlx::SqlitePool;
use url::Url;

use crate::{
    clients::webhook::generic_media_event,
    config::PrometheusSettings,
    core::model::{EventSource, MediaType},
    db::ingest_event,
};

#[derive(Clone)]
pub struct PrometheusRtorrentClient {
    http: Client,
    base_url: Url,
}

impl PrometheusRtorrentClient {
    pub fn from_settings(settings: &PrometheusSettings) -> Option<Self> {
        if !settings.enabled || !settings.rtorrent_enabled {
            return None;
        }
        Some(Self {
            http: Client::new(),
            base_url: settings.base_url.clone()?,
        })
    }

    pub async fn poll_torrents(&self, pool: &SqlitePool) -> anyhow::Result<usize> {
        let mut ingested = 0;
        ingested += self
            .poll_metric(pool, "rtorrent_downloads_started", "download_started")
            .await?;
        ingested += self
            .poll_metric(pool, "rtorrent_downloads_complete", "download_finished")
            .await?;
        Ok(ingested)
    }

    async fn poll_metric(
        &self,
        pool: &SqlitePool,
        metric: &str,
        event_type: &str,
    ) -> anyhow::Result<usize> {
        let mut url = self.base_url.join("api/v1/query")?;
        url.query_pairs_mut().append_pair("query", metric);
        let response: PrometheusResponse = self
            .http
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        let mut ingested = 0;
        for sample in response.data.result {
            if sample.value.last().and_then(|value| value.as_str()) != Some("1") {
                continue;
            }
            let Some(name) = sample.metric.name else {
                continue;
            };
            let Some(media_type) = infer_media_type(&name) else {
                continue;
            };
            let title = clean_title(&name);
            let payload = json!({
                "event_type": event_type,
                "external_id": format!(
                    "{}:{}",
                    sample.metric.info_hash.unwrap_or_else(|| name.clone()),
                    event_type
                ),
                "media_type": media_type.as_str(),
                "title": title,
                "download_client": "rtorrent",
                "observed_at": Utc::now().to_rfc3339(),
                "source_metric": metric,
            });
            ingest_event(
                pool,
                generic_media_event(EventSource::Torrent, event_type, payload),
            )
            .await?;
            ingested += 1;
        }

        Ok(ingested)
    }
}

fn infer_media_type(name: &str) -> Option<MediaType> {
    let lower = name.to_ascii_lowercase();
    if looks_like_episode(&lower) {
        return Some(MediaType::Series);
    }
    if contains_year(name) {
        return Some(MediaType::Movie);
    }
    None
}

fn looks_like_episode(lower: &str) -> bool {
    let bytes = lower.as_bytes();
    bytes.windows(6).any(|window| {
        window[0] == b's'
            && window[1].is_ascii_digit()
            && window[2].is_ascii_digit()
            && window[3] == b'e'
            && window[4].is_ascii_digit()
            && window[5].is_ascii_digit()
    })
}

fn contains_year(name: &str) -> bool {
    name.split(|ch: char| !ch.is_ascii_digit()).any(|part| {
        part.len() == 4
            && part
                .parse::<i64>()
                .is_ok_and(|year| (1900..=2100).contains(&year))
    })
}

fn clean_title(name: &str) -> String {
    name.split(['.', '_'])
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_string()
}

#[derive(Debug, Deserialize)]
struct PrometheusResponse {
    data: PrometheusData,
}

#[derive(Debug, Deserialize)]
struct PrometheusData {
    result: Vec<PrometheusSample>,
}

#[derive(Debug, Deserialize)]
struct PrometheusSample {
    metric: PrometheusMetric,
    value: Vec<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct PrometheusMetric {
    name: Option<String>,
    info_hash: Option<String>,
}
