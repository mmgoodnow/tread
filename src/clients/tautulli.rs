use reqwest::Client;
use serde::Deserialize;
use sqlx::SqlitePool;
use url::Url;

use crate::{
    clients::webhook::generic_media_event, config::TautulliSettings, core::model::EventSource,
    db::ingest_event,
};

#[derive(Clone)]
pub struct TautulliClient {
    http: Client,
    base_url: Url,
    api_key: String,
}

impl TautulliClient {
    pub fn from_settings(settings: &TautulliSettings) -> Option<Self> {
        Some(Self {
            http: Client::new(),
            base_url: with_trailing_slash(settings.base_url.clone()?),
            api_key: settings.api_key.clone()?,
        })
    }

    pub async fn poll_recently_added(&self, pool: &SqlitePool) -> anyhow::Result<usize> {
        let mut url = self.base_url.join("api/v2")?;
        url.query_pairs_mut()
            .append_pair("apikey", &self.api_key)
            .append_pair("cmd", "get_recently_added")
            .append_pair("count", "50");

        let response: TautulliResponse = self
            .http
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        let mut ingested = 0;
        for item in response.response.data.recently_added {
            let event = generic_media_event(EventSource::Tautulli, "recently_added", item);
            ingest_event(pool, event).await?;
            ingested += 1;
        }
        Ok(ingested)
    }
}

fn with_trailing_slash(mut url: Url) -> Url {
    if !url.path().ends_with('/') {
        url.set_path(&format!("{}/", url.path()));
    }
    url
}

#[derive(Debug, Deserialize)]
struct TautulliResponse {
    response: TautulliEnvelope,
}

#[derive(Debug, Deserialize)]
struct TautulliEnvelope {
    data: RecentlyAddedData,
}

#[derive(Debug, Deserialize)]
struct RecentlyAddedData {
    #[serde(default)]
    recently_added: Vec<serde_json::Value>,
}
