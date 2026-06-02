use reqwest::Client;
use serde::Deserialize;
use sqlx::SqlitePool;
use url::Url;

use crate::{
    clients::webhook::overseerr_request_from_payload, config::OverseerrSettings,
    db::upsert_media_request,
};

#[derive(Clone)]
pub struct OverseerrClient {
    http: Client,
    base_url: Url,
    api_key: String,
}

impl OverseerrClient {
    pub fn from_settings(settings: &OverseerrSettings) -> Option<Self> {
        Some(Self {
            http: Client::new(),
            base_url: with_trailing_slash(settings.base_url.clone()?),
            api_key: settings.api_key.clone()?,
        })
    }

    pub async fn poll_requests(&self, pool: &SqlitePool) -> anyhow::Result<usize> {
        let mut url = self.base_url.join("api/v1/request")?;
        url.query_pairs_mut()
            .append_pair("take", "100")
            .append_pair("skip", "0")
            .append_pair("sort", "added");

        let response: OverseerrRequestPage = self
            .http
            .get(url)
            .header("X-Api-Key", &self.api_key)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        let mut saved = 0;
        for item in response.results {
            if let Some(request) = overseerr_request_from_payload(&serde_json::to_value(item)?) {
                upsert_media_request(pool, request).await?;
                saved += 1;
            }
        }
        Ok(saved)
    }
}

fn with_trailing_slash(mut url: Url) -> Url {
    if !url.path().ends_with('/') {
        url.set_path(&format!("{}/", url.path()));
    }
    url
}

#[derive(Debug, Deserialize, serde::Serialize)]
struct OverseerrRequestPage {
    results: Vec<serde_json::Value>,
}
