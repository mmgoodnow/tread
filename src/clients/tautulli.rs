use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;
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
            let item = self.enrich_recently_added_item(item).await?;
            let event = generic_media_event(EventSource::Tautulli, "recently_added", item);
            ingest_event(pool, event).await?;
            ingested += 1;
        }
        Ok(ingested)
    }

    pub async fn enrich_recently_added_item(&self, item: Value) -> anyhow::Result<Value> {
        let Some(rating_key) = item
            .get("rating_key")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string)
            .or_else(|| {
                item.get("rating_key")
                    .and_then(Value::as_i64)
                    .map(|value| value.to_string())
            })
        else {
            return Ok(item);
        };

        let mut url = self.base_url.join("api/v2")?;
        url.query_pairs_mut()
            .append_pair("apikey", &self.api_key)
            .append_pair("cmd", "get_metadata")
            .append_pair("rating_key", &rating_key);

        let response: TautulliMetadataResponse = self
            .http
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        Ok(merge_missing_json(item, response.response.data))
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
    recently_added: Vec<Value>,
}

#[derive(Debug, Deserialize)]
struct TautulliMetadataResponse {
    response: TautulliMetadataEnvelope,
}

#[derive(Debug, Deserialize)]
struct TautulliMetadataEnvelope {
    data: Value,
}

fn merge_missing_json(mut item: Value, metadata: Value) -> Value {
    let (Some(item), Some(metadata)) = (item.as_object_mut(), metadata.as_object()) else {
        return item;
    };

    for (key, value) in metadata {
        let should_insert = match item.get(key) {
            None | Some(Value::Null) => true,
            Some(Value::String(value)) => value.trim().is_empty(),
            Some(Value::Array(value)) => value.is_empty(),
            _ => false,
        };
        if should_insert {
            item.insert(key.clone(), value.clone());
        }
    }

    Value::Object(item.clone())
}
