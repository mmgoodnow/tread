use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MediaType {
    Movie,
    Series,
}

impl MediaType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Movie => "movie",
            Self::Series => "series",
        }
    }
}

impl TryFrom<&str> for MediaType {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "movie" => Ok(Self::Movie),
            "series" | "tv" => Ok(Self::Series),
            other => anyhow::bail!("unsupported media type: {other}"),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum EventSource {
    Overseerr,
    Sonarr,
    Radarr,
    Torrent,
    Plex,
    Tautulli,
}

impl EventSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Overseerr => "overseerr",
            Self::Sonarr => "sonarr",
            Self::Radarr => "radarr",
            Self::Torrent => "torrent",
            Self::Plex => "plex",
            Self::Tautulli => "tautulli",
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MediaIdentity {
    pub media_type: MediaType,
    pub tmdb_id: Option<i64>,
    pub tvdb_id: Option<i64>,
    pub imdb_id: Option<String>,
    pub title: Option<String>,
    pub year: Option<i64>,
    pub season_number: Option<i64>,
    pub episode_number: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct IncomingRequest {
    pub overseerr_request_id: Option<i64>,
    pub identity: MediaIdentity,
    pub items: Vec<MediaRequestItemInput>,
    pub title: String,
    pub requested_by: Option<String>,
    pub requested_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct MediaRequestItemInput {
    pub season_number: Option<i64>,
    pub episode_number: Option<i64>,
    pub title: Option<String>,
    pub air_date: Option<DateTime<Utc>>,
    pub availability_class: AvailabilityClass,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AvailabilityClass {
    Existing,
    FutureAiring,
    Unknown,
}

impl AvailabilityClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Existing => "existing",
            Self::FutureAiring => "future_airing",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone)]
pub struct EventIngest {
    pub source: EventSource,
    pub event_type: String,
    pub external_id: Option<String>,
    pub identity: Option<MediaIdentity>,
    pub observed_at: DateTime<Utc>,
    pub payload_json: serde_json::Value,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MatchOutcome {
    pub media_request_id: i64,
    pub media_request_item_id: Option<i64>,
    pub confidence: f64,
}
