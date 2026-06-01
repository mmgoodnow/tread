use std::{net::SocketAddr, path::PathBuf, time::Duration};

use figment::{
    Figment,
    providers::{Env, Format, Serialized, Toml},
};
use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct Settings {
    pub bind_addr: SocketAddr,
    pub database_url: String,
    pub poll_interval_seconds: u64,
    pub overseerr: OverseerrSettings,
    pub tautulli: TautulliSettings,
    pub qbittorrent: QbittorrentSettings,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct OverseerrSettings {
    pub base_url: Option<Url>,
    pub api_key: Option<String>,
    pub enabled: bool,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct TautulliSettings {
    pub base_url: Option<Url>,
    pub api_key: Option<String>,
    pub enabled: bool,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct QbittorrentSettings {
    pub base_url: Option<Url>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub enabled: bool,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:80".parse().expect("valid default bind addr"),
            database_url: "sqlite://data/tread.db?mode=rwc".to_string(),
            poll_interval_seconds: 60,
            overseerr: OverseerrSettings::default(),
            tautulli: TautulliSettings::default(),
            qbittorrent: QbittorrentSettings::default(),
        }
    }
}

impl Settings {
    pub fn load(config_path: Option<PathBuf>) -> anyhow::Result<Self> {
        let mut figment = Figment::from(Serialized::defaults(Settings::default()));

        if let Some(path) = config_path {
            figment = figment.merge(Toml::file(path));
        }

        figment = figment.merge(Env::prefixed("TREAD_").split("__"));
        Ok(figment.extract()?)
    }

    pub fn poll_interval(&self) -> Duration {
        Duration::from_secs(self.poll_interval_seconds.max(5))
    }
}
