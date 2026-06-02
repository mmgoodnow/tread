use chrono::{DateTime, TimeZone, Utc};
use serde_json::{Value, json};

use crate::{
    core::model::{
        AvailabilityClass, EventIngest, EventSource, IncomingRequest, MediaIdentity,
        MediaRequestItemInput, MediaType,
    },
    db::parse_datetime_or_now,
};

pub fn overseerr_request_from_payload(payload: &Value) -> Option<IncomingRequest> {
    let request = payload.get("request").unwrap_or(payload);
    let media = request.get("media").or_else(|| payload.get("media"))?;
    let media_type = text_at(request, &["type"])
        .or_else(|| text_at(media, &["mediaType"]))
        .or_else(|| text_at(payload, &["media_type"]))
        .and_then(|value| MediaType::try_from(value.as_str()).ok())?;

    let title = text_at(request, &["title"])
        .or_else(|| text_at(media, &["title"]))
        .or_else(|| text_at(media, &["name"]))
        .or_else(|| text_at(media, &["externalServiceSlug"]))
        .unwrap_or_else(|| fallback_title(media, media_type));

    let season_number =
        int_at(request, &["seasonNumber"]).or_else(|| int_at(payload, &["season_number"]));
    let episode_number =
        int_at(request, &["episodeNumber"]).or_else(|| int_at(payload, &["episode_number"]));
    let items = request_items_from_payload(request, media_type, season_number, episode_number);

    Some(IncomingRequest {
        overseerr_request_id: int_at(request, &["id"]).or_else(|| int_at(payload, &["request_id"])),
        identity: MediaIdentity {
            media_type,
            tmdb_id: int_at(media, &["tmdbId"]).or_else(|| int_at(payload, &["tmdb_id"])),
            tvdb_id: int_at(media, &["tvdbId"]).or_else(|| int_at(payload, &["tvdb_id"])),
            imdb_id: text_at(media, &["imdbId"]).or_else(|| text_at(payload, &["imdb_id"])),
            title: Some(title.clone()),
            year: int_at(media, &["year"]).or_else(|| int_at(request, &["year"])),
            season_number,
            episode_number,
        },
        items,
        title,
        requested_by: text_at(request, &["requestedBy", "displayName"])
            .or_else(|| text_at(payload, &["requested_by"])),
        requested_at: parse_datetime_or_now(
            request
                .get("createdAt")
                .or_else(|| request.get("requestedAt"))
                .or_else(|| payload.get("requested_at")),
        ),
    })
}

fn fallback_title(media: &Value, media_type: MediaType) -> String {
    int_at(media, &["tmdbId"])
        .map(|id| format!("{} tmdb:{id}", media_type.as_str()))
        .or_else(|| {
            int_at(media, &["tvdbId"]).map(|id| format!("{} tvdb:{id}", media_type.as_str()))
        })
        .or_else(|| {
            text_at(media, &["imdbId"]).map(|id| format!("{} imdb:{id}", media_type.as_str()))
        })
        .unwrap_or_else(|| media_type.as_str().to_string())
}

fn request_items_from_payload(
    request: &Value,
    media_type: MediaType,
    season_number: Option<i64>,
    episode_number: Option<i64>,
) -> Vec<MediaRequestItemInput> {
    if media_type == MediaType::Movie {
        return vec![MediaRequestItemInput {
            season_number: None,
            episode_number: None,
            title: None,
            air_date: None,
            availability_class: AvailabilityClass::Existing,
        }];
    }

    if let Some(seasons) = request.get("seasons").and_then(Value::as_array) {
        let items = seasons
            .iter()
            .filter_map(|season| {
                int_at(season, &["seasonNumber"]).or_else(|| int_at(season, &["season_number"]))
            })
            .map(|season_number| MediaRequestItemInput {
                season_number: Some(season_number),
                episode_number: None,
                title: None,
                air_date: None,
                availability_class: AvailabilityClass::Unknown,
            })
            .collect::<Vec<_>>();
        if !items.is_empty() {
            return items;
        }
    }

    vec![MediaRequestItemInput {
        season_number,
        episode_number,
        title: None,
        air_date: None,
        availability_class: AvailabilityClass::Unknown,
    }]
}

pub fn generic_media_event(
    source: EventSource,
    default_event_type: &str,
    payload: Value,
) -> EventIngest {
    let request = payload.get("request").unwrap_or(&payload);
    let media = request
        .get("media")
        .or_else(|| payload.get("media"))
        .unwrap_or(&payload);
    let media_type = text_at(&payload, &["media_type"])
        .or_else(|| text_at(&payload, &["mediaType"]))
        .or_else(|| text_at(request, &["type"]))
        .or_else(|| text_at(media, &["media_type"]))
        .or_else(|| text_at(media, &["mediaType"]))
        .or_else(|| text_at(&payload, &["type"]))
        .and_then(|value| MediaType::try_from(value.as_str()).ok());
    let identity = media_type.map(|media_type| MediaIdentity {
        media_type,
        tmdb_id: int_at(&payload, &["tmdb_id"])
            .or_else(|| int_at(&payload, &["tmdbId"]))
            .or_else(|| int_at(media, &["tmdb_id"]))
            .or_else(|| int_at(media, &["tmdbId"])),
        tvdb_id: int_at(&payload, &["tvdb_id"])
            .or_else(|| int_at(&payload, &["tvdbId"]))
            .or_else(|| int_at(media, &["tvdb_id"]))
            .or_else(|| int_at(media, &["tvdbId"])),
        imdb_id: text_at(&payload, &["imdb_id"])
            .or_else(|| text_at(&payload, &["imdbId"]))
            .or_else(|| text_at(media, &["imdb_id"]))
            .or_else(|| text_at(media, &["imdbId"])),
        title: text_at(&payload, &["title"])
            .or_else(|| text_at(&payload, &["grandparent_title"]))
            .or_else(|| text_at(request, &["title"]))
            .or_else(|| text_at(media, &["title"]))
            .or_else(|| text_at(media, &["name"])),
        year: int_at(&payload, &["year"])
            .or_else(|| int_at(&payload, &["media_year"]))
            .or_else(|| int_at(media, &["year"])),
        season_number: int_at(&payload, &["season_number"])
            .or_else(|| int_at(&payload, &["seasonNumber"])),
        episode_number: int_at(&payload, &["episode_number"])
            .or_else(|| int_at(&payload, &["episodeNumber"])),
    });

    let event_type = text_at(&payload, &["event_type"])
        .or_else(|| text_at(&payload, &["eventType"]))
        .or_else(|| text_at(&payload, &["event"]))
        .unwrap_or_else(|| default_event_type.to_string());

    EventIngest {
        source,
        event_type: event_type.clone(),
        external_id: text_at(&payload, &["external_id"])
            .or_else(|| text_at(&payload, &["rating_key"]))
            .or_else(|| text_at(&payload, &["downloadId"]))
            .or_else(|| int_at(&payload, &["id"]).map(|id| id.to_string())),
        identity,
        observed_at: event_observed_at(source, &event_type, &payload),
        payload_json: payload,
    }
}

pub fn rtorrent_event(payload: Value) -> EventIngest {
    let event_type = text_at(&payload, &["event_type"])
        .or_else(|| text_at(&payload, &["eventType"]))
        .unwrap_or_else(|| "download_finished".to_string());
    let raw_name = text_at(&payload, &["title"])
        .or_else(|| text_at(&payload, &["name"]))
        .or_else(|| text_at(&payload, &["base_path"]).and_then(|path| raw_basename(&path)));
    let title = raw_name.as_deref().map(clean_title);
    let media_type = text_at(&payload, &["media_type"])
        .or_else(|| {
            raw_name
                .as_deref()
                .and_then(infer_media_type)
                .map(str::to_string)
        })
        .and_then(|value| MediaType::try_from(value.as_str()).ok());
    let identity = media_type.map(|media_type| MediaIdentity {
        media_type,
        tmdb_id: None,
        tvdb_id: None,
        imdb_id: None,
        title: title.clone(),
        year: raw_name.as_deref().and_then(infer_year),
        season_number: raw_name.as_deref().and_then(infer_season_number),
        episode_number: raw_name.as_deref().and_then(infer_episode_number),
    });
    let external_id = text_at(&payload, &["external_id"]).or_else(|| {
        text_at(&payload, &["info_hash"])
            .or_else(|| text_at(&payload, &["infoHash"]))
            .map(|hash| format!("{hash}:{event_type}"))
    });

    EventIngest {
        source: EventSource::Torrent,
        event_type,
        external_id,
        identity,
        observed_at: event_observed_at(EventSource::Torrent, "download_finished", &payload),
        payload_json: payload,
    }
}

pub fn rtorrent_payload_from_form(form: std::collections::HashMap<String, String>) -> Value {
    let event_type = form
        .get("event_type")
        .cloned()
        .or_else(|| form.get("eventType").cloned())
        .unwrap_or_else(|| {
            if form.get("complete").is_some_and(|value| value == "1") {
                "download_finished".to_string()
            } else {
                "download_started".to_string()
            }
        });

    json!({
        "event_type": event_type,
        "info_hash": form.get("info_hash").or_else(|| form.get("infoHash")),
        "base_path": form.get("base_path").or_else(|| form.get("basePath")),
        "label": form.get("label"),
        "complete": form.get("complete"),
        "observed_at": form.get("observed_at").or_else(|| form.get("observedAt")),
    })
}

pub fn arr_event(source: EventSource, payload: Value) -> EventIngest {
    let event_type = text_at(&payload, &["eventType"])
        .or_else(|| text_at(&payload, &["event_type"]))
        .unwrap_or_else(|| "unknown".to_string());

    let (media_type, root) = match source {
        EventSource::Sonarr => (MediaType::Series, payload.get("series").unwrap_or(&payload)),
        EventSource::Radarr => (MediaType::Movie, payload.get("movie").unwrap_or(&payload)),
        _ => (MediaType::Movie, &payload),
    };

    let identity = MediaIdentity {
        media_type,
        tmdb_id: int_at(root, &["tmdbId"]).or_else(|| int_at(&payload, &["tmdbId"])),
        tvdb_id: int_at(root, &["tvdbId"]).or_else(|| int_at(&payload, &["tvdbId"])),
        imdb_id: text_at(root, &["imdbId"]).or_else(|| text_at(&payload, &["imdbId"])),
        title: text_at(root, &["title"]),
        year: int_at(root, &["year"]),
        season_number: payload
            .get("episodes")
            .and_then(Value::as_array)
            .and_then(|episodes| episodes.first())
            .and_then(|episode| int_at(episode, &["seasonNumber"])),
        episode_number: payload
            .get("episodes")
            .and_then(Value::as_array)
            .and_then(|episodes| episodes.first())
            .and_then(|episode| int_at(episode, &["episodeNumber"])),
    };

    EventIngest {
        source,
        event_type,
        external_id: text_at(&payload, &["downloadId"])
            .or_else(|| text_at(&payload, &["release", "guid"]))
            .or_else(|| int_at(root, &["id"]).map(|id| id.to_string())),
        identity: Some(identity),
        observed_at: Utc::now(),
        payload_json: payload,
    }
}

fn text_at(value: &Value, path: &[&str]) -> Option<String> {
    let mut cursor = value;
    for segment in path {
        cursor = cursor.get(*segment)?;
    }
    cursor
        .as_str()
        .map(ToString::to_string)
        .or_else(|| cursor.as_i64().map(|n| n.to_string()))
}

fn int_at(value: &Value, path: &[&str]) -> Option<i64> {
    let mut cursor = value;
    for segment in path {
        cursor = cursor.get(*segment)?;
    }
    cursor
        .as_i64()
        .or_else(|| cursor.as_u64().and_then(|n| i64::try_from(n).ok()))
        .or_else(|| cursor.as_str()?.parse().ok())
}

fn raw_basename(path: &str) -> Option<String> {
    path.rsplit('/')
        .find(|part| !part.is_empty())
        .map(ToString::to_string)
        .filter(|name| !name.is_empty())
}

fn clean_title(name: &str) -> String {
    let normalized = name
        .split(['.', '_'])
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_string();

    let parts = normalized.split_whitespace().collect::<Vec<_>>();
    if let Some(year_index) = parts.iter().position(|part| {
        part.len() == 4
            && part
                .parse::<i64>()
                .is_ok_and(|year| (1900..=2100).contains(&year))
    }) {
        return parts[..year_index].join(" ");
    }

    if let Some(episode_index) = parts.iter().position(|part| {
        let lower = part.to_ascii_lowercase();
        let bytes = lower.as_bytes();
        bytes.len() == 6
            && bytes[0] == b's'
            && bytes[1].is_ascii_digit()
            && bytes[2].is_ascii_digit()
            && bytes[3] == b'e'
            && bytes[4].is_ascii_digit()
            && bytes[5].is_ascii_digit()
    }) {
        return parts[..episode_index].join(" ");
    }

    normalized
}

fn infer_media_type(title: &str) -> Option<&'static str> {
    let lower = title.to_ascii_lowercase();
    if infer_season_number(&lower).is_some() && infer_episode_number(&lower).is_some() {
        return Some("series");
    }
    infer_year(title).map(|_| "movie")
}

fn infer_year(title: &str) -> Option<i64> {
    title
        .split(|ch: char| !ch.is_ascii_digit())
        .find_map(|part| {
            (part.len() == 4)
                .then(|| part.parse::<i64>().ok())
                .flatten()
                .filter(|year| (1900..=2100).contains(year))
        })
}

fn infer_season_number(title: &str) -> Option<i64> {
    infer_episode_parts(title).map(|(season, _)| season)
}

fn infer_episode_number(title: &str) -> Option<i64> {
    infer_episode_parts(title).map(|(_, episode)| episode)
}

fn infer_episode_parts(title: &str) -> Option<(i64, i64)> {
    let lower = title.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    for window in bytes.windows(6) {
        if window[0] == b's'
            && window[1].is_ascii_digit()
            && window[2].is_ascii_digit()
            && window[3] == b'e'
            && window[4].is_ascii_digit()
            && window[5].is_ascii_digit()
        {
            let season = std::str::from_utf8(&window[1..3]).ok()?.parse().ok()?;
            let episode = std::str::from_utf8(&window[4..6]).ok()?.parse().ok()?;
            return Some((season, episode));
        }
    }
    None
}

fn event_observed_at(
    source: EventSource,
    event_type: &str,
    payload: &Value,
) -> chrono::DateTime<Utc> {
    let normalized = event_type.to_ascii_lowercase();
    let availability_event = matches!(source, EventSource::Plex | EventSource::Tautulli)
        && matches!(normalized.as_str(), "recently_added" | "plex_available");

    if availability_event {
        if let Some(value) = payload
            .get("added_at")
            .or_else(|| payload.get("date_added"))
            .or_else(|| payload.get("addedAt"))
            .and_then(parse_datetime_value)
        {
            return value;
        }
    }

    parse_datetime_or_now(payload.get("observed_at").or_else(|| payload.get("date")))
}

fn parse_datetime_value(value: &Value) -> Option<chrono::DateTime<Utc>> {
    if let Some(raw) = value.as_str() {
        return DateTime::parse_from_rfc3339(raw)
            .map(|dt| dt.with_timezone(&Utc))
            .ok()
            .or_else(|| raw.parse::<i64>().ok().and_then(unix_timestamp));
    }

    value.as_i64().and_then(unix_timestamp).or_else(|| {
        value
            .as_u64()
            .and_then(|timestamp| i64::try_from(timestamp).ok())
            .and_then(unix_timestamp)
    })
}

fn unix_timestamp(timestamp: i64) -> Option<chrono::DateTime<Utc>> {
    let seconds = if timestamp > 1_000_000_000_000 {
        timestamp / 1_000
    } else {
        timestamp
    };

    Utc.timestamp_opt(seconds, 0).single()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::overseerr_request_from_payload;
    use crate::core::model::MediaType;

    #[test]
    fn overseerr_request_accepts_embedded_media_without_title() {
        let request = overseerr_request_from_payload(&json!({
            "id": 42,
            "type": "tv",
            "createdAt": "2026-06-01T00:00:00.000Z",
            "requestedBy": {"displayName": "user"},
            "seasons": [{"seasonNumber": 1}],
            "media": {
                "mediaType": "tv",
                "tmdbId": 123,
                "tvdbId": 456,
                "imdbId": "tt123",
                "externalServiceSlug": "example-series"
            }
        }))
        .expect("request should parse");

        assert_eq!(request.overseerr_request_id, Some(42));
        assert_eq!(request.identity.media_type, MediaType::Series);
        assert_eq!(request.identity.tmdb_id, Some(123));
        assert_eq!(request.title, "example-series");
        assert_eq!(request.items.len(), 1);
        assert_eq!(request.items[0].season_number, Some(1));
    }
}
