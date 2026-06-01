use chrono::Utc;
use serde_json::Value;

use crate::{
    core::model::{EventIngest, EventSource, IncomingRequest, MediaIdentity, MediaType},
    db::parse_datetime_or_now,
};

pub fn overseerr_request_from_payload(payload: &Value) -> Option<IncomingRequest> {
    let request = payload.get("request").unwrap_or(payload);
    let media = request.get("media").or_else(|| payload.get("media"));
    let media_type = text_at(request, &["type"])
        .or_else(|| text_at(media?, &["mediaType"]))
        .or_else(|| text_at(payload, &["media_type"]))
        .and_then(|value| MediaType::try_from(value.as_str()).ok())?;

    let title = text_at(request, &["title"])
        .or_else(|| text_at(media?, &["title"]))
        .or_else(|| text_at(media?, &["name"]))?;

    Some(IncomingRequest {
        overseerr_request_id: int_at(request, &["id"]).or_else(|| int_at(payload, &["request_id"])),
        identity: MediaIdentity {
            media_type,
            tmdb_id: int_at(media?, &["tmdbId"]).or_else(|| int_at(payload, &["tmdb_id"])),
            tvdb_id: int_at(media?, &["tvdbId"]).or_else(|| int_at(payload, &["tvdb_id"])),
            imdb_id: text_at(media?, &["imdbId"]).or_else(|| text_at(payload, &["imdb_id"])),
            title: Some(title.clone()),
            year: int_at(media?, &["year"]).or_else(|| int_at(request, &["year"])),
            season_number: int_at(request, &["seasonNumber"])
                .or_else(|| int_at(payload, &["season_number"])),
            episode_number: int_at(request, &["episodeNumber"])
                .or_else(|| int_at(payload, &["episode_number"])),
        },
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

pub fn generic_media_event(
    source: EventSource,
    default_event_type: &str,
    payload: Value,
) -> EventIngest {
    let media_type = text_at(&payload, &["media_type"])
        .or_else(|| text_at(&payload, &["mediaType"]))
        .or_else(|| text_at(&payload, &["type"]))
        .and_then(|value| MediaType::try_from(value.as_str()).ok());
    let identity = media_type.map(|media_type| MediaIdentity {
        media_type,
        tmdb_id: int_at(&payload, &["tmdb_id"]).or_else(|| int_at(&payload, &["tmdbId"])),
        tvdb_id: int_at(&payload, &["tvdb_id"]).or_else(|| int_at(&payload, &["tvdbId"])),
        imdb_id: text_at(&payload, &["imdb_id"]).or_else(|| text_at(&payload, &["imdbId"])),
        title: text_at(&payload, &["title"]).or_else(|| text_at(&payload, &["grandparent_title"])),
        year: int_at(&payload, &["year"]).or_else(|| int_at(&payload, &["media_year"])),
        season_number: int_at(&payload, &["season_number"])
            .or_else(|| int_at(&payload, &["seasonNumber"])),
        episode_number: int_at(&payload, &["episode_number"])
            .or_else(|| int_at(&payload, &["episodeNumber"])),
    });

    EventIngest {
        source,
        event_type: text_at(&payload, &["event_type"])
            .or_else(|| text_at(&payload, &["eventType"]))
            .or_else(|| text_at(&payload, &["event"]))
            .unwrap_or_else(|| default_event_type.to_string()),
        external_id: text_at(&payload, &["external_id"])
            .or_else(|| text_at(&payload, &["rating_key"]))
            .or_else(|| text_at(&payload, &["downloadId"]))
            .or_else(|| int_at(&payload, &["id"]).map(|id| id.to_string())),
        identity,
        observed_at: parse_datetime_or_now(
            payload.get("observed_at").or_else(|| payload.get("date")),
        ),
        payload_json: payload,
    }
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
