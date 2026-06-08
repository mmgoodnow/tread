use chrono::{DateTime, TimeZone, Utc};
use serde_json::{Value, json};

use crate::{
    core::model::{
        AvailabilityClass, EventIngest, EventSource, IncomingRequest, MediaIdentifier,
        MediaIdentity, MediaRequestItemInput, MediaType,
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
            identifiers: identifiers_from_overseerr(media),
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
    let identity = generic_media_identity(source, &payload, request, media);

    let event_type = match source {
        EventSource::Overseerr => overseerr_event_type(&payload)
            .or_else(|| text_at(&payload, &["event_type"]))
            .or_else(|| text_at(&payload, &["eventType"]))
            .or_else(|| text_at(&payload, &["event"]))
            .unwrap_or_else(|| default_event_type.to_string()),
        _ => text_at(&payload, &["event_type"])
            .or_else(|| text_at(&payload, &["eventType"]))
            .or_else(|| text_at(&payload, &["event"]))
            .unwrap_or_else(|| default_event_type.to_string()),
    };

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

fn generic_media_identity(
    source: EventSource,
    payload: &Value,
    request: &Value,
    media: &Value,
) -> Option<MediaIdentity> {
    if source == EventSource::Tautulli {
        return tautulli_identity(payload);
    }

    let arr_root = match source {
        EventSource::Sonarr => payload.get("series"),
        EventSource::Radarr => payload.get("movie"),
        _ => None,
    };
    let media_type = match source {
        EventSource::Sonarr => Some(MediaType::Series),
        EventSource::Radarr => Some(MediaType::Movie),
        _ => text_at(payload, &["media_type"])
            .or_else(|| text_at(payload, &["mediaType"]))
            .or_else(|| text_at(request, &["type"]))
            .or_else(|| text_at(media, &["media_type"]))
            .or_else(|| text_at(media, &["mediaType"]))
            .or_else(|| text_at(payload, &["type"]))
            .and_then(|value| media_type_from_external(&value)),
    }?;

    let roots = [arr_root, Some(payload), Some(media), Some(request)];
    Some(MediaIdentity {
        media_type,
        tmdb_id: first_int(&roots, &[&["tmdb_id"], &["tmdbId"]])
            .or_else(|| tmdb_id_from_guids(payload.get("guids"))),
        tvdb_id: first_int(&roots, &[&["tvdb_id"], &["tvdbId"]])
            .or_else(|| tvdb_id_from_guids(payload.get("guids"))),
        imdb_id: first_text(&roots, &[&["imdb_id"], &["imdbId"]])
            .or_else(|| imdb_id_from_guids(payload.get("guids"))),
        title: first_text(
            &roots,
            &[
                &["title"],
                &["name"],
                &["grandparent_title"],
                &["parent_title"],
            ],
        ),
        year: first_int(&roots, &[&["year"], &["media_year"]]),
        season_number: int_at(payload, &["season_number"])
            .or_else(|| int_at(payload, &["seasonNumber"]))
            .or_else(|| {
                payload
                    .get("episodes")
                    .and_then(Value::as_array)
                    .and_then(|episodes| episodes.first())
                    .and_then(|episode| int_at(episode, &["seasonNumber"]))
            }),
        episode_number: int_at(payload, &["episode_number"])
            .or_else(|| int_at(payload, &["episodeNumber"]))
            .or_else(|| {
                payload
                    .get("episodes")
                    .and_then(Value::as_array)
                    .and_then(|episodes| episodes.first())
                    .and_then(|episode| int_at(episode, &["episodeNumber"]))
            }),
        identifiers: identifiers_from_generic(source, arr_root, payload),
    })
}

fn tautulli_identity(payload: &Value) -> Option<MediaIdentity> {
    let raw_media_type = text_at(payload, &["media_type"])
        .or_else(|| text_at(payload, &["mediaType"]))
        .or_else(|| text_at(payload, &["type"]))?;
    let media_type = media_type_from_external(&raw_media_type)?;
    let is_series_item = media_type == MediaType::Series;
    let title = if is_series_item {
        text_at(payload, &["grandparent_title"])
            .filter(|value| !value.trim().is_empty())
            .or_else(|| {
                text_at(payload, &["parent_title"]).filter(|value| !value.trim().is_empty())
            })
            .or_else(|| text_at(payload, &["show_title"]).filter(|value| !value.trim().is_empty()))
            .or_else(|| text_at(payload, &["title"]).filter(|value| !value.trim().is_empty()))
    } else {
        text_at(payload, &["title"]).filter(|value| !value.trim().is_empty())
    };

    let show_guids = if raw_media_type.eq_ignore_ascii_case("episode") {
        payload.get("grandparent_guids")
    } else if raw_media_type.eq_ignore_ascii_case("season") {
        payload
            .get("parent_guids")
            .or_else(|| payload.get("grandparent_guids"))
    } else {
        payload.get("guids")
    };

    Some(MediaIdentity {
        media_type,
        tmdb_id: if is_series_item {
            int_at(payload, &["grandparent_tmdb_id"])
                .or_else(|| int_at(payload, &["show_tmdb_id"]))
                .or_else(|| tmdb_id_from_guids(show_guids))
        } else {
            int_at(payload, &["tmdb_id"])
                .or_else(|| int_at(payload, &["tmdbId"]))
                .or_else(|| int_at(payload, &["themoviedb_id"]))
                .or_else(|| tmdb_id_from_guids(payload.get("guids")))
        },
        tvdb_id: if is_series_item {
            int_at(payload, &["grandparent_tvdb_id"])
                .or_else(|| int_at(payload, &["show_tvdb_id"]))
                .or_else(|| tvdb_id_from_guids(show_guids))
        } else {
            int_at(payload, &["tvdb_id"])
                .or_else(|| int_at(payload, &["tvdbId"]))
                .or_else(|| int_at(payload, &["thetvdb_id"]))
                .or_else(|| tvdb_id_from_guids(payload.get("guids")))
        },
        imdb_id: if is_series_item {
            text_at(payload, &["grandparent_imdb_id"])
                .or_else(|| text_at(payload, &["show_imdb_id"]))
                .or_else(|| imdb_id_from_guids(show_guids))
        } else {
            text_at(payload, &["imdb_id"])
                .or_else(|| text_at(payload, &["imdbId"]))
                .or_else(|| imdb_id_from_guids(payload.get("guids")))
        },
        title,
        year: int_at(payload, &["year"]).or_else(|| int_at(payload, &["media_year"])),
        season_number: int_at(payload, &["season_number"])
            .or_else(|| int_at(payload, &["seasonNumber"]))
            .or_else(|| int_at(payload, &["parent_media_index"]))
            .or_else(|| {
                raw_media_type
                    .eq_ignore_ascii_case("season")
                    .then(|| text_at(payload, &["title"]))
                    .flatten()
                    .and_then(|title| season_number_from_title(&title))
            })
            .or_else(|| season_number_from_title(&text_at(payload, &["parent_title"])?)),
        episode_number: int_at(payload, &["episode_number"])
            .or_else(|| int_at(payload, &["episodeNumber"]))
            .or_else(|| {
                raw_media_type
                    .eq_ignore_ascii_case("episode")
                    .then(|| int_at(payload, &["media_index"]))
                    .flatten()
            }),
        identifiers: identifiers_from_tautulli(payload, &raw_media_type),
    })
}

fn identifiers_from_overseerr(media: &Value) -> Vec<MediaIdentifier> {
    let mut identifiers = Vec::new();
    push_identifier(
        &mut identifiers,
        "overseerr_media_id",
        int_at(media, &["id"]),
    );
    identifiers
}

fn identifiers_from_generic(
    source: EventSource,
    arr_root: Option<&Value>,
    payload: &Value,
) -> Vec<MediaIdentifier> {
    let mut identifiers = Vec::new();
    match source {
        EventSource::Sonarr => {
            if let Some(root) = arr_root {
                push_identifier(&mut identifiers, "sonarr_series_id", int_at(root, &["id"]));
            }
        }
        EventSource::Radarr => {
            if let Some(root) = arr_root {
                push_identifier(&mut identifiers, "radarr_movie_id", int_at(root, &["id"]));
            }
        }
        EventSource::Plex => {
            push_plex_identifiers(&mut identifiers, payload, "media_type");
        }
        _ => {}
    }
    identifiers
}

fn identifiers_from_tautulli(payload: &Value, raw_media_type: &str) -> Vec<MediaIdentifier> {
    let mut identifiers = Vec::new();
    push_plex_identifiers_for_type(&mut identifiers, payload, raw_media_type);
    identifiers
}

fn push_plex_identifiers(
    identifiers: &mut Vec<MediaIdentifier>,
    payload: &Value,
    media_type_path: &str,
) {
    if let Some(raw_media_type) = text_at(payload, &[media_type_path]) {
        push_plex_identifiers_for_type(identifiers, payload, &raw_media_type);
    }
}

fn push_plex_identifiers_for_type(
    identifiers: &mut Vec<MediaIdentifier>,
    payload: &Value,
    raw_media_type: &str,
) {
    let normalized = raw_media_type.to_ascii_lowercase();
    if normalized == "episode" {
        push_identifier(
            identifiers,
            "plex_show_rating_key",
            text_at(payload, &["grandparent_rating_key"]),
        );
    } else if normalized == "season" {
        push_identifier(
            identifiers,
            "plex_show_rating_key",
            text_at(payload, &["parent_rating_key"])
                .or_else(|| text_at(payload, &["grandparent_rating_key"])),
        );
    } else {
        push_identifier(
            identifiers,
            "plex_rating_key",
            text_at(payload, &["rating_key"]),
        );
    }
}

fn push_identifier<T: ToString>(
    identifiers: &mut Vec<MediaIdentifier>,
    namespace: &str,
    value: Option<T>,
) {
    let Some(value) = value.map(|value| value.to_string()) else {
        return;
    };
    let value = value.trim();
    if value.is_empty() {
        return;
    }

    if identifiers
        .iter()
        .any(|identifier| identifier.namespace == namespace && identifier.value == value)
    {
        return;
    }

    identifiers.push(MediaIdentifier {
        namespace: namespace.to_string(),
        value: value.to_string(),
    });
}

fn media_type_from_external(value: &str) -> Option<MediaType> {
    match value.to_ascii_lowercase().as_str() {
        "movie" => Some(MediaType::Movie),
        "series" | "tv" | "show" | "episode" | "season" => Some(MediaType::Series),
        _ => None,
    }
}

fn first_int(roots: &[Option<&Value>], paths: &[&[&str]]) -> Option<i64> {
    roots
        .iter()
        .flatten()
        .find_map(|root| paths.iter().find_map(|path| int_at(root, path)))
}

fn first_text(roots: &[Option<&Value>], paths: &[&[&str]]) -> Option<String> {
    roots
        .iter()
        .flatten()
        .find_map(|root| paths.iter().find_map(|path| text_at(root, path)))
}

fn tmdb_id_from_guids(value: Option<&Value>) -> Option<i64> {
    id_from_guids(value, "tmdb")
}

fn tvdb_id_from_guids(value: Option<&Value>) -> Option<i64> {
    id_from_guids(value, "tvdb")
}

fn imdb_id_from_guids(value: Option<&Value>) -> Option<String> {
    guid_values(value).into_iter().find_map(|guid| {
        guid.strip_prefix("imdb://")
            .map(ToString::to_string)
            .filter(|id| !id.is_empty())
    })
}

fn id_from_guids(value: Option<&Value>, prefix: &str) -> Option<i64> {
    let prefix = format!("{prefix}://");
    guid_values(value).into_iter().find_map(|guid| {
        guid.strip_prefix(&prefix)
            .and_then(|id| id.parse::<i64>().ok())
    })
}

fn guid_values(value: Option<&Value>) -> Vec<String> {
    match value {
        Some(Value::Array(values)) => values
            .iter()
            .filter_map(Value::as_str)
            .map(ToString::to_string)
            .collect(),
        Some(Value::String(value)) => vec![value.clone()],
        _ => Vec::new(),
    }
}

fn season_number_from_title(title: &str) -> Option<i64> {
    title
        .strip_prefix("Season ")
        .and_then(|value| value.parse::<i64>().ok())
}

fn overseerr_event_type(payload: &Value) -> Option<String> {
    text_at(payload, &["notification_type"])
        .map(|value| value.to_ascii_lowercase().trim().replace([' ', '-'], "_"))
}

pub fn rtorrent_event(mut payload: Value) -> EventIngest {
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
        season_number: int_at(&payload, &["season_number"])
            .or_else(|| int_at(&payload, &["seasonNumber"]))
            .or_else(|| raw_name.as_deref().and_then(infer_season_number)),
        episode_number: int_at(&payload, &["episode_number"])
            .or_else(|| int_at(&payload, &["episodeNumber"]))
            .or_else(|| raw_name.as_deref().and_then(infer_episode_number)),
        identifiers: Vec::new(),
    });
    let external_id = text_at(&payload, &["external_id"]).or_else(|| {
        text_at(&payload, &["info_hash"])
            .or_else(|| text_at(&payload, &["infoHash"]))
            .map(|hash| format!("{hash}:{event_type}"))
    });
    if let Some(object) = payload.as_object_mut() {
        if let Some(title) = &title {
            object
                .entry("title")
                .or_insert_with(|| Value::String(title.clone()));
        }
        if let Some(identity) = &identity {
            object
                .entry("media_type")
                .or_insert_with(|| Value::String(identity.media_type.as_str().to_string()));
            if let Some(year) = identity.year {
                object
                    .entry("year")
                    .or_insert_with(|| Value::Number(year.into()));
            }
            if let Some(season_number) = identity.season_number {
                object
                    .entry("season_number")
                    .or_insert_with(|| Value::Number(season_number.into()));
            }
            if let Some(episode_number) = identity.episode_number {
                object
                    .entry("episode_number")
                    .or_insert_with(|| Value::Number(episode_number.into()));
            }
        }
    }

    EventIngest {
        source: EventSource::Torrent,
        event_type: event_type.clone(),
        external_id,
        identity,
        observed_at: event_observed_at(EventSource::Torrent, &event_type, &payload),
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
        identifiers: identifiers_from_generic(source, Some(root), &payload),
    };

    EventIngest {
        source,
        event_type,
        external_id: text_at(&payload, &["downloadId"])
            .or_else(|| text_at(&payload, &["release", "guid"]))
            .or_else(|| int_at(root, &["id"]).map(|id| id.to_string())),
        identity: Some(identity),
        observed_at: arr_observed_at(&payload),
        payload_json: payload,
    }
}

fn arr_observed_at(payload: &Value) -> chrono::DateTime<Utc> {
    let event_type = text_at(payload, &["eventType"])
        .or_else(|| text_at(payload, &["event_type"]))
        .unwrap_or_default()
        .to_ascii_lowercase();
    let import_event = matches!(
        event_type.as_str(),
        "download" | "import" | "download_import" | "moviedownloaded"
    );
    if import_event {
        if let Some(value) = payload
            .get("movieFile")
            .and_then(|file| file.get("dateAdded"))
            .or_else(|| {
                payload
                    .get("episodeFile")
                    .and_then(|file| file.get("dateAdded"))
            })
            .and_then(parse_datetime_value)
        {
            return value;
        }
    }

    parse_datetime_or_now(payload.get("observed_at").or_else(|| payload.get("date")))
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

    use super::{arr_event, generic_media_event, overseerr_request_from_payload, rtorrent_event};
    use crate::core::model::{EventSource, MediaType};

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

    #[test]
    fn tautulli_episode_uses_show_identity() {
        let event = generic_media_event(
            EventSource::Tautulli,
            "recently_added",
            json!({
                "event_type": "recently_added",
                "media_type": "episode",
                "title": "Final Girls",
                "grandparent_title": "Girl Rules",
                "parent_title": "Season 1",
                "grandparent_guids": ["imdb://tt35006947", "tmdb://278174", "tvdb://457154"],
                "guids": ["tmdb://6978558", "tvdb://11637988"],
                "rating_key": "117598",
                "observed_at": "2026-06-02T12:18:02Z"
            }),
        );

        let identity = event.identity.expect("identity");
        assert_eq!(identity.media_type, MediaType::Series);
        assert_eq!(identity.title.as_deref(), Some("Girl Rules"));
        assert_eq!(identity.tmdb_id, Some(278174));
        assert_eq!(identity.tvdb_id, Some(457154));
        assert_eq!(identity.imdb_id.as_deref(), Some("tt35006947"));
        assert_eq!(identity.season_number, Some(1));
    }

    #[test]
    fn tautulli_season_uses_parent_show_title() {
        let event = generic_media_event(
            EventSource::Tautulli,
            "recently_added",
            json!({
                "event_type": "recently_added",
                "media_type": "season",
                "title": "Season 1",
                "grandparent_title": "",
                "parent_title": "Rafa",
                "rating_key": "117805",
                "observed_at": "2026-06-07T07:42:37Z"
            }),
        );

        let identity = event.identity.expect("identity");
        assert_eq!(identity.media_type, MediaType::Series);
        assert_eq!(identity.title.as_deref(), Some("Rafa"));
        assert_eq!(identity.season_number, Some(1));
    }

    #[test]
    fn generic_sonarr_event_uses_nested_series_identifiers() {
        let event = generic_media_event(
            EventSource::Sonarr,
            "Download",
            json!({
                "eventType": "Download",
                "series": {
                    "title": "Rafa",
                    "tmdbId": 279884,
                    "tvdbId": 458014,
                    "imdbId": "tt35052852",
                    "year": 2026
                },
                "episodes": [{
                    "seasonNumber": 1,
                    "episodeNumber": 6
                }],
                "downloadId": "download-1",
                "observed_at": "2026-06-07T07:42:32Z"
            }),
        );

        let identity = event.identity.expect("identity");
        assert_eq!(identity.media_type, MediaType::Series);
        assert_eq!(identity.title.as_deref(), Some("Rafa"));
        assert_eq!(identity.tmdb_id, Some(279884));
        assert_eq!(identity.tvdb_id, Some(458014));
        assert_eq!(identity.imdb_id.as_deref(), Some("tt35052852"));
        assert_eq!(identity.season_number, Some(1));
        assert_eq!(identity.episode_number, Some(6));
    }

    #[test]
    fn rtorrent_event_preserves_stored_episode_parts() {
        let event = rtorrent_event(json!({
            "event_type": "download_finished",
            "title": "One Piece",
            "base_path": "/media/Raw/Sonarr/One.Piece.S23E10.mkv",
            "media_type": "series",
            "season_number": 23,
            "episode_number": 10,
            "info_hash": "abc123",
            "observed_at": "2026-06-07T16:48:07Z"
        }));

        let identity = event.identity.expect("identity");
        assert_eq!(identity.media_type, MediaType::Series);
        assert_eq!(identity.title.as_deref(), Some("One Piece"));
        assert_eq!(identity.season_number, Some(23));
        assert_eq!(identity.episode_number, Some(10));
    }

    #[test]
    fn arr_download_uses_file_date_added_as_import_time() {
        let event = arr_event(
            EventSource::Radarr,
            json!({
                "eventType": "Download",
                "observed_at": "2026-06-04T03:54:13.532277086Z",
                "movieFile": {
                    "dateAdded": "2026-06-04T03:54:12.8805848Z"
                },
                "movie": {
                    "title": "You, Me & Tuscany",
                    "tmdbId": 1455079,
                    "year": 2026
                },
                "downloadId": "download-1"
            }),
        );

        assert_eq!(
            event.observed_at.to_rfc3339(),
            "2026-06-04T03:54:12.880584800+00:00"
        );
    }
}
