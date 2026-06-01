use crate::core::model::{MatchOutcome, MediaIdentity};

pub fn normalize_title(title: &str) -> String {
    title
        .chars()
        .filter_map(|ch| {
            if ch.is_ascii_alphanumeric() {
                Some(ch.to_ascii_lowercase())
            } else if ch.is_whitespace() || matches!(ch, '-' | '_' | ':' | '.') {
                Some(' ')
            } else {
                None
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

pub fn score_identity(candidate: &MediaIdentity, request: &MediaIdentity) -> Option<f64> {
    if candidate.media_type != request.media_type {
        return None;
    }

    if candidate.tmdb_id.is_some() && candidate.tmdb_id == request.tmdb_id {
        return Some(1.0);
    }

    if candidate.tvdb_id.is_some() && candidate.tvdb_id == request.tvdb_id {
        return Some(0.95);
    }

    if candidate.imdb_id.is_some() && candidate.imdb_id == request.imdb_id {
        return Some(0.9);
    }

    let candidate_title = candidate.title.as_deref().map(normalize_title);
    let request_title = request.title.as_deref().map(normalize_title);
    if candidate_title.is_some()
        && candidate_title == request_title
        && candidate.year.is_some()
        && candidate.year == request.year
    {
        let mut score = 0.7;
        if candidate.season_number.is_some() && candidate.season_number == request.season_number {
            score += 0.1;
        }
        if candidate.episode_number.is_some() && candidate.episode_number == request.episode_number
        {
            score += 0.1;
        }
        return Some(score);
    }

    None
}

pub fn best_match<'a>(
    candidate: &MediaIdentity,
    requests: impl IntoIterator<Item = (i64, &'a MediaIdentity)>,
) -> Option<MatchOutcome> {
    requests
        .into_iter()
        .filter_map(|(media_request_id, request)| {
            score_identity(candidate, request).map(|confidence| MatchOutcome {
                media_request_id,
                media_request_item_id: None,
                confidence,
            })
        })
        .max_by(|a, b| a.confidence.total_cmp(&b.confidence))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::model::{MediaIdentity, MediaType};

    fn movie(title: &str, year: i64) -> MediaIdentity {
        MediaIdentity {
            media_type: MediaType::Movie,
            tmdb_id: None,
            tvdb_id: None,
            imdb_id: None,
            title: Some(title.to_string()),
            year: Some(year),
            season_number: None,
            episode_number: None,
        }
    }

    #[test]
    fn tmdb_beats_title_fallback() {
        let candidate = MediaIdentity {
            tmdb_id: Some(42),
            ..movie("Different", 2020)
        };
        let request = MediaIdentity {
            tmdb_id: Some(42),
            ..movie("Example", 2024)
        };

        assert_eq!(score_identity(&candidate, &request), Some(1.0));
    }

    #[test]
    fn title_year_fallback_is_lower_confidence() {
        let candidate = movie("The Example: Movie", 2024);
        let request = movie("The Example Movie", 2024);

        assert_eq!(score_identity(&candidate, &request), Some(0.7));
    }
}
