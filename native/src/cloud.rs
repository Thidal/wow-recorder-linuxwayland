// SPDX-License-Identifier: GPL-3.0-or-later

//! Warcraft Recorder Pro cloud upload.
//!
//! A blocking client for the same WCR API the Electron application talks to:
//! HTTP basic auth with the cloud account, a guild-scoped signed PUT (or a
//! signed multipart upload for large files) straight into the guild's bucket,
//! then the video's metadata posted in the legacy Electron sidecar shape the
//! website reads. Only upload and share links are ported; browsing,
//! downloading and deleting cloud videos stay out of this fork.
//!
//! No GTK and no async runtime: the coordinator's upload thread calls these
//! functions one video at a time.

use std::fmt;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;
use std::time::Duration;

use serde::Deserialize;
use serde_json::{Map, Value, json};
use ureq::{Agent, Body, SendBody, http::Response};

use crate::domain::{
    ActivityDetails, Category, GameFlavor, LibraryEntry, Outcome, TimelineKind, TimelineShape,
};

pub const API_URL: &str = "https://api.warcraftrecorder.com/api";
pub const WEBSITE_URL: &str = "https://warcraftrecorder.com";

/// Files at or above this size go up in parts of this size, matching the
/// Electron client so the API's signing limits behave identically.
pub const PART_BYTES: u64 = 100 * 1024 * 1024;
/// Direct-to-bucket PUTs are retried this many times, like the Electron client.
const PUT_ATTEMPTS: u32 = 5;
const PROGRESS_STEP_BYTES: u64 = 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloudCredentials {
    pub user: String,
    pub password: String,
    pub guild: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CloudError {
    /// The API rejected the account name or password.
    Unauthorized,
    NotAffiliated(String),
    NoWritePermission(String),
    /// The guild moved to Warcraft Logs; the old API no longer accepts it.
    Migrated(String),
    Http {
        status: u16,
        body: String,
    },
    Transport(String),
    Io(String),
    InvalidResponse(String),
}

impl fmt::Display for CloudError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unauthorized => f.write_str("the cloud account name or password was rejected"),
            Self::NotAffiliated(guild) => {
                write!(f, "the cloud account is not a member of guild {guild:?}")
            }
            Self::NoWritePermission(guild) => {
                write!(f, "the cloud account may not upload to guild {guild:?}")
            }
            Self::Migrated(guild) => write!(
                f,
                "guild {guild:?} has been migrated and no longer accepts uploads from this API"
            ),
            Self::Http { status, body } if body.is_empty() => {
                write!(f, "the cloud API answered HTTP {status}")
            }
            Self::Http { status, body } => {
                write!(f, "the cloud API answered HTTP {status}: {body}")
            }
            Self::Transport(error) => write!(f, "network error: {error}"),
            Self::Io(error) => write!(f, "could not read the recording: {error}"),
            Self::InvalidResponse(error) => write!(f, "unexpected cloud API response: {error}"),
        }
    }
}

impl std::error::Error for CloudError {}

impl From<ureq::Error> for CloudError {
    fn from(error: ureq::Error) -> Self {
        Self::Transport(error.to_string())
    }
}

impl From<io::Error> for CloudError {
    fn from(error: io::Error) -> Self {
        Self::Io(error.to_string())
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Affiliation {
    guild_name: String,
    write: bool,
}

#[derive(Debug, Deserialize)]
struct GuildInfo {
    #[serde(default)]
    migrated: bool,
}

#[derive(Debug, Deserialize)]
struct SignedPut {
    signed: String,
}

#[derive(Debug, Deserialize)]
struct SignedMultipart {
    urls: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct SharedLink {
    id: Value,
}

pub struct CloudClient {
    agent: Agent,
    api: String,
    website: String,
    auth: String,
    guild: String,
    part_bytes: u64,
}

impl CloudClient {
    pub fn new(credentials: &CloudCredentials) -> Self {
        Self::with_urls(credentials, API_URL, WEBSITE_URL)
    }

    /// Point the client at another API, for tests against a local server.
    pub fn with_urls(credentials: &CloudCredentials, api: &str, website: &str) -> Self {
        let agent: Agent = Agent::config_builder()
            // Statuses are checked per call so a 401 becomes `Unauthorized`.
            .http_status_as_error(false)
            .timeout_connect(Some(Duration::from_secs(20)))
            .timeout_recv_response(Some(Duration::from_secs(120)))
            // Signed bucket URLs are final; following a redirect would drop the
            // signature anyway.
            .max_redirects(0)
            .user_agent(concat!(
                "warcraft-recorder-linux/",
                env!("CARGO_PKG_VERSION")
            ))
            .build()
            .into();
        Self {
            agent,
            api: api.trim_end_matches('/').to_owned(),
            website: website.trim_end_matches('/').to_owned(),
            auth: basic_auth(&credentials.user, &credentials.password),
            guild: credentials.guild.clone(),
            part_bytes: PART_BYTES,
        }
    }

    /// Override the multipart threshold and part size, so tests can exercise
    /// multipart uploads without 100 MiB fixtures.
    pub fn with_part_bytes(mut self, part_bytes: u64) -> Self {
        self.part_bytes = part_bytes.max(1);
        self
    }

    /// Confirm the account can upload to the configured guild: the same checks
    /// the Electron client runs before it enables uploads.
    pub fn check_access(&self) -> Result<(), CloudError> {
        let response = self
            .agent
            .get(&format!("{}/user/affiliations", self.api))
            .header("Authorization", &self.auth)
            .call()?;
        let affiliations: Vec<Affiliation> = read_json(response)?;
        let affiliation = affiliations
            .iter()
            .find(|affiliation| affiliation.guild_name == self.guild)
            .ok_or_else(|| CloudError::NotAffiliated(self.guild.clone()))?;
        if !affiliation.write {
            return Err(CloudError::NoWritePermission(self.guild.clone()));
        }

        let response = self
            .agent
            .get(&self.guild_url(""))
            .header("Authorization", &self.auth)
            .call()?;
        let info: GuildInfo = read_json(response)?;
        if info.migrated {
            return Err(CloudError::Migrated(self.guild.clone()));
        }
        Ok(())
    }

    /// Upload the recording's media, then register its metadata. `progress`
    /// receives `(sent, total)` bytes. The media goes first so the website
    /// never lists a video without a file behind it.
    pub fn upload(
        &self,
        entry: &LibraryEntry,
        progress: &mut dyn FnMut(u64, u64),
    ) -> Result<(), CloudError> {
        let key = video_key(entry)?;
        let size = std::fs::metadata(&entry.media_path)?.len();
        if size < self.part_bytes {
            self.single_part_upload(&entry.media_path, &key, size, progress)?;
        } else {
            self.multipart_upload(&entry.media_path, &key, size, progress)?;
        }
        progress(size, size);

        let metadata = cloud_metadata(entry, size);
        let response = self
            .agent
            .post(&self.guild_url("/video"))
            .header("Authorization", &self.auth)
            .send_json(&metadata)?;
        check_status(response)?;

        // The Electron client always runs the housekeeper after an upload so
        // the guild has room for the next one; its outcome is advisory.
        match self
            .agent
            .post(&self.guild_url("/housekeeper"))
            .header("Authorization", &self.auth)
            .send_empty()
        {
            Ok(response) if !response.status().is_success() => {
                tracing::warn!(status = %response.status(), "cloud housekeeper failed");
            }
            Err(error) => tracing::warn!(%error, "cloud housekeeper failed"),
            Ok(_) => {}
        }
        Ok(())
    }

    /// Ask the API for a public link to an uploaded video.
    pub fn share_link(&self, entry: &LibraryEntry) -> Result<String, CloudError> {
        let name = video_name(entry)?;
        let response = self
            .agent
            .post(&self.guild_url(&format!("/video/{}/link", encode_component(&name))))
            .header("Authorization", &self.auth)
            .send_empty()?;
        let link: SharedLink = read_json(response)?;
        let id = match link.id {
            Value::String(id) => id,
            Value::Number(id) => id.to_string(),
            other => {
                return Err(CloudError::InvalidResponse(format!(
                    "link id is not a string or number: {other}"
                )));
            }
        };
        Ok(format!("{}/link/{id}", self.website))
    }

    fn guild_url(&self, suffix: &str) -> String {
        format!(
            "{}/guild/{}{suffix}",
            self.api,
            encode_component(&self.guild)
        )
    }

    fn single_part_upload(
        &self,
        path: &Path,
        key: &str,
        size: u64,
        progress: &mut dyn FnMut(u64, u64),
    ) -> Result<(), CloudError> {
        let response = self
            .agent
            .post(&self.guild_url("/upload"))
            .header("Authorization", &self.auth)
            .send_json(json!({ "key": key, "bytes": size }))?;
        let signed: SignedPut = read_json(response)?;
        self.put_range(&signed.signed, path, key, 0, size, &mut |sent| {
            progress(sent, size)
        })?;
        Ok(())
    }

    fn multipart_upload(
        &self,
        path: &Path,
        key: &str,
        size: u64,
        progress: &mut dyn FnMut(u64, u64),
    ) -> Result<(), CloudError> {
        let response = self
            .agent
            .post(&self.guild_url("/create-multipart-upload"))
            .header("Authorization", &self.auth)
            .send_json(json!({ "key": key, "total": size, "part": self.part_bytes }))?;
        let signed: SignedMultipart = read_json(response)?;
        let expected_parts = size.div_ceil(self.part_bytes);
        if signed.urls.len() as u64 != expected_parts {
            return Err(CloudError::InvalidResponse(format!(
                "expected {expected_parts} signed part URLs, got {}",
                signed.urls.len()
            )));
        }

        let mut etags = Vec::with_capacity(signed.urls.len());
        let mut offset = 0;
        for url in &signed.urls {
            let bytes = self.part_bytes.min(size - offset);
            let part_start = offset;
            let etag = self.put_range(url, path, key, part_start, bytes, &mut |sent| {
                progress(part_start + sent, size)
            })?;
            let etag = etag.ok_or_else(|| {
                CloudError::InvalidResponse("a part upload returned no ETag".to_owned())
            })?;
            etags.push(etag.replace('"', ""));
            offset += bytes;
        }

        let response = self
            .agent
            .post(&self.guild_url("/complete-multipart-upload"))
            .header("Authorization", &self.auth)
            .send_json(json!({ "key": key, "etags": etags }))?;
        check_status(response)?;
        Ok(())
    }

    /// PUT `len` bytes of `path` from `offset` to a signed bucket URL, retrying
    /// transient failures. Returns the response ETag.
    fn put_range(
        &self,
        url: &str,
        path: &Path,
        key: &str,
        offset: u64,
        len: u64,
        progress: &mut dyn FnMut(u64),
    ) -> Result<Option<String>, CloudError> {
        let mut last_error = None;
        for attempt in 1..=PUT_ATTEMPTS {
            let mut file = File::open(path)?;
            file.seek(SeekFrom::Start(offset))?;
            let mut reader = ProgressReader {
                inner: file.take(len),
                sent: 0,
                reported: 0,
                progress: &mut *progress,
            };
            // An explicit Content-Length keeps ureq from switching to chunked
            // encoding, which signed bucket PUTs reject.
            let result = self
                .agent
                .put(url)
                .header("Content-Type", content_type(key))
                .header("Content-Length", len.to_string())
                .send(SendBody::from_reader(&mut reader));
            match result {
                Ok(response) if response.status().is_success() => {
                    let etag = response
                        .headers()
                        .get("etag")
                        .and_then(|value| value.to_str().ok())
                        .map(str::to_owned);
                    return Ok(etag);
                }
                Ok(response) => {
                    let error = http_error(response);
                    tracing::warn!(%error, attempt, key, "cloud part upload failed");
                    last_error = Some(error);
                }
                Err(error) => {
                    tracing::warn!(%error, attempt, key, "cloud part upload failed");
                    last_error = Some(error.into());
                }
            }
        }
        Err(last_error.unwrap_or_else(|| CloudError::Transport("upload failed".to_owned())))
    }
}

struct ProgressReader<'a, R> {
    inner: R,
    sent: u64,
    reported: u64,
    progress: &'a mut dyn FnMut(u64),
}

impl<R: Read> Read for ProgressReader<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(buf)?;
        self.sent += read as u64;
        if self.sent - self.reported >= PROGRESS_STEP_BYTES || read == 0 {
            self.reported = self.sent;
            (self.progress)(self.sent);
        }
        Ok(read)
    }
}

fn read_json<T: serde::de::DeserializeOwned>(response: Response<Body>) -> Result<T, CloudError> {
    let mut response = check_status(response)?;
    response
        .body_mut()
        .read_json()
        .map_err(|error| CloudError::InvalidResponse(error.to_string()))
}

fn check_status(response: Response<Body>) -> Result<Response<Body>, CloudError> {
    if response.status().is_success() {
        Ok(response)
    } else {
        Err(http_error(response))
    }
}

fn http_error(mut response: Response<Body>) -> CloudError {
    let status = response.status().as_u16();
    if status == 401 {
        return CloudError::Unauthorized;
    }
    let body = response
        .body_mut()
        .with_config()
        .limit(2048)
        .read_to_string()
        .unwrap_or_default();
    CloudError::Http {
        status,
        body: body.trim().to_owned(),
    }
}

fn basic_auth(user: &str, password: &str) -> String {
    format!("Basic {}", base64(format!("{user}:{password}").as_bytes()))
}

fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let n = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        out.push(ALPHABET[(n >> 18) as usize & 63] as char);
        out.push(ALPHABET[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

/// `encodeURIComponent`, which is what the Electron client uses for guild and
/// video names in API paths.
fn encode_component(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z'
            | b'a'..=b'z'
            | b'0'..=b'9'
            | b'-'
            | b'_'
            | b'.'
            | b'!'
            | b'~'
            | b'*'
            | b'\''
            | b'('
            | b')' => out.push(byte as char),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

fn content_type(key: &str) -> &'static str {
    if key.ends_with(".png") {
        "image/png"
    } else {
        "video/mp4"
    }
}

fn video_key(entry: &LibraryEntry) -> Result<String, CloudError> {
    entry
        .media_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .ok_or_else(|| CloudError::Io("the recording has no file name".to_owned()))
}

fn video_name(entry: &LibraryEntry) -> Result<String, CloudError> {
    let key = video_key(entry)?;
    Ok(key.strip_suffix(".mp4").map(str::to_owned).unwrap_or(key))
}

// --- Legacy metadata ---

/// The video's metadata in the Electron sidecar shape the WCR API stores,
/// carrying only the keys the Electron client sends: the API maps them onto
/// table columns, so an unknown key is a failed insert.
pub fn cloud_metadata(entry: &LibraryEntry, size: u64) -> Value {
    let mut map = Map::new();
    let parent = match &entry.details {
        ActivityDetails::Clip {
            source_category, ..
        } => Some(source_category),
        _ => None,
    };
    let scored_as = parent.unwrap_or(&entry.category);

    map.insert("category".into(), json!(legacy_category(&entry.category)));
    if let Some(parent) = parent {
        map.insert("parentCategory".into(), json!(legacy_category(parent)));
        map.insert("clippedAt".into(), json!(entry.start_unix_ms));
    }
    map.insert("duration".into(), json!(entry.duration_ms as f64 / 1000.0));
    map.insert("start".into(), json!(entry.start_unix_ms));
    map.insert(
        "result".into(),
        json!(legacy_result(scored_as, entry.outcome)),
    );
    map.insert("flavour".into(), json!(legacy_flavour(&entry.flavor)));
    map.insert("overrun".into(), json!(0));
    map.insert("protected".into(), json!(entry.protected));
    if let Some(tag) = entry.tag.as_deref().filter(|tag| !tag.trim().is_empty()) {
        map.insert("tag".into(), json!(tag));
    }
    map.insert(
        "uniqueHash".into(),
        json!(entry.activity_hash.clone().unwrap_or_default()),
    );
    map.insert("size".into(), json!(size));

    if let Some(player) = &entry.player {
        let mut raw = Map::new();
        insert_some(&mut raw, "_GUID", player.guid.as_deref());
        raw.insert("_name".into(), json!(player.name));
        insert_some(&mut raw, "_realm", player.realm.as_deref());
        insert_some(&mut raw, "_specID", player.spec_id);
        map.insert("player".into(), Value::Object(raw));
    }
    let combatants: Vec<Value> = entry
        .combatants
        .iter()
        .map(|combatant| {
            let mut raw = Map::new();
            insert_some(&mut raw, "_GUID", combatant.guid.as_deref());
            insert_some(&mut raw, "_teamID", combatant.team_id);
            insert_some(&mut raw, "_specID", combatant.spec_id);
            insert_some(&mut raw, "_name", combatant.name.as_deref());
            insert_some(&mut raw, "_realm", combatant.realm.as_deref());
            insert_some(&mut raw, "_region", combatant.region.as_deref());
            Value::Object(raw)
        })
        .collect();
    map.insert("combatants".into(), Value::Array(combatants));
    map.insert("deaths".into(), Value::Array(legacy_deaths(entry)));

    match &entry.details {
        ActivityDetails::Raid {
            zone_id,
            zone_name,
            encounter_id,
            encounter_name,
            difficulty_id,
            difficulty,
            boss_percent,
            ..
        } => {
            insert_some(&mut map, "zoneID", *zone_id);
            insert_some(&mut map, "zoneName", zone_name.as_deref());
            insert_some(&mut map, "encounterID", *encounter_id);
            insert_some(&mut map, "encounterName", encounter_name.as_deref());
            insert_some(&mut map, "difficultyID", *difficulty_id);
            insert_some(&mut map, "difficulty", difficulty.as_deref());
            insert_some(&mut map, "bossPercent", *boss_percent);
        }
        ActivityDetails::Dungeon {
            zone_id,
            map_id,
            keystone_level,
            affixes,
            upgrade_level,
            ..
        } => {
            insert_some(&mut map, "zoneID", *zone_id);
            insert_some(&mut map, "mapID", *map_id);
            insert_some(&mut map, "keystoneLevel", *keystone_level);
            insert_some(&mut map, "upgradeLevel", *upgrade_level);
            map.insert("affixes".into(), json!(affixes));
            map.insert(
                "challengeModeTimeline".into(),
                Value::Array(legacy_segments(entry)),
            );
        }
        ActivityDetails::ArenaOrBattleground {
            map_id,
            map_name,
            team_mmr,
        } => {
            insert_some(&mut map, "zoneID", *map_id);
            insert_some(&mut map, "zoneName", map_name.as_deref());
            insert_some(&mut map, "teamMMR", *team_mmr);
        }
        ActivityDetails::SoloRounds {
            map_id,
            map_name,
            rounds_won,
            rounds_played,
            rounds,
        } => {
            insert_some(&mut map, "zoneID", *map_id);
            insert_some(&mut map, "zoneName", map_name.as_deref());
            insert_some(&mut map, "soloShuffleRoundsWon", *rounds_won);
            insert_some(&mut map, "soloShuffleRoundsPlayed", *rounds_played);
            let timeline: Vec<Value> = rounds
                .iter()
                .map(|round| {
                    let mut raw = Map::new();
                    raw.insert("round".into(), json!(round.round));
                    raw.insert("timestamp".into(), json!(round.start_ms as f64 / 1000.0));
                    raw.insert(
                        "result".into(),
                        json!(matches!(round.outcome, Outcome::Win)),
                    );
                    if let Some(duration_ms) = round.duration_ms {
                        raw.insert("duration".into(), json!(duration_ms as f64 / 1000.0));
                    }
                    Value::Object(raw)
                })
                .collect();
            map.insert("soloShuffleTimeline".into(), Value::Array(timeline));
        }
        ActivityDetails::Clip { .. } | ActivityDetails::Manual => {}
    }

    Value::Object(map)
}

fn insert_some<T: serde::Serialize>(map: &mut Map<String, Value>, key: &str, value: Option<T>) {
    if let Some(value) = value {
        map.insert(key.to_owned(), json!(value));
    }
}

fn legacy_category(category: &Category) -> &'static str {
    match category {
        Category::TwoVTwo => "2v2",
        Category::ThreeVThree => "3v3",
        Category::FiveVFive => "5v5",
        Category::Skirmish => "Skirmish",
        Category::SoloShuffle => "Solo Shuffle",
        Category::MythicPlus => "Mythic+",
        Category::Raids => "Raids",
        Category::Battlegrounds => "Battlegrounds",
        Category::Clip => "Clips",
        Category::Manual => "Manual",
    }
}

/// The inverse of the storage module's legacy outcome mapping: Mythic+ uses
/// `result` for a timed/completed key, everything else for a win or kill.
fn legacy_result(category: &Category, outcome: Outcome) -> bool {
    match category {
        Category::MythicPlus => matches!(outcome, Outcome::Complete | Outcome::Win),
        _ => matches!(outcome, Outcome::Win),
    }
}

fn legacy_flavour(flavor: &GameFlavor) -> &'static str {
    match flavor {
        GameFlavor::Classic | GameFlavor::Era => "Classic",
        GameFlavor::Retail | GameFlavor::Unknown(_) => "Retail",
    }
}

/// Death markers as the Electron `PlayerDeathType`: media-relative seconds,
/// the wall-clock date, and whether the dead player was on the recorder's side.
fn legacy_deaths(entry: &LibraryEntry) -> Vec<Value> {
    entry
        .timeline
        .iter()
        .filter(|item| matches!(item.kind(), TimelineKind::Death))
        .map(|item| {
            let name = item.label().unwrap_or_default();
            let spec_id = entry
                .combatants
                .iter()
                .find(|combatant| combatant.name.as_deref() == Some(name))
                .and_then(|combatant| combatant.spec_id)
                .unwrap_or(0);
            json!({
                "name": name,
                "specId": spec_id,
                "date": iso_timestamp(entry.start_unix_ms + item.start_ms() as i64),
                "timestamp": item.start_ms() as f64 / 1000.0,
                "friendly": item.outcome() != Some(Outcome::Win),
            })
        })
        .collect()
}

/// Mythic+ boss and trash spans as `RawChallengeModeTimelineSegment`s.
fn legacy_segments(entry: &LibraryEntry) -> Vec<Value> {
    entry
        .timeline
        .iter()
        .filter(|item| item.shape() == TimelineShape::Span)
        .filter_map(|item| {
            let segment_type = match item.kind() {
                TimelineKind::Encounter => "Boss",
                TimelineKind::Trash => "Trash",
                _ => return None,
            };
            let start = entry.start_unix_ms + item.start_ms() as i64;
            let end = entry.start_unix_ms + item.end_ms().unwrap_or(item.start_ms()) as i64;
            Some(json!({
                "segmentType": segment_type,
                "logStart": iso_timestamp(start),
                "logEnd": iso_timestamp(end),
                "timestamp": item.start_ms() as f64 / 1000.0,
            }))
        })
        .collect()
}

/// `Date.prototype.toISOString` for a Unix-epoch millisecond value.
fn iso_timestamp(epoch_ms: i64) -> String {
    let seconds = epoch_ms.div_euclid(1000);
    let millis = epoch_ms.rem_euclid(1000);
    let (year, month, day) = crate::media_jobs::civil_from_days(seconds.div_euclid(86_400));
    let of_day = seconds.rem_euclid(86_400);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{millis:03}Z",
        of_day / 3_600,
        of_day % 3_600 / 60,
        of_day % 60
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::TimelineItem;

    #[test]
    fn base64_matches_rfc_4648_vectors() {
        for (input, expected) in [
            ("", ""),
            ("f", "Zg=="),
            ("fo", "Zm8="),
            ("foo", "Zm9v"),
            ("foob", "Zm9vYg=="),
            ("fooba", "Zm9vYmE="),
            ("foobar", "Zm9vYmFy"),
        ] {
            assert_eq!(base64(input.as_bytes()), expected);
        }
        assert_eq!(basic_auth("user", "pass"), "Basic dXNlcjpwYXNz");
    }

    #[test]
    fn encode_component_matches_javascript() {
        assert_eq!(encode_component("Loud Farts"), "Loud%20Farts");
        assert_eq!(
            encode_component("Aùra's (Wipe) [M]"),
            "A%C3%B9ra's%20(Wipe)%20%5BM%5D"
        );
    }

    fn entry(category: Category, outcome: Outcome, details: ActivityDetails) -> LibraryEntry {
        LibraryEntry {
            id: crate::domain::RecordingId::from_media_name("a.mp4"),
            media_path: "/r/a.mp4".into(),
            sidecar_path: "/r/a.json".into(),
            category,
            flavor: GameFlavor::Era,
            title: "a".to_owned(),
            start_unix_ms: 0,
            duration_ms: 90_000,
            outcome,
            protected: true,
            tag: Some("  ".to_owned()),
            activity_hash: None,
            player: None,
            combatants: Vec::new(),
            details,
            timeline: vec![
                TimelineItem::span(TimelineKind::Trash, 0, 30_000, None, None, None).unwrap(),
                TimelineItem::span(TimelineKind::Encounter, 30_000, 90_000, None, None, None)
                    .unwrap(),
                TimelineItem::point(
                    TimelineKind::Death,
                    45_000,
                    Some("Foe".into()),
                    Some(Outcome::Win),
                    None,
                ),
            ],
            media: crate::domain::MediaFacts {
                fps: None,
                width: None,
                height: None,
                codec: None,
                has_content: true,
            },
            meter: crate::domain::MeterData::default(),
        }
    }

    #[test]
    fn mythic_plus_metadata_keeps_the_key_and_its_segments() {
        let dungeon = ActivityDetails::Dungeon {
            zone_id: Some(2_660),
            dungeon_name: Some("Ara-Kara".to_owned()),
            map_id: Some(503),
            keystone_level: Some(12),
            affixes: vec![9, 10],
            upgrade_level: Some(2),
        };
        let metadata = cloud_metadata(&entry(Category::MythicPlus, Outcome::Complete, dungeon), 7);
        assert_eq!(metadata["category"], "Mythic+");
        assert_eq!(metadata["result"], true);
        assert_eq!(metadata["flavour"], "Classic");
        assert_eq!(metadata["keystoneLevel"], 12);
        assert_eq!(metadata["mapID"], 503);
        assert_eq!(metadata["affixes"], json!([9, 10]));
        assert_eq!(metadata["uniqueHash"], "");
        assert!(metadata.get("tag").is_none());
        assert!(metadata.get("dungeonName").is_none());
        assert_eq!(
            metadata["challengeModeTimeline"],
            json!([
                { "segmentType": "Trash", "logStart": "1970-01-01T00:00:00.000Z",
                  "logEnd": "1970-01-01T00:00:30.000Z", "timestamp": 0.0 },
                { "segmentType": "Boss", "logStart": "1970-01-01T00:00:30.000Z",
                  "logEnd": "1970-01-01T00:01:30.000Z", "timestamp": 30.0 },
            ])
        );
        assert_eq!(metadata["deaths"][0]["friendly"], false);

        let abandoned = ActivityDetails::Dungeon {
            zone_id: None,
            dungeon_name: None,
            map_id: None,
            keystone_level: None,
            affixes: Vec::new(),
            upgrade_level: None,
        };
        let metadata = cloud_metadata(
            &entry(Category::MythicPlus, Outcome::Abandoned, abandoned),
            7,
        );
        assert_eq!(metadata["result"], false);
    }

    #[test]
    fn clips_carry_their_parent_category_and_score_like_it() {
        let clip = ActivityDetails::Clip {
            source_recording: crate::domain::RecordingId::from_media_name("s.mp4"),
            source_category: Category::MythicPlus,
            source_title: None,
        };
        let metadata = cloud_metadata(&entry(Category::Clip, Outcome::Complete, clip), 7);
        assert_eq!(metadata["category"], "Clips");
        assert_eq!(metadata["parentCategory"], "Mythic+");
        assert_eq!(metadata["clippedAt"], 0);
        assert_eq!(metadata["result"], true);
        assert!(metadata.get("challengeModeTimeline").is_none());
    }

    #[test]
    fn iso_timestamp_matches_javascript() {
        assert_eq!(iso_timestamp(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(iso_timestamp(1_790_276_105_689), "2026-09-24T18:55:05.689Z");
    }
}
