// SPDX-License-Identifier: GPL-3.0-or-later

//! The cloud client against an in-process fake of the WCR API and its signed
//! bucket URLs. The fake records every request, so the tests assert on the
//! exact protocol: basic auth on API calls only, fixed-length (never chunked)
//! bucket PUTs, multipart ETags, and the legacy metadata shape.

use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;

use serde_json::{Value, json};
use warcraft_recorder::cloud::{CloudClient, CloudCredentials, CloudError, cloud_metadata};
use warcraft_recorder::domain::{
    ActivityDetails, Category, CombatantSummary, GameFlavor, LibraryEntry, MediaFacts, MeterData,
    Outcome, PlayerSummary, RecordingId, TimelineItem, TimelineKind,
};

const AUTH: &str = "Basic dXNlcjpwYXNz";

#[derive(Clone, Debug)]
struct Recorded {
    method: String,
    path: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

struct FakeApi {
    base: String,
    requests: Arc<Mutex<Vec<Recorded>>>,
}

impl FakeApi {
    /// `affiliations` is the JSON the affiliations endpoint returns.
    fn start(affiliations: Value, migrated: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&requests);
        let own_base = base.clone();
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { break };
                let recorded = Arc::clone(&recorded);
                let affiliations = affiliations.clone();
                let base = own_base.clone();
                thread::spawn(move || {
                    serve(stream, &recorded, &affiliations, migrated, &base);
                });
            }
        });
        Self { base, requests }
    }

    fn client(&self) -> CloudClient {
        CloudClient::with_urls(
            &CloudCredentials {
                user: "user".to_owned(),
                password: "pass".to_owned(),
                guild: "Loud Farts".to_owned(),
            },
            &format!("{}/api", self.base),
            "https://website.test",
        )
    }

    fn requests(&self) -> Vec<Recorded> {
        self.requests.lock().unwrap().clone()
    }
}

fn serve(
    stream: TcpStream,
    recorded: &Mutex<Vec<Recorded>>,
    affiliations: &Value,
    migrated: bool,
    base: &str,
) {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut stream = stream;
    loop {
        let mut request_line = String::new();
        if reader.read_line(&mut request_line).unwrap_or(0) == 0 {
            return;
        }
        let mut parts = request_line.split_whitespace();
        let method = parts.next().unwrap_or_default().to_owned();
        let path = parts.next().unwrap_or_default().to_owned();
        let mut headers = HashMap::new();
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            let line = line.trim_end();
            if line.is_empty() {
                break;
            }
            let (name, value) = line.split_once(':').unwrap();
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_owned());
        }
        let length: usize = headers
            .get("content-length")
            .map(|value| value.parse().unwrap())
            .unwrap_or(0);
        let mut body = vec![0; length];
        reader.read_exact(&mut body).unwrap();
        recorded.lock().unwrap().push(Recorded {
            method: method.clone(),
            path: path.clone(),
            headers: headers.clone(),
            body,
        });

        let api_call = path.starts_with("/api/");
        let (status, extra, payload) = if api_call
            && headers.get("authorization").map(String::as_str) != Some(AUTH)
        {
            (401, String::new(), String::new())
        } else {
            match (method.as_str(), path.as_str()) {
                ("GET", "/api/user/affiliations") => (200, String::new(), affiliations.to_string()),
                ("GET", "/api/guild/Loud%20Farts") => (
                    200,
                    String::new(),
                    json!({ "migrated": migrated }).to_string(),
                ),
                ("POST", "/api/guild/Loud%20Farts/upload") => (
                    200,
                    String::new(),
                    json!({ "signed": format!("{base}/bucket/single") }).to_string(),
                ),
                ("POST", "/api/guild/Loud%20Farts/create-multipart-upload") => {
                    let request: Value =
                        serde_json::from_slice(&recorded.lock().unwrap().last().unwrap().body)
                            .unwrap();
                    let total = request["total"].as_u64().unwrap();
                    let part = request["part"].as_u64().unwrap();
                    let urls: Vec<String> = (0..total.div_ceil(part))
                        .map(|index| format!("{base}/bucket/part{index}"))
                        .collect();
                    (200, String::new(), json!({ "urls": urls }).to_string())
                }
                ("PUT", bucket) if bucket.starts_with("/bucket/") => (
                    200,
                    format!("ETag: \"etag-{}\"\r\n", &bucket["/bucket/".len()..]),
                    String::new(),
                ),
                ("POST", "/api/guild/Loud%20Farts/complete-multipart-upload")
                | ("POST", "/api/guild/Loud%20Farts/video")
                | ("POST", "/api/guild/Loud%20Farts/housekeeper") => {
                    (200, String::new(), "{}".to_owned())
                }
                ("POST", link) if link.ends_with("/link") => {
                    (200, String::new(), json!({ "id": "abc123" }).to_string())
                }
                _ => (404, String::new(), String::new()),
            }
        };
        let response = format!(
            "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{extra}\r\n{payload}",
            payload.len()
        );
        if stream.write_all(response.as_bytes()).is_err() {
            return;
        }
    }
}

fn entry(dir: &Path, bytes: usize) -> LibraryEntry {
    let media_path = dir.join("activity-1 - Loudfarts - Sszorak [M] (Wipe).mp4");
    fs::write(&media_path, (0..bytes).map(|i| i as u8).collect::<Vec<_>>()).unwrap();
    LibraryEntry {
        id: RecordingId::from_media_name("x"),
        sidecar_path: media_path.with_extension("json"),
        media_path,
        category: Category::Raids,
        flavor: GameFlavor::Retail,
        title: "Loudfarts - Sszorak [M] (Wipe)".to_owned(),
        start_unix_ms: 1_790_276_105_689,
        duration_ms: 294_821,
        outcome: Outcome::Loss,
        protected: false,
        tag: None,
        activity_hash: Some("6646616475d036a57a333dc1fa19e60f".to_owned()),
        player: Some(PlayerSummary {
            name: "Loudfarts".to_owned(),
            realm: Some("ArgentDawn".to_owned()),
            guid: Some("Player-3702-0A9181B5".to_owned()),
            class_id: None,
            spec_id: Some(251),
        }),
        combatants: vec![CombatantSummary {
            name: Some("Plumbster".to_owned()),
            realm: Some("ArgentDawn".to_owned()),
            guid: Some("Player-3702-0A386116".to_owned()),
            region: None,
            class_id: None,
            spec_id: Some(62),
            team_id: Some(1),
        }],
        details: ActivityDetails::Raid {
            zone_id: Some(0),
            zone_name: Some("Unknown Raid".to_owned()),
            encounter_id: Some(3420),
            encounter_name: Some("Sszorak".to_owned()),
            difficulty_id: Some(16),
            difficulty: Some("M".to_owned()),
            pull: None,
            boss_percent: Some(21),
        },
        timeline: vec![TimelineItem::point(
            TimelineKind::Death,
            96_182,
            Some("Plumbster".to_owned()),
            Some(Outcome::Loss),
            None,
        )],
        media: MediaFacts {
            fps: Some(30),
            width: None,
            height: None,
            codec: None,
            has_content: true,
        },
        meter: MeterData::default(),
    }
}

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("wr-cloud-{name}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn writer_affiliation() -> Value {
    json!([{ "id": 1, "userName": "user", "guildName": "Loud Farts",
             "read": true, "write": true, "del": false, "admin": false }])
}

#[test]
fn single_part_upload_puts_fixed_length_media_then_posts_metadata() {
    let api = FakeApi::start(writer_affiliation(), false);
    let dir = temp_dir("single");
    let entry = entry(&dir, 5_000);
    let client = api.client();

    client.check_access().unwrap();
    let mut last = (0, 0);
    client
        .upload(&entry, &mut |sent, total| last = (sent, total))
        .unwrap();
    assert_eq!(last, (5_000, 5_000));

    let requests = api.requests();
    let paths: Vec<String> = requests
        .iter()
        .map(|request| format!("{} {}", request.method, request.path))
        .collect();
    assert_eq!(
        paths,
        [
            "GET /api/user/affiliations",
            "GET /api/guild/Loud%20Farts",
            "POST /api/guild/Loud%20Farts/upload",
            "PUT /bucket/single",
            "POST /api/guild/Loud%20Farts/video",
            "POST /api/guild/Loud%20Farts/housekeeper",
        ]
    );
    let sign: Value = serde_json::from_slice(&requests[2].body).unwrap();
    assert_eq!(
        sign,
        json!({ "key": "activity-1 - Loudfarts - Sszorak [M] (Wipe).mp4", "bytes": 5000 })
    );

    let put = &requests[3];
    assert_eq!(put.headers.get("content-length").unwrap(), "5000");
    assert!(!put.headers.contains_key("transfer-encoding"));
    assert!(!put.headers.contains_key("authorization"));
    assert_eq!(put.headers.get("content-type").unwrap(), "video/mp4");
    assert_eq!(put.body, fs::read(&entry.media_path).unwrap());

    let metadata: Value = serde_json::from_slice(&requests[4].body).unwrap();
    assert_eq!(metadata, cloud_metadata(&entry, 5_000));
    assert_eq!(metadata["category"], "Raids");
    assert_eq!(metadata["result"], false);
    assert_eq!(metadata["encounterID"], 3420);
    assert_eq!(metadata["difficultyID"], 16);
    assert_eq!(metadata["duration"], 294.821);
    assert_eq!(metadata["player"]["_specID"], 251);
    assert_eq!(metadata["combatants"][0]["_teamID"], 1);
    assert_eq!(
        metadata["deaths"],
        json!([{ "name": "Plumbster", "specId": 62, "timestamp": 96.182,
                 "date": "2026-09-24T18:56:41.871Z", "friendly": true }])
    );
    // The API needs these for the website to group and date the video.
    assert_eq!(metadata["uniqueHash"], "6646616475d036a57a333dc1fa19e60f");
    assert_eq!(metadata["start"], 1_790_276_105_689_i64);
    assert!(metadata.get("videoName").is_none());

    let link = client.share_link(&entry).unwrap();
    assert_eq!(link, "https://website.test/link/abc123");
    let last = api.requests().pop().unwrap();
    assert_eq!(
        last.path,
        "/api/guild/Loud%20Farts/video/activity-1%20-%20Loudfarts%20-%20Sszorak%20%5BM%5D%20(Wipe)/link"
    );
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn large_files_upload_in_parts_and_complete_with_unquoted_etags() {
    let api = FakeApi::start(writer_affiliation(), false);
    let dir = temp_dir("multi");
    let entry = entry(&dir, 2_500);
    let client = api.client().with_part_bytes(1_000);

    client.upload(&entry, &mut |_, _| {}).unwrap();

    let requests = api.requests();
    let puts: Vec<&Recorded> = requests.iter().filter(|r| r.method == "PUT").collect();
    assert_eq!(puts.len(), 3);
    let lengths: Vec<&str> = puts
        .iter()
        .map(|put| put.headers["content-length"].as_str())
        .collect();
    assert_eq!(lengths, ["1000", "1000", "500"]);
    let joined: Vec<u8> = puts.iter().flat_map(|put| put.body.clone()).collect();
    assert_eq!(joined, fs::read(&entry.media_path).unwrap());

    let complete = requests
        .iter()
        .find(|r| r.path.ends_with("/complete-multipart-upload"))
        .unwrap();
    let body: Value = serde_json::from_slice(&complete.body).unwrap();
    assert_eq!(
        body["etags"],
        json!(["etag-part0", "etag-part1", "etag-part2"])
    );
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn access_checks_report_the_reason_uploads_are_refused() {
    let wrong_password = FakeApi::start(writer_affiliation(), false);
    let client = CloudClient::with_urls(
        &CloudCredentials {
            user: "user".to_owned(),
            password: "wrong".to_owned(),
            guild: "Loud Farts".to_owned(),
        },
        &format!("{}/api", wrong_password.base),
        "https://website.test",
    );
    assert_eq!(client.check_access(), Err(CloudError::Unauthorized));

    let reader_only = FakeApi::start(
        json!([{ "id": 1, "userName": "user", "guildName": "Loud Farts",
                 "read": true, "write": false, "del": false, "admin": false }]),
        false,
    );
    assert!(matches!(
        reader_only.client().check_access(),
        Err(CloudError::NoWritePermission(_))
    ));

    let other_guild = FakeApi::start(json!([]), false);
    assert!(matches!(
        other_guild.client().check_access(),
        Err(CloudError::NotAffiliated(_))
    ));

    let migrated = FakeApi::start(writer_affiliation(), true);
    assert!(matches!(
        migrated.client().check_access(),
        Err(CloudError::Migrated(_))
    ));
}
