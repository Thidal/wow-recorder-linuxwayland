// SPDX-License-Identifier: GPL-3.0-or-later

//! The cloud upload thread.
//!
//! Uploads are network-bound and can take minutes, so they run on their own
//! thread instead of the serial media worker: a long upload never delays the
//! finalization of the next pull. The coordinator dispatches one job at a time
//! over a capacity-one channel and folds the events into its snapshot.
//!
//! Quitting does not wait for an upload in flight. The coordinator drops the
//! job sender and never joins this thread; the process exit ends the transfer
//! and the recording stays on disk to upload again.

use std::sync::mpsc::{Receiver, Sender};
use std::time::{Duration, Instant};

use crate::cloud::{CloudClient, CloudCredentials};
use crate::domain::{LibraryEntry, RecordingId};

/// Progress events are throttled to this cadence so a fast link does not
/// flood the coordinator with snapshots.
const PROGRESS_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UploadAction {
    /// Upload the media and metadata, then fetch a share link.
    Upload,
    /// Only fetch a share link for an already uploaded video.
    ShareLink,
}

#[derive(Clone, Debug)]
pub struct UploadJob {
    pub action: UploadAction,
    pub entry: Box<LibraryEntry>,
    pub credentials: CloudCredentials,
    /// The user asked for this job, so its link goes to the clipboard.
    pub requested: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UploadEvent {
    Progress {
        id: RecordingId,
        sent: u64,
        total: u64,
    },
    /// The job finished. `link` is absent when the upload succeeded but the
    /// API would not produce a share link; `link_error` says why.
    Finished {
        id: RecordingId,
        title: String,
        link: Option<String>,
        link_error: Option<String>,
        requested: bool,
    },
    Failed {
        id: RecordingId,
        title: String,
        action: UploadAction,
        message: String,
    },
}

pub struct UploadWorker {
    jobs: Receiver<UploadJob>,
    events: Sender<UploadEvent>,
}

impl UploadWorker {
    pub fn new(jobs: Receiver<UploadJob>, events: Sender<UploadEvent>) -> Self {
        Self { jobs, events }
    }

    /// Run until the coordinator drops the job sender.
    pub fn run(self) {
        while let Ok(job) = self.jobs.recv() {
            let event = self.process(&job);
            if self.events.send(event).is_err() {
                return;
            }
        }
    }

    fn process(&self, job: &UploadJob) -> UploadEvent {
        let entry = &job.entry;
        let client = CloudClient::new(&job.credentials);
        let failed = |message: String| UploadEvent::Failed {
            id: entry.id.clone(),
            title: entry.title.clone(),
            action: job.action.clone(),
            message,
        };

        if job.action == UploadAction::Upload {
            if let Err(error) = client.check_access() {
                return failed(error.to_string());
            }
            let mut last_sent = Instant::now()
                .checked_sub(PROGRESS_INTERVAL)
                .unwrap_or_else(Instant::now);
            let mut progress = |sent: u64, total: u64| {
                if sent < total && last_sent.elapsed() < PROGRESS_INTERVAL {
                    return;
                }
                last_sent = Instant::now();
                let _ = self.events.send(UploadEvent::Progress {
                    id: entry.id.clone(),
                    sent,
                    total,
                });
            };
            tracing::info!(title = %entry.title, "cloud upload started");
            if let Err(error) = client.upload(entry, &mut progress) {
                tracing::warn!(%error, title = %entry.title, "cloud upload failed");
                return failed(error.to_string());
            }
            tracing::info!(title = %entry.title, "cloud upload finished");
        }

        match client.share_link(entry) {
            Ok(link) => UploadEvent::Finished {
                id: entry.id.clone(),
                title: entry.title.clone(),
                link: Some(link),
                link_error: None,
                requested: job.requested,
            },
            Err(error) if job.action == UploadAction::Upload => UploadEvent::Finished {
                id: entry.id.clone(),
                title: entry.title.clone(),
                link: None,
                link_error: Some(error.to_string()),
                requested: job.requested,
            },
            Err(error) => failed(error.to_string()),
        }
    }
}
