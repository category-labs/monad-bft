// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

//! AWS-SDK plumbing shared by the dynamo and s3 backends: static credentials,
//! the credential/region config prologue, and read statistics.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use aws_config::BehaviorVersion;
// The service crates re-export the same `aws-credential-types`/`aws-types`
// items; pick whichever enabled backend provides them.
#[cfg(feature = "dynamo")]
use aws_sdk_dynamodb::config::{Credentials, Region};
#[cfg(all(feature = "s3", not(feature = "dynamo")))]
use aws_sdk_s3::config::{Credentials, Region};

/// Explicit static credentials, for deployments with no ambient AWS credential
/// chain (DynamoDB Local / Alternator accept any non-empty pair; MinIO/Ceph
/// require an explicit pair).
#[derive(Clone)]
pub struct StaticCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

impl std::fmt::Debug for StaticCredentials {
    // Never print secret material.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StaticCredentials")
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"<redacted>")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

/// Resolves the AWS credential/region chain once (async: it performs I/O).
/// Per-endpoint overrides belong on the derived service clients, not here.
pub(crate) async fn load_sdk_config(
    region: Option<String>,
    profile: Option<String>,
    credentials: Option<StaticCredentials>,
    provider_name: &'static str,
) -> aws_config::SdkConfig {
    let mut loader = aws_config::defaults(BehaviorVersion::latest());
    if let Some(region) = region {
        loader = loader.region(Region::new(region));
    }
    if let Some(profile) = profile {
        loader = loader.profile_name(profile);
    }
    if let Some(creds) = credentials {
        loader = loader.credentials_provider(Credentials::new(
            creds.access_key_id,
            creds.secret_access_key,
            creds.session_token,
            None,
            provider_name,
        ));
    }
    loader.load().await
}

/// Lock-free read counters shared by the backends. The two `kinds` slots carry
/// backend-specific read-kind counts (dynamo: GetItem/Query; s3: range/full
/// GET); `items` is unused (0) by backends without an item notion.
#[derive(Debug, Default)]
pub(crate) struct ReadStats {
    started: AtomicU64,
    completed: AtomicU64,
    errors: AtomicU64,
    canceled: AtomicU64,
    kinds: [AtomicU64; 2],
    items: AtomicU64,
    bytes: AtomicU64,
    in_flight: AtomicU64,
    max_in_flight: AtomicU64,
}

/// One drained [`ReadStats`] window; backends map it onto their public
/// snapshot types.
pub(crate) struct ReadStatsWindow {
    pub started: u64,
    pub completed: u64,
    pub errors: u64,
    pub canceled: u64,
    pub kinds: [u64; 2],
    /// Only the dynamo backend reports items; 0 elsewhere.
    #[cfg_attr(not(feature = "dynamo"), allow(dead_code))]
    pub items: u64,
    pub bytes: u64,
    pub in_flight: u64,
    pub max_in_flight: u64,
}

impl ReadStats {
    /// Marks a read of `kind` (a `kinds` slot index) started and returns the
    /// RAII guard: `finish` records the outcome, dropping unfinished counts
    /// the read as canceled.
    pub(crate) fn start(self: &Arc<Self>, kind: usize) -> ReadGuard {
        self.started.fetch_add(1, Ordering::Relaxed);
        self.kinds[kind].fetch_add(1, Ordering::Relaxed);
        let in_flight = self.in_flight.fetch_add(1, Ordering::Relaxed) + 1;
        let mut prev = self.max_in_flight.load(Ordering::Relaxed);
        while in_flight > prev {
            match self.max_in_flight.compare_exchange_weak(
                prev,
                in_flight,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(next) => prev = next,
            }
        }
        ReadGuard {
            stats: Arc::clone(self),
            finished: false,
        }
    }

    /// Atomically reads and zeroes the counters since the last call
    /// (`in_flight` is sampled, not reset).
    pub(crate) fn take(&self) -> ReadStatsWindow {
        ReadStatsWindow {
            started: self.started.swap(0, Ordering::Relaxed),
            completed: self.completed.swap(0, Ordering::Relaxed),
            errors: self.errors.swap(0, Ordering::Relaxed),
            canceled: self.canceled.swap(0, Ordering::Relaxed),
            kinds: [
                self.kinds[0].swap(0, Ordering::Relaxed),
                self.kinds[1].swap(0, Ordering::Relaxed),
            ],
            items: self.items.swap(0, Ordering::Relaxed),
            bytes: self.bytes.swap(0, Ordering::Relaxed),
            in_flight: self.in_flight.load(Ordering::Relaxed),
            max_in_flight: self.max_in_flight.swap(0, Ordering::Relaxed),
        }
    }
}

pub(crate) struct ReadGuard {
    stats: Arc<ReadStats>,
    finished: bool,
}

impl ReadGuard {
    pub(crate) fn finish(mut self, error: bool, items: u64, bytes: u64) {
        self.finished = true;
        let stats = &self.stats;
        if error {
            stats.errors.fetch_add(1, Ordering::Relaxed);
        } else {
            stats.completed.fetch_add(1, Ordering::Relaxed);
            stats.items.fetch_add(items, Ordering::Relaxed);
            stats.bytes.fetch_add(bytes, Ordering::Relaxed);
        }
        stats.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Drop for ReadGuard {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let stats = &self.stats;
        stats.canceled.fetch_add(1, Ordering::Relaxed);
        stats.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}
