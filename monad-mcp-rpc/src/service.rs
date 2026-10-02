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

use std::{
    collections::HashMap,
    io,
    sync::{
        Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use monad_mcp_chorus::ledger::{Hash, LedgerReader, Tx};
use monad_mcp_node::chorus::types::{NodeId, ProposalIndex, Timestamp};
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, warn};

use crate::{
    config::RpcConfig,
    pending::{Admission, PendingFull, PendingSet, Record, SendOutcome},
    schedule::{LOOKAHEAD, Route},
    udp::UdpSender,
    watch::{LedgerWatch, Sighting},
};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LedgerStatus {
    pub highest_slot: Option<u64>,
    pub blocks_seen: u64,
    pub commits_seen: u64,
    pub error: Option<String>,
}

pub struct Submitted {
    pub record: Record,
    pub admission: Admission,
}

#[derive(Debug, thiserror::Error)]
pub enum SubmitError {
    #[error(transparent)]
    Full(#[from] PendingFull),
    // finished and pushed out of `retain_finished` while its send was out
    #[error("the tx finished and was evicted before its status could be read")]
    Evicted,
    // startup, too few validators, or a pinned lane no one holds yet
    #[error("no proposer {} within {} ms of the target slot", lane_name(*.0), LOOKAHEAD.as_millis())]
    NoLeader(Option<ProposalIndex>),
}

fn lane_name(lane: Option<ProposalIndex>) -> String {
    lane.map_or_else(|| "on any lane".into(), |lane| format!("on lane {lane}"))
}

// everything the http handlers and the maintenance loop share
pub struct RpcState {
    config: RpcConfig,
    pending: Mutex<PendingSet>,
    udp: UdpSender,
    nonces: AtomicU64,
    ledger: Mutex<LedgerStatus>,
    last_send: Mutex<Option<SendOutcome>>,
    // when the maintenance loop last finished a ledger poll
    maintained_at: Mutex<Instant>,
}

impl RpcState {
    pub fn new(config: RpcConfig) -> io::Result<Self> {
        let udp = UdpSender::bind(config.node_id, config.peers.clone())?;
        Ok(Self {
            pending: Mutex::new(PendingSet::new(config.pending())),
            udp,
            // distinct across restarts, so a default nonce never repeats a hash
            nonces: AtomicU64::new(unix_micros()),
            ledger: Mutex::new(LedgerStatus::default()),
            last_send: Mutex::new(None),
            maintained_at: Mutex::new(Instant::now()),
            config,
        })
    }

    pub fn config(&self) -> &RpcConfig {
        &self.config
    }

    pub fn next_nonce(&self) -> u64 {
        self.nonces.fetch_add(1, Ordering::Relaxed)
    }

    fn pending(&self) -> MutexGuard<'_, PendingSet> {
        self.pending.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn record(&self, hash: &Hash) -> Option<Record> {
        self.pending().get(hash).cloned()
    }

    pub fn counts(&self) -> (usize, usize) {
        let pending = self.pending();
        (pending.in_flight(), pending.tracked())
    }

    pub fn ledger_status(&self) -> LedgerStatus {
        self.ledger
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn last_send(&self) -> Option<SendOutcome> {
        self.last_send
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    // takes ownership of a new tx and sends it once to `lane`, or to any lane;
    // a known one is not resent, and a new one with no leader ahead is refused
    pub fn submit(&self, tx: Tx, lane: Option<ProposalIndex>) -> Result<Submitted, SubmitError> {
        let route = self.route(lane, None);
        let (hash, admission, known) = {
            let mut pending = self.pending();
            if route.is_none() && !pending.owns(&tx.hash()) {
                return Err(SubmitError::NoLeader(lane));
            }
            let (hash, admission) = pending.submit(tx.clone(), lane, Instant::now())?;
            let known = pending.get(&hash).filter(|_| !admission.sends()).cloned();
            (hash, admission, known)
        };
        let record = match known {
            Some(record) => record,
            None => self
                .send(hash, &tx, lane, route)
                .ok_or(SubmitError::Evicted)?,
        };
        Ok(Submitted { record, admission })
    }

    // to the first leader on `lane`, or any lane but `avoid`'s if another
    // leads, within the lookahead from now
    fn route(&self, lane: Option<ProposalIndex>, avoid: Option<NodeId>) -> Option<Route> {
        self.config.planner.route(unix_now(), lane, avoid)
    }

    // the record as of this send
    fn send(
        &self,
        hash: Hash,
        tx: &Tx,
        lane: Option<ProposalIndex>,
        route: Option<Route>,
    ) -> Option<Record> {
        let sent_at = Instant::now();
        let outcome = match route {
            Some(route) => match self.udp.send(route.leader, tx) {
                Ok(()) => SendOutcome::Sent(route),
                Err(error) => {
                    SendOutcome::Error(format!("send to {}: {error}", u64::from(route.leader)))
                }
            },
            None => SendOutcome::Error(SubmitError::NoLeader(lane).to_string()),
        };
        match &outcome {
            SendOutcome::Sent(route) => debug!(
                hash = %hex::encode(hash),
                slot = route.slot.0,
                lane = route.lane,
                leader = u64::from(route.leader),
                latency = ?route.latency,
                "sent tx"
            ),
            SendOutcome::Error(error) => warn!(hash = %hex::encode(hash), %error, "tx not sent"),
        }
        *self.last_send.lock().unwrap_or_else(|e| e.into_inner()) = Some(outcome.clone());
        self.pending().record_send(&hash, sent_at, outcome)
    }

    pub fn resend_due(&self) {
        let due = self.pending().take_due(Instant::now());
        if due.is_empty() {
            return;
        }
        // one route per lane and last leader: every tx in the batch is due at the same moment
        let mut routes = HashMap::new();
        for (hash, tx, lane, avoid) in due {
            let route = *routes
                .entry((lane, avoid))
                .or_insert_with(|| self.route(lane, avoid));
            self.send(hash, &tx, lane, route);
        }
    }

    pub fn apply(&self, sighting: &Sighting) {
        let mut committed = 0;
        {
            let mut pending = self.pending();
            for (hash, commit) in &sighting.commits {
                if pending.commit(hash, *commit) {
                    committed += 1;
                }
            }
        }
        if committed > 0 {
            info!(committed, slot = ?sighting.highest_slot, "txs seen in the ledger");
        }
        let mut ledger = self.ledger.lock().unwrap_or_else(|e| e.into_inner());
        ledger.highest_slot = ledger.highest_slot.max(sighting.highest_slot);
        ledger.blocks_seen += sighting.blocks as u64;
        ledger.commits_seen += committed;
        ledger.error = None;
    }

    fn ledger_error(&self, error: String) {
        self.ledger.lock().unwrap_or_else(|e| e.into_inner()).error = Some(error);
    }

    fn maintained(&self) {
        *self.maintained_at.lock().unwrap_or_else(|e| e.into_inner()) = Instant::now();
    }

    // time since the maintenance loop last polled the ledger
    pub fn maintenance_age(&self) -> Duration {
        self.maintained_at
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .elapsed()
    }

    // past this age the loop is taken to be stuck or dead
    pub fn maintenance_deadline(&self) -> Duration {
        (self.config.poll_interval * 10).max(Duration::from_secs(5))
    }
}

// polls the ledger, then resends what is still unseen, every poll interval
pub async fn maintain(state: std::sync::Arc<RpcState>, mut watch: LedgerWatch) {
    let mut interval = tokio::time::interval(state.config.poll_interval);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        interval.tick().await;
        let polled = tokio::task::spawn_blocking(move || {
            let result = watch.poll();
            (watch, result)
        })
        .await;
        match polled {
            Ok((returned, result)) => {
                watch = returned;
                state.maintained();
                match result {
                    Ok(sighting) => state.apply(&sighting),
                    Err(error) => {
                        warn!(%error, "ledger poll failed");
                        state.ledger_error(error.to_string());
                    }
                }
            }
            Err(error) => {
                // the watch went down with the poll; rereading from the start loses no commit
                warn!(%error, "ledger poll panicked; rereading the ledger");
                state.ledger_error(format!("ledger poll panicked: {error}"));
                watch = LedgerWatch::from_start(&LedgerReader::new(&state.config.ledger_dir));
            }
        }
        state.resend_due();
    }
}

pub fn watch_from_now(config: &RpcConfig) -> io::Result<LedgerWatch> {
    LedgerWatch::from_now(&LedgerReader::new(&config.ledger_dir)).map_err(io::Error::other)
}

pub fn unix_now() -> Timestamp {
    let since = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    Timestamp::from_nanos(since.as_nanos())
}

pub fn unix_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

pub fn unix_ms(at: SystemTime) -> u64 {
    at.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
