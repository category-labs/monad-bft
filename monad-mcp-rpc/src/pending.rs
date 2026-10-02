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

//! The txs the rpc owns: each is resent until the ledger shows it or its
//! attempts run out. Pure; the caller supplies the clock.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    time::{Duration, Instant, SystemTime},
};

use monad_mcp_chorus::ledger::{FinalizationPath, Hash, Tx};
use monad_mcp_node::chorus::types::{NodeId, ProposalIndex};

use crate::schedule::Route;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PendingConfig {
    pub resend_after: Duration,
    pub max_attempts: u32,
    // txs in flight at once
    pub max_pending: usize,
    pub retain_finished: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SendOutcome {
    // a datagram to the route's leader; udp says nothing more
    Sent(Route),
    // no leader within the lookahead, or the socket refused the datagram
    Error(String),
}

impl SendOutcome {
    pub fn status_str(&self) -> &'static str {
        match self {
            Self::Sent(_) => "sent",
            Self::Error(_) => "error",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Attempt {
    pub at: SystemTime,
    pub outcome: SendOutcome,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Commit {
    pub slot: u64,
    pub lane: u32,
    pub path: FinalizationPath,
    pub finalized_at_ns: u128,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Failure {
    NotCommitted { attempts: u32 },
}

impl Failure {
    pub fn reason(&self) -> String {
        match self {
            Self::NotCommitted { attempts } => {
                format!(
                    "not in the ledger after {attempts} attempts (are its leaders on proposal.source = \"mempool\"?)"
                )
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TxState {
    Pending,
    Committed(Commit),
    // a later commit still overrides this: the ledger is the truth
    Failed(Failure),
}

impl TxState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Committed(_) => "committed",
            Self::Failed(_) => "failed",
        }
    }
}

#[derive(Clone, Debug)]
pub struct Record {
    pub hash: Hash,
    pub tx: Tx,
    // the lane the request asked for; every send of the tx targets it
    pub lane: Option<ProposalIndex>,
    pub state: TxState,
    pub submitted_at: SystemTime,
    // at most max_attempts entries, oldest first
    pub history: Vec<Attempt>,
    // the last send, or the submission until the first send is recorded
    last_sent: Instant,
}

impl Record {
    pub fn attempts(&self) -> u32 {
        self.history.len() as u32
    }

    pub fn last_outcome(&self) -> Option<&SendOutcome> {
        self.history.last().map(|attempt| &attempt.outcome)
    }

    // the leader of the last send that went out
    pub fn last_leader(&self) -> Option<NodeId> {
        self.history
            .iter()
            .rev()
            .find_map(|attempt| match &attempt.outcome {
                SendOutcome::Sent(route) => Some(route.leader),
                SendOutcome::Error(_) => None,
            })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Admission {
    New,
    // a failed tx starts over with fresh attempts
    Retried,
    // pending or committed; nothing is sent again
    Known,
}

impl Admission {
    pub fn sends(self) -> bool {
        self != Self::Known
    }
}

pub type Due = (Hash, Tx, Option<ProposalIndex>, Option<NodeId>);

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0} txs are already in flight")]
pub struct PendingFull(pub usize);

pub struct PendingSet {
    config: PendingConfig,
    records: HashMap<Hash, Record>,
    in_flight: HashSet<Hash>,
    // committed and failed hashes, oldest first, for eviction
    finished: VecDeque<Hash>,
}

impl PendingSet {
    pub fn new(config: PendingConfig) -> Self {
        Self {
            config,
            records: HashMap::new(),
            in_flight: HashSet::new(),
            finished: VecDeque::new(),
        }
    }

    pub fn config(&self) -> &PendingConfig {
        &self.config
    }

    pub fn get(&self, hash: &Hash) -> Option<&Record> {
        self.records.get(hash)
    }

    pub fn in_flight(&self) -> usize {
        self.in_flight.len()
    }

    pub fn tracked(&self) -> usize {
        self.records.len()
    }

    // pending or committed: a submission of it is `Known`
    pub fn owns(&self, hash: &Hash) -> bool {
        self.records
            .get(hash)
            .is_some_and(|record| !matches!(record.state, TxState::Failed(_)))
    }

    // the caller sends the tx next and reports it with `record_send`
    pub fn submit(
        &mut self,
        tx: Tx,
        lane: Option<ProposalIndex>,
        now: Instant,
    ) -> Result<(Hash, Admission), PendingFull> {
        let hash = tx.hash();
        let failed = match self.records.get(&hash).map(|record| &record.state) {
            None => false,
            Some(TxState::Failed(_)) => true,
            Some(_) => return Ok((hash, Admission::Known)),
        };
        if self.in_flight.len() >= self.config.max_pending {
            return Err(PendingFull(self.in_flight.len()));
        }
        if failed {
            let record = self.records.get_mut(&hash).unwrap();
            record.state = TxState::Pending;
            record.tx = tx; // demo(tx-timeline): the retry is sent with this admission's stamp
            record.lane = lane;
            record.history.clear();
            record.last_sent = now;
            self.finished.retain(|finished| *finished != hash);
            self.in_flight.insert(hash);
            return Ok((hash, Admission::Retried));
        }
        self.records.insert(
            hash,
            Record {
                hash,
                tx,
                lane,
                state: TxState::Pending,
                submitted_at: SystemTime::now(),
                history: Vec::new(),
                last_sent: now,
            },
        );
        self.in_flight.insert(hash);
        Ok((hash, Admission::New))
    }

    // the record as of this send; none once it was finished and evicted
    pub fn record_send(
        &mut self,
        hash: &Hash,
        sent_at: Instant,
        outcome: SendOutcome,
    ) -> Option<Record> {
        let record = self.records.get_mut(hash)?;
        if record.history.len() < self.config.max_attempts as usize {
            record.history.push(Attempt {
                at: SystemTime::now(),
                outcome,
            });
        }
        record.last_sent = sent_at;
        Some(record.clone())
    }

    // txs to resend now with their lane and last leader, oldest send first;
    // those out of attempts fail instead
    pub fn take_due(&mut self, now: Instant) -> Vec<Due> {
        let mut due: Vec<(Instant, Hash)> = Vec::new();
        let mut exhausted = Vec::new();
        for hash in &self.in_flight {
            let record = &self.records[hash];
            if now.saturating_duration_since(record.last_sent) < self.config.resend_after {
                continue;
            }
            if record.attempts() >= self.config.max_attempts {
                exhausted.push((*hash, record.attempts()));
            } else {
                due.push((record.last_sent, *hash));
            }
        }
        for (hash, attempts) in exhausted {
            self.finish(hash, TxState::Failed(Failure::NotCommitted { attempts }));
        }
        due.sort_unstable();
        due.into_iter()
            .map(|(_, hash)| {
                let record = &self.records[&hash];
                (hash, record.tx.clone(), record.lane, record.last_leader())
            })
            .collect()
    }

    // the first commit seen wins; returns whether `hash` is tracked
    pub fn commit(&mut self, hash: &Hash, commit: Commit) -> bool {
        let Some(record) = self.records.get(hash) else {
            return false;
        };
        match record.state {
            TxState::Pending => self.finish(*hash, TxState::Committed(commit)),
            TxState::Failed(_) => {
                self.records.get_mut(hash).unwrap().state = TxState::Committed(commit);
            }
            TxState::Committed(_) => {}
        }
        true
    }

    fn finish(&mut self, hash: Hash, state: TxState) {
        self.records.get_mut(&hash).unwrap().state = state;
        self.in_flight.remove(&hash);
        self.finished.push_back(hash);
        while self.finished.len() > self.config.retain_finished {
            if let Some(old) = self.finished.pop_front() {
                self.records.remove(&old);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use monad_mcp_node::chorus::types::{NodeId, Slot};

    use super::*;

    const RESEND: Duration = Duration::from_millis(100);

    fn config() -> PendingConfig {
        PendingConfig {
            resend_after: RESEND,
            max_attempts: 3,
            max_pending: 4,
            retain_finished: 2,
        }
    }

    fn tx(nonce: u64) -> Tx {
        Tx {
            sender: [1; 20],
            nonce,
            payload: Bytes::from_static(b"pending"),
            sent_at_ns: 0,             // demo(tx-timeline)
            rpc_received_at_ns: 0,     // demo(tx-timeline)
            mempool_admitted_at_ns: 0, // demo(tx-timeline)
        }
    }

    // a datagram to the leader of `slot`; udp says nothing more
    fn sent(slot: u64) -> SendOutcome {
        SendOutcome::Sent(Route {
            slot: Slot(slot),
            lane: 1,
            leader: NodeId::dummy(2),
            latency: None,
        })
    }

    fn commit(slot: u64) -> Commit {
        Commit {
            slot,
            lane: 1,
            path: FinalizationPath::Fast,
            finalized_at_ns: 5,
        }
    }

    #[test]
    fn in_flight_txs_are_bounded_and_resubmission_is_idempotent() {
        let mut set = PendingSet::new(config());
        let t0 = Instant::now();
        for nonce in 0..4 {
            assert_eq!(set.submit(tx(nonce), None, t0).unwrap().1, Admission::New);
        }
        assert_eq!(set.submit(tx(9), None, t0), Err(PendingFull(4)));
        assert_eq!(
            set.submit(tx(2), None, t0).unwrap(),
            (tx(2).hash(), Admission::Known)
        );
        let mut restamped = tx(2); // demo(tx-timeline)
        restamped.rpc_received_at_ns = 9; // demo(tx-timeline)
        assert_eq!(set.submit(restamped, None, t0).unwrap().1, Admission::Known); // demo(tx-timeline)
        assert_eq!(set.get(&tx(2).hash()).unwrap().tx, tx(2)); // demo(tx-timeline)
        assert_eq!(set.in_flight(), 4);

        // finishing one frees its slot in the bound
        assert!(set.commit(&tx(0).hash(), commit(1)));
        assert_eq!(set.submit(tx(9), None, t0).unwrap().1, Admission::New);
        assert_eq!(set.in_flight(), 4);
        assert_eq!(set.tracked(), 5);
    }

    #[test]
    fn sent_then_resent_then_committed() {
        let mut set = PendingSet::new(config());
        let t0 = Instant::now();
        let (hash, _) = set.submit(tx(1), None, t0).unwrap();
        set.record_send(&hash, t0, sent(10));
        assert!(set.take_due(t0 + RESEND / 2).is_empty());

        let due = set.take_due(t0 + RESEND);
        assert_eq!(due, vec![(hash, tx(1), None, Some(NodeId::dummy(2)))]);
        set.record_send(&hash, t0 + RESEND, sent(13));
        assert!(set.take_due(t0 + RESEND + RESEND / 2).is_empty());

        assert!(set.commit(&hash, commit(12)));
        let record = set.get(&hash).unwrap();
        assert_eq!(record.state, TxState::Committed(commit(12)));
        assert_eq!(record.attempts(), 2);
        assert_eq!(record.last_outcome(), Some(&sent(13)));
        assert_eq!(record.history[0].outcome, sent(10));
        assert_eq!(set.in_flight(), 0);
        assert!(set.take_due(t0 + RESEND * 10).is_empty());

        // a second sighting keeps the first
        set.commit(&hash, commit(13));
        assert_eq!(
            set.get(&hash).unwrap().state,
            TxState::Committed(commit(12))
        );
    }

    #[test]
    fn a_tx_fails_a_resend_interval_after_its_last_attempt() {
        let mut set = PendingSet::new(config());
        let t0 = Instant::now();
        let (hash, _) = set.submit(tx(1), None, t0).unwrap();
        let mut now = t0;
        set.record_send(&hash, now, SendOutcome::Error("refused".into()));
        for _ in 1..3 {
            now += RESEND;
            assert_eq!(set.take_due(now).len(), 1);
            set.record_send(&hash, now, sent(20));
        }
        assert_eq!(set.get(&hash).unwrap().attempts(), 3);
        assert!(set.take_due(now + RESEND / 2).is_empty());
        assert_eq!(set.get(&hash).unwrap().state, TxState::Pending);

        assert!(set.take_due(now + RESEND).is_empty());
        assert_eq!(
            set.get(&hash).unwrap().state,
            TxState::Failed(Failure::NotCommitted { attempts: 3 })
        );
        assert_eq!(set.in_flight(), 0);

        // the ledger overrides a failure
        assert!(set.commit(&hash, commit(40)));
        assert_eq!(
            set.get(&hash).unwrap().state,
            TxState::Committed(commit(40))
        );
    }

    #[test]
    fn resubmitting_a_failed_tx_starts_it_over() {
        let mut set = PendingSet::new(config());
        let t0 = Instant::now();
        let (hash, _) = set.submit(tx(1), None, t0).unwrap();
        for i in 0..3 {
            set.take_due(t0 + RESEND * i);
            set.record_send(&hash, t0 + RESEND * i, sent(20));
        }
        set.take_due(t0 + RESEND * 3);
        assert!(matches!(set.get(&hash).unwrap().state, TxState::Failed(_)));

        let later = t0 + RESEND * 4;
        assert_eq!(
            set.submit(tx(1), None, later).unwrap().1,
            Admission::Retried
        );
        let record = set.get(&hash).unwrap();
        assert_eq!(
            (record.state.clone(), record.attempts()),
            (TxState::Pending, 0)
        );
        assert_eq!(set.in_flight(), 1);
        assert!(set.take_due(later + RESEND / 2).is_empty());
        assert_eq!(set.take_due(later + RESEND).len(), 1);

        // a pending or committed tx is not restarted
        assert_eq!(set.submit(tx(1), None, later).unwrap().1, Admission::Known);
        set.commit(&hash, commit(3));
        assert_eq!(set.submit(tx(1), None, later).unwrap().1, Admission::Known);
        // it is finished once, so eviction cannot drop a live record
        for nonce in 10..12 {
            set.submit(tx(nonce), None, later).unwrap();
            set.commit(&tx(nonce).hash(), commit(nonce));
        }
        assert!(set.get(&hash).is_none());
    }

    // demo(tx-timeline)
    #[test]
    fn a_retried_tx_takes_the_new_rpc_received_at() {
        let mut set = PendingSet::new(config());
        let t0 = Instant::now();
        let (hash, _) = set.submit(tx(1), None, t0).unwrap();
        for i in 0..3 {
            set.take_due(t0 + RESEND * i);
            set.record_send(&hash, t0 + RESEND * i, sent(20));
        }
        set.take_due(t0 + RESEND * 3);
        let restamped = Tx {
            rpc_received_at_ns: 9,
            ..tx(1)
        };
        let admission = set
            .submit(restamped.clone(), None, t0 + RESEND * 4)
            .unwrap()
            .1;
        assert_eq!(admission, Admission::Retried);
        assert_eq!(set.get(&hash).unwrap().tx, restamped);
    }

    // nothing the sender learns ends a tx early: only the ledger or the attempts do
    #[test]
    fn no_send_outcome_is_terminal() {
        for outcome in [sent(10), SendOutcome::Error("no leader".into())] {
            let mut set = PendingSet::new(config());
            let t0 = Instant::now();
            let (hash, _) = set.submit(tx(1), None, t0).unwrap();
            let snapshot = set.record_send(&hash, t0, outcome.clone()).unwrap();
            assert_eq!(snapshot.state, TxState::Pending);
            assert_eq!(snapshot.last_outcome(), Some(&outcome));
            assert_eq!(set.in_flight(), 1);
            assert_eq!(set.take_due(t0 + RESEND).len(), 1);
        }
    }

    #[test]
    fn a_send_outcome_names_its_status() {
        assert_eq!(sent(1).status_str(), "sent");
        assert_eq!(SendOutcome::Error("x".into()).status_str(), "error");
    }

    #[test]
    fn a_send_whose_record_was_evicted_meanwhile_reports_none() {
        let mut set = PendingSet::new(PendingConfig {
            retain_finished: 1,
            ..config()
        });
        let t0 = Instant::now();
        let (hash, _) = set.submit(tx(1), None, t0).unwrap();
        let (other, _) = set.submit(tx(2), None, t0).unwrap();
        // while tx(1)'s send is out it commits, then a later finish evicts it
        set.commit(&hash, commit(3));
        set.commit(&other, commit(4));
        assert!(set.record_send(&hash, t0, sent(10)).is_none());
        assert_eq!(
            set.record_send(&other, t0, sent(11)).unwrap().state,
            TxState::Committed(commit(4))
        );
    }

    #[test]
    fn a_submission_whose_first_send_never_lands_is_resent() {
        let mut set = PendingSet::new(config());
        let t0 = Instant::now();
        let (hash, _) = set.submit(tx(1), None, t0).unwrap();
        assert!(set.take_due(t0 + RESEND / 2).is_empty());
        assert_eq!(set.take_due(t0 + RESEND), vec![(hash, tx(1), None, None)]);
    }

    #[test]
    fn a_resend_keeps_the_requested_lane_and_a_retry_takes_the_new_one() {
        let mut set = PendingSet::new(config());
        let t0 = Instant::now();
        let (hash, _) = set.submit(tx(1), Some(3), t0).unwrap();
        assert_eq!(
            set.take_due(t0 + RESEND),
            vec![(hash, tx(1), Some(3), None)]
        );
        assert!(set.owns(&hash));
        for i in 1..=3 {
            set.record_send(&hash, t0 + RESEND * i, sent(10));
        }
        set.take_due(t0 + RESEND * 4);
        assert!(!set.owns(&hash), "failed");
        assert_eq!(
            set.submit(tx(1), Some(2), t0 + RESEND * 4).unwrap().1,
            Admission::Retried
        );
        assert_eq!(set.get(&hash).unwrap().lane, Some(2));
    }

    // a resend avoids the leader the tx last went to, not a refused send
    #[test]
    fn a_due_tx_names_the_leader_it_last_went_to() {
        let mut set = PendingSet::new(config());
        let t0 = Instant::now();
        let (hash, _) = set.submit(tx(1), None, t0).unwrap();
        set.record_send(&hash, t0, sent(10));
        set.record_send(&hash, t0, SendOutcome::Error("refused".into()));
        let due = set.take_due(t0 + RESEND);
        assert_eq!(due, vec![(hash, tx(1), None, Some(NodeId::dummy(2)))]);
    }

    #[test]
    fn due_txs_come_oldest_send_first() {
        let mut set = PendingSet::new(config());
        let t0 = Instant::now();
        for (nonce, offset) in [(1, 30), (2, 10), (3, 20)] {
            let (hash, _) = set.submit(tx(nonce), None, t0).unwrap();
            set.record_send(&hash, t0 + Duration::from_millis(offset), sent(10));
        }
        let due: Vec<u64> = set
            .take_due(t0 + RESEND * 2)
            .into_iter()
            .map(|(_, tx, ..)| tx.nonce)
            .collect();
        assert_eq!(due, vec![2, 3, 1]);
    }

    #[test]
    fn finished_records_are_evicted_oldest_first() {
        let mut set = PendingSet::new(config());
        let t0 = Instant::now();
        for nonce in 0..3 {
            set.submit(tx(nonce), None, t0).unwrap();
        }
        for nonce in 0..3 {
            set.commit(&tx(nonce).hash(), commit(nonce));
        }
        assert!(set.get(&tx(0).hash()).is_none());
        assert!(set.get(&tx(1).hash()).is_some());
        assert!(set.get(&tx(2).hash()).is_some());
        assert!(!set.commit(&tx(0).hash(), commit(0)));
        // an evicted tx is new again
        assert_eq!(set.submit(tx(0), None, t0).unwrap().1, Admission::New);
    }

    #[test]
    fn history_is_capped_at_max_attempts() {
        let mut set = PendingSet::new(config());
        let t0 = Instant::now();
        let (hash, _) = set.submit(tx(1), None, t0).unwrap();
        for _ in 0..10 {
            set.record_send(&hash, t0, sent(10));
        }
        assert_eq!(set.get(&hash).unwrap().attempts(), 3);
        assert!(!set.commit(&tx(2).hash(), commit(1)));
    }
}
