use std::{collections::HashMap, time::Duration};

use monad_types::{NodeId, Round};
use monad_validator::{leader_election::LeaderElection, validator_set::ValidatorSet};

use crate::{
    types::{
        message::TimeoutMessage,
        quorum_certificate::QuorumCertificate,
        signature::SignatureCollection,
        timeout::{HighQcRound, TimeoutCertificate},
    },
    validation::{safety::Safety, signing::Verified},
};

pub struct Pacemaker<T> {
    delta: Duration,

    current_round: Round,
    last_round_tc: Option<TimeoutCertificate>,

    // only need to store for current round
    pending_timeouts: HashMap<NodeId, Verified<TimeoutMessage<T>>>,

    // used to not duplicate broadcast/tc
    phase: Phase,
}

#[derive(PartialEq)]
enum Phase {
    ZeroHonest,
    OneHonest,
    SupermajorityHonest,
}

pub enum PacemakerCommand<T> {
    Broadcast(TimeoutMessage<T>),
    Schedule {
        duration: Duration,
        on_timeout: PacemakerTimerExpire,
    },
    Unschedule,
}

#[derive(Debug, Clone)]
pub struct PacemakerTimerExpire;

impl<T> Pacemaker<T>
where
    T: SignatureCollection,
{
    pub fn new(
        delta: Duration,
        current_round: Round,
        last_round_tc: Option<TimeoutCertificate>,
        pending_timeouts: HashMap<NodeId, Verified<TimeoutMessage<T>>>,
    ) -> Self {
        Self {
            delta,
            current_round,
            last_round_tc,
            pending_timeouts,

            phase: Phase::ZeroHonest,
        }
    }

    pub fn get_current_round(&self) -> Round {
        self.current_round
    }

    fn get_round_timer(&self) -> Duration {
        self.delta * 4
    }

    #[must_use]
    fn start_timer(&mut self, new_round: Round) -> PacemakerCommand<T> {
        assert!(new_round > self.current_round);

        self.phase = Phase::ZeroHonest;
        self.pending_timeouts.clear();

        self.current_round = new_round;

        PacemakerCommand::Schedule {
            duration: self.get_round_timer(),
            on_timeout: PacemakerTimerExpire,
        }
    }

    #[must_use]
    fn local_timeout_round(
        &self,
        safety: &mut Safety,
        high_qc: &QuorumCertificate<T>,
    ) -> Option<PacemakerCommand<T>> {
        safety
            .make_timeout(self.current_round, high_qc.clone(), &self.last_round_tc)
            .map(|timeout_info| {
                PacemakerCommand::Broadcast(TimeoutMessage {
                    tminfo: timeout_info,
                    last_round_tc: self.last_round_tc.clone(),
                })
            })
    }

    #[must_use]
    pub fn handle_event(
        &mut self,
        safety: &mut Safety,
        high_qc: &QuorumCertificate<T>,
        _event: PacemakerTimerExpire,
    ) -> Option<PacemakerCommand<T>> {
        self.phase = Phase::OneHonest;
        self.local_timeout_round(safety, high_qc)
    }

    #[must_use]
    pub fn process_remote_timeout<L: LeaderElection>(
        &mut self,
        validators: &ValidatorSet<L>,
        safety: &mut Safety,
        high_qc: &QuorumCertificate<T>,
        tmo: Verified<TimeoutMessage<T>>,
    ) -> (Option<TimeoutCertificate>, Vec<PacemakerCommand<T>>) {
        let mut ret_commands = Vec::new();

        let tm_info = &tmo.tminfo;
        if tm_info.round < self.current_round {
            return (None, ret_commands);
        }
        assert_eq!(tm_info.round, self.current_round);

        // it's fine to overwrite if already exists
        self.pending_timeouts.insert(*tmo.author(), tmo.clone());

        let timeouts = self.pending_timeouts.keys().copied().collect();

        if self.phase == Phase::ZeroHonest && validators.has_honest_vote(&timeouts) {
            ret_commands.push(PacemakerCommand::Unschedule);
            ret_commands.extend(self.local_timeout_round(safety, high_qc));
            self.phase = Phase::OneHonest;
        }
        let mut ret_tc = None;
        if self.phase == Phase::OneHonest && validators.has_super_majority_votes(&timeouts) {
            ret_tc = Some(TimeoutCertificate {
                round: tm_info.round,
                high_qc_rounds: self
                    .pending_timeouts
                    .values()
                    .map(|tmo| {
                        assert_eq!(tmo.tminfo.round, tm_info.round);
                        (
                            HighQcRound {
                                qc_round: tmo.tminfo.high_qc.info.vote.round,
                            },
                            *tmo.author_signature(),
                        )
                    })
                    .collect(),
            });
            self.phase = Phase::SupermajorityHonest;
        }

        (ret_tc, ret_commands)
    }

    #[must_use]
    pub fn advance_round_tc(&mut self, tc: &TimeoutCertificate) -> Option<PacemakerCommand<T>> {
        if tc.round < self.current_round {
            return None;
        }
        let round = tc.round;
        self.last_round_tc = Some(tc.clone());
        Some(self.start_timer(round + Round(1)))
    }

    #[must_use]
    pub fn advance_round_qc(&mut self, qc: &QuorumCertificate<T>) -> Option<PacemakerCommand<T>> {
        if qc.info.vote.round < self.current_round {
            return None;
        }
        self.last_round_tc = None;
        Some(self.start_timer(qc.info.vote.round + Round(1)))
    }
}
