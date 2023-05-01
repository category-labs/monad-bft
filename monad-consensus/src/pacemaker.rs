use std::{collections::HashMap, time::Duration};

use monad_crypto::Signature;
use monad_types::{NodeId, Round};
use monad_validator::{leader_election::LeaderElection, validator_set::ValidatorSet};

use crate::{
    types::{
        message::TimeoutMessage,
        quorum_certificate::QuorumCertificate,
        signature::SignatureCollection,
        timeout::{HighQcRound, HighQcRoundSigTuple, TimeoutCertificate},
    },
    validation::safety::Safety,
};

pub struct Pacemaker<S, T> {
    delta: Duration,

    current_round: Round,
    last_round_tc: Option<TimeoutCertificate<S>>,

    // only need to store for current round
    pending_timeouts: HashMap<NodeId, (TimeoutMessage<S, T>, S)>,

    // used to not duplicate broadcast/tc
    phase: PhaseHonest,
}

#[derive(PartialEq)]
enum PhaseHonest {
    Zero,
    One,
    Supermajority,
}

#[derive(Debug)]
pub enum PacemakerCommand<S, T> {
    Broadcast(TimeoutMessage<S, T>),
    Schedule {
        duration: Duration,
        on_timeout: PacemakerTimerExpire,
    },
    Unschedule,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PacemakerTimerExpire;

impl<S, T> Pacemaker<S, T>
where
    S: Signature,
    T: SignatureCollection,
{
    pub fn new(
        delta: Duration,
        current_round: Round,
        last_round_tc: Option<TimeoutCertificate<S>>,
    ) -> Self {
        Self {
            delta,
            current_round,
            last_round_tc,
            pending_timeouts: HashMap::new(),

            phase: PhaseHonest::Zero,
        }
    }

    pub fn get_current_round(&self) -> Round {
        self.current_round
    }

    pub fn next_round(&self) -> Round {
        self.current_round + Round(1)
    }

    fn get_round_timer(&self) -> Duration {
        self.delta * 4
    }

    #[must_use]
    fn start_timer(&mut self, new_round: Round) -> PacemakerCommand<S, T> {
        assert!(new_round > self.current_round);

        self.phase = PhaseHonest::Zero;
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
    ) -> Option<PacemakerCommand<S, T>> {
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
    ) -> Option<PacemakerCommand<S, T>> {
        self.phase = PhaseHonest::One;
        self.local_timeout_round(safety, high_qc)
    }

    #[must_use]
    pub fn process_remote_timeout<L: LeaderElection>(
        &mut self,
        validators: &ValidatorSet<L>,
        safety: &mut Safety,
        high_qc: &QuorumCertificate<T>,
        author: NodeId,
        signature: S,
        tmo: TimeoutMessage<S, T>,
    ) -> (Option<TimeoutCertificate<S>>, Vec<PacemakerCommand<S, T>>) {
        let mut ret_commands = Vec::new();

        let tm_info = &tmo.tminfo;
        if tm_info.round < self.current_round {
            return (None, ret_commands);
        }
        assert_eq!(tm_info.round, self.current_round);

        // it's fine to overwrite if already exists
        self.pending_timeouts
            .insert(author, (tmo.clone(), signature));

        let timeouts = self.pending_timeouts.keys().copied().collect();

        if self.phase == PhaseHonest::Zero && validators.has_honest_vote(&timeouts) {
            ret_commands.push(PacemakerCommand::Unschedule);
            ret_commands.extend(self.local_timeout_round(safety, high_qc));
            self.phase = PhaseHonest::One;
        }
        let mut ret_tc = None;
        if self.phase == PhaseHonest::One && validators.has_super_majority_votes(&timeouts) {
            ret_tc = Some(TimeoutCertificate {
                round: tm_info.round,
                high_qc_rounds: self
                    .pending_timeouts
                    .values()
                    .map(|(tmo, signature)| {
                        assert_eq!(tmo.tminfo.round, tm_info.round);
                        HighQcRoundSigTuple {
                            high_qc_round: HighQcRound {
                                qc_round: tmo.tminfo.high_qc.info.vote.round,
                            },
                            author_signature: *signature,
                        }
                    })
                    .collect(),
            });
            self.phase = PhaseHonest::Supermajority;
        }

        (ret_tc, ret_commands)
    }

    #[must_use]
    pub fn advance_round_tc(
        &mut self,
        tc: &TimeoutCertificate<S>,
    ) -> Option<PacemakerCommand<S, T>> {
        if tc.round < self.current_round {
            return None;
        }
        let round = tc.round;
        self.last_round_tc = Some(tc.clone());
        Some(self.start_timer(round + Round(1)))
    }

    #[must_use]
    pub fn advance_round_qc(
        &mut self,
        qc: &QuorumCertificate<T>,
    ) -> Option<PacemakerCommand<S, T>> {
        if qc.info.vote.round < self.current_round {
            return None;
        }
        self.last_round_tc = None;
        Some(self.start_timer(qc.info.vote.round + Round(1)))
    }
}
