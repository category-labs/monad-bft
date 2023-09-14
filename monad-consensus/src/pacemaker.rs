use std::{collections::HashMap, time::Duration};

use monad_consensus_types::{
    quorum_certificate::QuorumCertificate,
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    timeout::{Timeout, TimeoutCertificate},
    validation::Hasher,
    voting::ValidatorMapping,
};
use monad_types::{NodeId, Round};
use monad_validator::validator_set::ValidatorSetType;

use crate::{
    messages::message::TimeoutMessage,
    validation::{message::well_formed, safety::Safety},
};

#[derive(Debug)]
pub struct Pacemaker<SCT: SignatureCollection> {
    delta: Duration,

    current_round: Round,
    last_round_tc: Option<TimeoutCertificate<SCT>>,

    // only need to store for current round
    pending_timeouts: HashMap<NodeId, TimeoutMessage<SCT>>,

    // used to not duplicate broadcast/tc
    phase: PhaseHonest,
}

#[derive(Debug, PartialEq)]
enum PhaseHonest {
    Zero,
    One,
    Supermajority,
}

#[derive(Debug)]
pub enum PacemakerCommand<SCT: SignatureCollection> {
    PrepareTimeout(Timeout<SCT>),
    Broadcast(TimeoutMessage<SCT>),
    Schedule {
        duration: Duration,
        on_timeout: PacemakerTimerExpire,
    },
    ScheduleReset,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PacemakerTimerExpire;

impl<SCT: SignatureCollection> Pacemaker<SCT> {
    pub fn new(
        delta: Duration,
        current_round: Round,
        last_round_tc: Option<TimeoutCertificate<SCT>>,
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

    fn get_round_timer(&self) -> Duration {
        self.delta * 4
    }

    #[must_use]
    fn start_timer(&mut self, new_round: Round) -> PacemakerCommand<SCT> {
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
        high_qc: &QuorumCertificate<SCT>,
    ) -> Vec<PacemakerCommand<SCT>> {
        let mut cmds = vec![PacemakerCommand::ScheduleReset];
        let maybe_broadcast = safety
            .make_timeout(self.current_round, high_qc.clone(), &self.last_round_tc)
            .map(|timeout_info| {
                well_formed(
                    timeout_info.round,
                    timeout_info.high_qc.info.vote.round,
                    &self.last_round_tc,
                )
                .expect("invalid timeout");

                PacemakerCommand::PrepareTimeout(Timeout {
                    tminfo: timeout_info,
                    last_round_tc: self.last_round_tc.clone(),
                })
            });
        cmds.extend(maybe_broadcast);
        cmds
    }

    #[must_use]
    pub fn handle_event(
        &mut self,
        safety: &mut Safety,
        high_qc: &QuorumCertificate<SCT>,
        _event: PacemakerTimerExpire,
    ) -> Vec<PacemakerCommand<SCT>> {
        self.phase = PhaseHonest::One;
        self.local_timeout_round(safety, high_qc)
    }

    #[must_use]
    pub fn process_remote_timeout<H: Hasher, VST: ValidatorSetType>(
        &mut self,
        validators: &VST,
        validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
        safety: &mut Safety,
        high_qc: &QuorumCertificate<SCT>,
        author: NodeId,
        tmo: TimeoutMessage<SCT>,
    ) -> (Option<TimeoutCertificate<SCT>>, Vec<PacemakerCommand<SCT>>) {
        let mut ret_commands = Vec::new();

        let tm_info = &tmo.timeout.tminfo;
        if tm_info.round < self.current_round {
            return (None, ret_commands);
        }
        assert_eq!(tm_info.round, self.current_round);

        // it's fine to overwrite if already exists
        self.pending_timeouts.insert(author, tmo.clone());

        let timeouts: Vec<NodeId> = self.pending_timeouts.keys().copied().collect();

        if self.phase == PhaseHonest::Zero && validators.has_honest_vote(&timeouts) {
            // self.local_timeout_round emits PacemakerCommand::ScheduleReset
            ret_commands.extend(self.local_timeout_round(safety, high_qc));
            self.phase = PhaseHonest::One;
        }
        let mut ret_tc: Option<TimeoutCertificate<SCT>> = None;
        if self.phase == PhaseHonest::One && validators.has_super_majority_votes(&timeouts) {
            // FIXME: error handling when creating certificate
            ret_tc = Some(
                TimeoutCertificate::new::<H>(
                    tm_info.round,
                    self.pending_timeouts
                        .iter()
                        .map(|(node_id, tmo_msg)| {
                            (*node_id, tmo_msg.timeout.tminfo.clone(), tmo_msg.sig)
                        })
                        .collect::<Vec<_>>()
                        .as_slice(),
                    validator_mapping,
                )
                .expect("TimeoutCertificate creation"),
            );

            self.phase = PhaseHonest::Supermajority;
        }

        (ret_tc, ret_commands)
    }

    #[must_use]
    pub fn advance_round_tc(
        &mut self,
        tc: &TimeoutCertificate<SCT>,
    ) -> Option<PacemakerCommand<SCT>> {
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
        qc: &QuorumCertificate<SCT>,
    ) -> Option<PacemakerCommand<SCT>> {
        if qc.info.vote.round < self.current_round {
            return None;
        }
        self.last_round_tc = None;
        Some(self.start_timer(qc.info.vote.round + Round(1)))
    }
}
