use std::{collections::BTreeMap, time::Duration};

use monad_consensus_types::{
    metrics::Metrics,
    quorum_certificate::QuorumCertificate,
    signature_collection::{
        SignatureCollection, SignatureCollectionError, SignatureCollectionKeyPairType,
    },
    timeout::{Timeout, TimeoutCertificate},
    voting::ValidatorMapping,
};
use monad_types::{Epoch, NodeId, Round};
use monad_validator::{epoch_manager::EpochManager, validator_set::ValidatorSetType};

use crate::{
    messages::message::TimeoutMessage,
    validation::{message::well_formed, safety::Safety},
};

/// Pacemaker is responsible for tracking and advancing rounds
/// Rounds are advanced when QC/TCs are received.
/// Local round timeouts produce a timeout msg which is broadcast
/// to other nodes
/// Timeout msgs from other nodes are received and collected into
/// a Timeout Certificate which will advance the round
#[derive(PartialEq, Eq, Debug)]
pub struct Pacemaker<SCT: SignatureCollection> {
    /// upper transmission delay bound
    /// this is used to calculate the local round timeout duration
    delta: Duration,

    current_epoch: Epoch,
    current_round: Round,
    /// None if we advanced to the current round via QC
    /// Some(TC) if we advanced to the current round via TC
    last_round_tc: Option<TimeoutCertificate<SCT>>,

    /// map of the TimeoutMessages from other Nodes
    /// only needs to be stored for the current round as timeout messages
    /// carry a QC which will be processed and advance the node to the
    /// highest known round
    pending_timeouts: BTreeMap<NodeId<SCT::NodeIdPubKey>, TimeoutMessage<SCT>>,

    /// States for TimeoutMessage handling, ensures we only broadcast
    /// one message at each phase of handling
    phase: PhaseHonest,
}

/// The different states of TimeoutMessage handling
#[derive(Debug, PartialEq, Eq)]
enum PhaseHonest {
    /// Unsure that any honest timeout messages have been received
    /// ie, this Node itself has not timed out and it has less than F+1
    /// external timeout messages
    Zero,

    /// At least one honest timeout message has been received
    /// This requires either a local timeout (Node assumes itself to be honest)
    /// or F+1 external timeout messages
    One,

    /// 2F+1 timeout messages received
    Supermajority,
}

#[derive(Debug)]
pub enum PacemakerCommand<SCT: SignatureCollection> {
    /// event emitted whenever round changes. this is used by the router
    EnterRound((Epoch, Round)),

    /// create the Timeout which can be signed to create a TimeoutMessage
    /// this should be broadcast to all other nodes
    PrepareTimeout(Timeout<SCT>),

    /// schedule a local round timeout event after duration
    Schedule { duration: Duration },

    /// cancel the current local round timeout
    ScheduleReset,
}

impl<SCT: SignatureCollection> Pacemaker<SCT> {
    pub fn new(
        delta: Duration,
        current_epoch: Epoch,
        current_round: Round,
        last_round_tc: Option<TimeoutCertificate<SCT>>,
    ) -> Self {
        Self {
            delta,
            current_epoch,
            current_round,
            last_round_tc,
            pending_timeouts: BTreeMap::new(),

            phase: PhaseHonest::Zero,
        }
    }

    pub fn get_current_round(&self) -> Round {
        self.current_round
    }

    pub fn get_current_epoch(&self) -> Epoch {
        self.current_epoch
    }

    pub fn get_last_round_tc(&self) -> &Option<TimeoutCertificate<SCT>> {
        &self.last_round_tc
    }

    fn get_round_timer(&self) -> Duration {
        self.delta * 4
    }

    /// enter a new round. Phase is set to PhaseHonest::Zero and all
    /// pending timeout messages are cleared.
    /// Creates the command to start the local round timeout
    #[must_use]
    fn enter_round(&mut self, new_epoch: Epoch, new_round: Round) -> Vec<PacemakerCommand<SCT>> {
        assert!(new_epoch > self.current_epoch || new_round > self.current_round);

        self.phase = PhaseHonest::Zero;
        self.pending_timeouts.clear();

        self.current_epoch = new_epoch;
        self.current_round = new_round;

        vec![
            PacemakerCommand::EnterRound((new_epoch, new_round)),
            PacemakerCommand::Schedule {
                duration: self.get_round_timer(),
            },
        ]
    }

    /// invoked on local round timeout
    /// creates the TimeoutMessage to be broadcast and reschedules
    /// the local round timer
    #[must_use]
    fn local_timeout_round(
        &self,
        safety: &mut Safety,
        high_qc: &QuorumCertificate<SCT>,
    ) -> Vec<PacemakerCommand<SCT>> {
        let mut cmds = vec![PacemakerCommand::ScheduleReset];
        let maybe_broadcast = safety
            .make_timeout(
                self.current_epoch,
                self.current_round,
                high_qc.clone(),
                &self.last_round_tc,
            )
            .map(|timeout_info| {
                well_formed(
                    timeout_info.round,
                    timeout_info.high_qc.get_round(),
                    &self.last_round_tc,
                )
                .expect("invalid timeout");

                PacemakerCommand::PrepareTimeout(Timeout {
                    tminfo: timeout_info,
                    last_round_tc: self.last_round_tc.clone(),
                })
            });
        cmds.extend(maybe_broadcast);
        cmds.push(PacemakerCommand::Schedule {
            duration: self.get_round_timer(),
        });
        cmds
    }

    /// handle the local timeout event
    #[must_use]
    pub fn handle_event(
        &mut self,
        safety: &mut Safety,
        high_qc: &QuorumCertificate<SCT>,
    ) -> Vec<PacemakerCommand<SCT>> {
        self.phase = PhaseHonest::One;
        self.local_timeout_round(safety, high_qc)
    }

    /// handle timeout messages from external nodes
    /// if f+1 timeout messages are received, broadcast a timeout message if it
    /// has not done so before
    /// if 2f+1 timeout messages are received, create the Timeout Certificate
    #[must_use]
    pub fn process_remote_timeout<VST>(
        &mut self,
        validators: &VST,
        validator_mapping: &ValidatorMapping<
            SCT::NodeIdPubKey,
            SignatureCollectionKeyPairType<SCT>,
        >,
        safety: &mut Safety,
        high_qc: &QuorumCertificate<SCT>,
        author: NodeId<SCT::NodeIdPubKey>,
        timeout_msg: TimeoutMessage<SCT>,
    ) -> (Option<TimeoutCertificate<SCT>>, Vec<PacemakerCommand<SCT>>)
    where
        VST: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
    {
        let mut ret_commands = Vec::new();

        let tm_info = &timeout_msg.timeout.tminfo;
        if tm_info.round < self.current_round {
            return (None, ret_commands);
        }
        // timeout messages carry a high QC which must have been processed
        // prior to entering this function. Processing that QC guarantees
        // this invariant
        assert_eq!(tm_info.round, self.current_round);

        // it's fine to overwrite if already exists
        self.pending_timeouts.insert(author, timeout_msg.clone());

        let mut timeouts: Vec<NodeId<_>> = self.pending_timeouts.keys().copied().collect();

        if self.phase == PhaseHonest::Zero && validators.has_honest_vote(&timeouts) {
            // self.local_timeout_round emits PacemakerCommand::ScheduleReset
            ret_commands.extend(self.local_timeout_round(safety, high_qc));
            self.phase = PhaseHonest::One;
        }

        let mut ret_tc: Option<TimeoutCertificate<SCT>> = None;
        // try to create a TimeoutCertificate from the pending timeouts, filtering out
        // invalid timeout messages if there are signature errors
        while self.phase == PhaseHonest::One && validators.has_super_majority_votes(&timeouts) {
            match TimeoutCertificate::new(
                tm_info.epoch,
                tm_info.round,
                self.pending_timeouts
                    .iter()
                    .map(|(node_id, tm_msg)| (*node_id, tm_msg.timeout.tminfo.clone(), tm_msg.sig))
                    .collect::<Vec<_>>()
                    .as_slice(),
                validator_mapping,
            ) {
                Ok(tc) => {
                    self.phase = PhaseHonest::Supermajority;
                    ret_tc = Some(tc);
                }
                Err(err) => match err {
                    SignatureCollectionError::InvalidSignaturesCreate(invalid_sigs) => {
                        ret_commands.extend(self.handle_invalid_timeout(invalid_sigs));
                        timeouts = self.pending_timeouts.keys().copied().collect();
                    }
                    _ => {
                        unreachable!("unexpected error {}", err)
                    }
                },
            };
        }

        (ret_tc, ret_commands)
    }

    /// remove invalid timeout messages and collect evidence
    #[must_use]
    fn handle_invalid_timeout(
        &mut self,
        invalid_timeouts: Vec<(NodeId<SCT::NodeIdPubKey>, SCT::SignatureType)>,
    ) -> Vec<PacemakerCommand<SCT>> {
        for (node_id, sig) in invalid_timeouts {
            let removed = self.pending_timeouts.remove(&node_id);
            debug_assert_eq!(removed.expect("Timeout removed").sig, sig);
        }
        // TODO-3: evidence collection
        vec![]
    }

    /// advance the round based on a TC
    /// sets the last_round_tc to this TC
    /// TCs from older rounds are ignored
    #[must_use]
    pub fn advance_round_tc(
        &mut self,
        tc: &TimeoutCertificate<SCT>,
        epoch_manager: &EpochManager,
        metrics: &mut Metrics,
    ) -> Vec<PacemakerCommand<SCT>> {
        if tc.round < self.current_round {
            return Default::default();
        }
        metrics.consensus_events.enter_new_round_tc += 1;
        let new_round = tc.round + Round(1);
        let new_epoch = epoch_manager.get_epoch(new_round);
        self.last_round_tc = Some(tc.clone());
        self.enter_round(new_epoch, new_round)
    }

    /// advance the round based on a QC
    /// clears last_round_tc
    /// QCs from older rounds are ignored
    #[must_use]
    pub fn advance_round_qc(
        &mut self,
        qc: &QuorumCertificate<SCT>,
        epoch_manager: &EpochManager,
        metrics: &mut Metrics,
    ) -> Vec<PacemakerCommand<SCT>> {
        if qc.get_round() < self.current_round {
            return Default::default();
        }
        metrics.consensus_events.enter_new_round_qc += 1;
        self.last_round_tc = None;
        let new_round = qc.get_round() + Round(1);
        let new_epoch = epoch_manager.get_epoch(new_round);
        self.enter_round(new_epoch, new_round)
    }

    /// Update current epoch based on epoch manager. This function is used to
    /// sync pacemaker with current records in epoch manager
    #[must_use]
    pub fn advance_epoch(&mut self, epoch_manager: &EpochManager) -> Vec<PacemakerCommand<SCT>> {
        let new_epoch = epoch_manager.get_epoch(self.current_round);
        if new_epoch <= self.current_epoch {
            return Default::default();
        }
        self.enter_round(new_epoch, self.current_round)
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use monad_consensus_types::{
        ledger::CommitResult,
        quorum_certificate::QcInfo,
        timeout::TimeoutInfo,
        voting::{Vote, VoteInfo},
    };
    use monad_crypto::{
        certificate_signature::{CertificateKeyPair, CertificateSignature},
        hasher::{Hash, Hasher, HasherType},
        NopSignature,
    };
    use monad_multi_sig::MultiSig;
    use monad_testutil::{
        signing::{create_certificate_keys, create_keys},
        validators::create_keys_w_validators,
    };
    use monad_types::{BlockId, Epoch, SeqNum, Stake};
    use monad_validator::validator_set::{ValidatorSetFactory, ValidatorSetTypeFactory};
    use zerocopy::AsBytes;

    use super::*;

    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<SignatureType>;

    fn get_high_qc<SCT: SignatureCollection>(
        qc_epoch: Epoch,
        qc_round: Round,
        keys: &[SCT::NodeIdPubKey],
        certkeys: &[SignatureCollectionKeyPairType<SCT>],
        valmap: &ValidatorMapping<SCT::NodeIdPubKey, SignatureCollectionKeyPairType<SCT>>,
    ) -> QuorumCertificate<SCT> {
        let vote_info = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            epoch: qc_epoch,
            round: qc_round,
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let vote = Vote {
            vote_info,
            ledger_commit_info: CommitResult::NoCommit,
        };
        let qc_info = QcInfo { vote };
        let vote_hash = HasherType::hash_object(&vote);

        let mut sigs = Vec::new();
        for (key, certkey) in keys.iter().zip(certkeys.iter()) {
            let node_id = NodeId::new(*key);
            let sig =
                <SCT::SignatureType as CertificateSignature>::sign(vote_hash.as_ref(), certkey);
            sigs.push((node_id, sig));
        }

        let sigcol = SCT::new(sigs, valmap, vote_hash.as_ref()).expect("success");

        QuorumCertificate::<SCT>::new(qc_info, sigcol)
    }

    fn create_timeout_message<SCT: SignatureCollection>(
        certkeypair: &SignatureCollectionKeyPairType<SCT>,
        timeout_epoch: Epoch,
        timeout_round: Round,
        high_qc: QuorumCertificate<SCT>,
        valid: bool,
    ) -> TimeoutMessage<SCT> {
        let timeout = Timeout {
            tminfo: TimeoutInfo {
                epoch: timeout_epoch,
                round: timeout_round,
                high_qc,
            },
            last_round_tc: None,
        };

        let invalid_msg = b"invalid";

        let mut tmo_msg = TimeoutMessage::<SCT>::new(timeout, certkeypair);
        if !valid {
            tmo_msg.sig =
                <SCT::SignatureType as CertificateSignature>::sign(invalid_msg, certkeypair);
        }
        tmo_msg
    }

    #[test]
    fn all_honest() {
        let mut pacemaker = Pacemaker::<SignatureCollectionType>::new(
            Duration::from_secs(1),
            Epoch(1),
            Round(1),
            None,
        );
        let mut safety = Safety::default();

        let (keys, certkeys, valset, vmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(4, ValidatorSetFactory::default());
        let timeout_epoch = Epoch(1);
        let timeout_round = Round(1);
        let high_qc = get_high_qc(
            Epoch(1),
            Round(0),
            keys.iter()
                .map(CertificateKeyPair::pubkey)
                .collect::<Vec<_>>()
                .as_slice(),
            certkeys.as_slice(),
            &vmap,
        );

        let tm0 = create_timeout_message(
            &certkeys[0],
            timeout_epoch,
            timeout_round,
            high_qc.clone(),
            true,
        );
        let tm1 = create_timeout_message(
            &certkeys[1],
            timeout_epoch,
            timeout_round,
            high_qc.clone(),
            true,
        );
        let tm2 = create_timeout_message(
            &certkeys[2],
            timeout_epoch,
            timeout_round,
            high_qc.clone(),
            true,
        );
        let tm3 = create_timeout_message(
            &certkeys[3],
            timeout_epoch,
            timeout_round,
            high_qc.clone(),
            true,
        );

        let (tc, cmds) = pacemaker.process_remote_timeout(
            &valset,
            &vmap,
            &mut safety,
            &high_qc,
            NodeId::new(keys[0].pubkey()),
            tm0,
        );
        assert!(tc.is_none());
        assert!(cmds.is_empty());

        // enter PhaseHonest::One, timeout itself
        let (tc, cmds) = pacemaker.process_remote_timeout(
            &valset,
            &vmap,
            &mut safety,
            &high_qc,
            NodeId::new(keys[1].pubkey()),
            tm1,
        );
        assert_eq!(pacemaker.phase, PhaseHonest::One);
        assert!(tc.is_none());
        assert_eq!(cmds.len(), 3);
        assert!(matches!(cmds[0], PacemakerCommand::ScheduleReset));
        assert!(matches!(cmds[1], PacemakerCommand::PrepareTimeout(_)));
        assert!(matches!(cmds[2], PacemakerCommand::Schedule { .. }));

        // enter PhaseHonest::SuperMajority, TC is created
        let (tc, cmds) = pacemaker.process_remote_timeout(
            &valset,
            &vmap,
            &mut safety,
            &high_qc,
            NodeId::new(keys[2].pubkey()),
            tm2,
        );
        assert_eq!(pacemaker.phase, PhaseHonest::Supermajority);
        assert!(tc.is_some());
        assert!(cmds.is_empty());

        // in PhaseHonest::SuperMajority, pacemaker doesn't create TC with new timeouts
        let (tc, cmds) = pacemaker.process_remote_timeout(
            &valset,
            &vmap,
            &mut safety,
            &high_qc,
            NodeId::new(keys[3].pubkey()),
            tm3,
        );
        assert!(tc.is_none());
        assert!(cmds.is_empty());
    }

    #[test]
    fn phase_supermajority_invalid_no_progress() {
        let mut pacemaker = Pacemaker::<SignatureCollectionType>::new(
            Duration::from_secs(1),
            Epoch(1),
            Round(1),
            None,
        );
        let mut safety = Safety::default();

        let (keys, certkeys, valset, vmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(4, ValidatorSetFactory::default());
        let timeout_epoch = Epoch(1);
        let timeout_round = Round(1);
        let high_qc = get_high_qc(
            Epoch(1),
            Round(0),
            keys.iter()
                .map(CertificateKeyPair::pubkey)
                .collect::<Vec<_>>()
                .as_slice(),
            certkeys.as_slice(),
            &vmap,
        );

        let tm0_valid = create_timeout_message(
            &certkeys[0],
            timeout_epoch,
            timeout_round,
            high_qc.clone(),
            true,
        );
        let tm1_valid = create_timeout_message(
            &certkeys[1],
            timeout_epoch,
            timeout_round,
            high_qc.clone(),
            true,
        );
        let tm2_invalid = create_timeout_message(
            &certkeys[2],
            timeout_epoch,
            timeout_round,
            high_qc.clone(),
            false,
        );

        let _ = pacemaker.process_remote_timeout(
            &valset,
            &vmap,
            &mut safety,
            &high_qc,
            NodeId::new(keys[0].pubkey()),
            tm0_valid,
        );

        let _ = pacemaker.process_remote_timeout(
            &valset,
            &vmap,
            &mut safety,
            &high_qc,
            NodeId::new(keys[1].pubkey()),
            tm1_valid,
        );

        let (tc, cmds) = pacemaker.process_remote_timeout(
            &valset,
            &vmap,
            &mut safety,
            &high_qc,
            NodeId::new(keys[2].pubkey()),
            tm2_invalid,
        );
        assert!(tc.is_none());
        assert!(cmds.is_empty());
        assert_eq!(pacemaker.phase, PhaseHonest::One);
        assert_eq!(pacemaker.pending_timeouts.len(), 2);
    }

    #[test]
    fn phase_supermajority_invalid_progress() {
        let mut pacemaker = Pacemaker::<SignatureCollectionType>::new(
            Duration::from_secs(1),
            Epoch(1),
            Round(1),
            None,
        );
        let mut safety = Safety::default();

        let keys = create_keys::<SignatureType>(4);
        let certkeys = create_certificate_keys::<SignatureCollectionType>(4);

        let mut staking_list = keys
            .iter()
            .map(|k| NodeId::new(k.pubkey()))
            .zip(std::iter::repeat(Stake(1)))
            .collect::<Vec<_>>();

        // total stake = 7, f = 2
        // node1 holds f+1 stake
        // node1 + node2 has 2f+1 stake
        staking_list[0].1 = Stake(1);
        staking_list[1].1 = Stake(3);
        staking_list[2].1 = Stake(2);
        staking_list[3].1 = Stake(1);

        let voting_identity = keys
            .iter()
            .map(|k| NodeId::new(k.pubkey()))
            .zip(certkeys.iter().map(|k| k.pubkey()))
            .collect::<Vec<_>>();

        let valset = ValidatorSetFactory::default()
            .create(staking_list)
            .expect("create validator set");
        let vmap = ValidatorMapping::new(voting_identity);

        let epoch_manager = EpochManager::new(SeqNum(1000), Round(50));
        let timeout_epoch = Epoch(1);
        let timeout_round = Round(1);
        let high_qc = get_high_qc(
            Epoch(1),
            Round(0),
            keys.iter()
                .map(CertificateKeyPair::pubkey)
                .collect::<Vec<_>>()
                .as_slice(),
            certkeys.as_slice(),
            &vmap,
        );

        let tm0_invalid = create_timeout_message(
            &certkeys[0],
            timeout_epoch,
            timeout_round,
            high_qc.clone(),
            false,
        );
        let tm1_valid = create_timeout_message(
            &certkeys[1],
            timeout_epoch,
            timeout_round,
            high_qc.clone(),
            true,
        );
        let tm2_valid = create_timeout_message(
            &certkeys[2],
            timeout_epoch,
            timeout_round,
            high_qc.clone(),
            true,
        );

        let _ = pacemaker.process_remote_timeout(
            &valset,
            &vmap,
            &mut safety,
            &high_qc,
            NodeId::new(keys[1].pubkey()),
            tm1_valid,
        );

        let (tc, _cmds) = pacemaker.process_remote_timeout(
            &valset,
            &vmap,
            &mut safety,
            &high_qc,
            NodeId::new(keys[0].pubkey()),
            tm0_invalid,
        );
        assert!(tc.is_none());
        assert_eq!(pacemaker.pending_timeouts.len(), 2);

        // invalid timeout is removed
        // the remaining two timeouts has 5/7 stake, so TC is created
        let (tc, cmds) = pacemaker.process_remote_timeout(
            &valset,
            &vmap,
            &mut safety,
            &high_qc,
            NodeId::new(keys[2].pubkey()),
            tm2_valid,
        );
        assert_eq!(pacemaker.phase, PhaseHonest::Supermajority);
        assert!(tc.is_some());

        // assert the TC is created over the two valid timeouts
        let mut hasher = HasherType::new();
        hasher.update(Epoch(1).as_bytes());
        hasher.update(Round(1).as_bytes());
        hasher.update(Round(0).as_bytes());
        let timeout_hash = hasher.hash();
        let tc = tc.unwrap();
        assert_eq!(tc.high_qc_rounds.len(), 1);
        let sc = tc.high_qc_rounds.first().unwrap().sigs.clone();
        assert_eq!(
            sc.verify(&vmap, timeout_hash.as_ref())
                .unwrap()
                .into_iter()
                .collect::<HashSet<_>>(),
            vec![NodeId::new(keys[1].pubkey()), NodeId::new(keys[2].pubkey())]
                .into_iter()
                .collect::<HashSet<_>>()
        );

        assert!(cmds.is_empty());
        assert_eq!(pacemaker.pending_timeouts.len(), 2);
    }

    #[test]
    fn test_advance_epoch_clear() {
        let mut pacemaker = Pacemaker::<SignatureCollectionType>::new(
            Duration::from_secs(1),
            Epoch(1),
            Round(120),
            None,
        );

        let mut epoch_manager = EpochManager::new(SeqNum(100), Round(20));
        epoch_manager.schedule_epoch_start(SeqNum(100), Round(100));

        let (keys, certkeys, _valset, vmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(4, ValidatorSetFactory::default());
        let timeout_epoch = Epoch(1);
        let timeout_round = Round(1);
        let high_qc = get_high_qc(
            Epoch(1),
            Round(119),
            keys.iter()
                .map(CertificateKeyPair::pubkey)
                .collect::<Vec<_>>()
                .as_slice(),
            certkeys.as_slice(),
            &vmap,
        );

        let tm0 = create_timeout_message(&certkeys[0], timeout_epoch, timeout_round, high_qc, true);

        pacemaker
            .pending_timeouts
            .insert(NodeId::new(keys[0].pubkey()), tm0);
        assert!(!pacemaker.pending_timeouts.is_empty());

        let pacemaker_cmds = pacemaker.advance_epoch(&epoch_manager);
        assert_eq!(pacemaker_cmds.len(), 2);
        assert!(matches!(pacemaker_cmds[0], PacemakerCommand::EnterRound(_)));
        assert!(matches!(
            pacemaker_cmds[1],
            PacemakerCommand::Schedule { .. }
        ));

        // advancing epoch clears pending timeout
        assert!(pacemaker.pending_timeouts.is_empty());
    }
}
