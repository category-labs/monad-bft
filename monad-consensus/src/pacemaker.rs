use std::{collections::BTreeMap, marker::PhantomData, time::Duration};

use monad_chain_config::{revision::ChainRevision, ChainConfig};
use monad_consensus_types::{
    metrics::Metrics,
    quorum_certificate::QuorumCertificate,
    signature_collection::{
        SignatureCollection, SignatureCollectionError, SignatureCollectionKeyPairType,
    },
    timeout::{Timeout, TimeoutCertificate, TimeoutInfo},
    voting::ValidatorMapping,
    RoundCertificate,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_types::{Epoch, ExecutionProtocol, NodeId, Round};
use monad_validator::{epoch_manager::EpochManager, validator_set::ValidatorSetType};
use tracing::{debug, info};

use crate::{messages::message::TimeoutMessage, validation::safety::Safety};

/// Pacemaker is responsible for tracking and advancing rounds
/// Rounds are advanced when QC/TCs are received.
/// Local round timeouts produce a timeout msg which is broadcast
/// to other nodes
/// Timeout msgs from other nodes are received and collected into
/// a Timeout Certificate which will advance the round
#[derive(PartialEq, Eq, Debug)]
pub struct Pacemaker<ST, SCT, EPT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    /// upper transmission delay bound
    /// this is used to calculate the local round timeout duration
    delta: Duration,

    /// chain config determines vote delay - the time consensus waits between
    /// sending consecutive votes
    chain_config: CCT,

    /// estimate of time consensus takes to process a proposal
    local_processing: Duration,

    current_epoch: Epoch,

    // the certificate that advanced us to our current round
    high_certificate: RoundCertificate<ST, SCT, EPT>,
    high_qc: QuorumCertificate<SCT>,

    /// map of the TimeoutMessages from other Nodes
    /// only needs to be stored for the current round as timeout messages
    /// carry a QC which will be processed and advance the node to the
    /// highest known round
    pending_timeouts: BTreeMap<NodeId<SCT::NodeIdPubKey>, TimeoutMessage<ST, SCT, EPT>>,

    /// States for TimeoutMessage handling, ensures we only broadcast
    /// one message at each phase of handling
    phase: PhaseHonest,

    _phantom: PhantomData<CRT>,
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
}

#[derive(Debug, PartialEq, Eq)]
pub enum PacemakerCommand<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    /// event emitted whenever round changes. this is used by the router
    EnterRound(Epoch, Round),

    /// create the Timeout which can be signed to create a TimeoutMessage
    /// this should be broadcast to all other nodes
    PrepareTimeout(Timeout<ST, SCT, EPT>),

    /// schedule a local round timeout event after duration
    Schedule { duration: Duration },

    /// cancel the current local round timeout
    ScheduleReset,
}

impl<ST, SCT, EPT, CCT, CRT> Pacemaker<ST, SCT, EPT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,

    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    pub fn new(
        delta: Duration,
        chain_config: CCT,
        local_processing: Duration,
        epoch_manager: &EpochManager,
        high_certificate: RoundCertificate<ST, SCT, EPT>,
        high_qc: QuorumCertificate<SCT>,
    ) -> Self {
        let current_round = high_certificate.round() + Round(1);
        Self {
            delta,
            chain_config,
            local_processing,
            current_epoch: epoch_manager
                .get_epoch(current_round)
                .expect("init round must exist in epoch manager"),
            high_certificate,
            high_qc,
            pending_timeouts: BTreeMap::new(),

            phase: PhaseHonest::Zero,
            _phantom: Default::default(),
        }
    }

    pub fn get_current_round(&self) -> Round {
        self.high_certificate.round() + Round(1)
    }

    pub fn get_current_epoch(&self) -> Epoch {
        self.current_epoch
    }

    pub fn last_round_tc(&self) -> Option<&TimeoutCertificate<ST, SCT, EPT>> {
        match &self.high_certificate {
            RoundCertificate::Qc(_) => None,
            RoundCertificate::Tc(tc) => Some(tc),
        }
    }

    pub fn high_qc(&self) -> &QuorumCertificate<SCT> {
        &self.high_qc
    }

    pub fn high_certificate(&self) -> &RoundCertificate<ST, SCT, EPT> {
        &self.high_certificate
    }

    fn get_round_timer(&self, round: Round) -> Duration {
        // worse case time is round timer and vote delay start at effectively the same time and so
        // the round needs to accomdate the full vote-delay time and a local processing time
        // estimate
        let vote_delay = self
            .chain_config
            .get_chain_revision(round)
            .chain_params()
            .vote_pace;

        self.delta * 3 + vote_delay + self.local_processing
    }

    /// enter a new round. Phase is set to PhaseHonest::Zero and all
    /// pending timeout messages are cleared.
    /// Creates the command to start the local round timeout
    pub fn process_certificate(
        &mut self,
        metrics: &mut Metrics,
        epoch_manager: &EpochManager,
        certificate: RoundCertificate<ST, SCT, EPT>,
    ) -> Vec<PacemakerCommand<ST, SCT, EPT>> {
        if let RoundCertificate::Qc(qc) = &certificate {
            if qc.get_round() > self.high_qc.get_round() {
                self.high_qc = qc.clone();
            }
        }

        if certificate.round() <= self.high_certificate.round() {
            return Default::default();
        }
        match &certificate {
            RoundCertificate::Qc(_) => {
                metrics.consensus_events.enter_new_round_qc += 1;
            }
            RoundCertificate::Tc(_) => {
                metrics.consensus_events.enter_new_round_tc += 1;
            }
        };

        self.high_certificate = certificate;
        assert!(self.high_qc.get_round() <= self.high_certificate.round());
        self.current_epoch = epoch_manager
            .get_epoch(self.get_current_round())
            .expect("epoch always available for higher round");

        self.phase = PhaseHonest::Zero;
        self.pending_timeouts.clear();

        vec![
            PacemakerCommand::EnterRound(self.current_epoch, self.get_current_round()),
            PacemakerCommand::Schedule {
                duration: self.get_round_timer(self.get_current_round()),
            },
        ]
    }

    /// invoked on local round timeout
    /// creates the TimeoutMessage to be broadcast and reschedules
    /// the local round timer
    #[must_use]
    fn local_timeout_round(
        &self,
        safety: &mut Safety<ST, SCT, EPT>,
    ) -> Vec<PacemakerCommand<ST, SCT, EPT>> {
        let current_round = self.get_current_round();
        safety.timeout(current_round);
        vec![
            PacemakerCommand::ScheduleReset,
            PacemakerCommand::PrepareTimeout(Timeout {
                tminfo: TimeoutInfo {
                    epoch: self.get_current_epoch(),
                    round: current_round,
                    high_qc: self.high_qc.clone(),
                },
                last_round_tc: self.last_round_tc().cloned(),
            }),
            PacemakerCommand::Schedule {
                duration: self.get_round_timer(current_round),
            },
        ]
    }

    /// handle the local timeout event
    #[must_use]
    pub fn handle_event(
        &mut self,
        safety: &mut Safety<ST, SCT, EPT>,
    ) -> Vec<PacemakerCommand<ST, SCT, EPT>> {
        self.phase = PhaseHonest::One;
        self.local_timeout_round(safety)
    }

    /// handle timeout messages from external nodes
    /// if f+1 timeout messages are received, broadcast a timeout message if it
    /// has not done so before
    /// if 2f+1 timeout messages are received, create the Timeout Certificate
    #[must_use]
    pub fn process_remote_timeout<VST>(
        &mut self,
        metrics: &mut Metrics,
        epoch_manager: &EpochManager,
        validators: &VST,
        validator_mapping: &ValidatorMapping<
            SCT::NodeIdPubKey,
            SignatureCollectionKeyPairType<SCT>,
        >,
        safety: &mut Safety<ST, SCT, EPT>,
        author: NodeId<SCT::NodeIdPubKey>,
        timeout_msg: TimeoutMessage<ST, SCT, EPT>,
    ) -> Vec<PacemakerCommand<ST, SCT, EPT>>
    where
        VST: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
    {
        let mut ret_commands = Vec::new();

        let tm_info = &timeout_msg.timeout.tminfo;
        if tm_info.round < self.get_current_round() {
            return ret_commands;
        }
        // timeout messages carry a high QC which must have been processed
        // prior to entering this function. Processing that QC guarantees
        // this invariant
        assert_eq!(tm_info.round, self.get_current_round());

        // it's fine to overwrite if already exists
        self.pending_timeouts.insert(author, timeout_msg.clone());

        let mut timeouts: Vec<NodeId<_>> = self.pending_timeouts.keys().copied().collect();
        debug!(
            round = ?tm_info.round,
            epoch = ?tm_info.epoch,
            current_stake = ?validators.calculate_current_stake(&timeouts),
            total_stake = ?validators.get_total_stake(),
            "processing remote timeout"
        );

        if self.phase == PhaseHonest::Zero && validators.has_honest_vote(&timeouts) {
            // self.local_timeout_round emits PacemakerCommand::ScheduleReset
            ret_commands.extend(self.local_timeout_round(safety));
            self.phase = PhaseHonest::One;
        }

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
                    info!(?tc, "Created TC");
                    metrics.consensus_events.created_tc += 1;
                    ret_commands.extend(self.process_certificate(
                        metrics,
                        epoch_manager,
                        RoundCertificate::Tc(tc),
                    ));
                    assert_eq!(self.phase, PhaseHonest::Zero);
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

        ret_commands
    }

    /// remove invalid timeout messages and collect evidence
    #[must_use]
    fn handle_invalid_timeout(
        &mut self,
        invalid_timeouts: Vec<(NodeId<SCT::NodeIdPubKey>, SCT::SignatureType)>,
    ) -> Vec<PacemakerCommand<ST, SCT, EPT>> {
        for (node_id, sig) in invalid_timeouts {
            let removed = self.pending_timeouts.remove(&node_id);
            debug_assert_eq!(removed.expect("Timeout removed").sig, sig);
        }
        // TODO-3: evidence collection
        vec![]
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use monad_chain_config::{
        revision::{ChainParams, MockChainRevision},
        MockChainConfig,
    };
    use monad_consensus_types::{
        block::MockExecutionProtocol,
        timeout::{TimeoutDigest, TimeoutInfo},
        voting::Vote,
    };
    use monad_crypto::{
        certificate_signature::{CertificateKeyPair, CertificateSignature},
        hasher::Hash,
        NopSignature,
    };
    use monad_multi_sig::MultiSig;
    use monad_testutil::{
        signing::{create_certificate_keys, create_keys},
        validators::create_keys_w_validators,
    };
    use monad_types::{BlockId, Epoch, SeqNum, Stake};
    use monad_validator::validator_set::{ValidatorSetFactory, ValidatorSetTypeFactory};

    use super::*;

    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<SignatureType>;
    type ExecutionProtocolType = MockExecutionProtocol;
    type ChainConfigType = MockChainConfig;
    type ChainRevisionType = MockChainRevision;

    static CHAIN_PARAMS: ChainParams = ChainParams {
        tx_limit: 10_000,
        proposal_gas_limit: 300_000_000,
        proposal_byte_limit: 4_000_000,
        vote_pace: Duration::from_millis(0),
    };

    fn get_high_qc<SCT: SignatureCollection>(
        qc_epoch: Epoch,
        qc_round: Round,
        keys: &[SCT::NodeIdPubKey],
        certkeys: &[SignatureCollectionKeyPairType<SCT>],
        valmap: &ValidatorMapping<SCT::NodeIdPubKey, SignatureCollectionKeyPairType<SCT>>,
    ) -> QuorumCertificate<SCT> {
        let vote = Vote {
            id: BlockId(Hash([0x00_u8; 32])),
            epoch: qc_epoch,
            round: qc_round,
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
        };

        let vote_hash = alloy_rlp::encode(vote);

        let mut sigs = Vec::new();
        for (key, certkey) in keys.iter().zip(certkeys.iter()) {
            let node_id = NodeId::new(*key);
            let sig =
                <SCT::SignatureType as CertificateSignature>::sign(vote_hash.as_ref(), certkey);
            sigs.push((node_id, sig));
        }

        let sigcol = SCT::new(sigs, valmap, vote_hash.as_ref()).expect("success");

        QuorumCertificate::<SCT>::new(vote, sigcol)
    }

    fn create_timeout_message<ST, SCT, EPT>(
        certkeypair: &SignatureCollectionKeyPairType<SCT>,
        timeout_epoch: Epoch,
        timeout_round: Round,
        high_qc: QuorumCertificate<SCT>,
        valid: bool,
    ) -> TimeoutMessage<ST, SCT, EPT>
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        EPT: ExecutionProtocol,
    {
        let timeout = Timeout {
            tminfo: TimeoutInfo {
                epoch: timeout_epoch,
                round: timeout_round,
                high_qc,
            },
            last_round_tc: None,
        };

        let invalid_msg = b"invalid";

        let mut tmo_msg = TimeoutMessage::<ST, SCT, EPT>::new(timeout, certkeypair);
        if !valid {
            tmo_msg.sig =
                <SCT::SignatureType as CertificateSignature>::sign(invalid_msg, certkeypair);
        }
        tmo_msg
    }

    #[test]
    fn all_honest() {
        let epoch_manager = EpochManager::new(SeqNum(5), Round(5), &[(Epoch(1), Round(0))]);
        let mut metrics = Metrics::default();
        let mut pacemaker = Pacemaker::<
            SignatureType,
            SignatureCollectionType,
            ExecutionProtocolType,
            ChainConfigType,
            ChainRevisionType,
        >::new(
            Duration::from_secs(1),
            MockChainConfig::new(&CHAIN_PARAMS),
            Duration::from_secs(0),
            &epoch_manager,
            RoundCertificate::Qc(QuorumCertificate::genesis_qc()),
            QuorumCertificate::genesis_qc(),
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
        let tm3 = create_timeout_message(&certkeys[3], timeout_epoch, timeout_round, high_qc, true);

        let cmds = pacemaker.process_remote_timeout(
            &mut metrics,
            &epoch_manager,
            &valset,
            &vmap,
            &mut safety,
            NodeId::new(keys[0].pubkey()),
            tm0,
        );
        assert_eq!(pacemaker.get_current_round(), Round(1));
        assert!(cmds.is_empty());

        // enter PhaseHonest::One, timeout itself
        let cmds = pacemaker.process_remote_timeout(
            &mut metrics,
            &epoch_manager,
            &valset,
            &vmap,
            &mut safety,
            NodeId::new(keys[1].pubkey()),
            tm1,
        );
        assert_eq!(pacemaker.get_current_round(), Round(1));
        assert_eq!(pacemaker.phase, PhaseHonest::One);
        assert_eq!(cmds.len(), 3);
        assert!(matches!(cmds[0], PacemakerCommand::ScheduleReset));
        assert!(matches!(cmds[1], PacemakerCommand::PrepareTimeout(_)));
        assert!(matches!(cmds[2], PacemakerCommand::Schedule { .. }));

        // TC is created, round is incremented, and back to PhaseHonest::Zero
        let cmds = pacemaker.process_remote_timeout(
            &mut metrics,
            &epoch_manager,
            &valset,
            &vmap,
            &mut safety,
            NodeId::new(keys[2].pubkey()),
            tm2,
        );
        assert_eq!(metrics.consensus_events.created_tc, 1);
        assert_eq!(metrics.consensus_events.enter_new_round_tc, 1);
        assert_eq!(pacemaker.get_current_round(), Round(2));
        assert_eq!(pacemaker.phase, PhaseHonest::Zero);
        assert_eq!(
            cmds,
            vec![
                PacemakerCommand::EnterRound(Epoch(1), Round(2)),
                PacemakerCommand::Schedule {
                    duration: Duration::from_secs(3),
                },
            ]
        );

        let cmds = pacemaker.process_remote_timeout(
            &mut metrics,
            &epoch_manager,
            &valset,
            &vmap,
            &mut safety,
            NodeId::new(keys[3].pubkey()),
            tm3,
        );
        assert_eq!(pacemaker.get_current_round(), Round(2));
        assert_eq!(cmds, vec![]);
    }

    #[test]
    fn phase_supermajority_invalid_no_progress() {
        let epoch_manager = EpochManager::new(SeqNum(5), Round(5), &[(Epoch(1), Round(0))]);
        let mut metrics = Metrics::default();
        let mut pacemaker = Pacemaker::<
            SignatureType,
            SignatureCollectionType,
            ExecutionProtocolType,
            ChainConfigType,
            ChainRevisionType,
        >::new(
            Duration::from_secs(1),
            MockChainConfig::new(&CHAIN_PARAMS),
            Duration::from_secs(0),
            &epoch_manager,
            RoundCertificate::Qc(QuorumCertificate::genesis_qc()),
            QuorumCertificate::genesis_qc(),
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
        let tm2_invalid =
            create_timeout_message(&certkeys[2], timeout_epoch, timeout_round, high_qc, false);

        let _ = pacemaker.process_remote_timeout(
            &mut metrics,
            &epoch_manager,
            &valset,
            &vmap,
            &mut safety,
            NodeId::new(keys[0].pubkey()),
            tm0_valid,
        );

        let _ = pacemaker.process_remote_timeout(
            &mut metrics,
            &epoch_manager,
            &valset,
            &vmap,
            &mut safety,
            NodeId::new(keys[1].pubkey()),
            tm1_valid,
        );

        let cmds = pacemaker.process_remote_timeout(
            &mut metrics,
            &epoch_manager,
            &valset,
            &vmap,
            &mut safety,
            NodeId::new(keys[2].pubkey()),
            tm2_invalid,
        );
        assert_eq!(metrics.consensus_events.created_tc, 0);
        assert_eq!(metrics.consensus_events.enter_new_round_tc, 0);
        assert_eq!(pacemaker.get_current_round(), Round(1));
        assert_eq!(cmds, vec![]);
        assert_eq!(pacemaker.phase, PhaseHonest::One);
        assert_eq!(pacemaker.pending_timeouts.len(), 2);
    }

    #[test]
    fn phase_supermajority_invalid_progress() {
        let epoch_manager = EpochManager::new(SeqNum(5), Round(5), &[(Epoch(1), Round(0))]);
        let mut metrics = Metrics::default();
        let mut pacemaker = Pacemaker::<
            SignatureType,
            SignatureCollectionType,
            ExecutionProtocolType,
            ChainConfigType,
            ChainRevisionType,
        >::new(
            Duration::from_secs(1),
            MockChainConfig::new(&CHAIN_PARAMS),
            Duration::from_secs(0),
            &epoch_manager,
            RoundCertificate::Qc(QuorumCertificate::genesis_qc()),
            QuorumCertificate::genesis_qc(),
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

        let epoch_manager = EpochManager::new(SeqNum(1000), Round(50), &[(Epoch(1), Round(0))]);
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
        let tm2_valid =
            create_timeout_message(&certkeys[2], timeout_epoch, timeout_round, high_qc, true);

        let _ = pacemaker.process_remote_timeout(
            &mut metrics,
            &epoch_manager,
            &valset,
            &vmap,
            &mut safety,
            NodeId::new(keys[1].pubkey()),
            tm1_valid,
        );

        let _cmds = pacemaker.process_remote_timeout(
            &mut metrics,
            &epoch_manager,
            &valset,
            &vmap,
            &mut safety,
            NodeId::new(keys[0].pubkey()),
            tm0_invalid,
        );
        assert_eq!(pacemaker.get_current_round(), Round(1));
        assert_eq!(pacemaker.pending_timeouts.len(), 2);

        // invalid timeout is removed
        // the remaining two timeouts has 5/7 stake, so TC is created
        let cmds = pacemaker.process_remote_timeout(
            &mut metrics,
            &epoch_manager,
            &valset,
            &vmap,
            &mut safety,
            NodeId::new(keys[2].pubkey()),
            tm2_valid,
        );
        assert_eq!(metrics.consensus_events.created_tc, 1);
        assert_eq!(metrics.consensus_events.enter_new_round_tc, 1);
        assert_eq!(pacemaker.phase, PhaseHonest::Zero);
        assert_eq!(pacemaker.get_current_round(), Round(2));

        // assert the TC is created over the two valid timeouts
        let td = TimeoutDigest {
            epoch: Epoch(1),
            round: Round(1),
            high_qc_round: Round(0),
        };
        let timeout_hash = alloy_rlp::encode(td);
        let tc = pacemaker.last_round_tc().unwrap();
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

        assert_eq!(pacemaker.pending_timeouts.len(), 0);
        assert_eq!(
            cmds,
            vec![
                PacemakerCommand::EnterRound(Epoch(1), Round(2)),
                PacemakerCommand::Schedule {
                    duration: Duration::from_secs(3),
                },
            ]
        );
    }
}
