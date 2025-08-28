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

use std::{collections::BTreeMap, marker::PhantomData, time::Duration};

use monad_chain_config::{revision::ChainRevision, ChainConfig};
use monad_consensus_types::{
    metrics::Metrics,
    timeout::{HighExtend, TimeoutCertificate, TimeoutInfo},
    RoundCertificate,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_types::{Epoch, ExecutionProtocol, NodeId, Round, GENESIS_ROUND};
use monad_validator::{
    epoch_manager::EpochManager,
    signature_collection::{
        SignatureCollection, SignatureCollectionError, SignatureCollectionKeyPairType,
    },
    validator_mapping::ValidatorMapping,
    validator_set::ValidatorSetType,
};
use tracing::debug;

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
    EnterRound(Epoch, Round, RoundCertificate<ST, SCT, EPT>),

    /// create the Timeout which can be signed to create a TimeoutMessage
    /// this should be broadcast to all other nodes
    PrepareTimeout(
        TimeoutInfo,
        HighExtend<ST, SCT, EPT>,
        bool, // safe_to_vote
        Option<RoundCertificate<ST, SCT, EPT>>,
    ),

    /// schedule a local round timeout event after duration
    Schedule { round: Round, duration: Duration },

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
    #[must_use]
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn process_certificate(
        &mut self,
        metrics: &mut Metrics,
        epoch_manager: &EpochManager,
        safety: &mut Safety<ST, SCT, EPT>,
        certificate: RoundCertificate<ST, SCT, EPT>,
    ) -> Vec<PacemakerCommand<ST, SCT, EPT>> {
        if certificate.round() <= self.high_certificate.round() {
            return Default::default();
        }
        debug!(?certificate, "advancing round");
        match &certificate {
            RoundCertificate::Qc(_) => {
                metrics.consensus_events.enter_new_round_qc += 1;
            }
            RoundCertificate::Tc(_) => {
                metrics.consensus_events.enter_new_round_tc += 1;
            }
        };

        safety.process_certificate(&certificate);
        self.high_certificate = certificate;
        self.current_epoch = epoch_manager
            .get_epoch(self.get_current_round())
            .expect("epoch always available for higher round");

        self.phase = PhaseHonest::Zero;
        self.pending_timeouts.clear();

        vec![
            PacemakerCommand::EnterRound(
                self.current_epoch,
                self.get_current_round(),
                self.high_certificate.clone(),
            ),
            PacemakerCommand::Schedule {
                round: self.get_current_round(),
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
        let safe_to_include_vote = safety.timeout(current_round);

        let high_extend = safety
            .maybe_high_tip()
            .map_or(HighExtend::Qc(self.high_certificate.qc().clone()), |tip| {
                HighExtend::Tip(tip.clone())
            });

        let (high_tip_round, high_extend_qc_round) = match &high_extend {
            HighExtend::Tip(tip) => (
                tip.block_header.block_round,
                tip.block_header.qc.get_round(),
            ),
            HighExtend::Qc(qc) => (GENESIS_ROUND, qc.get_round()),
        };

        let timeout = TimeoutInfo {
            epoch: self.get_current_epoch(),
            round: current_round,
            high_tip_round,
            high_qc_round: high_extend_qc_round,
        };

        let last_round_rc = if high_extend_qc_round + Round(1) == current_round {
            None
        } else {
            Some(&self.high_certificate)
        };

        vec![
            PacemakerCommand::ScheduleReset,
            PacemakerCommand::PrepareTimeout(
                timeout,
                high_extend,
                safe_to_include_vote,
                last_round_rc.cloned(),
            ),
            PacemakerCommand::Schedule {
                round: current_round,
                duration: self.get_round_timer(current_round),
            },
        ]
    }

    /// handle the local timeout event
    #[must_use]
    pub fn process_local_timeout(
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

        let tm_info = &timeout_msg.as_ref().tminfo;
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

        if self.phase == PhaseHonest::Zero
            && validators
                .has_honest_vote(&timeouts)
                .expect("has_honest_vote succeeds since addresses are unique")
        {
            // self.local_timeout_round emits PacemakerCommand::ScheduleReset
            ret_commands.extend(self.local_timeout_round(safety));
            self.phase = PhaseHonest::One;
        }

        // try to create a TimeoutCertificate from the pending timeouts, filtering out
        // invalid timeout messages if there are signature errors
        while self.phase == PhaseHonest::One
            && validators
                .has_super_majority_votes(&timeouts)
                .expect("has_super_majority_votes succeeds since addresses are unique")
        {
            match TimeoutCertificate::new(
                tm_info.epoch,
                tm_info.round,
                self.pending_timeouts
                    .iter()
                    .map(|(node_id, tm_msg)| (*node_id, tm_msg.as_ref().clone()))
                    .collect::<Vec<_>>()
                    .as_slice(),
                validator_mapping,
            ) {
                Ok(tc) => {
                    metrics.consensus_events.created_tc += 1;
                    ret_commands.extend(self.process_certificate(
                        metrics,
                        epoch_manager,
                        safety,
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
        for (node_id, timeout_signature) in invalid_timeouts {
            let removed = self.pending_timeouts.remove(&node_id);
            debug_assert_eq!(
                removed.expect("Timeout removed").as_ref().timeout_signature,
                timeout_signature
            );
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
        quorum_certificate::QuorumCertificate,
        timeout::{HighExtendVote, TimeoutInfo},
        voting::Vote,
    };
    use monad_crypto::{
        certificate_signature::{CertificateKeyPair, CertificateSignature},
        hasher::Hash,
        signing_domain, NopSignature,
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
        max_reserve_balance: 1_000_000_000_000_000_000,
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
        };

        let vote_hash = alloy_rlp::encode(vote);

        let mut sigs = Vec::new();
        for (key, certkey) in keys.iter().zip(certkeys.iter()) {
            let node_id = NodeId::new(*key);
            let sig = <SCT::SignatureType as CertificateSignature>::sign::<signing_domain::Vote>(
                vote_hash.as_ref(),
                certkey,
            );
            sigs.push((node_id, sig));
        }

        let sigcol =
            SCT::new::<signing_domain::Vote>(sigs, valmap, vote_hash.as_ref()).expect("success");

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
        let timeout = TimeoutInfo {
            epoch: timeout_epoch,
            round: timeout_round,
            high_tip_round: GENESIS_ROUND,
            high_qc_round: high_qc.get_round(),
        };

        let invalid_msg = b"invalid";

        let mut tmo_msg = TimeoutMessage::<ST, SCT, EPT>::new(
            certkeypair,
            timeout,
            HighExtend::Qc(high_qc),
            true,
            None,
        );
        if !valid {
            if let HighExtendVote::Tip(_, Some(vote_signature)) = &mut tmo_msg.0.high_extend {
                *vote_signature = <SCT::SignatureType as CertificateSignature>::sign::<
                    signing_domain::Vote,
                >(invalid_msg, certkeypair);
            }
            tmo_msg.0.timeout_signature = <SCT::SignatureType as CertificateSignature>::sign::<
                signing_domain::Timeout,
            >(invalid_msg, certkeypair);
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
        assert!(matches!(
            cmds[1],
            PacemakerCommand::PrepareTimeout(_, _, _, _)
        ));
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
                PacemakerCommand::EnterRound(
                    Epoch(1),
                    Round(2),
                    pacemaker.high_certificate.clone()
                ),
                PacemakerCommand::Schedule {
                    round: Round(2),
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
        );
        let mut safety = Safety::default();

        let keys = create_keys::<SignatureType>(4);
        let certkeys = create_certificate_keys::<SignatureCollectionType>(4);

        let mut staking_list = keys
            .iter()
            .map(|k| NodeId::new(k.pubkey()))
            .zip(std::iter::repeat(Stake::ONE))
            .collect::<Vec<_>>();

        // total stake = 7, f = 2
        // node1 holds f+1 stake
        // node1 + node2 has 2f+1 stake
        staking_list[0].1 = Stake::ONE;
        staking_list[1].1 = Stake::from(3);
        staking_list[2].1 = Stake::from(2);
        staking_list[3].1 = Stake::ONE;

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
        let td = TimeoutInfo {
            epoch: Epoch(1),
            round: Round(1),
            high_qc_round: Round(0),
            high_tip_round: GENESIS_ROUND,
        };
        let timeout_hash = alloy_rlp::encode(td);
        let RoundCertificate::Tc(tc) = &pacemaker.high_certificate else {
            panic!("high_certificate should be a TC")
        };
        assert_eq!(tc.tip_rounds.len(), 1);
        let sc = tc.tip_rounds.first().unwrap().sigs.clone();
        assert_eq!(
            sc.verify::<signing_domain::Timeout>(&vmap, timeout_hash.as_ref())
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
                PacemakerCommand::EnterRound(
                    Epoch(1),
                    Round(2),
                    pacemaker.high_certificate.clone()
                ),
                PacemakerCommand::Schedule {
                    round: Round(2),
                    duration: Duration::from_secs(3),
                },
            ]
        );
    }
}
