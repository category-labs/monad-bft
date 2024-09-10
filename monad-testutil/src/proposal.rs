use std::marker::PhantomData;

use monad_consensus::{
    messages::message::{ProposalMessage, TimeoutMessage},
    validation::signing::Verified,
};
use monad_consensus_types::{
    block::{Block, BlockKind, BlockType},
    ledger::CommitResult,
    payload::{ExecutionProtocol, Payload, RandaoReveal, TransactionPayload},
    quorum_certificate::{QcInfo, QuorumCertificate},
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    state_root_hash::StateRootHash,
    timeout::{HighQcRound, HighQcRoundSigColTuple, Timeout, TimeoutCertificate, TimeoutInfo},
    voting::{ValidatorMapping, Vote, VoteInfo},
};
use monad_crypto::{
    certificate_signature::{
        CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey,
        CertificateSignatureRecoverable,
    },
    hasher::{Hasher, HasherType},
};
use monad_eth_types::EthAddress;
use monad_types::{Epoch, NodeId, Round, SeqNum};
use monad_validator::{
    epoch_manager::EpochManager,
    leader_election::LeaderElection,
    validator_set::{ValidatorSetType, ValidatorSetTypeFactory},
    validators_epoch_mapping::ValidatorsEpochMapping,
};

#[derive(Clone)]
pub struct ProposalGen<ST, SCT> {
    epoch: Epoch,
    round: Round,
    qc: QuorumCertificate<SCT>,
    high_qc: QuorumCertificate<SCT>,
    last_tc: Option<TimeoutCertificate<SCT>>,
    timestamp: u64,
    phantom: PhantomData<ST>,
}

impl<ST, SCT> Default for ProposalGen<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<ST, SCT> ProposalGen<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    pub fn new() -> Self {
        let genesis_qc = QuorumCertificate::genesis_qc();
        ProposalGen {
            epoch: Epoch(1),
            round: Round(0),
            qc: genesis_qc.clone(),
            high_qc: genesis_qc,
            last_tc: None,
            timestamp: 0,
            phantom: PhantomData,
        }
    }

    pub fn next_proposal<
        VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    >(
        &mut self,
        keys: &[ST::KeyPairType],
        certkeys: &[SignatureCollectionKeyPairType<SCT>],
        epoch_manager: &EpochManager,
        val_epoch_map: &ValidatorsEpochMapping<VTF, SCT>,
        election: &LT,
        txns: TransactionPayload,
        srh: StateRootHash,
    ) -> Verified<ST, ProposalMessage<SCT>> {
        // high_qc is the highest qc seen in a proposal
        let qc = if self.last_tc.is_some() {
            &self.high_qc
        } else {
            // entering new round from qc
            self.round += Round(1);
            self.epoch = epoch_manager.get_epoch(self.round).expect("epoch exists");
            &self.qc
        };
        self.timestamp += 1;

        let (leader_key, leader_certkey) = keys
            .iter()
            .zip(certkeys)
            .find(|(k, _)| {
                let epoch = epoch_manager.get_epoch(self.round).expect("epoch exists");
                k.pubkey()
                    == election
                        .get_leader(
                            self.round,
                            val_epoch_map.get_val_set(&epoch).unwrap().get_members(),
                        )
                        .pubkey()
            })
            .expect("key not in valset");

        let seq_num = match txns {
            TransactionPayload::List(_) => qc.get_seq_num() + SeqNum(1),
            TransactionPayload::Null => qc.get_seq_num(),
            TransactionPayload::CompressedList { .. } => todo!(),
        };
        let block_kind = match txns {
            TransactionPayload::List(_) => BlockKind::Executable,
            TransactionPayload::Null => BlockKind::Null,
            TransactionPayload::CompressedList { .. } => todo!(),
        };
        let payload = Payload { txns };
        let block = Block::new(
            NodeId::new(leader_key.pubkey()),
            self.timestamp,
            self.epoch,
            self.round,
            &ExecutionProtocol {
                state_root: srh,
                seq_num,
                beneficiary: EthAddress::default(),
                randao_reveal: RandaoReveal::new::<SCT::SignatureType>(self.round, leader_certkey),
            },
            payload.get_id(),
            block_kind,
            qc,
        );

        let validator_cert_pubkeys = val_epoch_map
            .get_cert_pubkeys(&epoch_manager.get_epoch(self.round).expect("epoch exists"))
            .expect("should have the current validator certificate pubkeys");
        self.high_qc = self.qc.clone();
        self.qc = self.get_next_qc(certkeys, &block, validator_cert_pubkeys);

        let proposal = ProposalMessage {
            block,
            payload,
            last_round_tc: self.last_tc.clone(),
        };
        self.last_tc = None;

        Verified::new(proposal, leader_key)
    }

    // next_tc uses the keys to generate a timeout certificate
    // to ensure that the consensus state is consistent with the ProposalGen state
    // call state.pacemaker.handle_event(&mut state.safety, &state.high_qc);
    // before adding the state's key to keys
    pub fn next_tc<VT: ValidatorSetType<NodeIdPubKey = CertificateSignaturePubKey<ST>>>(
        &mut self,
        keys: &[ST::KeyPairType],
        certkeys: &[SignatureCollectionKeyPairType<SCT>],
        valset: &VT,
        epoch_manager: &EpochManager,
        validator_mapping: &ValidatorMapping<
            CertificateSignaturePubKey<ST>,
            SignatureCollectionKeyPairType<SCT>,
        >,
    ) -> Vec<Verified<ST, TimeoutMessage<SCT>>> {
        let node_ids = keys
            .iter()
            .map(|keypair| NodeId::new(keypair.pubkey()))
            .collect::<Vec<_>>();
        if !valset.has_super_majority_votes(&node_ids) {
            return Vec::new();
        }

        let high_qc_round = HighQcRound {
            qc_round: self.high_qc.get_round(),
        };

        let tminfo = TimeoutInfo {
            epoch: self.epoch,
            round: self.round,
            high_qc: self.high_qc.clone(),
        };

        let tmo_digest = tminfo.timeout_digest();
        // aggregate all tmo signatures into one collection because all nodes share a global state
        // in reality we don't have this configuration because timeout messages
        // can't all contain TC carrying signatures from all validators. It's fine
        // for tests
        let mut tc_sigs = Vec::new();
        for (node_id, certkey) in node_ids.iter().zip(certkeys.iter()) {
            let sig =
                <SCT::SignatureType as CertificateSignature>::sign(tmo_digest.as_ref(), certkey);
            tc_sigs.push((*node_id, sig));
        }
        let tmo_sig_col = SCT::new(tc_sigs, validator_mapping, tmo_digest.as_ref()).unwrap();
        let high_qc_sig_tuple = HighQcRoundSigColTuple {
            high_qc_round,
            sigs: tmo_sig_col,
        };
        let tc = TimeoutCertificate::<SCT> {
            epoch: self.epoch,
            round: self.round,
            high_qc_rounds: vec![high_qc_sig_tuple],
        };

        let timeout = Timeout {
            tminfo,
            last_round_tc: self.last_tc.clone(),
        };

        let mut tmo_msgs = Vec::new();
        for (key, certkey) in keys.iter().zip(certkeys.iter()) {
            let tmo_msg = TimeoutMessage::new(timeout.clone(), certkey);
            tmo_msgs.push(Verified::<ST, _>::new(tmo_msg, key));
        }

        // entering new round through tc
        self.round += Round(1);
        self.epoch = epoch_manager.get_epoch(self.round).expect("epoch exists");
        self.last_tc = Some(tc);
        tmo_msgs
    }

    fn get_next_qc(
        &self,
        certkeys: &[SignatureCollectionKeyPairType<SCT>],
        block: &Block<SCT>,
        validator_mapping: &ValidatorMapping<
            CertificateSignaturePubKey<ST>,
            SignatureCollectionKeyPairType<SCT>,
        >,
    ) -> QuorumCertificate<SCT> {
        let vi = VoteInfo {
            id: block.get_id(),
            epoch: block.epoch,
            round: block.round,
            parent_id: block.qc.get_block_id(),
            parent_round: block.qc.get_round(),
            seq_num: block.execution.seq_num,
            timestamp: block.timestamp,
        };
        let qcinfo = QcInfo {
            vote: Vote {
                vote_info: vi,
                ledger_commit_info: CommitResult::Commit,
            },
        };

        let msg = HasherType::hash_object(&qcinfo.vote);

        let mut sigs = Vec::new();
        for ck in certkeys {
            let sig = <SCT::SignatureType as CertificateSignature>::sign(msg.as_ref(), ck);

            for (node_id, pubkey) in validator_mapping.map.iter() {
                if *pubkey == ck.pubkey() {
                    sigs.push((*node_id, sig));
                }
            }
        }

        let sigcol = SCT::new(sigs, validator_mapping, msg.as_ref()).unwrap();

        QuorumCertificate::new(qcinfo, sigcol)
    }
}
