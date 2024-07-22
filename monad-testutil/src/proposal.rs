use std::marker::PhantomData;

use monad_consensus::{
    messages::message::{ProposalMessage, TimeoutMessage},
    validation::signing::Verified,
};
use monad_consensus_types::{
    block::{Block, BlockType},
    ledger::CommitResult,
    payload::{ExecutionArtifacts, FullTransactionList, Payload, RandaoReveal},
    quorum_certificate::{QcInfo, QuorumCertificate},
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    timeout::{HighQcRound, HighQcRoundSigColTuple, Timeout, TimeoutCertificate, TimeoutInfo},
    voting::{ValidatorMapping, Vote, VoteInfo},
};
use monad_crypto::{
    certificate_signature::{
        CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey,
        CertificateSignatureRecoverable,
    },
    hasher::{Hash, Hasher, HasherType},
    NopKeyPair, NopPubKey, NopSignature,
};
use monad_eth_types::EthAddress;
use monad_types::{BlockId, Epoch, NodeId, Round, SeqNum, Stake};
use monad_validator::{
    epoch_manager::EpochManager,
    leader_election::LeaderElection,
    validator_set::{ValidatorSetFactory, ValidatorSetType, ValidatorSetTypeFactory},
    validators_epoch_mapping::ValidatorsEpochMapping,
};

use crate::{
    block::set_block_and_qc, signing::MockSignatures, validators::create_keys_w_validators,
};

#[derive(Clone)]
pub struct ProposalGen<ST, SCT> {
    epoch: Epoch,
    round: Round,
    qc: QuorumCertificate<SCT>,
    high_qc: QuorumCertificate<SCT>,
    last_tc: Option<TimeoutCertificate<SCT>>,
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
        txns: FullTransactionList,
        execution_header: ExecutionArtifacts,
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

        let block = Block::new(
            NodeId::new(leader_key.pubkey()),
            self.epoch,
            self.round,
            &Payload {
                txns,
                header: execution_header,
                seq_num: qc.get_seq_num() + SeqNum(1),
                beneficiary: EthAddress::default(),
                randao_reveal: RandaoReveal::new::<SCT::SignatureType>(self.round, leader_certkey),
            },
            qc,
        );

        let validator_cert_pubkeys = val_epoch_map
            .get_cert_pubkeys(&epoch_manager.get_epoch(self.round).expect("epoch exists"))
            .expect("should have the current validator certificate pubkeys");
        self.high_qc = self.qc.clone();
        self.qc = self.get_next_qc(certkeys, &block, validator_cert_pubkeys);

        let proposal = ProposalMessage {
            block,
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
            seq_num: block.payload.seq_num,
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
type SignatureType = NopSignature;
type SignatureCollectionType = MockSignatures<SignatureType>;

pub fn define_proposal_with_tc(
    known_epoch: Epoch,
    known_round: Round,
    val_epoch: Epoch,
    block_epoch: Epoch,
    block_round: Round,
    block_seq_num: SeqNum,
    qc_epoch: Epoch,
    qc_round: Round,
    qc_parent_round: Round,
    qc_seq_num: SeqNum,
    tc_epoch: Epoch,
    tc_round: Round,
    tc_epoch_signed: Epoch,
    tc_round_signed: Round,
    num_node: u32,
    val_set_update_interval: SeqNum,
    epoch_start_delay: Round,
) -> (
    Vec<NopKeyPair>,
    Vec<NopKeyPair>,
    EpochManager,
    ValidatorsEpochMapping<ValidatorSetFactory<NopPubKey>, SignatureCollectionType>,
    ProposalMessage<SignatureCollectionType>,
) {
    let (keys, cert_keys, _, _) = create_keys_w_validators::<
        SignatureType,
        SignatureCollectionType,
        _,
    >(num_node, ValidatorSetFactory::default());

    let mut vlist = Vec::new();
    let mut vmap_vec = Vec::new();

    for keypair in &keys {
        let node_id = NodeId::new(keypair.pubkey());

        vlist.push((node_id, Stake(1)));
        vmap_vec.push((node_id, keypair.pubkey()));
    }

    let _vset = ValidatorSetFactory::default().create(vlist).unwrap();

    // create valid QC
    let vi = VoteInfo {
        id: BlockId(Hash([0x09_u8; 32])),
        epoch: qc_epoch,
        round: qc_round,
        parent_id: BlockId(Hash([0x00_u8; 32])),
        parent_round: qc_parent_round,
        seq_num: qc_seq_num,
    };

    let qc = QuorumCertificate::<MockSignatures<SignatureType>>::new(
        QcInfo {
            vote: Vote {
                vote_info: vi,
                ledger_commit_info: CommitResult::Commit,
            },
        },
        MockSignatures::with_pubkeys(
            keys.iter()
                .map(|kp| kp.pubkey())
                .collect::<Vec<_>>()
                .as_slice(),
        ),
    );

    // Not actually signed
    let _tminfo = TimeoutInfo {
        epoch: tc_epoch_signed,
        round: tc_round_signed,
        high_qc: qc.clone(),
    };

    let high_qc_sig_tuple = HighQcRoundSigColTuple {
        high_qc_round: HighQcRound {
            qc_round: qc.get_round(),
        },
        sigs: MockSignatures::with_pubkeys(
            keys.iter()
                .map(|kp| kp.pubkey())
                .collect::<Vec<_>>()
                .as_slice(),
        ),
    };

    let tc = TimeoutCertificate {
        epoch: tc_epoch, // wrong epoch here
        round: tc_round,
        high_qc_rounds: vec![high_qc_sig_tuple],
    };

    // moved here because of valmap ownership
    let epoch_manager = EpochManager::new(
        val_set_update_interval,
        epoch_start_delay,
        &[(known_epoch, known_round)],
    );
    let mut val_epoch_map: ValidatorsEpochMapping<ValidatorSetFactory<_>, SignatureCollectionType> =
        ValidatorsEpochMapping::new(ValidatorSetFactory::default());

    val_epoch_map.insert(
        val_epoch,
        _vset.get_members().iter().map(|(a, b)| (*a, *b)).collect(),
        ValidatorMapping::new(vmap_vec),
    );

    let author = NodeId::new(keys[0].pubkey());
    let block = set_block_and_qc(
        author,
        block_epoch,
        block_round,
        block_seq_num,
        qc_epoch,
        qc_round,
        qc_parent_round,
        qc_seq_num,
        &[keys[0].pubkey(), keys[1].pubkey(), keys[2].pubkey()],
    );

    let proposal = ProposalMessage {
        block,
        last_round_tc: Some(tc),
    };

    (keys, cert_keys, epoch_manager, val_epoch_map, proposal)
}
