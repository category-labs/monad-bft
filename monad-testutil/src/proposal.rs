use std::marker::PhantomData;

use monad_consensus::{
    messages::message::{ProposalMessage, TimeoutMessage},
    validation::signing::Verified,
};
use monad_consensus_types::{
    block::{Block, BlockType},
    certificate_signature::{CertificateKeyPair, CertificateSignature},
    ledger::LedgerCommitInfo,
    message_signature::MessageSignature,
    payload::{ExecutionArtifacts, Payload, RandaoReveal, TransactionHashList},
    quorum_certificate::{QcInfo, QuorumCertificate},
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    timeout::{HighQcRound, HighQcRoundSigColTuple, Timeout, TimeoutCertificate, TimeoutInfo},
    voting::{ValidatorMapping, VoteInfo},
};
use monad_crypto::{
    hasher::{Hasher, HasherType},
    secp256k1::KeyPair,
};
use monad_eth_types::EthAddress;
use monad_types::{NodeId, Round, SeqNum};
use monad_validator::{leader_election::LeaderElection, validator_set::ValidatorSetType};

pub struct ProposalGen<ST, SCT> {
    round: Round,
    qc: QuorumCertificate<SCT>,
    high_qc: QuorumCertificate<SCT>,
    last_tc: Option<TimeoutCertificate<SCT>>,
    phantom: PhantomData<ST>,
}

impl<ST, SCT> ProposalGen<ST, SCT>
where
    ST: MessageSignature,
    SCT: SignatureCollection,
{
    pub fn new(genesis_qc: QuorumCertificate<SCT>) -> Self {
        ProposalGen {
            round: Round(0),
            qc: genesis_qc.clone(),
            high_qc: genesis_qc,
            last_tc: None,
            phantom: PhantomData,
        }
    }

    pub fn next_proposal<VT: ValidatorSetType, LT: LeaderElection>(
        &mut self,
        keys: &[KeyPair],
        certkeys: &[SignatureCollectionKeyPairType<SCT>],
        valset: &VT,
        election: &LT,
        validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
        txns: TransactionHashList,
        execution_header: ExecutionArtifacts,
    ) -> Verified<ST, ProposalMessage<SCT>> {
        // high_qc is the highest qc seen in a proposal
        let qc = if self.last_tc.is_some() {
            &self.high_qc
        } else {
            // entering new round from qc
            self.round += Round(1);
            &self.qc
        };

        let (leader_key, leader_certkey) = keys
            .iter()
            .zip(certkeys)
            .find(|(k, _)| k.pubkey() == election.get_leader(self.round, valset.get_list()).0)
            .expect("key not in valset");

        let block = Block::new::<HasherType>(
            NodeId(leader_key.pubkey()),
            self.round,
            &Payload {
                txns,
                header: execution_header,
                seq_num: qc.info.vote.seq_num + SeqNum(1),
                beneficiary: EthAddress::default(),
                randao_reveal: RandaoReveal::new::<SCT::SignatureType>(self.round, leader_certkey),
            },
            qc,
        );

        self.high_qc = self.qc.clone();
        self.qc = self.get_next_qc(certkeys, &block, validator_mapping);

        let proposal = ProposalMessage {
            block,
            last_round_tc: self.last_tc.clone(),
        };
        self.last_tc = None;

        Verified::new::<HasherType>(proposal, leader_key)
    }

    // next_tc uses the keys to generate a timeout certificate
    // to ensure that the consensus state is consistent with the ProposalGen state
    // call state.pacemaker.handle_event(&mut state.safety, &state.high_qc);
    // before adding the state's key to keys
    pub fn next_tc<VT: ValidatorSetType>(
        &mut self,
        keys: &[KeyPair],
        certkeys: &[SignatureCollectionKeyPairType<SCT>],
        valset: &VT,
        validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
    ) -> Vec<Verified<ST, TimeoutMessage<SCT>>> {
        let node_ids = keys
            .iter()
            .map(|keypair| NodeId(keypair.pubkey()))
            .collect::<Vec<_>>();
        if !valset.has_super_majority_votes(node_ids.iter()) {
            return Vec::new();
        }

        let high_qc_round = HighQcRound {
            qc_round: self.high_qc.info.vote.round,
        };

        let tminfo = TimeoutInfo {
            round: self.round,
            high_qc: self.high_qc.clone(),
        };

        let tmo_digest = tminfo.timeout_digest::<HasherType>();
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
            round: self.round,
            high_qc_rounds: vec![high_qc_sig_tuple],
        };

        let timeout = Timeout {
            tminfo,
            last_round_tc: self.last_tc.clone(),
        };

        let mut tmo_msgs = Vec::new();
        for (key, certkey) in keys.iter().zip(certkeys.iter()) {
            let tmo_msg = TimeoutMessage::new::<HasherType>(timeout.clone(), certkey);
            tmo_msgs.push(Verified::<ST, _>::new::<HasherType>(tmo_msg, key));
        }

        // entering new round through tc
        self.round += Round(1);
        self.last_tc = Some(tc);
        tmo_msgs
    }

    fn get_next_qc(
        &self,
        certkeys: &[SignatureCollectionKeyPairType<SCT>],
        block: &Block<SCT>,
        validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
    ) -> QuorumCertificate<SCT> {
        let vi = VoteInfo {
            id: block.get_id(),
            round: block.round,
            parent_id: block.qc.info.vote.id,
            parent_round: block.qc.info.vote.round,
            seq_num: block.payload.seq_num,
        };
        let commit = Some(block.get_id().0); // FIXME-1: is this hash correct?
        let lci = LedgerCommitInfo::new::<HasherType>(commit, &vi);
        let qcinfo = QcInfo {
            vote: vi,
            ledger_commit: lci,
        };

        let msg = HasherType::hash_object(&lci);

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

        QuorumCertificate::new::<HasherType>(qcinfo, sigcol)
    }
}
