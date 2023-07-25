use monad_consensus::{
    messages::message::{ProposalMessage, TimeoutMessage},
    validation::signing::Verified,
};
use monad_consensus_types::{
    block::{Block, TransactionList},
    ledger::LedgerCommitInfo,
    quorum_certificate::{QcInfo, QuorumCertificate},
    signature::{SignatureBuilder, SignatureCollection},
    timeout::{HighQcRound, HighQcRoundSigTuple, TimeoutCertificate, TimeoutInfo},
    validation::{Hashable, Hasher, Sha256Hash},
    voting::VoteInfo,
};
use monad_crypto::{secp256k1::KeyPair, Signature};
use monad_types::{NodeId, Round};
use monad_validator::{
    leader_election::LeaderElection, validator_property::ValidatorSetPropertyType,
    validator_set::ValidatorSetType,
};

pub struct ProposalGen<ST, SCT> {
    round: Round,
    qc: QuorumCertificate<SCT>,
    high_qc: QuorumCertificate<SCT>,
    last_tc: Option<TimeoutCertificate<ST>>,
}

impl<ST, SCT> ProposalGen<ST, SCT>
where
    ST: Signature,
    SCT: SignatureCollection,
{
    pub fn new(genesis_qc: QuorumCertificate<SCT>) -> Self {
        ProposalGen {
            round: Round(0),
            qc: genesis_qc.clone(),
            high_qc: genesis_qc,
            last_tc: None,
        }
    }

    pub fn next_proposal<
        VT: ValidatorSetType,
        VPT: ValidatorSetPropertyType,
        LT: LeaderElection,
    >(
        &mut self,
        keys: &Vec<KeyPair>,
        valset: &VT,
        election: &LT,
        valset_prop: &VPT,
        payload: &TransactionList,
    ) -> Verified<ST, ProposalMessage<ST, SCT>> {
        // high_qc is the highest qc seen in a proposal
        let qc = if self.last_tc.is_some() {
            &self.high_qc
        } else {
            // entering new round from qc
            self.round += Round(1);
            &self.qc
        };

        let leader_key = keys
            .iter()
            .find(|k| k.pubkey() == election.get_leader(self.round, valset.get_list()).0)
            .expect("key not in valset");

        let block = Block::new::<Sha256Hash>(NodeId(leader_key.pubkey()), self.round, payload, qc);

        self.high_qc = self.qc.clone();
        self.qc = self.get_next_qc(keys, &block, valset_prop);

        let proposal = ProposalMessage {
            block,
            last_round_tc: self.last_tc.clone(),
        };
        self.last_tc = None;

        Verified::new::<Sha256Hash>(proposal, leader_key)
    }

    // next_tc uses the keys to generate a timeout certificate
    // to ensure that the consensus state is consistent with the ProposalGen state
    // call state.pacemaker.handle_event(&mut state.safety, &state.high_qc, PacemakerTimerExpire);
    // before adding the state's key to keys
    pub fn next_tc<VT: ValidatorSetType>(
        &mut self,
        keys: &Vec<KeyPair>,
        valset: &VT,
    ) -> Vec<Verified<SCT::SignatureType, TimeoutMessage<ST, SCT>>> {
        let node_ids = keys
            .iter()
            .map(|keypair| NodeId(keypair.pubkey()))
            .collect::<Vec<_>>();
        if !valset.has_super_majority_votes(node_ids.iter()) {
            return Vec::new();
        }

        let mut tc = TimeoutCertificate::<ST> {
            round: self.round,
            high_qc_rounds: Vec::new(),
        };

        let high_qc_round = HighQcRound {
            qc_round: self.high_qc.info.vote.round,
        };
        let mut h = Sha256Hash::new();
        h.update(tc.round);
        high_qc_round.hash(&mut h);
        let msg_hash = h.hash();

        let mut tmo_msgs = Vec::new();
        for key in keys {
            let high_qc_sig_tuple = HighQcRoundSigTuple {
                high_qc_round,
                author_signature: ST::sign(msg_hash.as_ref(), key),
            };
            tc.high_qc_rounds.push(high_qc_sig_tuple);
            let tmo_msg = TimeoutMessage {
                tminfo: TimeoutInfo {
                    round: self.round,
                    high_qc: self.high_qc.clone(),
                },
                last_round_tc: self.last_tc.clone(),
            };
            tmo_msgs.push(Verified::new::<Sha256Hash>(tmo_msg, key));
        }

        // entering new round through tc
        self.round += Round(1);
        self.last_tc = Some(tc.clone());
        tmo_msgs
    }

    fn get_next_qc<VPT: ValidatorSetPropertyType>(
        &self,
        keys: &Vec<KeyPair>,
        block: &Block<SCT>,
        valset_prop: &VPT,
    ) -> QuorumCertificate<SCT> {
        let vi = VoteInfo {
            id: block.get_id(),
            round: block.round,
            parent_id: block.qc.info.vote.id,
            parent_round: block.qc.info.vote.round,
        };
        let commit = Some(block.get_id().0);
        let lci = LedgerCommitInfo::new::<Sha256Hash>(commit, &vi);
        let qcinfo = QcInfo {
            vote: vi,
            ledger_commit: lci,
        };

        let msg = Sha256Hash::hash_object(&lci);

        let mut builder = SignatureBuilder::new();
        for k in keys {
            let idx = valset_prop.get_index(&NodeId(k.pubkey())).unwrap();
            let s = SCT::SignatureType::sign(msg.as_ref(), k);
            builder.add_signature(idx, s);
        }

        let sigs = SCT::new(builder, valset_prop.get_voting_list(), msg.as_ref()).unwrap();

        QuorumCertificate::new(qcinfo, sigs)
    }
}
