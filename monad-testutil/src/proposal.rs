use monad_consensus::types::block::{Block, TransactionList};
use monad_consensus::types::ledger::LedgerCommitInfo;
use monad_consensus::types::message::{ProposalMessage, TimeoutMessage};
use monad_consensus::types::quorum_certificate::{QcInfo, QuorumCertificate};
use monad_consensus::types::signature::SignatureCollection;
use monad_consensus::types::timeout::TimeoutCertificate;
use monad_consensus::types::timeout::{HighQcRound, HighQcRoundSigTuple, TimeoutInfo};
use monad_consensus::types::voting::VoteInfo;
use monad_consensus::validation::hashing::{Hashable, Hasher, Sha256Hash};
use monad_consensus::validation::signing::Verified;
use monad_crypto::secp256k1::KeyPair;
use monad_crypto::Signature;
use monad_types::{NodeId, Round};
use monad_validator::{leader_election::LeaderElection, validator_set::ValidatorSet};

pub struct ProposalGen<S, T> {
    round: Round,
    qc: QuorumCertificate<T>,
    high_qc: QuorumCertificate<T>,
    last_tc: Option<TimeoutCertificate<S>>,
}

impl<S, T> ProposalGen<S, T>
where
    T: SignatureCollection<SignatureType = S>,
    S: Signature,
{
    pub fn new(genesis_qc: QuorumCertificate<T>) -> Self {
        ProposalGen {
            round: Round(0),
            qc: genesis_qc.clone(),
            high_qc: genesis_qc,
            last_tc: None,
        }
    }

    pub fn next_proposal<L: LeaderElection>(
        &mut self,
        keys: &Vec<KeyPair>,
        valset: &mut ValidatorSet<L>,
        payload: &TransactionList,
    ) -> Verified<T::SignatureType, ProposalMessage<T::SignatureType, T>> {
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
            .find(|k| k.pubkey() == valset.get_leader(self.round).0)
            .expect("key not in valset");

        let block = Block::new::<Sha256Hash>(NodeId(leader_key.pubkey()), self.round, payload, qc);

        self.high_qc = self.qc.clone();
        self.qc = self.get_next_qc(keys, &block);

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
    pub fn next_tc<L: LeaderElection>(
        &mut self,
        keys: &Vec<KeyPair>,
        valset: &mut ValidatorSet<L>,
    ) -> Vec<Verified<T::SignatureType, TimeoutMessage<S, T>>> {
        let node_ids = keys
            .iter()
            .map(|keypair| NodeId(keypair.pubkey()))
            .collect::<Vec<_>>();
        if !valset.has_super_majority_votes(node_ids.iter()) {
            return Vec::new();
        }

        let mut tc = TimeoutCertificate::<S> {
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
                author_signature: S::sign(msg_hash.as_ref(), key),
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

    fn get_next_qc(&self, keys: &Vec<KeyPair>, block: &Block<T>) -> QuorumCertificate<T> {
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

        let mut sigs = T::new();
        let msg = Sha256Hash::hash_object(&lci);
        for k in keys {
            let s = T::SignatureType::sign(msg.as_ref(), k);
            sigs.add_signature(s);
        }

        QuorumCertificate::new(qcinfo, sigs)
    }
}
