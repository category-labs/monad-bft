use monad_consensus::types::block::Block;
use monad_consensus::types::ledger::LedgerCommitInfo;
use monad_consensus::types::message::ProposalMessage;
use monad_consensus::types::quorum_certificate::{QcInfo, QuorumCertificate};
use monad_consensus::types::signature::SignatureCollection;
use monad_consensus::types::timeout::TimeoutCertificate;
use monad_consensus::types::voting::VoteInfo;
use monad_consensus::validation::hashing::{Hasher, Sha256Hash};
use monad_consensus::validation::signing::Verified;
use monad_crypto::secp256k1::KeyPair;
use monad_crypto::Signature;
use monad_types::{NodeId, Round};
use monad_validator::{leader_election::LeaderElection, validator_set::ValidatorSet};

pub struct ProposalGen<T> {
    round: Round,
    qc: QuorumCertificate<T>,
}

impl<T: SignatureCollection> ProposalGen<T> {
    pub fn new(genesis_qc: QuorumCertificate<T>) -> Self {
        ProposalGen {
            round: Round(1),
            qc: genesis_qc,
        }
    }

    pub fn next_proposal<L: LeaderElection>(
        &mut self,
        keys: &Vec<KeyPair>,
        valset: &mut ValidatorSet<L>,
    ) -> Verified<T::SignatureType, ProposalMessage<T::SignatureType, T>> {
        let leader_key = keys
            .iter()
            .find(|k| k.pubkey() == valset.get_leader(self.round).0)
            .expect("key not in valset");

        let block = Block::new::<Sha256Hash>(
            NodeId(leader_key.pubkey()),
            self.round,
            &Default::default(),
            &self.qc,
        );

        self.round += Round(1);
        self.qc = self.get_next_qc(keys, &block);

        let proposal = ProposalMessage {
            block,
            last_round_tc: None::<TimeoutCertificate<T::SignatureType>>,
        };

        Verified::new::<Sha256Hash>(proposal, leader_key)
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
            let s = T::SignatureType::sign(&msg, k);
            sigs.add_signature(s);
        }

        QuorumCertificate::new(qcinfo, sigs)
    }
}
