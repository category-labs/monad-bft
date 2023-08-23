use monad_consensus_types::{
    block::{Block, BlockType},
    certificate_signature::{CertificateKeyPair, CertificateSignature},
    ledger::LedgerCommitInfo,
    payload::{ExecutionArtifacts, Payload, TransactionList},
    quorum_certificate::{QcInfo, QuorumCertificate},
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    validation::{Hasher, Sha256Hash},
    voting::{ValidatorMapping, VoteInfo},
};
use monad_types::{BlockId, Hash, NodeId, Round};

// test utility if you only wish for simple block
#[derive(Clone, PartialEq, Eq)]
pub struct MockBlock {
    pub block_id: BlockId,
    pub parent_block_id: BlockId,
}

impl Default for MockBlock {
    fn default() -> Self {
        MockBlock {
            block_id: monad_types::BlockId(monad_types::Hash([0x00_u8; 32])),
            parent_block_id: monad_types::BlockId(monad_types::Hash([0x01_u8; 32])),
        }
    }
}

impl BlockType for MockBlock {
    fn get_id(&self) -> monad_types::BlockId {
        self.block_id
    }
    fn get_parent_id(&self) -> monad_types::BlockId {
        self.parent_block_id
    }
    fn get_seq_num(&self) -> u64 {
        0
    }
}

impl std::fmt::Debug for MockBlock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MockBlock").finish()
    }
}

pub fn setup_block<SCT: SignatureCollection>(
    author: NodeId,
    block_round: Round,
    qc_round: Round,
    txns: TransactionList,
    execution_header: ExecutionArtifacts,
    certkeys: &[SignatureCollectionKeyPairType<SCT>],
    validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
) -> Block<SCT> {
    let vi = VoteInfo {
        id: BlockId(Hash([42_u8; 32])),
        round: qc_round,
        parent_id: BlockId(Hash([43_u8; 32])),
        parent_round: Round(0),
    };
    let lci = LedgerCommitInfo::new::<Sha256Hash>(None, &vi);

    let qcinfo = QcInfo {
        vote: vi,
        ledger_commit: lci,
    };
    let qcinfo_hash = Sha256Hash::hash_object(&qcinfo.ledger_commit);

    let mut sigs = Vec::new();
    for certkey in certkeys.iter() {
        let sig = <SCT::SignatureType as CertificateSignature>::sign(qcinfo_hash.as_ref(), certkey);

        for (node_id, pubkey) in validator_mapping.map.iter() {
            if *pubkey == certkey.pubkey() {
                sigs.push((*node_id, sig));
            }
        }
    }

    let sig_col = SCT::new(sigs, validator_mapping, qcinfo_hash.as_ref()).unwrap();

    let qc = QuorumCertificate::<SCT>::new::<Sha256Hash>(qcinfo, sig_col);

    Block::<SCT>::new::<Sha256Hash>(
        author,
        block_round,
        &Payload {
            txns,
            header: execution_header,
            seq_num: 0,
        },
        &qc,
    )
}
