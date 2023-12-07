use monad_consensus_types::{
    block::{Block, BlockType},
    certificate_signature::{CertificateKeyPair, CertificateSignature},
    ledger::CommitResult,
    payload::{ExecutionArtifacts, Payload, RandaoReveal, TransactionHashList},
    quorum_certificate::{QcInfo, QuorumCertificate},
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    voting::{ValidatorMapping, Vote, VoteInfo},
};
use monad_crypto::hasher::{Hash, Hasher, HasherType};
use monad_eth_types::EthAddress;
use monad_types::{BlockId, NodeId, Round, SeqNum};

// test utility if you only wish for simple block
#[derive(Clone, PartialEq, Eq)]
pub struct MockBlock {
    pub block_id: BlockId,
    pub parent_block_id: BlockId,
}

impl Default for MockBlock {
    fn default() -> Self {
        MockBlock {
            block_id: BlockId(Hash([0x00_u8; 32])),
            parent_block_id: BlockId(Hash([0x01_u8; 32])),
        }
    }
}

impl BlockType for MockBlock {
    fn get_id(&self) -> BlockId {
        self.block_id
    }
    fn get_round(&self) -> Round {
        Round(1)
    }
    fn get_author(&self) -> NodeId {
        unimplemented!()
    }

    fn get_parent_id(&self) -> BlockId {
        self.parent_block_id
    }
    fn get_parent_round(&self) -> Round {
        Round(0)
    }

    fn get_seq_num(&self) -> SeqNum {
        SeqNum(0)
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
    txns: TransactionHashList,
    execution_header: ExecutionArtifacts,
    certkeys: &[SignatureCollectionKeyPairType<SCT>],
    validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
) -> Block<SCT> {
    let vi = VoteInfo {
        id: BlockId(Hash([42_u8; 32])),
        round: qc_round,
        parent_id: BlockId(Hash([43_u8; 32])),
        parent_round: Round(0),
        seq_num: SeqNum(0),
    };
    let qcinfo = QcInfo {
        vote: Vote {
            vote_info: vi,
            ledger_commit_info: CommitResult::NoCommit,
        },
    };
    let qcinfo_hash = HasherType::hash_object(&qcinfo.vote);

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

    let qc = QuorumCertificate::<SCT>::new(qcinfo, sig_col);

    Block::<SCT>::new(
        author,
        block_round,
        &Payload {
            txns,
            header: execution_header,
            seq_num: SeqNum(1),
            beneficiary: EthAddress::default(),
            randao_reveal: RandaoReveal::new::<SCT::SignatureType>(block_round, &certkeys[0]),
        },
        &qc,
    )
}
