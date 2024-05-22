use std::marker::PhantomData;

use monad_consensus_types::{
    block::{Block, BlockType},
    ledger::CommitResult,
    payload::{ExecutionArtifacts, FullTransactionList, Payload, RandaoReveal},
    quorum_certificate::{QcInfo, QuorumCertificate},
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    voting::{ValidatorMapping, Vote, VoteInfo},
};
use monad_crypto::{
    certificate_signature::{
        CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey,
        CertificateSignatureRecoverable, PubKey,
    },
    hasher::{Hash, Hasher, HasherType},
};
use monad_eth_types::EthAddress;
use monad_types::{BlockId, Epoch, NodeId, Round, SeqNum};

// test utility if you only wish for simple block
#[derive(Clone, PartialEq, Eq)]
pub struct MockBlock<PT: PubKey> {
    pub block_id: BlockId,
    pub parent_block_id: BlockId,

    _phantom: PhantomData<PT>,
}

impl<PT: PubKey> MockBlock<PT> {
    pub fn new(block_id: BlockId, parent_block_id: BlockId) -> Self {
        Self {
            block_id,
            parent_block_id,

            _phantom: PhantomData,
        }
    }
}

impl<PT: PubKey> Default for MockBlock<PT> {
    fn default() -> Self {
        MockBlock {
            block_id: BlockId(Hash([0x00_u8; 32])),
            parent_block_id: BlockId(Hash([0x01_u8; 32])),

            _phantom: PhantomData,
        }
    }
}

impl<SCT: SignatureCollection, PT: PubKey> BlockType<SCT> for MockBlock<PT> {
    type NodeIdPubKey = PT;
    type TxnHash = ();

    fn get_id(&self) -> BlockId {
        self.block_id
    }

    fn get_round(&self) -> Round {
        Round(1)
    }

    fn get_epoch(&self) -> Epoch {
        Epoch(1)
    }

    fn get_author(&self) -> NodeId<Self::NodeIdPubKey> {
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

    fn get_txn_hashes(&self) -> Vec<Self::TxnHash> {
        vec![]
    }

    fn is_txn_list_empty(&self) -> bool {
        true
    }

    fn get_txn_list_len(&self) -> usize {
        0
    }

    fn get_unvalidated_block_ref(&self) -> &Block<SCT> {
        unimplemented!()
    }

    fn get_unvalidated_block(self) -> Block<SCT> {
        unimplemented!()
    }

    fn get_qc(&self) -> &QuorumCertificate<SCT> {
        unimplemented!()
    }
}

impl<PT: PubKey> std::fmt::Debug for MockBlock<PT> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MockBlock").finish()
    }
}

pub fn setup_block<ST, SCT>(
    author: NodeId<CertificateSignaturePubKey<ST>>,
    block_round: Round,
    qc_round: Round,
    txns: FullTransactionList,
    execution_header: ExecutionArtifacts,
    certkeys: &[SignatureCollectionKeyPairType<SCT>],
    validator_mapping: &ValidatorMapping<
        CertificateSignaturePubKey<ST>,
        SignatureCollectionKeyPairType<SCT>,
    >,
) -> Block<SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    let vi = VoteInfo {
        id: BlockId(Hash([42_u8; 32])),
        epoch: Epoch(1),
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
        Epoch(1),
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
