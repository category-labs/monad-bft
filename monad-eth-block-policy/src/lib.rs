use std::collections::BTreeMap;

use monad_consensus_types::{
    block::{Block, BlockPolicy, BlockType},
    quorum_certificate::QuorumCertificate,
    signature_collection::SignatureCollection,
};
use monad_crypto::hasher::{Hashable, Hasher};
use monad_eth_tx::{EthSignedTransaction, EthTxHash};
use monad_eth_types::{EthAddress, Nonce};
use monad_types::{BlockId, Epoch, NodeId, Round, SeqNum};

/// A consensus block that has gone through the EthereumValidator and makes the decoded and
/// verified transactions availabe to access
#[derive(Debug, Clone)]
pub struct EthValidatedBlock<SCT: SignatureCollection> {
    pub block: Block<SCT>,
    pub validated_txns: Vec<EthSignedTransaction>,
    pub nonces: BTreeMap<EthAddress, Nonce>,
}

impl<SCT: SignatureCollection> EthValidatedBlock<SCT> {
    pub fn get_validated_txn_hashes(&self) -> Vec<EthTxHash> {
        self.validated_txns.iter().map(|t| t.hash()).collect()
    }

    pub fn get_nonces(&self) -> &BTreeMap<EthAddress, u64> {
        &self.nonces
    }

    pub fn get_total_gas(&self) -> u64 {
        self.validated_txns
            .iter()
            .fold(0, |acc, tx| acc + tx.gas_limit())
    }
}

impl<SCT: SignatureCollection> PartialEq for EthValidatedBlock<SCT> {
    fn eq(&self, other: &Self) -> bool {
        self.block == other.block
    }
}
impl<SCT: SignatureCollection> Eq for EthValidatedBlock<SCT> {}

impl<SCT: SignatureCollection> Hashable for EthValidatedBlock<SCT> {
    fn hash(&self, state: &mut impl Hasher) {
        self.block.get_id().hash(state);
    }
}

impl<SCT: SignatureCollection> BlockType<SCT> for EthValidatedBlock<SCT> {
    type NodeIdPubKey = SCT::NodeIdPubKey;
    type TxnHash = EthTxHash;

    fn get_id(&self) -> BlockId {
        self.block.get_id()
    }

    fn get_round(&self) -> Round {
        self.block.round
    }

    fn get_epoch(&self) -> Epoch {
        self.block.epoch
    }

    fn get_author(&self) -> NodeId<Self::NodeIdPubKey> {
        self.block.author
    }

    fn get_parent_id(&self) -> BlockId {
        self.block.qc.get_block_id()
    }

    fn get_parent_round(&self) -> Round {
        self.block.qc.get_round()
    }

    fn get_seq_num(&self) -> SeqNum {
        self.block.payload.seq_num
    }

    fn get_txn_hashes(&self) -> Vec<Self::TxnHash> {
        self.get_validated_txn_hashes()
    }

    fn get_qc(&self) -> &QuorumCertificate<SCT> {
        &self.block.qc
    }

    fn get_unvalidated_block(self) -> Block<SCT> {
        self.block
    }

    fn get_unvalidated_block_ref(&self) -> &Block<SCT> {
        &self.block
    }

    fn get_txn_list_len(&self) -> usize {
        self.validated_txns.len()
    }

    fn is_txn_list_empty(&self) -> bool {
        self.validated_txns.is_empty()
    }
}

/// A block policy for ethereum payloads
pub struct EthBlockPolicy {
    // Maps EthAddresses to the its nonces in the last committed block
    // TODO: All nonces exist here for now. Should be moved to a DB
    pub latest_nonces: BTreeMap<EthAddress, u64>,

    /// SeqNum of next block to be committed
    /// Not last_commit because first block is SeqNum(0)
    pub next_commit: SeqNum,
}

impl EthBlockPolicy {
    pub fn get_latest_nonce(
        &self,
        eth_address: &EthAddress,
        nonce_deltas: &BTreeMap<EthAddress, Nonce>,
    ) -> Option<Nonce> {
        if let Some(&blocktree_nonce) = nonce_deltas.get(eth_address) {
            return Some(blocktree_nonce);
        } else if let Some(&committed_nonce) = self.latest_nonces.get(eth_address) {
            return Some(committed_nonce);
        }

        None
    }
}

impl<SCT: SignatureCollection> BlockPolicy<SCT> for EthBlockPolicy {
    type ValidatedBlock = EthValidatedBlock<SCT>;

    fn check_coherency(
        &self,
        block: &Self::ValidatedBlock,
        extending_blocks: Vec<&Self::ValidatedBlock>,
    ) -> bool {
        assert_eq!(
            extending_blocks
                .iter()
                .chain(std::iter::once(&block))
                .next()
                .unwrap()
                .get_seq_num(),
            self.next_commit
        );
        // Get the latest nonce deltas at the parent block (block to extend)
        let mut blocktree_nonce_deltas = BTreeMap::new();
        for extending_block in extending_blocks {
            let block_nonces = extending_block.get_nonces();
            for (&address, &nonce) in block_nonces {
                blocktree_nonce_deltas
                    .entry(address)
                    .and_modify(|curr_nonce| *curr_nonce = nonce)
                    .or_insert(nonce);
            }
        }

        for txn in block.validated_txns.iter() {
            let eth_address = EthAddress(txn.recover_signer().expect("validated txn"));
            let txn_nonce = txn.nonce();

            let expected_nonce = self
                .get_latest_nonce(&eth_address, &blocktree_nonce_deltas)
                .map(|curr_nonce| curr_nonce + 1)
                .unwrap_or(0);
            if txn_nonce != expected_nonce {
                return false;
            }
            blocktree_nonce_deltas.insert(eth_address, txn_nonce);
        }

        true
    }

    fn update_committed_block(&mut self, block: &Self::ValidatedBlock) {
        assert_eq!(block.get_seq_num(), self.next_commit);
        self.next_commit = block.get_seq_num() + SeqNum(1);
        let acc_nonces = block.get_nonces();
        for (&address, &nonce) in acc_nonces {
            self.latest_nonces
                .entry(address)
                .and_modify(|curr_nonce| *curr_nonce = nonce)
                .or_insert(nonce);
        }
    }
}

pub mod utils;
