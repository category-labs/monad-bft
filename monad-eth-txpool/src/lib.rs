use std::collections::{BTreeMap, HashSet};

use alloy_rlp::Decodable;
use bytes::Bytes;
use monad_consensus_types::{
    block::{Block, BlockPolicy, BlockType},
    payload::FullTransactionList,
    quorum_certificate::QuorumCertificate,
    signature_collection::SignatureCollection,
    txpool::TxPool,
};
use monad_crypto::hasher::{Hashable, Hasher};
use monad_eth_tx::{EthFullTransactionList, EthSignedTransaction, EthTransaction, EthTxHash};
use monad_types::{BlockId, Epoch, NodeId, Round, SeqNum};

#[derive(Default)]
pub struct EthTxPool(BTreeMap<EthTxHash, EthTransaction>);

impl<SCT: SignatureCollection> TxPool<SCT, EthBlockPolicy> for EthTxPool {
    fn insert_tx(&mut self, tx: Bytes) {
        // TODO: unwrap can be removed when this is made generic over the actual
        // tx type rather than Bytes and decoding won't be necessary
        let eth_tx = EthTransaction::decode(&mut tx.as_ref()).unwrap();
        // TODO: sorting by gas_limit for proposal creation
        self.0.insert(eth_tx.hash(), eth_tx);
    }

    fn create_proposal(
        &mut self,
        tx_limit: usize,
        gas_limit: u64,
        pending_blocktree_txs: HashSet<
            <<EthBlockPolicy as BlockPolicy<SCT>>::ValidatedBlock as BlockType<SCT>>::TxnHash,
        >,
    ) -> (FullTransactionList, Option<FullTransactionList>) {
        let mut txs = Vec::new();
        let mut total_gas = 0;

        let mut txs_to_propose: Vec<_> = self.0.values().collect();
        // TODO: when sorting by gas fees is implemented, txs should be grouped by accounts
        txs_to_propose.sort_by(|a, b| a.nonce().cmp(&b.nonce()));

        for tx in txs_to_propose {
            if pending_blocktree_txs.contains(&tx.hash()) {
                continue;
            }

            if txs.len() == tx_limit || (total_gas + tx.gas_limit()) > gas_limit {
                break;
            }
            total_gas += tx.gas_limit();
            txs.push(tx.clone());
        }

        let proposal_num_tx = txs.len();
        let full_tx_list = EthFullTransactionList(txs).rlp_encode();

        tracing::info!(
            proposal_num_tx,
            proposal_total_gas = total_gas,
            proposal_tx_bytes = full_tx_list.len()
        );

        // TODO cascading behaviour for leftover txns once we have an idea of how we want
        // to forward
        self.0.clear();
        let leftovers = None;

        (FullTransactionList::new(full_tx_list), leftovers)
    }
}

/// A consensus block that has gone through the EthereumValidator and makes the decoded and
/// verified transactions availabe to access
#[derive(Debug, Clone)]
pub struct EthValidatedBlock<SCT: SignatureCollection> {
    pub block: Block<SCT>,
    pub validated_txns: Vec<EthSignedTransaction>,
}

impl<SCT: SignatureCollection> EthValidatedBlock<SCT> {
    pub fn get_validated_txn_hashes(&self) -> Vec<EthTxHash> {
        self.validated_txns.iter().map(|t| t.hash()).collect()
    }

    pub fn get_nonces(&self) -> Vec<u64> {
        self.validated_txns.iter().map(|t| t.nonce()).collect()
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
pub struct EthBlockPolicy;
impl<SCT: SignatureCollection> BlockPolicy<SCT> for EthBlockPolicy {
    type ValidatedBlock = EthValidatedBlock<SCT>;
}
