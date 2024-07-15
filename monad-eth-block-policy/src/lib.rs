use std::collections::BTreeMap;

use monad_consensus_types::{
    block::{Block, BlockPolicy, BlockType},
    quorum_certificate::QuorumCertificate,
    signature_collection::SignatureCollection,
};
use monad_crypto::hasher::{Hashable, Hasher};
use monad_eth_tx::{EthTransaction, EthTxHash};
use monad_eth_types::{EthAddress, Nonce};
use monad_types::{BlockId, Epoch, NodeId, Round, SeqNum};

pub trait AccountNonceRetrievable {
    fn get_account_nonces(&self) -> BTreeMap<EthAddress, Nonce>;
}

/// A consensus block that has gone through the EthereumValidator and makes the decoded and
/// verified transactions availabe to access
#[derive(Debug, Clone)]
pub struct EthValidatedBlock<SCT: SignatureCollection> {
    pub block: Block<SCT>,
    pub validated_txns: Vec<EthTransaction>,
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

impl<SCT: SignatureCollection> AccountNonceRetrievable for EthValidatedBlock<SCT> {
    fn get_account_nonces(&self) -> BTreeMap<EthAddress, Nonce> {
        let mut account_nonces = BTreeMap::new();
        let block_nonces = self.get_nonces();
        for (&address, &txn_nonce) in block_nonces {
            // account_nonce is the number of txns the account has sent. It's
            // one higher than the last txn nonce
            let acc_nonce = txn_nonce + 1;
            account_nonces.insert(address, acc_nonce);
        }
        account_nonces
    }
}

impl<SCT: SignatureCollection> AccountNonceRetrievable for Vec<&EthValidatedBlock<SCT>> {
    fn get_account_nonces(&self) -> BTreeMap<EthAddress, Nonce> {
        let mut account_nonces = BTreeMap::new();
        for block in self.iter() {
            let block_account_nonces = block.get_account_nonces();
            for (address, account_nonce) in block_account_nonces {
                account_nonces.insert(address, account_nonce);
            }
        }
        account_nonces
    }
}

/// A block policy for ethereum payloads
pub struct EthBlockPolicy {
    // Maps EthAddresses to the its nonces in the last committed block
    // TODO: All nonces exist here for now. Should be moved to a DB
    pub account_nonces: BTreeMap<EthAddress, Nonce>,

    /// SeqNum of last committed block
    pub last_commit: SeqNum,
}

impl EthBlockPolicy {
    pub fn get_account_nonce(
        &self,
        eth_address: &EthAddress,
        pending_block_nonces: &BTreeMap<EthAddress, Nonce>,
    ) -> Nonce {
        // Layers of access
        // 1. pending_block_nonces: coherent blocks in the blocks tree
        // 2. (TODO) LRU cache of triedb nonces, updated when blocks are committed
        // 3. (TODO) triedb query
        if let Some(&coherent_block_nonce) = pending_block_nonces.get(eth_address) {
            coherent_block_nonce
        } else if let Some(&committed_nonce) = self.account_nonces.get(eth_address) {
            committed_nonce
        } else {
            return 0;
            todo!("triedb read: nonce in triedb is always next nonce");
        }
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
            self.last_commit + SeqNum(1)
        );
        // Get the account nonce from coherent blocks to extend
        let mut pending_account_nonces = extending_blocks.get_account_nonces();

        for txn in block.validated_txns.iter() {
            let eth_address = EthAddress(txn.signer());
            let txn_nonce = txn.nonce();

            let expected_nonce = self.get_account_nonce(&eth_address, &pending_account_nonces);
            if txn_nonce != expected_nonce {
                return false;
            }
            pending_account_nonces.insert(eth_address, txn_nonce + 1);
        }

        true
    }

    fn update_committed_block(&mut self, block: &Self::ValidatedBlock) {
        assert_eq!(block.get_seq_num(), self.last_commit + SeqNum(1));
        self.last_commit = block.get_seq_num();
        let committed_block_account_nonces = block.get_account_nonces();
        for (address, account_nonce) in committed_block_account_nonces {
            self.account_nonces.insert(address, account_nonce);
        }
    }
}
