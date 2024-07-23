use std::{cmp::min, collections::BTreeMap};

use monad_consensus_types::{
    block::{Block, BlockPolicy, BlockType},
    quorum_certificate::QuorumCertificate,
    signature_collection::SignatureCollection,
    state_root_hash::StateRootHash,
};
use monad_crypto::hasher::{Hashable, Hasher};
use monad_eth_reserve_balance::{ReserveBalanceCacheResult, ReserveBalanceCacheTrait};
use monad_eth_tx::{EthTransaction, EthTxHash};
use monad_eth_types::{EthAddress, Nonce};
use monad_types::{BlockId, Epoch, NodeId, Round, SeqNum};
use sorted_vector_map::SortedVectorMap;
use tracing::debug;

pub trait AccountNonceRetrievable {
    fn get_account_nonces(&self) -> BTreeMap<EthAddress, Nonce>;
}

pub enum ComputeReserveBalanceResult {
    Val(u128),
    TrieDBNone, // No entry in Triedb
    NeedSync,   // Triedb needs sync
    Spent,      // Spent by the last execution-delay blocks carriage
}

pub enum ReserveBalanceCheck {
    Insert,
    Propose,
    Validate,
}

pub fn compute_intrinsic_gas(txn: &EthTransaction) -> u128 {
    // TODO implement intrinsic_gas formula
    0
}

pub fn compute_txn_carriage_cost(txn: &EthTransaction) -> u128 {
    let max_fee_per_gas = txn.max_fee_per_gas();
    let intrinsic_gas = compute_intrinsic_gas(txn);
    intrinsic_gas * max_fee_per_gas
}

// Computes reserve balance available for the account
pub fn compute_reserve_balance<SCT: SignatureCollection, RBCT: ReserveBalanceCacheTrait>(
    consensus_block_seq_num: SeqNum,
    cache: &mut RBCT,
    block_policy: &EthBlockPolicy,
    extending_blocks: Option<&Vec<&EthValidatedBlock<SCT>>>,
    eth_address: &EthAddress,
) -> ComputeReserveBalanceResult {
    let mut reserve_balance: u128;
    let mut block_seq_num: SeqNum;

    match cache.get_account_balance(consensus_block_seq_num, eth_address) {
        ReserveBalanceCacheResult::Val(seq_num, acc_balance) => {
            reserve_balance = min(acc_balance, block_policy.max_reserve_balance);
            block_seq_num = seq_num;
            debug!(
                "ReserveBalance compute 1: \
                        balance from cache: {:?} \
                        for TDB block: {:?} \
                        for address: {:?}",
                reserve_balance, block_seq_num, eth_address
            );
        }
        ReserveBalanceCacheResult::NeedSync => {
            debug!(
                "ReserveBalance compute 2: \
                        triedb needs sync \
                        consensus block seq num: {:?} \
                        for address: {:?}",
                consensus_block_seq_num, eth_address
            );
            return ComputeReserveBalanceResult::NeedSync;
        }
        ReserveBalanceCacheResult::None => {
            //Error
            return ComputeReserveBalanceResult::TrieDBNone;
        }
    }

    // Apply Carriage Cost for the txns from committed blocks
    let carriage_cost_committed: u128 =
        block_policy.compute_committed_txns_carriage_cost(eth_address, &block_seq_num);

    if reserve_balance < carriage_cost_committed {
        debug!(
            "ReserveBalance compute 3: \
                    Not sufficient balance: {:?} \
                    Carriage Cost Committed: {:?} \
                    consensus block:seq num {:?} \
                    for address: {:?}",
            reserve_balance, carriage_cost_committed, consensus_block_seq_num, eth_address
        );
        return ComputeReserveBalanceResult::Spent;
    } else {
        reserve_balance -= carriage_cost_committed;
        debug!(
            "ReserveBalance compute 4: \
                    updated balance to: {:?} \
                    Carriage Cost Committed: {:?} \
                    consensus block:seq num {:?} \
                    for address: {:?}",
            reserve_balance, carriage_cost_committed, consensus_block_seq_num, eth_address
        );
    }

    // Apply Carriage Cost for txns in extending blocks
    let mut carriage_cost_pending: u128 = 0;
    if let Some(blocks) = extending_blocks {
        for extending_block in blocks {
            for txn in &extending_block.validated_txns {
                if EthAddress(txn.signer()) == *eth_address {
                    carriage_cost_pending += compute_txn_carriage_cost(txn);
                }
            }
        }
    }

    if reserve_balance < carriage_cost_pending {
        debug!(
            "ReserveBalance compute 5: \
                    Not sufficient balance: {:?} \
                    Carriage Cost Pending: {:?} \
                    consensus block:seq num {:?} \
                    for address: {:?}",
            reserve_balance, carriage_cost_pending, consensus_block_seq_num, eth_address
        );
        return ComputeReserveBalanceResult::Spent;
    } else {
        reserve_balance -= carriage_cost_pending;
        debug!(
            "ReserveBalance compute 6: \
                    updated balance to: {:?} \
                    Carriage Cost Pending: {:?} \
                    consensus block:seq num {:?} \
                    for address: {:?}",
            reserve_balance, carriage_cost_pending, consensus_block_seq_num, eth_address
        );
    }

    ComputeReserveBalanceResult::Val(reserve_balance)
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

    fn get_state_root(&self) -> StateRootHash {
        self.block.payload.header.state_root
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

    // last execution-delay committed blocks
    pub txn_cache: SortedVectorMap<SeqNum, Vec<EthTransaction>>,

    /// Maximum reserve balance enforced by execution
    pub max_reserve_balance: u128,

    /// Cost for including transaction in the consensus
    pub execution_delay: u64,

    /// lowest-order bit 0 set: enable check for insert_tx
    /// lowest-order bit 1 set: enable check for create_proposal
    /// lowest-order bit 2 set: enable check for validation
    /// i.e. 0b00000111 all reserve balance checks are ehabled
    pub reserve_balance_check_mode: u8,
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

    pub fn get_committed_block_seq_num(&self) -> Option<SeqNum> {
        self.txn_cache.last_key_value().map(|seq_num| *seq_num.0)
    }

    pub fn update_txn_cache(&mut self, seq: SeqNum, txns: &[EthTransaction]) {
        let txn_cache_size: usize = self.execution_delay.try_into().unwrap();
        if self.txn_cache.len() >= txn_cache_size * 2 {
            let mut idx = 0;
            let mut divider: Option<SeqNum> = None;
            for (key, _) in &self.txn_cache {
                if idx == txn_cache_size {
                    divider = Some(*key);
                    break;
                }
                idx += 1;
            }
            if divider.is_some() {
                self.txn_cache.split_off(&divider.unwrap());
            }
        }
        self.txn_cache.insert(seq, txns.to_owned());
    }

    pub fn compute_committed_txns_carriage_cost(
        &self,
        eth_address: &EthAddress,
        triedb_block_seq_num: &SeqNum,
    ) -> u128 {
        let mut carriage_cost: u128 = 0;
        for item in &self.txn_cache {
            if item.0 > triedb_block_seq_num {
                for txn in item.1 {
                    if EthAddress(txn.signer()) == *eth_address {
                        carriage_cost += compute_txn_carriage_cost(txn);
                    }
                }
            }
        }
        carriage_cost
    }

    pub fn reserve_balance_check_enabled(&self, mode: ReserveBalanceCheck) -> bool {
        match mode {
            ReserveBalanceCheck::Insert => self.reserve_balance_check_mode & 0b00000001 > 0,
            ReserveBalanceCheck::Propose => self.reserve_balance_check_mode & 0b00000010 > 0,
            ReserveBalanceCheck::Validate => self.reserve_balance_check_mode & 0b00000100 > 0,
        }
    }
}

impl<SCT: SignatureCollection, RBCT: ReserveBalanceCacheTrait> BlockPolicy<SCT, RBCT>
    for EthBlockPolicy
{
    type ValidatedBlock = EthValidatedBlock<SCT>;

    fn check_coherency(
        &self,
        block: &Self::ValidatedBlock,
        extending_blocks: Vec<&Self::ValidatedBlock>,
        reserve_balance_cache: &mut RBCT,
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
            let res = compute_reserve_balance::<SCT, RBCT>(
                block.get_seq_num(),
                reserve_balance_cache,
                self,
                Some(&extending_blocks),
                &eth_address,
            );
            let reserve_balance = match res {
                ComputeReserveBalanceResult::Val(val) => {
                    debug!(
                        "ReserveBalance - check_coherency 1: \
                                reserve balance {:?} \
                                consensus block:seq num {:?} \
                                for address: {:?}",
                        val,
                        block.get_seq_num(),
                        eth_address
                    );
                    val
                }
                ComputeReserveBalanceResult::TrieDBNone => {
                    /* reserve balance is 0 */
                    debug!(
                        "ReserveBalance - check_coherency 2: \
                                TrieDBNone \
                                consensus block:seq num {:?} \
                                for address: {:?}",
                        block.get_seq_num(),
                        eth_address
                    );
                    0
                }
                ComputeReserveBalanceResult::NeedSync => {
                    /* TODO implement waiting, reserve balance is 0 for now */
                    debug!(
                        "ReserveBalance - check_coherency 3: \
                                NeedSync \
                                consensus block:seq num {:?} \
                                for address: {:?}",
                        block.get_seq_num(),
                        eth_address
                    );
                    0
                }
                ComputeReserveBalanceResult::Spent => {
                    debug!(
                        "ReserveBalance - check_coherency 4: \
                                Not sufficient balance: spent. \
                                consensus block:seq num {:?} \
                                for address: {:?}",
                        block.get_seq_num(),
                        eth_address
                    );
                    0
                }
            };

            let txn_carriage_cost = compute_txn_carriage_cost(txn);

            if reserve_balance >= txn_carriage_cost {
                pending_account_nonces.insert(eth_address, txn_nonce + 1);
            } else {
                debug!(
                    "ReserveBalance - check_coherency 5: \
                            Not sufficient balance: {:?} \
                            Txn Carriage Cost: {:?} \
                            consensus block:seq num {:?} \
                            for address: {:?}",
                    reserve_balance,
                    txn_carriage_cost,
                    block.get_seq_num(),
                    eth_address
                );
                return false;
            }
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
        self.update_txn_cache(block.get_seq_num(), &block.validated_txns);
    }
}
