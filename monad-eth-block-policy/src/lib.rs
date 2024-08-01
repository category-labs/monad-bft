use std::{cmp::min, collections::BTreeMap};

use monad_consensus_types::{
    block::{Block, BlockPolicy, BlockPolicyError, BlockType, CarriageCostValidationError},
    quorum_certificate::QuorumCertificate,
    signature_collection::SignatureCollection,
    state_root_hash::StateRootHash,
};
use monad_crypto::hasher::{Hashable, Hasher};
use monad_eth_reserve_balance::{
    state_backend::StateBackend, ReserveBalanceCacheResult, ReserveBalanceCacheTrait,
};
use monad_eth_tx::{EthTransaction, EthTxHash};
use monad_eth_types::{Balance, EthAddress, Nonce};
use monad_types::{BlockId, Epoch, NodeId, Round, SeqNum, GENESIS_SEQ_NUM};
use sorted_vector_map::SortedVectorMap;
use tracing::{debug, trace};

pub mod nonce;

/// Retriever trait for account nonces from block(s)
pub trait AccountNonceRetrievable {
    fn get_account_nonces(&self) -> BTreeMap<EthAddress, Nonce>;
}
pub enum ReserveBalanceCheck {
    Insert,
    Propose,
    Validate,
}

pub fn compute_intrinsic_gas(txn: &EthTransaction) -> u128 {
    // TODO implement full intrinsic_gas formula, use a simple formula for now.
    21000 + 32000
}

pub fn compute_txn_carriage_cost(txn: &EthTransaction) -> u128 {
    let max_fee_per_gas = txn.max_fee_per_gas();
    let intrinsic_gas = compute_intrinsic_gas(txn);
    intrinsic_gas * max_fee_per_gas
}

/// A consensus block that has gone through the EthereumValidator and makes the decoded and
/// verified transactions available to access
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

    fn get_timestamp(&self) -> u64 {
        self.block.timestamp
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

#[derive(Debug)]
struct BlockAccountNonce {
    nonces: BTreeMap<EthAddress, Nonce>,
}

impl BlockAccountNonce {
    fn get(&self, eth_address: &EthAddress) -> Option<Nonce> {
        self.nonces.get(eth_address).cloned()
    }
}

#[derive(Debug)]
struct CommittedTxnBuffer {
    // TODO: create a reserve balance map for transactions when validating
    // Block, and store it in EthValidatedBlock
    txns: SortedVectorMap<SeqNum, (Vec<EthTransaction>, BlockAccountNonce)>,
    size: usize, // should be execution delay
}

struct CommittedCarriageCostResult {
    carriage_cost: Balance,
    next_validate: SeqNum, // next block number to validate; included for assertions only
}

impl CommittedTxnBuffer {
    fn new(size: usize) -> Self {
        Self {
            txns: Default::default(),
            size,
        }
    }

    fn get_nonce(&self, eth_address: &EthAddress) -> Option<Nonce> {
        let mut maybe_account_nonce = None;

        for (_, (_, account_nonces)) in self.txns.iter() {
            if let Some(nonce) = account_nonces.get(eth_address) {
                if let Some(old_account_nonce) = maybe_account_nonce {
                    assert!(nonce > old_account_nonce);
                }
                maybe_account_nonce = Some(nonce);
            }
        }
        maybe_account_nonce
    }

    fn compute_carriage_cost(
        &self,
        base_seq_num: SeqNum,
        eth_address: &EthAddress,
    ) -> CommittedCarriageCostResult {
        let mut carriage_cost: u128 = 0;

        let mut next_validate = base_seq_num + SeqNum(1);
        for (&cache_seq_num, (txns, _)) in self.txns.iter() {
            if cache_seq_num > base_seq_num {
                assert_eq!(next_validate, cache_seq_num);
                for txn in txns {
                    if EthAddress(txn.signer()) == *eth_address {
                        carriage_cost += compute_txn_carriage_cost(txn);
                    }
                }
                next_validate += SeqNum(1);
            }
        }

        CommittedCarriageCostResult {
            carriage_cost,
            next_validate,
        }
    }

    fn update_committed_block<SCT: SignatureCollection>(&mut self, block: &EthValidatedBlock<SCT>) {
        let block_number = block.get_seq_num();
        if let Some((&last_block_num, _)) = self.txns.last_key_value() {
            assert_eq!(last_block_num + SeqNum(1), block_number);
        }

        if self.txns.len() >= self.size * 2 {
            let (&first_block_num, _) = self.txns.first_key_value().expect("txns non-empty");
            let divider = first_block_num + SeqNum(self.size as u64);

            // TODO: revisit once perf implications are understood
            self.txns = self.txns.split_off(&divider);
            assert_eq!(
                *self.txns.last_key_value().expect("non-empty").0 + SeqNum(1),
                block_number
            );
            assert_eq!(self.txns.len(), self.size);
        }
        self.txns.insert(
            block_number,
            (
                block.validated_txns.clone(),
                BlockAccountNonce {
                    nonces: block.get_account_nonces(),
                },
            ),
        );
    }
}

/// A block policy for ethereum payloads
pub struct EthBlockPolicy {
    /// SeqNum of last committed block
    last_commit: SeqNum,

    // last execution-delay committed transactions
    committed_cache: CommittedTxnBuffer,

    /// Maximum reserve balance enforced by execution
    max_reserve_balance: u128,

    /// Cost for including transaction in the consensus
    execution_delay: SeqNum,

    /// Chain ID
    chain_id: u64,

    /// lowest-order bit 0 set: enable check for insert_tx
    /// lowest-order bit 1 set: enable check for create_proposal
    /// lowest-order bit 2 set: enable check for validation
    /// i.e. 0b00000111 all reserve balance checks are ehabled
    reserve_balance_check_mode: u8,
}

impl EthBlockPolicy {
    pub fn new(
        last_commit: SeqNum,
        max_reserve_balance: u128,
        execution_delay: u64,
        reserve_balance_check_mode: u8,
        chain_id: u64,
    ) -> Self {
        Self {
            committed_cache: CommittedTxnBuffer::new(execution_delay as usize),
            last_commit,
            max_reserve_balance,
            execution_delay: SeqNum(execution_delay),
            reserve_balance_check_mode,
            chain_id,
        }
    }

    pub fn get_account_nonce<SBT: StateBackend, RBCT: ReserveBalanceCacheTrait<SBT>>(
        &self,
        eth_address: &EthAddress,
        pending_block_nonces: &BTreeMap<EthAddress, Nonce>,
        reserve_balance_cache: &mut RBCT,
    ) -> Result<Nonce, CarriageCostValidationError> {
        // Layers of access
        // 1. pending_block_nonces: coherent blocks in the blocks tree
        // 2. committed_block_nonces: always buffers the nonce of last `delay`
        //    committed blocks
        // 2. LRU cache of triedb nonces
        // 3. triedb query
        if let Some(&coherent_block_nonce) = pending_block_nonces.get(eth_address) {
            Ok(coherent_block_nonce)
        } else if let Some(committed_nonce) = self.committed_cache.get_nonce(eth_address) {
            Ok(committed_nonce)
        } else {
            // the cached account nonce must overlap with latest triedb, i.e.
            // account_nonces must keep nonces for last delay blocks in cache
            // the cache should keep track of block number for the nonce state
            // when purging, we never purge nonces newer than last_commit - delay

            match reserve_balance_cache.get_account(
                SeqNum(
                    self.last_commit.0.max(self.committed_cache.size as u64)
                        - self.committed_cache.size as u64,
                ),
                eth_address,
            ) {
                ReserveBalanceCacheResult::Val(_, nonce) => Ok(nonce),
                ReserveBalanceCacheResult::None => Err(CarriageCostValidationError::AccountNoExist),
                ReserveBalanceCacheResult::NeedSync => {
                    Err(CarriageCostValidationError::TrieDBNeedsSync)
                }
            }
        }
    }

    pub fn get_last_commit(&self) -> SeqNum {
        self.last_commit
    }

    pub fn reserve_balance_check_enabled(&self, mode: ReserveBalanceCheck) -> bool {
        match mode {
            ReserveBalanceCheck::Insert => self.reserve_balance_check_mode & 0b00000001 > 0,
            ReserveBalanceCheck::Propose => self.reserve_balance_check_mode & 0b00000010 > 0,
            ReserveBalanceCheck::Validate => self.reserve_balance_check_mode & 0b00000100 > 0,
        }
    }

    // Computes reserve balance available for the account
    pub fn compute_reserve_balance<
        SCT: SignatureCollection,
        SBT: StateBackend,
        RBCT: ReserveBalanceCacheTrait<SBT>,
    >(
        &self,
        consensus_block_seq_num: SeqNum,
        cache: &mut RBCT,
        extending_blocks: Option<&Vec<&EthValidatedBlock<SCT>>>,
        eth_address: &EthAddress,
    ) -> Result<u128, CarriageCostValidationError> {
        trace!(block = consensus_block_seq_num.0, "compute_reserve_balance");
        let mut reserve_balance: u128;

        // calculation correct only if GENESIS_SEQ_NUM == 0
        assert_eq!(GENESIS_SEQ_NUM, SeqNum(0));
        let base_seq_num = consensus_block_seq_num.max(self.execution_delay) - self.execution_delay;

        match cache.get_account(base_seq_num, eth_address) {
            ReserveBalanceCacheResult::Val(acc_balance, _) => {
                reserve_balance = min(acc_balance, self.max_reserve_balance);

                debug!(
                    "ReserveBalance compute 1: \
                        balance from cache: {:?} \
                        for TDB block: {:?} \
                        for address: {:?}",
                    reserve_balance, base_seq_num, eth_address
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
                return Err(CarriageCostValidationError::TrieDBNeedsSync);
            }
            ReserveBalanceCacheResult::None => {
                return Err(CarriageCostValidationError::AccountNoExist);
            }
        }

        // Apply Carriage Cost for the txns from committed blocks
        let CommittedCarriageCostResult {
            carriage_cost: carriage_cost_committed,
            next_validate,
        } = self
            .committed_cache
            .compute_carriage_cost(base_seq_num, eth_address);

        if reserve_balance < carriage_cost_committed {
            debug!(
                "ReserveBalance compute 3: \
                    Not sufficient balance: {:?} \
                    Carriage Cost Committed: {:?} \
                    consensus block:seq num {:?} \
                    for address: {:?}",
                reserve_balance, carriage_cost_committed, consensus_block_seq_num, eth_address
            );
            // FIXME: transactions are incorrectly included in committed block
            return Err(CarriageCostValidationError::InsufficientReserveBalance);
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
            if let Some(first_block) = blocks.first() {
                assert_eq!(
                    first_block.get_seq_num(),
                    next_validate,
                    "consensus sq {:?}\n first block {:?}\n committed_cache {:?}\n",
                    consensus_block_seq_num,
                    first_block,
                    self.committed_cache
                );
            }

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
            // FIXME: transactions are incorrectly included in pending blocks
            return Err(CarriageCostValidationError::InsufficientReserveBalance);
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

        Ok(reserve_balance)
    }

    pub fn get_chain_id(&self) -> u64 {
        self.chain_id
    }
}

impl<SCT: SignatureCollection, SBT: StateBackend, RBCT: ReserveBalanceCacheTrait<SBT>>
    BlockPolicy<SCT, SBT, RBCT> for EthBlockPolicy
{
    type ValidatedBlock = EthValidatedBlock<SCT>;

    fn check_coherency(
        &self,
        block: &Self::ValidatedBlock,
        extending_blocks: Vec<&Self::ValidatedBlock>,
        account_balance_cache: &mut RBCT,
    ) -> Result<(), BlockPolicyError> {
        trace!(?block, "check_coherency");
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

            let expected_nonce = self
                .get_account_nonce(&eth_address, &pending_account_nonces, account_balance_cache)
                .map_err(|err| match err {
                    // FIXME: restructure error types
                    CarriageCostValidationError::AccountNoExist => {
                        BlockPolicyError::BlockNotCoherent
                    }
                    _ => BlockPolicyError::CarriageCostError(err),
                })?;

            if txn_nonce != expected_nonce {
                return Err(BlockPolicyError::BlockNotCoherent);
            }
            let res = self.compute_reserve_balance::<SCT, SBT, RBCT>(
                block.get_seq_num(),
                account_balance_cache,
                Some(&extending_blocks),
                &eth_address,
            );
            let reserve_balance = match res {
                Ok(val) => {
                    trace!(
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
                Err(err) => {
                    trace!(
                        "ReserveBalance - check_coherency 2: \
                                        Carriage Cost Error:  {:?} \
                                        consensus block:seq num {:?} \
                                        for address: {:?}",
                        err,
                        block.get_seq_num(),
                        eth_address
                    );
                    return Err(match err {
                        CarriageCostValidationError::AccountNoExist => {
                            BlockPolicyError::BlockNotCoherent
                        }
                        _ => BlockPolicyError::CarriageCostError(err),
                    });
                }
            };

            let txn_carriage_cost = compute_txn_carriage_cost(txn);

            if reserve_balance >= txn_carriage_cost {
                pending_account_nonces.insert(eth_address, txn_nonce + 1);
            } else {
                trace!(
                    "ReserveBalance - check_coherency 3: \
                            Not sufficient balance: {:?} \
                            Txn Carriage Cost: {:?} \
                            consensus block:seq num {:?} \
                            for address: {:?}",
                    reserve_balance,
                    txn_carriage_cost,
                    block.get_seq_num(),
                    eth_address
                );
                return Err(BlockPolicyError::BlockNotCoherent);
            }
        }
        Ok(())
    }

    fn update_committed_block(&mut self, block: &Self::ValidatedBlock) {
        assert_eq!(block.get_seq_num(), self.last_commit + SeqNum(1));
        self.last_commit = block.get_seq_num();
        self.committed_cache.update_committed_block(block);
    }
}
