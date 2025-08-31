// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::{collections::BTreeMap, marker::PhantomData, ops::Deref};

use alloy_consensus::{
    transaction::{Recovered, Transaction},
    TxEnvelope,
};
use alloy_primitives::{Address, TxHash, U256};
use itertools::Itertools;
use monad_consensus_types::{
    block::{BlockPolicy, BlockPolicyError, ConsensusFullBlock},
    checkpoint::RootInfo,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_types::{Balance, EthAccount, EthExecutionProtocol, EthHeader, Nonce};
use monad_state_backend::{StateBackend, StateBackendError};
use monad_system_calls::SystemTransaction;
use monad_types::{BlockId, Epoch, Round, SeqNum, GENESIS_BLOCK_ID, GENESIS_SEQ_NUM};
use monad_validator::signature_collection::SignatureCollection;
use sorted_vector_map::SortedVectorMap;
use tracing::{trace, warn};

pub mod validation;

/// Retriever trait for account nonces from block(s)
pub trait AccountNonceRetrievable {
    fn get_account_nonces(&self) -> BTreeMap<Address, Nonce>;
}
pub enum ReserveBalanceCheck {
    Insert,
    Propose,
    Validate,
}

pub fn compute_txn_max_value(txn: &TxEnvelope) -> U256 {
    let txn_value = txn.value();
    let gas_limit = U256::from(txn.gas_limit());
    let max_fee = U256::from(txn.max_fee_per_gas());
    let gas_cost = gas_limit.checked_mul(max_fee).expect("no overflow");
    txn_value.saturating_add(gas_cost)
}

struct BlockLookupIndex {
    block_id: BlockId,
    seq_num: SeqNum,
    is_finalized: bool,
}

/// A consensus block that has gone through the EthereumValidator and makes the decoded and
/// verified transactions available to access
#[derive(Debug, Clone)]
pub struct EthValidatedBlock<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    pub block: ConsensusFullBlock<ST, SCT, EthExecutionProtocol>,
    pub system_txns: Vec<SystemTransaction>,
    pub validated_txns: Vec<Recovered<TxEnvelope>>,
    pub nonces: BTreeMap<Address, Nonce>,
    pub txn_fees: BTreeMap<Address, Balance>,
}

impl<ST, SCT> AsRef<EthValidatedBlock<ST, SCT>> for EthValidatedBlock<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn as_ref(&self) -> &EthValidatedBlock<ST, SCT> {
        self
    }
}

impl<ST, SCT> Deref for EthValidatedBlock<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    type Target = ConsensusFullBlock<ST, SCT, EthExecutionProtocol>;
    fn deref(&self) -> &Self::Target {
        &self.block
    }
}

impl<ST, SCT> EthValidatedBlock<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    pub fn get_validated_txn_hashes(&self) -> Vec<TxHash> {
        self.validated_txns.iter().map(|t| *t.tx_hash()).collect()
    }

    /// Returns the highest tx nonce per account in the block
    pub fn get_nonces(&self) -> &BTreeMap<Address, u64> {
        &self.nonces
    }

    pub fn get_total_gas(&self) -> u64 {
        self.validated_txns
            .iter()
            .fold(0, |acc, tx| acc + tx.gas_limit())
    }
}

impl<ST, SCT> PartialEq for EthValidatedBlock<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn eq(&self, other: &Self) -> bool {
        self.block == other.block
    }
}
impl<ST, SCT> Eq for EthValidatedBlock<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
}

impl<ST, SCT> AccountNonceRetrievable for EthValidatedBlock<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn get_account_nonces(&self) -> BTreeMap<Address, Nonce> {
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

impl<ST, SCT> AccountNonceRetrievable for Vec<&EthValidatedBlock<ST, SCT>>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn get_account_nonces(&self) -> BTreeMap<Address, Nonce> {
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
    nonces: BTreeMap<Address, Nonce>,
}

impl BlockAccountNonce {
    fn get(&self, eth_address: &Address) -> Option<Nonce> {
        self.nonces.get(eth_address).cloned()
    }
}

#[derive(Debug)]
struct BlockTxnFees {
    txn_fees: BTreeMap<Address, Balance>,
}

impl BlockTxnFees {
    fn get(&self, eth_address: &Address) -> Option<Balance> {
        self.txn_fees.get(eth_address).cloned()
    }
}

#[derive(Debug)]
struct CommittedBlock {
    block_id: BlockId,
    round: Round,
    epoch: Epoch,

    nonces: BlockAccountNonce,
    fees: BlockTxnFees,

    base_fee: u64,
    base_fee_trend: u64,
    base_fee_moment: u64,
    block_gas_usage: u64,
}

#[derive(Debug)]
struct CommittedBlkBuffer<ST, SCT> {
    blocks: SortedVectorMap<SeqNum, CommittedBlock>,
    size: usize, // should be execution delay

    _phantom: PhantomData<(ST, SCT)>,
}

struct CommittedTxnFeeResult {
    txn_fee: Balance,
    next_validate: SeqNum, // next block number to validate; included for assertions only
}

impl<ST, SCT> CommittedBlkBuffer<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn new(size: usize) -> Self {
        Self {
            blocks: Default::default(),
            size,

            _phantom: Default::default(),
        }
    }

    fn get_epoch(&self, seq_num: SeqNum) -> Option<Epoch> {
        self.blocks
            .get(&seq_num)
            .map(|committed_block| committed_block.epoch)
    }

    fn get_nonce(&self, eth_address: &Address) -> Option<Nonce> {
        let mut maybe_account_nonce = None;

        for block in self.blocks.values() {
            if let Some(nonce) = block.nonces.get(eth_address) {
                if let Some(old_account_nonce) = maybe_account_nonce {
                    assert!(nonce > old_account_nonce);
                }
                maybe_account_nonce = Some(nonce);
            }
        }
        maybe_account_nonce
    }

    fn compute_txn_fee(
        &self,
        base_seq_num: SeqNum,
        eth_address: &Address,
    ) -> CommittedTxnFeeResult {
        let mut txn_fee: Balance = Balance::ZERO;
        let mut next_validate = base_seq_num + SeqNum(1);

        // start iteration from base_seq_num (non inclusive)
        for (&cache_seq_num, block) in self.blocks.range(next_validate..) {
            assert_eq!(next_validate, cache_seq_num);
            if let Some(account_txn_fee) = block.fees.get(eth_address) {
                txn_fee = txn_fee.saturating_add(account_txn_fee);
            }
            next_validate += SeqNum(1);
        }

        CommittedTxnFeeResult {
            txn_fee,
            next_validate,
        }
    }

    fn update_committed_block(&mut self, block: &EthValidatedBlock<ST, SCT>) {
        let block_number = block.get_seq_num();
        if let Some((&last_block_num, _)) = self.blocks.last_key_value() {
            assert_eq!(last_block_num + SeqNum(1), block_number);
        }

        if self.blocks.len() >= self.size.saturating_mul(2) {
            let (&first_block_num, _) = self.blocks.first_key_value().expect("txns non-empty");
            let divider = first_block_num + SeqNum(self.size as u64);

            // TODO: revisit once perf implications are understood
            self.blocks = self.blocks.split_off(&divider);
            assert_eq!(
                *self.blocks.last_key_value().expect("non-empty").0 + SeqNum(1),
                block_number
            );
            assert_eq!(self.blocks.len(), self.size);
        }

        let block_gas_usage = block.get_total_gas();

        assert!(self
            .blocks
            .insert(
                block_number,
                CommittedBlock {
                    block_id: block.get_id(),
                    round: block.get_block_round(),
                    epoch: block.get_epoch(),
                    nonces: BlockAccountNonce {
                        nonces: block.get_account_nonces(),
                    },
                    fees: BlockTxnFees {
                        txn_fees: block.txn_fees.clone()
                    },

                    base_fee: block.block.header().base_fee,
                    base_fee_trend: block.block.header().base_fee_trend,
                    base_fee_moment: block.block.header().base_fee_moment,
                    block_gas_usage,
                },
            )
            .is_none());
    }
}

/// A block policy for ethereum payloads
#[derive(Debug)]
pub struct EthBlockPolicy<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    /// SeqNum of last committed block
    last_commit: SeqNum,

    // last execution-delay committed blocks
    committed_cache: CommittedBlkBuffer<ST, SCT>,

    execution_delay: SeqNum,

    /// Chain ID
    chain_id: u64,
}

impl<ST, SCT> EthBlockPolicy<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    pub fn new(
        last_commit: SeqNum, // TODO deprecate
        execution_delay: u64,
        chain_id: u64,
    ) -> Self {
        Self {
            committed_cache: CommittedBlkBuffer::new(execution_delay as usize),
            last_commit,
            execution_delay: SeqNum(execution_delay),
            chain_id,
        }
    }

    /// returns account nonces at the start of the provided consensus block
    pub fn get_account_base_nonces<'a>(
        &self,
        consensus_block_seq_num: SeqNum,
        state_backend: &impl StateBackend<ST, SCT>,
        extending_blocks: &Vec<&EthValidatedBlock<ST, SCT>>,
        addresses: impl Iterator<Item = &'a Address>,
    ) -> Result<BTreeMap<&'a Address, Nonce>, StateBackendError> {
        // Layers of access
        // 1. extending_blocks: coherent blocks in the blocks tree
        // 2. committed_block_nonces: always buffers the nonce of last `delay`
        //    committed blocks
        // 3. LRU cache of triedb nonces
        // 4. triedb query
        let mut account_nonces = BTreeMap::default();
        let pending_block_nonces = extending_blocks.get_account_nonces();
        let mut cache_misses = Vec::new();
        for address in addresses.unique() {
            if let Some(&pending_nonce) = pending_block_nonces.get(address) {
                // hit cache level 1
                account_nonces.insert(address, pending_nonce);
                continue;
            }
            if let Some(committed_nonce) = self.committed_cache.get_nonce(address) {
                // hit cache level 2
                account_nonces.insert(address, committed_nonce);
                continue;
            }
            cache_misses.push(address)
        }

        // the cached account nonce must overlap with latest triedb, i.e.
        // account_nonces must keep nonces for last delay blocks in cache
        // the cache should keep track of block number for the nonce state
        // when purging, we never purge nonces newer than last_commit - delay

        let base_seq_num = consensus_block_seq_num.max(self.execution_delay) - self.execution_delay;
        let cache_miss_statuses = self.get_account_statuses(
            state_backend,
            &Some(extending_blocks),
            cache_misses.iter().copied(),
            &base_seq_num,
        )?;
        account_nonces.extend(
            cache_misses
                .into_iter()
                .zip_eq(cache_miss_statuses)
                .map(|(address, status)| (address, status.map_or(0, |status| status.nonce))),
        );

        Ok(account_nonces)
    }

    pub fn get_last_commit(&self) -> SeqNum {
        self.last_commit
    }

    pub fn get_last_commit_epoch(&self) -> Epoch {
        if self.last_commit == GENESIS_SEQ_NUM {
            Epoch(1)
        } else {
            self.committed_cache
                .get_epoch(self.last_commit)
                .expect("last committed block in committed cache")
        }
    }

    fn get_block_index(
        &self,
        extending_blocks: &Option<&Vec<&EthValidatedBlock<ST, SCT>>>,
        base_seq_num: &SeqNum,
    ) -> Result<BlockLookupIndex, StateBackendError> {
        if base_seq_num <= &self.last_commit {
            if base_seq_num == &GENESIS_SEQ_NUM {
                Ok(BlockLookupIndex {
                    block_id: GENESIS_BLOCK_ID,
                    seq_num: GENESIS_SEQ_NUM,
                    is_finalized: true,
                })
            } else {
                let committed_block = &self
                    .committed_cache
                    .blocks
                    .get(base_seq_num)
                    .unwrap_or_else(|| panic!("queried recently committed block that doesn't exist, base_seq_num={:?}, last_commit={:?}", base_seq_num, self.last_commit));
                Ok(BlockLookupIndex {
                    block_id: committed_block.block_id,
                    seq_num: *base_seq_num,
                    is_finalized: true,
                })
            }
        } else if let Some(extending_blocks) = extending_blocks {
            let proposed_block = extending_blocks
                .iter()
                .find(|block| &block.get_seq_num() == base_seq_num)
                .expect("extending block doesn't exist");
            Ok(BlockLookupIndex {
                block_id: proposed_block.get_id(),
                seq_num: *base_seq_num,
                is_finalized: false,
            })
        } else {
            Err(StateBackendError::NotAvailableYet)
        }
    }

    fn get_account_statuses<'a>(
        &self,
        state_backend: &impl StateBackend<ST, SCT>,
        extending_blocks: &Option<&Vec<&EthValidatedBlock<ST, SCT>>>,
        addresses: impl Iterator<Item = &'a Address>,
        base_seq_num: &SeqNum,
    ) -> Result<Vec<Option<EthAccount>>, StateBackendError> {
        let block_index = self.get_block_index(extending_blocks, base_seq_num)?;
        state_backend.get_account_statuses(
            &block_index.block_id,
            base_seq_num,
            block_index.is_finalized,
            addresses,
        )
    }

    // Computes account balance available for the account
    pub fn compute_account_base_balances<'a>(
        &self,
        consensus_block_seq_num: SeqNum,
        state_backend: &impl StateBackend<ST, SCT>,
        extending_blocks: Option<&Vec<&EthValidatedBlock<ST, SCT>>>,
        addresses: impl Iterator<Item = &'a Address>,
    ) -> Result<BTreeMap<&'a Address, Balance>, StateBackendError>
    where
        SCT: SignatureCollection,
    {
        trace!(block = consensus_block_seq_num.0, "compute_base_balance");

        // calculation correct only if GENESIS_SEQ_NUM == 0
        assert_eq!(GENESIS_SEQ_NUM, SeqNum(0));
        let base_seq_num = consensus_block_seq_num.max(self.execution_delay) - self.execution_delay;

        let addresses = addresses.unique().collect_vec();
        let account_balances = self
            .get_account_statuses(
                state_backend,
                &extending_blocks,
                addresses.iter().copied(),
                &base_seq_num,
            )?
            .into_iter()
            .map(|maybe_status| maybe_status.map_or(Balance::ZERO, |status| status.balance))
            .collect_vec();

        let account_balances = addresses
            .into_iter()
            .zip_eq(account_balances)
            .map(|(address, mut account_balance)| {
                // Apply Txn Fees for the txns from committed blocks
                let CommittedTxnFeeResult {
                    txn_fee: txn_fee_committed,
                    mut next_validate,
                } = self.committed_cache.compute_txn_fee(base_seq_num, address);

                if account_balance < txn_fee_committed {
                    panic!(
                        "Committed block with incoherent transaction fee 
                            Not sufficient balance: {:?} \
                            Transaction Fee Committed: {:?} \
                            consensus block:seq num {:?} \
                            for address: {:?}",
                        account_balance, txn_fee_committed, consensus_block_seq_num, address
                    );
                } else {
                    account_balance -= txn_fee_committed;
                    trace!(
                        "AccountBalance compute 4: \
                            updated balance to: {:?} \
                            Transaction Fee Committed: {:?} \
                            consensus block:seq num {:?} \
                            for address: {:?}",
                        account_balance,
                        txn_fee_committed,
                        consensus_block_seq_num,
                        address
                    );
                }

                // Apply Txn Fees for txns in extending blocks
                let mut txn_fee_pending: Balance = Balance::ZERO;
                if let Some(blocks) = extending_blocks {
                    // handle the case where base_seq_num is a pending block
                    let next_blocks = blocks
                        .iter()
                        .skip_while(move |block| block.get_seq_num() < next_validate);
                    for extending_block in next_blocks {
                        assert_eq!(next_validate, extending_block.get_seq_num());
                        if let Some(txn_fee) = extending_block.txn_fees.get(address) {
                            txn_fee_pending = txn_fee_pending.saturating_add(*txn_fee);
                        }
                        next_validate += SeqNum(1);
                    }
                }

                if account_balance < txn_fee_pending {
                    panic!(
                        "Majority extended a block with an incoherent balance \
                            Not sufficient balance: {:?} \
                            Txn Fees Pending: {:?} \
                            consensus block:seq num {:?} \
                            for address: {:?}",
                        account_balance, txn_fee_pending, consensus_block_seq_num, address
                    );
                } else {
                    account_balance -= txn_fee_pending;
                    trace!(
                        "AccountBalance compute 6: \
                            updated balance to: {:?} \
                            Txn Fees Pending: {:?} \
                            consensus block:seq num {:?} \
                            for address: {:?}",
                        account_balance,
                        txn_fee_pending,
                        consensus_block_seq_num,
                        address
                    );
                }

                (address, account_balance)
            })
            .collect();
        Ok(account_balances)
    }

    fn get_parent_base_fee_fields<B>(&self, extending_blocks: &[B]) -> (u64, u64, u64, u64)
    where
        B: AsRef<EthValidatedBlock<ST, SCT>>,
    {
        // parent block is last block in extending_blocks or last_committed
        // block if there's no extending branch
        let (parent_base_fee, parent_trend, parent_moment, parent_gas_usage) =
            if let Some(parent_block) = extending_blocks.last() {
                let parent_gas_usage = parent_block
                    .as_ref()
                    .validated_txns
                    .iter()
                    .map(|txn| txn.gas_limit())
                    .sum::<u64>();
                (
                    parent_block.as_ref().header().base_fee,
                    parent_block.as_ref().header().base_fee_trend,
                    parent_block.as_ref().header().base_fee_moment,
                    parent_gas_usage,
                )
            } else {
                // genesis block doesn't exist in committed_cache
                // when upgrading, we treat the fork block as genesis block
                if self.last_commit == GENESIS_SEQ_NUM {
                    // genesis block
                    (
                        monad_tfm::base_fee::GENESIS_BASE_FEE,
                        monad_tfm::base_fee::GENESIS_BASE_FEE_TREND,
                        monad_tfm::base_fee::GENESIS_BASE_FEE_MOMENT,
                        0,
                    )
                } else {
                    let parent_block = self
                        .committed_cache
                        .blocks
                        .get(&self.last_commit)
                        .expect("last committed block must exist");
                    (
                        parent_block.base_fee,
                        parent_block.base_fee_trend,
                        parent_block.base_fee_moment,
                        parent_block.block_gas_usage,
                    )
                }
            };

        (
            parent_base_fee,
            parent_trend,
            parent_moment,
            parent_gas_usage,
        )
    }

    // TODO: introduce chain config to block policy to make parameters
    // configurable
    const BLOCK_GAS_LIMIT: u64 = 150_000_000;

    /// return value
    ///
    /// (base_fee, base_fee_trend, base_fee_moment)
    ///
    /// base_fee unit: MON-wei
    pub fn compute_base_fee<B>(&self, extending_blocks: &[B]) -> (u64, u64, u64)
    where
        B: AsRef<EthValidatedBlock<ST, SCT>>,
    {
        let (parent_base_fee, parent_trend, parent_moment, parent_gas_usage) =
            self.get_parent_base_fee_fields(extending_blocks);

        monad_tfm::base_fee::compute_base_fee(
            Self::BLOCK_GAS_LIMIT,
            parent_gas_usage,
            parent_base_fee,
            parent_trend,
            parent_moment,
        )
    }

    pub fn get_chain_id(&self) -> u64 {
        self.chain_id
    }
}

impl<ST, SCT, SBT> BlockPolicy<ST, SCT, EthExecutionProtocol, SBT> for EthBlockPolicy<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT>,
{
    type ValidatedBlock = EthValidatedBlock<ST, SCT>;

    fn check_coherency(
        &self,
        block: &Self::ValidatedBlock,
        extending_blocks: Vec<&Self::ValidatedBlock>,
        blocktree_root: RootInfo,
        state_backend: &SBT,
    ) -> Result<(), BlockPolicyError> {
        trace!(?block, "check_coherency");
        let first_block = extending_blocks
            .iter()
            .chain(std::iter::once(&block))
            .next()
            .unwrap();
        assert_eq!(first_block.get_seq_num(), self.last_commit + SeqNum(1));

        // check coherency against the block being extended or against the root of the blocktree if
        // there is no extending branch
        let (extending_seq_num, extending_timestamp) =
            if let Some(extended_block) = extending_blocks.last() {
                (extended_block.get_seq_num(), extended_block.get_timestamp())
            } else {
                (blocktree_root.seq_num, 0) //TODO: add timestamp to RootInfo
            };

        if block.get_seq_num() != extending_seq_num + SeqNum(1) {
            warn!(
                seq_num =? block.header().seq_num,
                round =? block.header().block_round,
                "block not coherent, doesn't equal parent_seq_num + 1"
            );
            return Err(BlockPolicyError::BlockNotCoherent);
        }

        if block.get_timestamp() <= extending_timestamp {
            warn!(
                seq_num =? block.header().seq_num,
                round =? block.header().block_round,
                ?extending_timestamp,
                block_timestamp =? block.get_timestamp(),
                "block not coherent, timestamp not monotonically increasing"
            );
            return Err(BlockPolicyError::TimestampError);
        }

        let expected_execution_results = self.get_expected_execution_results(
            block.get_seq_num(),
            extending_blocks.clone(),
            state_backend,
        )?;
        if block.get_execution_results() != &expected_execution_results {
            warn!(
                seq_num =? block.header().seq_num,
                round =? block.header().block_round,
                ?expected_execution_results,
                block_execution_results =? block.get_execution_results(),
                "block not coherent, execution result mismatch"
            );
            return Err(BlockPolicyError::ExecutionResultMismatch);
        }

        // verify base_fee fields
        let (base_fee, base_fee_trend, base_fee_moment) = self.compute_base_fee(&extending_blocks);
        if base_fee != block.header().base_fee {
            warn!(
                seq_num =? block.header().seq_num,
                round =? block.header().block_round,
                ?base_fee,
                block_base_fee =? block.header().base_fee,
                "block not coherent, base_fee mismatch"
            );
            return Err(BlockPolicyError::BaseFeeError);
        }
        if base_fee_trend != block.header().base_fee_trend {
            warn!(
                seq_num =? block.header().seq_num,
                round =? block.header().block_round,
                ?base_fee_trend,
                block_base_fee_trend =? block.header().base_fee_trend,
                "block not coherent, base_fee_trend mismatch"
            );
            return Err(BlockPolicyError::BaseFeeError);
        }
        if base_fee_moment != block.header().base_fee_moment {
            warn!(
                seq_num =? block.header().seq_num,
                round =? block.header().block_round,
                ?base_fee_moment,
                block_base_fee_moment =? block.header().base_fee_moment,
                "block not coherent, base_fee_moment mismatch"
            );
            return Err(BlockPolicyError::BaseFeeError);
        }

        let system_tx_signers = block.system_txns.iter().map(|txn| txn.signer());
        // TODO fix this unnecessary copy into a new vec to generate an owned Address
        let tx_signers = block
            .validated_txns
            .iter()
            .map(|txn| txn.signer())
            .chain(system_tx_signers)
            .collect_vec();
        // these must be updated as we go through txs in the block
        let mut account_nonces = self.get_account_base_nonces(
            block.get_seq_num(),
            state_backend,
            &extending_blocks,
            tx_signers.iter(),
        )?;
        // these must be updated as we go through txs in the block
        let mut account_balances = self.compute_account_base_balances(
            block.get_seq_num(),
            state_backend,
            Some(&extending_blocks),
            tx_signers.iter(),
        )?;

        for sys_txn in block.system_txns.iter() {
            let sys_txn_signer = sys_txn.signer();
            let sys_txn_nonce = sys_txn.nonce();

            let expected_nonce = account_nonces
                .get_mut(&sys_txn_signer)
                .expect("account_nonces should have been populated");

            if &sys_txn_nonce != expected_nonce {
                warn!(
                    seq_num =? block.header().seq_num,
                    round =? block.header().block_round,
                    "block not coherent, invalid nonce for system transaction"
                );
                return Err(BlockPolicyError::BlockNotCoherent);
            }
            *expected_nonce += 1;
        }

        for txn in block.validated_txns.iter() {
            let eth_address = txn.signer();
            let txn_nonce = txn.nonce();

            let expected_nonce = account_nonces
                .get_mut(&eth_address)
                .expect("account_nonces should have been populated");

            if &txn_nonce != expected_nonce {
                warn!(
                    seq_num =? block.header().seq_num,
                    round =? block.header().block_round,
                    "block not coherent, invalid nonce"
                );
                return Err(BlockPolicyError::BlockNotCoherent);
            }

            let account_balance = account_balances
                .get_mut(&eth_address)
                .expect("account_balances should have been populated");

            let txn_fee = compute_txn_max_value(txn);

            if *account_balance >= txn_fee {
                *account_balance -= txn_fee;
                *expected_nonce += 1;
                trace!(
                    "AccountBalance - check_coherency 2: \
                            updated balance: {:?} \
                            txn Fee: {:?} \
                            consensus block:seq num {:?} \
                            for address: {:?}",
                    account_balance,
                    txn_fee,
                    block.get_seq_num(),
                    eth_address
                );
            } else {
                trace!(
                    "AccountBalance - check_coherency 3: \
                            Not sufficient balance: {:?} \
                            Txn Fee: {:?} \
                            consensus block:seq num {:?} \
                            for address: {:?}",
                    account_balance,
                    txn_fee,
                    block.get_seq_num(),
                    eth_address
                );
                warn!(
                    seq_num =? block.header().seq_num,
                    round =? block.header().block_round,
                    "block not coherent, invalid balance"
                );
                return Err(BlockPolicyError::BlockNotCoherent);
            }
        }
        Ok(())
    }

    fn get_expected_execution_results(
        &self,
        block_seq_num: SeqNum,
        extending_blocks: Vec<&Self::ValidatedBlock>,
        state_backend: &SBT,
    ) -> Result<Vec<EthHeader>, StateBackendError> {
        if block_seq_num < self.execution_delay {
            return Ok(Vec::new());
        }
        let base_seq_num = block_seq_num - self.execution_delay;
        let block_index = self.get_block_index(&Some(&extending_blocks), &base_seq_num)?;

        let expected_execution_result = state_backend.get_execution_result(
            &block_index.block_id,
            &block_index.seq_num,
            block_index.is_finalized,
        )?;

        Ok(vec![expected_execution_result])
    }

    fn update_committed_block(&mut self, block: &Self::ValidatedBlock) {
        assert_eq!(block.get_seq_num(), self.last_commit + SeqNum(1));
        self.last_commit = block.get_seq_num();
        self.committed_cache.update_committed_block(block);
    }

    fn reset(&mut self, last_delay_committed_blocks: Vec<&Self::ValidatedBlock>) {
        self.committed_cache = CommittedBlkBuffer::new(self.committed_cache.size);
        for block in last_delay_committed_blocks {
            self.last_commit = block.get_seq_num();
            self.committed_cache.update_committed_block(block);
        }
    }
}

#[cfg(test)]
mod test {
    use alloy_consensus::{SignableTransaction, TxEip1559};
    use alloy_primitives::{Address, FixedBytes, PrimitiveSignature, TxKind, B256};
    use alloy_signer::SignerSync;
    use alloy_signer_local::PrivateKeySigner;
    use monad_crypto::NopSignature;
    use monad_testutil::signing::MockSignatures;
    use monad_types::{Hash, SeqNum};
    use proptest::{prelude::*, strategy::Just};

    use super::*;

    const BASE_FEE: u64 = 100_000_000_000;
    const BASE_FEE_TREND: u64 = 0;
    const BASE_FEE_MOMENT: u64 = 0;

    type SignatureType = NopSignature;
    type SignatureCollectionType = MockSignatures<SignatureType>;

    fn sign_tx(signature_hash: &FixedBytes<32>) -> PrimitiveSignature {
        let secret_key = B256::repeat_byte(0xAu8).to_string();
        let signer = &secret_key.parse::<PrivateKeySigner>().unwrap();
        signer.sign_hash_sync(signature_hash).unwrap()
    }

    #[test]
    fn test_compute_txn_fee() {
        // setup test addresses
        let address1 = Address(FixedBytes([0x11; 20]));
        let address2 = Address(FixedBytes([0x22; 20]));
        let address3 = Address(FixedBytes([0x33; 20]));
        let address4 = Address(FixedBytes([0x44; 20]));

        // add committed blocks to buffer
        let mut buffer = CommittedBlkBuffer::<SignatureType, SignatureCollectionType>::new(3);
        let block1 = CommittedBlock {
            block_id: BlockId(Hash(Default::default())),
            round: Round(0),
            epoch: Epoch(1),
            nonces: BlockAccountNonce {
                nonces: BTreeMap::from([(address1, 1), (address2, 1)]),
            },
            fees: BlockTxnFees {
                txn_fees: BTreeMap::from([
                    (address1, Balance::from(100)),
                    (address2, Balance::from(200)),
                ]),
            },
            base_fee: BASE_FEE,
            base_fee_trend: BASE_FEE_TREND,
            base_fee_moment: BASE_FEE_MOMENT,
            block_gas_usage: 0, // not used in this test
        };

        let block2 = CommittedBlock {
            block_id: BlockId(Hash(Default::default())),
            round: Round(0),
            epoch: Epoch(1),
            nonces: BlockAccountNonce {
                nonces: BTreeMap::from([(address1, 2), (address3, 1)]),
            },
            fees: BlockTxnFees {
                txn_fees: BTreeMap::from([
                    (address1, Balance::from(150)),
                    (address3, Balance::from(300)),
                ]),
            },
            base_fee: BASE_FEE,
            base_fee_trend: BASE_FEE_TREND,
            base_fee_moment: BASE_FEE_MOMENT,
            block_gas_usage: 0, // not used in this test
        };

        let block3 = CommittedBlock {
            block_id: BlockId(Hash(Default::default())),
            round: Round(0),
            epoch: Epoch(1),
            nonces: BlockAccountNonce {
                nonces: BTreeMap::from([(address2, 2), (address3, 2)]),
            },
            fees: BlockTxnFees {
                txn_fees: BTreeMap::from([
                    (address2, Balance::from(250)),
                    (address3, Balance::from(350)),
                ]),
            },
            base_fee: BASE_FEE,
            base_fee_trend: BASE_FEE_TREND,
            base_fee_moment: BASE_FEE_MOMENT,
            block_gas_usage: 0, // not used in this test
        };

        buffer.blocks.insert(SeqNum(1), block1);
        buffer.blocks.insert(SeqNum(2), block2);
        buffer.blocks.insert(SeqNum(3), block3);

        // test compute_txn_fee for different addresses and base sequence numbers
        assert_eq!(
            buffer.compute_txn_fee(SeqNum(0), &address1).txn_fee,
            U256::from(250)
        );
        assert_eq!(
            buffer.compute_txn_fee(SeqNum(1), &address1).txn_fee,
            U256::from(150)
        );
        assert_eq!(
            buffer.compute_txn_fee(SeqNum(2), &address1).txn_fee,
            U256::from(0)
        );

        assert_eq!(
            buffer.compute_txn_fee(SeqNum(0), &address2).txn_fee,
            U256::from(450)
        );
        assert_eq!(
            buffer.compute_txn_fee(SeqNum(0), &address3).txn_fee,
            U256::from(650)
        );

        // address that is not present in all blocks
        assert_eq!(
            buffer.compute_txn_fee(SeqNum(0), &address4).txn_fee,
            U256::from(0)
        );
    }

    #[test]
    fn test_compute_txn_fee_overflow() {
        // setup test addresses
        let address1 = Address(FixedBytes([0x11; 20]));
        let address2 = Address(FixedBytes([0x22; 20]));
        let address3 = Address(FixedBytes([0x33; 20]));

        // add committed blocks to buffer
        let mut buffer = CommittedBlkBuffer::<SignatureType, SignatureCollectionType>::new(3);
        let block1 = CommittedBlock {
            block_id: BlockId(Hash(Default::default())),
            round: Round(0),
            epoch: Epoch(1),
            nonces: BlockAccountNonce {
                nonces: BTreeMap::from([(address1, 1), (address2, 1)]),
            },
            fees: BlockTxnFees {
                txn_fees: BTreeMap::from([
                    (address1, U256::MAX - U256::from(1)),
                    (address2, U256::MAX),
                ]),
            },
            base_fee: BASE_FEE,
            base_fee_trend: BASE_FEE_TREND,
            base_fee_moment: BASE_FEE_MOMENT,
            block_gas_usage: 0, // not used in this test
        };

        let block2 = CommittedBlock {
            block_id: BlockId(Hash(Default::default())),
            round: Round(0),
            epoch: Epoch(1),
            nonces: BlockAccountNonce {
                nonces: BTreeMap::from([(address1, 2), (address3, 1)]),
            },
            fees: BlockTxnFees {
                txn_fees: BTreeMap::from([
                    (address1, U256::from(1)),
                    (address3, U256::MAX.div_ceil(U256::from(2))),
                ]),
            },
            base_fee: BASE_FEE,
            base_fee_trend: BASE_FEE_TREND,
            base_fee_moment: BASE_FEE_MOMENT,
            block_gas_usage: 0, // not used in this test
        };

        let block3 = CommittedBlock {
            block_id: BlockId(Hash(Default::default())),
            round: Round(0),
            epoch: Epoch(1),
            nonces: BlockAccountNonce {
                nonces: BTreeMap::from([(address2, 2), (address3, 2)]),
            },
            fees: BlockTxnFees {
                txn_fees: BTreeMap::from([
                    (address2, U256::MAX),
                    (address3, U256::MAX.div_ceil(U256::from(2)) + U256::from(1)),
                ]),
            },
            base_fee: BASE_FEE,
            base_fee_trend: BASE_FEE_TREND,
            base_fee_moment: BASE_FEE_MOMENT,
            block_gas_usage: 0, // not used in this test
        };

        buffer.blocks.insert(SeqNum(1), block1);
        buffer.blocks.insert(SeqNum(2), block2);
        buffer.blocks.insert(SeqNum(3), block3);

        // test compute_txn_fee for different addresses and base sequence numbers
        assert_eq!(
            buffer.compute_txn_fee(SeqNum(0), &address1).txn_fee,
            U256::MAX
        );
        assert_eq!(
            buffer.compute_txn_fee(SeqNum(1), &address1).txn_fee,
            U256::from(1)
        );
        assert_eq!(
            buffer.compute_txn_fee(SeqNum(2), &address1).txn_fee,
            U256::from(0)
        );

        assert_eq!(
            buffer.compute_txn_fee(SeqNum(0), &address2).txn_fee,
            U256::MAX
        );
        assert_eq!(
            buffer.compute_txn_fee(SeqNum(0), &address3).txn_fee,
            U256::MAX
        );
    }

    proptest! {
        #[test]
        fn test_compute_txn_max_value_no_overflow(
            gas_limit in 0u64..=u64::MAX,
            max_fee_per_gas in 0u128..=u128::MAX,
            value in prop_oneof![
                Just(U256::ZERO),
                Just(U256::MAX),
                any::<[u8; 32]>().prop_map(U256::from_be_bytes)
            ]
        ) {
            let tx = TxEip1559 {
                chain_id: 1337,
                nonce: 0,
                to: TxKind::Call(Address(FixedBytes([0x11; 20]))),
                max_fee_per_gas,
                max_priority_fee_per_gas: max_fee_per_gas,
                gas_limit,
                value,
                ..Default::default()
            };
            let signature = sign_tx(&tx.signature_hash());
            let tx_envelope = TxEnvelope::from(tx.into_signed(signature));

            let result = compute_txn_max_value(&tx_envelope);

            let gas_cost_u256 = U256::from(gas_limit).checked_mul(U256::from(max_fee_per_gas)).expect("overflow should not occur with U256overflow should not occur with U256");
            let expected_max_value = U256::from(value).saturating_add(gas_cost_u256);
            assert_eq!(result, expected_max_value);
        }
    }
    // TODO: check accounts for previous transactions in the block
}
