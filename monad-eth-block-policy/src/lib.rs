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

use std::{
    collections::BTreeMap,
    marker::PhantomData,
    ops::{Deref, Range, RangeFrom},
};

use alloy_consensus::{
    transaction::{Recovered, Transaction},
    TxEnvelope,
};
use alloy_primitives::{Address, TxHash, U256};
use itertools::Itertools;
use monad_consensus_types::{
    block::{
        AccountBalanceState, BlockPolicy, BlockPolicyBlockValidator,
        BlockPolicyBlockValidatorError, BlockPolicyError, ConsensusFullBlock, TxnFee, TxnFees,
    },
    checkpoint::RootInfo,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_types::{EthAccount, EthExecutionProtocol, EthHeader, BASE_FEE_PER_GAS};
use monad_state_backend::{StateBackend, StateBackendError};
use monad_system_calls::SystemTransaction;
use monad_types::{Balance, BlockId, Nonce, Round, SeqNum, GENESIS_BLOCK_ID, GENESIS_SEQ_NUM};
use monad_validator::signature_collection::SignatureCollection;
use sorted_vector_map::SortedVectorMap;
use tracing::{debug, trace, warn};

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
    let gas_cost = compute_txn_max_gas_cost(txn);
    txn_value.saturating_add(gas_cost)
}

pub fn compute_txn_max_gas_cost(txn: &TxEnvelope) -> U256 {
    let gas_limit = U256::from(txn.gas_limit());
    let max_fee = U256::from(txn.max_fee_per_gas());
    let priority_fee = U256::from(txn.max_priority_fee_per_gas().unwrap_or(0));
    let base_fee = U256::from(BASE_FEE_PER_GAS); //TODO: Use actual value once implemented
    let gas_bid = max_fee.min(base_fee.saturating_add(priority_fee));
    gas_limit.checked_mul(gas_bid).expect("no overflow")
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
    pub txn_fees: TxnFees,
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
struct BlockTxnFeeStates {
    txn_fees: TxnFees,
}

impl BlockTxnFeeStates {
    fn get(&self, eth_address: &Address) -> Option<TxnFee> {
        self.txn_fees.get(eth_address).cloned()
    }
}

#[derive(Debug)]
struct CommittedBlock {
    block_id: BlockId,
    round: Round,
    seq_num: SeqNum,
    nonces: BlockAccountNonce,
    fees: BlockTxnFeeStates,
}

#[derive(Debug)]
struct CommittedBlkBuffer<ST, SCT> {
    blocks: SortedVectorMap<SeqNum, CommittedBlock>,
    min_buffer_size: usize, // should be 2 * execution delay

    _phantom: PhantomData<(ST, SCT)>,
}

impl<ST, SCT> CommittedBlkBuffer<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn new(min_buffer_size: usize) -> Self {
        Self {
            blocks: Default::default(),
            min_buffer_size,

            _phantom: Default::default(),
        }
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

    fn update_account_balance(
        &self,
        account_balance: &mut AccountBalanceState,
        eth_address: &Address,
        execution_delay: SeqNum,
        emptying_txn_check_block_range: Range<SeqNum>,
        reserve_balance_check_block_range: RangeFrom<SeqNum>,
    ) -> Result<SeqNum, BlockPolicyError> {
        trace!(
            ?emptying_txn_check_block_range,
            ?reserve_balance_check_block_range,
            ?account_balance,
            ?eth_address,
            "before update_account_balance"
        );

        let mut next_validate = emptying_txn_check_block_range.start;
        for (_, block) in self.blocks.range(emptying_txn_check_block_range) {
            assert_eq!(next_validate, block.seq_num);
            if block.fees.get(eth_address).is_some()
                && account_balance.block_seqnum_of_latest_txn < block.seq_num
            {
                account_balance.block_seqnum_of_latest_txn = block.seq_num;
            }
            next_validate += SeqNum(1);
        }

        for (_, block) in self.blocks.range(reserve_balance_check_block_range) {
            assert_eq!(next_validate, block.seq_num);
            if let Some(block_txn_fees) = block.fees.get(eth_address) {
                let mut validator =
                    EthBlockPolicyBlockValidator::new(block.seq_num, execution_delay)?;
                trace!(
                    "applying fees for block {:?}, curr acc balance: {:?}",
                    block.seq_num,
                    account_balance
                );
                validator.try_apply_block_fees(account_balance, &block_txn_fees, eth_address)?;
            }
            next_validate += SeqNum(1);
        }

        trace!(
            ?account_balance,
            ?eth_address,
            "after update_account_balance"
        );

        Ok(next_validate)
    }

    fn update_committed_block(&mut self, block: &EthValidatedBlock<ST, SCT>) {
        let block_number = block.get_seq_num();
        debug!(?block_number, ?block.txn_fees, "update_committed_block");
        if let Some((&last_block_num, _)) = self.blocks.last_key_value() {
            assert_eq!(last_block_num + SeqNum(1), block_number);
        }

        let current_size = self.blocks.len();

        if current_size >= self.min_buffer_size.saturating_mul(2) {
            let (&first_block_num, _) = self.blocks.first_key_value().expect("txns non-empty");
            let divider =
                first_block_num + SeqNum(current_size as u64 - self.min_buffer_size as u64);

            // TODO: revisit once perf implications are understood
            self.blocks = self.blocks.split_off(&divider);
            assert_eq!(
                *self.blocks.last_key_value().expect("non-empty").0 + SeqNum(1),
                block_number
            );
            assert!(self.blocks.len() >= self.min_buffer_size);
        }

        assert!(self
            .blocks
            .insert(
                block_number,
                CommittedBlock {
                    block_id: block.get_id(),
                    round: block.get_block_round(),
                    seq_num: block.get_seq_num(),
                    nonces: BlockAccountNonce {
                        nonces: block.get_account_nonces(),
                    },
                    fees: BlockTxnFeeStates {
                        txn_fees: block.txn_fees.clone()
                    }
                },
            )
            .is_none());
    }
}

pub struct EthBlockPolicyBlockValidator {
    block_seq_num: SeqNum,
    execution_delay: SeqNum,
}

fn is_possibly_emptying_transaction(
    block_seq_num_of_curr_txn: SeqNum,
    block_seqnum_of_latest_txn: SeqNum,
    execution_delay: SeqNum,
) -> bool {
    // txn T is emptying if there is no "prior txn" i.e. a txn from the same sender sent from block P so that P >= block_number(T) - k + 1.
    let blocks_since_latest_txn =
        block_seq_num_of_curr_txn.max(block_seqnum_of_latest_txn) - block_seqnum_of_latest_txn;
    blocks_since_latest_txn > execution_delay - SeqNum(1)
}

impl BlockPolicyBlockValidator for EthBlockPolicyBlockValidator
where
    Self: Sized,
{
    type Transaction = Recovered<TxEnvelope>;

    fn new(block_seq_num: SeqNum, execution_delay: SeqNum) -> Result<Self, BlockPolicyError> {
        Ok(Self {
            block_seq_num,
            execution_delay,
        })
    }

    fn try_apply_block_fees(
        &mut self,
        account_balance: &mut AccountBalanceState,
        block_txn_fees: &TxnFee,
        eth_address: &Address,
    ) -> Result<(), BlockPolicyError> {
        let has_emptying_transaction = is_possibly_emptying_transaction(
            self.block_seq_num,
            account_balance.block_seqnum_of_latest_txn,
            self.execution_delay,
        );

        let mut block_gas_cost = block_txn_fees.max_gas_cost;
        if has_emptying_transaction {
            if account_balance.balance < block_txn_fees.first_txn_gas {
                debug!(
                    "Block with insufficient balance: {:?} \
                            first txn value {:?} \
                            first txn gas {:?} \
                            block seq_num {:?} \
                            address: {:?}",
                    account_balance,
                    block_txn_fees.first_txn_value,
                    block_txn_fees.first_txn_gas,
                    self.block_seq_num,
                    eth_address,
                );
                return Err(BlockPolicyError::BlockPolicyBlockValidatorError(
                    BlockPolicyBlockValidatorError::InsufficientBalance,
                ));
            }
            let first_txn_cost = block_txn_fees
                .first_txn_value
                .saturating_add(block_txn_fees.first_txn_gas);
            let estimated_balance = account_balance.balance.saturating_sub(first_txn_cost);

            account_balance.remaining_reserve_balance =
                estimated_balance.min(account_balance.max_reserve_balance);
            account_balance.balance = estimated_balance;

            debug!(
                "Block has emptying txn. updated balance: {:?} \
                        first txn value {:?} \
                        first txn gas {:?} \
                        block seq_num {:?} \
                        address: {:?}",
                account_balance,
                block_txn_fees.first_txn_value,
                block_txn_fees.first_txn_gas,
                self.block_seq_num,
                eth_address,
            );
        } else {
            block_gas_cost = block_txn_fees
                .max_gas_cost
                .saturating_add(block_txn_fees.first_txn_gas);
        }

        if account_balance.remaining_reserve_balance < block_gas_cost {
            debug!(
                "Block with insufficient reserve balance: {:?} \
                            max gas cost {:?} \
                            block seq_num {:?} \
                            address: {:?}",
                account_balance, block_gas_cost, self.block_seq_num, eth_address,
            );
            return Err(BlockPolicyError::BlockPolicyBlockValidatorError(
                BlockPolicyBlockValidatorError::InsufficientReserveBalance,
            ));
        }
        account_balance.remaining_reserve_balance = account_balance
            .remaining_reserve_balance
            .saturating_sub(block_gas_cost);
        account_balance.block_seqnum_of_latest_txn = self.block_seq_num;

        debug!(
            ?account_balance,
            ?self.block_seq_num,
            ?eth_address,
            "try_apply_block_fees updated balance state",
        );
        Ok(())
    }

    fn try_add_transaction(
        &mut self,
        account_balances: &mut BTreeMap<Address, AccountBalanceState>,
        txn: &Self::Transaction,
    ) -> Result<(), BlockPolicyError> {
        let eth_address = txn.signer();

        let maybe_account_balance = account_balances.get_mut(&eth_address);

        let Some(account_balance) = maybe_account_balance else {
            warn!(
                seq_num =?self.block_seq_num,
                ?eth_address,
                "account balance have not been populated"
            );
            return Err(BlockPolicyError::BlockPolicyBlockValidatorError(
                BlockPolicyBlockValidatorError::AccountBalanceMissing,
            ));
        };

        let is_emptying_transaction = is_possibly_emptying_transaction(
            self.block_seq_num,
            account_balance.block_seqnum_of_latest_txn,
            self.execution_delay,
        );

        // if an account for txn T is not delegated and has no prior txns, then T can charge into reserve.
        if is_emptying_transaction {
            let txn_max_gas = compute_txn_max_gas_cost(txn);
            if account_balance.balance < txn_max_gas {
                warn!(
                    seq_num =?self.block_seq_num,
                    ?account_balance,
                    ?txn_max_gas,
                    "Incoherent block with insufficient balance"
                );
                return Err(BlockPolicyError::BlockPolicyBlockValidatorError(
                    BlockPolicyBlockValidatorError::InsufficientBalance,
                ));
            }

            let txn_max_cost = compute_txn_max_value(txn);
            let estimated_balance = account_balance.balance.saturating_sub(txn_max_cost);
            let reserve_balance = account_balance.max_reserve_balance.min(estimated_balance);

            debug!(
                "New emptying txn. balance: {:?} \
                    txn_max_cost {:?} \
                    txn_max_gas {:?} \
                    estimated_balance {:?} \
                    new reserve balance {:?} \
                    block seq_num {:?} \
                    address: {:?}",
                account_balance,
                txn_max_cost,
                txn_max_gas,
                estimated_balance,
                reserve_balance,
                self.block_seq_num,
                eth_address,
            );
            account_balance.balance = estimated_balance;
            account_balance.remaining_reserve_balance = reserve_balance;
            account_balance.block_seqnum_of_latest_txn = self.block_seq_num;
        } else {
            let txn_max_gas = compute_txn_max_gas_cost(txn);
            if account_balance.remaining_reserve_balance < txn_max_gas {
                debug!(
                    seq_num =?self.block_seq_num,
                    ?account_balance,
                    ?txn_max_gas,
                    ?txn,
                    "Txn can not be accepted insufficient reserve balance"
                );
                return Err(BlockPolicyError::BlockPolicyBlockValidatorError(
                    BlockPolicyBlockValidatorError::InsufficientReserveBalance,
                ));
            }
            let reserve_balance = account_balance
                .remaining_reserve_balance
                .saturating_sub(txn_max_gas);

            account_balance.remaining_reserve_balance = reserve_balance;
            account_balance.block_seqnum_of_latest_txn = self.block_seq_num;
        }

        Ok(())
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

    pub execution_delay: SeqNum,

    /// Chain ID
    chain_id: u64,

    max_reserve_balance: Balance,
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
        max_reserve_balance: u128,
    ) -> Self {
        let cache_max_size = execution_delay.saturating_mul(2);
        Self {
            // Needs to be at least 2 * execution_delay to detect emptying transactions
            committed_cache: CommittedBlkBuffer::new((cache_max_size) as usize),
            last_commit,
            execution_delay: SeqNum(execution_delay),
            chain_id,
            max_reserve_balance: Balance::from(max_reserve_balance),
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
    ) -> Result<BTreeMap<Address, AccountBalanceState>, BlockPolicyError>
    where
        SCT: SignatureCollection,
    {
        // calculation correct only if GENESIS_SEQ_NUM == 0
        assert_eq!(GENESIS_SEQ_NUM, SeqNum(0));
        let base_seq_num =
            (consensus_block_seq_num).max(self.execution_delay) - self.execution_delay;

        debug!(
            ?base_seq_num,
            ?consensus_block_seq_num,
            "compute_account_base_balances"
        );
        let addresses = addresses.unique().collect_vec();
        let account_balances = self
            .get_account_statuses(
                state_backend,
                &extending_blocks,
                addresses.iter().copied(),
                &base_seq_num,
            )?
            .into_iter()
            .map(|maybe_status| {
                maybe_status.map_or(
                    AccountBalanceState::new(self.max_reserve_balance),
                    |status| {
                        AccountBalanceState {
                            balance: status.balance,
                            remaining_reserve_balance: status.balance.min(self.max_reserve_balance),
                            max_reserve_balance: self.max_reserve_balance,
                            block_seqnum_of_latest_txn: base_seq_num, // most pessimistic assumption
                        }
                    },
                )
            })
            .collect_vec();

        let account_balances: Result<BTreeMap<Address, AccountBalanceState>, BlockPolicyError> =
            addresses
                .into_iter()
                .cloned()
                .zip_eq(account_balances)
                .map(|(address, mut balance_state)| {
                    // N - k + 1
                    let reserve_balance_check_start = base_seq_num + SeqNum(1);
                    // N - 2k + 2
                    let emptying_txn_check_start = (reserve_balance_check_start + SeqNum(1))
                        .max(self.execution_delay)
                        - self.execution_delay;

                    // N - 2k + 2 (inclusive) to N - k + 1 (non inclusive)
                    let emptying_txn_check_block_range =
                        emptying_txn_check_start..reserve_balance_check_start;
                    // N - k + 1 (inclusive) to N (non inclusive)
                    let reserve_balance_check_block_range = reserve_balance_check_start..;

                    if emptying_txn_check_start > GENESIS_SEQ_NUM {
                        balance_state.block_seqnum_of_latest_txn =
                            emptying_txn_check_start - SeqNum(1);
                    }

                    // check for emptying txs and reserve balance in committed blocks
                    let mut next_validate = self.committed_cache.update_account_balance(
                        &mut balance_state,
                        &address,
                        self.execution_delay,
                        emptying_txn_check_block_range,
                        reserve_balance_check_block_range,
                    )?;

                    // check for emptying txs and reserve balance in extending blocks
                    if let Some(blocks) = extending_blocks {
                        // handle the case where base_seq_num is a pending block
                        let next_blocks = blocks
                            .iter()
                            .skip_while(move |block| block.get_seq_num() < next_validate);

                        for extending_block in next_blocks {
                            assert_eq!(next_validate, extending_block.get_seq_num());
                            if let Some(txn_fee) = extending_block.txn_fees.get(&address) {
                                // if still within check emptying range, update latest tx seq num
                                // otherwise check for reserve balance
                                if next_validate < reserve_balance_check_start {
                                    if balance_state.block_seqnum_of_latest_txn < next_validate {
                                        balance_state.block_seqnum_of_latest_txn =
                                            extending_block.get_seq_num();
                                    }
                                } else {
                                    let mut validator = EthBlockPolicyBlockValidator::new(
                                        extending_block.get_seq_num(),
                                        self.execution_delay,
                                    )?;

                                    validator.try_apply_block_fees(
                                        &mut balance_state,
                                        txn_fee,
                                        &address,
                                    )?;
                                }
                            }
                            next_validate += SeqNum(1);
                        }
                    }

                    Ok((address, balance_state))
                })
                .collect();
        account_balances
    }

    pub fn get_chain_id(&self) -> u64 {
        self.chain_id
    }

    pub fn max_reserve_balance(&self) -> Balance {
        self.max_reserve_balance
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

        let mut validator =
            EthBlockPolicyBlockValidator::new(block.get_seq_num(), self.execution_delay)?;

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

            validator.try_add_transaction(&mut account_balances, txn)?;
            *expected_nonce += 1;
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
        self.committed_cache = CommittedBlkBuffer::new(self.committed_cache.min_buffer_size);
        for block in last_delay_committed_blocks {
            self.last_commit = block.get_seq_num();
            self.committed_cache.update_committed_block(block);
        }
    }
}

#[cfg(test)]
mod test {
    use alloy_consensus::{SignableTransaction, TxEip1559};
    use alloy_primitives::{hex, Address, FixedBytes, PrimitiveSignature, TxKind, B256};
    use alloy_signer::SignerSync;
    use alloy_signer_local::PrivateKeySigner;
    use monad_crypto::NopSignature;
    use monad_eth_testutil::{
        generate_consensus_test_block, make_eip1559_tx_with_value, recover_tx,
    };
    use monad_eth_types::BASE_FEE_PER_GAS;
    use monad_state_backend::NopStateBackend;
    use monad_testutil::signing::MockSignatures;
    use monad_types::{Hash, SeqNum};
    use proptest::{prelude::*, strategy::Just};
    use rstest::*;
    use test_case::test_case;

    use super::*;

    type SignatureType = NopSignature;
    type SignatureCollectionType = MockSignatures<SignatureType>;
    type StateBackendType = NopStateBackend;

    const RESERVE_BALANCE: u128 = 1_000_000_000_000_000_000;
    const EXEC_DELAY: SeqNum = SeqNum(3);
    const S1: B256 = B256::new(hex!(
        "0ed2e19e3aca1a321349f295837988e9c6f95d4a6fc54cfab6befd5ee82662ad"
    ));
    const ONE_ETHER: u128 = 1_000_000_000_000_000_000;
    const HALF_ETHER: u128 = 500_000_000_000_000_000;

    fn sign_tx(signature_hash: &FixedBytes<32>) -> PrimitiveSignature {
        let secret_key = B256::repeat_byte(0xAu8).to_string();
        let signer = &secret_key.parse::<PrivateKeySigner>().unwrap();
        signer.sign_hash_sync(signature_hash).unwrap()
    }

    fn make_test_tx(
        gas_limit: u64,
        value: u128,
        nonce: u64,
        signer: FixedBytes<32>,
    ) -> Recovered<TxEnvelope> {
        recover_tx(make_eip1559_tx_with_value(
            signer,
            value,
            BASE_FEE_PER_GAS as u128,
            0, // priority fee
            gas_limit,
            nonce,
            0, // input length
        ))
    }

    fn make_test_block(
        round: Round,
        seq_num: SeqNum,
        txs: Vec<Recovered<TxEnvelope>>,
    ) -> EthValidatedBlock<NopSignature, MockSignatures<NopSignature>> {
        let consensus_test_block = generate_consensus_test_block(round, seq_num, txs);
        EthValidatedBlock {
            block: consensus_test_block.block,
            system_txns: Vec::new(),
            validated_txns: consensus_test_block.validated_txns,
            nonces: consensus_test_block.nonces,
            txn_fees: consensus_test_block.txn_fees,
        }
    }

    fn reserve_balance_coherency(
        block_policy: EthBlockPolicy<SignatureType, SignatureCollectionType>,
        incoming_block: EthValidatedBlock<SignatureType, SignatureCollectionType>,
        extending_blocks: Vec<&EthValidatedBlock<SignatureType, SignatureCollectionType>>,
        state_backend: &impl StateBackend<SignatureType, SignatureCollectionType>,
        addresses: Vec<Address>,
    ) -> Result<(), BlockPolicyError> {
        let mut account_balances = block_policy.compute_account_base_balances(
            incoming_block.get_seq_num(),
            state_backend,
            Some(&extending_blocks),
            addresses.iter(),
        )?;

        let mut validator = EthBlockPolicyBlockValidator::new(
            incoming_block.get_seq_num(),
            block_policy.execution_delay,
        )?;

        for txn in incoming_block.validated_txns.iter() {
            let eth_address = txn.signer();
            let txn_nonce = txn.nonce();

            validator.try_add_transaction(&mut account_balances, txn)?;
        }

        Ok(())
    }

    fn setup_block_policy_with_txs(
        txs: BTreeMap<u64, Vec<Recovered<TxEnvelope>>>,
        signers: Vec<Address>,
        state_backend: &impl StateBackend<SignatureType, SignatureCollectionType>,
        num_committed_blocks: usize,
    ) -> Result<(), BlockPolicyError> {
        let mut block_policy = EthBlockPolicy::<SignatureType, SignatureCollectionType>::new(
            SeqNum(17),
            EXEC_DELAY.0,
            1337,
            RESERVE_BALANCE,
        );

        // Build 5 sequential blocks (n-4 .. n)
        let seq_num = 18;
        let mut blocks = Vec::new();
        for offset in 0..=4 {
            let seq = seq_num + offset;
            let txs = txs.get(&offset).cloned().unwrap_or_default();
            let block = make_test_block(Round(1), SeqNum(seq), txs);
            blocks.push(block);
        }

        // Commit blocks
        for block in &blocks[0..num_committed_blocks] {
            BlockPolicy::<_, _, _, StateBackendType>::update_committed_block(
                &mut block_policy,
                block,
            );
        }

        // Last block is incoming_block
        // Remaining ones in the middle are extending_block
        let incoming_block = blocks[4].clone();
        let extending_blocks = blocks[num_committed_blocks..4].iter().collect();

        reserve_balance_coherency(
            block_policy,
            incoming_block,
            extending_blocks,
            state_backend,
            signers,
        )
    }

    #[test_case(3; "three committed blocks, one extending block")]
    #[test_case(0; "no committed blocks, four extending block")]
    fn test_check_reserve_balance_coherency(num_committed_blocks: usize) {
        //////////////////////////////////////////////////////////////////
        // Case1: Single emptying transaction                          ///
        //////////////////////////////////////////////////////////////////

        let tx1 = make_test_tx(50000, HALF_ETHER, 0, S1);
        let signer = tx1.signer();
        let txs = BTreeMap::from([(4, vec![tx1])]); // tx in block n

        // balance of signer at block n-3
        // minimum balance required is gas limit * gas bid
        let gas_cost = 50000 * BASE_FEE_PER_GAS as u128;
        let state_backend = NopStateBackend {
            balances: BTreeMap::from([(signer, U256::from(gas_cost))]),
            ..Default::default()
        };

        let result = setup_block_policy_with_txs(
            txs.clone(),
            vec![signer],
            &state_backend,
            num_committed_blocks,
        );
        assert!(result.is_ok(), "Block coherency check failed: {:?}", result);

        // should return error if fall below minimum balance
        let state_backend = NopStateBackend {
            balances: BTreeMap::from([(signer, U256::from(gas_cost - 1))]),
            ..Default::default()
        };
        let result =
            setup_block_policy_with_txs(txs, vec![signer], &state_backend, num_committed_blocks);
        assert!(
            result.is_err(),
            "Block coherency check should have failed: {:?}",
            result
        );

        ///////////////////////////////////////////////////////////////////////////////////
        // Case2: Emptying transaction + another transaction in same block              ///
        ///////////////////////////////////////////////////////////////////////////////////

        // first tx dips into reserve balance, second tx has gas cost less than remaining reserve balance
        let tx1 = make_test_tx(50000, ONE_ETHER, 0, S1);
        let tx2 = make_test_tx(50000, HALF_ETHER, 1, S1);
        let txs = BTreeMap::from([(4, vec![tx1, tx2])]); // txs in block n

        // balance of signer at block n-3
        let state_backend = NopStateBackend {
            balances: BTreeMap::from([(signer, U256::from(ONE_ETHER + HALF_ETHER))]),
            ..Default::default()
        };

        let result =
            setup_block_policy_with_txs(txs, vec![signer], &state_backend, num_committed_blocks);
        assert!(result.is_ok(), "Block coherency check failed: {:?}", result);

        // first tx dips into reserve balance, second tx has gas cost more than remaining reserve balance
        let tx1 = make_test_tx(50000, ONE_ETHER, 0, S1);
        let tx2 = make_test_tx(50000, HALF_ETHER, 1, S1);
        let txs = BTreeMap::from([(4, vec![tx1, tx2])]); // txs in block n

        // balance of signer at block n-3
        let gas_cost = 50000 * BASE_FEE_PER_GAS as u128;
        let balance = ONE_ETHER + (2 * gas_cost) - 1;
        let state_backend = NopStateBackend {
            balances: BTreeMap::from([(signer, U256::from(balance))]),
            ..Default::default()
        };

        let result =
            setup_block_policy_with_txs(txs, vec![signer], &state_backend, num_committed_blocks);
        assert!(
            result.is_err(),
            "Block coherency check should have failed: {:?}",
            result
        );

        // first tx doesn't dip into reserve balance, second tx has max reserve balance to spend from
        let tx1 = make_test_tx(50000, 0, 0, S1);
        let tx2 = make_test_tx(20_000_000, HALF_ETHER, 1, S1);
        let txs = BTreeMap::from([(4, vec![tx1, tx2.clone()])]); // txs in block n

        // balance of signer at block n-3
        assert_eq!(
            tx2.gas_limit() as u128 * BASE_FEE_PER_GAS as u128,
            RESERVE_BALANCE
        );
        let first_tx_gas_cost = 50000 * BASE_FEE_PER_GAS as u128;
        let second_tx_gas_cost = RESERVE_BALANCE;
        let balance = first_tx_gas_cost + second_tx_gas_cost;
        let state_backend = NopStateBackend {
            balances: BTreeMap::from([(signer, U256::from(balance))]),
            ..Default::default()
        };

        let result =
            setup_block_policy_with_txs(txs, vec![signer], &state_backend, num_committed_blocks);
        assert!(result.is_ok(), "Block coherency check failed: {:?}", result);

        ///////////////////////////////////////////////////////////////////////////////////
        // Case3: Emptying transaction + another transaction in different block         ///
        ///////////////////////////////////////////////////////////////////////////////////

        // first tx dips into reserve balance, second tx has gas cost less than remaining reserve balance
        let tx1 = make_test_tx(50000, ONE_ETHER, 0, S1);
        let tx2 = make_test_tx(50000, HALF_ETHER, 1, S1);
        // first tx in block n-2, second tx in block n
        let txs = BTreeMap::from([(2, vec![tx1]), (4, vec![tx2])]);

        // balance of signer at block n-3
        let state_backend = NopStateBackend {
            balances: BTreeMap::from([(signer, U256::from(ONE_ETHER + HALF_ETHER))]),
            ..Default::default()
        };

        let result =
            setup_block_policy_with_txs(txs, vec![signer], &state_backend, num_committed_blocks);
        assert!(result.is_ok(), "Block coherency check failed: {:?}", result);

        // first tx dips into reserve balance, second tx has gas cost more than remaining reserve balance
        let tx1 = make_test_tx(50000, ONE_ETHER, 0, S1);
        let tx2 = make_test_tx(50000, HALF_ETHER, 1, S1);
        // first tx in block n-2, second tx in block n
        let txs = BTreeMap::from([(2, vec![tx1]), (4, vec![tx2])]);

        // balance of signer at block n-3
        let gas_cost = 50000 * BASE_FEE_PER_GAS as u128;
        let balance = ONE_ETHER + (2 * gas_cost) - 1;
        let state_backend = NopStateBackend {
            balances: BTreeMap::from([(signer, U256::from(balance))]),
            ..Default::default()
        };

        let result =
            setup_block_policy_with_txs(txs, vec![signer], &state_backend, num_committed_blocks);
        assert!(
            result.is_err(),
            "Block coherency check should have failed: {:?}",
            result
        );

        // first tx doesn't dip into reserve balance, second tx has max reserve balance to spend from
        let tx1 = make_test_tx(50000, 0, 0, S1);
        let tx2 = make_test_tx(20_000_000, HALF_ETHER, 1, S1);
        // first tx in block n-2, second tx in block n
        let txs = BTreeMap::from([(2, vec![tx1]), (4, vec![tx2.clone()])]);

        // balance of signer at block n-3
        assert_eq!(
            tx2.gas_limit() as u128 * BASE_FEE_PER_GAS as u128,
            RESERVE_BALANCE
        );
        let first_tx_gas_cost = 50000 * BASE_FEE_PER_GAS as u128;
        let second_tx_gas_cost = RESERVE_BALANCE;
        let balance = first_tx_gas_cost + second_tx_gas_cost;
        let state_backend = NopStateBackend {
            balances: BTreeMap::from([(signer, U256::from(balance))]),
            ..Default::default()
        };

        let result =
            setup_block_policy_with_txs(txs, vec![signer], &state_backend, num_committed_blocks);
        assert!(result.is_ok(), "Block coherency check failed: {:?}", result);

        ///////////////////////////////////////////////////////////////////////////////////
        // Case4: Non-emptying transaction + another transaction in different block     ///
        ///////////////////////////////////////////////////////////////////////////////////

        // only gas cost of transactions are taken into account, txn value is not included when calculating reserve balance
        let tx1 = make_test_tx(50000, ONE_ETHER, 0, S1);
        let tx2 = make_test_tx(50000, HALF_ETHER, 1, S1);
        let tx3 = make_test_tx(50000, HALF_ETHER, 2, S1);
        // first tx in block n-3, second tx in block n-2, third tx in block n
        let txs = BTreeMap::from([(1, vec![tx1]), (2, vec![tx2]), (4, vec![tx3])]);

        // balance of signer at block n-3
        let gas_cost = 50000 * 2 * BASE_FEE_PER_GAS as u128;
        let state_backend = NopStateBackend {
            balances: BTreeMap::from([(signer, U256::from(gas_cost))]),
            ..Default::default()
        };

        let result =
            setup_block_policy_with_txs(txs, vec![signer], &state_backend, num_committed_blocks);
        assert!(result.is_ok(), "Block coherency check failed: {:?}", result);

        // transactions exceed reserve balance
        let tx1 = make_test_tx(50000, ONE_ETHER, 0, S1);
        let tx2 = make_test_tx(50000, HALF_ETHER, 1, S1);
        let tx3 = make_test_tx(50001, HALF_ETHER, 2, S1);
        // first tx in block n-3, second tx in block n-2, third tx in block n
        let txs = BTreeMap::from([(1, vec![tx1]), (2, vec![tx2]), (4, vec![tx3])]);

        // balance of signer at block n-3
        let gas_cost = 50000 * 2 * BASE_FEE_PER_GAS as u128;
        let state_backend = NopStateBackend {
            balances: BTreeMap::from([(signer, U256::from(gas_cost))]),
            ..Default::default()
        };

        let result =
            setup_block_policy_with_txs(txs, vec![signer], &state_backend, num_committed_blocks);
        assert!(
            result.is_err(),
            "Block coherency check should have failed: {:?}",
            result
        );
    }

    #[test]
    fn test_compute_account_balance_state() {
        // setup test addresses
        let address1 = Address(FixedBytes([0x11; 20]));
        let address2 = Address(FixedBytes([0x22; 20]));
        let address3 = Address(FixedBytes([0x33; 20]));

        let max_reserve_balance = Balance::from(RESERVE_BALANCE);

        // add committed blocks to buffer
        let mut buffer = CommittedBlkBuffer::<SignatureType, SignatureCollectionType>::new(3);
        let block1 = CommittedBlock {
            block_id: BlockId(Hash(Default::default())),
            round: Round(0),
            seq_num: SeqNum(1),
            nonces: BlockAccountNonce {
                nonces: BTreeMap::from([(address1, 1), (address2, 1)]),
            },
            fees: BlockTxnFeeStates {
                txn_fees: BTreeMap::from([
                    (
                        address1,
                        TxnFee {
                            first_txn_value: Balance::from(100),
                            first_txn_gas: Balance::from(10),
                            max_gas_cost: Balance::from(90),
                        },
                    ),
                    (
                        address2,
                        TxnFee {
                            first_txn_value: Balance::from(200),
                            first_txn_gas: Balance::from(10),
                            max_gas_cost: Balance::from(190),
                        },
                    ),
                ]),
            },
        };

        let block2 = CommittedBlock {
            block_id: BlockId(Hash(Default::default())),
            round: Round(0),
            seq_num: SeqNum(2),
            nonces: BlockAccountNonce {
                nonces: BTreeMap::from([(address1, 2), (address3, 1)]),
            },
            fees: BlockTxnFeeStates {
                txn_fees: BTreeMap::from([
                    (
                        address1,
                        TxnFee {
                            first_txn_value: Balance::from(150),
                            first_txn_gas: Balance::from(10),
                            max_gas_cost: Balance::from(140),
                        },
                    ),
                    (
                        address3,
                        TxnFee {
                            first_txn_value: Balance::from(300),
                            first_txn_gas: Balance::from(10),
                            max_gas_cost: Balance::from(290),
                        },
                    ),
                ]),
            },
        };

        let block3 = CommittedBlock {
            block_id: BlockId(Hash(Default::default())),
            round: Round(0),
            seq_num: SeqNum(3),
            nonces: BlockAccountNonce {
                nonces: BTreeMap::from([(address2, 2), (address3, 2)]),
            },
            fees: BlockTxnFeeStates {
                txn_fees: BTreeMap::from([
                    (
                        address2,
                        TxnFee {
                            first_txn_value: Balance::from(250),
                            first_txn_gas: Balance::from(10),
                            max_gas_cost: Balance::from(240),
                        },
                    ),
                    (
                        address3,
                        TxnFee {
                            first_txn_value: Balance::from(350),
                            first_txn_gas: Balance::from(10),
                            max_gas_cost: Balance::from(0),
                        },
                    ),
                ]),
            },
        };

        buffer.blocks.insert(SeqNum(1), block1);
        buffer.blocks.insert(SeqNum(2), block2);
        buffer.blocks.insert(SeqNum(3), block3);

        // committed blocks are out of range for emptying and reserve balance check
        let mut account_balance_address_1 = AccountBalanceState {
            balance: Balance::from(250),
            block_seqnum_of_latest_txn: GENESIS_SEQ_NUM,
            remaining_reserve_balance: Balance::from(250),
            max_reserve_balance,
        };
        let res = buffer.update_account_balance(
            &mut account_balance_address_1,
            &address1,
            EXEC_DELAY,
            SeqNum(4)..SeqNum(5),
            SeqNum(5)..,
        );
        assert!(res.is_ok());

        let emptying_txn_check_block_range = SeqNum(2)..SeqNum(3);
        let reserve_balance_check_block_range = SeqNum(3)..;

        let mut account_balance_address_2 = AccountBalanceState {
            balance: Balance::from(250),
            block_seqnum_of_latest_txn: GENESIS_SEQ_NUM,
            remaining_reserve_balance: Balance::from(250),
            max_reserve_balance,
        };
        let res = buffer.update_account_balance(
            &mut account_balance_address_2,
            &address2,
            EXEC_DELAY,
            emptying_txn_check_block_range.clone(),
            reserve_balance_check_block_range.clone(),
        );
        // no transaction in block2 (emptying transaction check)
        // gas cost + value more than balance in block3 (reserve balance check)
        assert_eq!(
            res,
            Err(BlockPolicyError::BlockPolicyBlockValidatorError(
                BlockPolicyBlockValidatorError::InsufficientReserveBalance
            ))
        );

        let mut account_balance_address_3 = AccountBalanceState {
            balance: Balance::from(250),
            block_seqnum_of_latest_txn: GENESIS_SEQ_NUM,
            remaining_reserve_balance: Balance::from(250),
            max_reserve_balance,
        };
        let res = buffer.update_account_balance(
            &mut account_balance_address_3,
            &address3,
            EXEC_DELAY,
            emptying_txn_check_block_range,
            reserve_balance_check_block_range,
        );
        // has a transaction in block2 (emptying transaction check)
        // gas cost more than balance in block3 (reserve balance check)
        assert!(res.is_ok());
        assert_eq!(
            account_balance_address_3.remaining_reserve_balance,
            Balance::from(240)
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

    #[test]
    fn test_validate_emptying_txn() {
        let reserve_balance = Balance::from(RESERVE_BALANCE);
        let latest_seq_num = SeqNum(1000);
        let txn_value = 1000;
        let block_seq_num = latest_seq_num + EXEC_DELAY;

        let tx = make_test_tx(50000, txn_value, 0, S1);
        let txs = vec![tx.clone()];
        let signer = tx.recover_signer().unwrap();
        let min_balance = compute_txn_max_gas_cost(&tx);

        let mut account_balances: BTreeMap<Address, AccountBalanceState> = BTreeMap::new();
        account_balances.insert(
            signer,
            AccountBalanceState {
                balance: min_balance,
                remaining_reserve_balance: min_balance,
                block_seqnum_of_latest_txn: latest_seq_num,
                max_reserve_balance: reserve_balance,
            },
        );

        let mut validator = EthBlockPolicyBlockValidator::new(block_seq_num, EXEC_DELAY).unwrap();

        for txn in txs.iter() {
            assert!(validator
                .try_add_transaction(&mut account_balances, txn)
                .is_ok());
        }

        let mut account_balances: BTreeMap<Address, AccountBalanceState> = BTreeMap::new();
        account_balances.insert(
            signer,
            AccountBalanceState {
                balance: min_balance - Balance::from(1),
                remaining_reserve_balance: min_balance,
                block_seqnum_of_latest_txn: latest_seq_num,
                max_reserve_balance: reserve_balance,
            },
        );

        let mut validator = EthBlockPolicyBlockValidator::new(block_seq_num, EXEC_DELAY).unwrap();

        for txn in txs.iter() {
            assert!(
                validator.try_add_transaction(&mut account_balances, txn)
                    == Err(BlockPolicyError::BlockPolicyBlockValidatorError(
                        BlockPolicyBlockValidatorError::InsufficientBalance
                    ))
            );
        }
    }

    #[test]
    fn test_validate_non_emptying_txn() {
        let reserve_balance = Balance::from(RESERVE_BALANCE);
        let latest_seq_num = SeqNum(1000);
        let txn_value = 1000;
        let block_seq_num = latest_seq_num + EXEC_DELAY - SeqNum(1);

        let tx = make_test_tx(50000, txn_value, 0, S1);
        let txs = vec![tx.clone()];
        let signer = tx.recover_signer().unwrap();
        let min_balance = compute_txn_max_gas_cost(&tx);

        let mut account_balances: BTreeMap<Address, AccountBalanceState> = BTreeMap::new();
        account_balances.insert(
            signer,
            AccountBalanceState {
                balance: min_balance,
                remaining_reserve_balance: min_balance,
                block_seqnum_of_latest_txn: latest_seq_num,
                max_reserve_balance: reserve_balance,
            },
        );

        let mut validator = EthBlockPolicyBlockValidator::new(block_seq_num, EXEC_DELAY).unwrap();

        for txn in txs.iter() {
            assert!(validator
                .try_add_transaction(&mut account_balances, txn)
                .is_ok());
        }

        let mut account_balances: BTreeMap<Address, AccountBalanceState> = BTreeMap::new();
        account_balances.insert(
            signer,
            AccountBalanceState {
                balance: min_balance,
                remaining_reserve_balance: min_balance - Balance::from(1),
                block_seqnum_of_latest_txn: latest_seq_num,
                max_reserve_balance: reserve_balance,
            },
        );

        let mut validator = EthBlockPolicyBlockValidator::new(block_seq_num, EXEC_DELAY).unwrap();

        for txn in txs.iter() {
            assert!(
                validator.try_add_transaction(&mut account_balances, txn)
                    == Err(BlockPolicyError::BlockPolicyBlockValidatorError(
                        BlockPolicyBlockValidatorError::InsufficientReserveBalance
                    ))
            );
        }
    }

    #[test]
    fn test_missing_balance() {
        let reserve_balance = Balance::from(RESERVE_BALANCE);
        let latest_seq_num = SeqNum(1000);
        let txn_value = 1000;
        let block_seq_num = latest_seq_num + EXEC_DELAY;

        let tx = make_test_tx(50000, txn_value, 0, S1);
        let txs = vec![tx.clone()];
        let min_balance = compute_txn_max_gas_cost(&tx);

        let address = Address(FixedBytes([0x11; 20]));

        let mut account_balances: BTreeMap<Address, AccountBalanceState> = BTreeMap::new();
        account_balances.insert(
            address,
            AccountBalanceState {
                balance: min_balance,
                remaining_reserve_balance: min_balance,
                block_seqnum_of_latest_txn: latest_seq_num,
                max_reserve_balance: reserve_balance,
            },
        );

        let mut validator = EthBlockPolicyBlockValidator::new(block_seq_num, EXEC_DELAY).unwrap();

        for txn in txs.iter() {
            assert!(
                validator.try_add_transaction(&mut account_balances, txn)
                    == Err(BlockPolicyError::BlockPolicyBlockValidatorError(
                        BlockPolicyBlockValidatorError::AccountBalanceMissing
                    ))
            );
        }
    }

    #[test]
    fn test_validator_inconsistency() {
        let reserve_balance = Balance::from(RESERVE_BALANCE);
        let latest_seq_num = SeqNum(1000);
        let txn_value = 1000;
        let block_seq_num = latest_seq_num + EXEC_DELAY;

        let tx = make_test_tx(50000, txn_value, 0, S1);
        let txs = vec![tx.clone()];
        let signer = tx.recover_signer().unwrap();
        let min_balance = compute_txn_max_value(&tx);

        // Empty reserve balance
        let mut account_balances: BTreeMap<Address, AccountBalanceState> = BTreeMap::new();
        account_balances.insert(
            signer,
            AccountBalanceState {
                balance: min_balance,
                remaining_reserve_balance: Balance::ZERO,
                block_seqnum_of_latest_txn: latest_seq_num,
                max_reserve_balance: reserve_balance,
            },
        );

        let mut validator = EthBlockPolicyBlockValidator::new(block_seq_num, EXEC_DELAY).unwrap();

        for txn in txs.iter() {
            assert!(validator
                .try_add_transaction(&mut account_balances, txn)
                .is_ok());
        }

        // Overdraft
        let block_seq_num = latest_seq_num;
        let min_reserve = compute_txn_max_gas_cost(&tx);
        let mut account_balances: BTreeMap<Address, AccountBalanceState> = BTreeMap::new();
        account_balances.insert(
            signer,
            AccountBalanceState {
                balance: Balance::ZERO,
                remaining_reserve_balance: min_reserve,
                block_seqnum_of_latest_txn: latest_seq_num,
                max_reserve_balance: reserve_balance,
            },
        );

        let mut validator = EthBlockPolicyBlockValidator::new(block_seq_num, EXEC_DELAY).unwrap();

        for txn in txs.iter() {
            assert!(validator
                .try_add_transaction(&mut account_balances, txn)
                .is_ok());
        }
    }

    #[test]
    fn test_validate_many_txn() {
        let reserve_balance = Balance::from(RESERVE_BALANCE);
        let latest_seq_num = SeqNum(1000);
        let txn_value = 1000;
        let block_seq_num = latest_seq_num + EXEC_DELAY;

        let tx1 = make_test_tx(50000, txn_value, 0, S1);
        let tx2 = make_test_tx(50000, txn_value * 2, 1, S1);
        let signer = tx1.recover_signer().unwrap();

        let txs = vec![tx1.clone(), tx2.clone()];
        let min_balance = compute_txn_max_value(&tx1) + compute_txn_max_gas_cost(&tx2);

        let mut account_balances: BTreeMap<Address, AccountBalanceState> = BTreeMap::new();
        account_balances.insert(
            signer,
            AccountBalanceState {
                balance: min_balance,
                remaining_reserve_balance: Balance::ZERO,
                block_seqnum_of_latest_txn: latest_seq_num,
                max_reserve_balance: reserve_balance,
            },
        );

        let mut validator = EthBlockPolicyBlockValidator::new(block_seq_num, EXEC_DELAY).unwrap();

        for txn in txs.iter() {
            assert!(validator
                .try_add_transaction(&mut account_balances, txn)
                .is_ok());
        }

        let min_reserve = compute_txn_max_gas_cost(&tx1) + compute_txn_max_gas_cost(&tx2);

        let mut account_balances: BTreeMap<Address, AccountBalanceState> = BTreeMap::new();
        account_balances.insert(
            signer,
            AccountBalanceState {
                balance: Balance::ZERO,
                remaining_reserve_balance: min_reserve,
                block_seqnum_of_latest_txn: latest_seq_num,
                max_reserve_balance: reserve_balance,
            },
        );

        let mut validator = EthBlockPolicyBlockValidator::new(latest_seq_num, EXEC_DELAY).unwrap();

        for txn in txs.iter() {
            assert!(validator
                .try_add_transaction(&mut account_balances, txn)
                .is_ok());
        }
    }

    const RESERVE_FAIL: Result<(), BlockPolicyError> =
        Err(BlockPolicyError::BlockPolicyBlockValidatorError(
            BlockPolicyBlockValidatorError::InsufficientReserveBalance,
        ));

    #[rstest]
    #[case(Balance::from(100), Balance::from(10), Balance::from(10), SeqNum(3), 1_u128, 1_u64, Ok(()))]
    #[case(Balance::from(5), Balance::from(10), Balance::from(10), SeqNum(3), 2_u128, 2_u64, Ok(()))]
    #[case(Balance::from(5), Balance::from(5), Balance::from(5), SeqNum(3), 0_u128, 5_u64, Ok(()))]
    #[case(
        Balance::from(5),
        Balance::from(10),
        Balance::from(10),
        SeqNum(3),
        7_u128,
        2_u64,
        Ok(())
    )]
    #[case(
        Balance::from(100),
        Balance::from(1),
        Balance::from(1),
        SeqNum(2),
        3_u128,
        2_u64,
        RESERVE_FAIL
    )]
    fn test_txn_tfm(
        #[case] account_balance: Balance,
        #[case] reserve_balance: Balance,
        #[case] max_reserve_balance: Balance,
        #[case] block_seq_num: SeqNum,
        #[case] txn_value: u128,
        #[case] txn_gas_limit: u64,
        #[case] expect: Result<(), BlockPolicyError>,
    ) {
        let abs = AccountBalanceState {
            balance: account_balance,
            remaining_reserve_balance: reserve_balance,
            block_seqnum_of_latest_txn: SeqNum(0),
            max_reserve_balance,
        };

        let txn = make_test_eip1559_tx(txn_value, 0, txn_gas_limit, S1);
        let signer = txn.recover_signer().unwrap();

        let mut account_balances: BTreeMap<Address, AccountBalanceState> = BTreeMap::new();
        account_balances.insert(signer, abs);

        let mut validator = EthBlockPolicyBlockValidator::new(block_seq_num, EXEC_DELAY).unwrap();

        assert_eq!(
            validator.try_add_transaction(&mut account_balances, &txn),
            expect
        );
    }

    #[rstest]
    #[case(
        Balance::from(100),
        Balance::from(5),
        Balance::from(10),
        vec![(4_u128, 2_u64), (4_u128, 2_u64), (4_u128, 2_u64)],
        vec![SeqNum(1), SeqNum(2), SeqNum(3)],
        vec![Ok(()), Ok(()), RESERVE_FAIL],
    )]
    #[case(
        Balance::from(100),
        Balance::from(6),
        Balance::from(10),
        vec![(4_u128, 2_u64), (4_u128, 2_u64), (4_u128, 2_u64)],
        vec![SeqNum(1), SeqNum(2), SeqNum(3)],
        vec![Ok(()), Ok(()), Ok(())],
    )]
    fn test_multi_txn_tfm(
        #[case] account_balance: Balance,
        #[case] reserve_balance: Balance,
        #[case] max_reserve_balance: Balance,
        #[case] txns: Vec<(u128, u64)>, // txn (value, gas_limit)
        #[case] txn_block_num: Vec<SeqNum>,
        #[case] expected: Vec<Result<(), BlockPolicyError>>,
    ) {
        assert_eq!(txns.len(), expected.len());
        assert_eq!(txns.len(), txn_block_num.len());

        let abs = AccountBalanceState {
            balance: account_balance,
            remaining_reserve_balance: reserve_balance,
            block_seqnum_of_latest_txn: SeqNum(0),
            max_reserve_balance,
        };

        let txns = txns
            .iter()
            .enumerate()
            .map(|(nonce, (value, gas_limit))| {
                make_test_eip1559_tx(*value, nonce as u64, *gas_limit, S1)
            })
            .collect_vec();
        let signer = txns[0].recover_signer().unwrap();

        let mut account_balances: BTreeMap<Address, AccountBalanceState> = BTreeMap::new();
        account_balances.insert(signer, abs);

        for ((tx, expect), seqnum) in txns.into_iter().zip(expected).zip(txn_block_num) {
            check_txn_helper(seqnum, &mut account_balances, &tx, expect);
        }
    }

    fn check_txn_helper(
        block_seq_num: SeqNum,
        account_balances: &mut BTreeMap<Address, AccountBalanceState>,
        txn: &Recovered<TxEnvelope>,
        expect: Result<(), BlockPolicyError>,
    ) {
        let mut validator = EthBlockPolicyBlockValidator::new(block_seq_num, EXEC_DELAY).unwrap();

        assert_eq!(
            validator.try_add_transaction(account_balances, txn),
            expect,
            "txn nonce {}",
            txn.nonce()
        );
    }

    fn make_test_eip1559_tx(
        value: u128,
        nonce: u64,
        gas_limit: u64,
        signer: FixedBytes<32>,
    ) -> Recovered<TxEnvelope> {
        recover_tx(make_eip1559_tx_with_value(
            signer, value, 1_u128, 0, gas_limit, nonce, 0,
        ))
    }

    fn make_txn_fees(first_txn_value: u64, first_txn_gas: u64, max_gas_cost: u64) -> TxnFee {
        TxnFee {
            first_txn_value: Balance::from(first_txn_value),
            first_txn_gas: Balance::from(first_txn_gas),
            max_gas_cost: Balance::from(max_gas_cost),
        }
    }

    fn apply_block_fees_helper(
        block_seq_num: SeqNum,
        account_balance: &mut AccountBalanceState,
        fees: &TxnFee,
        eth_address: &Address,
        expected_remaining_reserve: Balance,
        expect: Result<(), BlockPolicyError>,
    ) {
        let mut validator = EthBlockPolicyBlockValidator::new(block_seq_num, EXEC_DELAY).unwrap();

        assert_eq!(
            validator.try_apply_block_fees(account_balance, fees, eth_address),
            expect,
        );
        assert_eq!(
            account_balance.remaining_reserve_balance,
            expected_remaining_reserve
        );
    }

    #[rstest]
    #[case( // Has emptying txn, insufficient balance
        Balance::from(100),
        Balance::from(10),
        Balance::from(10),
        SeqNum(1),
        vec![(1001, 1, 100)], // value is not checked
        vec![SeqNum(4)],
        vec![Balance::ZERO],
        vec![RESERVE_FAIL],
    )]
    #[case( // Has emptying txn, insufficient reserve
        Balance::from(100),
        Balance::from(10),
        Balance::from(10),
        SeqNum(1),
        vec![(100, 1, 100)],
        vec![SeqNum(4)],
        vec![Balance::ZERO],
        vec![RESERVE_FAIL],
    )]
    #[case( // Has emptying txn, insufficient reserve
        Balance::from(100),
        Balance::from(10),
        Balance::from(10),
        SeqNum(1),
        vec![(90, 1, 4), (5, 1, 5)],
        vec![SeqNum(4), SeqNum(5)],
        vec![Balance::from(5), Balance::from(5)],
        vec![Ok(()), RESERVE_FAIL],
    )]
    #[case( // Has emptying txn, pass 
        Balance::from(100),
        Balance::from(10),
        Balance::from(10),
        SeqNum(1),
        vec![(90, 1, 4), (5, 1, 4)],
        vec![SeqNum(4), SeqNum(5)],
        vec![Balance::from(5), Balance::from(0)],
        vec![Ok(()), Ok(())],
    )]
    #[case( // reserve balance fail
        Balance::from(100),
        Balance::from(10),
        Balance::from(10),
        SeqNum(0),
        vec![(50, 1, 9), (0, 0, 0), (500, 1, 1)],
        vec![SeqNum(1), SeqNum(2), SeqNum(3)],
        vec![Balance::from(0), Balance::from(0)],
        vec![Ok(()), Ok(()), RESERVE_FAIL],
    )]
    fn test_try_apply_block_fees(
        #[case] account_balance: Balance,
        #[case] reserve_balance: Balance,
        #[case] max_reserve_balance: Balance,
        #[case] block_seqnum_of_latest_txn: SeqNum,
        #[case] blk_fees: Vec<(u64, u64, u64)>, // (first_txn_value, first_txn_gas, max_gas_cost)
        #[case] txn_block_num: Vec<SeqNum>,
        #[case] expected_remaining_reserve: Vec<Balance>,
        #[case] expected: Vec<Result<(), BlockPolicyError>>,
    ) {
        assert_eq!(blk_fees.len(), expected.len());
        assert_eq!(blk_fees.len(), txn_block_num.len());

        let address = Address(FixedBytes([0x11; 20]));

        let mut account_balance = AccountBalanceState {
            balance: account_balance,
            remaining_reserve_balance: reserve_balance,
            block_seqnum_of_latest_txn,
            max_reserve_balance,
        };

        let blk_fees = blk_fees
            .into_iter()
            .map(|x| make_txn_fees(x.0, x.1, x.2))
            .collect_vec();

        for (((fees, expect), seqnum), expected_remaining_reserve) in blk_fees
            .into_iter()
            .zip(expected)
            .zip(txn_block_num)
            .zip(expected_remaining_reserve)
        {
            apply_block_fees_helper(
                seqnum,
                &mut account_balance,
                &fees,
                &address,
                expected_remaining_reserve,
                expect,
            );
        }
    }
}
