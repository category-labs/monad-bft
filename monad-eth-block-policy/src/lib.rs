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
    collections::{BTreeMap, HashSet},
    marker::PhantomData,
    ops::{Deref, Range, RangeFrom},
};

use alloy_consensus::{
    transaction::{Recovered, Transaction},
    TxEnvelope,
};
use alloy_eips::eip7702::RecoveredAuthorization;
use alloy_primitives::{Address, TxHash, U256};
use itertools::Itertools;
use monad_chain_config::{
    execution_revision::MonadExecutionRevision, revision::ChainRevision, ChainConfig,
};
pub use monad_consensus_types::nonce_usage::{NonceUsage, NonceUsageMap};
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
use monad_eth_types::{
    EthAccount, EthExecutionProtocol, EthHeader, ExtractEthAddress, ValidatedTx,
};
use monad_state_backend::{StateBackend, StateBackendError};
use monad_system_calls::validator::{SystemTransactionValidationError, SystemTransactionValidator};
use monad_types::{
    Balance, BlockId, Epoch, Nonce, Round, SeqNum, GENESIS_BLOCK_ID, GENESIS_ROUND, GENESIS_SEQ_NUM,
};
use monad_validator::signature_collection::SignatureCollection;
use sorted_vector_map::SortedVectorMap;
use tracing::{debug, trace, warn};

pub mod nonce_usage;
pub mod validation;

pub enum ReserveBalanceCheck {
    Insert,
    Propose,
    Validate,
}

pub fn pre_tfm_compute_max_txn_cost(txn: &TxEnvelope) -> U256 {
    let txn_value = txn.value();
    let gas_limit = U256::from(txn.gas_limit());
    let max_fee = U256::from(txn.max_fee_per_gas());
    let max_gas_cost = gas_limit.checked_mul(max_fee).expect("no overflow");
    txn_value.saturating_add(max_gas_cost)
}

pub fn compute_txn_max_value(txn: &TxEnvelope, base_fee: u64) -> U256 {
    let txn_value = txn.value();
    let gas_cost = compute_txn_max_gas_cost(txn, base_fee);
    txn_value.saturating_add(gas_cost)
}

pub fn compute_txn_max_gas_cost(txn: &TxEnvelope, base_fee: u64) -> U256 {
    // pre eip-1559 transactions do not have priority fee
    // full gas price is charged
    if txn.is_legacy() || txn.is_eip2930() {
        let gas_limit = U256::from(txn.gas_limit());
        let max_fee = U256::from(txn.max_fee_per_gas());
        return gas_limit.checked_mul(max_fee).expect("no overflow");
    }

    // post eip-1559 transactions
    // gas price is min(max_fee, base_fee + priority_fee)
    let gas_limit = U256::from(txn.gas_limit());
    let max_fee = U256::from(txn.max_fee_per_gas());
    let priority_fee = U256::from(txn.max_priority_fee_per_gas().unwrap_or(0));
    let base_fee = U256::from(base_fee);
    let gas_bid = max_fee.min(base_fee.saturating_add(priority_fee));
    gas_limit.checked_mul(gas_bid).expect("no overflow")
}

struct BlockLookupIndex {
    block_id: BlockId,
    seq_num: SeqNum,
    round: Round,
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
    pub system_txns: Vec<ValidatedTx>,
    pub validated_txns: Vec<ValidatedTx>,
    pub nonce_usages: NonceUsageMap,
    pub txn_fees: TxnFees,
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
    epoch: Epoch,
    seq_num: SeqNum,
    nonce_usages: NonceUsageMap,
    timestamp_ns: u128,
    fees: BlockTxnFeeStates,

    base_fee: Option<u64>,
    base_fee_trend: Option<u64>,
    base_fee_moment: Option<u64>,
    block_gas_usage: u64,
}

#[derive(Debug)]
struct CommittedBlkBuffer<ST, SCT, CCT, CRT> {
    blocks: SortedVectorMap<SeqNum, CommittedBlock>,
    min_buffer_size: usize, // should be 2 * execution delay

    _phantom: PhantomData<(ST, SCT, fn(&CCT, &CRT))>,
}

impl<ST, SCT, CCT, CRT> CommittedBlkBuffer<ST, SCT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    fn new(min_buffer_size: usize) -> Self {
        Self {
            blocks: Default::default(),
            min_buffer_size,

            _phantom: Default::default(),
        }
    }

    fn get_epoch(&self, seq_num: SeqNum) -> Option<Epoch> {
        self.blocks
            .get(&seq_num)
            .map(|committed_block| committed_block.epoch)
    }

    fn update_account_balance(
        &self,
        account_balance: &mut AccountBalanceState,
        eth_address: &Address,
        execution_delay: SeqNum,
        emptying_txn_check_block_range: Range<SeqNum>,
        reserve_balance_check_block_range: RangeFrom<SeqNum>,
        chain_config: &CCT,
        merged_nonce_usages: &mut NonceUsageMap,
    ) -> Result<SeqNum, BlockPolicyError> {
        trace!(
            ?emptying_txn_check_block_range,
            ?reserve_balance_check_block_range,
            ?account_balance,
            ?eth_address,
            "before update_account_balance"
        );

        let mut next_validate = emptying_txn_check_block_range.start;
        for (seq_num, block) in self.blocks.range(emptying_txn_check_block_range) {
            assert_eq!(*seq_num, next_validate, "Emptying range is not contiguous");

            if block.fees.get(eth_address).is_some()
                && account_balance.block_seqnum_of_latest_txn < block.seq_num
            {
                account_balance.block_seqnum_of_latest_txn = block.seq_num;
            }
            next_validate += SeqNum(1);
        }

        for (seq_num, block) in self.blocks.range(reserve_balance_check_block_range) {
            assert_eq!(
                *seq_num, next_validate,
                "Reserve balance check range is not contiguous"
            );

            // Merge nonce_usages from previous blocks
            merged_nonce_usages.merge_with_previous_block(block.nonce_usages.map.iter());

            if let Some(block_txn_fees) = block.fees.get(eth_address) {
                let validator = EthBlockPolicyBlockValidator::new(
                    block.seq_num,
                    execution_delay,
                    block
                        .base_fee
                        .unwrap_or(monad_tfm::base_fee::PRE_TFM_BASE_FEE),
                    &chain_config.get_chain_revision(block.round),
                    &chain_config
                        .get_execution_chain_revision(timestamp_ns_to_secs(block.timestamp_ns)),
                )?;
                trace!(
                    "applying fees for block {:?}, curr acc balance: {:?}",
                    block.seq_num,
                    account_balance
                );
                validator.try_apply_block_fees(
                    account_balance,
                    &block_txn_fees,
                    eth_address,
                    merged_nonce_usages,
                )?;
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
        debug!(?block_number, "update_committed_block");
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

        let block_gas_usage = block.get_total_gas();

        assert!(self
            .blocks
            .insert(
                block_number,
                CommittedBlock {
                    block_id: block.get_id(),
                    round: block.get_block_round(),
                    epoch: block.get_epoch(),
                    seq_num: block.get_seq_num(),
                    nonce_usages: block.nonce_usages.clone(),
                    timestamp_ns: block.get_timestamp(),
                    fees: BlockTxnFeeStates {
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

pub struct EthBlockPolicyBlockValidator<CRT>
where
    CRT: ChainRevision,
{
    block_seq_num: SeqNum,
    execution_delay: SeqNum,
    base_fee: u64,
    chain_revision: CRT,
    execution_chain_revision: MonadExecutionRevision,
    _phantom: PhantomData<CRT>,
}

fn is_possibly_emptying_transaction(
    block_seq_num_of_curr_txn: SeqNum,
    balance_state: &AccountBalanceState,
    execution_delay: SeqNum,
) -> bool {
    // txn T is emptying if there is no "prior txn" i.e. a txn from the same sender sent from block P so that P >= block_number(T) - k + 1.
    let blocks_since_latest_txn = SeqNum(
        block_seq_num_of_curr_txn
            .0
            .saturating_sub(balance_state.block_seqnum_of_latest_txn.0),
    );
    !balance_state.is_delegated && blocks_since_latest_txn > execution_delay - SeqNum(1)
}

pub fn timestamp_ns_to_secs(timestamp_ns: u128) -> u64 {
    const NSEC_PER_SEC: u128 = 1_000_000_000;
    let timestamp_seconds = timestamp_ns / NSEC_PER_SEC;
    timestamp_seconds.min(u64::MAX.into()) as u64
}

impl<CRT> BlockPolicyBlockValidator<CRT> for EthBlockPolicyBlockValidator<CRT>
where
    Self: Sized,
    CRT: ChainRevision,
{
    type Transaction = Recovered<TxEnvelope>;

    fn new(
        block_seq_num: SeqNum,
        execution_delay: SeqNum,
        base_fee: u64,
        chain_revision: &CRT,
        execution_chain_revision: &MonadExecutionRevision,
    ) -> Result<Self, BlockPolicyError> {
        Ok(Self {
            block_seq_num,
            execution_delay,
            base_fee,
            chain_revision: *chain_revision,
            execution_chain_revision: *execution_chain_revision,
            _phantom: PhantomData,
        })
    }

    fn try_apply_block_fees(
        &self,
        account_balance: &mut AccountBalanceState,
        block_txn_fees: &TxnFee,
        eth_address: &Address,
        nonce_usages: &NonceUsageMap,
    ) -> Result<(), BlockPolicyError> {
        let tfm_enabled = self
            .execution_chain_revision
            .execution_chain_params()
            .tfm_enabled;
        let max_reserve_balance =
            Balance::from(self.chain_revision.chain_params().max_reserve_balance);

        if !tfm_enabled {
            if account_balance.balance < block_txn_fees.max_txn_cost {
                trace!(
                    seq_num =?self.block_seq_num,
                    ?account_balance,
                    block_txn_cost =?block_txn_fees.max_txn_cost,
                    "TFM disabled. block can not be accepted insufficient balance"
                );
                return Err(BlockPolicyError::BlockPolicyBlockValidatorError(
                    BlockPolicyBlockValidatorError::InsufficientBalance,
                ));
            }

            let estimated_balance = account_balance
                .balance
                .saturating_sub(block_txn_fees.max_txn_cost);
            account_balance.remaining_reserve_balance = estimated_balance.min(max_reserve_balance);
            account_balance.balance = estimated_balance;
            account_balance.block_seqnum_of_latest_txn = self.block_seq_num;

            trace!(
                "TFM disabled updated balance: {:?} \
                        txn max cost {:?} \
                        block seq_num {:?} \
                        address: {:?}",
                account_balance,
                block_txn_fees.max_txn_cost,
                self.block_seq_num,
                eth_address,
            );
            return Ok(());
        }

        let mut is_delegated_now = false;

        if let Some(is_valid_authorization) = block_txn_fees.valid_authorization {
            if is_valid_authorization {
                is_delegated_now = true;
            } else if let Some(authorization_nonce) = block_txn_fees.authorization_nonce {
                if let Some(nonce_usage) = nonce_usages.get(eth_address) {
                    match nonce_usage {
                        NonceUsage::Known(nonce) => {
                            is_delegated_now = *nonce == authorization_nonce;
                        }
                        NonceUsage::Possible(_) => {}
                    }
                }
            }
        }

        account_balance.is_delegated |= is_delegated_now;

        let has_emptying_transaction = is_possibly_emptying_transaction(
            self.block_seq_num,
            account_balance,
            self.execution_delay,
        );

        let mut block_gas_cost = block_txn_fees.max_gas_cost;
        let orig_account_balance = account_balance.balance;
        let orig_reserve_balance = account_balance.remaining_reserve_balance;

        if has_emptying_transaction {
            if account_balance.balance < block_txn_fees.first_txn_gas {
                trace!(
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

            account_balance.remaining_reserve_balance = estimated_balance.min(max_reserve_balance);
            account_balance.balance = estimated_balance;

            trace!(
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
            trace!(
                "Block with insufficient reserve balance: {:?} \
                            max gas cost {:?} \
                            block seq_num {:?} \
                            address: {:?}",
                account_balance,
                block_gas_cost,
                self.block_seq_num,
                eth_address,
            );
            account_balance.balance = orig_account_balance;
            account_balance.remaining_reserve_balance = orig_reserve_balance;

            return Err(BlockPolicyError::BlockPolicyBlockValidatorError(
                BlockPolicyBlockValidatorError::InsufficientReserveBalance,
            ));
        }
        account_balance.remaining_reserve_balance = account_balance
            .remaining_reserve_balance
            .saturating_sub(block_gas_cost);
        account_balance.block_seqnum_of_latest_txn = self.block_seq_num;

        trace!(
            ?account_balance,
            ?self.block_seq_num,
            ?eth_address,
            "try_apply_block_fees updated balance state",
        );
        Ok(())
    }

    fn try_add_transaction(
        &self,
        account_balances: &mut BTreeMap<&Address, AccountBalanceState>,
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

        if !self
            .execution_chain_revision
            .execution_chain_params()
            .tfm_enabled
        {
            let txn_cost = pre_tfm_compute_max_txn_cost(txn);
            if account_balance.balance < txn_cost {
                trace!(
                    seq_num =?self.block_seq_num,
                    ?account_balance,
                    ?txn_cost,
                    ?txn,
                    "TFM disabled. txn can not be accepted insufficient balance"
                );
                return Err(BlockPolicyError::BlockPolicyBlockValidatorError(
                    BlockPolicyBlockValidatorError::InsufficientBalance,
                ));
            }

            let estimated_balance = account_balance.balance.saturating_sub(txn_cost);
            account_balance.remaining_reserve_balance =
                estimated_balance.min(account_balance.max_reserve_balance);
            account_balance.balance = estimated_balance;
            account_balance.block_seqnum_of_latest_txn = self.block_seq_num;

            trace!(
                "TFM disabled. updated balance: {:?} \
                        txn cost {:?} \
                        block seq_num {:?} \
                        address: {:?}",
                account_balance,
                txn_cost,
                self.block_seq_num,
                eth_address,
            );
            return Ok(());
        }

        let is_emptying_transaction = is_possibly_emptying_transaction(
            self.block_seq_num,
            account_balance,
            self.execution_delay,
        );

        // if an account for txn T is not delegated and has no prior txns, then T can charge into reserve.
        if is_emptying_transaction {
            let txn_max_gas = compute_txn_max_gas_cost(txn, self.base_fee);
            if account_balance.balance < txn_max_gas {
                trace!(
                    seq_num =?self.block_seq_num,
                    ?account_balance,
                    ?txn_max_gas,
                    ?txn,
                    ?is_emptying_transaction,
                    "Emptying txn can not be accepted insufficient reserve balance"
                );
                return Err(BlockPolicyError::BlockPolicyBlockValidatorError(
                    BlockPolicyBlockValidatorError::InsufficientBalance,
                ));
            }

            let txn_max_cost = compute_txn_max_value(txn, self.base_fee);
            let estimated_balance = account_balance.balance.saturating_sub(txn_max_cost);
            let reserve_balance = account_balance.max_reserve_balance.min(estimated_balance);

            trace!(
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
            let txn_max_gas = compute_txn_max_gas_cost(txn, self.base_fee);
            if account_balance.remaining_reserve_balance < txn_max_gas {
                trace!(
                    seq_num =?self.block_seq_num,
                    ?account_balance,
                    ?txn_max_gas,
                    ?txn,
                    ?is_emptying_transaction,
                    "Non-emptying txn can not be accepted insufficient reserve balance"
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
pub struct EthBlockPolicy<ST, SCT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    /// SeqNum of last committed block
    last_commit: SeqNum,

    // last execution-delay committed blocks
    committed_cache: CommittedBlkBuffer<ST, SCT, CCT, CRT>,

    execution_delay: SeqNum,
}

impl<ST, SCT, CCT, CRT> EthBlockPolicy<ST, SCT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    CertificateSignaturePubKey<ST>: ExtractEthAddress,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    pub fn new(
        last_commit: SeqNum, // TODO deprecate
        execution_delay: u64,
    ) -> Self {
        let cache_max_size = execution_delay.saturating_mul(2);
        Self {
            // Needs to be at least 2 * execution_delay to detect emptying transactions
            committed_cache: CommittedBlkBuffer::new((cache_max_size) as usize),
            last_commit,
            execution_delay: SeqNum(execution_delay),
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

        let addresses = addresses.unique().collect::<HashSet<&'a Address>>();

        let base_seq_num = consensus_block_seq_num.max(self.execution_delay) - self.execution_delay;

        let mut cached_nonce_usages = NonceUsageMap::default();

        for nonce_usages in self
            .committed_cache
            .blocks
            .iter()
            .map(|(seq_num, block)| (*seq_num, &block.nonce_usages))
            .chain(
                extending_blocks
                    .iter()
                    .map(|block| (block.get_seq_num(), &block.nonce_usages)),
            )
            .filter(|(seq_num, _)| *seq_num > base_seq_num)
            .rev()
            .map(|(_, nonce_usages)| {
                nonce_usages
                    .map
                    .iter()
                    .filter(|(address, _)| addresses.contains(address))
            })
        {
            cached_nonce_usages.merge_with_previous_block(nonce_usages);
        }

        let mut account_nonces = BTreeMap::default();
        let mut cache_misses = Vec::new();

        for address in addresses {
            match cached_nonce_usages.get(address) {
                Some(NonceUsage::Known(nonce)) => {
                    account_nonces.insert(address, *nonce + 1);
                }
                Some(NonceUsage::Possible(possible)) => {
                    cache_misses.push((address, Some(possible)));
                }
                None => {
                    cache_misses.push((address, None));
                }
            }
        }

        // the cached account nonce must overlap with latest triedb, i.e.
        // account_nonces must keep nonces for last delay blocks in cache
        // the cache should keep track of block number for the nonce state
        // when purging, we never purge nonces newer than last_commit - delay

        let cache_miss_statuses = self.get_account_statuses(
            state_backend,
            &Some(extending_blocks),
            cache_misses.iter().map(|(address, _)| *address),
            &base_seq_num,
        )?;

        account_nonces.extend(cache_misses.into_iter().zip_eq(cache_miss_statuses).map(
            |((address, possible_nonces), status)| {
                let nonce = status.map_or(0, |status| status.nonce);

                (
                    address,
                    possible_nonces.map_or(nonce, |possible_nonces| {
                        NonceUsage::apply_possible_nonces_to_account_nonce(nonce, possible_nonces)
                    }),
                )
            },
        ));

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
                    round: GENESIS_ROUND,
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
                    round: committed_block.round,
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
                round: proposed_block.get_block_round(),
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
        chain_config: &CCT,
        extending_blocks: Option<&Vec<&EthValidatedBlock<ST, SCT>>>,
        addresses: impl Iterator<Item = &'a Address>,
    ) -> Result<BTreeMap<&'a Address, AccountBalanceState>, BlockPolicyError>
    where
        SCT: SignatureCollection,
    {
        // calculation correct only if GENESIS_SEQ_NUM == 0
        assert_eq!(GENESIS_SEQ_NUM, SeqNum(0));
        let base_seq_num = consensus_block_seq_num.max(self.execution_delay) - self.execution_delay;

        let block_index = self.get_block_index(&extending_blocks, &base_seq_num)?;
        let base_max_reserve_balance = Balance::from(
            chain_config
                .get_chain_revision(block_index.round)
                .chain_params()
                .max_reserve_balance,
        );

        let addresses = addresses.unique().collect_vec();
        let account_statuses = self.get_account_statuses(
            state_backend,
            &extending_blocks,
            addresses.iter().copied(),
            &base_seq_num,
        )?;

        let account_balances = account_statuses
            .iter()
            .map(|maybe_status| {
                maybe_status.as_ref().map_or(
                    AccountBalanceState::new(base_max_reserve_balance),
                    |status| {
                        AccountBalanceState {
                            balance: status.balance,
                            remaining_reserve_balance: status.balance.min(base_max_reserve_balance),
                            max_reserve_balance: base_max_reserve_balance,
                            block_seqnum_of_latest_txn: base_seq_num, // most pessimistic assumption
                            is_delegated: status.is_delegated,
                        }
                    },
                )
            })
            .collect_vec();

        // Initialize merged_nonce_usages for tracking across all committed and extending blocks
        // Start with all account nonces from the base block
        let mut merged_nonce_usages = NonceUsageMap::default();
        for (address, maybe_status) in addresses.iter().zip_eq(account_statuses.iter()) {
            if let Some(status) = maybe_status {
                merged_nonce_usages.add_known(**address, status.nonce);
            }
        }

        // Process account balances using a for loop to allow mutation of merged_nonce_usages
        let mut account_balance_map = BTreeMap::new();
        for (address, mut balance_state) in addresses.into_iter().zip_eq(account_balances) {
            // N - k + 1
            let reserve_balance_check_start = base_seq_num + SeqNum(1);
            // N - 2k + 2
            let mut emptying_txn_check_start = (reserve_balance_check_start + SeqNum(1))
                .max(self.execution_delay)
                - self.execution_delay;

            if emptying_txn_check_start == GENESIS_SEQ_NUM {
                emptying_txn_check_start += SeqNum(1);
            }

            // N - 2k + 2 (inclusive) to N - k + 1 (non inclusive)
            let emptying_txn_check_block_range =
                emptying_txn_check_start..reserve_balance_check_start;
            // N - k + 1 (inclusive) to N (non inclusive)
            let reserve_balance_check_block_range = reserve_balance_check_start..;

            if emptying_txn_check_start > GENESIS_SEQ_NUM {
                balance_state.block_seqnum_of_latest_txn = emptying_txn_check_start - SeqNum(1);
            }

            // check for emptying txs and reserve balance in committed blocks
            let mut next_validate = self.committed_cache.update_account_balance(
                &mut balance_state,
                address,
                self.execution_delay,
                emptying_txn_check_block_range,
                reserve_balance_check_block_range,
                chain_config,
                &mut merged_nonce_usages,
            )?;

            // check for emptying txs and reserve balance in extending blocks
            if let Some(blocks) = extending_blocks {
                // handle the case where base_seq_num is a pending block
                let next_blocks = blocks
                    .iter()
                    .skip_while(move |block| block.get_seq_num() < next_validate);

                for extending_block in next_blocks {
                    assert_eq!(next_validate, extending_block.get_seq_num());

                    // Merge nonce_usages from previous extending blocks
                    merged_nonce_usages
                        .merge_with_previous_block(extending_block.nonce_usages.map.iter());

                    if let Some(txn_fee) = extending_block.txn_fees.get(address) {
                        // if still within check emptying range, update latest tx seq num
                        // otherwise check for reserve balance
                        if next_validate < reserve_balance_check_start {
                            if balance_state.block_seqnum_of_latest_txn < next_validate {
                                balance_state.block_seqnum_of_latest_txn =
                                    extending_block.get_seq_num();
                            }
                        } else {
                            let validator = EthBlockPolicyBlockValidator::new(
                                extending_block.get_seq_num(),
                                self.execution_delay,
                                extending_block
                                    .get_base_fee()
                                    .unwrap_or(monad_tfm::base_fee::PRE_TFM_BASE_FEE),
                                &chain_config.get_chain_revision(extending_block.get_block_round()),
                                &chain_config.get_execution_chain_revision(timestamp_ns_to_secs(
                                    extending_block.get_timestamp(),
                                )),
                            )?;

                            validator.try_apply_block_fees(
                                &mut balance_state,
                                txn_fee,
                                address,
                                &merged_nonce_usages,
                            )?;
                        }
                    }
                    next_validate += SeqNum(1);
                }
            }

            account_balance_map.insert(address, balance_state);
        }

        Ok(account_balance_map)
    }

    /// return value:
    /// (parent_block_round, parent_base_fee, parent_trend, parent_moment, parent_gas_usage)
    fn get_parent_base_fee_fields<B>(&self, extending_blocks: &[B]) -> (Round, u64, u64, u64, u64)
    where
        B: AsRef<EthValidatedBlock<ST, SCT>>,
    {
        // parent block is last block in extending_blocks or last_committed
        // block if there's no extending branch
        let (
            parent_block_round,
            maybe_parent_base_fee,
            maybe_parent_trend,
            maybe_parent_moment,
            parent_gas_usage,
        ) = if let Some(parent_block) = extending_blocks.last() {
            let parent_gas_usage = parent_block
                .as_ref()
                .validated_txns
                .iter()
                .map(|txn| txn.gas_limit())
                .sum::<u64>();
            (
                parent_block.as_ref().header().block_round,
                parent_block.as_ref().header().base_fee,
                parent_block.as_ref().header().base_fee_trend,
                parent_block.as_ref().header().base_fee_moment,
                parent_gas_usage,
            )
        } else {
            // genesis block doesn't exist in committed_cache
            if self.last_commit == GENESIS_SEQ_NUM {
                // genesis block
                (
                    GENESIS_ROUND,
                    Some(monad_tfm::base_fee::GENESIS_BASE_FEE),
                    Some(monad_tfm::base_fee::GENESIS_BASE_FEE_TREND),
                    Some(monad_tfm::base_fee::GENESIS_BASE_FEE_MOMENT),
                    0,
                )
            } else {
                let parent_block = self
                    .committed_cache
                    .blocks
                    .get(&self.last_commit)
                    .expect("last committed block must exist");
                (
                    parent_block.round,
                    parent_block.base_fee,
                    parent_block.base_fee_trend,
                    parent_block.base_fee_moment,
                    parent_block.block_gas_usage,
                )
            }
        };

        // if parent block doesn't have base_fee fields, it must be pre-tfm
        // block and we return genesis values
        let (parent_base_fee, parent_trend, parent_moment) = match (
            maybe_parent_base_fee,
            maybe_parent_trend,
            maybe_parent_moment,
        ) {
            (Some(parent_base_fee), Some(parent_trend), Some(parent_moment)) => {
                (parent_base_fee, parent_trend, parent_moment)
            }
            _ => (
                monad_tfm::base_fee::GENESIS_BASE_FEE,
                monad_tfm::base_fee::GENESIS_BASE_FEE_TREND,
                monad_tfm::base_fee::GENESIS_BASE_FEE_MOMENT,
            ),
        };

        (
            parent_block_round,
            parent_base_fee,
            parent_trend,
            parent_moment,
            parent_gas_usage,
        )
    }

    /// compute the base fee according to tfm rules
    ///
    /// return value: (base_fee, base_fee_trend, base_fee_moment)
    ///
    /// base_fee unit: MON-wei
    pub fn compute_base_fee<B>(
        &self,
        extending_blocks: &[B],
        chain_config: &CCT,
        timestamp_ns: u128,
    ) -> Option<(u64, u64, u64)>
    where
        B: AsRef<EthValidatedBlock<ST, SCT>>,
    {
        let tfm_enabled = chain_config
            .get_execution_chain_revision(timestamp_ns_to_secs(timestamp_ns))
            .execution_chain_params()
            .tfm_enabled;
        if tfm_enabled {
            let (
                parent_block_round,
                parent_base_fee,
                parent_trend,
                parent_moment,
                parent_gas_usage,
            ) = self.get_parent_base_fee_fields(extending_blocks);
            let parent_block_gas_limit = chain_config
                .get_chain_revision(parent_block_round)
                .chain_params()
                .proposal_gas_limit;

            Some(monad_tfm::base_fee::compute_base_fee(
                parent_block_gas_limit,
                parent_gas_usage,
                parent_base_fee,
                parent_trend,
                parent_moment,
            ))
        } else {
            None
        }
    }

    pub fn get_execution_delay(&self) -> SeqNum {
        self.execution_delay
    }

    fn validate_system_transactions_input(
        &self,
        block: &EthValidatedBlock<ST, SCT>,
        extending_blocks: &Vec<&EthValidatedBlock<ST, SCT>>,
        chain_config: &CCT,
    ) -> Result<(), SystemTransactionValidationError> {
        let parent_block_epoch = {
            if let Some(extending_block) = extending_blocks.last() {
                extending_block.get_epoch()
            } else {
                assert_eq!(block.get_seq_num(), self.get_last_commit() + SeqNum(1));
                self.get_last_commit_epoch()
            }
        };

        SystemTransactionValidator::validate_system_transactions_input(
            block.block.header(),
            parent_block_epoch,
            &block.system_txns,
            chain_config,
        )
    }

    fn system_transaction_nonce_check(
        &self,
        system_txns: &[ValidatedTx],
        account_nonces: &mut BTreeMap<&Address, u64>,
    ) -> Result<(), BlockPolicyError> {
        for sys_txn in system_txns.iter() {
            let sys_txn_signer = sys_txn.signer();
            let sys_txn_nonce = sys_txn.nonce();

            let expected_nonce = account_nonces
                .get_mut(&sys_txn_signer)
                .expect("account_nonces should have been populated");

            if &sys_txn_nonce != expected_nonce {
                warn!(
                    ?sys_txn_nonce,
                    ?expected_nonce,
                    "block not coherent, invalid nonce for system transaction"
                );
                return Err(BlockPolicyError::BlockNotCoherent);
            }
            *expected_nonce += 1;
        }

        Ok(())
    }

    // this function checks the validity of nonces for a regular transaction
    fn nonce_check_and_update(
        &self,
        txn: &Recovered<TxEnvelope>,
        account_nonces: &mut BTreeMap<&Address, u64>,
    ) -> Result<(), BlockPolicyError> {
        let eth_address = txn.signer();
        let txn_nonce = txn.nonce();

        let expected_nonce = account_nonces
            .get_mut(&eth_address)
            .expect("account_nonces should have been populated");

        if &txn_nonce != expected_nonce {
            warn!(
                txn_nonce = ?txn_nonce,
                expected_nonce = ?expected_nonce,
                "block not coherent, invalid nonce"
            );
            return Err(BlockPolicyError::BlockNotCoherent);
        }
        *expected_nonce += 1;

        Ok(())
    }

    // https://eips.ethereum.org/EIPS/eip-7702#behavior
    // the nonce of authority is only incremented if the behavior checks
    // for the tuple pass
    // this function performs those checks
    fn eip_7702_valid_nonce_update(
        &self,
        auth_list: &[RecoveredAuthorization],
        account_nonces: &mut BTreeMap<&Address, u64>,
        chain_id: u64,
    ) {
        for (result, nonce, code_address, auth_chain_id) in auth_list
            .iter()
            .map(|a| (a.authority(), a.nonce(), a.address(), a.chain_id()))
        {
            match result {
                Some(authority) => {
                    trace!(?code_address, ?nonce, ?authority, "Authority");
                    if auth_chain_id != 0_u64 && auth_chain_id != chain_id {
                        continue;
                    }

                    let expected_nonce = account_nonces
                        .get_mut(&authority)
                        .expect("account_nonces should have been populated");

                    if *expected_nonce != nonce {
                        trace!(
                            ?expected_nonce,
                            auth_tuple_nonce = nonce,
                            ?authority,
                            "authority nonce error"
                        );
                        continue;
                    }
                    *expected_nonce += 1;
                }
                None => {
                    // skip authorization if there is error recovering signer
                    continue;
                }
            }
        }
    }

    fn extract_signers(
        &self,
        validated_txns: &[ValidatedTx],
        system_txns: &[ValidatedTx],
    ) -> Result<(HashSet<Address>, HashSet<Address>), BlockPolicyError> {
        // TODO fix this unnecessary copy into a new vec to generate an owned Address
        let mut tx_signers: HashSet<Address> =
            validated_txns.iter().map(|txn| txn.signer()).collect();

        let authority_addresses: HashSet<Address> = validated_txns
            .iter()
            .flat_map(|txn| {
                txn.authorizations_7702
                    .iter()
                    .filter_map(|recovered_auth| recovered_auth.authority())
            })
            .collect();

        tx_signers.extend(authority_addresses.iter().cloned());

        let mut system_tx_signers = system_txns.iter().map(|txn| txn.signer());
        tx_signers.extend(&mut system_tx_signers);

        Ok((tx_signers, authority_addresses))
    }
}

impl<ST, SCT, SBT, CCT, CRT> BlockPolicy<ST, SCT, EthExecutionProtocol, SBT, CCT, CRT>
    for EthBlockPolicy<ST, SCT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT>,
    CertificateSignaturePubKey<ST>: ExtractEthAddress,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    type ValidatedBlock = EthValidatedBlock<ST, SCT>;

    fn check_coherency(
        &self,
        block: &Self::ValidatedBlock,
        extending_blocks: Vec<&Self::ValidatedBlock>,
        blocktree_root: RootInfo,
        state_backend: &SBT,
        chain_config: &CCT,
    ) -> Result<(), BlockPolicyError> {
        let chain_id = chain_config.chain_id();

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
        let maybe_tfm_base_fees =
            self.compute_base_fee(&extending_blocks, chain_config, block.get_timestamp());

        let (base_fee, base_fee_trend, base_fee_moment) = match maybe_tfm_base_fees {
            Some((base_fee, base_fee_trend, base_fee_moment)) => {
                (Some(base_fee), Some(base_fee_trend), Some(base_fee_moment))
            }
            None => (None, None, None),
        };

        if base_fee != block.header().base_fee
            || base_fee_trend != block.header().base_fee_trend
            || base_fee_moment != block.header().base_fee_moment
        {
            warn!(
                seq_num =? block.header().seq_num,
                round =? block.header().block_round,
                expected_base_fees = ?(base_fee, base_fee_trend, base_fee_moment),
                block_base_fees = ?(block.header().base_fee, block.header().base_fee_trend, block.header().base_fee_moment),
                "block not coherent, base_fee mismatch"
            );
            return Err(BlockPolicyError::BaseFeeError);
        }

        let (tx_signers, authority_addresses) =
            self.extract_signers(&block.validated_txns, &block.system_txns)?;

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
            chain_config,
            Some(&extending_blocks),
            tx_signers.iter(),
        )?;

        for authority in &authority_addresses {
            let account_balance = account_balances
                .get_mut(authority)
                .expect("account_balances should have been populated for delegated accounts");

            trace!(?authority, "Setting account to is_delegated: true");
            account_balance.is_delegated = true;
        }

        let validator = EthBlockPolicyBlockValidator::new(
            block.get_seq_num(),
            self.execution_delay,
            block
                .get_base_fee()
                .unwrap_or(monad_tfm::base_fee::PRE_TFM_BASE_FEE),
            &chain_config.get_chain_revision(block.get_block_round()),
            &chain_config.get_execution_chain_revision(timestamp_ns_to_secs(block.get_timestamp())),
        )?;

        if let Err(system_txn_error) =
            self.validate_system_transactions_input(block, &extending_blocks, chain_config)
        {
            debug!(
                ?system_txn_error,
                "block not coherent, system transaction error"
            );
            return Err(BlockPolicyError::SystemTransactionError);
        }
        self.system_transaction_nonce_check(&block.system_txns, &mut account_nonces)?;

        for txn in block.validated_txns.iter() {
            self.nonce_check_and_update(txn, &mut account_nonces)?;
            validator.try_add_transaction(&mut account_balances, txn)?;

            // https://eips.ethereum.org/EIPS/eip-7702#behavior
            // "The authorization list is processed before the execution portion
            // of the transaction begins, but after the sender’s nonce is incremented."
            if txn.is_eip7702() {
                self.eip_7702_valid_nonce_update(
                    &txn.authorizations_7702,
                    &mut account_nonces,
                    chain_id,
                );
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

    fn update_committed_block(&mut self, block: &Self::ValidatedBlock, chain_config: &CCT) {
        assert_eq!(block.get_seq_num(), self.last_commit + SeqNum(1));
        self.last_commit = block.get_seq_num();
        self.committed_cache.update_committed_block(block);
    }

    fn reset(
        &mut self,
        last_delay_committed_blocks: Vec<&Self::ValidatedBlock>,
        chain_config: &CCT,
    ) {
        self.committed_cache = CommittedBlkBuffer::new(self.committed_cache.min_buffer_size);
        for block in last_delay_committed_blocks {
            self.last_commit = block.get_seq_num();
            self.committed_cache.update_committed_block(block);
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::{HashMap, VecDeque};

    use alloy_consensus::{SignableTransaction, TxEip1559};
    use alloy_eips::eip7702::Authorization;
    use alloy_primitives::{Address, FixedBytes, PrimitiveSignature, TxKind, B256};
    use alloy_signer::SignerSync;
    use alloy_signer_local::PrivateKeySigner;
    use monad_chain_config::{revision::MockChainRevision, MockChainConfig};
    use monad_crypto::NopSignature;
    use monad_eth_testutil::{
        generate_consensus_test_block, make_eip1559_tx, make_eip1559_tx_with_value,
        make_eip7702_tx, make_eip7702_tx_with_value, make_legacy_tx, make_signed_authorization,
        recover_tx, secret_to_eth_address, sign_authorization, S1, S2,
    };
    use monad_state_backend::NopStateBackend;
    use monad_testutil::signing::MockSignatures;
    use monad_types::{Balance, Hash, SeqNum};
    use proptest::{prelude::*, strategy::Just};
    use rstest::*;
    use test_case::test_case;

    use super::*;

    const BASE_FEE: u64 = 100_000_000_000;
    const BASE_FEE_TREND: u64 = 0;
    const BASE_FEE_MOMENT: u64 = 0;

    type SignatureType = NopSignature;
    type SignatureCollectionType = MockSignatures<SignatureType>;
    type StateBackendType = NopStateBackend;
    type ChainConfigType = MockChainConfig;
    type ChainRevisionType = MockChainRevision;

    const RESERVE_BALANCE: u128 = 10_000_000_000_000_000_000; // 10 MON
    const EXEC_DELAY: SeqNum = SeqNum(3);

    const ONE_ETHER: u128 = 1_000_000_000_000_000_000;
    const HALF_ETHER: u128 = 500_000_000_000_000_000;
    const CHAIN_ID: u64 = 1337;

    enum CoherencyCheckMode {
        ReserveBalanceCoherency,
        NonceCoherency,
    }

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
            BASE_FEE as u128,
            0, // priority fee
            gas_limit,
            nonce,
            0, // input length
        ))
    }

    fn make_test_delegation_tx(
        gas_limit: u64,
        value: u128,
        nonce: u64,
        signer: FixedBytes<32>,
        authorizations: HashMap<FixedBytes<32>, Authorization>,
    ) -> Recovered<TxEnvelope> {
        recover_tx(make_eip7702_tx_with_value(
            signer,
            value,
            BASE_FEE as u128,
            0, // priority fee
            gas_limit,
            nonce,
            authorizations
                .into_iter()
                .map(|(authority, authorization)| sign_authorization(authority, authorization))
                .collect(),
            0, // input length
        ))
    }

    pub fn make_test_block(
        round: Round,
        seq_num: SeqNum,
        txs: Vec<Recovered<TxEnvelope>>,
    ) -> EthValidatedBlock<NopSignature, MockSignatures<NopSignature>> {
        let consensus_test_block =
            generate_consensus_test_block(round, seq_num, BASE_FEE, &MockChainConfig::DEFAULT, txs);
        EthValidatedBlock {
            block: consensus_test_block.block,
            system_txns: Vec::new(),
            validated_txns: consensus_test_block.validated_txns,
            nonce_usages: unsafe {
                // Workaround for type resolution failure due to circular dependency
                #[allow(clippy::missing_transmute_annotations)]
                std::mem::transmute(consensus_test_block.nonce_usages)
            },
            txn_fees: consensus_test_block.txn_fees,
        }
    }

    fn reserve_balance_coherency(
        block_policy: EthBlockPolicy<
            SignatureType,
            SignatureCollectionType,
            ChainConfigType,
            ChainRevisionType,
        >,
        incoming_block: EthValidatedBlock<SignatureType, SignatureCollectionType>,
        extending_blocks: Vec<&EthValidatedBlock<SignatureType, SignatureCollectionType>>,
        state_backend: &impl StateBackend<SignatureType, SignatureCollectionType>,
        addresses: Vec<Address>,
    ) -> Result<(), BlockPolicyError> {
        let mut account_balances = block_policy.compute_account_base_balances(
            incoming_block.get_seq_num(),
            state_backend,
            &MockChainConfig::DEFAULT,
            Some(&extending_blocks),
            addresses.iter(),
        )?;

        let validator = EthBlockPolicyBlockValidator::new(
            incoming_block.get_seq_num(),
            block_policy.execution_delay,
            BASE_FEE,
            &MockChainRevision::DEFAULT,
            &MonadExecutionRevision::LATEST,
        )?;

        for txn in incoming_block.validated_txns.iter() {
            validator.try_add_transaction(&mut account_balances, txn)?;
        }

        Ok(())
    }

    fn nonce_coherency(
        block_policy: EthBlockPolicy<
            SignatureType,
            SignatureCollectionType,
            ChainConfigType,
            ChainRevisionType,
        >,
        incoming_block: EthValidatedBlock<SignatureType, SignatureCollectionType>,
        extending_blocks: Vec<&EthValidatedBlock<SignatureType, SignatureCollectionType>>,
        state_backend: &impl StateBackend<SignatureType, SignatureCollectionType>,
        addresses: Vec<Address>,
    ) -> Result<(), BlockPolicyError> {
        let mut account_nonces = block_policy.get_account_base_nonces(
            incoming_block.get_seq_num(),
            state_backend,
            &extending_blocks,
            addresses.iter(),
        )?;

        for txn in incoming_block.validated_txns.iter() {
            block_policy.nonce_check_and_update(txn, &mut account_nonces)?;
            if txn.is_eip7702() {
                block_policy.eip_7702_valid_nonce_update(
                    &txn.authorizations_7702,
                    &mut account_nonces,
                    CHAIN_ID,
                );
            }
        }

        Ok(())
    }

    fn setup_block_policy_with_txs(
        txs: BTreeMap<u64, Vec<Recovered<TxEnvelope>>>,
        signers: Vec<Address>,
        state_backend: &impl StateBackend<SignatureType, SignatureCollectionType>,
        num_committed_blocks: usize,
        coherency_check_mode: CoherencyCheckMode,
    ) -> Result<(), BlockPolicyError> {
        let mut block_policy = EthBlockPolicy::<
            SignatureType,
            SignatureCollectionType,
            ChainConfigType,
            ChainRevisionType,
        >::new(SeqNum(17), EXEC_DELAY.0);

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
            BlockPolicy::<_, _, _, StateBackendType, _, _>::update_committed_block(
                &mut block_policy,
                block,
                &MockChainConfig::DEFAULT,
            );
        }

        // Last block is incoming_block
        // Remaining ones in the middle are extending_block
        let incoming_block = blocks[4].clone();
        let extending_blocks = blocks[num_committed_blocks..4].iter().collect();

        match coherency_check_mode {
            CoherencyCheckMode::ReserveBalanceCoherency => reserve_balance_coherency(
                block_policy,
                incoming_block,
                extending_blocks,
                state_backend,
                signers,
            ),
            CoherencyCheckMode::NonceCoherency => nonce_coherency(
                block_policy,
                incoming_block,
                extending_blocks,
                state_backend,
                signers,
            ),
        }
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
        let gas_cost = 50000 * BASE_FEE as u128;
        let state_backend = NopStateBackend {
            balances: BTreeMap::from([(signer, U256::from(gas_cost))]),
            ..Default::default()
        };

        let result = setup_block_policy_with_txs(
            txs.clone(),
            vec![signer],
            &state_backend,
            num_committed_blocks,
            CoherencyCheckMode::ReserveBalanceCoherency,
        );
        assert!(result.is_ok(), "Block coherency check failed: {:?}", result);

        // should return error if fall below minimum balance
        let state_backend = NopStateBackend {
            balances: BTreeMap::from([(signer, U256::from(gas_cost - 1))]),
            ..Default::default()
        };
        let result = setup_block_policy_with_txs(
            txs,
            vec![signer],
            &state_backend,
            num_committed_blocks,
            CoherencyCheckMode::ReserveBalanceCoherency,
        );
        assert_eq!(
            result,
            Err(BlockPolicyError::BlockPolicyBlockValidatorError(
                BlockPolicyBlockValidatorError::InsufficientBalance
            )),
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

        let result = setup_block_policy_with_txs(
            txs,
            vec![signer],
            &state_backend,
            num_committed_blocks,
            CoherencyCheckMode::ReserveBalanceCoherency,
        );
        assert!(result.is_ok(), "Block coherency check failed: {:?}", result);

        // first tx dips into reserve balance, second tx has gas cost more than balance estimate
        let tx1 = make_test_tx(50000, ONE_ETHER, 0, S1);
        let tx2 = make_test_tx(50000, HALF_ETHER, 1, S1);
        let txs = BTreeMap::from([(4, vec![tx1, tx2])]); // txs in block n

        // balance of signer at block n-3
        let gas_cost = 50000 * BASE_FEE as u128;
        let balance = ONE_ETHER + (2 * gas_cost) - 1;
        let state_backend = NopStateBackend {
            balances: BTreeMap::from([(signer, U256::from(balance))]),
            ..Default::default()
        };

        let result = setup_block_policy_with_txs(
            txs,
            vec![signer],
            &state_backend,
            num_committed_blocks,
            CoherencyCheckMode::ReserveBalanceCoherency,
        );
        assert_eq!(
            result,
            Err(BlockPolicyError::BlockPolicyBlockValidatorError(
                BlockPolicyBlockValidatorError::InsufficientReserveBalance
            )),
            "Block coherency check should have failed: {:?}",
            result
        );

        // first tx doesn't dip into reserve balance, second tx has gas cost more than max reserve balance
        let tx1 = make_test_tx(50000, ONE_ETHER, 0, S1);
        let gas_limit = RESERVE_BALANCE as u64 / BASE_FEE + 1;
        let tx2 = make_test_tx(gas_limit, HALF_ETHER, 1, S1);
        let txs = BTreeMap::from([(4, vec![tx1, tx2])]); // txs in block n

        // balance of signer at block n-3
        let balance = 100 * ONE_ETHER;
        let state_backend = NopStateBackend {
            balances: BTreeMap::from([(signer, U256::from(balance))]),
            ..Default::default()
        };

        let result = setup_block_policy_with_txs(
            txs,
            vec![signer],
            &state_backend,
            num_committed_blocks,
            CoherencyCheckMode::ReserveBalanceCoherency,
        );
        assert_eq!(
            result,
            Err(BlockPolicyError::BlockPolicyBlockValidatorError(
                BlockPolicyBlockValidatorError::InsufficientReserveBalance
            )),
            "Block coherency check should have failed: {:?}",
            result
        );

        // first tx doesn't dip into reserve balance, second tx has gas cost equal to max reserve balance
        let tx1 = make_test_tx(50000, 0, 0, S1);
        let gas_limit = RESERVE_BALANCE as u64 / BASE_FEE;
        let tx2 = make_test_tx(gas_limit, HALF_ETHER, 1, S1);
        let txs = BTreeMap::from([(4, vec![tx1, tx2])]); // txs in block n

        // balance of signer at block n-3
        let first_tx_gas_cost = 50000 * BASE_FEE as u128;
        let second_tx_gas_cost = RESERVE_BALANCE;
        let balance = first_tx_gas_cost + second_tx_gas_cost;
        let state_backend = NopStateBackend {
            balances: BTreeMap::from([(signer, U256::from(balance))]),
            ..Default::default()
        };

        let result = setup_block_policy_with_txs(
            txs,
            vec![signer],
            &state_backend,
            num_committed_blocks,
            CoherencyCheckMode::ReserveBalanceCoherency,
        );
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

        let result = setup_block_policy_with_txs(
            txs,
            vec![signer],
            &state_backend,
            num_committed_blocks,
            CoherencyCheckMode::ReserveBalanceCoherency,
        );
        assert!(result.is_ok(), "Block coherency check failed: {:?}", result);

        // first tx dips into reserve balance, second tx has gas cost more than balance estimate
        let tx1 = make_test_tx(50000, ONE_ETHER, 0, S1);
        let tx2 = make_test_tx(50000, HALF_ETHER, 1, S1);
        // first tx in block n-2, second tx in block n
        let txs = BTreeMap::from([(2, vec![tx1]), (4, vec![tx2])]);

        // balance of signer at block n-3
        let gas_cost = 50000 * BASE_FEE as u128;
        let balance = ONE_ETHER + (2 * gas_cost) - 1;
        let state_backend = NopStateBackend {
            balances: BTreeMap::from([(signer, U256::from(balance))]),
            ..Default::default()
        };

        let result = setup_block_policy_with_txs(
            txs,
            vec![signer],
            &state_backend,
            num_committed_blocks,
            CoherencyCheckMode::ReserveBalanceCoherency,
        );
        assert_eq!(
            result,
            Err(BlockPolicyError::BlockPolicyBlockValidatorError(
                BlockPolicyBlockValidatorError::InsufficientReserveBalance
            )),
            "Block coherency check should have failed: {:?}",
            result
        );

        // first tx doesn't dip into reserve balance, second tx has gas cost more than max reserve balance
        let tx1 = make_test_tx(50000, ONE_ETHER, 0, S1);
        let gas_limit = RESERVE_BALANCE as u64 / BASE_FEE + 1;
        let tx2 = make_test_tx(gas_limit, HALF_ETHER, 1, S1);
        // first tx in block n-2, second tx in block n
        let txs = BTreeMap::from([(2, vec![tx1]), (4, vec![tx2])]);

        // balance of signer at block n-3
        let balance = 100 * ONE_ETHER;
        let state_backend = NopStateBackend {
            balances: BTreeMap::from([(signer, U256::from(balance))]),
            ..Default::default()
        };

        let result = setup_block_policy_with_txs(
            txs,
            vec![signer],
            &state_backend,
            num_committed_blocks,
            CoherencyCheckMode::ReserveBalanceCoherency,
        );
        assert_eq!(
            result,
            Err(BlockPolicyError::BlockPolicyBlockValidatorError(
                BlockPolicyBlockValidatorError::InsufficientReserveBalance
            )),
            "Block coherency check should have failed: {:?}",
            result
        );

        // first tx doesn't dip into reserve balance, second tx has gas cost equal to max reserve balance
        let tx1 = make_test_tx(50000, 0, 0, S1);
        let gas_limit = RESERVE_BALANCE as u64 / BASE_FEE;
        let tx2 = make_test_tx(gas_limit, HALF_ETHER, 1, S1);
        // first tx in block n-2, second tx in block n
        let txs = BTreeMap::from([(2, vec![tx1]), (4, vec![tx2])]);

        // balance of signer at block n-3
        let first_tx_gas_cost = 50000 * BASE_FEE as u128;
        let second_tx_gas_cost = RESERVE_BALANCE;
        let balance = first_tx_gas_cost + second_tx_gas_cost;
        let state_backend = NopStateBackend {
            balances: BTreeMap::from([(signer, U256::from(balance))]),
            ..Default::default()
        };

        let result = setup_block_policy_with_txs(
            txs,
            vec![signer],
            &state_backend,
            num_committed_blocks,
            CoherencyCheckMode::ReserveBalanceCoherency,
        );
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
        let gas_cost = 50000 * 2 * BASE_FEE as u128;
        let state_backend = NopStateBackend {
            balances: BTreeMap::from([(signer, U256::from(gas_cost))]),
            ..Default::default()
        };

        let result = setup_block_policy_with_txs(
            txs,
            vec![signer],
            &state_backend,
            num_committed_blocks,
            CoherencyCheckMode::ReserveBalanceCoherency,
        );
        assert!(result.is_ok(), "Block coherency check failed: {:?}", result);

        // transactions exceed balance estimate
        let tx1 = make_test_tx(50000, ONE_ETHER, 0, S1);
        let tx2 = make_test_tx(50000, HALF_ETHER, 1, S1);
        let tx3 = make_test_tx(50001, HALF_ETHER, 2, S1);
        // first tx in block n-3, second tx in block n-2, third tx in block n
        let txs = BTreeMap::from([(1, vec![tx1]), (2, vec![tx2]), (4, vec![tx3])]);

        // balance of signer at block n-3
        let gas_cost = 50000 * 2 * BASE_FEE as u128;
        let state_backend = NopStateBackend {
            balances: BTreeMap::from([(signer, U256::from(gas_cost))]),
            ..Default::default()
        };

        let result = setup_block_policy_with_txs(
            txs,
            vec![signer],
            &state_backend,
            num_committed_blocks,
            CoherencyCheckMode::ReserveBalanceCoherency,
        );
        assert_eq!(
            result,
            Err(BlockPolicyError::BlockPolicyBlockValidatorError(
                BlockPolicyBlockValidatorError::InsufficientReserveBalance
            )),
            "Block coherency check should have failed: {:?}",
            result
        );

        // transactions exceed max reserve balance
        let tx1 = make_test_tx(50000, ONE_ETHER, 0, S1);
        let tx2 = make_test_tx(50000, HALF_ETHER, 1, S1);
        let tx2_gas_cost = 50000 * BASE_FEE as u128;
        let tx3_gas_cost = RESERVE_BALANCE - tx2_gas_cost;
        let gas_limit = tx3_gas_cost as u64 / BASE_FEE;
        let tx3 = make_test_tx(gas_limit + 1, HALF_ETHER, 2, S1);
        // first tx in block n-3, second tx in block n-2, third tx in block n
        let txs = BTreeMap::from([(1, vec![tx1]), (2, vec![tx2]), (4, vec![tx3])]);

        // balance of signer at block n-3
        let balance = 100 * ONE_ETHER;
        let state_backend = NopStateBackend {
            balances: BTreeMap::from([(signer, U256::from(balance))]),
            ..Default::default()
        };

        let result = setup_block_policy_with_txs(
            txs,
            vec![signer],
            &state_backend,
            num_committed_blocks,
            CoherencyCheckMode::ReserveBalanceCoherency,
        );
        assert_eq!(
            result,
            Err(BlockPolicyError::BlockPolicyBlockValidatorError(
                BlockPolicyBlockValidatorError::InsufficientReserveBalance
            )),
            "Block coherency check should have failed: {:?}",
            result
        );

        //////////////////////////////////////////////////////////////////////////////////////////////////////
        // Case5: Non-emptying transaction (7702 delegations) + another transaction in different block     ///
        //////////////////////////////////////////////////////////////////////////////////////////////////////

        // first tx has an authorization from the signer
        let tx1 = make_test_delegation_tx(
            50000,
            0,
            0,
            S2,
            HashMap::from([(
                S1,
                Authorization {
                    chain_id: CHAIN_ID,
                    nonce: 0,
                    address: Address(FixedBytes([0x11; 20])),
                },
            )]),
        );
        let tx2 = make_test_tx(50000, HALF_ETHER, 1, S1);
        let tx3 = make_test_tx(50000, HALF_ETHER, 2, S1);
        // first tx in block n-3, second tx in block n-2, third tx in block n
        let txs = BTreeMap::from([(1, vec![tx1]), (2, vec![tx2]), (4, vec![tx3])]);

        // balance of signer at block n-3
        let gas_cost = 50000 * 2 * BASE_FEE as u128;
        let state_backend = NopStateBackend {
            balances: BTreeMap::from([(signer, U256::from(gas_cost))]),
            ..Default::default()
        };

        let result = setup_block_policy_with_txs(
            txs,
            vec![signer],
            &state_backend,
            num_committed_blocks,
            CoherencyCheckMode::ReserveBalanceCoherency,
        );
        assert!(result.is_ok(), "Block coherency check failed: {:?}", result);

        //////////////////////////////////////////////////////////////////////////
        // Case6: Emptying transaction (with another 7702 delegations)         ///
        //////////////////////////////////////////////////////////////////////////

        // first tx has an authorization from the signer
        let tx1 = make_test_delegation_tx(
            50000,
            0,
            0,
            S2,
            HashMap::from([(
                S1,
                Authorization {
                    chain_id: CHAIN_ID,
                    nonce: 0,
                    address: Address(FixedBytes([0x11; 20])),
                },
            )]),
        );
        let tx2 = make_test_tx(50000, HALF_ETHER, 1, S1);
        let signer1 = tx1.signer();
        let signer2 = tx2.signer();
        let txs = BTreeMap::from([(4, vec![tx1, tx2])]);

        // balance of signer at block n-3
        let gas_cost = 50000 * BASE_FEE as u128;
        let state_backend = NopStateBackend {
            balances: BTreeMap::from([
                (signer1, U256::from(gas_cost)),
                (signer2, U256::from(gas_cost)),
            ]),
            ..Default::default()
        };

        let result = setup_block_policy_with_txs(
            txs,
            vec![signer1, signer2],
            &state_backend,
            num_committed_blocks,
            CoherencyCheckMode::ReserveBalanceCoherency,
        );
        assert!(result.is_ok(), "Block coherency check failed: {:?}", result);

        /////////////////////////////////////////////////////////////////////////////
        // Case7: Multiple 7702 delegations and attempted emptying transaction    ///
        /////////////////////////////////////////////////////////////////////////////

        // first tx has multiple delegation and undelegation authorization from the signer
        let tx1 = make_test_delegation_tx(
            50000,
            0,
            0,
            S2,
            HashMap::from([
                (
                    S1,
                    Authorization {
                        chain_id: CHAIN_ID,
                        nonce: 0,
                        address: Address(FixedBytes([0x11; 20])),
                    },
                ),
                (
                    S1,
                    Authorization {
                        chain_id: CHAIN_ID,
                        nonce: 1,
                        address: Address::ZERO,
                    },
                ),
            ]),
        );
        let tx2 = make_test_tx(50000, HALF_ETHER, 2, S1);
        let signer1 = tx1.signer();
        let signer2 = tx2.signer();
        let txs = BTreeMap::from([(4, vec![tx1, tx2])]);

        // balance of signer at block n-3
        let gas_cost = 50000 * BASE_FEE as u128;
        let state_backend = NopStateBackend {
            balances: BTreeMap::from([
                (signer1, U256::from(gas_cost)),
                (signer2, U256::from(gas_cost)),
            ]),
            ..Default::default()
        };

        let result = setup_block_policy_with_txs(
            txs,
            vec![signer1, signer2],
            &state_backend,
            num_committed_blocks,
            CoherencyCheckMode::ReserveBalanceCoherency,
        );
        assert!(result.is_ok(), "Block coherency check failed: {:?}", result);
    }

    #[test_case(3; "three committed blocks, one extending block")]
    #[test_case(0; "no committed blocks, four extending block")]
    fn test_check_nonce_coherency(num_committed_blocks: usize) {
        //////////////////////////////////////////////////////////////////
        // Case1: No 7702 txs                                          ///
        //////////////////////////////////////////////////////////////////

        let tx1 = make_test_tx(50000, 0, 0, S1);
        let tx2 = make_test_tx(50000, 0, 1, S1);
        let tx3 = make_test_tx(50000, 0, 2, S1);
        let tx4 = make_test_tx(50000, 0, 3, S1);
        let signer = tx1.signer();
        let txs = BTreeMap::from([(2, vec![tx1]), (3, vec![tx2, tx3]), (4, vec![tx4])]);

        // balance of signer at block n-3
        let state_backend = NopStateBackend {
            balances: BTreeMap::from([(signer, U256::from(ONE_ETHER))]),
            ..Default::default()
        };

        let result = setup_block_policy_with_txs(
            txs,
            vec![signer],
            &state_backend,
            num_committed_blocks,
            CoherencyCheckMode::NonceCoherency,
        );
        assert!(result.is_ok(), "Block coherency check failed: {:?}", result);

        //////////////////////////////////////////////////////////////////
        // Case2: 7702 txs in incoming block                           ///
        //////////////////////////////////////////////////////////////////

        // correct sequencing of nonces
        let tx1 = make_test_tx(50000, 0, 0, S1);
        let tx2 = make_test_delegation_tx(
            50000,
            0,
            0,
            S2,
            HashMap::from([(
                S1,
                Authorization {
                    chain_id: CHAIN_ID,
                    nonce: 1,
                    address: Address(FixedBytes([0x11; 20])),
                },
            )]),
        );
        let tx3 = make_test_tx(50000, 0, 2, S1);
        let signer1 = tx1.signer();
        let signer2 = tx2.signer();
        let txs = BTreeMap::from([(2, vec![tx1]), (4, vec![tx2, tx3])]);

        // balance of signer at block n-3
        let state_backend = NopStateBackend {
            balances: BTreeMap::from([(signer1, U256::from(ONE_ETHER))]),
            nonces: BTreeMap::from([(signer1, 0), (signer2, 0)]),
        };

        let result = setup_block_policy_with_txs(
            txs,
            vec![signer1, signer2],
            &state_backend,
            num_committed_blocks,
            CoherencyCheckMode::NonceCoherency,
        );
        assert!(result.is_ok(), "Block coherency check failed: {:?}", result);

        // incorrect sequencing of nonces -- tx3 has incorrect nonce
        let tx1 = make_test_tx(50000, 0, 0, S1);
        let tx2 = make_test_delegation_tx(
            50000,
            0,
            0,
            S2,
            HashMap::from([(
                S1,
                Authorization {
                    chain_id: CHAIN_ID,
                    nonce: 1,
                    address: Address(FixedBytes([0x11; 20])),
                },
            )]),
        );
        let tx3 = make_test_tx(50000, 0, 1, S1);
        let signer1 = tx1.signer();
        let signer2 = tx2.signer();
        let txs = BTreeMap::from([(2, vec![tx1]), (4, vec![tx2, tx3])]);

        // balance of signer at block n-3
        let state_backend = NopStateBackend {
            balances: BTreeMap::from([(signer1, U256::from(ONE_ETHER))]),
            nonces: BTreeMap::from([(signer1, 0), (signer2, 0)]),
        };

        let result = setup_block_policy_with_txs(
            txs,
            vec![signer1, signer2],
            &state_backend,
            num_committed_blocks,
            CoherencyCheckMode::NonceCoherency,
        );
        assert!(
            result.is_err(),
            "Block coherency check should have failed: {:?}",
            result
        );

        // incorrect nonce in authorization -- shouldn't affect coherency
        let tx1 = make_test_tx(50000, 0, 0, S1);
        let tx2 = make_test_delegation_tx(
            50000,
            0,
            0,
            S2,
            HashMap::from([(
                S1,
                Authorization {
                    chain_id: CHAIN_ID,
                    nonce: 2,
                    address: Address(FixedBytes([0x11; 20])),
                },
            )]),
        );
        let tx3 = make_test_tx(50000, 0, 1, S1);
        let signer1 = tx1.signer();
        let signer2 = tx2.signer();
        let txs = BTreeMap::from([(2, vec![tx1]), (4, vec![tx2, tx3])]);

        // balance of signer at block n-3
        let state_backend = NopStateBackend {
            balances: BTreeMap::from([(signer1, U256::from(ONE_ETHER))]),
            nonces: BTreeMap::from([(signer1, 0), (signer2, 0)]),
        };

        let result = setup_block_policy_with_txs(
            txs,
            vec![signer1, signer2],
            &state_backend,
            num_committed_blocks,
            CoherencyCheckMode::NonceCoherency,
        );
        assert!(result.is_ok(), "Block coherency check failed: {:?}", result);

        //////////////////////////////////////////////////////////////////
        // Case3: 7702 txs in committed and extending blocks           ///
        //////////////////////////////////////////////////////////////////

        let tx1 = make_test_tx(50000, 0, 0, S1);
        let tx2 = make_test_delegation_tx(
            50000,
            0,
            0,
            S2,
            HashMap::from([(
                S1,
                Authorization {
                    chain_id: CHAIN_ID,
                    nonce: 1,
                    address: Address(FixedBytes([0x11; 20])),
                },
            )]),
        );
        let signer1 = tx1.signer();
        let signer2 = tx2.signer();

        // coherent incoming block
        let tx3 = make_test_tx(50000, 0, 2, S1);
        let txs = BTreeMap::from([
            (2, vec![tx1.clone()]),
            (3, vec![tx2.clone()]),
            (4, vec![tx3]),
        ]);

        // balance of signer at block n-3
        let state_backend = NopStateBackend {
            balances: BTreeMap::from([(signer1, U256::from(ONE_ETHER))]),
            nonces: BTreeMap::from([(signer1, 0), (signer2, 0)]),
        };

        let result = setup_block_policy_with_txs(
            txs,
            vec![signer1, signer2],
            &state_backend,
            num_committed_blocks,
            CoherencyCheckMode::NonceCoherency,
        );
        assert!(result.is_ok(), "Block coherency check failed: {:?}", result);

        // incoherent incoming block
        let tx3 = make_test_tx(50000, 0, 3, S1);
        let txs = BTreeMap::from([(2, vec![tx1]), (3, vec![tx2]), (4, vec![tx3])]);

        // balance of signer at block n-3
        let state_backend = NopStateBackend {
            balances: BTreeMap::from([(signer1, U256::from(ONE_ETHER))]),
            nonces: BTreeMap::from([(signer1, 0), (signer2, 0)]),
        };

        let result = setup_block_policy_with_txs(
            txs,
            vec![signer1, signer2],
            &state_backend,
            num_committed_blocks,
            CoherencyCheckMode::NonceCoherency,
        );
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
        let mut buffer = CommittedBlkBuffer::<
            SignatureType,
            SignatureCollectionType,
            MockChainConfig,
            MockChainRevision,
        >::new(3);
        let block1 = CommittedBlock {
            block_id: BlockId(Hash(Default::default())),
            round: Round(0),
            epoch: Epoch(1),
            seq_num: SeqNum(1),
            timestamp_ns: 1,
            nonce_usages: NonceUsageMap {
                map: BTreeMap::from([
                    (address1, NonceUsage::Known(1)),
                    (address2, NonceUsage::Known(1)),
                ]),
            },
            fees: BlockTxnFeeStates {
                txn_fees: BTreeMap::from([
                    (
                        address1,
                        TxnFee {
                            first_txn_value: Balance::from(100),
                            first_txn_gas: Balance::from(10),
                            max_gas_cost: Balance::from(90),
                            max_txn_cost: Balance::ZERO,
                            valid_authorization: None,
                            authorization_nonce: None,
                        },
                    ),
                    (
                        address2,
                        TxnFee {
                            first_txn_value: Balance::from(200),
                            first_txn_gas: Balance::from(10),
                            max_gas_cost: Balance::from(190),
                            max_txn_cost: Balance::ZERO,
                            valid_authorization: None,
                            authorization_nonce: None,
                        },
                    ),
                ]),
            },
            base_fee: Some(BASE_FEE),
            base_fee_trend: Some(BASE_FEE_TREND),
            base_fee_moment: Some(BASE_FEE_MOMENT),
            block_gas_usage: 0, // not used in this test
        };

        let block2 = CommittedBlock {
            block_id: BlockId(Hash(Default::default())),
            round: Round(0),
            epoch: Epoch(1),
            timestamp_ns: 1,
            seq_num: SeqNum(2),
            nonce_usages: NonceUsageMap {
                map: BTreeMap::from([
                    (address1, NonceUsage::Known(2)),
                    (address3, NonceUsage::Known(1)),
                ]),
            },
            fees: BlockTxnFeeStates {
                txn_fees: BTreeMap::from([
                    (
                        address1,
                        TxnFee {
                            first_txn_value: Balance::from(150),
                            first_txn_gas: Balance::from(10),
                            max_gas_cost: Balance::from(140),
                            max_txn_cost: Balance::ZERO,
                            valid_authorization: None,
                            authorization_nonce: None,
                        },
                    ),
                    (
                        address3,
                        TxnFee {
                            first_txn_value: Balance::from(300),
                            first_txn_gas: Balance::from(10),
                            max_gas_cost: Balance::from(290),
                            max_txn_cost: Balance::ZERO,
                            valid_authorization: None,
                            authorization_nonce: None,
                        },
                    ),
                ]),
            },
            base_fee: Some(BASE_FEE),
            base_fee_trend: Some(BASE_FEE_TREND),
            base_fee_moment: Some(BASE_FEE_MOMENT),
            block_gas_usage: 0, // not used in this test
        };

        let block3 = CommittedBlock {
            block_id: BlockId(Hash(Default::default())),
            round: Round(0),
            epoch: Epoch(1),
            seq_num: SeqNum(3),
            timestamp_ns: 1,
            nonce_usages: NonceUsageMap {
                map: BTreeMap::from([
                    (address2, NonceUsage::Known(2)),
                    (address3, NonceUsage::Known(2)),
                ]),
            },
            fees: BlockTxnFeeStates {
                txn_fees: BTreeMap::from([
                    (
                        address2,
                        TxnFee {
                            first_txn_value: Balance::from(250),
                            first_txn_gas: Balance::from(10),
                            max_gas_cost: Balance::from(240),
                            max_txn_cost: Balance::ZERO,
                            valid_authorization: None,
                            authorization_nonce: None,
                        },
                    ),
                    (
                        address3,
                        TxnFee {
                            first_txn_value: Balance::from(350),
                            first_txn_gas: Balance::from(10),
                            max_gas_cost: Balance::from(0),
                            max_txn_cost: Balance::ZERO,
                            valid_authorization: None,
                            authorization_nonce: None,
                        },
                    ),
                ]),
            },
            base_fee: Some(BASE_FEE),
            base_fee_trend: Some(BASE_FEE_TREND),
            base_fee_moment: Some(BASE_FEE_MOMENT),
            block_gas_usage: 0, // not used in this test
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
            is_delegated: false,
        };
        let mut merged_nonce_usages = NonceUsageMap::default();
        let res = buffer.update_account_balance(
            &mut account_balance_address_1,
            &address1,
            EXEC_DELAY,
            SeqNum(4)..SeqNum(5),
            SeqNum(5)..,
            &MockChainConfig::DEFAULT,
            &mut merged_nonce_usages,
        );
        assert!(res.is_ok());

        let emptying_txn_check_block_range = SeqNum(2)..SeqNum(3);
        let reserve_balance_check_block_range = SeqNum(3)..;

        let mut account_balance_address_2 = AccountBalanceState {
            balance: Balance::from(250),
            block_seqnum_of_latest_txn: GENESIS_SEQ_NUM,
            remaining_reserve_balance: Balance::from(250),
            max_reserve_balance,
            is_delegated: false,
        };
        let mut merged_nonce_usages = NonceUsageMap::default();
        let res = buffer.update_account_balance(
            &mut account_balance_address_2,
            &address2,
            EXEC_DELAY,
            emptying_txn_check_block_range.clone(),
            reserve_balance_check_block_range.clone(),
            &MockChainConfig::DEFAULT,
            &mut merged_nonce_usages,
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
            is_delegated: false,
        };
        let mut merged_nonce_usages = NonceUsageMap::default();
        let res = buffer.update_account_balance(
            &mut account_balance_address_3,
            &address3,
            EXEC_DELAY,
            emptying_txn_check_block_range,
            reserve_balance_check_block_range,
            &MockChainConfig::DEFAULT,
            &mut merged_nonce_usages,
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

            let result = compute_txn_max_value(&tx_envelope, BASE_FEE);

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
        let min_balance = compute_txn_max_gas_cost(&tx, BASE_FEE);

        let mut account_balances: BTreeMap<&Address, AccountBalanceState> = BTreeMap::new();
        account_balances.insert(
            &signer,
            AccountBalanceState {
                balance: min_balance,
                remaining_reserve_balance: min_balance,
                block_seqnum_of_latest_txn: latest_seq_num,
                max_reserve_balance: reserve_balance,
                is_delegated: false,
            },
        );

        let validator = EthBlockPolicyBlockValidator::new(
            block_seq_num,
            EXEC_DELAY,
            BASE_FEE,
            &MockChainRevision::DEFAULT,
            &MonadExecutionRevision::LATEST,
        )
        .unwrap();

        for txn in txs.iter() {
            assert!(validator
                .try_add_transaction(&mut account_balances, txn)
                .is_ok());
        }

        let mut account_balances: BTreeMap<&Address, AccountBalanceState> = BTreeMap::new();
        account_balances.insert(
            &signer,
            AccountBalanceState {
                balance: min_balance - Balance::from(1),
                remaining_reserve_balance: min_balance,
                block_seqnum_of_latest_txn: latest_seq_num,
                max_reserve_balance: reserve_balance,
                is_delegated: false,
            },
        );

        let validator = EthBlockPolicyBlockValidator::new(
            block_seq_num,
            EXEC_DELAY,
            BASE_FEE,
            &MockChainRevision::DEFAULT,
            &MonadExecutionRevision::LATEST,
        )
        .unwrap();

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
        let min_balance = compute_txn_max_gas_cost(&tx, BASE_FEE);

        let mut account_balances: BTreeMap<&Address, AccountBalanceState> = BTreeMap::new();
        account_balances.insert(
            &signer,
            AccountBalanceState {
                balance: min_balance,
                remaining_reserve_balance: min_balance,
                block_seqnum_of_latest_txn: latest_seq_num,
                max_reserve_balance: reserve_balance,
                is_delegated: false,
            },
        );

        let validator = EthBlockPolicyBlockValidator::new(
            block_seq_num,
            EXEC_DELAY,
            BASE_FEE,
            &MockChainRevision::DEFAULT,
            &MonadExecutionRevision::LATEST,
        )
        .unwrap();

        for txn in txs.iter() {
            assert!(validator
                .try_add_transaction(&mut account_balances, txn)
                .is_ok());
        }

        let mut account_balances: BTreeMap<&Address, AccountBalanceState> = BTreeMap::new();
        account_balances.insert(
            &signer,
            AccountBalanceState {
                balance: min_balance,
                remaining_reserve_balance: min_balance - Balance::from(1),
                block_seqnum_of_latest_txn: latest_seq_num,
                max_reserve_balance: reserve_balance,
                is_delegated: false,
            },
        );

        let validator = EthBlockPolicyBlockValidator::new(
            block_seq_num,
            EXEC_DELAY,
            BASE_FEE,
            &MockChainRevision::DEFAULT,
            &MonadExecutionRevision::LATEST,
        )
        .unwrap();

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
        let min_balance = compute_txn_max_gas_cost(&tx, BASE_FEE);

        let address = Address(FixedBytes([0x11; 20]));

        let mut account_balances: BTreeMap<&Address, AccountBalanceState> = BTreeMap::new();
        account_balances.insert(
            &address,
            AccountBalanceState {
                balance: min_balance,
                remaining_reserve_balance: min_balance,
                block_seqnum_of_latest_txn: latest_seq_num,
                max_reserve_balance: reserve_balance,
                is_delegated: false,
            },
        );

        let validator = EthBlockPolicyBlockValidator::new(
            block_seq_num,
            EXEC_DELAY,
            BASE_FEE,
            &MockChainRevision::DEFAULT,
            &MonadExecutionRevision::LATEST,
        )
        .unwrap();

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
        let min_balance = compute_txn_max_value(&tx, BASE_FEE);

        // Empty reserve balance
        let mut account_balances: BTreeMap<&Address, AccountBalanceState> = BTreeMap::new();
        account_balances.insert(
            &signer,
            AccountBalanceState {
                balance: min_balance,
                remaining_reserve_balance: Balance::ZERO,
                block_seqnum_of_latest_txn: latest_seq_num,
                max_reserve_balance: reserve_balance,
                is_delegated: false,
            },
        );

        let validator = EthBlockPolicyBlockValidator::new(
            block_seq_num,
            EXEC_DELAY,
            BASE_FEE,
            &MockChainRevision::DEFAULT,
            &MonadExecutionRevision::LATEST,
        )
        .unwrap();

        for txn in txs.iter() {
            assert!(validator
                .try_add_transaction(&mut account_balances, txn)
                .is_ok());
        }

        // Overdraft
        let block_seq_num = latest_seq_num;
        let min_reserve = compute_txn_max_gas_cost(&tx, BASE_FEE);
        let mut account_balances: BTreeMap<&Address, AccountBalanceState> = BTreeMap::new();
        account_balances.insert(
            &signer,
            AccountBalanceState {
                balance: Balance::ZERO,
                remaining_reserve_balance: min_reserve,
                block_seqnum_of_latest_txn: latest_seq_num,
                max_reserve_balance: reserve_balance,
                is_delegated: false,
            },
        );

        let validator = EthBlockPolicyBlockValidator::new(
            block_seq_num,
            EXEC_DELAY,
            BASE_FEE,
            &MockChainRevision::DEFAULT,
            &MonadExecutionRevision::LATEST,
        )
        .unwrap();

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
        let min_balance =
            compute_txn_max_value(&tx1, BASE_FEE) + compute_txn_max_gas_cost(&tx2, BASE_FEE);

        let mut account_balances: BTreeMap<&Address, AccountBalanceState> = BTreeMap::new();
        account_balances.insert(
            &signer,
            AccountBalanceState {
                balance: min_balance,
                remaining_reserve_balance: Balance::ZERO,
                block_seqnum_of_latest_txn: latest_seq_num,
                max_reserve_balance: reserve_balance,
                is_delegated: false,
            },
        );

        let validator = EthBlockPolicyBlockValidator::new(
            block_seq_num,
            EXEC_DELAY,
            BASE_FEE,
            &MockChainRevision::DEFAULT,
            &MonadExecutionRevision::LATEST,
        )
        .unwrap();

        for txn in txs.iter() {
            assert!(validator
                .try_add_transaction(&mut account_balances, txn)
                .is_ok());
        }

        let min_reserve =
            compute_txn_max_gas_cost(&tx1, BASE_FEE) + compute_txn_max_gas_cost(&tx2, BASE_FEE);

        let mut account_balances: BTreeMap<&Address, AccountBalanceState> = BTreeMap::new();
        account_balances.insert(
            &signer,
            AccountBalanceState {
                balance: Balance::ZERO,
                remaining_reserve_balance: min_reserve,
                block_seqnum_of_latest_txn: latest_seq_num,
                max_reserve_balance: reserve_balance,
                is_delegated: false,
            },
        );

        let validator = EthBlockPolicyBlockValidator::new(
            latest_seq_num,
            EXEC_DELAY,
            BASE_FEE,
            &MockChainRevision::DEFAULT,
            &MonadExecutionRevision::LATEST,
        )
        .unwrap();

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

    const BALANCE_FAIL: Result<(), BlockPolicyError> =
        Err(BlockPolicyError::BlockPolicyBlockValidatorError(
            BlockPolicyBlockValidatorError::InsufficientBalance,
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
    // emptying txn case
    #[case(
        Balance::from(100),
        Balance::from(10),
        Balance::from(10),
        SeqNum(3),
        100_u128,
        100_u64,
        Ok(())
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
            is_delegated: false,
        };

        let txn = make_test_eip1559_tx(txn_value, 0, txn_gas_limit, S1);
        let signer = txn.recover_signer().unwrap();

        let mut account_balances: BTreeMap<&Address, AccountBalanceState> = BTreeMap::new();
        account_balances.insert(&signer, abs);

        let validator = EthBlockPolicyBlockValidator::new(
            block_seq_num,
            EXEC_DELAY,
            BASE_FEE,
            &MockChainRevision::DEFAULT,
            &MonadExecutionRevision::LATEST,
        )
        .unwrap();

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
    // attempt to empty, but other txns within k
    #[case(
        Balance::from(100),
        Balance::from(10),
        Balance::from(10),
        vec![(4_u128, 4_u64), (96_u128, 96_u64)],
        vec![SeqNum(1), SeqNum(3)],
        vec![Ok(()), RESERVE_FAIL],
    )]
    // successful attempt to empty because emptying is more than k
    #[case(
        Balance::from(100),
        Balance::from(10),
        Balance::from(10),
        vec![(4_u128, 4_u64), (96_u128, 0_u64)],
        vec![SeqNum(1), SeqNum(4)],
        vec![Ok(()), Ok(())],
    )]
    // single emptying txn
    #[case(
        Balance::from(100),
        Balance::from(10),
        Balance::from(10),
        vec![(100_u128, 4_u64)],
        vec![SeqNum(4)],
        vec![Ok(())],
    )]
    // Test transactions across multiple blocks with
    // enough cooling period (k gap between txns)
    // but reserve lower than fees
    #[case(
        Balance::from(100),
        Balance::from(1),
        Balance::from(1),
        vec![(3_u128, 2_u64), (3_u128, 2_u64)],
        vec![SeqNum(4), SeqNum(7)],
        vec![Ok(()), Ok(())],
    )]
    // Test with low reserve balance
    #[case(
        Balance::from(100),
        Balance::from(1),
        Balance::from(1),
        vec![(3_u128, 2_u64), (3_u128, 2_u64)],
        vec![SeqNum(4), SeqNum(4)],
        vec![Ok(()), RESERVE_FAIL],
    )]
    // Test inclusion but will revert in execution
    // first txn bring balance down to reserve (Ok)
    // second tries to spend all (fails)
    // third is allowed by consensus but expected to revert in execution
    // because it tries to dip into reserve
    #[case(
        Balance::from(1000),
        Balance::from(10),
        Balance::from(10),
        vec![(990_u128, 1_u64), (5_u128, 10_u64), (8_u128, 1_u64)],
        vec![SeqNum(4), SeqNum(5), SeqNum(6)],
        vec![Ok(()), RESERVE_FAIL, Ok(())],
    )]
    // Zero value transfer test
    // first txn is not potentially emptying
    #[case(
        Balance::from(20),
        Balance::from(10),
        Balance::from(10),
        vec![(0_u128, 5_u64), (0_u128, 5_u64), (0_u128, 5_u64)],
        vec![SeqNum(2), SeqNum(2), SeqNum(2)],
        vec![Ok(()), Ok(()), RESERVE_FAIL],
    )]
    // Reserve boundary test
    // first txn is not potentially emptying
    #[case(
        Balance::from(100),
        Balance::from(20),
        Balance::from(20),
        vec![(10_u128, 20_u64), (0_u128, 1_u64)],
        vec![SeqNum(2), SeqNum(2)],
        vec![Ok(()), RESERVE_FAIL],
    )]
    // Zero value transfer test
    // first txn is potentially emptying
    #[case(
        Balance::from(20),
        Balance::from(10),
        Balance::from(10),
        vec![(0_u128, 5_u64), (0_u128, 5_u64), (0_u128, 5_u64)],
        vec![SeqNum(3), SeqNum(3), SeqNum(3)],
        vec![Ok(()), Ok(()), Ok(())],
    )]
    // Reserve boundary test
    // first txn is potentially emptying
    #[case(
        Balance::from(100),
        Balance::from(20),
        Balance::from(20),
        vec![(10_u128, 20_u64), (0_u128, 1_u64)],
        vec![SeqNum(3), SeqNum(3)],
        vec![Ok(()), Ok(())],
    )]
    // Reserve boundary test with many txns
    // first txn is potentially emptying
    // the last txn in the list exceeds the reserve balance
    #[case(
        Balance::from(100),
        Balance::from(20),
        Balance::from(20),
        vec![(10_u128, 20_u64), (0_u128, 1_u64), (0_u128, 18_u64), (0_u128, 1_u64), (0_u128, 1_u64)],
        vec![SeqNum(3), SeqNum(3), SeqNum(3), SeqNum(3), SeqNum(3)],
        vec![Ok(()), Ok(()), Ok(()), Ok(()), RESERVE_FAIL],
    )]
    fn test_multi_txn_tfm(
        #[case] account_balance: Balance,
        #[case] reserve_balance: Balance,
        #[case] max_reserve_balance: Balance,
        #[case] txns: Vec<(u128, u64)>, // txn (value, gas_limit)
        #[case] txn_block_num: Vec<SeqNum>,
        #[case] expected: Vec<Result<(), BlockPolicyError>>,
    ) {
        multi_txn_tfm_helper(
            false,
            account_balance,
            reserve_balance,
            max_reserve_balance,
            txns,
            txn_block_num,
            expected,
        )
    }

    #[rstest]
    // attempt to exceed reserve after k blocks but will fail
    // because delegated
    #[case(
        Balance::from(100),
        Balance::from(10),
        Balance::from(10),
        vec![(20_u128, 15_u64)],
        vec![SeqNum(3)],
        vec![RESERVE_FAIL],
    )]
    // Reserve boundary test
    #[case(
        Balance::from(100),
        Balance::from(20),
        Balance::from(20),
        vec![(10_u128, 20_u64), (0_u128, 1_u64)],
        vec![SeqNum(3), SeqNum(3)],
        vec![Ok(()), RESERVE_FAIL],
    )]
    fn test_try_add_delegated_txns(
        #[case] account_balance: Balance,
        #[case] reserve_balance: Balance,
        #[case] max_reserve_balance: Balance,
        #[case] txns: Vec<(u128, u64)>, // txn (value, gas_limit)
        #[case] txn_block_num: Vec<SeqNum>,
        #[case] expected: Vec<Result<(), BlockPolicyError>>,
    ) {
        multi_txn_tfm_helper(
            true,
            account_balance,
            reserve_balance,
            max_reserve_balance,
            txns,
            txn_block_num,
            expected,
        )
    }

    fn multi_txn_tfm_helper(
        is_delegated: bool,
        account_balance: Balance,
        reserve_balance: Balance,
        max_reserve_balance: Balance,
        txns: Vec<(u128, u64)>, // txn (value, gas_limit)
        txn_block_num: Vec<SeqNum>,
        expected: Vec<Result<(), BlockPolicyError>>,
    ) {
        assert_eq!(txns.len(), expected.len());
        assert_eq!(txns.len(), txn_block_num.len());

        let abs = AccountBalanceState {
            balance: account_balance,
            remaining_reserve_balance: reserve_balance,
            block_seqnum_of_latest_txn: SeqNum(0),
            max_reserve_balance,
            is_delegated,
        };

        let txns = txns
            .iter()
            .enumerate()
            .map(|(nonce, (value, gas_limit))| {
                make_test_eip1559_tx(*value, nonce as u64, *gas_limit, S1)
            })
            .collect_vec();
        let signer = txns[0].recover_signer().unwrap();

        let mut account_balances: BTreeMap<&Address, AccountBalanceState> = BTreeMap::new();
        account_balances.insert(&signer, abs);

        for ((tx, expect), seqnum) in txns.into_iter().zip(expected).zip(txn_block_num) {
            check_txn_helper(seqnum, &mut account_balances, &tx, expect);
        }
    }

    fn check_txn_helper(
        block_seq_num: SeqNum,
        account_balances: &mut BTreeMap<&Address, AccountBalanceState>,
        txn: &Recovered<TxEnvelope>,
        expect: Result<(), BlockPolicyError>,
    ) {
        let validator = EthBlockPolicyBlockValidator::new(
            block_seq_num,
            EXEC_DELAY,
            BASE_FEE,
            &MockChainRevision::DEFAULT,
            &MonadExecutionRevision::LATEST,
        )
        .unwrap();

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

    fn make_txn_fees(
        first_txn_value: u64,
        first_txn_gas: u64,
        max_gas_cost: u64,
        is_valid_authorization: bool,
    ) -> TxnFee {
        TxnFee {
            first_txn_value: Balance::from(first_txn_value),
            first_txn_gas: Balance::from(first_txn_gas),
            max_gas_cost: Balance::from(max_gas_cost),
            max_txn_cost: Balance::ZERO,
            valid_authorization: Some(is_valid_authorization),
            authorization_nonce: None,
        }
    }

    fn apply_block_fees_helper(
        block_seq_num: SeqNum,
        account_balance: &mut AccountBalanceState,
        fees: &TxnFee,
        eth_address: &Address,
        expected_remaining_reserve: Balance,
        expected_is_delegated: bool,
        expect: Result<(), BlockPolicyError>,
    ) {
        let validator = EthBlockPolicyBlockValidator::new(
            block_seq_num,
            EXEC_DELAY,
            BASE_FEE,
            &MockChainRevision::DEFAULT,
            &MonadExecutionRevision::LATEST,
        )
        .unwrap();

        let nonce_usages = NonceUsageMap::default();
        assert_eq!(
            validator.try_apply_block_fees(account_balance, fees, eth_address, &nonce_usages),
            expect,
        );
        assert_eq!(
            account_balance.remaining_reserve_balance,
            expected_remaining_reserve
        );
        assert_eq!(account_balance.is_delegated, expected_is_delegated);
    }

    #[rstest]
    #[case( // Has emptying txn, insufficient balance
        Balance::from(100),
        Balance::from(10),
        Balance::from(10),
        SeqNum(1),
        vec![(1001, 1, 100)], // value is not checked
        vec![SeqNum(4)],
        vec![Balance::from(10)],
        vec![RESERVE_FAIL],
    )]
    #[case( // Has emptying txn, insufficient reserve
        Balance::from(100),
        Balance::from(10),
        Balance::from(10),
        SeqNum(1),
        vec![(100, 1, 100)],
        vec![SeqNum(4)],
        vec![Balance::from(10)],
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
            is_delegated: false,
        };

        let blk_fees = blk_fees
            .into_iter()
            .map(|x| make_txn_fees(x.0, x.1, x.2, false))
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
                false,
                expect,
            );
        }
    }

    #[test]
    fn test_resolve_authorizations_in_extending() {
        let block_policy = EthBlockPolicy::<
            SignatureType,
            SignatureCollectionType,
            ChainConfigType,
            ChainRevisionType,
        >::new(GENESIS_SEQ_NUM, EXEC_DELAY.0);

        let state_backend = NopStateBackend::default();

        let addresses = [secret_to_eth_address(S2)];

        // Valid authorization nonce increments nonce
        {
            let (s2, s2_nonce) = block_policy
                .get_account_base_nonces(
                    SeqNum(1),
                    &state_backend,
                    &vec![&make_test_block(
                        Round(1),
                        SeqNum(1),
                        vec![recover_tx(make_eip7702_tx(
                            S1,
                            BASE_FEE as u128,
                            0,
                            100_000,
                            0,
                            vec![make_signed_authorization(S2, secret_to_eth_address(S1), 0)],
                            0,
                        ))],
                    )],
                    addresses.iter(),
                )
                .unwrap()
                .pop_first()
                .unwrap();

            assert_eq!(s2, &secret_to_eth_address(S2));
            assert_eq!(s2_nonce, 1);
        }

        // Bad nonce invalidates authorization
        {
            let (s2, s2_nonce) = block_policy
                .get_account_base_nonces(
                    SeqNum(1),
                    &state_backend,
                    &vec![&make_test_block(
                        Round(1),
                        SeqNum(1),
                        vec![recover_tx(make_eip7702_tx(
                            S1,
                            BASE_FEE as u128,
                            0,
                            100_000,
                            0,
                            vec![make_signed_authorization(S2, secret_to_eth_address(S1), 1)],
                            0,
                        ))],
                    )],
                    addresses.iter(),
                )
                .unwrap()
                .pop_first()
                .unwrap();

            assert_eq!(s2, &secret_to_eth_address(S2));
            assert_eq!(s2_nonce, 0);
        }

        // Only resolve nonce in the last execution_delay blocks
        {
            let (s2, s2_nonce) = block_policy
                .get_account_base_nonces(
                    SeqNum(10),
                    &state_backend,
                    &vec![
                        // nonces beyond execution_delay should not be taken into account
                        &make_test_block(
                            Round(1),
                            SeqNum(7),
                            vec![recover_tx(make_eip7702_tx(
                                S1,
                                BASE_FEE as u128,
                                0,
                                100_000,
                                0,
                                vec![make_signed_authorization(S2, secret_to_eth_address(S1), 0)],
                                0,
                            ))],
                        ),
                        &make_test_block(Round(1), SeqNum(8), vec![]),
                        &make_test_block(Round(1), SeqNum(9), vec![]),
                    ],
                    addresses.iter(),
                )
                .unwrap()
                .pop_first()
                .unwrap();

            assert_eq!(s2, &secret_to_eth_address(S2));
            assert_eq!(s2_nonce, 0);
        }
    }

    #[rstest]
    #[case( // Has emptying txn, no auth in fly
        Balance::from(1),
        Balance::from(10),
        SeqNum(1),
        false,
        vec![(11, 1, 1, false)],
        vec![SeqNum(4)],
        vec![Balance::from(10_u64)],
        vec![false],
        vec![RESERVE_FAIL],
    )]
    #[case( // Has emptying txn, auth in fly
        Balance::from(1),
        Balance::from(10),
        SeqNum(1),
        true,
        vec![(11, 1, 1, false)],
        vec![SeqNum(4)],
        vec![Balance::from(8_u64)],
        vec![true],
        vec![Ok(())],
    )]
    #[case( // delegated in initial state, auth
        Balance::from(100),
        Balance::from(10),
        SeqNum(1),
        true,
        vec![(11, 1, 1, true)],
        vec![SeqNum(4)],
        vec![Balance::from(8_u64)],
        vec![true],
        vec![Ok(())],
    )]
    #[case( // delegated in initial state, no auth
        Balance::from(100),
        Balance::from(10),
        SeqNum(1),
        true,
        vec![(11, 1, 1, false)],
        vec![SeqNum(4)],
        vec![Balance::from(8_u64)],
        vec![true],
        vec![Ok(())],
    )]
    #[case( // delegated in initial state, auth
        Balance::from(100),
        Balance::from(10),
        SeqNum(1),
        false,
        vec![(11, 1, 10, true), (11, 1, 1, false)],
        vec![SeqNum(4), SeqNum(5)],
        vec![Balance::from(10_u64), Balance::from(8_u64)],
        vec![true, true],
        vec![RESERVE_FAIL, Ok(())],
    )]
    #[case( // delegated in initial state, auth
        Balance::from(1),
        Balance::from(10),
        SeqNum(1),
        false,
        vec![(11, 1, 2, true), (11, 1, 1, false), (4, 2, 0, false)],
        vec![SeqNum(1), SeqNum(2), SeqNum(5)],
        vec![Balance::from(7_u64), Balance::from(5_u64), Balance::from(3_u64)],
        vec![true, true, true],
        vec![Ok(()), Ok(()), Ok(())],
    )]
    #[case( // delegated in initial state, auth
        Balance::from(1),
        Balance::from(10),
        SeqNum(1),
        false,
        vec![(11, 1, 2, false), (11, 1, 1, false), (4, 2, 0, false)],
        vec![SeqNum(1), SeqNum(2), SeqNum(5)],
        vec![Balance::from(7_u64), Balance::from(5_u64), Balance::from(5_u64)],
        vec![false, false, false],
        vec![Ok(()), Ok(()), BALANCE_FAIL],
    )]

    fn test_compute_delegation(
        #[case] account_balance: Balance,
        #[case] reserve_balance: Balance,
        #[case] block_seqnum_of_latest_txn: SeqNum,
        #[case] is_delegated: bool,
        #[case] blk_fees: Vec<(u64, u64, u64, bool)>, // (first_txn_value, first_txn_gas, max_gas_cost, is_delegated)
        #[case] txn_block_num: Vec<SeqNum>,
        #[case] expected_remaining_reserve: Vec<Balance>,
        #[case] expected_is_delegated: Vec<bool>,
        #[case] expected: Vec<Result<(), BlockPolicyError>>,
    ) {
        assert_eq!(blk_fees.len(), expected.len());
        assert_eq!(blk_fees.len(), txn_block_num.len());

        let address = Address(FixedBytes([0x11; 20]));

        let mut account_balance = AccountBalanceState {
            balance: account_balance,
            remaining_reserve_balance: reserve_balance,
            max_reserve_balance: Balance::ZERO, // unused
            block_seqnum_of_latest_txn,
            is_delegated,
        };

        let blk_fees = blk_fees
            .into_iter()
            .map(|x| make_txn_fees(x.0, x.1, x.2, x.3))
            .collect_vec();

        for ((((fees, expect), seqnum), expected_remaining_reserve), expected_is_delegated) in
            blk_fees
                .into_iter()
                .zip(expected)
                .zip(txn_block_num)
                .zip(expected_remaining_reserve)
                .zip(expected_is_delegated)
        {
            apply_block_fees_helper(
                seqnum,
                &mut account_balance,
                &fees,
                &address,
                expected_remaining_reserve,
                expected_is_delegated,
                expect,
            );
        }
    }

    #[test]
    fn test_compute_txn_max_gas_cost_legacy() {
        let base_fee = 100_000_000_000; // 100 gwei
        let gas_price = 300_000_000_000; // 300 gwei
        let gas_limit = 100_000;

        let tx = make_legacy_tx(S1, gas_price, gas_limit, 0, 0);
        let max_gas_cost = compute_txn_max_gas_cost(&tx, base_fee);

        assert_eq!(max_gas_cost, Balance::from(gas_price * gas_limit as u128));
    }

    #[test]
    fn test_compute_txn_max_gas_cost_eip1559() {
        let base_fee = 100_000_000_000; // 100 gwei
        let max_fee_per_gas = 300_000_000_000;
        let max_priority_fee_per_gas = 2_000_000_000; // 2 gwei
        let gas_limit = 100_000;

        let tx = make_eip1559_tx(
            S1,
            max_fee_per_gas,
            max_priority_fee_per_gas,
            gas_limit,
            0,
            0,
        );
        let max_gas_cost = compute_txn_max_gas_cost(&tx, base_fee);

        assert_eq!(
            max_gas_cost,
            Balance::from((base_fee as u128 + max_priority_fee_per_gas) * gas_limit as u128)
        );
    }

    #[test]
    fn test_merged_nonce_usages_initialization() {
        // Test that merged_nonce_usages is initialized with account nonces from base block
        let address1 = secret_to_eth_address(S1);
        let address2 = secret_to_eth_address(S2);

        let mut merged_nonce_usages = NonceUsageMap::default();

        // Simulate initialization from account statuses
        merged_nonce_usages.add_known(address1, 5);
        merged_nonce_usages.add_known(address2, 10);

        // Verify nonces are stored correctly
        assert_eq!(
            merged_nonce_usages.get(&address1),
            Some(&NonceUsage::Known(5))
        );
        assert_eq!(
            merged_nonce_usages.get(&address2),
            Some(&NonceUsage::Known(10))
        );
    }

    #[test]
    fn test_nonce_usages_merge_with_previous_block() {
        // Test that nonce_usages correctly merges with previous block nonces
        // Note: merge_with_previous keeps Known values, merges into Possible values
        let address1 = secret_to_eth_address(S1);
        let address2 = secret_to_eth_address(S2);

        let mut merged_nonce_usages = NonceUsageMap::default();
        merged_nonce_usages.add_known(address1, 5);

        // Create a block with nonce usage
        let mut block_nonce_usages = NonceUsageMap::default();
        block_nonce_usages.add_known(address1, 6); // Won't override Known value
        block_nonce_usages.add_known(address2, 10); // New address will be added

        // Merge block nonces into merged_nonce_usages
        merged_nonce_usages.merge_with_previous_block(block_nonce_usages.map.iter());

        // Verify merged state - address1 keeps its Known(5), address2 gets added as Known(10)
        assert_eq!(
            merged_nonce_usages.get(&address1),
            Some(&NonceUsage::Known(5)), // Keeps original Known value
            "Known nonce values should not be overridden"
        );
        assert_eq!(
            merged_nonce_usages.get(&address2),
            Some(&NonceUsage::Known(10)),
            "New addresses should be added from previous block"
        );
    }

    #[test]
    fn test_try_apply_block_fees_with_nonce_usages() {
        // Test that try_apply_block_fees uses nonce_usages for authorization validation
        // The authorization_nonce is checked against the transaction sender's nonce
        let address = secret_to_eth_address(S1);

        let mut account_balance = AccountBalanceState {
            balance: Balance::from(ONE_ETHER),
            remaining_reserve_balance: Balance::from(ONE_ETHER),
            max_reserve_balance: Balance::from(RESERVE_BALANCE),
            block_seqnum_of_latest_txn: SeqNum(1),
            is_delegated: false,
        };

        let fees = TxnFee {
            first_txn_value: Balance::from(0),
            first_txn_gas: Balance::from(0),
            max_gas_cost: Balance::from(100_000),
            max_txn_cost: Balance::from(100_000),
            valid_authorization: Some(false), // Invalid authorization
            authorization_nonce: Some(5),
        };

        // Create nonce_usages with the transaction sender's nonce matching the authorization
        let mut nonce_usages = NonceUsageMap::default();
        nonce_usages.add_known(address, 5); // Sender's nonce matches authorization_nonce

        let validator = EthBlockPolicyBlockValidator::new(
            SeqNum(5),
            EXEC_DELAY,
            BASE_FEE,
            &MockChainRevision::DEFAULT,
            &MonadExecutionRevision::LATEST,
        )
        .unwrap();

        // Should set is_delegated to true because sender's nonce matches authorization_nonce
        let result =
            validator.try_apply_block_fees(&mut account_balance, &fees, &address, &nonce_usages);

        assert!(result.is_ok());
        assert!(
            account_balance.is_delegated,
            "Account should be marked as delegated when sender nonce matches authorization_nonce"
        );
    }

    #[test]
    fn test_try_apply_block_fees_with_mismatched_nonce() {
        // Test that authorization fails when nonce doesn't match
        let address = secret_to_eth_address(S1);
        let authority = secret_to_eth_address(S2);

        let mut account_balance = AccountBalanceState {
            balance: Balance::from(ONE_ETHER),
            remaining_reserve_balance: Balance::from(ONE_ETHER),
            max_reserve_balance: Balance::from(RESERVE_BALANCE),
            block_seqnum_of_latest_txn: SeqNum(1),
            is_delegated: false,
        };

        let fees = TxnFee {
            first_txn_value: Balance::from(0),
            first_txn_gas: Balance::from(0),
            max_gas_cost: Balance::from(100_000),
            max_txn_cost: Balance::from(100_000),
            valid_authorization: Some(false), // Invalid authorization
            authorization_nonce: Some(5),
        };

        // Create nonce_usages with different nonce
        let mut nonce_usages = NonceUsageMap::default();
        nonce_usages.add_known(authority, 10); // Different nonce

        let validator = EthBlockPolicyBlockValidator::new(
            SeqNum(5),
            EXEC_DELAY,
            BASE_FEE,
            &MockChainRevision::DEFAULT,
            &MonadExecutionRevision::LATEST,
        )
        .unwrap();

        let result =
            validator.try_apply_block_fees(&mut account_balance, &fees, &address, &nonce_usages);

        assert!(result.is_ok());
        assert!(
            !account_balance.is_delegated,
            "Account should NOT be marked as delegated"
        );
    }

    #[test]
    fn test_nonce_usages_accumulation_across_blocks() {
        // Test that nonce_usages accumulates correctly across multiple blocks
        // Note: merge keeps Known values from the current map (they represent later blocks)
        let address1 = secret_to_eth_address(S1);
        let address2 = secret_to_eth_address(S2);
        let address3 = Address::repeat_byte(0x03);

        // Start with base block nonces (earliest state)
        let mut merged_nonce_usages = NonceUsageMap::default();
        merged_nonce_usages.add_known(address1, 5);
        merged_nonce_usages.add_known(address2, 10);

        // Block 1 updates address1 (but merge won't override Known)
        let mut block1_nonces = NonceUsageMap::default();
        block1_nonces.add_known(address1, 6);
        merged_nonce_usages.merge_with_previous_block(block1_nonces.map.iter());

        // Block 2 updates address2 and adds address3
        let mut block2_nonces = NonceUsageMap::default();
        block2_nonces.add_known(address2, 11);
        block2_nonces.add_known(address3, 20); // New address
        merged_nonce_usages.merge_with_previous_block(block2_nonces.map.iter());

        // Verify final state - Known values from initialization are preserved
        assert_eq!(
            merged_nonce_usages.get(&address1),
            Some(&NonceUsage::Known(5)), // Keeps original (base block has priority)
            "Base block Known nonce should be preserved"
        );
        assert_eq!(
            merged_nonce_usages.get(&address2),
            Some(&NonceUsage::Known(10)), // Keeps original (base block has priority)
            "Base block Known nonce should be preserved"
        );
        assert_eq!(
            merged_nonce_usages.get(&address3),
            Some(&NonceUsage::Known(20)),
            "New addresses from later blocks should be added"
        );
    }

    #[test]
    fn test_update_account_balance_with_nonce_usages() {
        // Test that update_account_balance correctly uses and updates merged_nonce_usages
        let address = secret_to_eth_address(S1);

        let mut buffer = CommittedBlkBuffer::<
            SignatureType,
            SignatureCollectionType,
            ChainConfigType,
            ChainRevisionType,
        >::new(4);

        // Add some blocks to the buffer
        let block = make_test_block(Round(1), SeqNum(1), vec![]);
        buffer.update_committed_block(&block);

        let mut account_balance = AccountBalanceState {
            balance: Balance::from(ONE_ETHER),
            remaining_reserve_balance: Balance::from(ONE_ETHER),
            max_reserve_balance: Balance::from(RESERVE_BALANCE),
            block_seqnum_of_latest_txn: SeqNum(0),
            is_delegated: false,
        };

        let mut merged_nonce_usages = NonceUsageMap::default();
        merged_nonce_usages.add_known(address, 5);

        let result = buffer.update_account_balance(
            &mut account_balance,
            &address,
            EXEC_DELAY,
            SeqNum(1)..SeqNum(2),
            SeqNum(2)..,
            &MockChainConfig::DEFAULT,
            &mut merged_nonce_usages,
        );

        assert!(result.is_ok());
        // merged_nonce_usages should still be accessible and may have been updated
        assert!(merged_nonce_usages.get(&address).is_some());
    }

    #[test]
    fn test_possible_to_known_nonce_conversion_and_is_delegated() {
        // Test that Possible nonces are converted to Known when merged with previous Known nonce
        // and that is_delegated is set correctly based on the Known nonce
        let address = secret_to_eth_address(S1);

        // Start with a NonceUsageMap containing Possible nonces for an address
        let mut current_nonce_usages = NonceUsageMap::default();
        let possible_nonces = VecDeque::from(vec![6, 7, 8]);
        current_nonce_usages
            .map
            .insert(address, NonceUsage::Possible(possible_nonces));

        // Create a previous block with Known nonce
        let mut previous_block_nonces = NonceUsageMap::default();
        previous_block_nonces.add_known(address, 5);

        // Merge: Possible([6, 7, 8]) with previous Known(5) should convert to Known(8)
        // because 6 = 5+1, 7 = 6+1, 8 = 7+1 (all sequential)
        current_nonce_usages.merge_with_previous_block(previous_block_nonces.map.iter());

        // Verify the conversion to Known(8)
        assert_eq!(
            current_nonce_usages.get(&address),
            Some(&NonceUsage::Known(8)),
            "Possible nonces should convert to Known when merged with previous Known nonce"
        );

        // Now test that is_delegated is set correctly based on the Known nonce
        let mut account_balance = AccountBalanceState {
            balance: Balance::from(ONE_ETHER),
            remaining_reserve_balance: Balance::from(ONE_ETHER),
            max_reserve_balance: Balance::from(RESERVE_BALANCE),
            block_seqnum_of_latest_txn: SeqNum(1),
            is_delegated: false,
        };

        // Create fees with authorization_nonce matching the converted Known nonce
        let fees = TxnFee {
            first_txn_value: Balance::from(0),
            first_txn_gas: Balance::from(0),
            max_gas_cost: Balance::from(100_000),
            max_txn_cost: Balance::from(100_000),
            valid_authorization: Some(false), // Invalid authorization initially
            authorization_nonce: Some(8),     // Matches the converted Known(8)
        };

        let validator = EthBlockPolicyBlockValidator::new(
            SeqNum(5),
            EXEC_DELAY,
            BASE_FEE,
            &MockChainRevision::DEFAULT,
            &MonadExecutionRevision::LATEST,
        )
        .unwrap();

        // Apply fees with the converted nonce_usages
        let result = validator.try_apply_block_fees(
            &mut account_balance,
            &fees,
            &address,
            &current_nonce_usages,
        );

        assert!(result.is_ok());
        assert!(
            account_balance.is_delegated,
            "is_delegated should be true when Known nonce matches authorization_nonce"
        );
    }

    #[test]
    fn test_possible_to_known_partial_conversion() {
        // Test that Possible nonces are partially converted when not all are sequential
        let address = secret_to_eth_address(S1);

        // Start with Possible nonces that are NOT all sequential
        let mut current_nonce_usages = NonceUsageMap::default();
        let possible_nonces = VecDeque::from(vec![6, 7, 10]); // 10 is not sequential
        current_nonce_usages
            .map
            .insert(address, NonceUsage::Possible(possible_nonces));

        // First, test merging Possible with Possible to verify 10 is preserved
        let mut test_merge_possible = current_nonce_usages.clone();
        let mut previous_possible_nonces = NonceUsageMap::default();
        previous_possible_nonces
            .map
            .insert(address, NonceUsage::Possible(VecDeque::from(vec![3, 4])));

        test_merge_possible.merge_with_previous_block(previous_possible_nonces.map.iter());

        // After merging Possible with Possible, verify 10 is still in the result
        if let Some(NonceUsage::Possible(nonces)) = test_merge_possible.get(&address) {
            assert!(
                nonces.contains(&10),
                "Possible nonces should still contain 10 after merging Possible with Possible"
            );
            assert_eq!(
                nonces.clone(),
                VecDeque::from(vec![3, 4, 6, 7, 10]),
                "Merging Possible([6,7,10]) with Possible([3,4]) should prepend to get [3,4,6,7,10]"
            );
        } else {
            panic!("Expected Possible nonces after Possible-to-Possible merge");
        }

        // Create a previous block with Known nonce
        let mut previous_block_nonces = NonceUsageMap::default();
        previous_block_nonces.add_known(address, 5);

        // Merge: Possible([6, 7, 10]) with previous Known(5) should convert to Known(7)
        // because 6 = 5+1, 7 = 6+1, but 10 != 7+1 so it stops
        current_nonce_usages.merge_with_previous_block(previous_block_nonces.map.iter());

        // Verify the conversion to Known(7), not Known(10)
        assert_eq!(
            current_nonce_usages.get(&address),
            Some(&NonceUsage::Known(7)),
            "Possible nonces should convert to Known up to the first non-sequential nonce"
        );

        // Test that is_delegated is NOT set when authorization_nonce doesn't match
        let mut account_balance = AccountBalanceState {
            balance: Balance::from(ONE_ETHER),
            remaining_reserve_balance: Balance::from(ONE_ETHER),
            max_reserve_balance: Balance::from(RESERVE_BALANCE),
            block_seqnum_of_latest_txn: SeqNum(1),
            is_delegated: false,
        };

        // Create fees with authorization_nonce that doesn't match
        let fees = TxnFee {
            first_txn_value: Balance::from(0),
            first_txn_gas: Balance::from(0),
            max_gas_cost: Balance::from(100_000),
            max_txn_cost: Balance::from(100_000),
            valid_authorization: Some(false),
            authorization_nonce: Some(10), // Doesn't match Known(7)
        };

        let validator = EthBlockPolicyBlockValidator::new(
            SeqNum(5),
            EXEC_DELAY,
            BASE_FEE,
            &MockChainRevision::DEFAULT,
            &MonadExecutionRevision::LATEST,
        )
        .unwrap();

        let result = validator.try_apply_block_fees(
            &mut account_balance,
            &fees,
            &address,
            &current_nonce_usages,
        );

        assert!(result.is_ok());
        assert!(
            !account_balance.is_delegated,
            "is_delegated should be false when Known nonce doesn't match authorization_nonce"
        );
    }
}
