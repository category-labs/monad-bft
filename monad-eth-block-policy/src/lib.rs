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
    collections::{BTreeMap, BTreeSet},
    marker::PhantomData,
    ops::Deref,
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
use monad_eth_txpool_types::TransactionError;
use monad_eth_types::{EthAccount, EthExecutionProtocol, EthHeader, BASE_FEE_PER_GAS};
use monad_state_backend::{StateBackend, StateBackendError};
use monad_system_calls::SystemTransaction;
use monad_types::{Balance, BlockId, Nonce, Round, SeqNum, GENESIS_BLOCK_ID, GENESIS_SEQ_NUM};
use monad_validator::signature_collection::SignatureCollection;
use sorted_vector_map::SortedVectorMap;
use tracing::{debug, warn};

/// Retriever trait for account nonces from block(s)
pub trait AccountNonceRetrievable {
    fn get_account_nonces(&self) -> BTreeMap<Address, Nonce>;
}
pub enum ReserveBalanceCheck {
    Insert,
    Propose,
    Validate,
}

fn compute_intrinsic_gas(tx: &TxEnvelope) -> u64 {
    // base stipend
    let mut intrinsic_gas = 21000;

    // YP, Eqn. 60, first summation
    // 4 gas for each zero byte and 16 gas for each non zero byte
    let zero_data_len = tx.input().iter().filter(|v| **v == 0).count() as u64;
    let non_zero_data_len = tx.input().len() as u64 - zero_data_len;
    intrinsic_gas += zero_data_len * 4;
    // EIP-2028: Transaction data gas cost reduction (was originally 64 for non zero byte)
    intrinsic_gas += non_zero_data_len * 16;

    if tx.kind().is_create() {
        // adds 32000 to intrinsic gas if transaction is contract creation
        intrinsic_gas += 32000;
        // EIP-3860: Limit and meter initcode
        // Init code stipend for bytecode analysis
        intrinsic_gas += ((tx.input().len() as u64 + 31) / 32) * 2;
    }

    // EIP-2930
    let access_list = tx
        .access_list()
        .map(|list| list.0.as_slice())
        .unwrap_or(&[]);
    let accessed_slots: usize = access_list.iter().map(|item| item.storage_keys.len()).sum();
    // each address in access list costs 2400 gas
    intrinsic_gas += access_list.len() as u64 * 2400;
    // each storage key in access list costs 1900 gas
    intrinsic_gas += accessed_slots as u64 * 1900;

    intrinsic_gas
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

/// Stateless helper function to check validity of an Ethereum transaction
pub fn static_validate_transaction(
    tx: &TxEnvelope,
    chain_id: u64,
    proposal_gas_limit: u64,
    max_code_size: usize,
) -> Result<(), TransactionError> {
    // EIP-2 - verify that s is in the lower half of the curve
    if tx.signature().normalize_s().is_some() {
        return Err(TransactionError::UnsupportedTransactionType);
    }

    // EIP-155
    // We allow legacy transactions without chain_id specified to pass through
    if let Some(tx_chain_id) = tx.chain_id() {
        if tx_chain_id != chain_id {
            return Err(TransactionError::InvalidChainId);
        }
    }

    // EIP-1559
    if let Some(max_priority_fee) = tx.max_priority_fee_per_gas() {
        if max_priority_fee > tx.max_fee_per_gas() {
            return Err(TransactionError::MaxPriorityFeeTooHigh);
        }
    }

    // EIP-3860
    // max init_code is (2 * max_code_size)
    let max_init_code_size: usize = 2 * max_code_size;
    if tx.kind().is_create() && tx.input().len() > max_init_code_size {
        return Err(TransactionError::InitCodeLimitExceeded);
    }

    // YP eq. 62 - intrinsic gas validation
    let intrinsic_gas = compute_intrinsic_gas(tx);
    if tx.gas_limit() < intrinsic_gas {
        return Err(TransactionError::GasLimitTooLow);
    }

    if tx.gas_limit() > proposal_gas_limit {
        return Err(TransactionError::GasLimitTooHigh);
    }

    if tx.is_eip4844() {
        return Err(TransactionError::UnsupportedTransactionType);
    }

    if tx.is_eip7702() {
        match tx.authorization_list() {
            Some(auth_list) => {
                if auth_list.is_empty() {
                    return Err(TransactionError::InvalidSetCodeTx);
                }
            }
            None => return Err(TransactionError::InvalidSetCodeTx),
        }
    }

    Ok(())
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
    min_buffer_size: usize, // should be execution delay

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
        base_seq_num: &mut SeqNum,
        account_balance: &mut AccountBalanceState,
        eth_address: &Address,
        execution_delay: SeqNum,
    ) -> Result<(), BlockPolicyError> {
        let next_seq_num = *base_seq_num + SeqNum(1);

        // base_seq_num = N - k, where N is the proposed block and k is execution delay.
        // transactions inclusion starting block = N - k + 1
        // check for emptying txn starting block = N - 2k + 2
        let txn_inclusion_start = next_seq_num;
        let emptying_txn_check_start =
            (txn_inclusion_start + SeqNum(1)).max(execution_delay) - execution_delay;

        let emptying_txn_check_block_range =
            self.blocks.range(emptying_txn_check_start..next_seq_num);
        let block_range = self.blocks.range(txn_inclusion_start..);

        if emptying_txn_check_start > GENESIS_SEQ_NUM {
            account_balance.block_seqnum_of_latest_txn = emptying_txn_check_start - SeqNum(1);
        }

        debug!(
            ?base_seq_num,
            ?emptying_txn_check_start,
            ?txn_inclusion_start,
            ?account_balance,
            ?eth_address,
            "before update_account_balance"
        );

        for (iter_seq_num, block) in emptying_txn_check_block_range {
            assert!(*iter_seq_num == block.seq_num);
            if block.fees.get(eth_address).is_some()
                && account_balance.block_seqnum_of_latest_txn < block.seq_num
            {
                account_balance.block_seqnum_of_latest_txn = block.seq_num;
            }
        }

        for (iter_seq_num, block) in block_range {
            assert!(*iter_seq_num == block.seq_num);
            if let Some(block_txn_fees) = block.fees.get(eth_address) {
                let mut validator =
                    EthBlockPolicyBlockValidator::new(block.seq_num, execution_delay)?;
                debug!(
                    "applying fees for block {:?}, curr acc balance: {:?}",
                    block.seq_num, account_balance
                );
                validator.try_apply_block_fees(account_balance, &block_txn_fees, eth_address)?;
            }
            *base_seq_num = *iter_seq_num;
        }

        debug!(
            ?base_seq_num,
            ?account_balance,
            ?eth_address,
            "after update_account_balance"
        );

        Ok(())
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

pub struct EthBlockPolicyBlockState {
    fees: BTreeMap<Address, TxnFees>,
}

pub struct EthBlockPolicyBlockValidator {
    block_seq_num: SeqNum,
    execution_delay: SeqNum,
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
        // txn T is emptying if there is no "prior txn" i.e. a txn from the same sender sent from block P so that P >= block_number(T) - k + 1.
        let blocks_since_latest_txn = self
            .block_seq_num
            .max(account_balance.block_seqnum_of_latest_txn)
            - account_balance.block_seqnum_of_latest_txn;

        let has_emptying_transaction = !account_balance.is_delegated
            && blocks_since_latest_txn > (self.execution_delay - SeqNum(1));

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

        // txn T is emptying if there is no "prior txn" i.e. a txn from the same sender sent from block P so that P >= block_number(T) - k + 1.
        let blocks_since_latest_txn = self
            .block_seq_num
            .max(account_balance.block_seqnum_of_latest_txn)
            - account_balance.block_seqnum_of_latest_txn;

        if account_balance.is_delegated {
            debug!(
                seq_num =?self.block_seq_num,
                ?eth_address,
                "account is_delegated"
            );
        }
        let is_emptying_transaction = !account_balance.is_delegated
            && blocks_since_latest_txn > (self.execution_delay - SeqNum(1));

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
        Self {
            // Needs to be at least 2 * execution_delay to detect emptying transactions
            committed_cache: CommittedBlkBuffer::new((execution_delay * 2) as usize),
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
                            is_delegated: status.is_delegated,
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
                    // Apply Txn Fees for the txns from committed blocks
                    let mut next_seq_num = base_seq_num;
                    self.committed_cache.update_account_balance(
                        &mut next_seq_num,
                        &mut balance_state,
                        &address,
                        self.execution_delay,
                    )?;

                    next_seq_num += SeqNum(1);
                    // Apply Txn Fees for txns in extending blocks
                    if let Some(blocks) = extending_blocks {
                        // handle the case where base_seq_num is a pending block
                        let next_blocks = blocks
                            .iter()
                            .skip_while(move |block| block.get_seq_num() < next_seq_num);
                        for extending_block in next_blocks {
                            debug!(?next_seq_num, "looking for txn fees in extending block");
                            assert_eq!(next_seq_num, extending_block.get_seq_num());
                            if let Some(txn_fee) = extending_block.txn_fees.get(&address) {
                                debug!(?next_seq_num, ?address, ?txn_fee, "try_apply_block_fees");
                                let mut validator = EthBlockPolicyBlockValidator::new(
                                    next_seq_num,
                                    self.execution_delay,
                                )?;

                                validator.try_apply_block_fees(
                                    &mut balance_state,
                                    txn_fee,
                                    &address,
                                )?;
                            }
                            next_seq_num += SeqNum(1);
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
        debug!(?block, "check_coherency");

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

        // TODO fix this unnecessary copy into a new vec to generate an owned Address
        let mut authority_addresses: BTreeSet<Address> = BTreeSet::new();
        let mut tx_signers: Vec<Address> = Vec::new();
        let mut authority_nonces = BTreeMap::new();

        for tx_signer in block.validated_txns.iter().map(|txn| {
            if txn.is_eip7702() {
                if let Some(auth_list) = txn.authorization_list() {
                    for (result , nonce, code_address) in auth_list.iter().map(|a| (a.recover_authority(), a.nonce(), a.address())) {
                        match result {
                            Ok(authority) => {
                                debug!(eth_address = ?txn.signer(), ?code_address, ?nonce, ?authority, "Authority");
                                authority_addresses.insert(authority);

                                // First increment than validate against account's nonce+1.
                                authority_nonces
                                    .entry(authority)
                                    .or_insert(nonce);
                            }
                            Err(error) => {
                                warn!(?error, "Can not process authorization list");
                                return Err(BlockPolicyError::Eip7702Error);
                            }
                        }
                    }
                }
            };

            Ok(txn.signer())
        }) {
            match tx_signer {
                Ok(address) => tx_signers.push(address),
                Err(err) => {
                    return Err(err);
                }
            }
        }

        tx_signers.extend(authority_addresses.iter().cloned());

        let mut system_tx_signers = block.system_txns.iter().map(|txn| txn.signer());
        tx_signers.extend(&mut system_tx_signers);

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

        for authority in &authority_addresses {
            let account_balance = account_balances
                .get_mut(authority)
                .expect("account_balances should have been populated for delegated accounts");

            debug!(?authority, "Setting account to is_delegated: true");
            account_balance.is_delegated = true;
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

            if authority_addresses.contains(&eth_address) {
                let maybe_nonce = authority_nonces.get(&eth_address);
                match maybe_nonce {
                    Some(n) => {
                        if *n != txn_nonce + 1 {
                            warn!(nonce = ?*n, seq_num = ?block.get_seq_num(), ?eth_address, "authority nonce error");
                        }
                    }
                    None => {
                        warn!(seq_num = ?block.get_seq_num(), ?eth_address, "authority no nonce");
                    }
                }
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
    use std::str::FromStr;

    use alloy_consensus::{SignableTransaction, TxEip1559, TxLegacy};
    use alloy_primitives::{hex, Address, Bytes, FixedBytes, PrimitiveSignature, TxKind, B256};
    use alloy_signer::SignerSync;
    use alloy_signer_local::PrivateKeySigner;
    use monad_crypto::NopSignature;
    use monad_eth_testutil::{make_eip1559_tx_with_value, make_legacy_tx, recover_tx};
    use monad_eth_types::BASE_FEE_PER_GAS;
    use monad_testutil::signing::MockSignatures;
    use monad_types::{Hash, SeqNum};
    use proptest::{prelude::*, strategy::Just};
    use rstest::*;

    use super::*;

    type SignatureType = NopSignature;
    type SignatureCollectionType = MockSignatures<SignatureType>;

    const RESERVE_BALANCE: u128 = 1_000_000_000_000_000_000;
    const EXEC_DELAY: SeqNum = SeqNum(3);
    const S1: B256 = B256::new(hex!(
        "0ed2e19e3aca1a321349f295837988e9c6f95d4a6fc54cfab6befd5ee82662ad"
    ));

    fn sign_tx(signature_hash: &FixedBytes<32>) -> PrimitiveSignature {
        let secret_key = B256::repeat_byte(0xAu8).to_string();
        let signer = &secret_key.parse::<PrivateKeySigner>().unwrap();
        signer.sign_hash_sync(signature_hash).unwrap()
    }

    fn make_test_tx(value: u64, nonce: u64, signer: FixedBytes<32>) -> Recovered<TxEnvelope> {
        recover_tx(make_legacy_tx(
            signer,
            BASE_FEE_PER_GAS as u128,
            value,
            nonce,
            0,
        ))
    }

    #[test]
    fn test_compute_account_balance_state() {
        // setup test addresses
        let address1 = Address(FixedBytes([0x11; 20]));
        let address2 = Address(FixedBytes([0x22; 20]));
        let address3 = Address(FixedBytes([0x33; 20]));
        let address4 = Address(FixedBytes([0x44; 20]));

        let max_reserve_balance = Balance::from(RESERVE_BALANCE);

        // add committed blocks to buffer
        let mut buffer = CommittedBlkBuffer::<SignatureType, SignatureCollectionType>::new(3);
        let block0 = CommittedBlock {
            block_id: BlockId(Hash(Default::default())),
            round: Round(0),
            seq_num: SeqNum(0),
            nonces: BlockAccountNonce {
                nonces: BTreeMap::default(),
            },
            fees: BlockTxnFeeStates {
                txn_fees: BTreeMap::default(),
            },
        };
        let block1 = CommittedBlock {
            block_id: BlockId(Hash(Default::default())),
            round: Round(0),
            seq_num: SeqNum(1),
            nonces: BlockAccountNonce {
                nonces: BTreeMap::default(),
            },
            fees: BlockTxnFeeStates {
                txn_fees: BTreeMap::default(),
            },
        };
        let block2 = CommittedBlock {
            block_id: BlockId(Hash(Default::default())),
            round: Round(0),
            seq_num: SeqNum(2),
            nonces: BlockAccountNonce {
                nonces: BTreeMap::default(),
            },
            fees: BlockTxnFeeStates {
                txn_fees: BTreeMap::default(),
            },
        };
        let block3 = CommittedBlock {
            block_id: BlockId(Hash(Default::default())),
            round: Round(0),
            seq_num: SeqNum(3),
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
                            is_delegated: false,
                        },
                    ),
                    (
                        address2,
                        TxnFee {
                            first_txn_value: Balance::from(200),
                            first_txn_gas: Balance::from(10),
                            max_gas_cost: Balance::from(190),
                            is_delegated: false,
                        },
                    ),
                ]),
            },
        };

        let block4 = CommittedBlock {
            block_id: BlockId(Hash(Default::default())),
            round: Round(0),
            seq_num: SeqNum(4),
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
                            is_delegated: false,
                        },
                    ),
                    (
                        address3,
                        TxnFee {
                            first_txn_value: Balance::from(300),
                            first_txn_gas: Balance::from(10),
                            max_gas_cost: Balance::from(290),
                            is_delegated: false,
                        },
                    ),
                ]),
            },
        };

        let block5 = CommittedBlock {
            block_id: BlockId(Hash(Default::default())),
            round: Round(0),
            seq_num: SeqNum(5),
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
                            is_delegated: false,
                        },
                    ),
                    (
                        address3,
                        TxnFee {
                            first_txn_value: Balance::from(350),
                            first_txn_gas: Balance::from(0),
                            max_gas_cost: Balance::from(350),
                            is_delegated: false,
                        },
                    ),
                ]),
            },
        };

        let block6 = CommittedBlock {
            block_id: BlockId(Hash(Default::default())),
            round: Round(3),
            seq_num: SeqNum(6),
            nonces: BlockAccountNonce {
                nonces: BTreeMap::from([(address2, 3), (address3, 3)]),
            },
            fees: BlockTxnFeeStates {
                txn_fees: BTreeMap::from([
                    (
                        address2,
                        TxnFee {
                            first_txn_value: Balance::from(1250),
                            first_txn_gas: Balance::from(0),
                            max_gas_cost: Balance::from(0),
                            is_delegated: false,
                        },
                    ),
                    (
                        address4,
                        TxnFee {
                            first_txn_value: Balance::from(1350),
                            first_txn_gas: Balance::from(0),
                            max_gas_cost: Balance::from(0),
                            is_delegated: false,
                        },
                    ),
                ]),
            },
        };

        buffer.blocks.insert(SeqNum(0), block0);
        buffer.blocks.insert(SeqNum(1), block1);
        buffer.blocks.insert(SeqNum(2), block2);
        buffer.blocks.insert(SeqNum(3), block3);
        buffer.blocks.insert(SeqNum(4), block4);
        buffer.blocks.insert(SeqNum(5), block5);
        buffer.blocks.insert(SeqNum(6), block6);

        let mut account_balance = AccountBalanceState {
            balance: Balance::from(250),
            block_seqnum_of_latest_txn: GENESIS_SEQ_NUM,
            remaining_reserve_balance: Balance::from(250),
            max_reserve_balance,
            is_delegated: false,
        };
        let mut next_seq_num = SeqNum(2);
        let res = buffer.update_account_balance(
            &mut next_seq_num,
            &mut account_balance,
            &address1,
            EXEC_DELAY,
        );

        assert_eq!(
            res,
            Err(BlockPolicyError::BlockPolicyBlockValidatorError(
                BlockPolicyBlockValidatorError::InsufficientReserveBalance
            ))
        );

        assert_eq!(next_seq_num, SeqNum(3));
        assert_eq!(account_balance.block_seqnum_of_latest_txn, SeqNum(3));
        assert_eq!(account_balance.remaining_reserve_balance, Balance::from(50));

        let mut account_balance = AccountBalanceState {
            balance: Balance::from(250),
            block_seqnum_of_latest_txn: GENESIS_SEQ_NUM,
            remaining_reserve_balance: Balance::from(249),
            max_reserve_balance,
            is_delegated: false,
        };
        let mut next_seq_num = SeqNum(1);
        let res = buffer.update_account_balance(
            &mut next_seq_num,
            &mut account_balance,
            &address1,
            EXEC_DELAY,
        );
        assert_eq!(
            res,
            Err(BlockPolicyError::BlockPolicyBlockValidatorError(
                BlockPolicyBlockValidatorError::InsufficientReserveBalance
            ))
        );

        let mut account_balance = AccountBalanceState {
            balance: Balance::from(350),
            block_seqnum_of_latest_txn: GENESIS_SEQ_NUM,
            remaining_reserve_balance: Balance::from(350),
            max_reserve_balance,
            is_delegated: false,
        };
        let mut next_seq_num = SeqNum(1);
        let res = buffer.update_account_balance(
            &mut next_seq_num,
            &mut account_balance,
            &address1,
            EXEC_DELAY,
        );
        assert!(res.is_ok());
        assert_eq!(next_seq_num, SeqNum(6));
        assert_eq!(account_balance.block_seqnum_of_latest_txn, SeqNum(4));
        assert_eq!(account_balance.remaining_reserve_balance, Balance::ZERO);

        let mut account_balance = AccountBalanceState::new(max_reserve_balance);
        let mut next_seq_num = SeqNum(5);
        let res = buffer.update_account_balance(
            &mut next_seq_num,
            &mut account_balance,
            &address1,
            EXEC_DELAY,
        );
        assert!(res.is_ok());
        assert_eq!(next_seq_num, SeqNum(6));
        assert_eq!(account_balance.block_seqnum_of_latest_txn, SeqNum(4));

        let mut account_balance = AccountBalanceState {
            balance: Balance::from(650),
            block_seqnum_of_latest_txn: GENESIS_SEQ_NUM,
            remaining_reserve_balance: Balance::from(450),
            max_reserve_balance,
            is_delegated: false,
        };
        let mut next_seq_num = SeqNum(0);
        let res = buffer.update_account_balance(
            &mut next_seq_num,
            &mut account_balance,
            &address2,
            EXEC_DELAY,
        );
        assert!(res.is_ok());
        assert_eq!(next_seq_num, SeqNum(6));
        assert_eq!(account_balance.block_seqnum_of_latest_txn, SeqNum(6));

        let mut account_balance = AccountBalanceState {
            balance: Balance::from(950),
            block_seqnum_of_latest_txn: GENESIS_SEQ_NUM,
            remaining_reserve_balance: Balance::from(650),
            max_reserve_balance,
            is_delegated: false,
        };
        let mut next_seq_num = SeqNum(0);
        let res = buffer.update_account_balance(
            &mut next_seq_num,
            &mut account_balance,
            &address3,
            EXEC_DELAY,
        );
        assert!(res.is_ok());
        assert_eq!(next_seq_num, SeqNum(6));
        assert_eq!(account_balance.block_seqnum_of_latest_txn, SeqNum(5));

        // address that is not present in all blocks
        let mut account_balance = AccountBalanceState::new(max_reserve_balance);
        let mut next_seq_num = SeqNum(3);
        let res = buffer.update_account_balance(
            &mut next_seq_num,
            &mut account_balance,
            &address4,
            EXEC_DELAY,
        );
        assert!(res.is_ok());
        assert_eq!(next_seq_num, SeqNum(6));
        assert_eq!(account_balance.block_seqnum_of_latest_txn, SeqNum(6));
    }

    #[test]
    fn test_static_validate_transaction() {
        let address = Address(FixedBytes([0x11; 20]));
        const CHAIN_ID: u64 = 1337;
        const PROPOSAL_GAS_LIMIT: u64 = 300_000_000;

        // pre EIP-155 transaction with no chain id is allowed
        let tx_no_chain_id = TxLegacy {
            chain_id: None,
            nonce: 0,
            to: TxKind::Call(address),
            gas_price: 1000,
            gas_limit: 1_000_000,
            ..Default::default()
        };
        let signature = sign_tx(&tx_no_chain_id.signature_hash());
        let txn = tx_no_chain_id.into_signed(signature);

        let result = static_validate_transaction(&txn.into(), CHAIN_ID, PROPOSAL_GAS_LIMIT, 0x6000);
        assert!(matches!(result, Ok(())));

        // transaction with incorrect chain id
        let tx_invalid_chain_id = TxEip1559 {
            chain_id: CHAIN_ID - 1,
            nonce: 0,
            to: TxKind::Call(address),
            max_fee_per_gas: 1000,
            max_priority_fee_per_gas: 10,
            gas_limit: 1_000_000,
            ..Default::default()
        };
        let signature = sign_tx(&tx_invalid_chain_id.signature_hash());
        let txn = tx_invalid_chain_id.into_signed(signature);

        let result = static_validate_transaction(&txn.into(), CHAIN_ID, PROPOSAL_GAS_LIMIT, 0x6000);
        assert!(matches!(result, Err(TransactionError::InvalidChainId)));

        // contract deployment transaction with input data larger than 2 * 0x6000 (initcode limit)
        let input = vec![0; 2 * 0x6000 + 1];
        let tx_over_initcode_limit = TxEip1559 {
            chain_id: CHAIN_ID,
            nonce: 0,
            to: TxKind::Create,
            max_fee_per_gas: 10000,
            max_priority_fee_per_gas: 10,
            gas_limit: 1_000_000,
            input: input.into(),
            ..Default::default()
        };
        let signature = sign_tx(&tx_over_initcode_limit.signature_hash());
        let txn = tx_over_initcode_limit.into_signed(signature);

        let result = static_validate_transaction(&txn.into(), CHAIN_ID, PROPOSAL_GAS_LIMIT, 0x6000);
        assert!(matches!(
            result,
            Err(TransactionError::InitCodeLimitExceeded)
        ));

        // transaction with larger max priority fee than max fee per gas
        let tx_priority_fee_too_high = TxEip1559 {
            chain_id: CHAIN_ID,
            nonce: 0,
            to: TxKind::Call(address),
            max_fee_per_gas: 1000,
            max_priority_fee_per_gas: 10000,
            gas_limit: 1_000_000,
            input: vec![].into(),
            ..Default::default()
        };
        let signature = sign_tx(&tx_priority_fee_too_high.signature_hash());
        let txn = tx_priority_fee_too_high.into_signed(signature);

        let result = static_validate_transaction(&txn.into(), CHAIN_ID, PROPOSAL_GAS_LIMIT, 0x6000);
        assert!(matches!(
            result,
            Err(TransactionError::MaxPriorityFeeTooHigh)
        ));

        // transaction with gas limit lower than intrinsic gas
        let tx_gas_limit_too_low = TxEip1559 {
            chain_id: CHAIN_ID,
            nonce: 0,
            to: TxKind::Call(address),
            max_fee_per_gas: 1000,
            max_priority_fee_per_gas: 10,
            gas_limit: 20_000,
            input: vec![].into(),
            ..Default::default()
        };
        let signature = sign_tx(&tx_gas_limit_too_low.signature_hash());
        let txn = tx_gas_limit_too_low.into_signed(signature);

        let result = static_validate_transaction(&txn.into(), CHAIN_ID, PROPOSAL_GAS_LIMIT, 0x6000);
        assert!(matches!(result, Err(TransactionError::GasLimitTooLow)));

        // transaction with gas limit higher than block gas limit
        let tx_gas_limit_too_high = TxEip1559 {
            chain_id: CHAIN_ID,
            nonce: 0,
            to: TxKind::Call(address),
            max_fee_per_gas: 1000,
            max_priority_fee_per_gas: 10,
            gas_limit: PROPOSAL_GAS_LIMIT + 1,
            input: vec![].into(),
            ..Default::default()
        };
        let signature = sign_tx(&tx_gas_limit_too_high.signature_hash());
        let txn = tx_gas_limit_too_high.into_signed(signature);

        let result = static_validate_transaction(&txn.into(), CHAIN_ID, PROPOSAL_GAS_LIMIT, 0x6000);
        assert!(matches!(result, Err(TransactionError::GasLimitTooHigh)));
    }

    #[test]
    fn test_compute_intrinsic_gas() {
        const CHAIN_ID: u64 = 1337;
        let tx = TxEip1559 {
            chain_id: CHAIN_ID,
            nonce: 0,
            to: TxKind::Create,
            max_fee_per_gas: 1000,
            max_priority_fee_per_gas: 10,
            gas_limit: 1_000_000,
            input: Bytes::from_str("0x6040608081523462000414").unwrap(),
            ..Default::default()
        };
        let signature = sign_tx(&tx.signature_hash());
        let tx = tx.into_signed(signature);

        let result = compute_intrinsic_gas(&tx.into());
        assert_eq!(result, 53166);
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
    #[test]
    fn test_validate_emptying_txn() {
        let reserve_balance = Balance::from(RESERVE_BALANCE);
        let latest_seq_num = SeqNum(1000);
        let txn_value = 1000;
        let block_seq_num = latest_seq_num + EXEC_DELAY;

        let tx = make_test_tx(txn_value, 0, S1);
        let txs = vec![tx.clone()];
        let signer = tx.recover_signer().unwrap();
        let min_balance = compute_txn_max_value(&tx);

        let mut account_balances: BTreeMap<Address, AccountBalanceState> = BTreeMap::new();
        account_balances.insert(
            signer,
            AccountBalanceState {
                balance: min_balance,
                remaining_reserve_balance: min_balance,
                block_seqnum_of_latest_txn: latest_seq_num,
                max_reserve_balance: reserve_balance,
                is_delegated: false,
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
                is_delegated: false,
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
    fn test_validate_nonemptying_txn() {
        let reserve_balance = Balance::from(RESERVE_BALANCE);
        let latest_seq_num = SeqNum(1000);
        let txn_value = 1000;
        let block_seq_num = latest_seq_num + EXEC_DELAY - SeqNum(1);

        let tx = make_test_tx(txn_value, 0, S1);
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
                is_delegated: false,
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
                is_delegated: false,
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

        let tx = make_test_tx(txn_value, 0, S1);
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
                is_delegated: false,
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

        let tx = make_test_tx(txn_value, 0, S1);
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
                is_delegated: false,
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
                is_delegated: false,
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

        let tx1 = make_test_tx(txn_value, 0, S1);
        let tx2 = make_test_tx(txn_value * 2, 1, S1);
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
                is_delegated: false,
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
                is_delegated: false,
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

        let txn = make_test_eip1559_tx(txn_value, 0, txn_gas_limit, signer());
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
            is_delegated: false,
        };

        let txns = txns
            .iter()
            .enumerate()
            .map(|(nonce, (value, gas_limit))| {
                make_test_eip1559_tx(*value, nonce as u64, *gas_limit, signer())
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

    fn signer() -> B256 {
        B256::new(hex!(
            "0ed2e19e3aca1a321349f295837988e9c6f95d4a6fc54cfab6befd5ee82662ad"
        ))
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
            is_delegated: false,
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
            is_delegated: false,
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
