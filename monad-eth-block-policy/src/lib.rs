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
    signature_collection::SignatureCollection,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_txpool_types::TransactionError;
use monad_eth_types::{Balance, EthAccount, EthExecutionProtocol, EthHeader, Nonce};
use monad_state_backend::{StateBackend, StateBackendError};
use monad_types::{BlockId, Round, SeqNum, GENESIS_BLOCK_ID, GENESIS_ROUND, GENESIS_SEQ_NUM};
use sorted_vector_map::SortedVectorMap;
use tracing::{debug, trace, warn};

/// Retriever trait for account nonces from block(s)
pub trait AccountNonceRetrievable {
    fn get_account_nonces(&self) -> BTreeMap<Address, Nonce>;
}
pub enum ReserveBalanceCheck {
    Insert,
    Propose,
    Validate,
}

#[derive(Debug, Clone)]
pub struct TxnFee {
    pub max_cost: Balance,
    pub carriage_cost: Balance,
}
pub type TxnFees = BTreeMap<Address, TxnFee>;

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

#[allow(clippy::unnecessary_fallible_conversions)]
pub fn compute_txn_max_value(txn: &TxEnvelope) -> U256 {
    let txn_value = U256::try_from(txn.value()).unwrap();
    let gas_cost = U256::from(txn.gas_limit() as u128 * txn.max_fee_per_gas());

    txn_value.saturating_add(gas_cost)
}

pub fn compute_txn_carriage_cost(txn: &TxEnvelope) -> U256 {
    U256::from(txn.gas_limit() as u128 * txn.max_fee_per_gas())
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
        if let Some(auth_list) = tx.authorization_list() {
            if auth_list.is_empty() || auth_list.len() >= 20 {
                return Err(TransactionError::InvalidSetCodeTx);
            }
        } else {
            return Err(TransactionError::InvalidSetCodeTx);
        }
    }

    Ok(())
}

struct BlockLookupIndex {
    block_id: BlockId,
    seq_num: SeqNum,
    round: Round,
    is_finalized: bool,
}

#[derive(Debug, Clone)]
pub struct AccountBalanceState {
    pub balance: Balance,
    pub reserve_balance: Balance,
    pub is_delegated: bool,
    pub last_txn_seqnum: SeqNum,
}

impl AccountBalanceState {
    pub fn new_not_delegated() -> Self {
        AccountBalanceState {
            balance: Balance::ZERO,
            reserve_balance: Balance::ZERO,
            is_delegated: false,
            last_txn_seqnum: GENESIS_SEQ_NUM,
        }
    }
}

// address to not delegated and elegated accounts
pub type AccountBalanceStates = BTreeMap<Address, AccountBalanceState>;

/// A consensus block that has gone through the EthereumValidator and makes the decoded and
/// verified transactions available to access
#[derive(Debug, Clone)]
pub struct EthValidatedBlock<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    pub block: ConsensusFullBlock<ST, SCT, EthExecutionProtocol>,
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

    nonces: BlockAccountNonce,
    fees: BlockTxnFeeStates,
}

#[derive(Debug)]
struct CommittedBlkBuffer<ST, SCT> {
    blocks: SortedVectorMap<SeqNum, CommittedBlock>,
    size: usize, // should be execution delay

    _phantom: PhantomData<(ST, SCT)>,
}

struct CommittedTxnFeeResult {
    carriage_cost: Balance,
    max_cost: Balance,
    last_txn_seq_num: SeqNum,
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
        execution_delay: SeqNum,
    ) -> CommittedTxnFeeResult {
        let mut carriage_cost = Balance::ZERO;
        let mut max_cost = Balance::ZERO;
        let mut next_validate = base_seq_num + SeqNum(1);
        let mut last_txn_seq_num = GENESIS_SEQ_NUM;

        // Compute balance from blocks before the base
        let start_seq_num = next_validate.max(execution_delay) - execution_delay;
        for (&cache_seq_num, block) in self.blocks.range(start_seq_num..next_validate) {
            if block.fees.get(eth_address).is_some() {
                last_txn_seq_num = cache_seq_num;
            }
        }

        // start iteration from base_seq_num (non inclusive)
        for (&cache_seq_num, block) in self.blocks.range(next_validate..) {
            assert_eq!(next_validate, cache_seq_num);
            if let Some(txn_fee) = block.fees.get(eth_address) {
                carriage_cost = carriage_cost.saturating_add(txn_fee.carriage_cost);
                max_cost = max_cost.saturating_add(txn_fee.max_cost);
            }
            last_txn_seq_num = next_validate;
            next_validate += SeqNum(1);
        }

        CommittedTxnFeeResult {
            max_cost,
            carriage_cost,
            last_txn_seq_num,
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

        assert!(self
            .blocks
            .insert(
                block_number,
                CommittedBlock {
                    block_id: block.get_id(),
                    round: block.get_round(),
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

    /// Cost for including transaction in the consensus
    execution_delay: SeqNum,

    /// Chain ID
    chain_id: u64,

    reserve_balance: Balance,

    min_blocks_since_last_txn: SeqNum,
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
        reserve_balance: u128,
    ) -> Self {
        Self {
            committed_cache: CommittedBlkBuffer::new(execution_delay as usize),
            last_commit,
            execution_delay: SeqNum(execution_delay),
            chain_id,
            reserve_balance: Balance::from(reserve_balance),
            min_blocks_since_last_txn: SeqNum(execution_delay),
        }
    }

    /// returns account nonces at the start of the provided consensus block
    pub fn get_account_base_nonces<'a>(
        &self,
        consensus_block_seq_num: SeqNum,
        state_backend: &impl StateBackend,
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
            debug!(
                ?base_seq_num,
                last_commit = self.last_commit.0,
                "base seq num committed"
            );
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
            debug!(?base_seq_num, "base seq num proposed");
            let proposed_block = extending_blocks
                .iter()
                .find(|block| &block.get_seq_num() == base_seq_num)
                .expect("extending block doesn't exist");
            Ok(BlockLookupIndex {
                block_id: proposed_block.get_id(),
                seq_num: *base_seq_num,
                round: proposed_block.get_round(),
                is_finalized: false,
            })
        } else {
            Err(StateBackendError::NotAvailableYet)
        }
    }

    fn get_account_statuses<'a>(
        &self,
        state_backend: &impl StateBackend,
        extending_blocks: &Option<&Vec<&EthValidatedBlock<ST, SCT>>>,
        addresses: impl Iterator<Item = &'a Address>,
        base_seq_num: &SeqNum,
    ) -> Result<Vec<Option<EthAccount>>, StateBackendError> {
        let block_index = self.get_block_index(extending_blocks, base_seq_num)?;
        state_backend.get_account_statuses(
            &block_index.block_id,
            base_seq_num,
            &block_index.round,
            block_index.is_finalized,
            addresses,
        )
    }

    // Computes account balance available for the account
    pub fn compute_account_base_balances<'a>(
        &self,
        consensus_block_seq_num: SeqNum,
        state_backend: &impl StateBackend,
        extending_blocks: Option<&Vec<&EthValidatedBlock<ST, SCT>>>,
        addresses: impl Iterator<Item = &'a Address>,
    ) -> Result<BTreeMap<&'a Address, AccountBalanceState>, StateBackendError>
    where
        SCT: SignatureCollection,
    {
        debug!(?self.min_blocks_since_last_txn, ?consensus_block_seq_num, "compute_account_base_balances");

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
            .map(|maybe_status| {
                maybe_status.map_or(AccountBalanceState::new_not_delegated(), |status| {
                    AccountBalanceState {
                        balance: status.balance,
                        reserve_balance: status.balance.min(self.reserve_balance),
                        is_delegated: status.is_delegated,
                        last_txn_seqnum: base_seq_num, // most pessimistic assumption
                    }
                })
            })
            .collect_vec();

        let account_balances = addresses
            .into_iter()
            .zip_eq(account_balances)
            .map(|(address, mut balance_state)| {
                // Apply Txn Fees for the txns from committed blocks
                let CommittedTxnFeeResult {
                    mut max_cost,
                    mut carriage_cost,
                    mut last_txn_seq_num,
                    mut next_validate,
                } = self.committed_cache.compute_txn_fee(
                    base_seq_num,
                    address,
                    self.execution_delay,
                );

                let mut can_charge_into_reserve = !balance_state.is_delegated
                    && next_validate
                        >= (balance_state.last_txn_seqnum + self.min_blocks_since_last_txn);

                if balance_state.reserve_balance < carriage_cost {
                    panic!(
                        "Committed block with insufficient reserve balance: {:?} \
                            Transaction Carriage Cost Committed: {:?} \
                            consensus_block_seq_num: {:?} \
                            for address: {:?}",
                        balance_state, carriage_cost, consensus_block_seq_num, address
                    );
                }

                if !balance_state.is_delegated {
                    if !can_charge_into_reserve
                        && (balance_state
                            .balance
                            .saturating_sub(balance_state.reserve_balance)
                            < max_cost)
                        || can_charge_into_reserve && (balance_state.balance < max_cost)
                    {
                        warn!(
                            ?balance_state,
                            ?can_charge_into_reserve,
                            ?max_cost,
                            ?carriage_cost,
                            ?consensus_block_seq_num,
                            ?address,
                            "Committed block with insufficient balance",
                        );
                    }
                    balance_state.balance = balance_state.balance.saturating_sub(max_cost);
                } else {
                    // No checks for delegated accounts.
                }

                balance_state.reserve_balance -= carriage_cost;

                trace!(
                    ?balance_state,
                    ?can_charge_into_reserve,
                    ?max_cost,
                    ?carriage_cost,
                    ?consensus_block_seq_num,
                    ?next_validate,
                    ?address,
                    "AccountBalanceState compute committed block: updated balance",
                );

                // Apply Txn Fees for txns in extending blocks
                let mut max_cost_pending: Balance = Balance::ZERO;
                let mut carriage_cost_pending: Balance = Balance::ZERO;
                if let Some(blocks) = extending_blocks {
                    // handle the case where base_seq_num is a pending block
                    let next_blocks = blocks
                        .iter()
                        .skip_while(move |block| block.get_seq_num() < next_validate);
                    for extending_block in next_blocks {
                        assert_eq!(next_validate, extending_block.get_seq_num());
                        if let Some(txn_fee) = extending_block.txn_fees.get(address) {
                            max_cost_pending = max_cost_pending.saturating_add(txn_fee.max_cost);
                            carriage_cost_pending =
                                carriage_cost_pending.saturating_add(txn_fee.carriage_cost);
                            balance_state.last_txn_seqnum = next_validate;
                        }
                        next_validate += SeqNum(1);
                    }
                }

                if balance_state.reserve_balance < carriage_cost_pending {
                    panic!(
                        "Pending block with insufficient reserve balance: {:?} \
                            Transaction Carriage Cost Pending {:?} \
                            consensus block:seq num {:?} \
                            for address: {:?}",
                        balance_state, carriage_cost_pending, consensus_block_seq_num, address
                    );
                }

                can_charge_into_reserve = !balance_state.is_delegated
                    && next_validate
                        >= (balance_state.last_txn_seqnum + self.min_blocks_since_last_txn);

                if !balance_state.is_delegated {
                    if !can_charge_into_reserve
                        && (balance_state
                            .balance
                            .saturating_sub(balance_state.reserve_balance)
                            < max_cost_pending)
                        || can_charge_into_reserve && (balance_state.balance < max_cost_pending)
                    {
                        warn!(
                            ?balance_state,
                            ?carriage_cost_pending,
                            ?consensus_block_seq_num,
                            ?can_charge_into_reserve,
                            ?address,
                            "Pending block with insufficient balance"
                        );
                    }

                    balance_state.balance = balance_state.balance.saturating_sub(max_cost_pending);
                } else {
                    // No checks for delegated accounts.
                }

                balance_state.reserve_balance -= carriage_cost_pending;

                trace!(
                    ?balance_state,
                    ?can_charge_into_reserve,
                    ?max_cost,
                    ?carriage_cost,
                    ?consensus_block_seq_num,
                    ?next_validate,
                    ?address,
                    "AccountBalanceState compute pending block: updated balance",
                );

                (address, balance_state)
            })
            .collect();
        Ok(account_balances)
    }

    pub fn get_chain_id(&self) -> u64 {
        self.chain_id
    }
}

impl<ST, SCT, SBT> BlockPolicy<ST, SCT, EthExecutionProtocol, SBT> for EthBlockPolicy<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend,
{
    type ValidatedBlock = EthValidatedBlock<ST, SCT>;

    fn check_coherency(
        &self,
        block: &Self::ValidatedBlock,
        extending_blocks: Vec<&Self::ValidatedBlock>,
        blocktree_root: RootInfo,
        state_backend: &SBT,
    ) -> Result<Self::ValidatedBlock, BlockPolicyError> {
        trace!(?block, "check_coherency");
        let mut txn_fees = BTreeMap::default();

        let block_seq_num = block.get_seq_num();
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
                round =? block.header().round,
                "block not coherent, doesn't equal parent_seq_num + 1"
            );
            return Err(BlockPolicyError::BlockNotCoherent);
        }

        if block.get_timestamp() <= extending_timestamp {
            warn!(
                seq_num =? block.header().seq_num,
                round =? block.header().round,
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
                round =? block.header().round,
                ?expected_execution_results,
                block_execution_results =? block.get_execution_results(),
                "block not coherent, execution result mismatch"
            );
            return Err(BlockPolicyError::ExecutionResultMismatch);
        }

        // TODO fix this unnecessary copy into a new vec to generate an owned Address
        let mut delegated_addresses: Vec<Address> = Vec::new();
        let mut tx_signers: Vec<Address> = Vec::new();

        for tx_signer in block.validated_txns.iter().map(|txn| {
            if txn.is_eip7702() {
                if let Some(auth_list) = txn.authorization_list() {
                    for result in auth_list.iter().map(|a| a.recover_authority()) {
                        match result {
                            Ok(address) => {
                                delegated_addresses.push(address);
                            }
                            Err(error) => {
                                warn!(?error, "Can not process authorization list");
                                return Err(BlockPolicyError::BlockNotCoherent);
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

        for addr in delegated_addresses {
            debug!(?addr, "Adding delegated address");
            tx_signers.push(addr);
        }

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

        // TODO check for empty acct here
        for (idx, txn) in block.validated_txns.iter().enumerate() {
            let eth_address = txn.signer();
            let txn_nonce = txn.nonce();

            if txn.is_eip7702() {
                if let Some(auth_list) = txn.authorization_list() {
                    let authorities = auth_list
                        .iter()
                        .map(|a| (*a.address(), a.nonce(), a.address))
                        .collect_vec();
                    for (signer, nonce, address) in authorities {
                        let expected_delegated_nonce = account_nonces.get(&signer).expect(
                            "account_nonces should have been populated for delegated accounts",
                        );
                        if &nonce != expected_delegated_nonce {
                            warn!(
                                seq_num =? block.header().seq_num,
                                round =? block.header().round,
                                ?nonce,
                                "block not coherent, invalid nonce for delegated address"
                            );
                            return Err(BlockPolicyError::BlockNotCoherent);
                        }
                        let account_balance = account_balances.get_mut(&signer).expect(
                            "account_balances should have been populated for delegated accounts",
                        );

                        if address == Address::ZERO {
                            debug!(?address, ?account_balance.is_delegated, "Setting account is_delegated: false");
                            account_balance.is_delegated = false;
                        } else {
                            debug!(?address, ?account_balance.is_delegated, "Setting account is_delegated: true");
                            account_balance.is_delegated = true;
                        }
                    }
                }
            }

            let expected_nonce = account_nonces
                .get_mut(&eth_address)
                .expect("account_nonces should have been populated");

            if &txn_nonce != expected_nonce {
                warn!(
                    seq_num =? block.header().seq_num,
                    round =? block.header().round,
                    "block not coherent, invalid nonce"
                );
                return Err(BlockPolicyError::BlockNotCoherent);
            }

            let account_balance = account_balances
                .get_mut(&eth_address)
                .expect("account_balances should have been populated");

            // if an account is not delegated and had no transactions in the last execution_delay blocks, then can charge into reserve.
            let blocks_since_last_txn = block_seq_num.max(account_balance.last_txn_seqnum)
                - account_balance.last_txn_seqnum;

            let can_charge_into_reserve = 
                (!account_balance.is_delegated
                    && blocks_since_last_txn >= self.min_blocks_since_last_txn);
            let max_cost = compute_txn_max_value(txn);
            let carriage_cost = compute_txn_carriage_cost(txn);

            if account_balance.reserve_balance >= carriage_cost
                && (!can_charge_into_reserve
                    && (account_balance
                        .balance
                        .saturating_sub(account_balance.reserve_balance)
                        >= max_cost)
                    || can_charge_into_reserve && (account_balance.balance >= max_cost))
            {
                account_balance.balance -= max_cost;
                account_balance.reserve_balance -= carriage_cost;
                *expected_nonce += 1;
                trace!(
                    ?eth_address,
                    seq_num =? block.header().seq_num,
                    round =? block.header().round,
                    ?account_balance,
                    ?max_cost,
                    ?carriage_cost,
                    ?can_charge_into_reserve,
                    ?blocks_since_last_txn,
                    "AccountBalance - check_coherency 2"
                );
            } else {
                trace!(
                    ?eth_address,
                    seq_num =? block.header().seq_num,
                    round =? block.header().round,
                    ?account_balance,
                    ?max_cost,
                    ?carriage_cost,
                    ?can_charge_into_reserve,
                    ?blocks_since_last_txn,
                    "AccountBalance - check_coherency 3"
                );
                warn!(
                    seq_num =? block.header().seq_num,
                    round =? block.header().round,
                    ?account_balance,
                    ?max_cost,
                    ?carriage_cost,
                    ?can_charge_into_reserve,
                    "block not coherent, invalid balance"
                );
                return Err(BlockPolicyError::BlockNotCoherent);
            }

            let txn_fee_entry = txn_fees.entry(eth_address).or_insert(TxnFee {
                max_cost: Balance::ZERO,
                carriage_cost: Balance::ZERO,
            });

            txn_fee_entry.max_cost = txn_fee_entry.max_cost.saturating_add(max_cost);
            txn_fee_entry.carriage_cost = txn_fee_entry.carriage_cost.saturating_add(carriage_cost);
        }
        let mut res_block = block.clone();
        res_block.txn_fees = txn_fees;
        Ok(res_block)
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
            &block_index.round,
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
    use std::str::FromStr;

    use alloy_consensus::{SignableTransaction, TxEip1559, TxLegacy};
    use alloy_primitives::{Address, Bytes, FixedBytes, PrimitiveSignature, TxKind, B256};
    use alloy_signer::SignerSync;
    use alloy_signer_local::PrivateKeySigner;
    use monad_crypto::NopSignature;
    use monad_testutil::signing::MockSignatures;
    use monad_types::{Hash, SeqNum};

    use super::*;

    type SignatureType = NopSignature;
    type SignatureCollectionType = MockSignatures<SignatureType>;

    fn sign_tx(signature_hash: &FixedBytes<32>) -> PrimitiveSignature {
        let secret_key = B256::repeat_byte(0xAu8).to_string();
        let signer = &secret_key.parse::<PrivateKeySigner>().unwrap();
        signer.sign_hash_sync(signature_hash).unwrap()
    }

    const EXEC_DELAY: u64 = 3;

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
            nonces: BlockAccountNonce {
                nonces: BTreeMap::from([(address1, 1), (address2, 1)]),
            },
            fees: BlockTxnFeeStates {
                txn_fees: BTreeMap::from([
                    (
                        address1,
                        TxnFee {
                            max_cost: Balance::from(100),
                            carriage_cost: Balance::from(100),
                        },
                    ),
                    (
                        address2,
                        TxnFee {
                            max_cost: Balance::from(200),
                            carriage_cost: Balance::from(200),
                        },
                    ),
                ]),
            },
        };

        let block2 = CommittedBlock {
            block_id: BlockId(Hash(Default::default())),
            round: Round(0),
            nonces: BlockAccountNonce {
                nonces: BTreeMap::from([(address1, 2), (address3, 1)]),
            },
            fees: BlockTxnFeeStates {
                txn_fees: BTreeMap::from([
                    (
                        address1,
                        TxnFee {
                            max_cost: Balance::from(150),
                            carriage_cost: Balance::from(150),
                        },
                    ),
                    (
                        address3,
                        TxnFee {
                            max_cost: Balance::from(300),
                            carriage_cost: Balance::from(300),
                        },
                    ),
                ]),
            },
        };

        let block3 = CommittedBlock {
            block_id: BlockId(Hash(Default::default())),
            round: Round(0),
            nonces: BlockAccountNonce {
                nonces: BTreeMap::from([(address2, 2), (address3, 2)]),
            },
            fees: BlockTxnFeeStates {
                txn_fees: BTreeMap::from([
                    (
                        address2,
                        TxnFee {
                            max_cost: Balance::from(250),
                            carriage_cost: Balance::from(250),
                        },
                    ),
                    (
                        address3,
                        TxnFee {
                            max_cost: Balance::from(350),
                            carriage_cost: Balance::from(350),
                        },
                    ),
                ]),
            },
        };

        buffer.blocks.insert(SeqNum(1), block1);
        buffer.blocks.insert(SeqNum(2), block2);
        buffer.blocks.insert(SeqNum(3), block3);

        // test compute_txn_fee for different addresses and base sequence numbers
        assert_eq!(
            buffer
                .compute_txn_fee(SeqNum(0), &address1, SeqNum(EXEC_DELAY))
                .carriage_cost,
            U256::from(250)
        );
        assert_eq!(
            buffer
                .compute_txn_fee(SeqNum(1), &address1, SeqNum(EXEC_DELAY))
                .carriage_cost,
            U256::from(150)
        );
        assert_eq!(
            buffer
                .compute_txn_fee(SeqNum(2), &address1, SeqNum(EXEC_DELAY))
                .carriage_cost,
            U256::from(0)
        );

        assert_eq!(
            buffer
                .compute_txn_fee(SeqNum(0), &address2, SeqNum(EXEC_DELAY))
                .carriage_cost,
            U256::from(450)
        );
        assert_eq!(
            buffer
                .compute_txn_fee(SeqNum(0), &address3, SeqNum(EXEC_DELAY))
                .carriage_cost,
            U256::from(650)
        );

        // address that is not present in all blocks
        assert_eq!(
            buffer
                .compute_txn_fee(SeqNum(0), &address4, SeqNum(EXEC_DELAY))
                .carriage_cost,
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
            nonces: BlockAccountNonce {
                nonces: BTreeMap::from([(address1, 1), (address2, 1)]),
            },
            fees: BlockTxnFeeStates {
                txn_fees: BTreeMap::from([
                    (
                        address1,
                        TxnFee {
                            max_cost: U256::MAX - U256::from(1),
                            carriage_cost: U256::MAX - U256::from(1),
                        },
                    ),
                    (
                        address2,
                        TxnFee {
                            max_cost: Balance::MAX,
                            carriage_cost: Balance::MAX,
                        },
                    ),
                ]),
            },
        };

        let block2 = CommittedBlock {
            block_id: BlockId(Hash(Default::default())),
            round: Round(0),
            nonces: BlockAccountNonce {
                nonces: BTreeMap::from([(address1, 2), (address3, 1)]),
            },
            fees: BlockTxnFeeStates {
                txn_fees: BTreeMap::from([
                    (
                        address1,
                        TxnFee {
                            max_cost: Balance::from(1),
                            carriage_cost: Balance::from(1),
                        },
                    ),
                    (
                        address3,
                        TxnFee {
                            max_cost: U256::MAX.div_ceil(U256::from(2)),
                            carriage_cost: U256::MAX.div_ceil(U256::from(2)),
                        },
                    ),
                ]),
            },
        };

        let block3 = CommittedBlock {
            block_id: BlockId(Hash(Default::default())),
            round: Round(0),
            nonces: BlockAccountNonce {
                nonces: BTreeMap::from([(address2, 2), (address3, 2)]),
            },
            fees: BlockTxnFeeStates {
                txn_fees: BTreeMap::from([
                    (
                        address2,
                        TxnFee {
                            max_cost: Balance::MAX,
                            carriage_cost: Balance::MAX,
                        },
                    ),
                    (
                        address3,
                        TxnFee {
                            max_cost: U256::MAX.div_ceil(U256::from(2)) + U256::from(1),
                            carriage_cost: U256::MAX.div_ceil(U256::from(2)) + U256::from(1),
                        },
                    ),
                ]),
            },
        };

        buffer.blocks.insert(SeqNum(1), block1);
        buffer.blocks.insert(SeqNum(2), block2);
        buffer.blocks.insert(SeqNum(3), block3);

        // test compute_txn_fee for different addresses and base sequence numbers
        assert_eq!(
            buffer
                .compute_txn_fee(SeqNum(0), &address1, SeqNum(EXEC_DELAY))
                .carriage_cost,
            U256::MAX
        );
        assert_eq!(
            buffer
                .compute_txn_fee(SeqNum(1), &address1, SeqNum(EXEC_DELAY))
                .carriage_cost,
            U256::from(1)
        );
        assert_eq!(
            buffer
                .compute_txn_fee(SeqNum(2), &address1, SeqNum(EXEC_DELAY))
                .carriage_cost,
            U256::from(0)
        );

        assert_eq!(
            buffer
                .compute_txn_fee(SeqNum(0), &address2, SeqNum(EXEC_DELAY))
                .carriage_cost,
            U256::MAX
        );
        assert_eq!(
            buffer
                .compute_txn_fee(SeqNum(0), &address3, SeqNum(EXEC_DELAY))
                .carriage_cost,
            U256::MAX
        );
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

    // TODO: check accounts for previous transactions in the block
}
