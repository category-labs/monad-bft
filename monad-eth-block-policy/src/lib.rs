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
use monad_eth_types::{Balance, EthAccount, EthExecutionProtocol, Nonce};
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

pub fn checked_sum(arg1: U256, arg2: U256) -> U256 {
    let max_u128 = U256::from(u128::MAX);
    match arg1.checked_add(arg2) {
        Some(val) => {
            if val > max_u128 {
                max_u128
            } else {
                val
            }
        }
        None => max_u128,
    }
}

fn checked_from(arg: U256) -> u128 {
    match u128::try_from(arg) {
        Ok(val) => val,
        Err(_err) => u128::MAX,
    }
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

#[allow(clippy::unnecessary_fallible_conversions)]
pub fn compute_txn_max_value(txn: &TxEnvelope) -> U256 {
    let txn_value = U256::try_from(txn.value()).unwrap();
    let gas_cost = U256::from(txn.gas_limit() as u128 * txn.max_fee_per_gas());

    checked_sum(txn_value, gas_cost)
}

pub fn compute_txn_max_value_to_u128(txn: &TxEnvelope) -> u128 {
    let txn_value = compute_txn_max_value(txn);
    trace!(
        "compute_txn_max_value gas_cost + txn_value: {:?} ",
        txn_value
    );
    checked_from(txn_value)
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

    if tx.is_eip4844() || tx.is_eip7702() {
        return Err(TransactionError::UnsupportedTransactionType);
    }

    Ok(())
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
    pub validated_txns: Vec<Recovered<TxEnvelope>>,
    pub nonces: BTreeMap<Address, Nonce>,
    pub txn_fees: BTreeMap<Address, U256>,
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
    txn_fees: BTreeMap<Address, U256>,
}

impl BlockTxnFees {
    fn get(&self, eth_address: &Address) -> Option<U256> {
        self.txn_fees.get(eth_address).cloned()
    }
}

#[derive(Debug)]
struct CommittedBlock {
    block_id: BlockId,
    round: Round,

    nonces: BlockAccountNonce,
    fees: BlockTxnFees,
}

#[derive(Debug)]
struct CommittedBlkBuffer<ST, SCT> {
    blocks: SortedVectorMap<SeqNum, CommittedBlock>,
    size: usize, // should be execution delay

    _phantom: PhantomData<(ST, SCT)>,
}

struct CommittedTxnFeeResult {
    txn_fee: U256,
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
    ) -> CommittedTxnFeeResult {
        let mut txn_fee: U256 = U256::ZERO;
        let mut next_validate = base_seq_num + SeqNum(1);

        // start iteration from base_seq_num (non inclusive)
        for (&cache_seq_num, block) in self.blocks.range(next_validate..) {
            assert_eq!(next_validate, cache_seq_num);
            if let Some(account_txn_fee) = block.fees.get(eth_address) {
                txn_fee = checked_sum(txn_fee, account_txn_fee);
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
                    fees: BlockTxnFees {
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

    fn get_account_statuses<'a>(
        &self,
        state_backend: &impl StateBackend,
        extending_blocks: &Option<&Vec<&EthValidatedBlock<ST, SCT>>>,
        addresses: impl Iterator<Item = &'a Address>,
        base_seq_num: &SeqNum,
    ) -> Result<Vec<Option<EthAccount>>, StateBackendError> {
        let (base_block_id, base_block_round, base_block_is_finalized) = if base_seq_num
            <= &self.last_commit
        {
            debug!(
                ?base_seq_num,
                last_commit = self.last_commit.0,
                "base seq num committed"
            );
            if base_seq_num == &GENESIS_SEQ_NUM {
                (GENESIS_BLOCK_ID, GENESIS_ROUND, true)
            } else {
                let committed_block = &self
                    .committed_cache
                    .blocks
                    .get(base_seq_num)
                    .unwrap_or_else(|| panic!("queried recently committed block that doesn't exist, base_seq_num={:?}, last_commit={:?}", base_seq_num, self.last_commit));
                (committed_block.block_id, committed_block.round, true)
            }
        } else if let Some(extending_blocks) = extending_blocks {
            debug!(?base_seq_num, "base seq num proposed");
            let proposed_block = extending_blocks
                .iter()
                .find(|block| &block.get_seq_num() == base_seq_num)
                .expect("extending block doesn't exist");
            (proposed_block.get_id(), proposed_block.get_round(), false)
        } else {
            return Err(StateBackendError::NotAvailableYet);
        };
        state_backend.get_account_statuses(
            &base_block_id,
            base_seq_num,
            &base_block_round,
            base_block_is_finalized,
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
            .map(|maybe_status| maybe_status.map_or(0, |status| status.balance))
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

                let txn_fee_committed_u128 = checked_from(txn_fee_committed);

                if account_balance < txn_fee_committed_u128 {
                    panic!(
                        "Committed block with incoherent transaction fee 
                            Not sufficient balance: {:?} \
                            Transaction Fee Committed: {:?} \
                            consensus block:seq num {:?} \
                            for address: {:?}",
                        account_balance, txn_fee_committed_u128, consensus_block_seq_num, address
                    );
                } else {
                    account_balance -= txn_fee_committed_u128;
                    trace!(
                        "AccountBalance compute 4: \
                            updated balance to: {:?} \
                            Transaction Fee Committed: {:?} \
                            consensus block:seq num {:?} \
                            for address: {:?}",
                        account_balance,
                        txn_fee_committed_u128,
                        consensus_block_seq_num,
                        address
                    );
                }

                // Apply Txn Fees for txns in extending blocks
                let mut txn_fee_pending: U256 = U256::ZERO;
                if let Some(blocks) = extending_blocks {
                    // handle the case where base_seq_num is a pending block
                    let next_blocks = blocks
                        .iter()
                        .skip_while(move |block| block.get_seq_num() < next_validate);
                    for extending_block in next_blocks {
                        assert_eq!(next_validate, extending_block.get_seq_num());
                        if let Some(txn_fee) = extending_block.txn_fees.get(address) {
                            txn_fee_pending = checked_sum(txn_fee_pending, *txn_fee);
                        }
                        next_validate += SeqNum(1);
                    }
                }

                let txn_fee_pending_u128 = checked_from(txn_fee_pending);

                if account_balance < txn_fee_pending_u128 {
                    panic!(
                        "Majority extended a block with an incoherent balance \
                            Not sufficient balance: {:?} \
                            Txn Fees Pending: {:?} \
                            consensus block:seq num {:?} \
                            for address: {:?}",
                        account_balance, txn_fee_pending_u128, consensus_block_seq_num, address
                    );
                } else {
                    account_balance -= txn_fee_pending_u128;
                    trace!(
                        "AccountBalance compute 6: \
                            updated balance to: {:?} \
                            Txn Fees Pending: {:?} \
                            consensus block:seq num {:?} \
                            for address: {:?}",
                        account_balance,
                        txn_fee_pending_u128,
                        consensus_block_seq_num,
                        address
                    );
                }

                (address, account_balance)
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

        // TODO fix this unnecessary copy into a new vec to generate an owned Address
        let tx_signers = block
            .validated_txns
            .iter()
            .map(|txn| txn.signer())
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

        for txn in block.validated_txns.iter() {
            let eth_address = txn.signer();
            let txn_nonce = txn.nonce();

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

            let txn_fee = compute_txn_max_value_to_u128(txn);

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
                    round =? block.header().round,
                    "block not coherent, invalid balance"
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
    use alloy_consensus::{SignableTransaction, TxEip1559, TxLegacy};
    use alloy_primitives::{Address, FixedBytes, PrimitiveSignature, TxKind, B256};
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
            fees: BlockTxnFees {
                txn_fees: BTreeMap::from([
                    (address1, U256::from(100)),
                    (address2, U256::from(200)),
                ]),
            },
        };

        let block2 = CommittedBlock {
            block_id: BlockId(Hash(Default::default())),
            round: Round(0),
            nonces: BlockAccountNonce {
                nonces: BTreeMap::from([(address1, 2), (address3, 1)]),
            },
            fees: BlockTxnFees {
                txn_fees: BTreeMap::from([
                    (address1, U256::from(150)),
                    (address3, U256::from(300)),
                ]),
            },
        };

        let block3 = CommittedBlock {
            block_id: BlockId(Hash(Default::default())),
            round: Round(0),
            nonces: BlockAccountNonce {
                nonces: BTreeMap::from([(address2, 2), (address3, 2)]),
            },
            fees: BlockTxnFees {
                txn_fees: BTreeMap::from([
                    (address2, U256::from(250)),
                    (address3, U256::from(350)),
                ]),
            },
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
            nonces: BlockAccountNonce {
                nonces: BTreeMap::from([(address1, 1), (address2, 1)]),
            },
            fees: BlockTxnFees {
                txn_fees: BTreeMap::from([
                    (address1, U256::from(u128::MAX - 1)),
                    (address2, U256::from(u128::MAX)),
                ]),
            },
        };

        let block2 = CommittedBlock {
            block_id: BlockId(Hash(Default::default())),
            round: Round(0),
            nonces: BlockAccountNonce {
                nonces: BTreeMap::from([(address1, 2), (address3, 1)]),
            },
            fees: BlockTxnFees {
                txn_fees: BTreeMap::from([
                    (address1, U256::from(1)),
                    (address3, U256::from(u128::MAX / 2)),
                ]),
            },
        };

        let block3 = CommittedBlock {
            block_id: BlockId(Hash(Default::default())),
            round: Round(0),
            nonces: BlockAccountNonce {
                nonces: BTreeMap::from([(address2, 2), (address3, 2)]),
            },
            fees: BlockTxnFees {
                txn_fees: BTreeMap::from([
                    (address2, U256::from(u128::MAX)),
                    (address3, U256::from(u128::MAX / 2 + 1)),
                ]),
            },
        };

        buffer.blocks.insert(SeqNum(1), block1);
        buffer.blocks.insert(SeqNum(2), block2);
        buffer.blocks.insert(SeqNum(3), block3);

        // test compute_txn_fee for different addresses and base sequence numbers
        assert_eq!(
            buffer.compute_txn_fee(SeqNum(0), &address1).txn_fee,
            U256::from(u128::MAX)
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
            U256::from(u128::MAX)
        );
        assert_eq!(
            buffer.compute_txn_fee(SeqNum(0), &address3).txn_fee,
            U256::from(u128::MAX)
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

    // TODO: check accounts for previous transactions in the block
}
