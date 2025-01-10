use std::collections::BTreeMap;

use alloy_consensus::transaction::Transaction;
use alloy_primitives::U256;
use itertools::Itertools;
use monad_consensus_types::{
    block::{Block, BlockPolicy, BlockPolicyError, BlockType, FullBlock},
    payload::{Payload, PayloadId, PROPOSAL_GAS_LIMIT},
    quorum_certificate::QuorumCertificate,
    signature_collection::SignatureCollection,
    state_root_hash::StateRootHash,
};
use monad_crypto::hasher::{Hashable, Hasher};
use monad_eth_tx::{EthSignedTransaction, EthTransaction, EthTxHash};
use monad_eth_types::{Balance, EthAddress, Nonce};
use monad_state_backend::{StateBackend, StateBackendError};
use monad_types::{BlockId, Epoch, NodeId, Round, SeqNum, GENESIS_SEQ_NUM};
use sorted_vector_map::SortedVectorMap;
use tracing::trace;

/// Retriever trait for account nonces from block(s)
pub trait AccountNonceRetrievable {
    fn get_account_nonces(&self) -> BTreeMap<EthAddress, Nonce>;
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

fn compute_intrinsic_gas(tx: &EthSignedTransaction) -> u64 {
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
pub fn compute_txn_max_value(txn: &EthSignedTransaction) -> U256 {
    let txn_value = U256::try_from(txn.value()).unwrap();
    let gas_cost = U256::from(txn.gas_limit() as u128 * txn.max_fee_per_gas());

    checked_sum(txn_value, gas_cost)
}

pub fn compute_txn_max_value_to_u128(txn: &EthSignedTransaction) -> u128 {
    let txn_value = compute_txn_max_value(txn);
    trace!(
        "compute_txn_max_value gas_cost + txn_value: {:?} ",
        txn_value
    );
    checked_from(txn_value)
}

// allow for more fine grain debugging if needed
#[derive(Debug)]
pub enum TransactionError {
    InvalidChainId,
    MaxPriorityFeeTooHigh,
    InitCodeLimitExceeded,
    GasLimitTooLow,
    GasLimitTooHigh,
}

/// Stateless helper function to check validity of an Ethereum transaction
pub fn static_validate_transaction(
    tx: &EthSignedTransaction,
    chain_id: u64,
) -> Result<(), TransactionError> {
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
    const DATA_SIZE_LIMIT: usize = 2 * 0x6000;
    if tx.kind().is_create() && tx.input().len() > DATA_SIZE_LIMIT {
        return Err(TransactionError::InitCodeLimitExceeded);
    }

    // YP eq. 62 - intrinsic gas validation
    let intrinsic_gas = compute_intrinsic_gas(tx);
    if tx.gas_limit() < intrinsic_gas {
        return Err(TransactionError::GasLimitTooLow);
    }

    if tx.gas_limit() > PROPOSAL_GAS_LIMIT {
        return Err(TransactionError::GasLimitTooHigh);
    }

    Ok(())
}

/// A consensus block that has gone through the EthereumValidator and makes the decoded and
/// verified transactions available to access
#[derive(Debug, Clone)]
pub struct EthValidatedBlock<SCT: SignatureCollection> {
    pub block: Block<SCT>,
    pub orig_payload: Payload,
    pub validated_txns: Vec<EthTransaction>,
    pub nonces: BTreeMap<EthAddress, Nonce>,
    pub txn_fees: BTreeMap<EthAddress, U256>,
}

impl<SCT: SignatureCollection> EthValidatedBlock<SCT> {
    pub fn get_validated_txn_hashes(&self) -> Vec<EthTxHash> {
        self.validated_txns.iter().map(|t| *t.tx_hash()).collect()
    }

    /// Returns the highest tx nonce per account in the block
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

    fn get_payload(&self) -> Payload {
        self.orig_payload.clone()
    }

    fn get_payload_id(&self) -> PayloadId {
        self.block.payload_id
    }

    fn get_parent_id(&self) -> BlockId {
        self.block.qc.get_block_id()
    }

    fn get_parent_round(&self) -> Round {
        self.block.qc.get_round()
    }

    fn get_seq_num(&self) -> SeqNum {
        self.block.execution.seq_num
    }

    fn get_state_root(&self) -> StateRootHash {
        self.block.execution.state_root
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

    fn is_empty_block(&self) -> bool {
        self.block.is_empty_block()
    }

    fn get_full_block(self) -> FullBlock<SCT> {
        FullBlock {
            block: self.block,
            payload: self.orig_payload,
        }
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
struct BlockTxnFees {
    txn_fees: BTreeMap<EthAddress, U256>,
}

impl BlockTxnFees {
    fn get(&self, eth_address: &EthAddress) -> Option<U256> {
        self.txn_fees.get(eth_address).cloned()
    }
}

#[derive(Debug)]
struct CommittedBlkBuffer {
    blocks: SortedVectorMap<SeqNum, (BlockAccountNonce, BlockTxnFees)>,
    size: usize, // should be execution delay
}

struct CommittedTxnFeeResult {
    txn_fee: U256,
    next_validate: SeqNum, // next block number to validate; included for assertions only
}

impl CommittedBlkBuffer {
    fn new(size: usize) -> Self {
        Self {
            blocks: Default::default(),
            size,
        }
    }

    fn get_nonce(&self, eth_address: &EthAddress) -> Option<Nonce> {
        let mut maybe_account_nonce = None;

        for (_, (account_nonces, _)) in self.blocks.iter() {
            if let Some(nonce) = account_nonces.get(eth_address) {
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
        eth_address: &EthAddress,
    ) -> CommittedTxnFeeResult {
        let mut txn_fee: U256 = U256::ZERO;
        let mut next_validate = base_seq_num + SeqNum(1);

        // start iteration from base_seq_num (non inclusive)
        for (&cache_seq_num, (_nonce, block_txn_fees)) in self.blocks.range(next_validate..) {
            assert_eq!(next_validate, cache_seq_num);
            if let Some(account_txn_fee) = block_txn_fees.get(eth_address) {
                txn_fee = checked_sum(txn_fee, account_txn_fee);
            }
            next_validate += SeqNum(1);
        }

        CommittedTxnFeeResult {
            txn_fee,
            next_validate,
        }
    }

    fn update_committed_block<SCT: SignatureCollection>(&mut self, block: &EthValidatedBlock<SCT>) {
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
                (
                    BlockAccountNonce {
                        nonces: block.get_account_nonces(),
                    },
                    BlockTxnFees {
                        txn_fees: block.txn_fees.clone()
                    }
                ),
            )
            .is_none());
    }
}

/// A block policy for ethereum payloads
pub struct EthBlockPolicy {
    /// SeqNum of last committed block
    last_commit: SeqNum,

    // last execution-delay committed transactions
    committed_cache: CommittedBlkBuffer,

    /// Cost for including transaction in the consensus
    execution_delay: SeqNum,

    /// Chain ID
    chain_id: u64,
}

impl EthBlockPolicy {
    pub fn new(last_commit: SeqNum, execution_delay: u64, chain_id: u64) -> Self {
        Self {
            committed_cache: CommittedBlkBuffer::new(execution_delay as usize),
            last_commit,
            execution_delay: SeqNum(execution_delay),
            chain_id,
        }
    }

    /// returns account nonces at the start of the provided consensus block
    pub fn get_account_base_nonces<'a, SCT>(
        &self,
        consensus_block_seq_num: SeqNum,
        state_backend: &impl StateBackend,
        extending_blocks: &Vec<&EthValidatedBlock<SCT>>,
        addresses: impl Iterator<Item = &'a EthAddress>,
    ) -> Result<BTreeMap<&'a EthAddress, Nonce>, StateBackendError>
    where
        SCT: SignatureCollection,
    {
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
        let cache_miss_statuses =
            state_backend.get_account_statuses(base_seq_num, cache_misses.iter().copied())?;
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

    // Computes account balance available for the account
    pub fn compute_account_base_balances<'a, SCT>(
        &self,
        consensus_block_seq_num: SeqNum,
        state_backend: &impl StateBackend,
        extending_blocks: Option<&Vec<&EthValidatedBlock<SCT>>>,
        addresses: impl Iterator<Item = &'a EthAddress>,
    ) -> Result<BTreeMap<&'a EthAddress, Balance>, StateBackendError>
    where
        SCT: SignatureCollection,
    {
        // TODO this is error-prone, easy to forget
        // TODO write tests that fail if this doesn't exist
        let extending_blocks = extending_blocks.map(|extending_blocks| {
            extending_blocks
                .iter()
                .filter(|block| !block.is_empty_block())
                .collect_vec()
        });

        trace!(block = consensus_block_seq_num.0, "compute_base_balance");

        // calculation correct only if GENESIS_SEQ_NUM == 0
        assert_eq!(GENESIS_SEQ_NUM, SeqNum(0));
        let base_seq_num = consensus_block_seq_num.max(self.execution_delay) - self.execution_delay;

        let addresses = addresses.unique().collect_vec();
        let account_balances = state_backend
            .get_account_statuses(base_seq_num, addresses.iter().copied())?
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
                    next_validate,
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
                if let Some(blocks) = &extending_blocks {
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
                        if let Some(txn_fee) = extending_block.txn_fees.get(address) {
                            txn_fee_pending = checked_sum(txn_fee_pending, *txn_fee);
                        }
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

impl<SCT, SBT> BlockPolicy<SCT, SBT> for EthBlockPolicy
where
    SCT: SignatureCollection,
    SBT: StateBackend,
{
    type ValidatedBlock = EthValidatedBlock<SCT>;

    fn check_coherency(
        &self,
        block: &Self::ValidatedBlock,
        extending_blocks: Vec<&Self::ValidatedBlock>,
        state_backend: &SBT,
    ) -> Result<(), BlockPolicyError> {
        trace!(?block, "check_coherency");
        // TODO: short circuit check_coherency for null blocks
        let first_block = extending_blocks
            .iter()
            .chain(std::iter::once(&block))
            .next()
            .unwrap();
        if first_block.is_empty_block() {
            assert_eq!(first_block.get_seq_num(), self.last_commit);
        } else {
            assert_eq!(first_block.get_seq_num(), self.last_commit + SeqNum(1));
        }

        // TODO fix this unnecessary copy into a new vec to generate an owned EthAddress
        let tx_signers = block
            .validated_txns
            .iter()
            .map(|txn| EthAddress(txn.signer()))
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
            let eth_address = EthAddress(txn.signer());
            let txn_nonce = txn.nonce();

            let expected_nonce = account_nonces
                .get_mut(&eth_address)
                .expect("account_nonces should have been populated");

            if &txn_nonce != expected_nonce {
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
                return Err(BlockPolicyError::BlockNotCoherent);
            }
        }
        Ok(())
    }

    fn update_committed_block(&mut self, block: &Self::ValidatedBlock) {
        if block.is_empty_block() {
            // TODO this is error-prone, easy to forget
            // TODO write tests that fail if this doesn't exist
            return;
        }
        assert_eq!(block.get_seq_num(), self.last_commit + SeqNum(1));
        self.last_commit = block.get_seq_num();
        self.committed_cache.update_committed_block(block);
    }

    fn reset(&mut self, last_delay_non_null_committed_blocks: Vec<&Self::ValidatedBlock>) {
        self.committed_cache = CommittedBlkBuffer::new(self.committed_cache.size);
        // TODO this is error-prone, easy to forget
        // TODO write tests that fail if this doesn't exist
        let blocks = last_delay_non_null_committed_blocks
            .into_iter()
            .filter(|block| !block.is_empty_block());
        for block in blocks {
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
    use monad_eth_types::EthAddress;
    use monad_types::SeqNum;

    use super::*;

    fn sign_tx(signature_hash: &FixedBytes<32>) -> PrimitiveSignature {
        let secret_key = B256::repeat_byte(0xAu8).to_string();
        let signer = &secret_key.parse::<PrivateKeySigner>().unwrap();
        signer.sign_hash_sync(signature_hash).unwrap()
    }

    #[test]
    fn test_compute_txn_fee() {
        // setup test addresses
        let address1 = EthAddress(Address(FixedBytes([0x11; 20])));
        let address2 = EthAddress(Address(FixedBytes([0x22; 20])));
        let address3 = EthAddress(Address(FixedBytes([0x33; 20])));
        let address4 = EthAddress(Address(FixedBytes([0x44; 20])));

        // add committed blocks to buffer
        let mut buffer = CommittedBlkBuffer::new(3);
        let block1 = (
            BlockAccountNonce {
                nonces: BTreeMap::from([(address1, 1), (address2, 1)]),
            },
            BlockTxnFees {
                txn_fees: BTreeMap::from([
                    (address1, U256::from(100)),
                    (address2, U256::from(200)),
                ]),
            },
        );

        let block2 = (
            BlockAccountNonce {
                nonces: BTreeMap::from([(address1, 2), (address3, 1)]),
            },
            BlockTxnFees {
                txn_fees: BTreeMap::from([
                    (address1, U256::from(150)),
                    (address3, U256::from(300)),
                ]),
            },
        );

        let block3 = (
            BlockAccountNonce {
                nonces: BTreeMap::from([(address2, 2), (address3, 2)]),
            },
            BlockTxnFees {
                txn_fees: BTreeMap::from([
                    (address2, U256::from(250)),
                    (address3, U256::from(350)),
                ]),
            },
        );

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
        let address1 = EthAddress(Address(FixedBytes([0x11; 20])));
        let address2 = EthAddress(Address(FixedBytes([0x22; 20])));
        let address3 = EthAddress(Address(FixedBytes([0x33; 20])));

        // add committed blocks to buffer
        let mut buffer = CommittedBlkBuffer::new(3);
        let block1 = (
            BlockAccountNonce {
                nonces: BTreeMap::from([(address1, 1), (address2, 1)]),
            },
            BlockTxnFees {
                txn_fees: BTreeMap::from([
                    (address1, U256::from(u128::MAX - 1)),
                    (address2, U256::from(u128::MAX)),
                ]),
            },
        );

        let block2 = (
            BlockAccountNonce {
                nonces: BTreeMap::from([(address1, 2), (address3, 1)]),
            },
            BlockTxnFees {
                txn_fees: BTreeMap::from([
                    (address1, U256::from(1)),
                    (address3, U256::from(u128::MAX / 2)),
                ]),
            },
        );

        let block3 = (
            BlockAccountNonce {
                nonces: BTreeMap::from([(address2, 2), (address3, 2)]),
            },
            BlockTxnFees {
                txn_fees: BTreeMap::from([
                    (address2, U256::from(u128::MAX)),
                    (address3, U256::from(u128::MAX / 2 + 1)),
                ]),
            },
        );

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

        let result = static_validate_transaction(&txn.into(), CHAIN_ID);
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

        let result = static_validate_transaction(&txn.into(), CHAIN_ID);
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

        let result = static_validate_transaction(&txn.into(), CHAIN_ID);
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

        let result = static_validate_transaction(&txn.into(), CHAIN_ID);
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

        let result = static_validate_transaction(&txn.into(), CHAIN_ID);
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

        let result = static_validate_transaction(&txn.into(), CHAIN_ID);
        assert!(matches!(result, Err(TransactionError::GasLimitTooHigh)));
    }

    // TODO: check accounts for previous transactions in the block
}
