use std::{
    cmp::min, cmp::Ordering, collections::{BTreeMap, BinaryHeap}, default, path::{Path, PathBuf}
};

use monad_types::{ SeqNum };
use alloy_rlp::Decodable;
use bytes::Bytes;
use monad_consensus_types::{
    payload::FullTransactionList, signature_collection::SignatureCollection, txpool::TxPool,
};
use monad_eth_block_policy::{EthBlockPolicy, EthValidatedBlock};
use monad_eth_tx::{EthFullTransactionList, EthTransaction, EthTxHash};
use monad_eth_types::{EthAddress, Nonce};
use reth_primitives::{Transaction, TxEip1559, TxEip2930, TxEip4844, TxLegacy};
use sorted_vector_map::SortedVectorMap;

use monad_triedb::Handle as TriedbHandle;
use tracing::{info, debug};

type VirtualTimestamp = u64;

const CARRIAGE_COST: u128 = 35;
const MAX_RESERVE_BALANCE: u128 = 100;

/// Needed to have control over Ord implementation
#[derive(Debug, Eq, PartialEq)]
struct WrappedTransaction<'a> {
    inner: &'a EthTransaction,
    sender: EthAddress,
    insertion_time: VirtualTimestamp,
}

impl<'a> WrappedTransaction<'a> {
    pub fn effective_tip_per_gas(&self) -> u128 {
        match self.inner.transaction {
            Transaction::Legacy(TxLegacy { gas_price, .. })
            | Transaction::Eip2930(TxEip2930 { gas_price, .. }) => gas_price,
            Transaction::Eip1559(TxEip1559 {
                max_priority_fee_per_gas,
                ..
            })
            | Transaction::Eip4844(TxEip4844 {
                max_priority_fee_per_gas,
                ..
            }) => max_priority_fee_per_gas,
        }
    }

    pub fn hash(&self) -> EthTxHash {
        self.inner.hash()
    }

    pub fn gas_limit(&self) -> u64 {
        self.inner.gas_limit()
    }

    pub fn inner(&self) -> &EthTransaction {
        self.inner
    }
}

impl<'a> Ord for WrappedTransaction<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        match self
            .effective_tip_per_gas()
            .cmp(&other.effective_tip_per_gas())
        {
            order @ Ordering::Less | order @ Ordering::Greater => order,
            // Use the VirtualTimestamp as a tie-breaker
            Ordering::Equal => self.insertion_time.cmp(&other.insertion_time),
        }
    }
}

impl<'a> PartialOrd for WrappedTransaction<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug, Default)]
struct TransactionGroup {
    transactions: SortedVectorMap<Nonce, EthTransaction>,
}

impl TransactionGroup {
    fn add(&mut self, transaction: EthTransaction) {
        self.transactions.insert(transaction.nonce(), transaction);
    }
}

#[derive(Clone, Debug, Default)]
struct ReserveBalanceCache {
    cache: BTreeMap<EthAddress, u128>,
    // None - the cache is purged.
    last_block_id: Option<SeqNum>
}


// exists for the current block and address - return
// does not existst for the current block and address - need to compute
// the block is not current - purge the cache

enum ReserveBalanceCacheResult {
    Val(u128),
    None, // No entry in the cache
    Stale, // The committed block for which the cache computed is stale
    Purged,
    Mismatch // Should not happen, error
}

enum ComputeReserveBalanceResult {
    Val(u128),
    TrieDBNone, // No entrhy in TrieDB
    Spent // Spent by the last K blocks carriage 
}

impl ReserveBalanceCache {
    fn get_reserve_balance(&self, block_id: SeqNum, address: &EthAddress) -> ReserveBalanceCacheResult {
        if let Some(last_block_id) = self.last_block_id {
            if block_id < last_block_id {
                return ReserveBalanceCacheResult::Mismatch;
            }
            else if block_id > last_block_id {
                return ReserveBalanceCacheResult::Stale;
            }
            else {
                if let Some(cached_balance) = self.cache.get(address) {
                    return ReserveBalanceCacheResult::Val(*cached_balance);
                }
                else {
                    return ReserveBalanceCacheResult::None;
                }
            }
        }
        else {
            return ReserveBalanceCacheResult::Purged
        }
    }

    fn purge(&mut self) {
        self.cache.clear();
        self.last_block_id = None;
    }

    fn update_reserve_balance_cache(&mut self, block_id: &SeqNum, address: &EthAddress, balance: u128) {
        if self.last_block_id.is_none() {
            self.last_block_id = Some(*block_id);
        }

        self.cache.insert(*address, balance);
    }

}

fn compute_reserve_balance_for_propose<SCT: SignatureCollection>(
    block_policy: &EthBlockPolicy,
    extending_blocks: &Vec<&EthValidatedBlock<SCT>>,
    eth_address: &EthAddress,
    tdb_path: &Path) -> ComputeReserveBalanceResult {

    let handle = TriedbHandle::try_new(tdb_path).unwrap();
    let triedb_block_id = handle.latest_block();

    let mut reserve_balance: u128 = 0;
    // Get the balance from the latest triedb block
    if let Some(acc_balance) = handle.get_account_balance(triedb_block_id, eth_address) {
        reserve_balance = min(acc_balance, MAX_RESERVE_BALANCE);
        debug!("ReserveBalance propose 1: \
                Account balance from TDB: {:?} \
                for address: {:?} \
                computed reserve balance: {:?}",
                acc_balance,
                eth_address,
                reserve_balance);
    }
    else {
        debug!("ReserveBalance propose 2: \
                 Cant get account balance from TDB for address: {:?}", eth_address);
        return ComputeReserveBalanceResult::TrieDBNone;
    }
    // Apply Carriage Cost for the txns from committed blocks
    let txn_cnt: u128 = block_policy.count_txns_for_address(eth_address);

    if reserve_balance < txn_cnt * CARRIAGE_COST {
        debug!("ReserveBalance propose 3: \
                Not sufficient balance: {:?} \
                committed txn cnt: {:?} \
                for address: {:?}",
                reserve_balance,
                txn_cnt,
                eth_address);
        return ComputeReserveBalanceResult::Spent;
    }
    else {
        reserve_balance = reserve_balance - txn_cnt * CARRIAGE_COST;
        debug!("ReserveBalance propose 4: \
                updated balance to: {:?} \
                committed txn cnt: {:?} \
                for address: {:?}",
                reserve_balance,
                txn_cnt,
                eth_address);
    }

    // count txns in extending blocks
    let mut pending_txn_cnt:u128 = 0;
    for extending_block in extending_blocks {
        for txn in &extending_block.validated_txns {
            if EthAddress(txn.recover_signer().unwrap()) == *eth_address {
                pending_txn_cnt = pending_txn_cnt + 1;
            }
        }
    }

    if reserve_balance < pending_txn_cnt * CARRIAGE_COST {
        debug!("ReserveBalance propose 5: \
                Not sufficient balance: {:?} \
                pending txn cnt: {:?} \
                for address: {:?}",
                reserve_balance,
                pending_txn_cnt,
                eth_address);
        return ComputeReserveBalanceResult::Spent;
    }
    else {
        reserve_balance = reserve_balance - pending_txn_cnt * CARRIAGE_COST;
        debug!("ReserveBalance propose 6: \
                updated balance to: {:?} \
                pending txn cnt: {:?} \
                for address: {:?}",
                reserve_balance,
                pending_txn_cnt,
                eth_address);
    }

    ComputeReserveBalanceResult::Val(reserve_balance)
}

type Pool = SortedVectorMap<EthAddress, TransactionGroup>;

#[derive(Clone, Default, Debug)]
pub struct EthTxPool {
    pool: Pool,
    garbage: Vec<Pool>,
    path: PathBuf,
    reserve_balance_cache: ReserveBalanceCache,
}

impl EthTxPool {
    pub fn new (
        triedb_path: PathBuf,
    ) -> Self {
        Self { pool: Pool::default(),
               garbage: Vec::default(),
               path: triedb_path.clone(),
               reserve_balance_cache: ReserveBalanceCache::default(),
        }
    }

    fn remove_invalid_nonces<SCT: SignatureCollection>(
        &mut self,
        block_policy: &EthBlockPolicy,
        extending_blocks: Vec<&EthValidatedBlock<SCT>>,
        blocktree_nonce_deltas: BTreeMap<EthAddress, Nonce>,
    ) {
        let tdb_path =  self.path.as_path();
        self.pool
            .iter_mut()
            .for_each(|(eth_address, transaction_group)| {
                let mut high_nonce =
                    block_policy.get_latest_nonce(eth_address, &blocktree_nonce_deltas);
                let mut maybe_nonce_to_remove = None;

                // Remove transactions with nonces lower than the lowest valid nonce
                let lowest_valid_nonce = high_nonce.map(|nonce| nonce + 1).unwrap_or(0);
                transaction_group
                    .transactions
                    .retain(|&nonce, _| nonce >= lowest_valid_nonce);

                let mut reserve_balance:u128 = 0;
                match compute_reserve_balance_for_propose::<SCT>(block_policy, &extending_blocks, eth_address, tdb_path) {
                    ComputeReserveBalanceResult::Val(val) => { reserve_balance = val; }
                    ComputeReserveBalanceResult::TrieDBNone => { /* reserve balance is 0 */}
                    _ => ()
                }

                for (nonce, _) in &transaction_group.transactions {
                    if high_nonce.is_some() && *nonce != high_nonce.unwrap() + 1 {
                        maybe_nonce_to_remove = Some(*nonce);
                        break;
                    }
                    // Reserve balance validation
                    if reserve_balance < CARRIAGE_COST {
                        maybe_nonce_to_remove = Some(*nonce);
                        debug!("ReserveBalance propose 7: \
                                insufficient balance: {:?} \
                                at nonce: {:?} \
                                for address: {:?}",
                                reserve_balance,
                                nonce,
                                eth_address);
                        break;
                    }
                    else {
                        reserve_balance = reserve_balance - CARRIAGE_COST;
                        debug!("ReserveBalance propose 8: \
                                updated balance to: {:?} \
                                at nonce: {:?} \
                                for address: {:?}",
                                reserve_balance,
                                nonce,
                                eth_address);
                    }

                    high_nonce = Some(*nonce);
                }

                match maybe_nonce_to_remove {
                    None => {}
                    Some(nonce_to_remove) => {
                        let _ = transaction_group.transactions.split_off(&nonce_to_remove);
                    }
                }
            });
        self.pool
            .retain(|_, transaction_group| !transaction_group.transactions.is_empty())
    }
}


impl EthTxPool {
    fn compute_reserve_balance_for_insert(
        &mut self,
        block_policy: &EthBlockPolicy,
        eth_address: &EthAddress) -> (SeqNum, ComputeReserveBalanceResult) {
        let mut reserve_balance: u128 = 0;
        let block_id = &block_policy.get_committed_block_seq_num().unwrap();
        match self.reserve_balance_cache.get_reserve_balance(*block_id, eth_address) {
            ReserveBalanceCacheResult::Val(balance) => {
                debug!("ReserveBalance insert 1: \
                        balance from cache: {:?} \
                        latest block_id: {:?} \
                        for address: {:?}",
                        balance,
                        block_id,
                        eth_address);
                return (*block_id, ComputeReserveBalanceResult::Val(balance));
            },
            ReserveBalanceCacheResult::Stale => { 
                debug!("ReserveBalance insert 2: \
                        cache is stale. \
                        latest block_id: {:?} \
                        for address: {:?}",
                        block_id,
                        eth_address);
                self.reserve_balance_cache.purge(); 
            }
            _ => ()
        }

        let handle = TriedbHandle::try_new(self.path.as_path()).unwrap();
        let triedb_block_id = handle.latest_block();

        // Get the balance from the latest triedb block
        if let Some(acc_balance) = handle.get_account_balance(triedb_block_id, eth_address) {
            reserve_balance = min(acc_balance, MAX_RESERVE_BALANCE);
            debug!("ReserveBalance insert 3: \
                    Account balance from TDB: {:?} \
                    latest block_id: {:?} \
                    for address: {:?} \
                    computed reserve balance: {:?}",
                    acc_balance,
                    block_id,
                    eth_address,
                    reserve_balance);
        }
        else {
            debug!("ReserveBalance insert 4: \
                    Cant get account balance from TDB for address: {:?} latest block_id {:?}", eth_address, block_id);
            return (*block_id, ComputeReserveBalanceResult::TrieDBNone);
        }

        // Apply Carriage Cost for the txns from committed blocks
        let txn_cnt: u128 = block_policy.count_txns_for_address(eth_address);

        if reserve_balance < txn_cnt * CARRIAGE_COST {
            debug!("ReserveBalance insert 5: \
                    Not sufficient balance: {:?} \
                    mempool txn cnt: {:?} \
                    latest block_id: {:?} \
                    for address: {:?}",
                    reserve_balance,
                    txn_cnt,
                    block_id,
                    eth_address);
            return (*block_id, ComputeReserveBalanceResult::Spent);
        }
        else {
            reserve_balance = reserve_balance - txn_cnt * CARRIAGE_COST;
            debug!("ReserveBalance insert 6: \
                    updated balance to: {:?} \
                    mempool txn cnt: {:?} \
                    latest block_id: {:?} \
                    for address: {:?}",
                    reserve_balance,
                    txn_cnt,
                    block_id,
                    eth_address);
        }

        // Balalnce cache  keeps track of the latest _committed_ block
        self.reserve_balance_cache.update_reserve_balance_cache(&block_id, eth_address, reserve_balance);

        (*block_id, ComputeReserveBalanceResult::Val(reserve_balance))
    }
}

impl<SCT: SignatureCollection> TxPool<SCT, EthBlockPolicy> for EthTxPool {
    fn insert_tx(&mut self, tx: Bytes, block_policy: &EthBlockPolicy) {
        // TODO: unwrap can be removed when this is made generic over the actual
        // tx type rather than Bytes and decoding won't be necessary
        // TODO(rene): sender recovery is done inline here
        let eth_tx = EthTransaction::decode(&mut tx.as_ref()).unwrap();
        let sender = EthAddress(eth_tx.signer());

        let (block_id, res) = self.compute_reserve_balance_for_insert(block_policy, &sender);
        if let ComputeReserveBalanceResult::Val(reserve_balance) = res {
            if reserve_balance > CARRIAGE_COST {
                // TODO(rene): should any transaction validation occur here before inserting into mempool
                self.pool.entry(sender).or_default().add(eth_tx);
                self.reserve_balance_cache.update_reserve_balance_cache(&block_id, &sender, reserve_balance - CARRIAGE_COST);
                debug!("ReserveBalance insert 7: \
                        updated reserve balance to: {:?} \
                        block_id: {:?} \
                        for address: {:?}",
                        reserve_balance - CARRIAGE_COST,
                        block_id,
                        sender);
            }
            else {
                debug!("ReserveBalance insert 7: \
                        do not add txn to the pool. insufficient balance: {:?} \
                        block_id: {:?} \
                        for address: {:?}",
                        reserve_balance,
                        block_id,
                        sender);

            }
        }
    }

    fn create_proposal(
        &mut self,
        tx_limit: usize,
        proposal_gas_limit: u64,
        block_policy: &EthBlockPolicy,
        extending_blocks: Vec<&EthValidatedBlock<SCT>>,
    ) -> FullTransactionList {
        // Get the latest nonce deltas at the parent block (block to extend)
        let mut blocktree_nonce_deltas = BTreeMap::new();
        for extending_block in &extending_blocks {
            let block_nonces = extending_block.get_nonces();
            for (&address, &nonce) in block_nonces {
                blocktree_nonce_deltas
                    .entry(address)
                    .and_modify(|curr_nonce| *curr_nonce = nonce)
                    .or_insert(nonce);
            }
        }
        self.remove_invalid_nonces(block_policy, extending_blocks, blocktree_nonce_deltas);

        let mut txs = Vec::new();
        let mut total_gas = 0;

        let mut transaction_iters = self
            .pool
            .iter()
            .map(|(address, group)| (*address, group.transactions.iter()))
            .collect::<BTreeMap<_, _>>();

        let mut virtual_time: VirtualTimestamp = 0;

        let mut max_heap = BinaryHeap::<WrappedTransaction>::new();


        // queue one eligible transaction for each account (they will be the ones with the lowest nonce)
        for (account, transaction_iter) in transaction_iters.iter_mut() {
            match transaction_iter.next() {
                None => {
                    unreachable!()
                }
                Some((_, transaction)) => {
                    max_heap.push(WrappedTransaction {
                        inner: transaction,
                        sender: *account,
                        insertion_time: virtual_time,
                    });
                    virtual_time += 1;
                }
            }
        }

        // loop invariant: `max_heap` contains one transaction per account (lowest nonce).
        // the root of the heap will be the best priced eligible transaction
        while !max_heap.is_empty() {
            let WrappedTransaction {
                inner: best_tx,
                sender: address,
                ..
            } = max_heap.pop().unwrap();

            if txs.len() == tx_limit {
                break;
            }

            // If this transaction will take us out of the block limit, we cannot include it.
            // This will create a nonce gap, so we no longer want to consider this account, but
            // we want to continue considering other accounts.
            if (total_gas + best_tx.gas_limit()) > proposal_gas_limit {
                transaction_iters.remove(&address);
                continue;
            }

            // maintain the loop invariant because we just removed one element from the heap
            match transaction_iters.get_mut(&address).unwrap().next() {
                None => {}
                Some((_, transaction)) => {
                    max_heap.push(WrappedTransaction {
                        inner: transaction,
                        sender: address,
                        insertion_time: virtual_time,
                    });
                    virtual_time += 1;
                }
            }

            total_gas += best_tx.gas_limit();
            txs.push(best_tx.clone());
        }

        let proposal_num_tx = txs.len();
        let full_tx_list = EthFullTransactionList(txs).rlp_encode();

        tracing::info!(
            proposal_num_tx,
            proposal_total_gas = total_gas,
            proposal_tx_bytes = full_tx_list.len()
        );

        if !self.garbage.is_empty() {
            tracing::warn!(
                garbage_len = self.garbage.len(),
                "we have received consecutive proposals without a mempool clear event in between"
            );
        }

        let old_pool = std::mem::take(&mut self.pool);
        self.garbage.push(old_pool);

        FullTransactionList::new(full_tx_list)
    }

    fn clear(&mut self) {
        self.garbage.clear();
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use alloy_primitives::{hex, B256};
    use alloy_rlp::Decodable;
    use monad_consensus_types::txpool::TxPool;
    use monad_crypto::NopSignature;
    use monad_eth_block_policy::utils::generate_random_block_with_txns;
    use monad_eth_tx::{utils::make_tx, EthSignedTransaction};
    use monad_eth_types::EthAddress;
    use monad_multi_sig::MultiSig;
    use tracing_test::traced_test;

    use crate::{EthBlockPolicy, EthTxPool};

    type Pool = dyn TxPool<MultiSig<NopSignature>, EthBlockPolicy>;

    #[test]
    #[traced_test]
    fn test_create_proposal_with_insufficient_tx_limit() {
        let tx = make_tx(B256::repeat_byte(0xAu8), 1, 1, 0, 10);
        let mut pool = EthTxPool::default();
        let eth_block_policy = EthBlockPolicy {
            latest_nonces: BTreeMap::new(),
        };
        Pool::insert_tx(&mut pool, tx.envelope_encoded().into());
        assert_eq!(pool.pool.len(), 1);
        assert_eq!(pool.pool.first_key_value().unwrap().1.transactions.len(), 1);

        let encoded_txns =
            Pool::create_proposal(&mut pool, 0, 1_000_000, &eth_block_policy, Vec::new());

        let decoded_txns = Vec::<EthSignedTransaction>::decode(&mut encoded_txns.as_ref()).unwrap();
        assert_eq!(decoded_txns, vec![]);
        assert!(pool.pool.is_empty());
    }

    #[test]
    #[traced_test]
    fn test_create_proposal_with_insufficient_gas_limit() {
        let tx = make_tx(B256::repeat_byte(0xAu8), 1, 6400, 0, 10);
        let mut pool = EthTxPool::default();
        let eth_block_policy = EthBlockPolicy {
            latest_nonces: BTreeMap::new(),
        };
        Pool::insert_tx(&mut pool, tx.envelope_encoded().into());
        assert_eq!(pool.pool.len(), 1);
        assert_eq!(pool.pool.first_key_value().unwrap().1.transactions.len(), 1);

        let encoded_txns = Pool::create_proposal(&mut pool, 1, 6399, &eth_block_policy, Vec::new());

        let decoded_txns = Vec::<EthSignedTransaction>::decode(&mut encoded_txns.as_ref()).unwrap();
        assert_eq!(decoded_txns, vec![]);
        assert!(pool.pool.is_empty());
    }

    #[test]
    #[traced_test]
    fn test_create_partial_proposal_with_insufficient_gas_limit() {
        let t1 = make_tx(B256::repeat_byte(0xAu8), 1, 6400, 0, 10);
        let t2 = make_tx(B256::repeat_byte(0xAu8), 1, 6400, 1, 10);
        let t3 = make_tx(B256::repeat_byte(0xAu8), 1, 6400, 2, 10);
        let expected_txs = vec![
            make_tx(B256::repeat_byte(0xAu8), 1, 6400, 0, 10),
            make_tx(B256::repeat_byte(0xAu8), 1, 6400, 1, 10),
        ];
        let mut pool = EthTxPool::default();
        let eth_block_policy = EthBlockPolicy {
            latest_nonces: BTreeMap::new(),
        };
        Pool::insert_tx(&mut pool, t1.envelope_encoded().into());
        Pool::insert_tx(&mut pool, t2.envelope_encoded().into());
        Pool::insert_tx(&mut pool, t3.envelope_encoded().into());
        assert_eq!(pool.pool.len(), 1);
        assert_eq!(pool.pool.first_key_value().unwrap().1.transactions.len(), 3);

        let encoded_txns =
            Pool::create_proposal(&mut pool, 2, 6400 * 2, &eth_block_policy, Vec::new());
        let decoded_txns = Vec::<EthSignedTransaction>::decode(&mut encoded_txns.as_ref()).unwrap();
        assert_eq!(decoded_txns, expected_txs);
    }

    #[test]
    #[traced_test]
    fn test_basic_price_priority() {
        let s1 = B256::repeat_byte(0xAu8); // 0xC171033d5CBFf7175f29dfD3A63dDa3d6F8F385E
        let s2 = B256::repeat_byte(0xBu8); // 0xf288ECAF15790EfcAc528946963A6Db8c3f8211d
        let txs = vec![make_tx(s1, 1, 1, 0, 10), make_tx(s2, 2, 2, 3, 10)];
        let expected_txs = vec![make_tx(s2, 2, 2, 3, 10), make_tx(s1, 1, 1, 0, 10)];
        let mut pool = EthTxPool::default();
        let eth_block_policy = EthBlockPolicy {
            latest_nonces: BTreeMap::new(),
        };
        for tx in txs.iter() {
            Pool::insert_tx(&mut pool, tx.clone().envelope_encoded().into());
        }
        let encoded_txns = Pool::create_proposal(&mut pool, 2, 3, &eth_block_policy, Vec::new());
        let decoded_txns = Vec::<EthSignedTransaction>::decode(&mut encoded_txns.as_ref()).unwrap();
        assert_eq!(decoded_txns, expected_txs);
    }

    #[test]
    #[traced_test]
    fn test_resubmit_with_better_price() {
        let s1 = B256::repeat_byte(0xAu8); // 0xC171033d5CBFf7175f29dfD3A63dDa3d6F8F385E
        let txs = vec![make_tx(s1, 1, 1, 0, 10), make_tx(s1, 2, 2, 0, 10)];
        let expected_txs = vec![make_tx(s1, 2, 2, 0, 10)];
        let mut pool = EthTxPool::default();
        let eth_block_policy = EthBlockPolicy {
            latest_nonces: BTreeMap::new(),
        };
        for tx in txs.iter() {
            Pool::insert_tx(&mut pool, tx.clone().envelope_encoded().into());
        }
        let encoded_txns = Pool::create_proposal(&mut pool, 2, 3, &eth_block_policy, Vec::new());
        let decoded_txns = Vec::<EthSignedTransaction>::decode(&mut encoded_txns.as_ref()).unwrap();
        assert_eq!(decoded_txns, expected_txs);
    }

    #[test]
    #[traced_test]
    fn nontrivial_example() {
        let s1: B256 =
            hex!("0ed2e19e3aca1a321349f295837988e9c6f95d4a6fc54cfab6befd5ee82662ad").into(); // pubkey starts with AAA
        let s2: B256 =
            hex!("009ac901cf45a2e92e7e7bdf167dc52e3a6232be3c56cc3b05622b247c2c716a").into(); // pubkey starts with BBB
        let s3: B256 =
            hex!("0d756f31a3e98f1ae46475687cbfe3085ec74b3abdd712decff3e1e5e4c697a2").into(); // pubkey starts with CCC
        let txs = vec![
            make_tx(s1, 10, 1, 0, 10),
            make_tx(s1, 5, 1, 1, 10),
            make_tx(s1, 3, 1, 2, 10),
            make_tx(s2, 5, 1, 0, 10),
            make_tx(s2, 3, 1, 1, 10),
            make_tx(s2, 1, 1, 2, 10),
            make_tx(s3, 8, 1, 0, 10),
            make_tx(s3, 9, 1, 1, 10),
            make_tx(s3, 10, 1, 2, 10),
        ];
        let expected_txs = vec![
            make_tx(s1, 10, 1, 0, 10),
            make_tx(s3, 8, 1, 0, 10),
            make_tx(s3, 9, 1, 1, 10),
            make_tx(s3, 10, 1, 2, 10),
            make_tx(s1, 5, 1, 1, 10),
            make_tx(s2, 5, 1, 0, 10),
            make_tx(s2, 3, 1, 1, 10),
            make_tx(s1, 3, 1, 2, 10),
            make_tx(s2, 1, 1, 2, 10),
        ];
        let mut pool = EthTxPool::default();
        let eth_block_policy = EthBlockPolicy {
            latest_nonces: BTreeMap::new(),
        };
        for tx in txs.iter() {
            Pool::insert_tx(&mut pool, tx.clone().envelope_encoded().into());
        }
        let encoded_txns =
            Pool::create_proposal(&mut pool, 200, 300, &eth_block_policy, Vec::new());
        let decoded_txns = Vec::<EthSignedTransaction>::decode(&mut encoded_txns.as_ref()).unwrap();
        assert_eq!(decoded_txns, expected_txs);
    }

    #[test]
    #[traced_test]
    fn another_non_trivial_example() {
        let s1: B256 =
            hex!("0ed2e19e3aca1a321349f295837988e9c6f95d4a6fc54cfab6befd5ee82662ad").into(); // pubkey starts with AAA
        let s2: B256 =
            hex!("009ac901cf45a2e92e7e7bdf167dc52e3a6232be3c56cc3b05622b247c2c716a").into(); // pubkey starts with BBB
        let s3: B256 =
            hex!("0d756f31a3e98f1ae46475687cbfe3085ec74b3abdd712decff3e1e5e4c697a2").into(); // pubkey starts with CCC
        let txs = vec![
            make_tx(s1, 10, 1, 0, 10),
            make_tx(s1, 5, 1, 1, 10),
            make_tx(s2, 5, 1, 0, 10),
            make_tx(s2, 3, 1, 1, 10),
            make_tx(s3, 8, 1, 0, 10),
            make_tx(s3, 9, 1, 1, 10),
        ];
        let expected_txs = vec![
            make_tx(s1, 10, 1, 0, 10),
            make_tx(s3, 8, 1, 0, 10),
            make_tx(s3, 9, 1, 1, 10),
            make_tx(s1, 5, 1, 1, 10),
            make_tx(s2, 5, 1, 0, 10),
            make_tx(s2, 3, 1, 1, 10),
        ];

        let mut pool = EthTxPool::default();
        let eth_block_policy = EthBlockPolicy {
            latest_nonces: BTreeMap::new(),
        };
        for tx in txs.iter() {
            Pool::insert_tx(&mut pool, tx.clone().envelope_encoded().into());
        }
        let encoded_txns =
            Pool::create_proposal(&mut pool, 200, 300, &eth_block_policy, Vec::new());
        let decoded_txns = Vec::<EthSignedTransaction>::decode(&mut encoded_txns.as_ref()).unwrap();
        assert_eq!(decoded_txns, expected_txs);
    }

    #[test]
    #[traced_test]
    fn attacker_tries_to_include_transaction_with_large_gas_limit_to_exit_proposal_creation_early()
    {
        let s1: B256 =
            hex!("0ed2e19e3aca1a321349f295837988e9c6f95d4a6fc54cfab6befd5ee82662ad").into(); // pubkey starts with AAA
        let s2: B256 =
            hex!("009ac901cf45a2e92e7e7bdf167dc52e3a6232be3c56cc3b05622b247c2c716a").into(); // pubkey starts with BBB
        let txs = vec![
            make_tx(s1, 10, 100, 0, 10),
            make_tx(s2, 1, 1, 0, 10),
            make_tx(s2, 1, 1, 1, 10),
            make_tx(s2, 1, 1, 2, 10),
            make_tx(s2, 1, 1, 3, 10),
            make_tx(s2, 1, 1, 4, 10),
            make_tx(s2, 1, 1, 5, 10),
            make_tx(s2, 1, 1, 6, 10),
            make_tx(s2, 1, 1, 7, 10),
            make_tx(s2, 1, 1, 8, 10),
            make_tx(s2, 1, 1, 9, 10),
        ];
        let expected_txs = vec![
            make_tx(s2, 1, 1, 0, 10),
            make_tx(s2, 1, 1, 1, 10),
            make_tx(s2, 1, 1, 2, 10),
            make_tx(s2, 1, 1, 3, 10),
            make_tx(s2, 1, 1, 4, 10),
            make_tx(s2, 1, 1, 5, 10),
            make_tx(s2, 1, 1, 6, 10),
            make_tx(s2, 1, 1, 7, 10),
            make_tx(s2, 1, 1, 8, 10),
            make_tx(s2, 1, 1, 9, 10),
        ];

        let mut pool = EthTxPool::default();
        let eth_block_policy = EthBlockPolicy {
            latest_nonces: BTreeMap::new(),
        };
        for tx in txs.iter() {
            Pool::insert_tx(&mut pool, tx.clone().envelope_encoded().into());
        }
        let encoded_txns = Pool::create_proposal(&mut pool, 200, 10, &eth_block_policy, Vec::new());
        let decoded_txns = Vec::<EthSignedTransaction>::decode(&mut encoded_txns.as_ref()).unwrap();
        assert_eq!(decoded_txns, expected_txs);
    }

    #[test]
    #[traced_test]
    fn nondeterminism() {
        let s1: B256 =
            hex!("0ed2e19e3aca1a321349f295837988e9c6f95d4a6fc54cfab6befd5ee82662ad").into(); // pubkey starts with AAA
        let s2: B256 =
            hex!("009ac901cf45a2e92e7e7bdf167dc52e3a6232be3c56cc3b05622b247c2c716a").into(); // pubkey starts with BBB
        let s3: B256 =
            hex!("29b6132c13a004a476484efd02bbd0614527f8f34b94a360201c611b111deac9").into(); // pubkey starts with CCC
        let s4: B256 =
            hex!("871683e86bef90f2e790e60e4245916c731f540eec4a998697c2cbab4e156868").into(); // pubkey starts with DDD
        let s5: B256 =
            hex!("9c82e5ab4dda8da5391393c5eb7cb8b79ca8e03b3028be9ba1e31f2480e17dc8").into(); // pubkey starts with EEE
        let txs = vec![
            make_tx(s1, 1, 1, 0, 10),
            make_tx(s1, 1, 1, 1, 10),
            make_tx(s2, 1, 1, 0, 10),
            make_tx(s2, 1, 1, 1, 10),
            make_tx(s3, 1, 1, 0, 10),
            make_tx(s3, 1, 1, 1, 10),
            make_tx(s4, 1, 1, 0, 10),
            make_tx(s4, 1, 1, 1, 10),
            make_tx(s5, 1, 1, 0, 10),
            make_tx(s5, 1, 1, 1, 10),
        ];
        let expected_txs = vec![
            make_tx(s5, 1, 1, 0, 10),
            make_tx(s5, 1, 1, 1, 10),
            make_tx(s4, 1, 1, 0, 10),
            make_tx(s4, 1, 1, 1, 10),
            make_tx(s3, 1, 1, 0, 10),
            make_tx(s3, 1, 1, 1, 10),
            make_tx(s2, 1, 1, 0, 10),
            make_tx(s2, 1, 1, 1, 10),
            make_tx(s1, 1, 1, 0, 10),
            make_tx(s1, 1, 1, 1, 10),
        ];

        let mut pool = EthTxPool::default();
        let eth_block_policy = EthBlockPolicy {
            latest_nonces: BTreeMap::new(),
        };
        for tx in txs.iter() {
            Pool::insert_tx(&mut pool, tx.clone().envelope_encoded().into());
        }
        let encoded_txns = Pool::create_proposal(&mut pool, 10, 10, &eth_block_policy, Vec::new());
        let decoded_txns = Vec::<EthSignedTransaction>::decode(&mut encoded_txns.as_ref()).unwrap();
        assert_eq!(expected_txs, decoded_txns);
    }

    #[test]
    fn test_zero_nonce_included_in_block() {
        // The first transaction from an account with 0 nonce should be including in the block

        let sender_1_key = B256::random();
        let txn_nonce_zero = make_tx(sender_1_key, 1, 1, 0, 10);

        let mut eth_tx_pool = EthTxPool::default();
        let eth_block_policy = EthBlockPolicy {
            latest_nonces: BTreeMap::new(),
        };

        Pool::insert_tx(&mut eth_tx_pool, txn_nonce_zero.envelope_encoded().into());

        let encoded_txns = Pool::create_proposal(
            &mut eth_tx_pool,
            10_000,
            50_000,
            &eth_block_policy,
            Vec::new(),
        );
        let decoded_txns = Vec::<EthSignedTransaction>::decode(&mut encoded_txns.as_ref()).unwrap();
        assert_eq!(decoded_txns, vec![txn_nonce_zero]);
    }

    #[test]
    fn test_invalid_nonce_not_in_block() {
        // A transaction with nonce 3 should not be included in the block if the la

        let sender_1_key = B256::random();
        // Txn with nonce = 0
        let txn_nonce_zero = make_tx(sender_1_key, 1, 1, 0, 10);
        // Txn with nonce = 1
        let txn_nonce_one = make_tx(sender_1_key, 1, 1, 1, 10);
        // Txn with nonce = 3
        let txn_nonce_three = make_tx(sender_1_key, 1, 1, 3, 10);

        let mut eth_tx_pool = EthTxPool::default();
        let eth_block_policy = EthBlockPolicy {
            latest_nonces: BTreeMap::new(),
        };

        Pool::insert_tx(&mut eth_tx_pool, txn_nonce_zero.envelope_encoded().into());
        Pool::insert_tx(&mut eth_tx_pool, txn_nonce_one.envelope_encoded().into());
        Pool::insert_tx(&mut eth_tx_pool, txn_nonce_three.envelope_encoded().into());

        let encoded_txns = Pool::create_proposal(
            &mut eth_tx_pool,
            10_000,
            50_000,
            &eth_block_policy,
            Vec::new(),
        );
        let decoded_txns = Vec::<EthSignedTransaction>::decode(&mut encoded_txns.as_ref()).unwrap();

        // only transactions with nonce 0 and 1 should be included
        assert_eq!(decoded_txns, vec![txn_nonce_zero, txn_nonce_one]);
    }

    #[test]
    fn test_nonce_exists_in_committed_block() {
        // A transaction with nonce 0 should not be included in the block if the latest nonce of the account is 0

        let sender_1_key = B256::random();
        let txn_nonce_zero = make_tx(sender_1_key, 1, 1, 0, 10);
        let sender_1_address = EthAddress(txn_nonce_zero.recover_signer().unwrap());

        let mut eth_tx_pool = EthTxPool::default();
        let eth_block_policy = EthBlockPolicy {
            latest_nonces: vec![(sender_1_address, 0)].into_iter().collect(),
        };
        Pool::insert_tx(&mut eth_tx_pool, txn_nonce_zero.envelope_encoded().into());

        let encoded_txns = Pool::create_proposal(
            &mut eth_tx_pool,
            10_000,
            50_000,
            &eth_block_policy,
            Vec::new(),
        );
        let decoded_txns = Vec::<EthSignedTransaction>::decode(&mut encoded_txns.as_ref()).unwrap();

        assert_eq!(decoded_txns, vec![]);
    }

    #[test]
    fn test_nonce_exists_in_pending_block() {
        // A transaction with nonce 0 should not be included in the block if the latest nonce of the account is 0

        let sender_1_key = B256::random();
        // generate two transactions, both with nonce = 0
        let txn_1_nonce_zero = make_tx(sender_1_key, 1, 1, 0, 10);
        let txn_2_nonce_zero = make_tx(sender_1_key, 1, 1, 0, 1000);

        let mut eth_tx_pool = EthTxPool::default();
        // create the extending block with txn 1
        let extending_block = generate_random_block_with_txns(vec![txn_1_nonce_zero]);
        let eth_block_policy = EthBlockPolicy {
            latest_nonces: BTreeMap::new(),
        };

        // insert txn 2 into the tx pool
        Pool::insert_tx(&mut eth_tx_pool, txn_2_nonce_zero.envelope_encoded().into());

        let encoded_txns = Pool::create_proposal(
            &mut eth_tx_pool,
            10_000,
            50_000,
            &eth_block_policy,
            vec![&extending_block],
        );
        let decoded_txns = Vec::<EthSignedTransaction>::decode(&mut encoded_txns.as_ref()).unwrap();

        assert_eq!(decoded_txns, vec![]);
    }

    #[test]
    fn test_combine_nonces_of_blocks() {
        // TxPool should combine the nonces of commited block and pending blocks to check nonce

        let sender_1_key = B256::random();

        // txn with nonce = 1
        let txn_nonce_one = make_tx(sender_1_key, 1, 1, 1, 10);
        // txn with nonce = 2
        let txn_nonce_two = make_tx(sender_1_key, 1, 1, 2, 10);
        // txn with nonce = 3
        let txn_nonce_three = make_tx(sender_1_key, 1, 1, 3, 10);

        let mut eth_tx_pool = EthTxPool::default();
        let sender_1_address = EthAddress(txn_nonce_one.recover_signer().unwrap());
        let eth_block_policy = EthBlockPolicy {
            latest_nonces: vec![(sender_1_address, 0)].into_iter().collect(),
        };

        Pool::insert_tx(&mut eth_tx_pool, txn_nonce_one.envelope_encoded().into());
        Pool::insert_tx(&mut eth_tx_pool, txn_nonce_two.envelope_encoded().into());
        Pool::insert_tx(&mut eth_tx_pool, txn_nonce_three.envelope_encoded().into());

        // create the extending block 1 with txn 1
        let extending_block_1 = generate_random_block_with_txns(vec![txn_nonce_one]);
        // create the extending block 2 with txn 2
        let extending_block_2 = generate_random_block_with_txns(vec![txn_nonce_two]);

        let encoded_txns = eth_tx_pool.create_proposal(
            10_000,
            50_000,
            &eth_block_policy,
            vec![&extending_block_1, &extending_block_2],
        );
        let decoded_txns = Vec::<EthSignedTransaction>::decode(&mut encoded_txns.as_ref()).unwrap();

        assert_eq!(decoded_txns, vec![txn_nonce_three]);
    }
}
