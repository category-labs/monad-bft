use std::{
    cmp::Ordering,
    collections::{BTreeMap, BinaryHeap},
};

use alloy_rlp::Decodable;
use bytes::Bytes;
use monad_consensus_types::{
    payload::FullTransactionList,
    signature_collection::SignatureCollection,
    state::StateBackend,
    txpool::{TxPool, TxPoolInsertionError},
};
use monad_eth_block_policy::{AccountNonceRetrievable, EthBlockPolicy, EthValidatedBlock};
use monad_eth_tx::{EthFullTransactionList, EthTransaction, EthTxHash};
use monad_eth_types::{EthAddress, Nonce};
use reth_primitives::{Transaction, TxEip1559, TxEip2930, TxEip4844, TxLegacy};
use sorted_vector_map::SortedVectorMap;

type VirtualTimestamp = u64;

/// Needed to have control over Ord implementation
#[derive(Debug, PartialEq)]
struct WrappedTransaction<'a> {
    inner: &'a EthTransaction,
    insertion_time: VirtualTimestamp,
    price_gas_limit_ratio: f64,
}

fn effective_tip_per_gas(transaction: &EthTransaction) -> u128 {
    match transaction.transaction {
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

impl<'a> WrappedTransaction<'a> {
    pub fn effective_tip_per_gas(&self) -> u128 {
        effective_tip_per_gas(self.inner)
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

impl<'a> Eq for WrappedTransaction<'a> {}

impl<'a> Ord for WrappedTransaction<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Unwrap safety: Okay to unwrap here so long as we guarantee price_gas_limit_ratio is not NaN
        // when inserting into the mempool. partial_cmp is guaranteed to return Some(_) if neither
        // operand is NaN
        (self.price_gas_limit_ratio, self.insertion_time)
            .partial_cmp(&(other.price_gas_limit_ratio, other.insertion_time))
            .unwrap()
    }
}

impl<'a> PartialOrd for WrappedTransaction<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug, Default)]
struct TransactionGroup {
    transactions: SortedVectorMap<Nonce, (EthTransaction, f64)>,
}

impl TransactionGroup {
    fn add(&mut self, transaction: EthTransaction, ratio: f64) {
        self.transactions
            .insert(transaction.nonce(), (transaction, ratio));
    }

    fn find_nonce_gap(&self, mut next_nonce: Nonce) -> Option<Nonce> {
        for (nonce, _) in self.transactions.iter() {
            if *nonce != next_nonce {
                return Some(*nonce);
            }
            next_nonce += 1;
        }

        None
    }
}

type Pool = SortedVectorMap<EthAddress, TransactionGroup>;

#[derive(Clone, Default, Debug)]
pub struct EthTxPool {
    /// pool is transient, garbage collected after creating a proposal
    pool: Pool,
    garbage: Vec<Pool>,
}

impl EthTxPool {
    /// Removes nonces that cannot extend the current block tree branch, as the
    /// txpool is transient and garbage collected after proposal creation
    fn remove_invalid_nonces<SBT: StateBackend>(
        &mut self,
        block_policy: &EthBlockPolicy,
        blocktree_nonce_deltas: BTreeMap<EthAddress, Nonce>,
        state_backend: &SBT,
    ) {
        self.pool
            .iter_mut()
            .for_each(|(eth_address, transaction_group)| {
                let lowest_valid_nonce = block_policy.get_account_nonce(
                    eth_address,
                    &blocktree_nonce_deltas,
                    state_backend,
                );

                // Remove transactions with nonces lower than the lowest valid nonce
                transaction_group
                    .transactions
                    .retain(|&nonce, _| nonce >= lowest_valid_nonce);

                let maybe_nonce_gap = transaction_group.find_nonce_gap(lowest_valid_nonce);

                if let Some(gap) = maybe_nonce_gap {
                    // TODO: garbage collect
                    let _ = transaction_group.transactions.split_off(&gap);
                }
            });
        // TODO: garbage collect
        self.pool
            .retain(|_, transaction_group| !transaction_group.transactions.is_empty())
    }

    fn total_txns(&self) -> usize {
        self.pool
            .iter()
            .map(|(_, txn_group)| txn_group.transactions.len())
            .sum()
    }
}

impl<SCT: SignatureCollection, SBT: StateBackend> TxPool<SCT, SBT, EthBlockPolicy> for EthTxPool {
    fn insert_tx(&mut self, tx: Bytes) -> Result<(), TxPoolInsertionError> {
        // TODO: unwrap can be removed when this is made generic over the actual
        // tx type rather than Bytes and decoding won't be necessary
        // TODO(rene): sender recovery is done inline here
        let eth_tx = EthTransaction::decode(&mut tx.as_ref())
            .map_err(|_e| TxPoolInsertionError::NotWellFormed)?;
        let sender = EthAddress(eth_tx.signer());

        // TODO(rene): should any transaction validation occur here before inserting into mempool
        // TODO we should definitely return out early here if the nonce is invalid so that we don't
        //      forward txs that are known to be invalid
        // TODO once we have dynamic base fee, we should also exit out early if base fee isn't high
        // enough
        // we are going to compute a price : gas_limit ratio so we cannot have zero in the denominator
        if eth_tx.gas_limit() == 0 {
            return Err(TxPoolInsertionError::NotWellFormed);
        }

        let ratio = (effective_tip_per_gas(&eth_tx) as f64) / (eth_tx.gas_limit() as f64);

        if ratio.is_nan() {
            return Err(TxPoolInsertionError::NotWellFormed);
        }

        self.pool.entry(sender).or_default().add(eth_tx, ratio);
        Ok(())
    }

    fn create_proposal(
        &mut self,
        tx_limit: usize,
        proposal_gas_limit: u64,
        block_policy: &EthBlockPolicy,
        extending_blocks: Vec<&EthValidatedBlock<SCT>>,
        state_backend: &SBT,
    ) -> FullTransactionList {
        // Get the latest nonce from txns in the extending blocks
        let extending_account_nonces = extending_blocks.get_account_nonces();
        self.remove_invalid_nonces(block_policy, extending_account_nonces, state_backend);

        let mut txs = Vec::new();
        let mut total_gas = 0;

        let mut transaction_iters: BTreeMap<
            EthAddress,
            sorted_vector_map::map::Iter<u64, (reth_primitives::TransactionSignedEcRecovered, f64)>,
        > = self
            .pool
            .iter()
            .map(|(address, group)| (*address, group.transactions.iter()))
            .collect::<BTreeMap<_, _>>();

        let mut virtual_time: VirtualTimestamp = 0;

        let mut max_heap = BinaryHeap::<WrappedTransaction>::new();

        // queue one eligible transaction for each account (they will be the ones with the lowest nonce)
        for (_, transaction_iter) in transaction_iters.iter_mut() {
            match transaction_iter.next() {
                None => {
                    unreachable!()
                }
                Some((_, (transaction, price_gas_limit_ratio))) => {
                    max_heap.push(WrappedTransaction {
                        inner: transaction,
                        insertion_time: virtual_time,
                        price_gas_limit_ratio: *price_gas_limit_ratio,
                    });
                    virtual_time += 1;
                }
            }
        }

        // loop invariant: `max_heap` contains one transaction per account (lowest nonce).
        // the root of the heap will be the best priced eligible transaction
        while let Some(WrappedTransaction { inner: best_tx, .. }) = max_heap.pop() {
            let address = EthAddress(best_tx.signer());

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
                Some((_, (transaction, price_gas_limit_ratio))) => {
                    max_heap.push(WrappedTransaction {
                        inner: transaction,
                        insertion_time: virtual_time,
                        price_gas_limit_ratio: *price_gas_limit_ratio,
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
            proposal_tx_bytes = full_tx_list.len(),
            "created proposal"
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
    use alloy_primitives::{hex, B256};
    use alloy_rlp::Decodable;
    use monad_consensus_types::txpool::{TxPool, TxPoolInsertionError};
    use monad_crypto::NopSignature;
    use monad_eth_block_policy::nonce::InMemoryState;
    use monad_eth_testutil::{generate_random_block_with_txns, make_tx};
    use monad_eth_tx::EthSignedTransaction;
    use monad_eth_types::EthAddress;
    use monad_multi_sig::MultiSig;
    use monad_types::{SeqNum, GENESIS_SEQ_NUM};
    use tracing_test::traced_test;

    use crate::{EthBlockPolicy, EthTxPool};

    type Pool = dyn TxPool<MultiSig<NopSignature>, InMemoryState, EthBlockPolicy>;

    #[test]
    #[traced_test]
    fn test_create_proposal_with_insufficient_tx_limit() {
        let tx = make_tx(B256::repeat_byte(0xAu8), 1, 1, 0, 10);
        let mut pool = EthTxPool::default();
        let eth_block_policy = EthBlockPolicy::new(GENESIS_SEQ_NUM, SeqNum(5));
        Pool::insert_tx(&mut pool, tx.envelope_encoded().into()).unwrap();
        assert_eq!(pool.pool.len(), 1);
        assert_eq!(pool.pool.first_key_value().unwrap().1.transactions.len(), 1);

        let encoded_txns = Pool::create_proposal(
            &mut pool,
            0,
            1_000_000,
            &eth_block_policy,
            Vec::new(),
            &InMemoryState::default(),
        );

        let decoded_txns = Vec::<EthSignedTransaction>::decode(&mut encoded_txns.as_ref()).unwrap();
        assert_eq!(decoded_txns, vec![]);
        assert!(pool.pool.is_empty());
    }

    #[test]
    #[traced_test]
    fn test_create_proposal_with_insufficient_gas_limit() {
        let tx = make_tx(B256::repeat_byte(0xAu8), 1, 6400, 0, 10);
        let mut pool = EthTxPool::default();
        let eth_block_policy = EthBlockPolicy::new(GENESIS_SEQ_NUM, SeqNum(5));
        Pool::insert_tx(&mut pool, tx.envelope_encoded().into()).unwrap();
        assert_eq!(pool.pool.len(), 1);
        assert_eq!(pool.pool.first_key_value().unwrap().1.transactions.len(), 1);

        let encoded_txns = Pool::create_proposal(
            &mut pool,
            1,
            6399,
            &eth_block_policy,
            Vec::new(),
            &InMemoryState::default(),
        );

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
        let eth_block_policy = EthBlockPolicy::new(GENESIS_SEQ_NUM, SeqNum(5));
        Pool::insert_tx(&mut pool, t1.envelope_encoded().into()).unwrap();
        Pool::insert_tx(&mut pool, t2.envelope_encoded().into()).unwrap();
        Pool::insert_tx(&mut pool, t3.envelope_encoded().into()).unwrap();
        assert_eq!(pool.pool.len(), 1);
        assert_eq!(pool.pool.first_key_value().unwrap().1.transactions.len(), 3);

        let encoded_txns = Pool::create_proposal(
            &mut pool,
            2,
            6400 * 2,
            &eth_block_policy,
            Vec::new(),
            &InMemoryState::default(),
        );
        let decoded_txns = Vec::<EthSignedTransaction>::decode(&mut encoded_txns.as_ref()).unwrap();
        assert_eq!(decoded_txns, expected_txs);
    }

    #[test]
    fn test_basic_price_priority() {
        let s1 = B256::repeat_byte(0xAu8); // 0xC171033d5CBFf7175f29dfD3A63dDa3d6F8F385E
        let s2 = B256::repeat_byte(0xBu8); // 0xf288ECAF15790EfcAc528946963A6Db8c3f8211d
        let txs = vec![make_tx(s1, 1, 1, 0, 10), make_tx(s2, 2, 2, 0, 10)];
        let expected_txs = vec![make_tx(s2, 2, 2, 0, 10), make_tx(s1, 1, 1, 0, 10)];
        let mut pool = EthTxPool::default();
        let eth_block_policy = EthBlockPolicy::new(GENESIS_SEQ_NUM, SeqNum(5));
        for tx in txs.iter() {
            Pool::insert_tx(&mut pool, tx.clone().envelope_encoded().into()).unwrap();
        }
        let encoded_txns = Pool::create_proposal(
            &mut pool,
            2,
            3,
            &eth_block_policy,
            Vec::new(),
            &InMemoryState::default(),
        );
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
        let eth_block_policy = EthBlockPolicy::new(GENESIS_SEQ_NUM, SeqNum(5));
        for tx in txs.iter() {
            Pool::insert_tx(&mut pool, tx.clone().envelope_encoded().into()).unwrap();
        }
        let encoded_txns = Pool::create_proposal(
            &mut pool,
            2,
            3,
            &eth_block_policy,
            Vec::new(),
            &InMemoryState::default(),
        );
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
        let eth_block_policy = EthBlockPolicy::new(GENESIS_SEQ_NUM, SeqNum(5));
        for tx in txs.iter() {
            Pool::insert_tx(&mut pool, tx.clone().envelope_encoded().into()).unwrap();
        }
        let encoded_txns = Pool::create_proposal(
            &mut pool,
            200,
            300,
            &eth_block_policy,
            Vec::new(),
            &InMemoryState::default(),
        );
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
        let eth_block_policy = EthBlockPolicy::new(GENESIS_SEQ_NUM, SeqNum(5));
        for tx in txs.iter() {
            Pool::insert_tx(&mut pool, tx.clone().envelope_encoded().into()).unwrap();
        }
        let encoded_txns = Pool::create_proposal(
            &mut pool,
            200,
            300,
            &eth_block_policy,
            Vec::new(),
            &InMemoryState::default(),
        );
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
        let eth_block_policy = EthBlockPolicy::new(GENESIS_SEQ_NUM, SeqNum(5));
        for tx in txs.iter() {
            Pool::insert_tx(&mut pool, tx.clone().envelope_encoded().into()).unwrap();
        }
        let encoded_txns = Pool::create_proposal(
            &mut pool,
            200,
            10,
            &eth_block_policy,
            Vec::new(),
            &InMemoryState::default(),
        );
        let decoded_txns = Vec::<EthSignedTransaction>::decode(&mut encoded_txns.as_ref()).unwrap();
        assert_eq!(decoded_txns, expected_txs);
    }

    #[test]
    #[traced_test]
    fn suboptimal_block() {
        let s1: B256 =
            hex!("0ed2e19e3aca1a321349f295837988e9c6f95d4a6fc54cfab6befd5ee82662ad").into(); // pubkey starts with AAA
        let s2: B256 =
            hex!("009ac901cf45a2e92e7e7bdf167dc52e3a6232be3c56cc3b05622b247c2c716a").into(); // pubkey starts with BBB
        let txs = vec![
            make_tx(s1, 2, 10, 0, 10),
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
        for tx in txs.iter() {
            Pool::insert_tx(&mut pool, tx.clone().envelope_encoded().into()).unwrap();
        }
        let encoded_txns = Pool::create_proposal(
            &mut pool,
            10,
            10,
            &EthBlockPolicy::new(GENESIS_SEQ_NUM, SeqNum(5)),
            Default::default(),
            &InMemoryState::default(),
        );
        let decoded_txns = Vec::<EthSignedTransaction>::decode(&mut encoded_txns.as_ref()).unwrap();
        assert_eq!(decoded_txns, expected_txs);
    }

    #[test]
    #[traced_test]
    fn zero_gas_limit() {
        let s1: B256 =
            hex!("0ed2e19e3aca1a321349f295837988e9c6f95d4a6fc54cfab6befd5ee82662ad").into(); // pubkey starts with AAA
        let txs = vec![make_tx(s1, 1, 0, 0, 10)];
        let mut pool = EthTxPool::default();
        for tx in txs.iter() {
            let r = Pool::insert_tx(&mut pool, tx.clone().envelope_encoded().into());
            assert!(matches!(
                r.expect_err("gas limit 0 tx"),
                TxPoolInsertionError::NotWellFormed
            ));
        }
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
        let eth_block_policy = EthBlockPolicy::new(GENESIS_SEQ_NUM, SeqNum(5));
        for tx in txs.iter() {
            Pool::insert_tx(&mut pool, tx.clone().envelope_encoded().into()).unwrap();
        }
        let encoded_txns = Pool::create_proposal(
            &mut pool,
            10,
            10,
            &eth_block_policy,
            Vec::new(),
            &InMemoryState::default(),
        );
        let decoded_txns = Vec::<EthSignedTransaction>::decode(&mut encoded_txns.as_ref()).unwrap();
        assert_eq!(expected_txs, decoded_txns);
    }

    #[test]
    fn test_zero_nonce_included_in_block() {
        // The first transaction from an account with 0 nonce should be including in the block

        let sender_1_key = B256::random();
        let txn_nonce_zero = make_tx(sender_1_key, 1, 1, 0, 10);

        let mut eth_tx_pool = EthTxPool::default();
        let eth_block_policy = EthBlockPolicy::new(GENESIS_SEQ_NUM, SeqNum(5));

        Pool::insert_tx(&mut eth_tx_pool, txn_nonce_zero.envelope_encoded().into()).unwrap();

        let encoded_txns = Pool::create_proposal(
            &mut eth_tx_pool,
            10_000,
            50_000,
            &eth_block_policy,
            Vec::new(),
            &InMemoryState::default(),
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
        let eth_block_policy = EthBlockPolicy::new(GENESIS_SEQ_NUM, SeqNum(5));

        Pool::insert_tx(&mut eth_tx_pool, txn_nonce_zero.envelope_encoded().into()).unwrap();
        Pool::insert_tx(&mut eth_tx_pool, txn_nonce_one.envelope_encoded().into()).unwrap();
        Pool::insert_tx(&mut eth_tx_pool, txn_nonce_three.envelope_encoded().into()).unwrap();

        let encoded_txns = Pool::create_proposal(
            &mut eth_tx_pool,
            10_000,
            50_000,
            &eth_block_policy,
            Vec::new(),
            &InMemoryState::default(),
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
        let txn_nonce_one = make_tx(sender_1_key, 1, 1, 1, 10);
        let sender_1_address = EthAddress(txn_nonce_zero.recover_signer().unwrap());

        let mut eth_tx_pool = EthTxPool::default();
        let eth_block_policy = EthBlockPolicy::new(GENESIS_SEQ_NUM, SeqNum(5));
        Pool::insert_tx(&mut eth_tx_pool, txn_nonce_zero.envelope_encoded().into()).unwrap();
        Pool::insert_tx(&mut eth_tx_pool, txn_nonce_one.envelope_encoded().into()).unwrap();

        let encoded_txns = Pool::create_proposal(
            &mut eth_tx_pool,
            10_000,
            50_000,
            &eth_block_policy,
            Vec::new(),
            &InMemoryState::new(vec![(sender_1_address, 1)]),
        );
        let decoded_txns = Vec::<EthSignedTransaction>::decode(&mut encoded_txns.as_ref()).unwrap();

        assert_eq!(decoded_txns, vec![txn_nonce_one]);
    }

    #[test]
    fn test_nonce_exists_in_pending_block() {
        // A transaction with nonce 0 should not be included in the block if the latest nonce of the account is 0

        let sender_1_key = B256::random();
        // generate two transactions, both with nonce = 0
        let txn_1_nonce_zero = make_tx(sender_1_key, 1, 1, 0, 10);
        let txn_2_nonce_zero = make_tx(sender_1_key, 1, 1, 0, 1000);
        let txn_nonce_one = make_tx(sender_1_key, 1, 1, 1, 10);

        let mut eth_tx_pool = EthTxPool::default();
        // create the extending block with txn 1
        let extending_block = generate_random_block_with_txns(vec![txn_1_nonce_zero]);
        let eth_block_policy = EthBlockPolicy::new(GENESIS_SEQ_NUM, SeqNum(5));

        // insert txn 2 into the tx pool
        Pool::insert_tx(&mut eth_tx_pool, txn_2_nonce_zero.envelope_encoded().into()).unwrap();
        Pool::insert_tx(&mut eth_tx_pool, txn_nonce_one.envelope_encoded().into()).unwrap();

        let encoded_txns = Pool::create_proposal(
            &mut eth_tx_pool,
            10_000,
            50_000,
            &eth_block_policy,
            vec![&extending_block],
            &InMemoryState::default(),
        );
        let decoded_txns = Vec::<EthSignedTransaction>::decode(&mut encoded_txns.as_ref()).unwrap();

        assert_eq!(decoded_txns, vec![txn_nonce_one]);
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
        let eth_block_policy = EthBlockPolicy::new(GENESIS_SEQ_NUM, SeqNum(5));

        Pool::insert_tx(&mut eth_tx_pool, txn_nonce_one.envelope_encoded().into()).unwrap();
        Pool::insert_tx(&mut eth_tx_pool, txn_nonce_two.envelope_encoded().into()).unwrap();
        Pool::insert_tx(&mut eth_tx_pool, txn_nonce_three.envelope_encoded().into()).unwrap();

        // create the extending block 1 with txn 1
        let extending_block_1 = generate_random_block_with_txns(vec![txn_nonce_one]);
        // create the extending block 2 with txn 2
        let extending_block_2 = generate_random_block_with_txns(vec![txn_nonce_two]);

        let encoded_txns = eth_tx_pool.create_proposal(
            10_000,
            50_000,
            &eth_block_policy,
            vec![&extending_block_1, &extending_block_2],
            &InMemoryState::new(vec![(sender_1_address, 1)]),
        );
        let decoded_txns = Vec::<EthSignedTransaction>::decode(&mut encoded_txns.as_ref()).unwrap();

        assert_eq!(decoded_txns, vec![txn_nonce_three]);
    }
}
