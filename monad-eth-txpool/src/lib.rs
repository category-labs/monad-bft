use std::collections::{BTreeMap, BinaryHeap};

use alloy_rlp::Decodable;
use bytes::Bytes;
use itertools::Itertools;
use monad_consensus_types::{
    payload::FullTransactionList,
    signature_collection::SignatureCollection,
    txpool::{TxPool, TxPoolInsertionError},
};
use monad_eth_block_policy::{
    compute_txn_carriage_cost, static_validate_transaction, EthBlockPolicy, EthValidatedBlock,
};
use monad_eth_tx::{EthFullTransactionList, EthTransaction};
use monad_eth_types::{Balance, EthAddress};
use monad_state_backend::{StateBackend, StateBackendError};
use monad_types::SeqNum;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use tracing::{info, trace, warn};

use crate::{
    pool::Pool,
    transaction::{VirtualTimestamp, WrappedTransaction},
    utils::effective_tip_per_gas,
};

mod pool;
mod transaction;
mod utils;

const MAX_TXPOOL_SIZE: usize = 1024 * 32;

#[derive(Clone, Debug, Default)]
pub struct EthTxPool {
    /// pool is transient, garbage collected after creating a proposal
    pool: Pool,
    garbage: Vec<Pool>,
}

impl EthTxPool {
    /// Removes nonces that cannot extend the current block tree branch, as the
    /// txpool is transient and garbage collected after proposal creation
    fn validate_nonces_and_carriage_fee<SCT>(
        &mut self,
        proposed_seq_num: SeqNum,
        block_policy: &EthBlockPolicy,
        state_backend: &impl StateBackend,
        extending_blocks: &Vec<&EthValidatedBlock<SCT>>,
    ) -> Result<(), StateBackendError>
    where
        SCT: SignatureCollection,
    {
        // TODO don't copy all the addresses
        let addresses = self.pool.iter_addresses().cloned().collect::<Vec<_>>();

        let account_base_nonces = block_policy.get_account_base_nonces(
            proposed_seq_num,
            state_backend,
            extending_blocks,
            addresses.iter(),
        )?;

        let account_base_reserve_balances = block_policy.compute_account_base_reserve_balances(
            proposed_seq_num,
            state_backend,
            Some(extending_blocks),
            addresses.iter(),
        )?;

        self.pool.validate_nonces_and_carriage_fee(
            proposed_seq_num,
            account_base_nonces,
            account_base_reserve_balances,
        );

        Ok(())
    }

    fn validate_and_insert_tx(
        &mut self,
        valid_chain_id: u64,
        eth_tx: EthTransaction,
        reserve_balance: &Balance,
    ) -> Result<(), TxPoolInsertionError> {
        // TODO(abenedito): Smarter tx eviction when pool is full
        if self.pool.num_txs() > MAX_TXPOOL_SIZE {
            return Err(TxPoolInsertionError::PoolFull);
        }

        let sender = EthAddress(eth_tx.signer());
        let txn_hash = eth_tx.hash();

        if static_validate_transaction(&eth_tx, valid_chain_id).is_err() {
            return Err(TxPoolInsertionError::NotWellFormed);
        }

        let ratio = (effective_tip_per_gas(&eth_tx) as f64) / (eth_tx.gas_limit() as f64);

        if ratio.is_nan() {
            return Err(TxPoolInsertionError::NotWellFormed);
        }

        // TODO nonce check

        let txn_carriage_cost = compute_txn_carriage_cost(&eth_tx);
        if &txn_carriage_cost > reserve_balance {
            trace!(
                "ReserveBalance insert_tx 2 \
                            do not add txn to the pool. insufficient balance: {:?} \
                            txn_carriage_cost: {:?} \
                            for address: {:?}",
                reserve_balance,
                txn_carriage_cost,
                sender
            );
            return Err(TxPoolInsertionError::InsufficientBalance);
        }

        // TODO: this doesn't account for txns already in the mempool,
        // an account can still send infinite transactions into the
        // mempool

        // TODO(rene): should any transaction validation occur here before inserting into mempool
        self.pool.add_tx(sender, eth_tx, ratio);
        trace!(
            "ReserveBalance insert_tx 1 \
                            reserve balance: {:?} \
                            txn carriage cost: {:?} \
                            for address: {:?}",
            reserve_balance,
            txn_carriage_cost,
            sender
        );
        trace!(txn_hash = ?txn_hash, ?sender, "txn inserted into txpool");
        Ok(())
    }

    fn validate_and_insert_txs<SCT>(
        &mut self,
        block_policy: &EthBlockPolicy,
        state_backend: &impl StateBackend,
        txs: Vec<EthTransaction>,
    ) -> Result<Vec<Result<(), TxPoolInsertionError>>, StateBackendError>
    where
        SCT: SignatureCollection,
    {
        let senders = txs.iter().map(|tx| EthAddress(tx.signer())).collect_vec();
        let block_seq_num = block_policy.get_last_commit() + SeqNum(1); // ?????
        let sender_reserve_balances = block_policy.compute_account_base_reserve_balances::<SCT>(
            block_seq_num,
            state_backend,
            None,
            senders.iter(),
        )?;

        let results = txs
            .into_iter()
            .zip(senders.iter())
            .map(|(eth_tx, sender)| {
                self.validate_and_insert_tx(
                    block_policy.get_chain_id(),
                    eth_tx,
                    sender_reserve_balances
                        .get(sender)
                        .expect("sender_reserve_balances should be populated"),
                )
            })
            .collect();
        Ok(results)
    }
}

impl<SCT, SBT> TxPool<SCT, EthBlockPolicy, SBT> for EthTxPool
where
    SCT: SignatureCollection,
    SBT: StateBackend,
{
    fn insert_tx(
        &mut self,
        txns: Vec<Bytes>,
        block_policy: &EthBlockPolicy,
        state_backend: &SBT,
    ) -> Vec<Bytes> {
        // TODO: unwrap can be removed when this is made generic over the actual
        // tx type rather than Bytes and decoding won't be necessary
        // TODO(rene): sender recovery is done inline here
        let (decoded_txs, raw_txs): (Vec<_>, Vec<_>) = txns
            .into_par_iter()
            .filter_map(|b| {
                EthTransaction::decode(&mut b.as_ref())
                    .ok()
                    .map(|valid_tx| (valid_tx, b))
            })
            .unzip();

        let Ok(insertion_results) =
            self.validate_and_insert_txs::<SCT>(block_policy, state_backend, decoded_txs)
        else {
            // can't insert, state backend is delayed
            return Vec::new();
        };

        insertion_results
            .into_iter()
            .zip(raw_txs)
            .filter_map(|(insertion_result, b)| match insertion_result {
                Ok(()) => Some(b),
                Err(_) => None,
            })
            .collect::<Vec<_>>()
    }

    fn create_proposal(
        &mut self,
        proposed_seq_num: SeqNum,
        tx_limit: usize,
        proposal_gas_limit: u64,
        block_policy: &EthBlockPolicy,
        extending_blocks: Vec<&EthValidatedBlock<SCT>>,
        state_backend: &SBT,
    ) -> Result<FullTransactionList, StateBackendError> {
        if let Err(err) = self.validate_nonces_and_carriage_fee(
            proposed_seq_num,
            block_policy,
            state_backend,
            &extending_blocks,
        ) {
            trace!("ReserveBalance create_proposal returned error: {:?}", err);
            return Err(err);
        }

        let mut txs = Vec::new();
        let mut total_gas = 0;

        let mut transaction_iters: BTreeMap<
            EthAddress,
            sorted_vector_map::map::Iter<u64, (reth_primitives::TransactionSignedEcRecovered, f64)>,
        > = self
            .pool
            .iter()
            .map(|(address, group)| (*address, group.iter()))
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

            // We also want to make sure that the transaction has max_fee_per_gas larger than base fee
            // TODO(kai): currently block base fee is hardcoded to 1000 in monad-ledger
            // update this when base fee is included in consensus proposal
            if best_tx.max_fee_per_gas() < 1000 {
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
            trace!(txn_hash = ?best_tx.hash(), "txn included in proposal");
            txs.push(best_tx.clone());
        }

        let proposal_num_tx = txs.len();
        let full_tx_list = EthFullTransactionList(txs).rlp_encode();

        info!(
            ?proposed_seq_num,
            ?proposal_num_tx,
            proposal_total_gas = total_gas,
            proposal_tx_bytes = full_tx_list.len(),
            "created proposal"
        );

        if !self.garbage.is_empty() {
            warn!(
                garbage_len = self.garbage.len(),
                "we have received consecutive proposals without a mempool clear event in between"
            );
        }

        let old_pool = std::mem::take(&mut self.pool);
        self.garbage.push(old_pool);

        Ok(FullTransactionList::new(full_tx_list))
    }

    fn clear(&mut self) {
        self.garbage.clear();
    }
}

#[cfg(test)]
mod test {
    use alloy_primitives::{hex, B256};
    use alloy_rlp::Decodable;
    use bytes::Bytes;
    use monad_consensus_types::txpool::TxPool;
    use monad_crypto::NopSignature;
    use monad_eth_testutil::{generate_random_block_with_txns, make_tx};
    use monad_eth_tx::EthSignedTransaction;
    use monad_eth_types::{Balance, EthAddress};
    use monad_multi_sig::MultiSig;
    use monad_state_backend::{InMemoryBlockState, InMemoryState, InMemoryStateInner};
    use monad_types::{SeqNum, GENESIS_SEQ_NUM};
    use tracing_test::traced_test;

    use crate::{EthBlockPolicy, EthTxPool};

    const EXECUTION_DELAY: u64 = 4;
    const BASE_FEE: u128 = 1000;
    const GAS_LIMIT: u64 = 30000;

    type Pool = dyn TxPool<MultiSig<NopSignature>, EthBlockPolicy, InMemoryState>;

    fn make_test_block_policy() -> EthBlockPolicy {
        EthBlockPolicy::new(GENESIS_SEQ_NUM, u128::MAX, EXECUTION_DELAY, 1337)
    }

    #[test]
    #[traced_test]
    fn test_create_proposal_with_insufficient_tx_limit() {
        let tx = make_tx(B256::repeat_byte(0xAu8), BASE_FEE, GAS_LIMIT, 0, 10);
        let mut pool = EthTxPool::default();
        let eth_block_policy = make_test_block_policy();
        let acc = std::iter::once((EthAddress(tx.recover_signer().unwrap()), 0));
        let state_backend = InMemoryStateInner::new(
            Balance::MAX,
            SeqNum(4),
            InMemoryBlockState::genesis(acc.collect()),
        );
        assert!(!Pool::insert_tx(
            &mut pool,
            vec![tx.envelope_encoded().into()],
            &eth_block_policy,
            &state_backend,
        )
        .is_empty());
        assert_eq!(pool.pool.num_addresses(), 1);
        assert_eq!(pool.pool.num_txs(), 1);

        let encoded_txns = Pool::create_proposal(
            &mut pool,
            SeqNum(0),
            0,
            1_000_000,
            &eth_block_policy,
            Vec::new(),
            &state_backend,
        )
        .unwrap();

        let decoded_txns = Vec::<EthSignedTransaction>::decode(&mut encoded_txns.as_ref()).unwrap();
        assert_eq!(decoded_txns, vec![]);
        assert!(pool.pool.is_empty());
    }

    #[test]
    #[traced_test]
    fn test_create_proposal_with_insufficient_gas_limit() {
        let tx = make_tx(B256::repeat_byte(0xAu8), BASE_FEE, GAS_LIMIT + 1, 0, 10);
        let mut pool = EthTxPool::default();
        let eth_block_policy = make_test_block_policy();
        let acc = std::iter::once((EthAddress(tx.recover_signer().unwrap()), 0));
        let state_backend = InMemoryStateInner::new(
            Balance::MAX,
            SeqNum(4),
            InMemoryBlockState::genesis(acc.collect()),
        );
        assert!(!Pool::insert_tx(
            &mut pool,
            vec![tx.envelope_encoded().into()],
            &eth_block_policy,
            &state_backend,
        )
        .is_empty());
        assert_eq!(pool.pool.num_addresses(), 1);
        assert_eq!(pool.pool.num_txs(), 1);

        let encoded_txns = Pool::create_proposal(
            &mut pool,
            SeqNum(0),
            1,
            GAS_LIMIT,
            &eth_block_policy,
            Vec::new(),
            &state_backend,
        )
        .unwrap();

        let decoded_txns = Vec::<EthSignedTransaction>::decode(&mut encoded_txns.as_ref()).unwrap();
        assert_eq!(decoded_txns, vec![]);
        assert!(pool.pool.is_empty());
    }

    #[test]
    #[traced_test]
    fn test_create_partial_proposal_with_insufficient_gas_limit() {
        let t1 = make_tx(B256::repeat_byte(0xAu8), BASE_FEE, GAS_LIMIT, 0, 10);
        let t2 = make_tx(B256::repeat_byte(0xAu8), BASE_FEE, GAS_LIMIT, 1, 10);
        let t3 = make_tx(B256::repeat_byte(0xAu8), BASE_FEE, GAS_LIMIT, 2, 10);
        let expected_txs = vec![
            make_tx(B256::repeat_byte(0xAu8), BASE_FEE, GAS_LIMIT, 0, 10),
            make_tx(B256::repeat_byte(0xAu8), BASE_FEE, GAS_LIMIT, 1, 10),
        ];
        let mut pool = EthTxPool::default();
        let eth_block_policy = make_test_block_policy();
        let acc = std::iter::once((EthAddress(t1.recover_signer().unwrap()), 0));
        let state_backend = InMemoryStateInner::new(
            Balance::MAX,
            SeqNum(4),
            InMemoryBlockState::genesis(acc.collect()),
        );

        let txns = vec![
            t1.envelope_encoded().into(),
            t2.envelope_encoded().into(),
            t3.envelope_encoded().into(),
        ];

        assert!(!Pool::insert_tx(&mut pool, txns, &eth_block_policy, &state_backend,).is_empty());
        assert_eq!(pool.pool.num_addresses(), 1);
        assert_eq!(pool.pool.num_txs(), 3);

        let encoded_txns = Pool::create_proposal(
            &mut pool,
            SeqNum(0),
            2,
            GAS_LIMIT * 2,
            &eth_block_policy,
            Vec::new(),
            &state_backend,
        )
        .unwrap();
        let decoded_txns = Vec::<EthSignedTransaction>::decode(&mut encoded_txns.as_ref()).unwrap();
        assert_eq!(decoded_txns, expected_txs);
    }

    #[test]
    fn test_basic_price_priority() {
        let s1 = B256::repeat_byte(0xAu8); // 0xC171033d5CBFf7175f29dfD3A63dDa3d6F8F385E
        let s2 = B256::repeat_byte(0xBu8); // 0xf288ECAF15790EfcAc528946963A6Db8c3f8211d
        let txs = vec![
            make_tx(s1, BASE_FEE, GAS_LIMIT, 0, 10),
            make_tx(s2, 2 * BASE_FEE, 2 * GAS_LIMIT, 0, 10),
        ];

        let a1 = EthAddress(txs[0].recover_signer().unwrap());
        let a2 = EthAddress(txs[1].recover_signer().unwrap());

        let expected_txs = vec![
            make_tx(s2, 2 * BASE_FEE, 2 * GAS_LIMIT, 0, 10),
            make_tx(s1, BASE_FEE, GAS_LIMIT, 0, 10),
        ];
        let mut pool = EthTxPool::default();
        let eth_block_policy = make_test_block_policy();
        let state_backend = InMemoryStateInner::new(
            Balance::MAX,
            SeqNum(4),
            InMemoryBlockState::genesis(vec![(a1, 0), (a2, 0)].into_iter().collect()),
        );

        let txns: Vec<Bytes> = txs
            .iter()
            .map(|t| t.clone().envelope_encoded().into())
            .collect();
        assert!(!Pool::insert_tx(&mut pool, txns, &eth_block_policy, &state_backend,).is_empty());

        let encoded_txns = Pool::create_proposal(
            &mut pool,
            SeqNum(0),
            2,
            GAS_LIMIT * 3,
            &eth_block_policy,
            Vec::new(),
            &state_backend,
        )
        .unwrap();
        let decoded_txns = Vec::<EthSignedTransaction>::decode(&mut encoded_txns.as_ref()).unwrap();
        assert_eq!(decoded_txns, expected_txs);
    }

    #[test]
    #[traced_test]
    fn test_resubmit_with_better_price() {
        let s1 = B256::repeat_byte(0xAu8); // 0xC171033d5CBFf7175f29dfD3A63dDa3d6F8F385E
        let txs = vec![
            make_tx(s1, BASE_FEE, GAS_LIMIT, 0, 10),
            make_tx(s1, 2 * BASE_FEE, 2 * GAS_LIMIT, 0, 10),
        ];
        let a1 = EthAddress(txs[0].recover_signer().unwrap());
        let expected_txs = vec![make_tx(s1, 2 * BASE_FEE, 2 * GAS_LIMIT, 0, 10)];

        let mut pool = EthTxPool::default();
        let eth_block_policy = make_test_block_policy();
        let state_backend = InMemoryStateInner::new(
            Balance::MAX,
            SeqNum(4),
            InMemoryBlockState::genesis(vec![(a1, 0)].into_iter().collect()),
        );
        let txns: Vec<Bytes> = txs
            .iter()
            .map(|t| t.clone().envelope_encoded().into())
            .collect();
        assert!(!Pool::insert_tx(&mut pool, txns, &eth_block_policy, &state_backend,).is_empty());
        let encoded_txns = Pool::create_proposal(
            &mut pool,
            SeqNum(0),
            2,
            3 * GAS_LIMIT,
            &eth_block_policy,
            Vec::new(),
            &state_backend,
        )
        .unwrap();
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
            make_tx(s1, 10 * BASE_FEE, GAS_LIMIT, 0, 10),
            make_tx(s1, 5 * BASE_FEE, GAS_LIMIT, 1, 10),
            make_tx(s1, 3 * BASE_FEE, GAS_LIMIT, 2, 10),
            make_tx(s2, 5 * BASE_FEE, GAS_LIMIT, 0, 10),
            make_tx(s2, 3 * BASE_FEE, GAS_LIMIT, 1, 10),
            make_tx(s2, 1 * BASE_FEE, GAS_LIMIT, 2, 10),
            make_tx(s3, 8 * BASE_FEE, GAS_LIMIT, 0, 10),
            make_tx(s3, 9 * BASE_FEE, GAS_LIMIT, 1, 10),
            make_tx(s3, 10 * BASE_FEE, GAS_LIMIT, 2, 10),
        ];
        let expected_txs = vec![
            make_tx(s1, 10 * BASE_FEE, GAS_LIMIT, 0, 10),
            make_tx(s3, 8 * BASE_FEE, GAS_LIMIT, 0, 10),
            make_tx(s3, 9 * BASE_FEE, GAS_LIMIT, 1, 10),
            make_tx(s3, 10 * BASE_FEE, GAS_LIMIT, 2, 10),
            make_tx(s1, 5 * BASE_FEE, GAS_LIMIT, 1, 10),
            make_tx(s2, 5 * BASE_FEE, GAS_LIMIT, 0, 10),
            make_tx(s2, 3 * BASE_FEE, GAS_LIMIT, 1, 10),
            make_tx(s1, 3 * BASE_FEE, GAS_LIMIT, 2, 10),
            make_tx(s2, 1 * BASE_FEE, GAS_LIMIT, 2, 10),
        ];
        let acc = txs
            .iter()
            .map(|tx| (EthAddress(tx.recover_signer().unwrap()), 0));
        let mut pool = EthTxPool::default();
        let eth_block_policy = make_test_block_policy();
        let state_backend = InMemoryStateInner::new(
            Balance::MAX,
            SeqNum(4),
            InMemoryBlockState::genesis(acc.collect()),
        );
        let txns: Vec<Bytes> = txs
            .iter()
            .map(|t| t.clone().envelope_encoded().into())
            .collect();
        assert!(!Pool::insert_tx(&mut pool, txns, &eth_block_policy, &state_backend,).is_empty());
        let encoded_txns = Pool::create_proposal(
            &mut pool,
            SeqNum(0),
            200,
            300 * GAS_LIMIT,
            &eth_block_policy,
            Vec::new(),
            &state_backend,
        )
        .unwrap();
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
            make_tx(s1, 10 * BASE_FEE, GAS_LIMIT, 0, 10),
            make_tx(s1, 5 * BASE_FEE, GAS_LIMIT, 1, 10),
            make_tx(s2, 5 * BASE_FEE, GAS_LIMIT, 0, 10),
            make_tx(s2, 3 * BASE_FEE, GAS_LIMIT, 1, 10),
            make_tx(s3, 8 * BASE_FEE, GAS_LIMIT, 0, 10),
            make_tx(s3, 9 * BASE_FEE, GAS_LIMIT, 1, 10),
        ];
        let expected_txs = vec![
            make_tx(s1, 10 * BASE_FEE, GAS_LIMIT, 0, 10),
            make_tx(s3, 8 * BASE_FEE, GAS_LIMIT, 0, 10),
            make_tx(s3, 9 * BASE_FEE, GAS_LIMIT, 1, 10),
            make_tx(s1, 5 * BASE_FEE, GAS_LIMIT, 1, 10),
            make_tx(s2, 5 * BASE_FEE, GAS_LIMIT, 0, 10),
            make_tx(s2, 3 * BASE_FEE, GAS_LIMIT, 1, 10),
        ];

        let acc = txs
            .iter()
            .map(|tx| (EthAddress(tx.recover_signer().unwrap()), 0));
        let mut pool = EthTxPool::default();
        let eth_block_policy = make_test_block_policy();
        let state_backend = InMemoryStateInner::new(
            Balance::MAX,
            SeqNum(4),
            InMemoryBlockState::genesis(acc.collect()),
        );
        let txns: Vec<Bytes> = txs
            .iter()
            .map(|t| t.clone().envelope_encoded().into())
            .collect();
        assert!(!Pool::insert_tx(&mut pool, txns, &eth_block_policy, &state_backend,).is_empty());
        let encoded_txns = Pool::create_proposal(
            &mut pool,
            SeqNum(0),
            200,
            300 * GAS_LIMIT,
            &eth_block_policy,
            Vec::new(),
            &state_backend,
        )
        .unwrap();
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
            make_tx(s1, 10 * BASE_FEE, 100 * GAS_LIMIT, 0, 10),
            make_tx(s2, BASE_FEE, GAS_LIMIT, 0, 10),
            make_tx(s2, BASE_FEE, GAS_LIMIT, 1, 10),
            make_tx(s2, BASE_FEE, GAS_LIMIT, 2, 10),
            make_tx(s2, BASE_FEE, GAS_LIMIT, 3, 10),
            make_tx(s2, BASE_FEE, GAS_LIMIT, 4, 10),
            make_tx(s2, BASE_FEE, GAS_LIMIT, 5, 10),
            make_tx(s2, BASE_FEE, GAS_LIMIT, 6, 10),
            make_tx(s2, BASE_FEE, GAS_LIMIT, 7, 10),
            make_tx(s2, BASE_FEE, GAS_LIMIT, 8, 10),
            make_tx(s2, BASE_FEE, GAS_LIMIT, 9, 10),
        ];
        let expected_txs = vec![
            make_tx(s2, BASE_FEE, GAS_LIMIT, 0, 10),
            make_tx(s2, BASE_FEE, GAS_LIMIT, 1, 10),
            make_tx(s2, BASE_FEE, GAS_LIMIT, 2, 10),
            make_tx(s2, BASE_FEE, GAS_LIMIT, 3, 10),
            make_tx(s2, BASE_FEE, GAS_LIMIT, 4, 10),
            make_tx(s2, BASE_FEE, GAS_LIMIT, 5, 10),
            make_tx(s2, BASE_FEE, GAS_LIMIT, 6, 10),
            make_tx(s2, BASE_FEE, GAS_LIMIT, 7, 10),
            make_tx(s2, BASE_FEE, GAS_LIMIT, 8, 10),
            make_tx(s2, BASE_FEE, GAS_LIMIT, 9, 10),
        ];

        let acc = txs
            .iter()
            .map(|tx| (EthAddress(tx.recover_signer().unwrap()), 0));
        let mut pool = EthTxPool::default();
        let eth_block_policy = make_test_block_policy();
        let state_backend = InMemoryStateInner::new(
            Balance::MAX,
            SeqNum(4),
            InMemoryBlockState::genesis(acc.collect()),
        );
        let txns: Vec<Bytes> = txs
            .iter()
            .map(|t| t.clone().envelope_encoded().into())
            .collect();
        assert!(!Pool::insert_tx(&mut pool, txns, &eth_block_policy, &state_backend,).is_empty());
        let encoded_txns = Pool::create_proposal(
            &mut pool,
            SeqNum(0),
            200,
            10 * GAS_LIMIT,
            &eth_block_policy,
            Vec::new(),
            &state_backend,
        )
        .unwrap();
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
            make_tx(s1, 2 * BASE_FEE, 10 * GAS_LIMIT, 0, 10),
            make_tx(s2, BASE_FEE, GAS_LIMIT, 0, 10),
            make_tx(s2, BASE_FEE, GAS_LIMIT, 1, 10),
            make_tx(s2, BASE_FEE, GAS_LIMIT, 2, 10),
            make_tx(s2, BASE_FEE, GAS_LIMIT, 3, 10),
            make_tx(s2, BASE_FEE, GAS_LIMIT, 4, 10),
            make_tx(s2, BASE_FEE, GAS_LIMIT, 5, 10),
            make_tx(s2, BASE_FEE, GAS_LIMIT, 6, 10),
            make_tx(s2, BASE_FEE, GAS_LIMIT, 7, 10),
            make_tx(s2, BASE_FEE, GAS_LIMIT, 8, 10),
            make_tx(s2, BASE_FEE, GAS_LIMIT, 9, 10),
        ];
        let expected_txs = vec![
            make_tx(s2, BASE_FEE, GAS_LIMIT, 0, 10),
            make_tx(s2, BASE_FEE, GAS_LIMIT, 1, 10),
            make_tx(s2, BASE_FEE, GAS_LIMIT, 2, 10),
            make_tx(s2, BASE_FEE, GAS_LIMIT, 3, 10),
            make_tx(s2, BASE_FEE, GAS_LIMIT, 4, 10),
            make_tx(s2, BASE_FEE, GAS_LIMIT, 5, 10),
            make_tx(s2, BASE_FEE, GAS_LIMIT, 6, 10),
            make_tx(s2, BASE_FEE, GAS_LIMIT, 7, 10),
            make_tx(s2, BASE_FEE, GAS_LIMIT, 8, 10),
            make_tx(s2, BASE_FEE, GAS_LIMIT, 9, 10),
        ];

        let acc = txs
            .iter()
            .map(|tx| (EthAddress(tx.recover_signer().unwrap()), 0));
        let mut pool = EthTxPool::default();
        let eth_block_policy = make_test_block_policy();
        let state_backend = InMemoryStateInner::new(
            Balance::MAX,
            SeqNum(4),
            InMemoryBlockState::genesis(acc.collect()),
        );
        let txns: Vec<Bytes> = txs
            .iter()
            .map(|t| t.clone().envelope_encoded().into())
            .collect();
        assert!(!Pool::insert_tx(&mut pool, txns, &eth_block_policy, &state_backend,).is_empty());
        let encoded_txns = Pool::create_proposal(
            &mut pool,
            SeqNum(0),
            10,
            10 * GAS_LIMIT,
            &eth_block_policy,
            Default::default(),
            &state_backend,
        )
        .unwrap();
        let decoded_txns = Vec::<EthSignedTransaction>::decode(&mut encoded_txns.as_ref()).unwrap();
        assert_eq!(decoded_txns, expected_txs);
    }

    #[test]
    #[traced_test]
    fn zero_gas_limit() {
        let s1: B256 =
            hex!("0ed2e19e3aca1a321349f295837988e9c6f95d4a6fc54cfab6befd5ee82662ad").into(); // pubkey starts with AAA
        let txs = vec![make_tx(s1, BASE_FEE, 0, 0, 10)];
        let acc = txs
            .iter()
            .map(|tx| (EthAddress(tx.recover_signer().unwrap()), 0));
        let mut pool = EthTxPool::default();
        let eth_block_policy = make_test_block_policy();
        let state_backend = InMemoryStateInner::new(
            Balance::MAX,
            SeqNum(4),
            InMemoryBlockState::genesis(acc.collect()),
        );
        let txns: Vec<Bytes> = txs
            .iter()
            .map(|t| t.clone().envelope_encoded().into())
            .collect();
        assert!(Pool::insert_tx(&mut pool, txns, &eth_block_policy, &state_backend,).is_empty());
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
            make_tx(s1, BASE_FEE, GAS_LIMIT, 0, 10),
            make_tx(s1, BASE_FEE, GAS_LIMIT, 1, 10),
            make_tx(s2, BASE_FEE, GAS_LIMIT, 0, 10),
            make_tx(s2, BASE_FEE, GAS_LIMIT, 1, 10),
            make_tx(s3, BASE_FEE, GAS_LIMIT, 0, 10),
            make_tx(s3, BASE_FEE, GAS_LIMIT, 1, 10),
            make_tx(s4, BASE_FEE, GAS_LIMIT, 0, 10),
            make_tx(s4, BASE_FEE, GAS_LIMIT, 1, 10),
            make_tx(s5, BASE_FEE, GAS_LIMIT, 0, 10),
            make_tx(s5, BASE_FEE, GAS_LIMIT, 1, 10),
        ];
        let expected_txs = vec![
            make_tx(s5, BASE_FEE, GAS_LIMIT, 0, 10),
            make_tx(s5, BASE_FEE, GAS_LIMIT, 1, 10),
            make_tx(s4, BASE_FEE, GAS_LIMIT, 0, 10),
            make_tx(s4, BASE_FEE, GAS_LIMIT, 1, 10),
            make_tx(s3, BASE_FEE, GAS_LIMIT, 0, 10),
            make_tx(s3, BASE_FEE, GAS_LIMIT, 1, 10),
            make_tx(s2, BASE_FEE, GAS_LIMIT, 0, 10),
            make_tx(s2, BASE_FEE, GAS_LIMIT, 1, 10),
            make_tx(s1, BASE_FEE, GAS_LIMIT, 0, 10),
            make_tx(s1, BASE_FEE, GAS_LIMIT, 1, 10),
        ];

        let acc = txs
            .iter()
            .map(|tx| (EthAddress(tx.recover_signer().unwrap()), 0));
        let mut pool = EthTxPool::default();
        let eth_block_policy = make_test_block_policy();
        let state_backend = InMemoryStateInner::new(
            Balance::MAX,
            SeqNum(4),
            InMemoryBlockState::genesis(acc.collect()),
        );
        let txns: Vec<Bytes> = txs
            .iter()
            .map(|t| t.clone().envelope_encoded().into())
            .collect();
        assert!(!Pool::insert_tx(&mut pool, txns, &eth_block_policy, &state_backend,).is_empty());
        let encoded_txns = Pool::create_proposal(
            &mut pool,
            SeqNum(0),
            10,
            10 * GAS_LIMIT,
            &eth_block_policy,
            Vec::new(),
            &state_backend,
        )
        .unwrap();
        let decoded_txns = Vec::<EthSignedTransaction>::decode(&mut encoded_txns.as_ref()).unwrap();
        assert_eq!(expected_txs, decoded_txns);
    }

    #[test]
    fn test_zero_nonce_included_in_block() {
        // The first transaction from an account with 0 nonce should be including in the block

        let sender_1_key = B256::random();
        let txn_nonce_zero = make_tx(sender_1_key, BASE_FEE, GAS_LIMIT, 0, 10);

        let acc = vec![(EthAddress(txn_nonce_zero.recover_signer().unwrap()), 0)];
        let mut eth_tx_pool = EthTxPool::default();
        let eth_block_policy = make_test_block_policy();
        let state_backend = InMemoryStateInner::new(
            Balance::MAX,
            SeqNum(4),
            InMemoryBlockState::genesis(acc.into_iter().collect()),
        );

        let txns = vec![txn_nonce_zero.envelope_encoded().into()];

        assert!(
            !Pool::insert_tx(&mut eth_tx_pool, txns, &eth_block_policy, &state_backend,).is_empty()
        );

        let encoded_txns = Pool::create_proposal(
            &mut eth_tx_pool,
            SeqNum(0),
            10_000,
            50_000,
            &eth_block_policy,
            Vec::new(),
            &state_backend,
        )
        .unwrap();
        let decoded_txns = Vec::<EthSignedTransaction>::decode(&mut encoded_txns.as_ref()).unwrap();
        assert_eq!(decoded_txns, vec![txn_nonce_zero]);
    }

    #[test]
    fn test_invalid_nonce_not_in_block() {
        // A transaction with nonce 3 should not be included in the block if the la

        let sender_1_key = B256::random();
        // Txn with nonce = 0
        let txn_nonce_zero = make_tx(sender_1_key, BASE_FEE, GAS_LIMIT, 0, 10);
        // Txn with nonce = 1
        let txn_nonce_one = make_tx(sender_1_key, BASE_FEE, GAS_LIMIT, 1, 10);
        // Txn with nonce = 3
        let txn_nonce_three = make_tx(sender_1_key, BASE_FEE, GAS_LIMIT, 3, 10);
        let sender_1_address = EthAddress(txn_nonce_zero.recover_signer().unwrap());

        let mut eth_tx_pool = EthTxPool::default();
        let eth_block_policy = make_test_block_policy();
        let state_backend = InMemoryStateInner::new(
            Balance::MAX,
            SeqNum(4),
            InMemoryBlockState::genesis(std::iter::once((sender_1_address, 0)).collect()),
        );

        let txns = vec![
            txn_nonce_zero.envelope_encoded().into(),
            txn_nonce_one.envelope_encoded().into(),
            txn_nonce_three.envelope_encoded().into(),
        ];

        assert!(
            !Pool::insert_tx(&mut eth_tx_pool, txns, &eth_block_policy, &state_backend,).is_empty()
        );

        let encoded_txns = Pool::create_proposal(
            &mut eth_tx_pool,
            SeqNum(0),
            10_000,
            1_000_000_000,
            &eth_block_policy,
            Vec::new(),
            &state_backend,
        )
        .unwrap();
        let decoded_txns = Vec::<EthSignedTransaction>::decode(&mut encoded_txns.as_ref()).unwrap();

        // only transactions with nonce 0 and 1 should be included
        assert_eq!(decoded_txns, vec![txn_nonce_zero, txn_nonce_one]);
    }

    #[test]
    fn test_nonce_exists_in_committed_block() {
        // A transaction with nonce 0 should not be included in the block if the latest nonce of the account is 0

        let sender_1_key = B256::random();
        let txn_nonce_zero = make_tx(sender_1_key, BASE_FEE, GAS_LIMIT, 0, 10);
        let txn_nonce_one = make_tx(sender_1_key, BASE_FEE, GAS_LIMIT, 1, 10);
        let sender_1_address = EthAddress(txn_nonce_zero.recover_signer().unwrap());

        let mut eth_tx_pool = EthTxPool::default();
        let eth_block_policy = make_test_block_policy();
        let state_backend = InMemoryStateInner::new(
            Balance::MAX,
            SeqNum(4),
            InMemoryBlockState::genesis(std::iter::once((sender_1_address, 1)).collect()),
        );

        let txns = vec![
            txn_nonce_zero.envelope_encoded().into(),
            txn_nonce_one.envelope_encoded().into(),
        ];
        assert!(
            !Pool::insert_tx(&mut eth_tx_pool, txns, &eth_block_policy, &state_backend,).is_empty()
        );

        let encoded_txns = Pool::create_proposal(
            &mut eth_tx_pool,
            SeqNum(0),
            10_000,
            1_000_000_000,
            &eth_block_policy,
            Vec::new(),
            &state_backend,
        )
        .unwrap();
        let decoded_txns = Vec::<EthSignedTransaction>::decode(&mut encoded_txns.as_ref()).unwrap();

        assert_eq!(decoded_txns, vec![txn_nonce_one]);
    }

    #[test]
    fn test_nonce_exists_in_pending_block() {
        // A transaction with nonce 0 should not be included in the block if the latest nonce of the account is 0

        let sender_1_key = B256::random();
        // generate two transactions, both with nonce = 0
        let txn_1_nonce_zero = make_tx(sender_1_key, BASE_FEE, GAS_LIMIT, 0, 10);
        let txn_2_nonce_zero = make_tx(sender_1_key, BASE_FEE, GAS_LIMIT, 0, 1000);
        let txn_nonce_one = make_tx(sender_1_key, BASE_FEE, GAS_LIMIT, 1, 10);

        let acc = vec![(EthAddress(txn_1_nonce_zero.recover_signer().unwrap()), 0)];
        let mut eth_tx_pool = EthTxPool::default();
        let eth_block_policy = make_test_block_policy();
        let state_backend = InMemoryStateInner::new(
            Balance::MAX,
            SeqNum(4),
            InMemoryBlockState::genesis(acc.into_iter().collect()),
        );
        // create the extending block with txn 1
        let extending_block = generate_random_block_with_txns(vec![txn_1_nonce_zero]);

        let txns = vec![
            txn_2_nonce_zero.envelope_encoded().into(),
            txn_nonce_one.envelope_encoded().into(),
        ];
        assert!(
            !Pool::insert_tx(&mut eth_tx_pool, txns, &eth_block_policy, &state_backend,).is_empty()
        );

        let encoded_txns = Pool::create_proposal(
            &mut eth_tx_pool,
            SeqNum(0),
            10_000,
            1_000_000_000,
            &eth_block_policy,
            vec![&extending_block],
            &state_backend,
        )
        .unwrap();
        let decoded_txns = Vec::<EthSignedTransaction>::decode(&mut encoded_txns.as_ref()).unwrap();

        assert_eq!(decoded_txns, vec![txn_nonce_one]);
    }

    #[traced_test]
    #[test]
    fn test_combine_nonces_of_blocks() {
        // TxPool should combine the nonces of commited block and pending blocks to check nonce

        let sender_1_key = B256::random();

        // txn with nonce = 1
        let txn_nonce_one = make_tx(sender_1_key, BASE_FEE, GAS_LIMIT, 1, 10);
        // txn with nonce = 2
        let txn_nonce_two = make_tx(sender_1_key, BASE_FEE, GAS_LIMIT, 2, 10);
        // txn with nonce = 3
        let txn_nonce_three = make_tx(sender_1_key, BASE_FEE, GAS_LIMIT, 3, 10);

        let mut eth_tx_pool = EthTxPool::default();
        let sender_1_address = EthAddress(txn_nonce_one.recover_signer().unwrap());

        let eth_block_policy = make_test_block_policy();
        let state_backend = InMemoryStateInner::new(
            Balance::MAX,
            SeqNum(4),
            InMemoryBlockState::genesis(std::iter::once((sender_1_address, 1)).collect()),
        );

        let txns = vec![
            txn_nonce_one.envelope_encoded().into(),
            txn_nonce_two.envelope_encoded().into(),
            txn_nonce_three.envelope_encoded().into(),
        ];

        assert!(
            !Pool::insert_tx(&mut eth_tx_pool, txns, &eth_block_policy, &state_backend,).is_empty()
        );

        // create the extending block 1 with txn 1
        let extending_block_1 = generate_random_block_with_txns(vec![txn_nonce_one]);
        // create the extending block 2 with txn 2
        let extending_block_2 = generate_random_block_with_txns(vec![txn_nonce_two]);

        let encoded_txns = eth_tx_pool
            .create_proposal(
                SeqNum(0),
                10_000,
                1_000_000_000,
                &eth_block_policy,
                vec![&extending_block_1, &extending_block_2],
                &state_backend,
            )
            .unwrap();
        let decoded_txns = Vec::<EthSignedTransaction>::decode(&mut encoded_txns.as_ref()).unwrap();

        assert_eq!(decoded_txns, vec![txn_nonce_three]);
    }

    #[test]
    fn test_invalid_chain_id() {
        let sender_1_key = B256::random();
        let txn_nonce_one = make_tx(sender_1_key, BASE_FEE, GAS_LIMIT, 1, 10);

        let mut eth_tx_pool = EthTxPool::default();
        let sender_1_address = EthAddress(txn_nonce_one.recover_signer().unwrap());

        // eth block policy has a different chain id than the transaction
        let eth_block_policy = EthBlockPolicy::new(GENESIS_SEQ_NUM, u128::MAX, 0, 1);
        let state_backend = InMemoryStateInner::new(
            Balance::MAX,
            SeqNum(4),
            InMemoryBlockState::genesis(std::iter::once((sender_1_address, 0)).collect()),
        );

        let txns = vec![txn_nonce_one.envelope_encoded().into()];
        let result = Pool::insert_tx(&mut eth_tx_pool, txns, &eth_block_policy, &state_backend);

        // transaction should not be inserted into the pool
        assert!(result.is_empty());
    }
}
