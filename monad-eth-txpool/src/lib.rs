use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap, HashSet},
};

use alloy_rlp::Decodable;
use bytes::Bytes;
use monad_consensus_types::{
    block::{Block, BlockPolicy, BlockType},
    payload::FullTransactionList,
    quorum_certificate::QuorumCertificate,
    signature_collection::SignatureCollection,
    txpool::TxPool,
};
use monad_crypto::hasher::{Hashable, Hasher};
use monad_eth_tx::{
    EthAddress, EthFullTransactionList, EthSignedTransaction, EthTransaction, EthTxHash, Nonce,
};
use monad_types::{BlockId, Epoch, NodeId, Round, SeqNum};
use reth_primitives::{Transaction, TxEip1559, TxEip2930, TxEip4844, TxLegacy};
use sorted_vector_map::SortedVectorMap;

type VirtualTimestamp = u64;

/// Needed to have control over Ord implementation
#[derive(Debug, Default, Eq, PartialEq)]
struct WrappedTransaction {
    inner: EthTransaction,
    sender: EthAddress,
    insertion_time: VirtualTimestamp,
}

impl WrappedTransaction {
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
        &self.inner
    }
}

impl Ord for WrappedTransaction {
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

impl PartialOrd for WrappedTransaction {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Default)]
struct TransactionGroup {
    transactions: SortedVectorMap<Nonce, EthTransaction>,
}

impl TransactionGroup {
    fn add(&mut self, transaction: EthTransaction) {
        self.transactions.insert(transaction.nonce(), transaction);
    }
}

type MapType<K, V> = SortedVectorMap<K, V>;

#[derive(Default, Debug)]
pub struct EthTxPool {
    table: MapType<EthAddress, TransactionGroup>,
}

impl EthTxPool {
    fn clear(&mut self) {
        self.table.clear();
    }

    fn remove_nonce_gaps(&mut self) {
        self.table.iter_mut().for_each(|(_, transaction_group)| {
            let mut high_nonce = None;
            let mut maybe_nonce_to_remove = None;
            for (nonce, _) in &transaction_group.transactions {
                if high_nonce.is_some() && *nonce != high_nonce.unwrap() + 1 {
                    maybe_nonce_to_remove = Some(*nonce);
                    break;
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
        self.table
            .retain(|_, transaction_group| !transaction_group.transactions.is_empty())
    }

    fn remove_pending_transactions<SCT: SignatureCollection>(
        &mut self,
        pending_blocktree_txs: &HashSet<
            <<EthBlockPolicy as BlockPolicy<SCT>>::ValidatedBlock as BlockType<SCT>>::TxnHash,
        >,
    ) {
        self.table.iter_mut().for_each(|(_, transaction_group)| {
            transaction_group
                .transactions
                .retain(|_, transaction| !pending_blocktree_txs.contains(&transaction.hash()))
        });
        self.table
            .retain(|_, transaction_group| !transaction_group.transactions.is_empty())
    }
}

impl<SCT: SignatureCollection> TxPool<SCT, EthBlockPolicy> for EthTxPool {
    fn insert_tx(&mut self, tx: Bytes) {
        // TODO: unwrap can be removed when this is made generic over the actual
        // tx type rather than Bytes and decoding won't be necessary
        // TODO(rene): sender recovery is done inline here
        let eth_tx = EthTransaction::decode(&mut tx.as_ref()).unwrap();
        let sender = eth_tx.signer();

        // TODO(rene): should any transaction validation occur here before inserting into mempool
        self.table.entry(sender).or_default().add(eth_tx);
    }

    fn create_proposal(
        &mut self,
        tx_limit: usize,
        proposal_gas_limit: u64,
        pending_blocktree_txs: HashSet<
            <<EthBlockPolicy as BlockPolicy<SCT>>::ValidatedBlock as BlockType<SCT>>::TxnHash,
        >,
    ) -> (FullTransactionList, Option<FullTransactionList>) {
        self.remove_pending_transactions::<SCT>(&pending_blocktree_txs);
        self.remove_nonce_gaps();

        let mut txs = Vec::new();
        let mut total_gas = 0;

        let mut transaction_iters = self
            .table
            .iter()
            .map(|(address, group)| (*address, group.transactions.iter()))
            .collect::<HashMap<_, _>>();

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
                        inner: transaction.clone(),
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
                        inner: transaction.clone(),
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

        // TODO cascading behaviour for leftover txns once we have an idea of how we want
        // to forward
        self.clear();
        let leftovers = None;

        (FullTransactionList::new(full_tx_list), leftovers)
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use alloy_primitives::{hex, Address, B256};
    use alloy_rlp::Decodable;
    use monad_consensus_types::txpool::TxPool;
    use monad_crypto::NopSignature;
    use monad_eth_tx::EthSignedTransaction;
    use monad_multi_sig::MultiSig;
    use reth_primitives::{
        sign_message, Transaction, TransactionKind, TransactionSigned, TxLegacy,
    };
    use tracing_test::traced_test;

    use crate::{EthBlockPolicy, EthTxPool};

    fn make_tx(
        sender: B256,
        gas_price: u128,
        gas_limit: u64,
        nonce: u64,
        input_len: usize,
    ) -> TransactionSigned {
        let input = vec![0; input_len];
        let transaction = Transaction::Legacy(TxLegacy {
            chain_id: Some(1337),
            nonce,
            gas_price,
            gas_limit,
            to: TransactionKind::Call(Address::repeat_byte(0u8)),
            value: 0.into(),
            input: input.into(),
        });

        let hash = transaction.signature_hash();

        let sender_secret_key = sender;
        let signature =
            sign_message(sender_secret_key, hash).expect("signature should always succeed");

        TransactionSigned::from_transaction_and_signature(transaction, signature)
    }

    type Pool = dyn TxPool<MultiSig<NopSignature>, EthBlockPolicy>;

    #[test]
    #[traced_test]
    fn test_nonce_gap_transaction_not_included() {
        let txs = vec![
            make_tx(B256::repeat_byte(0xAu8), 1, 1, 0, 10),
            make_tx(B256::repeat_byte(0xAu8), 1, 1, 1, 10),
            make_tx(B256::repeat_byte(0xAu8), 1, 1, 2, 10),
            make_tx(B256::repeat_byte(0xAu8), 1, 1, 4, 10),
            make_tx(B256::repeat_byte(0xAu8), 1, 1, 5, 10),
            make_tx(B256::repeat_byte(0xAu8), 1, 1, 6, 10),
        ];
        let expected_txs = vec![
            make_tx(B256::repeat_byte(0xAu8), 1, 1, 0, 10),
            make_tx(B256::repeat_byte(0xAu8), 1, 1, 1, 10),
            make_tx(B256::repeat_byte(0xAu8), 1, 1, 2, 10),
        ];
        let mut pool = EthTxPool::default();
        for tx in txs {
            Pool::insert_tx(&mut pool, tx.envelope_encoded().into());
        }
        assert_eq!(pool.table.len(), 1);
        assert_eq!(
            pool.table.first_key_value().unwrap().1.transactions.len(),
            6
        );

        let (encoded_txns, _) = Pool::create_proposal(&mut pool, 6, 1_000_000, HashSet::default());

        let decoded_txns = Vec::<EthSignedTransaction>::decode(&mut encoded_txns.as_ref()).unwrap();
        assert_eq!(decoded_txns, expected_txs);
        assert!(pool.table.is_empty());
    }

    #[test]
    #[traced_test]
    fn test_create_proposal_with_insufficient_tx_limit() {
        let tx = make_tx(B256::repeat_byte(0xAu8), 1, 1, 0, 10);
        let mut pool = EthTxPool::default();
        Pool::insert_tx(&mut pool, tx.envelope_encoded().into());
        assert_eq!(pool.table.len(), 1);
        assert_eq!(
            pool.table.first_key_value().unwrap().1.transactions.len(),
            1
        );

        let (encoded_txns, _) = Pool::create_proposal(&mut pool, 0, 1_000_000, HashSet::default());

        let decoded_txns = Vec::<EthSignedTransaction>::decode(&mut encoded_txns.as_ref()).unwrap();
        assert_eq!(decoded_txns, vec![]);
        assert!(pool.table.is_empty());
    }

    #[test]
    #[traced_test]
    fn test_create_proposal_with_insufficient_gas_limit() {
        let tx = make_tx(B256::repeat_byte(0xAu8), 1, 6400, 0, 10);
        let mut pool = EthTxPool::default();
        Pool::insert_tx(&mut pool, tx.envelope_encoded().into());
        assert_eq!(pool.table.len(), 1);
        assert_eq!(
            pool.table.first_key_value().unwrap().1.transactions.len(),
            1
        );

        let (encoded_txns, _) = Pool::create_proposal(&mut pool, 1, 6399, HashSet::default());

        let decoded_txns = Vec::<EthSignedTransaction>::decode(&mut encoded_txns.as_ref()).unwrap();
        assert_eq!(decoded_txns, vec![]);
        assert!(pool.table.is_empty());
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
        Pool::insert_tx(&mut pool, t1.envelope_encoded().into());
        Pool::insert_tx(&mut pool, t2.envelope_encoded().into());
        Pool::insert_tx(&mut pool, t3.envelope_encoded().into());
        assert_eq!(pool.table.len(), 1);
        assert_eq!(
            pool.table.first_key_value().unwrap().1.transactions.len(),
            3
        );

        let (encoded_txns, _) = Pool::create_proposal(&mut pool, 2, 6400 * 2, HashSet::default());
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
        for tx in txs.iter() {
            Pool::insert_tx(&mut pool, tx.clone().envelope_encoded().into());
        }
        let (encoded_txns, _) = Pool::create_proposal(&mut pool, 2, 3, HashSet::default());
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
        for tx in txs.iter() {
            Pool::insert_tx(&mut pool, tx.clone().envelope_encoded().into());
        }
        let (encoded_txns, _) = Pool::create_proposal(&mut pool, 2, 3, HashSet::default());
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
        for tx in txs.iter() {
            Pool::insert_tx(&mut pool, tx.clone().envelope_encoded().into());
        }
        let (encoded_txns, _) = Pool::create_proposal(&mut pool, 200, 300, HashSet::default());
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
        for tx in txs.iter() {
            Pool::insert_tx(&mut pool, tx.clone().envelope_encoded().into());
        }
        let (encoded_txns, _) = Pool::create_proposal(&mut pool, 200, 300, HashSet::default());
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
        for tx in txs.iter() {
            Pool::insert_tx(&mut pool, tx.clone().envelope_encoded().into());
        }
        let (encoded_txns, _) = Pool::create_proposal(&mut pool, 200, 10, HashSet::default());
        let decoded_txns = Vec::<EthSignedTransaction>::decode(&mut encoded_txns.as_ref()).unwrap();
        assert_eq!(decoded_txns, expected_txs);
    }
}

/// A consensus block that has gone through the EthereumValidator and makes the decoded and
/// verified transactions availabe to access
#[derive(Debug, Clone)]
pub struct EthValidatedBlock<SCT: SignatureCollection> {
    pub block: Block<SCT>,
    pub validated_txns: Vec<EthSignedTransaction>,
}

impl<SCT: SignatureCollection> EthValidatedBlock<SCT> {
    pub fn get_validated_txn_hashes(&self) -> Vec<EthTxHash> {
        self.validated_txns.iter().map(|t| t.hash()).collect()
    }

    pub fn get_nonces(&self) -> Vec<u64> {
        self.validated_txns.iter().map(|t| t.nonce()).collect()
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

    fn get_parent_id(&self) -> BlockId {
        self.block.qc.get_block_id()
    }

    fn get_parent_round(&self) -> Round {
        self.block.qc.get_round()
    }

    fn get_seq_num(&self) -> SeqNum {
        self.block.payload.seq_num
    }

    fn get_txn_hashes(&self) -> Vec<Self::TxnHash> {
        self.get_validated_txn_hashes()
    }

    fn get_qc(&self) -> &QuorumCertificate<SCT> {
        &self.block.qc
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

    fn is_txn_list_empty(&self) -> bool {
        self.validated_txns.is_empty()
    }
}

/// A block policy for ethereum payloads
pub struct EthBlockPolicy;
impl<SCT: SignatureCollection> BlockPolicy<SCT> for EthBlockPolicy {
    type ValidatedBlock = EthValidatedBlock<SCT>;
}
