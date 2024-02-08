pub mod tx_states;

use std::collections::BTreeMap;

use monad_consensus_types::payload::FullTransactionList;
use monad_eth_tx::{EthFullTransactionList, EthTransaction, EthTxHash};
use monad_eth_types::EthAddress;
use tx_states::{EthTxState, EthTxStates};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EthAccountState {
    pub max_reserve_balance: usize,
    pub reserve_balance: usize,
    pub transaction_states: EthTxStates,
}

impl EthAccountState {
    pub fn update_max_reserve_balance(&mut self, new_max: usize) {
        // TODO: Handle case where the new max reserve balance is less than the current
        // reserve balance in the account
        self.max_reserve_balance = new_max;
    }

    pub fn update_reserve_balance(&mut self, delta: usize) {
        assert!(self.reserve_balance + delta <= self.max_reserve_balance);

        self.reserve_balance += delta;
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EthAccountStates(pub BTreeMap<EthAddress, EthAccountState>);

impl EthAccountStates {
    /// Transactions from IPC and leftover transactions from previous leader should be validated
    /// using this function
    pub fn validate_single_txn(&mut self, txn: &EthTransaction) -> bool {
        let acc_address = EthAddress(txn.signer());

        let Some(acc_state) = self.0.get_mut(&acc_address) else {
            // Account has no state yet => No reserve balance
            return false;
        };

        let txn_hash: EthTxHash = txn.hash;

        if acc_state.transaction_states.states.contains_key(&txn_hash) {
            // Transaction already in the pool
            return true;
        }

        // TODO: Use consensus base fee
        if acc_state.reserve_balance >= 1 {
            // Decrement the reserve balance
            acc_state.reserve_balance -= 1;

            // Store transaction as not proposed
            acc_state
                .transaction_states
                .states
                .insert(txn_hash, EthTxState::NotProposed);

            acc_state.transaction_states.num_not_proposed += 1;

            return true;
        }

        // Not enough reserve balance
        false
    }

    /// Transactions from block proposals should be validated using this function
    ///
    /// Block validations should be done atomically. Existing transactions from the pool
    /// should not be evicted until all the transactions from the new block are validated
    pub fn validate_block_txns(&mut self, txns: &FullTransactionList) -> bool {
        let Ok(eth_txns) = EthFullTransactionList::rlp_decode(txns.bytes().clone()) else {
            return false;
        };

        // Group transactions of the block by sender address
        let mut txns_from_address = BTreeMap::<EthAddress, Vec<EthTransaction>>::new();
        for txn in eth_txns.0 {
            txns_from_address
                .entry(EthAddress(txn.signer()))
                .or_default()
                .push(txn);
        }

        // Validate all the transactions in the block before (possibly) evicting transactions
        for (address, txns) in txns_from_address.iter() {
            let Some(acc_state) = self.0.get(address) else {
                // Account has no reserve balance
                return false;
            };

            let mut num_new_txns = 0;
            for txn in txns {
                if !acc_state.transaction_states.states.contains_key(&txn.hash) {
                    num_new_txns += 1;
                }
            }

            // TODO: Use consensus base fee
            if acc_state.reserve_balance + acc_state.transaction_states.num_not_proposed()
                < num_new_txns
            {
                return false;
            }
        }
        // At this point, the block is valid.

        let mut evicted_txns = Vec::new();

        // Add all the transactions from the block as Proposed into the account states
        for (address, txns) in txns_from_address {
            let acc_state = self
                .0
                .get_mut(&address)
                .expect("transactions were verified");

            for txn in txns {
                // If there is not enough reserve balance, evict a transaction
                if acc_state.reserve_balance == 0 {
                    let evicted_txn = acc_state
                        .transaction_states
                        .evict_not_proposed()
                        .expect("should evict one transaction");
                    evicted_txns.push(evicted_txn);
                } else {
                    // TODO: Use consensus base fee
                    acc_state.reserve_balance -= 1;
                }

                acc_state.transaction_states.add_proposed(txn.hash);
            }
        }

        for _evicted_txn in evicted_txns {
            // TODO: Remove evicted transactions from the pool
        }

        true
    }

    /// Reverts the proposed transactions in a rejected block
    pub fn remove_block_txns(&mut self, txns: &FullTransactionList) {
        let eth_txns =
            EthFullTransactionList::rlp_decode(txns.bytes().clone()).expect("block was verified");

        for txn in eth_txns.0 {
            let acc_adress = EthAddress(txn.signer());
            let acc_state = self
                .0
                .get_mut(&acc_adress)
                .expect("transaction was verified");

            acc_state.transaction_states.remove_proposed(txn.hash);
        }
    }

    /// Removes the transactions that were committed with a block
    /// TODO: Committed transactions should also be removed from the pool
    pub fn update_committed_txns(&mut self, txns: &FullTransactionList) {
        let eth_txns =
            EthFullTransactionList::rlp_decode(txns.bytes().clone()).expect("block was verified");

        for txn in eth_txns.0 {
            let acc_adress = EthAddress(txn.signer());
            let acc_state = self
                .0
                .get_mut(&acc_adress)
                .expect("transaction was verified");

            acc_state.transaction_states.update_committed(txn.hash);
        }
    }

    // Max reserve balance updates from exection
    pub fn update_max_reserve_balances(&mut self, max_reserve_balances: Vec<(EthAddress, usize)>) {
        for (acc_address, max_reserve_balance) in max_reserve_balances {
            let acc_state = self.0.entry(acc_address).or_default();

            acc_state.update_max_reserve_balance(max_reserve_balance);
        }
    }

    // Reserve balance updates from execution
    pub fn update_reserve_balances(&mut self, deltas: Vec<(EthAddress, usize)>) {
        for (acc_address, delta) in deltas {
            let acc_state = self
                .0
                .get_mut(&acc_address)
                .expect("account should exist with a max reserve balance");

            acc_state.update_reserve_balance(delta);
        }
    }
}

#[cfg(test)]
mod test {
    use alloy_rlp::Decodable;
    use monad_consensus_types::payload::FullTransactionList;
    use monad_eth_tx::{EthFullTransactionList, EthTransaction};
    use monad_eth_types::EthAddress;
    use reth_primitives::{hex_literal::hex, TransactionSigned};

    use crate::{tx_states::EthTxState, EthAccountStates};

    pub fn get_transaction() -> EthTransaction {
        // random transaction: https://etherscan.io/getRawTx?tx=0x49f2c03bcc1d12a1eaca1ffecd8a6fe2850b00e3ba12cbc385e4e6565176e332
        let tx_hex = "02f87101828d6c808504fcb0d341830186a094cf46e7c0b3f80bdad860d77cf15632bc2d44da07873c23b66510780080c001a031fac5647975c16f7ed5ec746be5b7b89e603a1515353c93da56112cf54655ada0265f935f484271460ee24b29c2b59aa4960e26b58f18efcbac7af7c135f076fd";
        let bytes = hex::decode(tx_hex).expect("decodable hex");
        let eth_tx_signed = TransactionSigned::decode(&mut bytes.as_slice()).unwrap();
        let eth_tx: EthTransaction = eth_tx_signed.into_ecrecovered().unwrap();

        eth_tx
    }

    #[test]
    fn test_decrement_reserve_balance_single_txn() {
        let mut acc_states = EthAccountStates::default();

        let eth_tx = get_transaction();
        let eth_address_1 = EthAddress(eth_tx.signer());

        // update max reserve balance and reserve balance
        acc_states.update_max_reserve_balances(vec![(eth_address_1, 10)]);
        acc_states.update_reserve_balances(vec![(eth_address_1, 1)]);

        let acc_state = acc_states.0.get(&eth_address_1).unwrap();
        assert!(acc_state.max_reserve_balance == 10);
        assert!(acc_state.reserve_balance == 1);

        // try to validate the transaction
        assert!(acc_states.validate_single_txn(&eth_tx));

        // assert reserve balance was decremented
        let acc_state = acc_states.0.get(&eth_address_1).unwrap();
        assert!(acc_state.max_reserve_balance == 10);
        assert!(acc_state.reserve_balance == 0);

        // assert transaction is in not proposed state
        assert!(acc_state.transaction_states.num_not_proposed == 1);
        assert!(
            *acc_state
                .transaction_states
                .states
                .get(&eth_tx.hash)
                .expect("transaction should be added to state")
                == EthTxState::NotProposed
        );
    }

    #[test]
    fn test_zero_reserve_balance_single_txn() {
        let mut acc_states = EthAccountStates::default();

        let eth_tx = get_transaction();
        let eth_address_1 = EthAddress(eth_tx.signer());

        // update max reserve balance and let reserve balance = 0
        acc_states.update_max_reserve_balances(vec![(eth_address_1, 10)]);

        let acc_state = acc_states.0.get(&eth_address_1).unwrap();
        assert!(acc_state.max_reserve_balance == 10);
        assert!(acc_state.reserve_balance == 0);

        // try to validate the transaction
        assert!(!acc_states.validate_single_txn(&eth_tx));

        // assert reserve balance was not decremented
        let acc_state = acc_states.0.get(&eth_address_1).unwrap();
        assert!(acc_state.max_reserve_balance == 10);
        assert!(acc_state.reserve_balance == 0);

        // assert transaction is not in the transaction states
        assert!(acc_state.transaction_states.num_not_proposed == 0);
        assert!(acc_state
            .transaction_states
            .states
            .get(&eth_tx.hash)
            .is_none());
    }

    #[test]
    fn test_validate_block_txns() {
        let mut acc_states = EthAccountStates::default();

        // random transaction: https://etherscan.io/getRawTx?tx=0x49f2c03bcc1d12a1eaca1ffecd8a6fe2850b00e3ba12cbc385e4e6565176e332
        // sender: 0x5124fcC2B3F99F571AD67D075643C743F38f1C34
        let tx_hex_1 = hex!("02f87101828d6c808504fcb0d341830186a094cf46e7c0b3f80bdad860d77cf15632bc2d44da07873c23b66510780080c001a031fac5647975c16f7ed5ec746be5b7b89e603a1515353c93da56112cf54655ada0265f935f484271460ee24b29c2b59aa4960e26b58f18efcbac7af7c135f076fd");
        let eth_tx_1_signed = TransactionSigned::decode(&mut &tx_hex_1[..]).unwrap();
        let eth_tx_1 = eth_tx_1_signed.into_ecrecovered().unwrap();

        // random transaction: https://etherscan.io/getRawTx?tx=0xfc7995e1c5727e7fccd8798f74563f8192e9dcfb0c0a056dddc272ac4401df69
        // sender: 0xab97925eB84fe0260779F58B7cb08d77dcB1ee2B
        let tx_hex_2 = hex!("02f87501830f2ee88477359400852e90edd000825208941570f6fa6ca8f4ee5cc2ed7b6dd57aa499205fb287aa87bee538000080c080a0c7568bce3221fb8b74e845de43d4c6e5a732a2f7f6f68a07e3970b2711b86824a00c2c269b8632a8b3d7433b7b2ef36d1c16deb2af96d768a16da7e359eddde9e7");
        let eth_tx_2_signed = TransactionSigned::decode(&mut &tx_hex_2[..]).unwrap();
        let eth_tx_2 = eth_tx_2_signed.into_ecrecovered().unwrap();

        let eth_list = EthFullTransactionList(vec![eth_tx_1.clone(), eth_tx_2.clone()]);
        let eth_address_1 = EthAddress(eth_list.0[0].signer());
        let eth_address_2 = EthAddress(eth_list.0[1].signer());

        // update max reserve balance and reserve balance
        acc_states.update_max_reserve_balances(vec![(eth_address_1, 10), (eth_address_2, 10)]);
        acc_states.update_reserve_balances(vec![(eth_address_1, 1), (eth_address_2, 2)]);

        // validate the block transactions
        let full_list = FullTransactionList::new(eth_list.rlp_encode());
        assert!(acc_states.validate_block_txns(&full_list));

        // assert reserve balances were decremented
        let acc_1_state = acc_states.0.get(&eth_address_1).unwrap();
        assert!(acc_1_state.max_reserve_balance == 10);
        assert!(acc_1_state.reserve_balance == 0);

        let acc_2_state = acc_states.0.get(&eth_address_2).unwrap();
        assert!(acc_2_state.max_reserve_balance == 10);
        assert!(acc_2_state.reserve_balance == 1);

        // assert transactions are stored as proposed
        assert!(acc_1_state.transaction_states.num_not_proposed == 0);
        assert!(
            *acc_1_state
                .transaction_states
                .states
                .get(&eth_tx_1.hash)
                .unwrap()
                == EthTxState::Proposed(1)
        );

        assert!(acc_2_state.transaction_states.num_not_proposed == 0);
        assert!(
            *acc_2_state
                .transaction_states
                .states
                .get(&eth_tx_2.hash)
                .unwrap()
                == EthTxState::Proposed(1)
        );
    }

    #[test]
    fn test_evict_transaction() {
        let mut acc_states = EthAccountStates::default();

        // random transaction: https://etherscan.io/getRawTx?tx=0x08abdb78f03578457dbee50579ec87560004a7fc2d59ccc25bebd29906f54b76
        // sender: 0x5124fcC2B3F99F571AD67D075643C743F38f1C34
        let tx_hex_1 = hex!("02f87201828dbb808507c29a7b4a830186a09422eec85ba6a5cd97ead4728ea1c69e1d9c6fa778881933fb6186170c5180c001a0606ae3e421ffbaa95e29dbdd60938adb0dff3d977eee221b1d58d31125e5a49ea00c2c644d2edb5d840d30ba827aa2e7e1df525f4029f0cf8c17b58f6051ba6af3");
        let eth_tx_1_signed = TransactionSigned::decode(&mut &tx_hex_1[..]).unwrap();
        let eth_tx_1 = eth_tx_1_signed.into_ecrecovered().unwrap();

        // random transaction: https://etherscan.io/getRawTx?tx=0x49f2c03bcc1d12a1eaca1ffecd8a6fe2850b00e3ba12cbc385e4e6565176e332
        // sender: 0x5124fcC2B3F99F571AD67D075643C743F38f1C34
        // same sender as previous transaction
        let tx_hex_2 = hex!("02f87101828d6c808504fcb0d341830186a094cf46e7c0b3f80bdad860d77cf15632bc2d44da07873c23b66510780080c001a031fac5647975c16f7ed5ec746be5b7b89e603a1515353c93da56112cf54655ada0265f935f484271460ee24b29c2b59aa4960e26b58f18efcbac7af7c135f076fd");
        let eth_tx_2_signed = TransactionSigned::decode(&mut &tx_hex_2[..]).unwrap();
        let eth_tx_2 = eth_tx_2_signed.into_ecrecovered().unwrap();

        // random transaction: https://etherscan.io/getRawTx?tx=0xfc7995e1c5727e7fccd8798f74563f8192e9dcfb0c0a056dddc272ac4401df69
        // sender: 0xab97925eB84fe0260779F58B7cb08d77dcB1ee2B
        let tx_hex_3 = hex!("02f87501830f2ee88477359400852e90edd000825208941570f6fa6ca8f4ee5cc2ed7b6dd57aa499205fb287aa87bee538000080c080a0c7568bce3221fb8b74e845de43d4c6e5a732a2f7f6f68a07e3970b2711b86824a00c2c269b8632a8b3d7433b7b2ef36d1c16deb2af96d768a16da7e359eddde9e7");
        let eth_tx_3_signed = TransactionSigned::decode(&mut &tx_hex_3[..]).unwrap();
        let eth_tx_3 = eth_tx_3_signed.into_ecrecovered().unwrap();

        let eth_list = EthFullTransactionList(vec![eth_tx_2.clone(), eth_tx_3.clone()]);
        let eth_address_1 = EthAddress(eth_list.0[0].signer());
        let eth_address_2 = EthAddress(eth_list.0[1].signer());

        // update max reserve balance and reserve balance
        acc_states.update_max_reserve_balances(vec![(eth_address_1, 10), (eth_address_2, 10)]);
        acc_states.update_reserve_balances(vec![(eth_address_1, 1), (eth_address_2, 1)]);

        // validate single transaction account 1
        assert!(acc_states.validate_single_txn(&eth_tx_1));

        // first transaction should be stored as not proposed
        let acc_1_state = acc_states.0.get(&eth_address_1).unwrap();
        assert!(acc_1_state.reserve_balance == 0);
        assert!(
            *acc_1_state
                .transaction_states
                .states
                .get(&eth_tx_1.hash)
                .unwrap()
                == EthTxState::NotProposed
        );

        // block validation should evict the first transaction
        let full_list = FullTransactionList::new(eth_list.rlp_encode());
        assert!(acc_states.validate_block_txns(&full_list));

        let acc_1_state = acc_states.0.get(&eth_address_1).unwrap();
        let acc_2_state = acc_states.0.get(&eth_address_2).unwrap();
        assert!(acc_1_state.reserve_balance == 0);
        assert!(acc_2_state.reserve_balance == 0);
        assert!(acc_1_state
            .transaction_states
            .states
            .get(&eth_tx_1.hash)
            .is_none());
        assert!(
            *acc_1_state
                .transaction_states
                .states
                .get(&eth_tx_2.hash)
                .unwrap()
                == EthTxState::Proposed(1)
        );
        assert!(
            *acc_2_state
                .transaction_states
                .states
                .get(&eth_tx_3.hash)
                .unwrap()
                == EthTxState::Proposed(1)
        );
    }

    #[test]
    fn test_zero_reserve_balance_block_txns() {
        let mut acc_states = EthAccountStates::default();

        // random transaction: https://etherscan.io/getRawTx?tx=0x49f2c03bcc1d12a1eaca1ffecd8a6fe2850b00e3ba12cbc385e4e6565176e332
        // sender: 0x5124fcC2B3F99F571AD67D075643C743F38f1C34
        let tx_hex_1 = hex!("02f87101828d6c808504fcb0d341830186a094cf46e7c0b3f80bdad860d77cf15632bc2d44da07873c23b66510780080c001a031fac5647975c16f7ed5ec746be5b7b89e603a1515353c93da56112cf54655ada0265f935f484271460ee24b29c2b59aa4960e26b58f18efcbac7af7c135f076fd");
        let eth_tx_1_signed = TransactionSigned::decode(&mut &tx_hex_1[..]).unwrap();
        let eth_tx_1 = eth_tx_1_signed.into_ecrecovered().unwrap();

        // random transaction: https://etherscan.io/getRawTx?tx=0xfc7995e1c5727e7fccd8798f74563f8192e9dcfb0c0a056dddc272ac4401df69
        // sender: 0xab97925eB84fe0260779F58B7cb08d77dcB1ee2B
        let tx_hex_2 = hex!("02f87501830f2ee88477359400852e90edd000825208941570f6fa6ca8f4ee5cc2ed7b6dd57aa499205fb287aa87bee538000080c080a0c7568bce3221fb8b74e845de43d4c6e5a732a2f7f6f68a07e3970b2711b86824a00c2c269b8632a8b3d7433b7b2ef36d1c16deb2af96d768a16da7e359eddde9e7");
        let eth_tx_2_signed = TransactionSigned::decode(&mut &tx_hex_2[..]).unwrap();
        let eth_tx_2 = eth_tx_2_signed.into_ecrecovered().unwrap();

        let eth_list = EthFullTransactionList(vec![eth_tx_1.clone(), eth_tx_2.clone()]);
        let eth_address_1 = EthAddress(eth_list.0[0].signer());
        let eth_address_2 = EthAddress(eth_list.0[1].signer());

        // update max reserve balance and reserve balance
        acc_states.update_max_reserve_balances(vec![(eth_address_1, 10), (eth_address_2, 10)]);
        acc_states.update_reserve_balances(vec![(eth_address_1, 1), (eth_address_2, 0)]);

        // block validation should fail
        let full_list = FullTransactionList::new(eth_list.rlp_encode());
        assert!(!acc_states.validate_block_txns(&full_list));

        // assert reserve balances were not decremented
        let acc_1_state = acc_states.0.get(&eth_address_1).unwrap();
        assert!(acc_1_state.max_reserve_balance == 10);
        assert!(acc_1_state.reserve_balance == 1);

        let acc_2_state = acc_states.0.get(&eth_address_2).unwrap();
        assert!(acc_2_state.max_reserve_balance == 10);
        assert!(acc_2_state.reserve_balance == 0);

        // assert transactions are not stored
        assert!(acc_1_state.transaction_states.num_not_proposed == 0);
        assert!(acc_1_state
            .transaction_states
            .states
            .get(&eth_tx_1.hash)
            .is_none());

        assert!(acc_2_state.transaction_states.num_not_proposed == 0);
        assert!(acc_2_state
            .transaction_states
            .states
            .get(&eth_tx_2.hash)
            .is_none());
    }

    #[test]
    fn test_proposed_not_evicted() {
        let mut acc_states = EthAccountStates::default();

        // random transaction: https://etherscan.io/getRawTx?tx=0x08abdb78f03578457dbee50579ec87560004a7fc2d59ccc25bebd29906f54b76
        // sender: 0x5124fcC2B3F99F571AD67D075643C743F38f1C34
        let tx_hex_1 = hex!("02f87201828dbb808507c29a7b4a830186a09422eec85ba6a5cd97ead4728ea1c69e1d9c6fa778881933fb6186170c5180c001a0606ae3e421ffbaa95e29dbdd60938adb0dff3d977eee221b1d58d31125e5a49ea00c2c644d2edb5d840d30ba827aa2e7e1df525f4029f0cf8c17b58f6051ba6af3");
        let eth_tx_1_signed = TransactionSigned::decode(&mut &tx_hex_1[..]).unwrap();
        let eth_tx_1 = eth_tx_1_signed.into_ecrecovered().unwrap();

        // random transaction: https://etherscan.io/getRawTx?tx=0x49f2c03bcc1d12a1eaca1ffecd8a6fe2850b00e3ba12cbc385e4e6565176e332
        // sender: 0x5124fcC2B3F99F571AD67D075643C743F38f1C34
        // same sender as previous transaction
        let tx_hex_2 = hex!("02f87101828d6c808504fcb0d341830186a094cf46e7c0b3f80bdad860d77cf15632bc2d44da07873c23b66510780080c001a031fac5647975c16f7ed5ec746be5b7b89e603a1515353c93da56112cf54655ada0265f935f484271460ee24b29c2b59aa4960e26b58f18efcbac7af7c135f076fd");
        let eth_tx_2_signed = TransactionSigned::decode(&mut &tx_hex_2[..]).unwrap();
        let eth_tx_2 = eth_tx_2_signed.into_ecrecovered().unwrap();

        let eth_list_1: EthFullTransactionList = EthFullTransactionList(vec![eth_tx_1.clone()]);
        let eth_list_2: EthFullTransactionList = EthFullTransactionList(vec![eth_tx_2.clone()]);
        let eth_address_1 = EthAddress(eth_list_1.0[0].signer());

        // update max reserve balance and reserve balance
        acc_states.update_max_reserve_balances(vec![(eth_address_1, 10)]);
        acc_states.update_reserve_balances(vec![(eth_address_1, 1)]);

        // validate block 1
        let full_list_1 = FullTransactionList::new(eth_list_1.rlp_encode());
        assert!(acc_states.validate_block_txns(&full_list_1));

        // first transaction should be stored as proposed
        let acc_1_state = acc_states.0.get(&eth_address_1).unwrap();
        assert!(acc_1_state.reserve_balance == 0);
        assert!(
            *acc_1_state
                .transaction_states
                .states
                .get(&eth_tx_1.hash)
                .unwrap()
                == EthTxState::Proposed(1)
        );

        // block 2 validation should be rejected
        let full_list_2 = FullTransactionList::new(eth_list_2.rlp_encode());
        assert!(!acc_states.validate_block_txns(&full_list_2));

        let acc_1_state = acc_states.0.get(&eth_address_1).unwrap();
        assert!(acc_1_state.reserve_balance == 0);
        // first transaction should still be stored as proposed
        assert!(
            *acc_1_state
                .transaction_states
                .states
                .get(&eth_tx_1.hash)
                .unwrap()
                == EthTxState::Proposed(1)
        );
        // second transaction should not be in stored
        assert!(acc_1_state
            .transaction_states
            .states
            .get(&eth_tx_2.hash)
            .is_none());
    }

    #[test]
    fn test_remove_rejected_block_txns() {
        let mut acc_states = EthAccountStates::default();

        // random transaction: https://etherscan.io/getRawTx?tx=0x08abdb78f03578457dbee50579ec87560004a7fc2d59ccc25bebd29906f54b76
        // sender: 0x5124fcC2B3F99F571AD67D075643C743F38f1C34
        let tx_hex_1 = hex!("02f87201828dbb808507c29a7b4a830186a09422eec85ba6a5cd97ead4728ea1c69e1d9c6fa778881933fb6186170c5180c001a0606ae3e421ffbaa95e29dbdd60938adb0dff3d977eee221b1d58d31125e5a49ea00c2c644d2edb5d840d30ba827aa2e7e1df525f4029f0cf8c17b58f6051ba6af3");
        let eth_tx_1_signed = TransactionSigned::decode(&mut &tx_hex_1[..]).unwrap();
        let eth_tx_1 = eth_tx_1_signed.into_ecrecovered().unwrap();

        let eth_list_1: EthFullTransactionList = EthFullTransactionList(vec![eth_tx_1.clone()]);
        let eth_address_1 = EthAddress(eth_list_1.0[0].signer());

        // update max reserve balance and reserve balance
        acc_states.update_max_reserve_balances(vec![(eth_address_1, 10)]);
        acc_states.update_reserve_balances(vec![(eth_address_1, 1)]);

        // validate block 1
        let full_list_1 = FullTransactionList::new(eth_list_1.rlp_encode());
        assert!(acc_states.validate_block_txns(&full_list_1));

        // transaction should be stored as proposed
        let acc_1_state = acc_states.0.get(&eth_address_1).unwrap();
        assert!(acc_1_state.reserve_balance == 0);
        assert!(
            *acc_1_state
                .transaction_states
                .states
                .get(&eth_tx_1.hash)
                .unwrap()
                == EthTxState::Proposed(1)
        );

        // block gets rejected. i.e. pruned from blocktree or failed to get 2f+1 votes
        acc_states.remove_block_txns(&full_list_1);

        // transaction should be stored as not proposed
        let acc_1_state = acc_states.0.get(&eth_address_1).unwrap();
        assert!(acc_1_state.reserve_balance == 0);
        assert!(
            *acc_1_state
                .transaction_states
                .states
                .get(&eth_tx_1.hash)
                .unwrap()
                == EthTxState::NotProposed
        );
    }

    #[test]
    fn test_update_committed_txns() {
        let mut acc_states = EthAccountStates::default();

        // random transaction: https://etherscan.io/getRawTx?tx=0x08abdb78f03578457dbee50579ec87560004a7fc2d59ccc25bebd29906f54b76
        // sender: 0x5124fcC2B3F99F571AD67D075643C743F38f1C34
        let tx_hex_1 = hex!("02f87201828dbb808507c29a7b4a830186a09422eec85ba6a5cd97ead4728ea1c69e1d9c6fa778881933fb6186170c5180c001a0606ae3e421ffbaa95e29dbdd60938adb0dff3d977eee221b1d58d31125e5a49ea00c2c644d2edb5d840d30ba827aa2e7e1df525f4029f0cf8c17b58f6051ba6af3");
        let eth_tx_1_signed = TransactionSigned::decode(&mut &tx_hex_1[..]).unwrap();
        let eth_tx_1 = eth_tx_1_signed.into_ecrecovered().unwrap();

        let eth_list_1: EthFullTransactionList = EthFullTransactionList(vec![eth_tx_1.clone()]);
        let eth_address_1 = EthAddress(eth_list_1.0[0].signer());

        // update max reserve balance and reserve balance
        acc_states.update_max_reserve_balances(vec![(eth_address_1, 10)]);
        acc_states.update_reserve_balances(vec![(eth_address_1, 1)]);

        // validate block 1
        let full_list_1 = FullTransactionList::new(eth_list_1.rlp_encode());
        assert!(acc_states.validate_block_txns(&full_list_1));

        // transaction should be stored as proposed
        let acc_1_state = acc_states.0.get(&eth_address_1).unwrap();
        assert!(acc_1_state.reserve_balance == 0);
        assert!(
            *acc_1_state
                .transaction_states
                .states
                .get(&eth_tx_1.hash)
                .unwrap()
                == EthTxState::Proposed(1)
        );

        // update committed transactions
        acc_states.update_committed_txns(&full_list_1);

        // transaction should be stored as committed
        let acc_1_state = acc_states.0.get(&eth_address_1).unwrap();
        assert!(acc_1_state.reserve_balance == 0);
        assert!(
            *acc_1_state
                .transaction_states
                .states
                .get(&eth_tx_1.hash)
                .unwrap()
                == EthTxState::Committed(1)
        );

        // remove block transactions after it was pruned
        acc_states.remove_block_txns(&full_list_1);

        // transaction should be removed from the states
        let acc_1_state = acc_states.0.get(&eth_address_1).unwrap();
        assert!(acc_1_state.reserve_balance == 0);
        assert!(acc_1_state
            .transaction_states
            .states
            .get(&eth_tx_1.hash)
            .is_none());
    }
}
