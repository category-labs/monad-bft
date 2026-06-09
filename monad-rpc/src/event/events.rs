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
    ops::Deref,
    sync::{Arc, OnceLock},
};

use monad_exec_events::BlockCommitState;
use serde_json::value::RawValue;

use crate::types::{eth_json::MonadNotification, serialize::SharedJsonSerialized};

pub type HeaderNotification = SharedJsonSerialized<MonadNotification<Header>>;
pub type Header = SharedJsonSerialized<alloy_rpc_types::eth::Header>;

pub type BlockTransactions = Arc<BlockTransactionList>;
pub type BlockTransaction = (Transaction, TransactionReceipt, Box<[LogNotification]>);

pub type Transaction = SharedJsonSerialized<alloy_rpc_types::Transaction>;
pub type TransactionReceipt = SharedJsonSerialized<alloy_rpc_types::TransactionReceipt>;

pub type LogNotification = SharedJsonSerialized<MonadNotification<Log>>;
pub type Log = SharedJsonSerialized<alloy_rpc_types::eth::Log>;

#[derive(Debug)]
pub struct BlockTransactionList {
    transactions: Box<[BlockTransaction]>,
    receipts_json: OnceLock<Box<RawValue>>,
}

impl BlockTransactionList {
    pub fn new(transactions: Box<[BlockTransaction]>) -> Self {
        Self {
            transactions,
            receipts_json: OnceLock::new(),
        }
    }

    pub fn receipts_json<F, E>(&self, build: F) -> Result<&Box<RawValue>, E>
    where
        F: FnOnce(&[BlockTransaction]) -> Result<Box<RawValue>, E>,
    {
        if let Some(serialized) = self.receipts_json.get() {
            return Ok(serialized);
        }

        let serialized = build(&self.transactions)?;
        let _ = self.receipts_json.set(serialized);
        Ok(self
            .receipts_json
            .get()
            .expect("receipts json populated above"))
    }
}

impl Deref for BlockTransactionList {
    type Target = [BlockTransaction];

    fn deref(&self) -> &Self::Target {
        &self.transactions
    }
}

#[derive(Clone, Debug)]
pub enum EventServerEvent {
    Gap,

    Block {
        commit_state: BlockCommitState,
        header: HeaderNotification,
        transactions: BlockTransactions,
    },
}

#[cfg(test)]
mod test {
    use crate::event::EventServerEvent;

    #[test]
    fn size() {
        assert_eq!(std::mem::size_of::<EventServerEvent>(), 24);
    }
}
