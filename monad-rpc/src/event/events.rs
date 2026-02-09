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

use std::sync::Arc;

use monad_exec_events::BlockCommitState;
use monad_exec_events::ffi::{monad_c_address, monad_c_bytes32};

use crate::types::{eth_json::MonadNotification, serialize::SharedJsonSerialized};

pub type HeaderNotification = SharedJsonSerialized<MonadNotification<Header>>;
pub type Header = SharedJsonSerialized<alloy_rpc_types::eth::Header>;

pub type BlockTransactions = Arc<Box<[BlockTransaction]>>;
pub type BlockTransaction = (Transaction, TransactionReceipt, Box<[LogNotification]>);

pub type Transaction = SharedJsonSerialized<alloy_rpc_types::Transaction>;
pub type TransactionReceipt = SharedJsonSerialized<alloy_rpc_types::TransactionReceipt>;

pub type LogNotification = SharedJsonSerialized<MonadNotification<Log>>;
pub type Log = SharedJsonSerialized<alloy_rpc_types::eth::Log>;

/// A storage change extracted from execution events.
#[derive(Clone, Debug)]
pub struct StorageChange {
    pub address: monad_c_address,
    pub key: monad_c_bytes32,
    pub old_value: monad_c_bytes32,
    pub new_value: monad_c_bytes32,
    pub txn_index: usize,
}

#[derive(Clone, Debug)]
pub enum EventServerEvent {
    Gap,

    Block {
        commit_state: BlockCommitState,
        header: HeaderNotification,
        transactions: BlockTransactions,
        storage_changes: Arc<Vec<StorageChange>>,
    },
}

#[cfg(test)]
mod test {
    use crate::event::EventServerEvent;

    #[test]
    fn size() {
        assert_eq!(std::mem::size_of::<EventServerEvent>(), 32);
    }
}
