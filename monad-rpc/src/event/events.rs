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

use monad_exec_events::ffi::{monad_c_address, monad_c_bytes32};

use crate::{eth_json_types::MonadNotification, serialize::SharedJsonSerialized};

type Header = SharedJsonSerialized<alloy_rpc_types::eth::Header>;
type Log = SharedJsonSerialized<alloy_rpc_types::eth::Log>;
type Block =
    SharedJsonSerialized<alloy_rpc_types::eth::Block<alloy_rpc_types::Transaction, Header>>;

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
        header: SharedJsonSerialized<MonadNotification<Header>>,
        block: SharedJsonSerialized<MonadNotification<Block>>,
        logs: Arc<Vec<SharedJsonSerialized<MonadNotification<Log>>>>,
        storage_changes: Arc<Vec<StorageChange>>,
        tx_hashes: Arc<Vec<alloy_primitives::B256>>,
    },
}

#[cfg(test)]
mod test {
    use crate::event::EventServerEvent;

    #[test]
    fn size() {
        assert_eq!(std::mem::size_of::<EventServerEvent>(), 40);
    }
}
