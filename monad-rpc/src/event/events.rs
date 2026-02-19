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

use crate::types::{eth_json::MonadNotification, serialize::SharedJsonSerialized};

type Header = SharedJsonSerialized<alloy_rpc_types::eth::Header>;
type Log = SharedJsonSerialized<alloy_rpc_types::eth::Log>;
type Block =
    SharedJsonSerialized<alloy_rpc_types::eth::Block<alloy_rpc_types::Transaction, Header>>;

#[derive(Clone, Debug)]
pub enum EventServerEvent {
    Gap,

    Block {
        header: SharedJsonSerialized<MonadNotification<Header>>,
        block: SharedJsonSerialized<MonadNotification<Block>>,
        logs: Arc<Vec<SharedJsonSerialized<MonadNotification<Log>>>>,
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
