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

pub mod stub;

use bytes::Bytes;
use monad_mcp_common::{Inbound, Outbound, WirePriority};

pub use self::stub::{decode_frame, encode_frame};
use crate::{cadence::Recipients, common::NodeId, component::Link};

// what crosses the wire between nodes
#[derive(Clone)]
pub enum NodeMessage {
    Cadence(Bytes),
    // one chunk with its header, or a header alone
    Chunk(Bytes),
    ChunkRequest(Bytes),
    // rlp(Tx), sent by the rpc to the proposer it chose
    Tx(Bytes),
}

// the stub transport sends in order and does not read the priority
pub type NodeOutbound = Outbound<NodeMessage, Recipients, WirePriority>;

// the transport authenticates the sender
pub type NodeInbound = Inbound<NodeMessage, NodeId>;

// what the transport hands the node and takes from it
pub type NetworkHandle = Link<NodeOutbound, NodeInbound>;
