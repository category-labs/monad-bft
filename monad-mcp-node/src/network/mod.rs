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

use crate::{chorus::types::NodeId, component::Link};

// what crosses the wire between nodes
#[derive(Clone)]
pub enum Packet {
    Cadence(Bytes),
    // one chunk with its header, or a header alone
    Chunk(Bytes),
    ChunkRequest(Bytes),
}

// todo: move this into shared types
pub type Outbound = crate::chorus::Outbound<Packet>;

// the transport authenticates the sender
pub struct Inbound {
    pub from: NodeId,
    pub packet: Packet,
}

// what the transport hands the node and takes from it
pub type NetworkHandle = Link<Outbound, Inbound>;
