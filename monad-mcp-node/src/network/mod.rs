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
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

use crate::chorus::types::NodeId;

// what crosses the wire between nodes
pub enum Packet {
    Cadence(Bytes),
    // one chunk with its header, or a header alone
    Chunk(Bytes),
    ChunkRequest(Bytes),
}

pub enum Outbound {
    Broadcast(Packet),
    Unicast { to: NodeId, packet: Packet },
}

// the transport authenticates the sender
pub struct Inbound {
    pub from: NodeId,
    pub packet: Packet,
}

// one end of a two-way channel
pub struct Link<Out, In> {
    sender: UnboundedSender<Out>,
    receiver: UnboundedReceiver<In>,
}

pub type NetworkHandle = Link<Outbound, Inbound>;

impl<Out, In> Link<Out, In> {
    pub fn pair() -> (Link<Out, In>, Link<In, Out>) {
        let (out_sender, out_receiver) = unbounded_channel();
        let (in_sender, in_receiver) = unbounded_channel();
        let ours = Link {
            sender: out_sender,
            receiver: in_receiver,
        };
        let theirs = Link {
            sender: in_sender,
            receiver: out_receiver,
        };
        (ours, theirs)
    }

    // dropped silently once the other end is gone
    pub fn send(&self, message: Out) {
        self.sender.send(message).ok();
    }

    pub fn sender(&self) -> UnboundedSender<Out> {
        self.sender.clone()
    }

    // None once the other end is gone
    pub async fn recv(&mut self) -> Option<In> {
        self.receiver.recv().await
    }
}
