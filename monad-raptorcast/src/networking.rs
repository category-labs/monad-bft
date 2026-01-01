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

use std::net::{IpAddr, SocketAddr};

use bytes::Bytes;
use monad_dataplane::{BroadcastMsg, RecvTcpMsg, RecvUdpMsg, TcpMsg, UnicastMsg};
use monad_types::UdpPriority;

pub use self::dp::Dataplane;

pub trait TcpMessageSource {
    #[allow(async_fn_in_trait)] // Send bound not used for now
    async fn recv(&mut self) -> RecvTcpMsg;
}

pub trait TcpMessageSink {
    fn write(&self, dest: SocketAddr, msg: TcpMsg);
}

pub trait UdpMessageEndpoint {
    #[allow(async_fn_in_trait)] // Send bound not used for now
    async fn recv(&mut self) -> RecvUdpMsg;

    fn write(&self, dst: SocketAddr, payload: Bytes, stride: u16);
    fn write_broadcast(&self, msg: BroadcastMsg);
    fn write_broadcast_with_priority(&self, msg: BroadcastMsg, priority: UdpPriority);
    fn write_unicast(&self, msg: UnicastMsg);
    fn write_unicast_with_priority(&self, msg: UnicastMsg, priority: UdpPriority);
}

pub trait Control {
    fn update_trusted(&self, added: Vec<IpAddr>, removed: Vec<IpAddr>);
    fn disconnect(&self, addr: SocketAddr);
}

pub trait DataplaneType {
    type TcpMessageSource: TcpMessageSource;
    type TcpMessageSink: TcpMessageSink;
    type UdpMessageEndpoint: UdpMessageEndpoint;
    type Control: Control;
}

mod dp {
    use std::net::{IpAddr, SocketAddr};

    use bytes::Bytes;
    use monad_dataplane::{
        BroadcastMsg, DataplaneControl, RecvTcpMsg, RecvUdpMsg, TcpMsg, TcpSocketReader,
        TcpSocketWriter, UdpSocketHandle, UnicastMsg,
    };
    use monad_types::UdpPriority;

    use super::{Control, DataplaneType, TcpMessageSink, TcpMessageSource, UdpMessageEndpoint};

    pub struct Dataplane;
    impl DataplaneType for Dataplane {
        type TcpMessageSource = TcpSocketReader;
        type TcpMessageSink = TcpSocketWriter;
        type UdpMessageEndpoint = UdpSocketHandle;
        type Control = DataplaneControl;
    }

    impl TcpMessageSource for TcpSocketReader {
        async fn recv(&mut self) -> RecvTcpMsg {
            TcpSocketReader::recv(self).await
        }
    }

    impl TcpMessageSink for TcpSocketWriter {
        fn write(&self, dest: SocketAddr, msg: TcpMsg) {
            TcpSocketWriter::write(self, dest, msg)
        }
    }

    impl Control for DataplaneControl {
        fn update_trusted(&self, added: Vec<IpAddr>, removed: Vec<IpAddr>) {
            self.update_trusted(added, removed)
        }

        fn disconnect(&self, addr: SocketAddr) {
            self.disconnect(addr)
        }
    }

    impl UdpMessageEndpoint for UdpSocketHandle {
        async fn recv(&mut self) -> RecvUdpMsg {
            UdpSocketHandle::recv(self).await
        }

        fn write(&self, dst: SocketAddr, payload: Bytes, stride: u16) {
            UdpSocketHandle::write(self, dst, payload, stride)
        }

        fn write_broadcast(&self, msg: BroadcastMsg) {
            UdpSocketHandle::write_broadcast(self, msg)
        }

        fn write_broadcast_with_priority(&self, msg: BroadcastMsg, priority: UdpPriority) {
            UdpSocketHandle::write_broadcast_with_priority(self, msg, priority)
        }

        fn write_unicast(&self, msg: UnicastMsg) {
            UdpSocketHandle::write_unicast(self, msg)
        }

        fn write_unicast_with_priority(&self, msg: UnicastMsg, priority: UdpPriority) {
            UdpSocketHandle::write_unicast_with_priority(self, msg, priority)
        }
    }
}
