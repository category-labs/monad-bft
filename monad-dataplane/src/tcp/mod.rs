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

use std::{net::SocketAddr, time::Duration};

use monoio::{
    net::{ListenerOpts, TcpListener},
    spawn,
};
use tokio::sync::mpsc;
use zerocopy::{
    byteorder::little_endian::{U32, U64},
    AsBytes, FromBytes,
};

use super::{RecvTcpMsg, TcpMsg};

pub mod rx;
pub mod tx;

const TCP_MESSAGE_LENGTH_LIMIT: usize = 3 * 1024 * 1024;

const HEADER_MAGIC: u32 = 0x434e5353; // "SSNC"
const HEADER_VERSION: u32 = 1;

#[derive(AsBytes, Debug, FromBytes)]
#[repr(C)]
struct TcpMsgHdr {
    magic: U32,
    version: U32,
    length: U64,
}

impl TcpMsgHdr {
    fn new(length: u64) -> TcpMsgHdr {
        TcpMsgHdr {
            magic: U32::new(HEADER_MAGIC),
            version: U32::new(HEADER_VERSION),
            length: U64::new(length),
        }
    }
}

pub fn spawn_tasks(
    local_addr: SocketAddr,
    tcp_ingress_tx: mpsc::Sender<RecvTcpMsg>,
    tcp_egress_rx: mpsc::Receiver<(SocketAddr, TcpMsg)>,
) {
    let opts = ListenerOpts::new().reuse_addr(true);
    let tcp_listener = TcpListener::bind_with_config(local_addr, &opts).unwrap();

    spawn(rx::task(tcp_listener, tcp_ingress_tx));
    spawn(tx::task(tcp_egress_rx));
}

// Minimum message receive/transmit speed in bytes per second.  Messages that are
// transferred slower than this are aborted.
const MINIMUM_TRANSFER_SPEED: u64 = 1_000_000;

// Allow for at least this transfer time, so that very small messages still have
// a chance to be transferred successfully.
const MINIMUM_TRANSFER_TIME: Duration = Duration::from_secs(10);

fn message_timeout(len: usize) -> Duration {
    Duration::from_millis(u64::try_from(len).unwrap() / (MINIMUM_TRANSFER_SPEED / 1000))
        .max(MINIMUM_TRANSFER_TIME)
}
