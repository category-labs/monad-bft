use std::{net::SocketAddr, time::Duration};

use bytes::Bytes;
use monoio::spawn;
use tokio::sync::mpsc;
use zerocopy::{
    byteorder::little_endian::{U32, U64},
    AsBytes, FromBytes,
};

use super::TcpMsg;

mod rx;
mod tx;

const TCP_MESSAGE_LENGTH_LIMIT: usize = 1024 * 1024 * 1024;

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
    tcp_ingress_tx: mpsc::Sender<(SocketAddr, Bytes)>,
    tcp_egress_rx: mpsc::Receiver<(SocketAddr, TcpMsg)>,
) {
    spawn(rx::task(local_addr, tcp_ingress_tx));
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
