use std::{net::SocketAddr, thread};

use bytes::Bytes;
use monoio::{IoUringDriver, RuntimeBuilder};
use tokio::sync::{mpsc, mpsc::error::TrySendError};
use tracing::warn;

pub mod tcp;
pub mod udp;

pub struct Dataplane {
    tcp_ingress_rx: mpsc::Receiver<(SocketAddr, Bytes)>,
    tcp_egress_tx: mpsc::Sender<(SocketAddr, Bytes)>,
    udp_ingress_rx: mpsc::Receiver<RecvMsg>,
    udp_egress_tx: mpsc::Sender<(SocketAddr, Bytes, u16)>,
}

#[derive(Clone)]
pub struct BroadcastMsg {
    pub targets: Vec<SocketAddr>,
    pub payload: Bytes,
    pub stride: u16,
}

#[derive(Clone)]
pub struct UnicastMsg {
    pub msgs: Vec<(SocketAddr, Bytes)>,
    pub stride: u16,
}

#[derive(Clone)]
pub struct RecvMsg {
    pub src_addr: SocketAddr,
    pub payload: Bytes,
    pub stride: u16,
}

const TCP_INGRESS_CHANNEL_SIZE: usize = 1024;
const TCP_EGRESS_CHANNEL_SIZE: usize = 1024;
const UDP_INGRESS_CHANNEL_SIZE: usize = 12_800;
const UDP_EGRESS_CHANNEL_SIZE: usize = 2048;

impl Dataplane {
    /// 1_000 = 1 Gbps, 10_000 = 10 Gbps
    pub fn new(local_addr: &SocketAddr, up_bandwidth_mbps: u64) -> Self {
        let local_addr = *local_addr;

        let (tcp_ingress_tx, tcp_ingress_rx) = mpsc::channel(TCP_INGRESS_CHANNEL_SIZE);
        let (tcp_egress_tx, tcp_egress_rx) = mpsc::channel(TCP_EGRESS_CHANNEL_SIZE);
        let (udp_ingress_tx, udp_ingress_rx) = mpsc::channel(UDP_INGRESS_CHANNEL_SIZE);
        let (udp_egress_tx, udp_egress_rx) = mpsc::channel(UDP_EGRESS_CHANNEL_SIZE);

        thread::spawn(move || {
            RuntimeBuilder::<IoUringDriver>::new()
                .enable_timer()
                .build()
                .expect("Failed building the Runtime")
                .block_on(async move {
                    tcp::spawn_tasks(local_addr, tcp_ingress_tx, tcp_egress_rx);

                    udp::spawn_tasks(local_addr, udp_ingress_tx, udp_egress_rx, up_bandwidth_mbps);

                    futures::future::pending::<()>().await;
                });
        });

        Dataplane {
            tcp_ingress_rx,
            tcp_egress_tx,
            udp_ingress_rx,
            udp_egress_tx,
        }
    }

    pub async fn tcp_read(&mut self) -> (SocketAddr, Bytes) {
        self.tcp_ingress_rx
            .recv()
            .await
            .expect("tcp_ingress_rx channel should never be closed")
    }

    pub fn tcp_write(&mut self, addr: SocketAddr, msg: Bytes) {
        match self.tcp_egress_tx.try_send((addr, msg)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => warn!("tcp_egress_tx channel full"),
            Err(TrySendError::Closed(_)) => panic!("tcp_egress_tx channel closed"),
        }
    }

    pub async fn udp_read(&mut self) -> RecvMsg {
        self.udp_ingress_rx
            .recv()
            .await
            .expect("udp_ingress_rx channel should never be closed")
    }

    pub fn udp_write_broadcast(&mut self, msg: BroadcastMsg) {
        for target in msg.targets {
            match self
                .udp_egress_tx
                .try_send((target, msg.payload.clone(), msg.stride))
            {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    warn!("udp_egress_tx channel full");
                    return;
                }
                Err(TrySendError::Closed(_)) => panic!("udp_egress_tx channel closed"),
            }
        }
    }

    pub fn udp_write_unicast(&mut self, msg: UnicastMsg) {
        for (addr, payload) in msg.msgs {
            match self.udp_egress_tx.try_send((addr, payload, msg.stride)) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    warn!("udp_egress_tx channel full");
                    return;
                }
                Err(TrySendError::Closed(_)) => panic!("udp_egress_tx channel closed"),
            }
        }
    }
}
