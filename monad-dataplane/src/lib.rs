use std::{net::SocketAddr, thread};

use bytes::Bytes;
use futures::channel::oneshot;
use monoio::{IoUringDriver, RuntimeBuilder};
use tokio::sync::{mpsc, mpsc::error::TrySendError};
use tracing::warn;

pub(crate) mod buffer_ext;
pub mod tcp;
pub mod udp;

pub struct DataplaneBuilder {
    local_addr: SocketAddr,
    /// 1_000 = 1 Gbps, 10_000 = 10 Gbps
    up_bandwidth_mbps: u64,
    udp_buffer_size: Option<usize>,
}

impl DataplaneBuilder {
    pub fn new(local_addr: &SocketAddr, up_bandwidth_mbps: u64) -> Self {
        Self {
            local_addr: *local_addr,
            up_bandwidth_mbps,
            udp_buffer_size: None,
        }
    }

    /// with_udp_buffer_size sets the buffer size for udp socket that is managed by dataplane
    /// to a requested value.
    pub fn with_udp_buffer_size(mut self, buffer_size: usize) -> Self {
        self.udp_buffer_size = Some(buffer_size);
        self
    }

    pub fn build(self) -> Dataplane {
        let DataplaneBuilder {
            local_addr,
            up_bandwidth_mbps,
            udp_buffer_size: buffer_size,
        } = self;

        let (tcp_ingress_tx, tcp_ingress_rx) = mpsc::channel(TCP_INGRESS_CHANNEL_SIZE);
        let (tcp_egress_tx, tcp_egress_rx) = mpsc::channel(TCP_EGRESS_CHANNEL_SIZE);
        let (udp_ingress_tx, udp_ingress_rx) = mpsc::channel(UDP_INGRESS_CHANNEL_SIZE);
        let (udp_egress_tx, udp_egress_rx) = mpsc::channel(UDP_EGRESS_CHANNEL_SIZE);

        thread::Builder::new()
            .name("monad-dataplane".into())
            .spawn(move || {
                RuntimeBuilder::<IoUringDriver>::new()
                    .enable_timer()
                    .build()
                    .expect("Failed building the Runtime")
                    .block_on(async move {
                        tcp::spawn_tasks(local_addr, tcp_ingress_tx, tcp_egress_rx);

                        udp::spawn_tasks(
                            local_addr,
                            udp_ingress_tx,
                            udp_egress_rx,
                            up_bandwidth_mbps,
                            buffer_size,
                        );

                        futures::future::pending::<()>().await;
                    });
            })
            .expect("failed to spawn dataplane thread");

        Dataplane {
            tcp_ingress_rx,
            tcp_egress_tx,
            udp_ingress_rx,
            udp_egress_tx,

            tcp_msgs_dropped: 0,
            udp_msgs_dropped: 0,
        }
    }
}

pub struct Dataplane {
    tcp_ingress_rx: mpsc::Receiver<(SocketAddr, Bytes)>,
    tcp_egress_tx: mpsc::Sender<(SocketAddr, TcpMsg)>,
    udp_ingress_rx: mpsc::Receiver<RecvMsg>,
    udp_egress_tx: mpsc::Sender<(SocketAddr, Bytes, u16)>,

    tcp_msgs_dropped: usize,
    udp_msgs_dropped: usize,
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

pub struct TcpMsg {
    pub msg: Bytes,
    pub completion: Option<oneshot::Sender<()>>,
}

const TCP_INGRESS_CHANNEL_SIZE: usize = 1024;
const TCP_EGRESS_CHANNEL_SIZE: usize = 1024;
const UDP_INGRESS_CHANNEL_SIZE: usize = 12_800;
const UDP_EGRESS_CHANNEL_SIZE: usize = 12_800;

impl Dataplane {
    pub async fn tcp_read(&mut self) -> (SocketAddr, Bytes) {
        self.tcp_ingress_rx
            .recv()
            .await
            .expect("tcp_ingress_rx channel should never be closed")
    }

    pub fn tcp_write(&mut self, addr: SocketAddr, msg: TcpMsg) {
        let msg_length = msg.msg.len();

        match self.tcp_egress_tx.try_send((addr, msg)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                self.tcp_msgs_dropped += 1;

                warn!(
                    num_msgs_dropped = 1,
                    total_tcp_msgs_dropped = self.tcp_msgs_dropped,
                    ?addr,
                    msg_length,
                    "tcp_egress_tx channel full, dropping message"
                );
            }
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
        let num_targets = msg.targets.len();

        for (i, target) in msg.targets.into_iter().enumerate() {
            match self
                .udp_egress_tx
                .try_send((target, msg.payload.clone(), msg.stride))
            {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    let num_msgs_dropped = num_targets - i;

                    self.udp_msgs_dropped += num_msgs_dropped;

                    warn!(
                        num_msgs_dropped,
                        total_udp_msgs_dropped = self.udp_msgs_dropped,
                        msg_length = msg.payload.len(),
                        "udp_egress_tx channel full, dropping message"
                    );

                    return;
                }
                Err(TrySendError::Closed(_)) => panic!("udp_egress_tx channel closed"),
            }
        }
    }

    pub fn udp_write_unicast(&mut self, msg: UnicastMsg) {
        let num_msgs = msg.msgs.len();

        for (i, (addr, payload)) in msg.msgs.into_iter().enumerate() {
            match self.udp_egress_tx.try_send((addr, payload, msg.stride)) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    let num_msgs_dropped = num_msgs - i;

                    self.udp_msgs_dropped += num_msgs_dropped;

                    warn!(
                        num_msgs_dropped,
                        total_udp_msgs_dropped = self.udp_msgs_dropped,
                        "udp_egress_tx channel full, dropping message"
                    );

                    return;
                }
                Err(TrySendError::Closed(_)) => panic!("udp_egress_tx channel closed"),
            }
        }
    }
}
