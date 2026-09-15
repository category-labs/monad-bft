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

use std::{
    fs::OpenOptions,
    io::{BufWriter, Read, Write},
    net::{SocketAddr, TcpListener, UdpSocket},
    num::NonZeroU64,
    os::unix::io::AsRawFd,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

use bytes::Bytes;
use clap::{Parser, Subcommand};
use futures::{channel::oneshot, executor::block_on, stream::FuturesUnordered, StreamExt};
use monad_dataplane::{DataplaneBuilder, TcpMsg, TcpSocketId, UdpSocketId, UnicastMsg};
use monad_executor::ExecutorMetrics;
use monad_types::UdpPriority;
use prometheus::{Encoder, Registry, TextEncoder};
use tracing::{info, warn};
use tracing_manytrace::{ManytraceLayer, TracingExtension};
use tracing_subscriber::{layer::SubscriberExt, Layer};

const UDP_SEGMENT: i32 = 103;
const SOL_UDP: i32 = 17;
const TCP_MAX_MESSAGE_SIZE: usize = 3 * 1024 * 1024;
const TCP_QUEUED_MESSAGE_BYTE_LIMIT: usize = 4 * 1024 * 1024;
const UDP_RECEIVE_BATCH_SIZE: usize = 1024;

extern "C" {
    fn setsockopt(
        socket: i32,
        level: i32,
        name: i32,
        value: *const std::ffi::c_void,
        option_len: u32,
    ) -> i32;
}

#[derive(Parser)]
#[command(name = "throughput")]
#[command(about = "dataplane throughput test")]
struct Args {
    #[arg(
        long,
        global = true,
        help = "Unix socket exposed to a manytrace collector"
    )]
    manytrace_socket: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    #[command(alias = "w", about = "run gso-based udp writer")]
    Writer(WriterArgs),
    #[command(alias = "nw", about = "run native dataplane writer")]
    NativeWriter(NativeWriterArgs),
    #[command(alias = "r", about = "run native udp reader")]
    Reader(ReaderArgs),
    #[command(alias = "tw", about = "run a TCP dataplane writer")]
    TcpWriter(TcpWriterArgs),
    #[command(alias = "tr", about = "run a TCP dataplane reader")]
    TcpReader(TcpReaderArgs),
}

#[derive(clap::Args)]
struct WriterArgs {
    #[arg(help = "target address to send packets to")]
    target: SocketAddr,

    #[arg(
        long,
        default_value = "1",
        help = "number of concurrent sender threads"
    )]
    writers: usize,

    #[arg(
        long,
        default_value = "1472",
        help = "packet size in bytes (max 1472 for standard MTU)"
    )]
    packet_size: usize,

    #[arg(
        long,
        default_value = "44",
        help = "burst size (number of packets per GSO send, max total 65536 bytes)"
    )]
    burst_size: usize,
}

#[derive(clap::Args)]
struct NativeWriterArgs {
    #[command(flatten)]
    sampling: TxSamplingArgs,

    #[arg(help = "target address, or comma-separated target addresses, to send packets to")]
    #[arg(value_delimiter = ',', required = true)]
    target: Vec<SocketAddr>,

    #[arg(
        long,
        default_value = "1472",
        help = "packet size in bytes (max 1472 for standard MTU)"
    )]
    packet_size: usize,

    #[arg(
        short = 'w',
        long = "wb",
        default_value = "1000",
        help = "writer bandwidth in Mbps (megabits per second)"
    )]
    writer_bandwidth_mbps: u64,

    #[arg(
        short = 'd',
        long = "db",
        default_value = "10000",
        help = "dataplane bandwidth limit in Mbps (should be >= writer bandwidth)"
    )]
    dataplane_bandwidth_mbps: u64,

    #[arg(
        long,
        help = "per-peer dataplane bandwidth limit in Mbps (defaults to the global limit)"
    )]
    peer_bandwidth_mbps: Option<u64>,

    #[arg(long, default_value = "4", help = "number of UDP TX workers")]
    tx_workers: usize,

    #[arg(
        long,
        default_value = "128",
        help = "number of messages to write before sleeping"
    )]
    batch_size: usize,

    #[arg(long, help = "address for externally scraped Prometheus metrics")]
    metrics_addr: Option<SocketAddr>,
}

#[derive(clap::Args)]
struct ReaderArgs {
    #[arg(
        long,
        default_value = "0.0.0.0:19999",
        help = "bind address for receiver"
    )]
    bind_addr: SocketAddr,

    #[arg(
        short = 'm',
        long,
        default_value = "false",
        help = "use multishot ringbuf receive"
    )]
    multishot: bool,

    #[arg(long, help = "address for externally scraped Prometheus metrics")]
    metrics_addr: Option<SocketAddr>,
}

#[derive(clap::Args)]
struct TcpWriterArgs {
    #[command(flatten)]
    sampling: TxSamplingArgs,

    #[arg(help = "receiver address, or comma-separated receiver addresses")]
    #[arg(value_delimiter = ',', required = true)]
    target: Vec<SocketAddr>,

    #[arg(
        long,
        default_value = "0.0.0.0:0",
        help = "TCP bind address for the writer"
    )]
    bind_addr: SocketAddr,

    #[arg(
        long,
        default_value = "1048576",
        help = "TCP message payload size in bytes"
    )]
    message_size: usize,

    #[arg(
        short = 'd',
        long = "db",
        default_value = "2000",
        help = "shared dataplane bandwidth limit in Mbps"
    )]
    dataplane_bandwidth_mbps: u64,

    #[arg(
        long,
        default_value = "2",
        help = "maximum TCP messages awaiting completion per receiver"
    )]
    in_flight_per_receiver: usize,

    #[arg(long, default_value = "4", help = "number of UDP TX workers")]
    udp_tx_workers: usize,

    #[arg(long, help = "address for externally scraped Prometheus metrics")]
    metrics_addr: SocketAddr,

    #[arg(
        long,
        help = "optional comma-separated UDP receivers for concurrent high-priority bursts"
    )]
    #[arg(value_delimiter = ',')]
    udp_burst_target: Vec<SocketAddr>,

    #[arg(
        long,
        default_value = "1200",
        help = "UDP payload bytes per burst packet"
    )]
    udp_burst_packet_size: usize,

    #[arg(
        long,
        default_value = "52428800",
        help = "UDP payload bytes submitted in each burst"
    )]
    udp_burst_bytes: usize,

    #[arg(
        long,
        default_value = "2000",
        help = "interval between UDP burst starts in milliseconds"
    )]
    udp_burst_interval_ms: u64,
}

#[derive(clap::Args)]
struct TcpReaderArgs {
    #[arg(
        long,
        default_value = "0.0.0.0:19999",
        help = "TCP bind address for the receiver"
    )]
    bind_addr: SocketAddr,

    #[arg(
        short = 'd',
        long = "db",
        default_value = "10000",
        help = "shared dataplane bandwidth limit in Mbps"
    )]
    dataplane_bandwidth_mbps: u64,

    #[arg(long, help = "address for externally scraped Prometheus metrics")]
    metrics_addr: SocketAddr,
}

#[derive(clap::Args)]
struct TxSamplingArgs {
    #[arg(long, help = "write timestamped send counters to a new CSV file")]
    tx_samples_csv: Option<PathBuf>,

    #[arg(
        long,
        default_value = "10",
        help = "send-counter sampling interval in milliseconds"
    )]
    tx_sample_interval_ms: NonZeroU64,
}

fn main() {
    let args = Args::parse();
    let _manytrace_agent = setup_tracing(args.manytrace_socket);

    match args.command {
        Command::Writer(options) => run_writer(
            options.target,
            options.writers,
            options.packet_size,
            options.burst_size,
        ),
        Command::NativeWriter(options) => run_native_writer(options),
        Command::Reader(options) => {
            run_native(options.bind_addr, options.multishot, options.metrics_addr)
        }
        Command::TcpWriter(options) => run_tcp_writer(options),
        Command::TcpReader(options) => run_tcp_reader(
            options.bind_addr,
            options.dataplane_bandwidth_mbps,
            options.metrics_addr,
        ),
    }
}

fn setup_tracing(manytrace_socket: Option<String>) -> Option<agent::Agent> {
    let filter = || {
        tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
    };
    if let Some(socket_path) = manytrace_socket {
        let extension = Arc::new(TracingExtension::new());
        let agent = agent::AgentBuilder::new(socket_path)
            .register_tracing(Box::new((*extension).clone()))
            .build()
            .expect("failed to build manytrace agent");
        let subscriber = tracing_subscriber::Registry::default()
            .with(ManytraceLayer::new(extension))
            .with(tracing_subscriber::fmt::layer().with_filter(filter()));
        tracing::subscriber::set_global_default(subscriber)
            .expect("failed to set tracing subscriber");
        info!("manytrace tracing enabled");
        Some(agent)
    } else {
        let subscriber = tracing_subscriber::Registry::default()
            .with(tracing_subscriber::fmt::layer().with_filter(filter()));
        tracing::subscriber::set_global_default(subscriber)
            .expect("failed to set tracing subscriber");
        None
    }
}

fn run_writer(target_addr: SocketAddr, num_writers: usize, packet_size: usize, burst_size: usize) {
    assert!(
        packet_size > 0 && packet_size <= 1472,
        "packet_size must be between 1 and 1472 bytes"
    );
    assert!(burst_size > 0, "burst_size must be greater than 0");

    let total_buffer_size = packet_size * burst_size;
    assert!(
        total_buffer_size < 65536,
        "total buffer size (packet_size * burst_size = {}) must be less than 65536 bytes",
        total_buffer_size
    );
    let msgs_sent = Arc::new(AtomicU64::new(0));

    let mut writers = Vec::new();

    for writer_id in 0..num_writers {
        let msgs_sent_clone = msgs_sent.clone();

        let writer = thread::spawn(move || {
            let socket = UdpSocket::bind("0.0.0.0:0").expect("failed to bind writer socket");
            socket.set_nonblocking(true).unwrap();

            let send_buf_size = (total_buffer_size * 2).max(1024 * 1024);
            unsafe {
                let optval = send_buf_size as i32;
                let ret = setsockopt(
                    socket.as_raw_fd(),
                    libc::SOL_SOCKET,
                    libc::SO_SNDBUF,
                    &optval as *const _ as *const std::ffi::c_void,
                    std::mem::size_of_val(&optval) as u32,
                );
                if ret != 0 {
                    eprintln!(
                        "failed to set SO_SNDBUF: {}",
                        std::io::Error::last_os_error()
                    );
                }
            }

            let gso_size = packet_size as u16;

            unsafe {
                let optval = gso_size as i32;
                let ret = setsockopt(
                    socket.as_raw_fd(),
                    SOL_UDP,
                    UDP_SEGMENT,
                    &optval as *const _ as *const std::ffi::c_void,
                    std::mem::size_of_val(&optval) as u32,
                );
                if ret != 0 {
                    if writer_id == 0 {
                        info!("gso not supported, falling back to regular sends");
                    }
                } else if writer_id == 0 {
                    info!(
                        packet_size = packet_size,
                        burst_size = burst_size,
                        total_buffer_size = total_buffer_size,
                        gso_segment_size = gso_size,
                        writers = num_writers,
                        "gso enabled"
                    );
                }
            }

            let gso_buffer = vec![0u8; packet_size * burst_size];

            let mut last_log = Instant::now();
            let log_interval = Duration::from_secs(1);
            let mut msgs_sent = 0u64;
            let mut bytes_sent = 0u64;

            loop {
                match socket.send_to(&gso_buffer, target_addr) {
                    Ok(_) => {
                        msgs_sent_clone.fetch_add(burst_size as u64, Ordering::Relaxed);
                        msgs_sent += burst_size as u64;
                        bytes_sent += (packet_size * burst_size) as u64;

                        let now = Instant::now();
                        if now.duration_since(last_log) >= log_interval {
                            let elapsed = now.duration_since(last_log).as_secs_f64();
                            let msgs_per_sec = msgs_sent as f64 / elapsed;
                            let mbps = (bytes_sent as f64 * 8.0) / elapsed / 1_000_000.0;

                            info!(
                                writer_id = writer_id,
                                msgs_sent = msgs_sent,
                                msgs_per_sec = format!("{:.0}", msgs_per_sec),
                                mbps = format!("{:.2}", mbps),
                                "writer throughput"
                            );

                            msgs_sent = 0;
                            bytes_sent = 0;
                            last_log = now;
                        }
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::yield_now();
                    }
                    Err(e) => {
                        eprintln!("writer {} send error: {}", writer_id, e);
                        break;
                    }
                }
            }
        });

        writers.push(writer);
    }

    for writer in writers {
        writer.join().expect("writer thread panicked");
    }
}

fn run_native_writer(options: NativeWriterArgs) {
    let NativeWriterArgs {
        sampling,
        target: target_addrs,
        packet_size,
        writer_bandwidth_mbps,
        dataplane_bandwidth_mbps,
        peer_bandwidth_mbps,
        tx_workers,
        batch_size,
        metrics_addr,
    } = options;
    assert!(
        packet_size > 0 && packet_size <= 1472,
        "packet_size must be between 1 and 1472 bytes"
    );
    assert!(!target_addrs.is_empty(), "at least one target is required");
    assert!(
        writer_bandwidth_mbps > 0,
        "writer_bandwidth_mbps must be greater than 0"
    );
    assert!(
        dataplane_bandwidth_mbps > 0,
        "dataplane_bandwidth_mbps must be greater than 0"
    );
    assert!(
        peer_bandwidth_mbps.is_none_or(|bandwidth| bandwidth > 0),
        "peer_bandwidth_mbps must be greater than 0"
    );
    assert!(batch_size > 0, "batch_size must be greater than 0");

    let bind_addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
    info!(
        bind_addr = %bind_addr,
        ?target_addrs,
        packet_size = packet_size,
        writer_bandwidth_mbps = writer_bandwidth_mbps,
        dataplane_bandwidth_mbps = dataplane_bandwidth_mbps,
        peer_bandwidth_mbps = peer_bandwidth_mbps.unwrap_or(dataplane_bandwidth_mbps),
        tx_workers,
        batch_size = batch_size,
        "starting native dataplane writer"
    );

    let mut builder =
        DataplaneBuilder::new(dataplane_bandwidth_mbps).with_udp_tx_workers(tx_workers);
    if let Some(peer_bandwidth_mbps) = peer_bandwidth_mbps {
        builder = builder.with_udp_peer_bandwidth_mbps(peer_bandwidth_mbps);
    }
    let mut dataplane = builder
        .with_udp_sockets([(UdpSocketId::Raptorcast, bind_addr)])
        .build();

    dataplane
        .block_until_ready(Duration::from_secs(5))
        .then_some(())
        .expect("dataplane not ready");
    if let Some(metrics_addr) = metrics_addr {
        start_metrics_server(dataplane.metrics(), metrics_addr);
    }
    start_tx_sampler(dataplane.metrics(), sampling);

    let udp_socket = dataplane
        .udp_sockets
        .take(UdpSocketId::Raptorcast)
        .expect("failed to get writer socket");

    let writer = udp_socket.writer().clone();
    let payload = Bytes::from(vec![0u8; packet_size]);

    let sleep_duration_nanos =
        (packet_size as u64 * batch_size as u64 * 8 * 1_000) / writer_bandwidth_mbps;
    let sleep_duration = Duration::from_nanos(sleep_duration_nanos);
    let mut target_index = 0;

    loop {
        for _ in 0..batch_size {
            writer.write(
                target_addrs[target_index],
                payload.clone(),
                packet_size as u16,
            );
            target_index = (target_index + 1) % target_addrs.len();
        }
        thread::sleep(sleep_duration);
    }
}

fn run_native(bind_addr: SocketAddr, multishot: bool, metrics_addr: Option<SocketAddr>) {
    info!(addr = %bind_addr, multishot, "starting native dataplane reader");

    let mut dataplane = DataplaneBuilder::new(10_000)
        .with_udp_multishot(multishot)
        .with_udp_sockets([(UdpSocketId::Raptorcast, bind_addr)])
        .build();

    dataplane
        .block_until_ready(Duration::from_secs(5))
        .then_some(())
        .expect("dataplane not ready");
    if let Some(metrics_addr) = metrics_addr {
        start_metrics_server(dataplane.metrics(), metrics_addr);
    }

    let mut udp_socket = dataplane
        .udp_sockets
        .take(UdpSocketId::Raptorcast)
        .expect("failed to get bench socket");

    let mut msgs_received = 0u64;
    let mut bytes_received = 0u64;
    let mut last_log = Instant::now();
    let log_interval = Duration::from_secs(1);
    let mut messages = Vec::with_capacity(UDP_RECEIVE_BATCH_SIZE);

    loop {
        messages.clear();
        msgs_received +=
            block_on(udp_socket.recv_many(&mut messages, UDP_RECEIVE_BATCH_SIZE)) as u64;
        bytes_received += messages
            .iter()
            .map(|message| message.payload.len() as u64)
            .sum::<u64>();

        let now = Instant::now();
        if now.duration_since(last_log) >= log_interval {
            let elapsed = now.duration_since(last_log).as_secs_f64();
            let msgs_per_sec = msgs_received as f64 / elapsed;
            let mbps = (bytes_received as f64 * 8.0) / elapsed / 1_000_000.0;

            info!(
                bind_addr = %bind_addr,
                msgs_received = msgs_received,
                msgs_per_sec = format!("{:.0}", msgs_per_sec),
                mbps = format!("{:.2}", mbps),
                "native throughput stats"
            );

            msgs_received = 0;
            bytes_received = 0;
            last_log = now;
        }
    }
}

struct UdpBurstConfig {
    targets: Vec<SocketAddr>,
    packet_size: usize,
    bytes: usize,
    interval: Duration,
}

fn run_tcp_writer(options: TcpWriterArgs) {
    let TcpWriterArgs {
        sampling,
        target: target_addrs,
        bind_addr,
        message_size,
        dataplane_bandwidth_mbps,
        in_flight_per_receiver,
        udp_tx_workers,
        metrics_addr,
        udp_burst_target,
        udp_burst_packet_size,
        udp_burst_bytes,
        udp_burst_interval_ms,
    } = options;
    let udp_burst = (!udp_burst_target.is_empty()).then_some(UdpBurstConfig {
        targets: udp_burst_target,
        packet_size: udp_burst_packet_size,
        bytes: udp_burst_bytes,
        interval: Duration::from_millis(udp_burst_interval_ms),
    });
    assert!(
        (1..=TCP_MAX_MESSAGE_SIZE).contains(&message_size),
        "message_size must be between 1 and {TCP_MAX_MESSAGE_SIZE} bytes"
    );
    assert!(
        dataplane_bandwidth_mbps > 0,
        "dataplane_bandwidth_mbps must be greater than 0"
    );
    assert!(
        !target_addrs.is_empty(),
        "at least one receiver is required"
    );
    let queued_bytes = message_size
        .checked_mul(in_flight_per_receiver)
        .expect("message_size * in_flight_per_receiver overflows usize");
    assert!(
        queued_bytes <= TCP_QUEUED_MESSAGE_BYTE_LIMIT,
        "message_size * in_flight_per_receiver must not exceed the TCP per-peer queue limit of \
         {TCP_QUEUED_MESSAGE_BYTE_LIMIT} bytes"
    );
    assert!(
        in_flight_per_receiver > 0,
        "in_flight_per_receiver must be greater than 0"
    );
    assert!(udp_tx_workers > 0, "udp_tx_workers must be greater than 0");
    for (index, target) in target_addrs.iter().enumerate() {
        assert!(
            !target_addrs[..index].contains(target),
            "duplicate TCP receiver address {target}"
        );
    }
    if let Some(udp_burst) = &udp_burst {
        assert!(
            !udp_burst.targets.is_empty(),
            "at least one UDP burst receiver is required"
        );
        assert!(
            (1..=1472).contains(&udp_burst.packet_size),
            "udp_burst_packet_size must be between 1 and 1472 bytes"
        );
        assert!(
            udp_burst.bytes > 0,
            "udp_burst_bytes must be greater than 0"
        );
        assert!(
            !udp_burst.interval.is_zero(),
            "udp_burst_interval_ms must be greater than 0"
        );
    }

    let mut builder = DataplaneBuilder::new(dataplane_bandwidth_mbps)
        .with_udp_tx_workers(udp_tx_workers)
        .with_tcp_sockets([(TcpSocketId::Raptorcast, bind_addr)]);
    if udp_burst.is_some() {
        builder =
            builder.with_udp_sockets([(UdpSocketId::DirectUdp, "0.0.0.0:0".parse().unwrap())]);
    }
    let mut sender = builder.build();
    assert!(
        sender.block_until_ready(Duration::from_secs(5)),
        "sender dataplane not ready"
    );
    start_metrics_server(sender.metrics(), metrics_addr);
    start_tx_sampler(sender.metrics(), sampling);

    let sender_socket = sender
        .tcp_sockets
        .take(TcpSocketId::Raptorcast)
        .expect("failed to get TCP sender socket");
    info!(
        local_addr = %sender_socket.local_addr(),
        ?target_addrs,
        message_size,
        dataplane_bandwidth_mbps,
        in_flight_per_receiver,
        "starting TCP dataplane writer"
    );

    if let Some(UdpBurstConfig {
        targets,
        packet_size,
        bytes,
        interval,
    }) = udp_burst
    {
        let udp_socket = sender
            .udp_sockets
            .take(UdpSocketId::DirectUdp)
            .expect("failed to get UDP burst sender socket");
        let local_addr = udp_socket.local_addr();
        let udp_writer = udp_socket.writer().clone();
        thread::Builder::new()
            .name("throughput-udp-bursts".to_owned())
            .spawn(move || {
                let bytes_per_target = bytes / targets.len();
                let remainder = bytes % targets.len();
                let burst = targets
                    .iter()
                    .enumerate()
                    .map(|(index, target)| {
                        let target_bytes = bytes_per_target + usize::from(index < remainder);
                        (*target, Bytes::from(vec![0_u8; target_bytes]))
                    })
                    .collect::<Vec<_>>();
                info!(
                    %local_addr,
                    ?targets,
                    packet_size,
                    bytes,
                    interval_ms = interval.as_millis(),
                    "starting high-priority UDP bursts"
                );
                loop {
                    udp_writer.write_unicast_with_priority(
                        UnicastMsg {
                            msgs: burst.clone(),
                            stride: packet_size as u16,
                        },
                        UdpPriority::High,
                    );
                    thread::sleep(interval);
                }
            })
            .expect("failed to spawn UDP burst writer thread");
    }

    let payload = Bytes::from(vec![0_u8; message_size]);
    block_on(drive_tcp_load(
        &target_addrs,
        in_flight_per_receiver,
        |addr| {
            let (completion_tx, completion_rx) = oneshot::channel();
            sender_socket.write(
                addr,
                TcpMsg {
                    msg: payload.clone(),
                    completion: Some(completion_tx),
                },
            );
            completion_rx
        },
    ));
}

/// Replenish the window of whichever peer completes, independently of other peers.
async fn drive_tcp_load(
    targets: &[SocketAddr],
    in_flight_per_receiver: usize,
    mut send: impl FnMut(SocketAddr) -> oneshot::Receiver<()>,
) {
    let mut submit = |addr| {
        let completion = send(addr);
        async move { (addr, completion.await) }
    };
    let mut pending = FuturesUnordered::new();
    for _ in 0..in_flight_per_receiver {
        for &addr in targets {
            pending.push(submit(addr));
        }
    }
    while let Some((addr, result)) = pending.next().await {
        result.expect("TCP message was dropped before send completion");
        pending.push(submit(addr));
    }
}

fn run_tcp_reader(bind_addr: SocketAddr, dataplane_bandwidth_mbps: u64, metrics_addr: SocketAddr) {
    assert!(
        dataplane_bandwidth_mbps > 0,
        "dataplane_bandwidth_mbps must be greater than 0"
    );

    let mut receiver = DataplaneBuilder::new(dataplane_bandwidth_mbps)
        .with_tcp_sockets([(TcpSocketId::Raptorcast, bind_addr)])
        .build();
    assert!(
        receiver.block_until_ready(Duration::from_secs(5)),
        "receiver dataplane not ready"
    );
    start_metrics_server(receiver.metrics(), metrics_addr);

    let mut receiver_socket = receiver
        .tcp_sockets
        .take(TcpSocketId::Raptorcast)
        .expect("failed to get TCP receiver socket");
    info!(
        local_addr = %receiver_socket.local_addr(),
        dataplane_bandwidth_mbps,
        "starting TCP dataplane reader"
    );

    loop {
        let _message = block_on(receiver_socket.recv());
    }
}

fn start_tx_sampler(metrics: &ExecutorMetrics, options: TxSamplingArgs) {
    let Some(path) = options.tx_samples_csv else {
        return;
    };
    let handles = metrics.metric_handles();
    let counter = |name| {
        handles
            .iter()
            .find(|(key, _, _)| *key == name)
            .expect("dataplane send counter is registered")
            .1
            .clone()
    };
    let udp = counter("monad.dataplane.udp.total_bytes_sent");
    let tcp = counter("monad.dataplane.tcp.total_bytes_sent");
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .expect("failed to create send-counter CSV (must not already exist)");
    let interval = Duration::from_millis(options.tx_sample_interval_ms.get());
    thread::Builder::new()
        .name("throughput-samples".to_owned())
        .spawn(move || {
            let mut output = BufWriter::new(file);
            writeln!(output, "elapsed_ns,udp_sent_bytes,tcp_completed_bytes")
                .expect("failed to write sample header");
            let epoch = Instant::now();
            let mut next = epoch;
            loop {
                // Read live counters, not HTTP snapshots. TCP advances on complete
                // messages, so this remains a completion-rate trace, not a wire trace.
                let udp_bytes = udp.get();
                let tcp_bytes = tcp.get();
                if writeln!(
                    output,
                    "{},{udp_bytes},{tcp_bytes}",
                    epoch.elapsed().as_nanos()
                )
                .and_then(|_| output.flush())
                .is_err()
                {
                    warn!("failed to write send-counter sample");
                    return;
                }
                next += interval;
                let now = Instant::now();
                if next <= now {
                    next = now + interval;
                }
                thread::sleep(next.saturating_duration_since(now));
            }
        })
        .expect("failed to spawn send-counter sampler");
}

fn start_metrics_server(metrics: &ExecutorMetrics, metrics_addr: SocketAddr) {
    let registry = Registry::new();
    metrics
        .register(&registry)
        .expect("failed to register dataplane metrics");
    let listener = TcpListener::bind(metrics_addr).expect("failed to bind metrics server");
    let actual_addr = listener
        .local_addr()
        .expect("metrics server has no address");
    info!(metrics_addr = %actual_addr, "serving Prometheus metrics");

    thread::Builder::new()
        .name("throughput-metrics".to_owned())
        .spawn(move || {
            for stream in listener.incoming() {
                let mut stream = match stream {
                    Ok(stream) => stream,
                    Err(error) => {
                        warn!(?error, "metrics connection failed");
                        continue;
                    }
                };
                let mut request = [0_u8; 1024];
                if stream.read(&mut request).is_err() {
                    continue;
                }

                let mut body = Vec::new();
                let encoder = TextEncoder::new();
                if encoder.encode(&registry.gather(), &mut body).is_err() {
                    continue;
                }
                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: \
                     close\r\n\r\n",
                    encoder.format_type(),
                    body.len()
                );
                if stream.write_all(header.as_bytes()).is_ok() {
                    let _ = stream.write_all(&body);
                }
            }
        })
        .expect("failed to spawn metrics server thread");
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, collections::BTreeMap};

    use futures::FutureExt;

    use super::*;

    #[test]
    fn sampling_defaults_to_ten_ms_and_rejects_zero() {
        for command in ["nw", "tw"] {
            let args = [
                "throughput",
                command,
                "127.0.0.1:1",
                "--metrics-addr",
                "127.0.0.1:3",
                "--tx-samples-csv",
                "samples.csv",
            ];
            let options = Args::try_parse_from(args).unwrap();
            let sampling = match options.command {
                Command::NativeWriter(options) => options.sampling,
                Command::TcpWriter(options) => options.sampling,
                _ => unreachable!(),
            };
            assert_eq!(sampling.tx_sample_interval_ms.get(), 10);
            assert_eq!(sampling.tx_samples_csv, Some(PathBuf::from("samples.csv")));
            assert!(
                Args::try_parse_from(args.into_iter().chain(["--tx-sample-interval-ms", "0"]))
                    .is_err()
            );
        }
    }

    #[test]
    fn writer_arguments_parse_typed_addresses_and_keep_aliases() {
        let args = Args::try_parse_from([
            "throughput",
            "tw",
            "127.0.0.1:1,127.0.0.1:2",
            "--metrics-addr",
            "127.0.0.1:3",
            "--udp-burst-target",
            "127.0.0.1:4,127.0.0.1:5",
        ])
        .unwrap();
        let Command::TcpWriter(options) = args.command else {
            panic!("wrong command")
        };
        assert_eq!(
            options.target,
            [
                "127.0.0.1:1".parse::<SocketAddr>().unwrap(),
                "127.0.0.1:2".parse::<SocketAddr>().unwrap()
            ]
        );
        assert_eq!(
            options.metrics_addr,
            "127.0.0.1:3".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(options.udp_burst_target.len(), 2);
        assert!(Args::try_parse_from([
            "throughput",
            "tw",
            "invalid",
            "--metrics-addr",
            "127.0.0.1:3"
        ])
        .is_err());
        assert!(Args::try_parse_from(["throughput", "nw"]).is_err());
        let args = Args::try_parse_from([
            "throughput",
            "nw",
            "127.0.0.1:1",
            "--metrics-addr",
            "127.0.0.1:3",
            "--manytrace-socket",
            "/tmp/throughput-trace.sock",
        ])
        .unwrap();
        assert_eq!(
            args.manytrace_socket.as_deref(),
            Some("/tmp/throughput-trace.sock")
        );
        let Command::NativeWriter(options) = args.command else {
            panic!("wrong command")
        };
        assert_eq!(options.metrics_addr, Some("127.0.0.1:3".parse().unwrap()));
    }

    #[test]
    fn slow_peer_does_not_block_replenishing_healthy_peer() {
        let slow: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let healthy: SocketAddr = "127.0.0.1:2".parse().unwrap();
        let completions = RefCell::new(BTreeMap::<SocketAddr, Vec<oneshot::Sender<()>>>::new());
        let submitted = RefCell::new(BTreeMap::<SocketAddr, usize>::new());
        let targets = [slow, healthy];
        let mut load = Box::pin(drive_tcp_load(&targets, 2, |addr| {
            let (tx, rx) = oneshot::channel();
            completions.borrow_mut().entry(addr).or_default().push(tx);
            *submitted.borrow_mut().entry(addr).or_default() += 1;
            rx
        }));
        assert!(load.as_mut().now_or_never().is_none());
        for expected in 3..=5 {
            completions
                .borrow_mut()
                .get_mut(&healthy)
                .unwrap()
                .pop()
                .unwrap()
                .send(())
                .unwrap();
            assert!(load.as_mut().now_or_never().is_none());
            assert_eq!(submitted.borrow()[&healthy], expected);
            assert_eq!(submitted.borrow()[&slow], 2);
            assert_eq!(completions.borrow()[&healthy].len(), 2);
        }
    }
}
