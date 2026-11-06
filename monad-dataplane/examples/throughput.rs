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
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use clap::{Parser, Subcommand};
use futures::{channel::oneshot, executor::block_on, stream::FuturesUnordered, StreamExt};
use monad_dataplane::{DataplaneBuilder, TcpMsg, TcpSocketId, UdpSocketId, UnicastMsg};
use monad_executor::ExecutorMetrics;
use monad_types::UdpPriority;
use tracing::info;

const UDP_SEGMENT: i32 = 103;
const SOL_UDP: i32 = 17;
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
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    #[command(about = "run a paced TCP writer with optional UDP bursts")]
    TcpWriter(TcpWriterArgs),
    #[command(about = "count received TCP stream bytes without waiting for complete messages")]
    TcpReader {
        #[arg(long, default_value = "0.0.0.0:19999")]
        bind_addr: SocketAddr,
        #[command(flatten)]
        sampling: SamplingArgs,
    },
    #[command(alias = "w", about = "run gso-based udp writer")]
    Writer {
        #[arg(help = "target address to send packets to")]
        target: String,

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
    },
    #[command(alias = "nw", about = "run native dataplane writer")]
    NativeWriter {
        #[arg(help = "target address, or comma-separated target addresses, to send packets to")]
        target: String,

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
    },
    #[command(alias = "r", about = "run native udp reader")]
    Reader {
        #[command(flatten)]
        sampling: SamplingArgs,
        #[arg(
            long,
            default_value = "0.0.0.0:19999",
            help = "bind address for receiver"
        )]
        bind_addr: String,

        #[arg(
            short = 'm',
            long,
            default_value = "false",
            help = "use multishot ringbuf receive"
        )]
        multishot: bool,
    },
}

#[derive(clap::Args)]
struct SamplingArgs {
    #[arg(long, help = "write timestamped live byte counters to a new CSV file")]
    samples_csv: Option<PathBuf>,
    #[arg(long, default_value = "10")]
    sample_interval_ms: NonZeroU64,
}

#[derive(clap::Args)]
struct TcpWriterArgs {
    #[arg(value_delimiter = ',', required = true)]
    target: Vec<SocketAddr>,
    #[arg(long, default_value = "2097152")]
    message_size: usize,
    #[arg(long = "db", default_value = "1000")]
    dataplane_bandwidth_mbps: NonZeroU64,
    #[arg(long)]
    peer_bandwidth_mbps: Option<NonZeroU64>,
    #[arg(long, default_value = "2")]
    in_flight_per_receiver: usize,
    #[arg(long, default_value = "4")]
    udp_tx_workers: usize,
    #[arg(long, value_delimiter = ',')]
    udp_burst_target: Vec<SocketAddr>,
    #[arg(long, default_value = "high", value_parser = parse_udp_priority)]
    udp_burst_priority: UdpPriority,
    #[arg(long, default_value = "1200")]
    udp_burst_packet_size: u16,
    #[arg(long, default_value = "52428800")]
    udp_burst_bytes: usize,
    #[arg(long, default_value = "2000")]
    udp_burst_interval_ms: NonZeroU64,
    #[command(flatten)]
    sampling: SamplingArgs,
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    match args.command {
        Command::TcpWriter(options) => run_tcp_writer(options),
        Command::TcpReader {
            bind_addr,
            sampling,
        } => run_tcp_reader(bind_addr, sampling),
        Command::Writer {
            target,
            writers,
            packet_size,
            burst_size,
        } => {
            let target_addr: SocketAddr = target.parse().expect("invalid target address");
            run_writer(target_addr, writers, packet_size, burst_size);
        }
        Command::NativeWriter {
            target,
            packet_size,
            writer_bandwidth_mbps,
            dataplane_bandwidth_mbps,
            peer_bandwidth_mbps,
            tx_workers,
            batch_size,
        } => {
            let target_addrs: Vec<SocketAddr> = target
                .split(',')
                .map(|target| target.parse().expect("invalid target address"))
                .collect();
            run_native_writer(
                target_addrs,
                packet_size,
                writer_bandwidth_mbps,
                dataplane_bandwidth_mbps,
                peer_bandwidth_mbps,
                tx_workers,
                batch_size,
            );
        }
        Command::Reader {
            bind_addr,
            multishot,
            sampling,
        } => {
            let bind_addr: SocketAddr = bind_addr.parse().expect("invalid bind address");
            run_native(bind_addr, multishot, sampling);
        }
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

fn run_native_writer(
    target_addrs: Vec<SocketAddr>,
    packet_size: usize,
    writer_bandwidth_mbps: u64,
    dataplane_bandwidth_mbps: u64,
    peer_bandwidth_mbps: Option<u64>,
    tx_workers: usize,
    batch_size: usize,
) {
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

fn run_native(bind_addr: SocketAddr, multishot: bool, sampling: SamplingArgs) {
    info!(addr = %bind_addr, multishot, "starting native dataplane reader");

    let mut dataplane = DataplaneBuilder::new(10_000)
        .with_udp_multishot(multishot)
        .with_udp_sockets([(UdpSocketId::Raptorcast, bind_addr)])
        .build();

    dataplane
        .block_until_ready(Duration::from_secs(5))
        .then_some(())
        .expect("dataplane not ready");

    start_dataplane_samples(dataplane.metrics(), sampling, true);

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

fn parse_udp_priority(value: &str) -> Result<UdpPriority, &'static str> {
    match value {
        "high" => Ok(UdpPriority::High),
        "regular" => Ok(UdpPriority::Regular),
        _ => Err("priority must be high or regular"),
    }
}

fn run_tcp_writer(options: TcpWriterArgs) {
    assert!((1..=3 * 1024 * 1024).contains(&options.message_size));
    assert!(options.in_flight_per_receiver > 0);
    assert!(
        options
            .message_size
            .checked_mul(options.in_flight_per_receiver)
            .is_some_and(|bytes| bytes <= 4 * 1024 * 1024),
        "outstanding messages must fit the per-peer TCP queue"
    );
    assert!((1..=1472).contains(&options.udp_burst_packet_size));
    assert!(options.udp_burst_bytes > 0);
    for (index, target) in options.target.iter().enumerate() {
        assert!(
            !options.target[..index].contains(target),
            "duplicate TCP target"
        );
    }
    let mut builder = DataplaneBuilder::new(options.dataplane_bandwidth_mbps.get())
        .with_udp_tx_workers(options.udp_tx_workers)
        .with_tcp_sockets([(TcpSocketId::Raptorcast, "0.0.0.0:0".parse().unwrap())]);
    if let Some(peer) = options.peer_bandwidth_mbps {
        builder = builder.with_udp_peer_bandwidth_mbps(peer.get());
    }
    if !options.udp_burst_target.is_empty() {
        builder =
            builder.with_udp_sockets([(UdpSocketId::DirectUdp, "0.0.0.0:0".parse().unwrap())]);
    }
    let mut dataplane = builder.build();
    assert!(dataplane.block_until_ready(Duration::from_secs(5)));
    start_dataplane_samples(dataplane.metrics(), options.sampling, false);
    let tcp_socket = dataplane.tcp_sockets.take(TcpSocketId::Raptorcast).unwrap();

    if !options.udp_burst_target.is_empty() {
        let udp_socket = dataplane.udp_sockets.take(UdpSocketId::DirectUdp).unwrap();
        let udp_writer = udp_socket.writer().clone();
        thread::spawn(move || {
            let target_count = options.udp_burst_target.len();
            let burst = UnicastMsg {
                msgs: options
                    .udp_burst_target
                    .into_iter()
                    .enumerate()
                    .map(|(index, addr)| {
                        let bytes = options.udp_burst_bytes / target_count
                            + usize::from(index < options.udp_burst_bytes % target_count);
                        (addr, Bytes::from(vec![0; bytes]))
                    })
                    .collect(),
                stride: options.udp_burst_packet_size,
            };
            let interval = Duration::from_millis(options.udp_burst_interval_ms.get());
            let epoch = Instant::now();
            let mut next = epoch + interval;
            // Allow the TCP connections to reach steady state before the first burst.
            loop {
                thread::sleep(next.saturating_duration_since(Instant::now()));
                info!(
                    elapsed_ms = epoch.elapsed().as_millis(),
                    bytes = options.udp_burst_bytes,
                    priority = ?options.udp_burst_priority,
                    "submitting UDP burst"
                );
                udp_writer.write_unicast_with_priority(burst.clone(), options.udp_burst_priority);
                next += interval;
            }
        });
    }

    let payload = Bytes::from(vec![0; options.message_size]);
    block_on(async {
        let submit = |addr| {
            let (complete, completion) = oneshot::channel();
            tcp_socket.write_with_priority(
                addr,
                TcpMsg {
                    msg: payload.clone(),
                    completion: Some(complete),
                },
                UdpPriority::Regular,
            );
            async move { (addr, completion.await) }
        };
        let mut pending = FuturesUnordered::new();
        for &addr in &options.target {
            for _ in 0..options.in_flight_per_receiver {
                pending.push(submit(addr));
            }
        }
        while let Some((addr, result)) = pending.next().await {
            result.expect("TCP message dropped before completion");
            pending.push(submit(addr));
        }
    });
}

fn run_tcp_reader(bind_addr: SocketAddr, sampling: SamplingArgs) {
    let listener = TcpListener::bind(bind_addr).expect("failed to bind TCP reader");
    let bytes = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    start_samples(sampling, {
        let bytes = bytes.clone();
        let errors = errors.clone();
        move || {
            [
                0,
                bytes.load(Ordering::Relaxed),
                0,
                0,
                0,
                errors.load(Ordering::Relaxed),
            ]
        }
    });
    info!(local_addr = %listener.local_addr().unwrap(), "TCP stream reader ready");
    for stream in listener.incoming() {
        let mut stream = stream.expect("TCP accept failed");
        let bytes = bytes.clone();
        let errors = errors.clone();
        thread::spawn(move || {
            let mut buffer = vec![0; 64 * 1024];
            loop {
                match stream.read(&mut buffer) {
                    Ok(0) => return,
                    Ok(count) => {
                        bytes.fetch_add(count as u64, Ordering::Relaxed);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(error) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(?error, "TCP stream read failed");
                        return;
                    }
                }
            }
        });
    }
}

fn start_dataplane_samples(metrics: &ExecutorMetrics, options: SamplingArgs, receive: bool) {
    if options.samples_csv.is_none() {
        return;
    }
    let handles = metrics.metric_handles();
    let counter = |name| {
        handles
            .iter()
            .find(|(key, _, _)| *key == name)
            .expect("registered dataplane counter")
            .1
            .clone()
    };
    let udp = counter(if receive {
        "monad.dataplane.udp.total_bytes_received"
    } else {
        "monad.dataplane.udp.total_bytes_sent"
    });
    let tcp = counter(if receive {
        "monad.dataplane.tcp.total_bytes_received"
    } else {
        "monad.dataplane.tcp.total_bytes_sent"
    });
    let udp_drops = counter("monad.dataplane.udp.total_egress_messages_dropped");
    let tcp_drops = counter("monad.dataplane.tcp.total_egress_messages_dropped");
    let udp_errors = counter(if receive {
        "monad.dataplane.udp.total_receive_errors"
    } else {
        "monad.dataplane.udp.total_send_errors"
    });
    let tcp_errors = counter(if receive {
        "monad.dataplane.tcp.total_receive_errors"
    } else {
        "monad.dataplane.tcp.total_send_errors"
    });
    let connect_errors = counter("monad.dataplane.tcp.total_outbound_connection_errors");
    start_samples(options, move || {
        [
            udp.get(),
            tcp.get(),
            udp_drops.get(),
            tcp_drops.get(),
            udp_errors.get(),
            tcp_errors.get() + connect_errors.get(),
        ]
    });
}

fn start_samples(options: SamplingArgs, snapshot: impl Fn() -> [u64; 6] + Send + 'static) {
    let Some(path) = options.samples_csv else {
        return;
    };
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .expect("sample CSV must not already exist");
    let interval = Duration::from_millis(options.sample_interval_ms.get());
    thread::spawn(move || {
        let mut output = BufWriter::new(file);
        writeln!(
            output,
            "unix_ns,udp_bytes,tcp_bytes,udp_dropped,tcp_dropped,udp_errors,tcp_errors"
        )
        .unwrap();
        let mut next = Instant::now();
        loop {
            let [udp, tcp, udp_drops, tcp_drops, udp_errors, tcp_errors] = snapshot();
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            writeln!(
                output,
                "{timestamp},{udp},{tcp},{udp_drops},{tcp_drops},{udp_errors},{tcp_errors}"
            )
            .unwrap();
            output.flush().unwrap();
            next += interval;
            let now = Instant::now();
            if next <= now {
                next = now + interval;
            }
            thread::sleep(next.saturating_duration_since(now));
        }
    });
}
