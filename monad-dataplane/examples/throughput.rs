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
    net::{SocketAddr, UdpSocket},
    os::unix::io::AsRawFd,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

use bytes::Bytes;
use clap::{Parser, Subcommand};
use futures::executor::block_on;
use monad_dataplane::{DataplaneBuilder, UdpSocketConfig};
use tracing::info;

const UDP_SEGMENT: i32 = 103;
const SOL_UDP: i32 = 17;

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
#[command(about = "udp throughput test")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
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
        #[arg(help = "target address to send packets to")]
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
            default_value = "128",
            help = "number of messages to write before sleeping"
        )]
        batch_size: usize,
    },
    #[command(alias = "r", about = "run native udp reader")]
    Reader {
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

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    match args.command {
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
            batch_size,
        } => {
            let target_addr: SocketAddr = target.parse().expect("invalid target address");
            run_native_writer(
                target_addr,
                packet_size,
                writer_bandwidth_mbps,
                dataplane_bandwidth_mbps,
                batch_size,
            );
        }
        Command::Reader {
            bind_addr,
            multishot,
        } => {
            let bind_addr: SocketAddr = bind_addr.parse().expect("invalid bind address");
            run_native(bind_addr, multishot);
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
    target_addr: SocketAddr,
    packet_size: usize,
    writer_bandwidth_mbps: u64,
    dataplane_bandwidth_mbps: u64,
    batch_size: usize,
) {
    assert!(
        packet_size > 0 && packet_size <= 1472,
        "packet_size must be between 1 and 1472 bytes"
    );
    assert!(
        writer_bandwidth_mbps > 0,
        "writer_bandwidth_mbps must be greater than 0"
    );
    assert!(
        dataplane_bandwidth_mbps > 0,
        "dataplane_bandwidth_mbps must be greater than 0"
    );
    assert!(batch_size > 0, "batch_size must be greater than 0");

    let bind_addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
    info!(
        bind_addr = %bind_addr,
        target_addr = %target_addr,
        packet_size = packet_size,
        writer_bandwidth_mbps = writer_bandwidth_mbps,
        dataplane_bandwidth_mbps = dataplane_bandwidth_mbps,
        batch_size = batch_size,
        "starting native dataplane writer"
    );

    let mut dataplane = DataplaneBuilder::new(&bind_addr, dataplane_bandwidth_mbps)
        .extend_udp_sockets(vec![UdpSocketConfig {
            socket_addr: bind_addr,
            label: "writer".to_string(),
        }])
        .build();

    dataplane
        .block_until_ready(Duration::from_secs(5))
        .then_some(())
        .expect("dataplane not ready");

    let udp_socket = dataplane
        .take_udp_socket_handle("writer")
        .expect("failed to get writer socket");

    let writer = udp_socket.writer().clone();
    let payload = Bytes::from(vec![0u8; packet_size]);

    let sleep_duration_nanos =
        (packet_size as u64 * batch_size as u64 * 8 * 1_000) / writer_bandwidth_mbps;
    let sleep_duration = Duration::from_nanos(sleep_duration_nanos);

    loop {
        for _ in 0..batch_size {
            writer.write(target_addr, payload.clone(), packet_size as u16);
        }
        thread::sleep(sleep_duration);
    }
}

fn run_native(bind_addr: SocketAddr, multishot: bool) {
    info!(addr = %bind_addr, multishot, "starting native dataplane reader");

    let mut dataplane = DataplaneBuilder::new(&bind_addr, 10_000)
        .with_udp_multishot(multishot)
        .extend_udp_sockets(vec![UdpSocketConfig {
            socket_addr: bind_addr,
            label: "bench".to_string(),
        }])
        .build();

    dataplane
        .block_until_ready(Duration::from_secs(5))
        .then_some(())
        .expect("dataplane not ready");

    let mut udp_socket = dataplane
        .take_udp_socket_handle("bench")
        .expect("failed to get bench socket");

    let mut msgs_received = 0u64;
    let mut bytes_received = 0u64;
    let mut last_log = Instant::now();
    let log_interval = Duration::from_secs(1);

    loop {
        let msg = block_on(udp_socket.recv());
        msgs_received += 1;
        bytes_received += msg.payload.len() as u64;

        let now = Instant::now();
        if now.duration_since(last_log) >= log_interval {
            let elapsed = now.duration_since(last_log).as_secs_f64();
            let msgs_per_sec = msgs_received as f64 / elapsed;
            let mbps = (bytes_received as f64 * 8.0) / elapsed / 1_000_000.0;

            info!(
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
