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

use byte_unit::Byte;
use bytes::Bytes;
use clap::{Parser, Subcommand};
use futures::executor::block_on;
use io_uring::{opcode, types, IoUring};
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
    #[command(alias = "w", about = "run udp writer")]
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

        #[arg(
            long,
            default_value = "64KB",
            help = "max write size (e.g., 64KB, 128KB, 256KB)"
        )]
        max_write_size: String,
    },
    #[command(alias = "uw", about = "run io-uring writer")]
    UringWriter {
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
            long,
            default_value = "128",
            help = "number of concurrent send operations in io-uring"
        )]
        batch_size: usize,

        #[arg(long, default_value = "256", help = "io-uring queue depth")]
        ring_size: u32,

        #[arg(
            long,
            default_value = "false",
            help = "enable SQ polling to avoid syscalls"
        )]
        sqpoll: bool,

        #[arg(
            long,
            default_value = "2000",
            help = "sqpoll idle timeout in milliseconds"
        )]
        sqpoll_idle: u32,
    },
    #[command(alias = "r", about = "run udp reader")]
    Reader {
        #[arg(
            long,
            default_value = "0.0.0.0:19999",
            help = "bind address for receiver"
        )]
        bind_addr: String,

        #[command(subcommand)]
        mode: ReaderMode,
    },
}

#[derive(Subcommand)]
enum ReaderMode {
    #[command(alias = "n", about = "native mode - print throughput in receiver task")]
    Native,
    #[command(
        alias = "r",
        about = "registered buffer ring mode - use io_uring with registered buffer rings"
    )]
    Ring {
        #[arg(long, default_value = "256", help = "number of buffers in the ring")]
        buf_count: usize,
        #[arg(
            short = 'n',
            long,
            default_value = "4",
            help = "number of reuseport reader threads (1-16)"
        )]
        num_readers: usize,
    },
    #[command(
        alias = "p",
        about = "SO_REUSEPORT mode - spawn multiple threads with SO_REUSEPORT and io_uring"
    )]
    Reuseport {
        #[arg(
            short = 'n',
            long,
            default_value = "4",
            help = "number of reader threads (1-16)"
        )]
        num_readers: usize,
        #[arg(
            long,
            default_value = "128",
            help = "number of concurrent recv operations per thread"
        )]
        batch_size: usize,
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
            max_write_size,
        } => {
            let target_addr: SocketAddr = target.parse().expect("invalid target address");
            let max_write_size_bytes = Byte::parse_str(&max_write_size, true)
                .expect("invalid max_write_size format")
                .as_u64() as usize;
            run_native_writer(
                target_addr,
                packet_size,
                writer_bandwidth_mbps,
                dataplane_bandwidth_mbps,
                batch_size,
                max_write_size_bytes,
            );
        }
        Command::UringWriter {
            target,
            packet_size,
            writer_bandwidth_mbps,
            batch_size,
            ring_size,
            sqpoll,
            sqpoll_idle,
        } => {
            let target_addr: SocketAddr = target.parse().expect("invalid target address");
            run_uring_writer(
                target_addr,
                packet_size,
                writer_bandwidth_mbps,
                batch_size,
                ring_size,
                sqpoll,
                sqpoll_idle,
            );
        }
        Command::Reader { bind_addr, mode } => {
            let bind_addr: SocketAddr = bind_addr.parse().expect("invalid bind address");
            match mode {
                ReaderMode::Native => run_native(bind_addr),
                ReaderMode::Ring {
                    buf_count,
                    num_readers,
                } => run_ring(bind_addr, num_readers, buf_count),
                ReaderMode::Reuseport {
                    num_readers,
                    batch_size,
                } => run_reuseport(bind_addr, num_readers, batch_size),
            }
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
    max_write_size: usize,
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
        max_write_size = max_write_size,
        "starting native dataplane writer"
    );

    let mut dataplane = DataplaneBuilder::new(&bind_addr, dataplane_bandwidth_mbps)
        .with_udp_max_write_size(max_write_size)
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

fn run_native(bind_addr: SocketAddr) {
    info!(addr = %bind_addr, "starting native dataplane (without SO_REUSEPORT)");

    let mut dataplane = DataplaneBuilder::new(&bind_addr, 10_000)
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

fn run_ring(bind_addr: SocketAddr, num_readers: usize, _buf_count: usize) {
    info!(addr = %bind_addr, num_readers, "starting dataplane with ringbuf mode");

    let mut dataplane = DataplaneBuilder::new(&bind_addr, 10_000)
        .enable_so_reuseport(num_readers)
        .uring_udp_ringbuf(true)
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
                num_readers,
                msgs_received = msgs_received,
                msgs_per_sec = format!("{:.0}", msgs_per_sec),
                mbps = format!("{:.2}", mbps),
                "ringbuf throughput stats"
            );

            msgs_received = 0;
            bytes_received = 0;
            last_log = now;
        }
    }
}

fn run_reuseport(bind_addr: SocketAddr, num_readers: usize, _batch_size: usize) {
    info!(addr = %bind_addr, num_readers, "starting dataplane with SO_REUSEPORT");

    let mut dataplane = DataplaneBuilder::new(&bind_addr, 10_000)
        .enable_so_reuseport(num_readers)
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
                num_readers,
                msgs_received = msgs_received,
                msgs_per_sec = format!("{:.0}", msgs_per_sec),
                mbps = format!("{:.2}", mbps),
                "reuseport throughput stats"
            );

            msgs_received = 0;
            bytes_received = 0;
            last_log = now;
        }
    }
}

fn run_uring_writer(
    target_addr: SocketAddr,
    packet_size: usize,
    writer_bandwidth_mbps: u64,
    batch_size: usize,
    ring_size: u32,
    sqpoll: bool,
    sqpoll_idle: u32,
) {
    assert!(
        packet_size > 0 && packet_size <= 1472,
        "packet_size must be between 1 and 1472 bytes"
    );
    assert!(
        writer_bandwidth_mbps > 0,
        "writer_bandwidth_mbps must be greater than 0"
    );
    assert!(batch_size > 0, "batch_size must be greater than 0");
    assert!(
        batch_size <= ring_size as usize,
        "batch_size must be <= ring_size"
    );

    let socket = UdpSocket::bind("0.0.0.0:0").expect("failed to bind writer socket");
    socket
        .connect(target_addr)
        .expect("failed to connect socket");
    socket.set_nonblocking(true).unwrap();
    let socket_fd = socket.as_raw_fd();

    info!(
        target_addr = %target_addr,
        packet_size = packet_size,
        writer_bandwidth_mbps = writer_bandwidth_mbps,
        batch_size = batch_size,
        ring_size = ring_size,
        sqpoll = sqpoll,
        sqpoll_idle = sqpoll_idle,
        registered_files = 1,
        registered_buffers = batch_size,
        "starting io-uring writer with registered files and buffers"
    );

    let mut ring: IoUring = IoUring::builder()
        .setup_coop_taskrun()
        .build(ring_size)
        .expect("failed to create io-uring");

    let buffers: Vec<Vec<u8>> = (0..batch_size).map(|_| vec![0u8; packet_size]).collect();

    let buffer_iovecs: Vec<libc::iovec> = buffers
        .iter()
        .map(|buf| libc::iovec {
            iov_base: buf.as_ptr() as *mut std::ffi::c_void,
            iov_len: buf.len(),
        })
        .collect();

    let submitter = ring.submitter();
    submitter
        .register_files(&[socket_fd])
        .expect("failed to register files");
    unsafe {
        submitter
            .register_buffers(&buffer_iovecs)
            .expect("failed to register buffers");
    }

    let (submitter, mut sq, mut cq) = ring.split();

    let mut msgs_sent = 0u64;
    let mut bytes_sent = 0u64;
    let mut last_log = Instant::now();
    let log_interval = Duration::from_secs(1);

    let mut in_flight = 0;
    let mut buf_idx = 0;

    loop {
        // Fill SQ
        while in_flight < batch_size {
            let send_e = opcode::Send::new(
                types::Fixed(0),
                buffers[buf_idx].as_ptr(),
                buffers[buf_idx].len() as u32,
            )
            .build()
            .user_data(buf_idx as u64);

            unsafe {
                if sq.push(&send_e).is_err() {
                    break;
                }
            }

            in_flight += 1;
            buf_idx = (buf_idx + 1) % batch_size;
        }

        sq.sync();

        if sqpoll {
            match submitter.submit() {
                Ok(_) => {}
                Err(e) => {
                    info!(error = %e, "submit error");
                }
            }
        } else {
            // If we have nothing in flight and nothing to submit, we must wait.
            // But here we always try to keep in_flight > 0.
            // If in_flight is full, we should wait for at least one.
            let wait_nr = if in_flight == batch_size { 1 } else { 0 };
            match submitter.submit_and_wait(wait_nr) {
                Ok(_) => {}
                Err(e) => {
                    info!(error = %e, "submit_and_wait error");
                }
            }
        }

        cq.sync();
        let mut completed = 0;
        for cqe in &mut cq {
            completed += 1;
            let ret = cqe.result();

            if ret < 0 {
                // -105 is ENOBUFS, which can happen if we overrun the NIC/socket buffer
                if ret != -105 {
                    info!(error = ret, "send error");
                }
            } else {
                msgs_sent += 1;
                bytes_sent += ret as u64;
            }
        }
        in_flight -= completed;

        // If we didn't complete anything and we are full, we need to yield/wait to avoid busy loop
        if completed == 0 && in_flight == batch_size {
            if !sqpoll {
                // submit_and_wait(1) above should handle this, but just in case
            } else {
                thread::yield_now();
            }
        }

        let now = Instant::now();
        if now.duration_since(last_log) >= log_interval {
            let elapsed = now.duration_since(last_log).as_secs_f64();
            let msgs_per_sec = msgs_sent as f64 / elapsed;
            let mbps = (bytes_sent as f64 * 8.0) / elapsed / 1_000_000.0;

            info!(
                msgs_sent = msgs_sent,
                msgs_per_sec = format!("{:.0}", msgs_per_sec),
                mbps = format!("{:.2}", mbps),
                "uring writer throughput"
            );

            msgs_sent = 0;
            bytes_sent = 0;
            last_log = now;
        }
    }
}
