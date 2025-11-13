use std::{
    hint::black_box,
    mem,
    net::{SocketAddr, UdpSocket},
    os::unix::io::AsRawFd,
    sync::{
        atomic::{AtomicU16, AtomicU64, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

use bytes::BytesMut;
use clap::{Parser, Subcommand};
use futures::stream::{FuturesUnordered, StreamExt};
use io_uring::{opcode, types, IoUring};
use monoio::{net::udp::UdpSocket as MonoioUdpSocket, IoUringDriver, RuntimeBuilder};
use tokio::sync::mpsc;
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

#[repr(C)]
struct RecvBuffer {
    buffer: [u8; 1500],
    addr: libc::sockaddr_storage,
    iov: libc::iovec,
    msg: libc::msghdr,
}

impl RecvBuffer {
    fn new() -> Self {
        let mut buf = Self {
            buffer: [0u8; 1500],
            addr: unsafe { mem::zeroed() },
            iov: libc::iovec {
                iov_base: std::ptr::null_mut(),
                iov_len: 0,
            },
            msg: unsafe { mem::zeroed() },
        };

        buf.iov.iov_base = buf.buffer.as_mut_ptr() as *mut _;
        buf.iov.iov_len = buf.buffer.len();

        buf.msg.msg_name = &mut buf.addr as *mut _ as *mut _;
        buf.msg.msg_namelen = mem::size_of::<libc::sockaddr_storage>() as u32;
        buf.msg.msg_iov = &mut buf.iov as *mut _;
        buf.msg.msg_iovlen = 1;

        buf
    }

    fn reset(&mut self) {
        self.iov.iov_base = self.buffer.as_mut_ptr() as *mut _;
        self.iov.iov_len = self.buffer.len();

        self.msg.msg_name = &mut self.addr as *mut _ as *mut _;
        self.msg.msg_namelen = mem::size_of::<libc::sockaddr_storage>() as u32;
        self.msg.msg_iov = &mut self.iov as *mut _;
        self.msg.msg_iovlen = 1;
    }
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

        #[arg(long, default_value = "1", help = "number of concurrent sender threads")]
        writers: usize,

        #[arg(long, default_value = "1", help = "packet size in bytes")]
        packet_size: usize,

        #[arg(long, default_value = "32", help = "burst size (number of packets per GSO send)")]
        burst_size: usize,
    },
    #[command(alias = "r", about = "run udp reader")]
    Reader {
        #[arg(long, default_value = "0.0.0.0:19999", help = "bind address for receiver")]
        bind_addr: String,

        #[command(subcommand)]
        mode: ReaderMode,
    },
}

#[derive(Subcommand)]
enum ReaderMode {
    #[command(alias = "n", about = "native mode - print throughput in receiver task")]
    Native,
    #[command(alias = "c", about = "channel mode - send to channel and print throughput after receiving")]
    Channel,
    #[command(alias = "b", about = "batch mode - spawn multiple recv futures concurrently")]
    Batch {
        #[arg(long, default_value = "64", help = "number of concurrent recv operations")]
        batch_size: usize,
    },
    #[command(alias = "u", about = "io_uring mode - use io_uring directly")]
    Uring {
        #[arg(long, default_value = "128", help = "number of concurrent recv operations")]
        batch_size: usize,
    },
    #[command(alias = "r", about = "registered buffer ring mode - use io_uring with registered buffer rings")]
    Ring {
        #[arg(long, default_value = "256", help = "number of buffers in the ring")]
        buf_count: usize,
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
        Command::Reader { bind_addr, mode } => {
            let bind_addr: SocketAddr = bind_addr.parse().expect("invalid bind address");
            match mode {
                ReaderMode::Native => run_native(bind_addr),
                ReaderMode::Channel => run_channel(bind_addr),
                ReaderMode::Batch { batch_size } => run_batch(bind_addr, batch_size),
                ReaderMode::Uring { batch_size } => run_uring(bind_addr, batch_size),
                ReaderMode::Ring { buf_count } => run_ring(bind_addr, buf_count),
            }
        }
    }
}

fn run_writer(target_addr: SocketAddr, num_writers: usize, packet_size: usize, burst_size: usize) {
    let msgs_sent = Arc::new(AtomicU64::new(0));

    let mut writers = Vec::new();

    for writer_id in 0..num_writers {
        let msgs_sent_clone = msgs_sent.clone();

        let writer = thread::spawn(move || {
            let socket = UdpSocket::bind("0.0.0.0:0").expect("failed to bind writer socket");
            socket.set_nonblocking(true).unwrap();

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

fn run_native(bind_addr: SocketAddr) {
    RuntimeBuilder::<IoUringDriver>::new()
        .build()
        .expect("failed to build monoio runtime")
        .block_on(async move {
            let socket = MonoioUdpSocket::bind(bind_addr).expect("failed to bind socket");
            let local_addr = socket.local_addr().expect("failed to get local addr");

            info!(addr = %local_addr, "socket ready");

            monoio::spawn(async move {
                let mut msgs_received = 0u64;
                let mut bytes_received = 0u64;
                let mut last_log = Instant::now();
                let log_interval = Duration::from_secs(1);

                loop {
                    let buf = BytesMut::with_capacity(1500);
                    match socket.recv_from(buf).await {
                        (Ok((len, _src_addr)), _buf) => {
                            msgs_received += 1;
                            bytes_received += len as u64;

                            let now = Instant::now();
                            if now.duration_since(last_log) >= log_interval {
                                let elapsed = now.duration_since(last_log).as_secs_f64();
                                let msgs_per_sec = msgs_received as f64 / elapsed;
                                let mbps = (bytes_received as f64 * 8.0) / elapsed / 1_000_000.0;

                                info!(
                                    msgs_received = msgs_received,
                                    msgs_per_sec = format!("{:.0}", msgs_per_sec),
                                    mbps = format!("{:.2}", mbps),
                                    "throughput stats"
                                );

                                msgs_received = 0;
                                bytes_received = 0;
                                last_log = now;
                            }
                        }
                        (Err(e), _) => {
                            eprintln!("recv error: {}", e);
                            break;
                        }
                    }
                }
            })
            .await;
        });
}

fn run_channel(bind_addr: SocketAddr) {
    RuntimeBuilder::<IoUringDriver>::new()
        .build()
        .expect("failed to build monoio runtime")
        .block_on(async move {
            let socket = MonoioUdpSocket::bind(bind_addr).expect("failed to bind socket");
            let local_addr = socket.local_addr().expect("failed to get local addr");

            info!(addr = %local_addr, "socket ready");

            let (tx, mut rx) = mpsc::channel::<(usize, SocketAddr)>(12_800);

            monoio::spawn(async move {
                loop {
                    let buf = BytesMut::with_capacity(1500);
                    match socket.recv_from(buf).await {
                        (Ok((len, src_addr)), _buf) => {
                            if tx.send((len, src_addr)).await.is_err() {
                                break;
                            }
                        }
                        (Err(e), _) => {
                            eprintln!("recv error: {}", e);
                            break;
                        }
                    }
                }
            });

            let mut msgs_received = 0u64;
            let mut bytes_received = 0u64;
            let mut last_log = Instant::now();
            let log_interval = Duration::from_secs(1);

            loop {
                if let Some((len, _src_addr)) = rx.recv().await {
                    msgs_received += 1;
                    bytes_received += len as u64;

                    let now = Instant::now();
                    if now.duration_since(last_log) >= log_interval {
                        let elapsed = now.duration_since(last_log).as_secs_f64();
                        let msgs_per_sec = msgs_received as f64 / elapsed;
                        let mbps = (bytes_received as f64 * 8.0) / elapsed / 1_000_000.0;

                        info!(
                            msgs_received = msgs_received,
                            msgs_per_sec = format!("{:.0}", msgs_per_sec),
                            mbps = format!("{:.2}", mbps),
                            "channel throughput stats"
                        );

                        msgs_received = 0;
                        bytes_received = 0;
                        last_log = now;
                    }
                } else {
                    break;
                }
            }
        });
}

fn run_batch(bind_addr: SocketAddr, batch_size: usize) {
    RuntimeBuilder::<IoUringDriver>::new()
        .build()
        .expect("failed to build monoio runtime")
        .block_on(async move {
            let socket = MonoioUdpSocket::bind(bind_addr).expect("failed to bind socket");
            let local_addr = socket.local_addr().expect("failed to get local addr");

            info!(addr = %local_addr, batch_size = batch_size, "socket ready with batch mode");

            monoio::spawn(async move {
                let mut msgs_received = 0u64;
                let mut bytes_received = 0u64;
                let mut last_log = Instant::now();
                let log_interval = Duration::from_secs(1);

                let mut pending = FuturesUnordered::new();

                for _ in 0..batch_size {
                    let buf = BytesMut::with_capacity(1500);
                    pending.push(socket.recv_from(buf));
                }

                while let Some((result, _buf)) = pending.next().await {
                    match result {
                        Ok((len, _src_addr)) => {
                            msgs_received += 1;
                            bytes_received += len as u64;

                            let buf = BytesMut::with_capacity(1500);
                            pending.push(socket.recv_from(buf));

                            let now = Instant::now();
                            if now.duration_since(last_log) >= log_interval {
                                let elapsed = now.duration_since(last_log).as_secs_f64();
                                let msgs_per_sec = msgs_received as f64 / elapsed;
                                let mbps = (bytes_received as f64 * 8.0) / elapsed / 1_000_000.0;

                                info!(
                                    msgs_received = msgs_received,
                                    msgs_per_sec = format!("{:.0}", msgs_per_sec),
                                    mbps = format!("{:.2}", mbps),
                                    "batch throughput stats"
                                );

                                msgs_received = 0;
                                bytes_received = 0;
                                last_log = now;
                            }
                        }
                        Err(e) => {
                            eprintln!("recv error: {}", e);
                            break;
                        }
                    }
                }
            })
            .await;
        });
}

fn run_uring(bind_addr: SocketAddr, batch_size: usize) {
    let socket = UdpSocket::bind(bind_addr).expect("failed to bind socket");
    let local_addr = socket.local_addr().expect("failed to get local addr");
    let fd = socket.as_raw_fd();

    info!(addr = %local_addr, batch_size = batch_size, "socket ready with io_uring");

    let mut ring = IoUring::new(batch_size as u32).expect("failed to create io_uring");

    let mut buffers: Vec<RecvBuffer> = (0..batch_size).map(|_| RecvBuffer::new()).collect();

    let mut msgs_received = 0u64;
    let mut bytes_received = 0u64;
    let mut last_log = Instant::now();
    let log_interval = Duration::from_secs(1);

    for i in 0..batch_size {
        let msg_ptr = &mut buffers[i].msg as *mut libc::msghdr;

        let recv_e = opcode::RecvMsg::new(types::Fd(fd), msg_ptr)
            .build()
            .user_data(i as u64);

        unsafe {
            if ring.submission().push(&recv_e).is_err() {
                panic!("failed to push recv operation");
            }
        }
    }

    ring.submit().expect("failed to submit");

    loop {
        ring.submit_and_wait(1).expect("failed to submit and wait");

        let cqes: Vec<_> = ring.completion().collect();

        for cqe in cqes {
            let result = cqe.result();
            let user_data = cqe.user_data() as usize;

            if result > 0 {
                msgs_received += 1;
                bytes_received += result as u64;

                buffers[user_data].reset();
                let msg_ptr = &mut buffers[user_data].msg as *mut libc::msghdr;

                let recv_e = opcode::RecvMsg::new(types::Fd(fd), msg_ptr)
                    .build()
                    .user_data(user_data as u64);

                unsafe {
                    if ring.submission().push(&recv_e).is_err() {
                        eprintln!("failed to push recv operation");
                    }
                }
            } else if result < 0 {
                eprintln!("recv error: {}", std::io::Error::from_raw_os_error(-result));
            }

            let now = Instant::now();
            if now.duration_since(last_log) >= log_interval {
                let elapsed = now.duration_since(last_log).as_secs_f64();
                let msgs_per_sec = msgs_received as f64 / elapsed;
                let mbps = (bytes_received as f64 * 8.0) / elapsed / 1_000_000.0;

                info!(
                    msgs_received = msgs_received,
                    msgs_per_sec = format!("{:.0}", msgs_per_sec),
                    mbps = format!("{:.2}", mbps),
                    "io_uring throughput stats"
                );

                msgs_received = 0;
                bytes_received = 0;
                last_log = now;
            }
        }
    }
}

fn run_ring(bind_addr: SocketAddr, buf_count: usize) {
    use io_uring::register::Probe;

    let socket = UdpSocket::bind(bind_addr).expect("failed to bind socket");
    let local_addr = socket.local_addr().expect("failed to get local addr");
    let fd = socket.as_raw_fd();

    info!(addr = %local_addr, buf_count = buf_count, "socket ready with registered buffer ring");

    let mut ring = IoUring::new(256).expect("failed to create io_uring");

    let mut probe = Probe::new();
    ring.submitter().register_probe(&mut probe).expect("failed to register probe");
    
    if !probe.is_supported(opcode::RecvMulti::CODE) {
        eprintln!("RecvMulti not supported on this kernel (requires 6.0+)");
        return;
    }

    let buf_group_id = 0xbeef;
    let ring_entries = buf_count.next_power_of_two() as u16;
    let buf_len = 1500;

    let entry_size = mem::size_of::<types::BufRingEntry>();
    let ring_size = entry_size * ring_entries as usize;

    let ring_mem = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            ring_size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_ANONYMOUS | libc::MAP_SHARED | libc::MAP_POPULATE,
            -1,
            0,
        )
    };

    if ring_mem == libc::MAP_FAILED {
        eprintln!("mmap failed");
        return;
    }

    let buf_ring_ptr = ring_mem as *mut types::BufRingEntry;

    let mut buffers: Vec<Vec<u8>> = (0..buf_count)
        .map(|_| vec![0u8; buf_len])
        .collect();

    for (bid, buf) in buffers.iter_mut().enumerate() {
        let ring_idx = (bid as u16) & (ring_entries - 1);
        let entry = unsafe { &mut *buf_ring_ptr.add(ring_idx as usize) };
        entry.set_addr(buf.as_mut_ptr() as u64);
        entry.set_len(buf_len as u32);
        entry.set_bid(bid as u16);
    }

    let shared_tail = unsafe { types::BufRingEntry::tail(buf_ring_ptr) } as *const AtomicU16;
    unsafe {
        (*shared_tail).store(buf_count as u16, Ordering::Release);
    }

    unsafe {
        ring.submitter()
            .register_buf_ring_with_flags(buf_ring_ptr as u64, ring_entries, buf_group_id, 0)
            .expect("failed to register buffer ring");
    }

    info!("registered buffer ring with {} buffers", buf_count);

    let recv_e = opcode::RecvMulti::new(types::Fd(fd), buf_group_id)
        .build()
        .user_data(0x99);

    unsafe {
        ring.submission().push(&recv_e).expect("failed to push");
    }

    ring.submit().expect("failed to submit");

    let mut msgs_received = 0u64;
    let mut bytes_received = 0u64;
    let mut last_log = Instant::now();
    let log_interval = Duration::from_secs(1);

    loop {
        match ring.submit_and_wait(1) {
            Ok(_) => {}
            Err(e) => {
                eprintln!("submit_and_wait error: {}", e);
                break;
            }
        }

        let cqes: Vec<_> = ring.completion().collect();

        for cqe in cqes {
            let result = cqe.result();

            if result > 0 {
                msgs_received += 1;
                bytes_received += result as u64;

                let bid = io_uring::cqueue::buffer_select(cqe.flags()).unwrap();

                let data_len = result as usize;
                let mut owned_buf = BytesMut::with_capacity(data_len);
                unsafe {
                    owned_buf.set_len(data_len);
                    std::ptr::copy_nonoverlapping(
                        buffers[bid as usize].as_ptr(),
                        owned_buf.as_mut_ptr(),
                        data_len,
                    );
                }
                black_box(owned_buf);

                let ring_idx = bid & (ring_entries - 1);
                let entry = unsafe { &mut *buf_ring_ptr.add(ring_idx as usize) };
                entry.set_addr(buffers[bid as usize].as_mut_ptr() as u64);
                entry.set_len(buf_len as u32);
                entry.set_bid(bid);

                let local_tail = unsafe { (*shared_tail).load(Ordering::Acquire) };
                unsafe {
                    (*shared_tail).store(local_tail.wrapping_add(1), Ordering::Release);
                }
            } else if result < 0 && result != -libc::ENOBUFS {
                eprintln!("recv error: {}", std::io::Error::from_raw_os_error(-result));
            }

            if !io_uring::cqueue::more(cqe.flags()) {
                let recv_e = opcode::RecvMulti::new(types::Fd(fd), buf_group_id)
                    .build()
                    .user_data(0x99);

                unsafe {
                    ring.submission().push(&recv_e).expect("failed to push");
                }
            }

            let now = Instant::now();
            if now.duration_since(last_log) >= log_interval {
                let elapsed = now.duration_since(last_log).as_secs_f64();
                let msgs_per_sec = msgs_received as f64 / elapsed;
                let mbps = (bytes_received as f64 * 8.0) / elapsed / 1_000_000.0;

                info!(
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
}
