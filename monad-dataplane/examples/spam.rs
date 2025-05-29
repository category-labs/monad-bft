use std::{
    collections::HashMap,
    net::SocketAddr,
    thread,
    time::{Instant, SystemTime},
};

use bytes::{Bytes, BytesMut};
use clap::Parser;
use futures::executor::block_on;
use governor::{Quota, RateLimiter};
use hdrhistogram::Histogram;
use monad_dataplane::{udp::DEFAULT_SEGMENT_SIZE, Dataplane, DataplaneBuilder, TcpMsg, UnicastMsg};
use rand::Rng;
use tracing::{debug, info, info_span, level_filters::LevelFilter, warn, Instrument};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
struct Args {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Clone, Parser)]
struct SenderArgs {
    #[clap(
        short,
        long,
        default_value_t = 1,
        help = "number of threads to use",
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    threads: u64,
    #[clap(
        short,
        long,
        value_parser = parse_bytes,
        help = "rate in bytes (not bits) per second (e.g., 100kb, 1mb, 1gb). rate is defined per thread",
        default_value = "10mb",
    )]
    rate: u64,
    #[clap(
        short,
        long,
        value_parser = parse_bytes,
        default_value = "64kb",
        help = "size of each message in bytes. rate will be adjusted appropriately",
    )]
    message_size: u64,
    #[clap(long, default_value_t = 128 << 10, help = "size of the kernel buffer in bytes")]
    buffer_size: u64,
    #[clap(
        long,
        default_value = "0.0.0.0:6060",
        help = "address to bind to"
    )]
    receiver: SocketAddr,
}

#[derive(Debug, Clone, Parser)]
struct ReceiverArgs {
    #[clap(
        short,
        long,
        default_value = "0.0.0.0:6060",
        help = "address to bind to"
    )]
    bind: SocketAddr,
    #[clap(long, default_value_t = 128 << 10, help = "size of the kernel buffer in bytes")]
    buffer_size: u64,
}

impl ReceiverArgs {
    fn dataplane(&self) -> Dataplane {
        DataplaneBuilder::new(&self.bind, 1000)
            .with_buffer_size(self.buffer_size as usize)
            .build()
    }
}

#[derive(Debug, Clone, Parser)]
enum Command {
    #[clap(name = "udp-recv", alias = "ur")]
    UdpReceiver(ReceiverArgs),
    #[clap(name = "udp-send", alias = "us")]
    UdpSender(SenderArgs),
    #[clap(name = "tcp-recv", alias = "tr")]
    TcpReceiver(ReceiverArgs),
    #[clap(name = "tcp-send", alias = "ts")]
    TcpSender(SenderArgs),
}

fn main() -> Result<(), String> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();
    let args = Args::parse();
    match args.command {
        Command::UdpReceiver(args) => receiver::<UdpProtocol>(args),
        Command::UdpSender(args) => sender::<UdpProtocol>(args),
        Command::TcpReceiver(args) => receiver::<TcpProtocol>(args),
        Command::TcpSender(args) => sender::<TcpProtocol>(args),
    }
}

async fn sender_fut<W: Protocol>(
    dp: &mut W,
    receiver: SocketAddr,
    rate: u32,
    msg_size: u32,
) -> Result<(), String> {
    info!(
        "sending to {} at rate {} msg/s with msg size {}",
        receiver, rate, msg_size
    );

    let rate_limiter = RateLimiter::direct(Quota::per_second(rate.try_into().unwrap()));
    let mut reuse = vec![0u8; msg_size as usize];
    rand::thread_rng()
        .try_fill(&mut reuse[..])
        .expect("filled with random bytes");
    let mut sent_bytes = 0;
    let mut last_reported = Instant::now();

    loop {
        rate_limiter.until_ready().await;
        let sent = SystemTime::now();
        let unix_timestamp = sent
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos() as u64;

        reuse[0..8].copy_from_slice(&unix_timestamp.to_le_bytes());
        let msg = Bytes::copy_from_slice(&reuse);

        dp.write(receiver, msg);
        sent_bytes += msg_size;
        if last_reported.elapsed().as_secs() >= 10 {
            info!("rate {rate}", rate = sent_bytes / 10);
            sent_bytes = 0;
            last_reported = Instant::now();
        }
    }
}

trait Protocol {
    fn new(dataplane: Dataplane) -> Self
    where
        Self: Sized;
    fn write(&mut self, addr: SocketAddr, msg: Bytes);
    async fn read(&mut self) -> (SocketAddr, Bytes);
}

struct TcpProtocol {
    dataplane: Dataplane,
}

impl Protocol for TcpProtocol {
    fn new(dataplane: Dataplane) -> Self {
        Self { dataplane }
    }

    fn write(&mut self, addr: SocketAddr, msg: Bytes) {
        self.dataplane.tcp_write(
            addr,
            TcpMsg {
                msg,
                completion: None,
            },
        );
    }

    async fn read(&mut self) -> (SocketAddr, Bytes) {
        self.dataplane.tcp_read().await
    }
}

struct UdpProtocol {
    dataplane: Dataplane,
    pending_messages: HashMap<SocketAddr, HashMap<u32, MessageBuffer>>,
    next_msg_id: u32,
}

struct MessageBuffer {
    total_chunks: u16,
    received_chunks: HashMap<u16, Bytes>,
}

impl Protocol for UdpProtocol {
    fn new(dataplane: Dataplane) -> Self {
        Self {
            dataplane,
            pending_messages: HashMap::new(),
            next_msg_id: 0,
        }
    }

    fn write(&mut self, addr: SocketAddr, msg: Bytes) {
        let msg_id = self.next_msg_id;
        self.next_msg_id = self.next_msg_id.wrapping_add(1);

        let header_size = 6;
        let first_chunk_header_size = 8;

        let first_chunk_size = DEFAULT_SEGMENT_SIZE as usize - first_chunk_header_size;
        let regular_chunk_size = DEFAULT_SEGMENT_SIZE as usize - header_size;

        let remaining_after_first = if msg.len() > first_chunk_size {
            msg.len() - first_chunk_size
        } else {
            0
        };
        let additional_chunks = if remaining_after_first > 0 {
            remaining_after_first.div_ceil(regular_chunk_size)
        } else {
            0
        };
        let total_chunks = (1 + additional_chunks) as u16;

        let mut msgs = Vec::new();
        let mut remaining_data = &msg[..];

        for chunk_id in 0..total_chunks {
            let chunk_data = if chunk_id == 0 {
                let chunk_size = first_chunk_size.min(remaining_data.len());
                let chunk = &remaining_data[..chunk_size];
                remaining_data = &remaining_data[chunk_size..];

                let mut buffer = BytesMut::with_capacity(first_chunk_header_size + chunk.len());
                buffer.extend_from_slice(&msg_id.to_be_bytes());
                buffer.extend_from_slice(&chunk_id.to_be_bytes());
                buffer.extend_from_slice(&total_chunks.to_be_bytes());
                buffer.extend_from_slice(chunk);
                buffer.freeze()
            } else {
                let chunk_size = regular_chunk_size.min(remaining_data.len());
                let chunk = &remaining_data[..chunk_size];
                remaining_data = &remaining_data[chunk_size..];

                let mut buffer = BytesMut::with_capacity(header_size + chunk.len());
                buffer.extend_from_slice(&msg_id.to_be_bytes());
                buffer.extend_from_slice(&chunk_id.to_be_bytes());
                buffer.extend_from_slice(chunk);
                buffer.freeze()
            };

            msgs.push((addr, chunk_data));
        }

        self.dataplane.udp_write_unicast(UnicastMsg {
            stride: DEFAULT_SEGMENT_SIZE,
            msgs,
        });
    }

    async fn read(&mut self) -> (SocketAddr, Bytes) {
        loop {
            let recv_msg = self.dataplane.udp_read().await;
            let src_addr = recv_msg.src_addr;
            let payload = recv_msg.payload;

            if payload.len() < 6 {
                warn!(
                    "dropping invalid message from {}: too short {}",
                    src_addr,
                    payload.len()
                );
                continue;
            }

            let msg_id = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
            let chunk_id = u16::from_be_bytes([payload[4], payload[5]]);

            let (data, total_chunks) = if chunk_id == 0 {
                if payload.len() < 8 {
                    warn!("dropping invalid chunk 0 from {}: too short", src_addr);
                    continue;
                }
                let total_chunks = u16::from_be_bytes([payload[6], payload[7]]);
                let data = Bytes::copy_from_slice(&payload[8..]);
                (data, Some(total_chunks))
            } else {
                let data = Bytes::copy_from_slice(&payload[6..]);
                (data, None)
            };

            debug!(
                "received chunk {} for message {} from {}",
                chunk_id, msg_id, src_addr
            );

            let addr_messages = self.pending_messages.entry(src_addr).or_default();
            let msg_buffer = addr_messages
                .entry(msg_id)
                .or_insert_with(|| MessageBuffer {
                    total_chunks: 0,
                    received_chunks: HashMap::new(),
                });

            if let Some(total) = total_chunks {
                msg_buffer.total_chunks = total;
            }

            if msg_buffer.total_chunks > 0 && chunk_id >= msg_buffer.total_chunks {
                warn!(
                    "invalid chunk {} for message {} from {} (total_chunks: {})",
                    chunk_id, msg_id, src_addr, msg_buffer.total_chunks
                );
                continue;
            }

            msg_buffer.received_chunks.insert(chunk_id, data);

            if msg_buffer.received_chunks.len() == msg_buffer.total_chunks as usize {
                let mut complete_data = BytesMut::new();
                for i in 0..msg_buffer.total_chunks {
                    if let Some(chunk_data) = msg_buffer.received_chunks.remove(&i) {
                        complete_data.extend_from_slice(&chunk_data);
                    }
                }

                addr_messages.remove(&msg_id);
                if addr_messages.is_empty() {
                    self.pending_messages.remove(&src_addr);
                }
                return (src_addr, complete_data.freeze());
            }
        }
    }
}

fn sender<DP: Protocol>(args: SenderArgs) -> Result<(), String> {
    thread::scope(|s| {
        for thread_id in 0..args.threads {
            let addr = "0.0.0.0:0"
                .parse()
                .map_err(|e| format!("Failed to parse socket address: {}", e))?;
            let buffer_size = args.buffer_size;
            let rate_limit = args.rate / args.message_size;
            let msg_size = args.message_size;
            let receiver = args.receiver;

            s.spawn(move || {
                let dp = DataplaneBuilder::new(&addr, 1000)
                    .with_buffer_size(buffer_size as usize)
                    .build();
                let mut dp = DP::new(dp);
                let fut = sender_fut(&mut dp, receiver, rate_limit as u32, msg_size as u32)
                    .instrument(info_span!("sender", thread_id));
                block_on(fut).expect("sender failed");
            });
        }
        Ok(())
    })
}

async fn receiver_fut(dp: &mut impl Protocol) -> Result<(), String> {
    info!("started");

    let mut hist = Histogram::<u64>::new(3).unwrap();
    let mut last_reported = Instant::now();
    let mut received_bytes = 0;
    let mut msgs = 0;
    loop {
        let (_, msg) = dp.read().await;
        received_bytes += msg.len();
        msgs += 1;

        if msg.len() >= 8 {
            let send_time_nanos = u64::from_le_bytes(msg[0..8].try_into().unwrap());
            let now_nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            hist.record(now_nanos.saturating_sub(send_time_nanos))
                .expect("no record error");
        }
        let interval = last_reported.elapsed().as_secs();
        if interval >= 10 {
            info!(
                "receiver rate {rate} bytes/s {msgs} msgs/s over {interval}, latency p50: {p50}ns, p99: {p99}ns",
                rate = received_bytes as u64 / interval,
                msgs = msgs / interval,
                p50 = hist.value_at_quantile(0.5),
                p99 = hist.value_at_quantile(0.99)
            );
            msgs = 0;
            received_bytes = 0;
            last_reported = Instant::now();
            hist.reset();
        }
    }
}

fn receiver<DP: Protocol>(args: ReceiverArgs) -> Result<(), String> {
    let dp = args.dataplane();
    let mut dp = DP::new(dp);
    let fut = receiver_fut(&mut dp).instrument(info_span!("receiver"));
    block_on(fut)
}

fn parse_bytes(value: &str) -> Result<u64, String> {
    let value = value.trim().to_lowercase();

    let split_pos = value
        .find(|c: char| !c.is_ascii_digit())
        .ok_or_else(|| "No units provided".to_string())?;

    let (num_str, unit_str) = value.split_at(split_pos);

    let base_value: u64 = num_str
        .parse()
        .map_err(|_| format!("Failed to parse number: {}", num_str))?;

    match unit_str.trim() {
        "b" => Ok(base_value),
        "kb" => Ok(base_value << 10),
        "mb" => Ok(base_value << 20),
        "gb" => Ok(base_value << 30),
        _ => Err(format!("Unknown unit: {}", unit_str)),
    }
}
