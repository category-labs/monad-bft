use std::{
    net::{SocketAddr, UdpSocket},
    os::unix::io::AsRawFd,
    time::{Duration, Instant, SystemTime},
};

use byte_unit::Byte;
use clap::{Parser, Subcommand};
use secp256k1::rand::{rngs::StdRng, Rng, SeedableRng};
use tracing::info;
use zerocopy::IntoBytes;

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

fn parse_duration(s: &str) -> Result<Duration, String> {
    humantime::parse_duration(s).map_err(|e| e.to_string())
}

fn parse_size(s: &str) -> Result<usize, String> {
    let byte = Byte::parse_str(s, true).map_err(|e| e.to_string())?;
    Ok(byte.as_u64() as usize)
}

#[derive(Parser)]
#[command(name = "spam")]
#[command(about = "spam wireauth packets at specified rate")]
#[command(after_help = "Examples:
  spam hs 127.0.0.1:9000 0445bad356c9ab26d80fa5c7b69f3d615eec352067751687a0af3491b9b7f44c0cfa91fb4e738378f1558025ff8f32e38cd910c43573836dc39b0e4a129a8d1476
  spam handshake 10.0.0.1:8080 02f3d2b8c9a7e4f1d6a5b3c8e7f2a9d4c6b1e8f5a2d7c3b9e4f1a6d8c2b5e9f3a7 --rate 5000
  spam d 127.0.0.1:9000
  spam data 192.168.1.100:7000 --rate 256MB")]
struct Args {
    #[command(subcommand)]
    command: SpamCommand,
}

#[derive(Subcommand)]
enum SpamCommand {
    #[command(alias = "hs", about = "spam handshake initiation packets")]
    #[command(after_help = "Examples:
  spam hs 127.0.0.1:9000 0445bad356c9ab26d80fa5c7b69f3d615eec352067751687a0af3491b9b7f44c0cfa91fb4e738378f1558025ff8f32e38cd910c43573836dc39b0e4a129a8d1476
  spam handshake 10.0.0.1:8080 02f3d2b8c9a7e4f1d6a5b3c8e7f2a9d4c6b1e8f5a2d7c3b9e4f1a6d8c2b5e9f3a7 --rate 5000
  spam hs 192.168.1.100:7000 03a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2 --rate 1000 --interval 500ms")]
    Handshake {
        #[arg(help = "peer address to handshake with")]
        peer: SocketAddr,

        #[arg(help = "peer public key (hex-encoded secp256k1 public key)")]
        peer_public_key: String,

        #[arg(long, default_value = "2000", help = "handshakes per interval")]
        rate: u64,

        #[arg(
            long,
            default_value = "1s",
            value_parser = parse_duration,
            help = "measurement interval (e.g., 1s, 100ms)"
        )]
        interval: Duration,
    },
    #[command(alias = "d", about = "spam random 32-byte data packets")]
    #[command(after_help = "Examples:
  spam d 127.0.0.1:9000
  spam data 10.0.0.1:8080 --rate 256MB --interval 2s
  spam d 192.168.1.100:7000 --rate 1GB --interval 5s")]
    Data {
        #[arg(help = "peer address to send data to")]
        peer: SocketAddr,

        #[arg(
            long,
            default_value = "128MB",
            value_parser = parse_size,
            help = "target bandwidth (e.g., 1000MB, 100KB)"
        )]
        rate: usize,

        #[arg(
            long,
            default_value = "1s",
            value_parser = parse_duration,
            help = "measurement interval (e.g., 1s, 100ms)"
        )]
        interval: Duration,
    },
}

fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    match args.command {
        SpamCommand::Handshake {
            peer,
            peer_public_key,
            rate,
            interval,
        } => run_handshake_spam(peer, &peer_public_key, rate, interval),
        SpamCommand::Data {
            peer,
            rate,
            interval,
        } => run_data_spam(peer, rate, interval),
    }
}

fn run_spam_loop(
    peer: SocketAddr,
    packet: Vec<u8>,
    rate: usize,
    interval: Duration,
    spam_type: &str,
) -> std::io::Result<()> {
    info!(
        peer = %peer,
        rate = rate,
        interval = ?interval,
        spam_type = spam_type,
        "starting spam"
    );

    let socket = UdpSocket::bind("0.0.0.0:0")?;
    socket.set_nonblocking(true)?;

    let packet_size = packet.len();
    let packets_per_interval = (rate as f64 / packet_size as f64).ceil() as u64;
    let packet_interval_nanos = interval.as_nanos() as u64 / packets_per_interval.max(1);
    let packet_interval = Duration::from_nanos(packet_interval_nanos);

    let gso_size = packet_size as u16;
    let burst_size = if packet_interval < Duration::from_micros(100) {
        ((Duration::from_micros(100).as_nanos() as u64 / packet_interval_nanos.max(1)) as usize)
            .clamp(1, 64)
    } else {
        1
    };

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
            info!("gso not supported, falling back to regular sends");
        }
    }

    let mut gso_buffer = vec![0u8; packet_size * burst_size];
    for chunk in gso_buffer.chunks_mut(packet_size) {
        chunk.copy_from_slice(&packet);
    }

    info!(
        packets_per_interval = packets_per_interval,
        packet_interval = ?packet_interval,
        packet_size = packet_size,
        burst_size = burst_size,
        gso_enabled = true,
        "calculated packet rate"
    );

    let mut packets_sent = 0u64;
    let mut next_packet = Instant::now();
    let mut next_stats = Instant::now() + interval;
    let should_sleep = packet_interval >= Duration::from_millis(1);

    loop {
        let now = Instant::now();

        while now >= next_packet {
            match socket.send_to(&gso_buffer, peer) {
                Ok(_) => {
                    packets_sent += burst_size as u64;
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e),
            }

            next_packet += packet_interval * burst_size as u32;

            if next_packet <= now {
                next_packet = now;
            }
        }

        if now >= next_stats {
            let rate_mbps =
                (packets_sent * packet_size as u64) as f64 / interval.as_secs_f64() / 1_000_000.0;
            info!(
                packets_sent = packets_sent,
                rate_mbps = format!("{:.2}", rate_mbps),
                "stats"
            );
            packets_sent = 0;
            next_stats = now + interval;
        }

        if should_sleep {
            let sleep_time = next_packet
                .saturating_duration_since(now)
                .min(Duration::from_millis(1));
            std::thread::sleep(sleep_time);
        }
    }
}

fn run_handshake_spam_loop(
    peer: SocketAddr,
    packet: Vec<u8>,
    handshakes_per_interval: u64,
    interval: Duration,
) -> std::io::Result<()> {
    info!(
        peer = %peer,
        handshakes_per_interval = handshakes_per_interval,
        interval = ?interval,
        "starting handshake spam"
    );

    let socket = UdpSocket::bind("0.0.0.0:0")?;
    socket.set_nonblocking(true)?;

    let packet_size = packet.len();
    let packet_interval_nanos = interval.as_nanos() as u64 / handshakes_per_interval.max(1);
    let packet_interval = Duration::from_nanos(packet_interval_nanos);

    let gso_size = packet_size as u16;
    let burst_size = if packet_interval < Duration::from_micros(100) {
        ((Duration::from_micros(100).as_nanos() as u64 / packet_interval_nanos.max(1)) as usize)
            .clamp(1, 64)
    } else {
        1
    };

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
            info!("gso not supported, falling back to regular sends");
        }
    }

    let mut gso_buffer = vec![0u8; packet_size * burst_size];
    for chunk in gso_buffer.chunks_mut(packet_size) {
        chunk.copy_from_slice(&packet);
    }

    info!(
        packet_interval = ?packet_interval,
        packet_size = packet_size,
        burst_size = burst_size,
        gso_enabled = true,
        "calculated handshake interval"
    );

    let mut handshakes_sent = 0u64;
    let mut next_packet = Instant::now();
    let mut next_stats = Instant::now() + interval;
    let should_sleep = packet_interval >= Duration::from_millis(1);

    loop {
        let now = Instant::now();

        while now >= next_packet {
            match socket.send_to(&gso_buffer, peer) {
                Ok(_) => {
                    handshakes_sent += burst_size as u64;
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e),
            }

            next_packet += packet_interval * burst_size as u32;

            if next_packet <= now {
                next_packet = now;
            }
        }

        if now >= next_stats {
            info!(handshakes_sent = handshakes_sent, "stats");
            handshakes_sent = 0;
            next_stats = now + interval;
        }

        if should_sleep {
            let sleep_time = next_packet
                .saturating_duration_since(now)
                .min(Duration::from_millis(1));
            std::thread::sleep(sleep_time);
        }
    }
}

fn run_handshake_spam(
    peer: SocketAddr,
    peer_public_key_hex: &str,
    rate: u64,
    interval: Duration,
) -> std::io::Result<()> {
    let socket = UdpSocket::bind("0.0.0.0:0")?;
    let id = socket.local_addr()?.port() as u64;
    let mut rng = StdRng::seed_from_u64(id);
    let local_keypair = monad_secp::KeyPair::generate(&mut rng);

    let peer_public_key_bytes = hex::decode(peer_public_key_hex).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid peer public key hex: {}", e),
        )
    })?;

    let peer_public = monad_secp::PubKey::from_slice(&peer_public_key_bytes).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid peer public key: {}", e),
        )
    })?;

    let (init_msg, _state) = monad_wireauth::handshake::send_handshake_init(
        &mut rng,
        SystemTime::now(),
        0,
        &local_keypair,
        &peer_public,
        None,
    );
    let packet = init_msg.as_bytes().to_vec();

    run_handshake_spam_loop(peer, packet, rate, interval)
}

fn run_data_spam(peer: SocketAddr, rate: usize, interval: Duration) -> std::io::Result<()> {
    let socket = UdpSocket::bind("0.0.0.0:0")?;
    let id = socket.local_addr()?.port() as u64;
    let mut rng = StdRng::seed_from_u64(id);

    let mut packet = [0u8; 32];
    packet[0] = monad_wireauth::messages::TYPE_DATA;
    rng.fill(&mut packet[1..]);

    run_spam_loop(peer, packet.to_vec(), rate, interval, "data")
}
