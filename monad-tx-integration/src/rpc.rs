use alloy_primitives::Address;
use monad_state_backend::StateBackend;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};

use crate::router::SBT;

pub type NodeState = SBT;

pub async fn start_rpc_server(
    state: NodeState,
    listen_addr: &str,
) -> std::io::Result<std::net::SocketAddr> {
    let listener = TcpListener::bind(listen_addr).await?;
    let addr = listener.local_addr()?;

    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, peer)) => {
                    tracing::debug!(%peer, "rpc connection");
                    let state = state.clone();
                    tokio::spawn(handle_connection(stream, state));
                }
                Err(e) => {
                    tracing::warn!(?e, "rpc accept error");
                }
            }
        }
    });

    Ok(addr)
}

async fn handle_connection(stream: TcpStream, state: NodeState) {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    loop {
        line.clear();
        match reader.read_line(&mut line).await {
            Ok(0) => break, // EOF
            Ok(_) => {
                let response = process_command(line.trim(), &state);
                if writer.write_all(response.as_bytes()).await.is_err() {
                    break;
                }
                if writer.write_all(b"\n").await.is_err() {
                    break;
                }
            }
            Err(_) => break,
        }
    }
}

fn process_command(cmd: &str, state: &NodeState) -> String {
    let parts: Vec<&str> = cmd.split_whitespace().collect();

    match parts.as_slice() {
        ["NONCE", addr_str] => {
            let addr = match addr_str.parse::<Address>() {
                Ok(a) => a,
                Err(_) => return "-ERR invalid address".to_string(),
            };

            let guard = state.lock().unwrap();
            let latest_seq = match guard.raw_read_latest_finalized_block() {
                Some(seq) => seq,
                None => return "-ERR no committed state".to_string(),
            };

            if let Some(block_state) = guard.committed_state(&latest_seq) {
                let nonce = block_state.get_nonce(&addr).unwrap_or(0);
                format!(":{}", nonce)
            } else {
                "-ERR state not found".to_string()
            }
        }
        ["PING"] => "+PONG".to_string(),
        _ => "-ERR unknown command".to_string(),
    }
}

// Client helper for submitter
pub async fn query_nonce(rpc_addr: &str, address: &Address) -> std::io::Result<u64> {
    use tokio::{io::AsyncReadExt, net::TcpStream};

    let mut stream = TcpStream::connect(rpc_addr).await?;
    let cmd = format!("NONCE {}\n", address);
    stream.write_all(cmd.as_bytes()).await?;

    let mut response = String::new();
    let mut buf = [0u8; 128];
    loop {
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        response.push_str(&String::from_utf8_lossy(&buf[..n]));
        if response.contains('\n') {
            break;
        }
    }

    let response = response.trim();
    if response.starts_with(':') {
        response[1..]
            .parse()
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "bad nonce"))
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            response.to_string(),
        ))
    }
}
