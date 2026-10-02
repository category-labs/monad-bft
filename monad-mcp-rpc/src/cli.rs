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

//! `mcp-tx`: sends txs through the rpc or straight to a node over udp.

use std::{
    io::{Read, Write},
    net::SocketAddr,
    path::PathBuf,
    time::{Duration, Instant},
};

use bytes::Bytes;
use clap::{Args, Parser, Subcommand};
use monad_mcp_chorus::ledger::{Address, MAX_TX_PAYLOAD, Tx};
use monad_mcp_node::chorus::types::NodeId;
use serde::Serialize;

use crate::{
    api::{SubmitView, TxView, decode_hex, hex0x, parse_fixed},
    config::DEFAULT_HTTP_ADDR,
    service::{unix_micros, unix_now},
    udp,
};

pub const WAIT_POLL: Duration = Duration::from_millis(200);

#[derive(Debug, Parser)]
#[command(
    name = "mcp-tx",
    about = "Send txs to an MCP node, through the rpc or straight over udp"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// send one tx or a burst
    Send(SendArgs),
    /// print the rpc's view of a tx
    Status(StatusArgs),
}

#[derive(Debug, Args)]
pub struct SendArgs {
    /// base url of monad-mcp-rpc (the default target)
    #[arg(long)]
    pub rpc: Option<String>,
    /// a validator's udp address, bypassing the rpc
    #[arg(long, requires = "sender_id", conflicts_with_all = ["rpc", "wait"])]
    pub node: Option<SocketAddr>,
    /// with --node: the validator id the frames claim; the node drops any other sender
    #[arg(long, requires = "node")]
    pub sender_id: Option<u64>,
    /// 20-byte hex address; random when omitted
    #[arg(long)]
    pub sender: Option<String>,
    /// nonce of the first tx, incremented per tx; the clock in microseconds when omitted
    #[arg(long)]
    pub nonce: Option<u64>,
    /// utf-8 payload [default: "mcp-tx <nonce>"]
    #[arg(long, group = "body")]
    pub payload: Option<String>,
    #[arg(long, group = "body")]
    pub payload_hex: Option<String>,
    /// raw payload bytes from a file
    #[arg(long, group = "body")]
    pub payload_file: Option<PathBuf>,
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u64).range(1..))]
    pub count: u64,
    /// milliseconds between txs of a burst
    #[arg(long, default_value_t = 0)]
    pub interval: u64,
    /// with --rpc: wait up to this many seconds for every tx to commit or fail
    #[arg(long)]
    pub wait: Option<u64>,
}

#[derive(Debug, Args)]
pub struct StatusArgs {
    pub hash: String,
    #[arg(long)]
    pub rpc: Option<String>,
    /// wait up to this many seconds for the tx to commit or fail
    #[arg(long)]
    pub wait: Option<u64>,
}

#[derive(Debug, thiserror::Error)]
pub enum CliError {
    #[error(
        "payload is {len} bytes, over the {MAX_TX_PAYLOAD}-byte limit (MAX_TX_PAYLOAD); not sending"
    )]
    PayloadTooLarge { len: usize },
    #[error("{0}")]
    Usage(String),
    #[error("read {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("udp to {node}: {source}")]
    Udp {
        node: SocketAddr,
        source: std::io::Error,
    },
    #[error("rpc: {0}")]
    Http(#[from] reqwest::Error),
    #[error("rpc answered {status}: {message}")]
    Rpc { status: u16, message: String },
    #[error("{0}")]
    NotCommitted(String),
    #[error("write output: {0}")]
    Output(#[from] std::io::Error),
}

pub fn default_rpc() -> String {
    format!("http://{DEFAULT_HTTP_ADDR}")
}

fn rpc_base(rpc: Option<&str>) -> String {
    rpc.map_or_else(default_rpc, |url| url.trim_end_matches('/').to_owned())
}

impl SendArgs {
    fn payload(&self) -> Result<Option<Vec<u8>>, CliError> {
        let payload = if let Some(text) = &self.payload {
            text.clone().into_bytes()
        } else if let Some(hex) = &self.payload_hex {
            decode_hex(hex)
                .ok_or_else(|| CliError::Usage("--payload-hex is not valid hex".into()))?
        } else if let Some(path) = &self.payload_file {
            // one byte past the cap is enough to refuse it, whatever the file's size
            let mut payload = Vec::new();
            std::fs::File::open(path)
                .and_then(|file| {
                    file.take(MAX_TX_PAYLOAD as u64 + 1)
                        .read_to_end(&mut payload)
                })
                .map_err(|source| CliError::Read {
                    path: path.clone(),
                    source,
                })?;
            payload
        } else {
            return Ok(None);
        };
        Ok(Some(payload))
    }

    // every tx of the burst, checked before anything is sent
    pub fn txs(&self) -> Result<Vec<Tx>, CliError> {
        let payload = self.payload()?;
        if let Some(payload) = &payload
            && payload.len() > MAX_TX_PAYLOAD
        {
            return Err(CliError::PayloadTooLarge { len: payload.len() });
        }
        let sender: Address = match &self.sender {
            Some(sender) => parse_fixed(sender)
                .ok_or_else(|| CliError::Usage("--sender must be 20 bytes of hex".into()))?,
            None => rand::random(),
        };
        let first = self.nonce.unwrap_or_else(unix_micros);
        (0..self.count)
            .map(|i| {
                let nonce = first
                    .checked_add(i)
                    .ok_or_else(|| CliError::Usage("--nonce overflows over --count".into()))?;
                let payload = payload
                    .clone()
                    .unwrap_or_else(|| format!("mcp-tx {nonce}").into_bytes());
                Ok(Tx {
                    sender,
                    nonce,
                    payload: Bytes::from(payload),
                    sent_at_ns: 0,         // demo(tx-timeline): stamped as each is sent
                    rpc_received_at_ns: 0, // demo(tx-timeline)
                    mempool_admitted_at_ns: 0, // demo(tx-timeline)
                })
            })
            .collect()
    }
}

#[derive(Serialize)]
struct DirectView {
    tx_hash: String,
    node: String,
    sender_id: u64,
    status: &'static str,
}

fn print_json(out: &mut impl Write, value: &impl Serialize) -> Result<(), CliError> {
    serde_json::to_writer(&mut *out, value).map_err(std::io::Error::other)?;
    writeln!(out)?;
    Ok(())
}

async fn rpc_error(response: reqwest::Response) -> CliError {
    let status = response.status().as_u16();
    let body = response.text().await.unwrap_or_default();
    let message = serde_json::from_str::<serde_json::Value>(&body)
        .ok()
        .and_then(|v| v.get("error").and_then(|e| e.as_str()).map(str::to_owned))
        .unwrap_or(body);
    CliError::Rpc { status, message }
}

fn request_body(tx: &Tx) -> serde_json::Value {
    serde_json::json!({
        "sender": hex0x(&tx.sender),
        "nonce": tx.nonce,
        "payload_hex": hex0x(&tx.payload),
        "sent_at_ns": tx.sent_at_ns, // demo(tx-timeline)
    })
}

async fn fetch_status(
    client: &reqwest::Client,
    base: &str,
    hash: &str,
) -> Result<TxView, CliError> {
    let response = client.get(format!("{base}/tx/{hash}")).send().await?;
    if !response.status().is_success() {
        return Err(rpc_error(response).await);
    }
    Ok(response.json().await?)
}

async fn wait_final(
    client: &reqwest::Client,
    base: &str,
    hash: &str,
    until: Instant,
) -> Result<TxView, CliError> {
    loop {
        let view = fetch_status(client, base, hash).await?;
        if view.state != "pending" || Instant::now() >= until {
            return Ok(view);
        }
        tokio::time::sleep(WAIT_POLL).await;
    }
}

pub async fn run(cli: Cli, out: &mut impl Write) -> Result<(), CliError> {
    match cli.command {
        Command::Send(args) => send(args, out).await,
        Command::Status(args) => status(args, out).await,
    }
}

async fn send(args: SendArgs, out: &mut impl Write) -> Result<(), CliError> {
    let txs = args.txs()?;
    let interval = Duration::from_millis(args.interval);
    if let (Some(node), Some(sender_id)) = (args.node, args.sender_id) {
        let udp_error = |source| CliError::Udp { node, source };
        let socket = udp::bind_ephemeral(node.is_ipv6()).map_err(udp_error)?;
        for (i, mut tx) in txs.into_iter().enumerate() {
            if i > 0 {
                tokio::time::sleep(interval).await;
            }
            tx.sent_at_ns = unix_now().as_nanos() as u64; // demo(tx-timeline)
            socket
                .send_to(&udp::frame(NodeId::dummy(sender_id), &tx), node)
                .map_err(udp_error)?;
            let view = DirectView {
                tx_hash: hex0x(&tx.hash()),
                node: node.to_string(),
                sender_id,
                status: "sent",
            };
            print_json(out, &view)?;
        }
        return Ok(());
    }

    let base = rpc_base(args.rpc.as_deref());
    let client = reqwest::Client::new();
    let mut hashes = Vec::with_capacity(txs.len());
    for (i, mut tx) in txs.into_iter().enumerate() {
        if i > 0 {
            tokio::time::sleep(interval).await;
        }
        tx.sent_at_ns = unix_now().as_nanos() as u64; // demo(tx-timeline)
        let response = client
            .post(format!("{base}/tx"))
            .json(&request_body(&tx))
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(rpc_error(response).await);
        }
        let view: SubmitView = response.json().await?;
        print_json(out, &view)?;
        hashes.push(view.tx.tx_hash);
    }
    if let Some(secs) = args.wait {
        let until = Instant::now() + Duration::from_secs(secs);
        let mut unfinished = 0;
        for hash in &hashes {
            let view = wait_final(&client, &base, hash, until).await?;
            unfinished += usize::from(view.state != "committed");
            print_json(out, &view)?;
        }
        if unfinished > 0 {
            return Err(CliError::NotCommitted(format!(
                "{unfinished} of {} txs not committed",
                hashes.len()
            )));
        }
    }
    Ok(())
}

async fn status(args: StatusArgs, out: &mut impl Write) -> Result<(), CliError> {
    if parse_fixed::<32>(&args.hash).is_none() {
        return Err(CliError::Usage("the hash must be 32 bytes of hex".into()));
    }
    let base = rpc_base(args.rpc.as_deref());
    let client = reqwest::Client::new();
    let view = match args.wait {
        Some(secs) => {
            let until = Instant::now() + Duration::from_secs(secs);
            wait_final(&client, &base, &args.hash, until).await?
        }
        None => fetch_status(&client, &base, &args.hash).await?,
    };
    print_json(out, &view)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn send_args(args: &[&str]) -> SendArgs {
        let cli = Cli::try_parse_from([&["mcp-tx", "send"], args].concat()).unwrap();
        match cli.command {
            Command::Send(args) => args,
            Command::Status(_) => unreachable!(),
        }
    }

    #[test]
    fn a_payload_over_the_cap_is_refused_not_truncated() {
        let over = "a".repeat(MAX_TX_PAYLOAD + 1);
        let error = send_args(&["--payload", &over]).txs().unwrap_err();
        assert!(matches!(error, CliError::PayloadTooLarge { len } if len == MAX_TX_PAYLOAD + 1));
        assert!(error.to_string().contains("1025 bytes"));

        let over_hex = "00".repeat(MAX_TX_PAYLOAD + 1);
        assert!(matches!(
            send_args(&["--payload-hex", &over_hex]).txs(),
            Err(CliError::PayloadTooLarge { .. })
        ));

        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("big");
        std::fs::write(&file, vec![1; MAX_TX_PAYLOAD + 1]).unwrap();
        assert!(matches!(
            send_args(&["--payload-file", file.to_str().unwrap()]).txs(),
            Err(CliError::PayloadTooLarge { .. })
        ));
        // an endless file is refused after reading just past the cap
        assert!(matches!(
            send_args(&["--payload-file", "/dev/zero"]).txs(),
            Err(CliError::PayloadTooLarge { len }) if len == MAX_TX_PAYLOAD + 1
        ));
        std::fs::write(&file, vec![1; MAX_TX_PAYLOAD]).unwrap();
        let txs = send_args(&["--payload-file", file.to_str().unwrap()])
            .txs()
            .unwrap();
        assert_eq!(txs[0].payload.len(), MAX_TX_PAYLOAD);

        let max = "a".repeat(MAX_TX_PAYLOAD);
        let txs = send_args(&["--payload", &max]).txs().unwrap();
        assert_eq!(txs[0].payload.len(), MAX_TX_PAYLOAD);
        assert!(txs[0].validate().is_ok());
    }

    #[test]
    fn a_burst_shares_a_sender_and_counts_up_nonces() {
        let sender = "22".repeat(20);
        let txs = send_args(&["--sender", &sender, "--nonce", "5", "--count", "3"])
            .txs()
            .unwrap();
        assert_eq!(
            txs.iter().map(|tx| tx.nonce).collect::<Vec<_>>(),
            vec![5, 6, 7]
        );
        assert!(txs.iter().all(|tx| tx.sender == [0x22; 20]));
        assert_eq!(&txs[1].payload[..], b"mcp-tx 6");

        let random = send_args(&["--count", "2", "--payload", "x"])
            .txs()
            .unwrap();
        assert_eq!(random[0].sender, random[1].sender);
        assert_eq!(random[0].nonce + 1, random[1].nonce);
        let other = send_args(&["--payload", "x"]).txs().unwrap();
        assert_ne!(random[0].sender, other[0].sender);
    }

    #[test]
    fn bad_arguments_are_rejected() {
        assert!(matches!(
            send_args(&["--sender", "0x12"]).txs(),
            Err(CliError::Usage(_))
        ));
        assert!(matches!(
            send_args(&["--payload-hex", "xyz"]).txs(),
            Err(CliError::Usage(_))
        ));
        assert!(matches!(
            send_args(&["--nonce", &u64::MAX.to_string(), "--count", "2"]).txs(),
            Err(CliError::Usage(_))
        ));
        for args in [
            vec![
                "--rpc",
                "http://x",
                "--node",
                "127.0.0.1:9",
                "--sender-id",
                "0",
            ],
            vec!["--payload", "a", "--payload-hex", "00"],
            vec!["--count", "0"],
            vec!["--node", "127.0.0.1:9", "--sender-id", "0", "--wait", "5"],
            // a frame needs a validator id to be accepted
            vec!["--node", "127.0.0.1:9"],
            vec!["--sender-id", "0"],
            vec!["--node", "localhost", "--sender-id", "0"],
            vec!["--node", "127.0.0.1:9", "--sender-id", "-1"],
            vec!["--socket", "/s"],
        ] {
            assert!(
                Cli::try_parse_from([&["mcp-tx", "send"], &args[..]].concat()).is_err(),
                "{args:?}"
            );
        }
    }

    #[test]
    fn a_direct_send_names_a_node_address_and_a_sender_id() {
        let parse = |args: &[&str]| Cli::try_parse_from([&["mcp-tx", "send"], args].concat());
        assert!(parse(&["--node", "127.0.0.1:9000", "--sender-id", "4"]).is_ok());
        assert!(parse(&["--node", "[::1]:9000", "--sender-id", "0", "--count", "3"]).is_ok());
        assert!(parse(&["--rpc", "http://127.0.0.1:8080", "--wait", "5"]).is_ok());
    }

    #[test]
    fn the_rpc_url_defaults_and_loses_a_trailing_slash() {
        assert_eq!(rpc_base(None), "http://127.0.0.1:8080");
        assert_eq!(rpc_base(Some("http://h:1/")), "http://h:1");
    }
}
