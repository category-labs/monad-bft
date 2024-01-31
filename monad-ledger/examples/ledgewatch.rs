use std::{
    fs::File,
    io::{self, Read, Seek},
    path::PathBuf,
    thread, time,
};

use alloy_rlp::{Decodable, Encodable};
use clap::Parser;
use monad_crypto::hasher::{Blake3Hash, Hash, Hasher};
use reth_primitives::Block;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    ledger_path: PathBuf,
}

const KECCAK_HDR_LEN: usize = 32;
const POLL_PERIOD_MS: u64 = 1000;

fn main() -> io::Result<()> {
    let args = Args::parse();

    let mut f = File::open(args.ledger_path)?;

    let mut cursor = KECCAK_HDR_LEN;
    let mut running_hash = Hash::default();

    loop {
        let mut hasher = Blake3Hash::new();
        hasher.update(running_hash);
        let end = f.seek(io::SeekFrom::End(0))?;
        f.seek(io::SeekFrom::Start(cursor as u64))?;
        if cursor > end as usize {
            thread::sleep(time::Duration::from_millis(POLL_PERIOD_MS));
            continue;
        }
        let read_size = (end as usize) - cursor;
        let mut buf = vec![0; read_size];
        f.read_exact(&mut buf)?;

        let y = Block::decode(&mut &buf[..]);
        let c = match y {
            Ok(x) => {
                for t in &x.body {
                    hasher.update(t.hash());
                }
                running_hash = hasher.hash();

                println!(
                    "seqnum: {:?}, running_hash: {}",
                    x.header.number, running_hash,
                );
                x.length() + KECCAK_HDR_LEN
            }
            Err(_) => {
                thread::sleep(time::Duration::from_millis(POLL_PERIOD_MS));
                0
            }
        };

        cursor += c;
    }
}
