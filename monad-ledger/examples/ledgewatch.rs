use std::{
    fs::File,
    io::{self, Read, Seek},
    path::PathBuf,
    thread, time,
};

use alloy_rlp::{Decodable, Encodable};
use clap::Parser;
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

    loop {
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
                println!("seqnum: {:?}, num_tx: {:?}", x.header.number, x.body.len());
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
