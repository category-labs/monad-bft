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

use std::{io::Write, path::PathBuf};

use clap::Parser;
use monad_bls::BlsSignatureCollection;
use monad_crypto::certificate_signature::CertificateSignaturePubKey;
use monad_eth_types::EthExecutionProtocol;
use monad_executor_glue::LogFriendlyMonadEvent;
use monad_secp::SecpSignature;
use monad_types::Deserializable;
use monad_wal::{
    reader::{WALReader, WALReaderConfig},
    WALError,
};
use rayon::iter::{IntoParallelIterator, ParallelIterator};

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    wal_path: PathBuf,

    #[arg(short, default_value_t = 1)]
    jobs: usize,
}

type SigType = SecpSignature;
type SigColType = BlsSignatureCollection<CertificateSignaturePubKey<SigType>>;
type ExecutionProtocolType = EthExecutionProtocol;
type WrappedEvent = LogFriendlyMonadEvent<SigType, SigColType, ExecutionProtocolType>;

fn main() {
    let args = Args::parse();

    let start = std::time::Instant::now();

    rayon::ThreadPoolBuilder::new()
        .num_threads(args.jobs)
        .build_global()
        .unwrap();

    let config = WALReaderConfig::new(args.wal_path);
    let mut reader: WALReader<WrappedEvent> = config.build().unwrap();

    // this loads everything into memory at once
    // this is not ideal, but each file is capped at 1GB currently anyways
    let raw_events: Vec<_> = std::iter::repeat(())
        .map_while(move |()| match reader.load_one_raw() {
            Ok(raw) => Some(raw),
            Err(WALError::IOError(err)) if err.kind() == std::io::ErrorKind::UnexpectedEof => None,
            Err(err) => panic!("error reading WAL: {:?}", err),
        })
        .collect();

    eprintln!("done read from disk in {:?}", start.elapsed());

    // build output strings in memory
    let events = raw_events
        .into_par_iter()
        .map(|raw| {
            let event = WrappedEvent::deserialize(&raw).expect("failed to deserialize WAL event");
            serde_json::to_string(&event).unwrap()
        })
        .collect_vec_list();

    eprintln!("done serializing events in {:?}", start.elapsed());

    let mut stdout = std::io::stdout().lock();
    let mut buffered_stdout = std::io::BufWriter::new(&mut stdout);
    for event in events.iter().flatten() {
        writeln!(buffered_stdout, "{}", event).unwrap();
    }
    buffered_stdout.flush().unwrap();

    eprintln!("done flushing in {:?}", start.elapsed());
}
