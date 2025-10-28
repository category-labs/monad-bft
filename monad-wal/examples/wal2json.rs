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
    fmt::Debug,
    io::{ErrorKind, Write},
    path::PathBuf,
};

use clap::Parser;
use dateparser::DateTimeUtc;
use itertools::Itertools;
use monad_bls::BlsSignatureCollection;
use monad_crypto::certificate_signature::CertificateSignaturePubKey;
use monad_eth_types::EthExecutionProtocol;
use monad_executor_glue::LogFriendlyMonadEvent;
use monad_secp::SecpSignature;
use monad_types::Deserializable;
use monad_wal::reader::{events_iter_in_range, events_iter_raw, WALReader, WALReaderConfig};
use rayon::iter::{IntoParallelIterator, ParallelIterator};

#[derive(Parser, Debug)]
#[command(long_about = "\
Selected timestamp formats
  excerpt from https://docs.rs/dateparser/latest/dateparser/index.html#accepted-date-formats

unix timestamp:
  1511648546
  1620021848429
rfc3339:
  2021-05-01T01:17:02.604456Z
  2017-11-25T22:34:50Z
yyyy-mm-dd:
  2021-02-21
hh:mm:ss:
  01:06:06
  4:00pm
hh:mm:ss z:
  01:06:06 PST")]
struct Args {
    #[arg(short, default_value_t = 1)]
    jobs: usize,

    #[arg(short, default_value = "2000-01-01")]
    after: DateTimeUtc,

    #[arg(short, default_value = "2100-01-01")]
    before: DateTimeUtc,

    #[arg(value_delimiter = ' ')]
    paths: Vec<PathBuf>,
}

type SigType = SecpSignature;
type SigColType = BlsSignatureCollection<CertificateSignaturePubKey<SigType>>;
type ExecutionProtocolType = EthExecutionProtocol;
type WrappedEvent = LogFriendlyMonadEvent<SigType, SigColType, ExecutionProtocolType>;

const EVENTS_SERIALIZE_BATCH_SIZE: usize = 10_000;

fn main() {
    let args = Args::parse();
    if args.paths.is_empty() {
        eprintln!("error: no wal paths specified");
        std::process::exit(1);
    }

    let start = std::time::Instant::now();

    rayon::ThreadPoolBuilder::new()
        .num_threads(args.jobs)
        .build_global()
        .unwrap();

    let timestamp_range = args.after.0..=args.before.0;

    let raw_events_iter = events_iter_in_range(
        args.paths.into_iter().map(|path| {
            let config = WALReaderConfig::new(path);
            let reader: WALReader<WrappedEvent> = config.build().unwrap();
            events_iter_raw(reader)
        }),
        |event| WrappedEvent::deserialize_timestamp(event),
        timestamp_range,
    );

    let mut num_events = 0usize;
    let mut stdout = std::io::stdout().lock();
    let mut buffered_stdout = std::io::BufWriter::new(&mut stdout);

    for raw_events_batch in raw_events_iter
        .chunks(EVENTS_SERIALIZE_BATCH_SIZE)
        .into_iter()
    {
        let raw_events_batch = raw_events_batch.collect_vec();
        num_events += raw_events_batch.len();
        eprintln!("read {} events in {:?}", num_events, start.elapsed());

        // build output strings in memory
        let serialized_events = raw_events_batch
            .into_par_iter()
            .map(|raw| {
                let event =
                    WrappedEvent::deserialize(&raw).expect("failed to deserialize WAL event");
                serde_json::to_string(&event).unwrap()
            })
            .collect_vec_list();
        eprintln!("serialized {} events in {:?}", num_events, start.elapsed());

        for serialized_event in serialized_events.iter().flatten() {
            match writeln!(buffered_stdout, "{}", serialized_event) {
                Ok(()) => {}
                Err(e) if e.kind() == ErrorKind::BrokenPipe => {
                    // pipe broken, exit early
                    std::process::exit(0);
                }
                Err(e) => {
                    eprintln!("error writing to stdout: {}", e);
                    std::process::exit(1);
                }
            }
        }

        eprintln!(
            "wrote {} events to stdout in {:?}",
            num_events,
            start.elapsed()
        );
    }

    buffered_stdout.flush().unwrap();
}
