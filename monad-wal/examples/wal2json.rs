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

use std::path::PathBuf;

use clap::Parser;
use monad_bls::BlsSignatureCollection;
use monad_crypto::certificate_signature::CertificateSignaturePubKey;
use monad_eth_types::EthExecutionProtocol;
use monad_executor_glue::LogFriendlyMonadEvent;
use monad_secp::SecpSignature;
use monad_wal::wal::WALoggerConfig;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    wal_path: PathBuf,
}

type SigType = SecpSignature;
type SigColType = BlsSignatureCollection<CertificateSignaturePubKey<SigType>>;
type ExecutionProtocolType = EthExecutionProtocol;
type WrappedEvent = LogFriendlyMonadEvent<SigType, SigColType, ExecutionProtocolType>;

fn main() {
    let args = Args::parse();

    let logger_config: WALoggerConfig<WrappedEvent> = WALoggerConfig::new(args.wal_path, false);
    logger_config.events().for_each(|event| {
        let event_json = serde_json::to_string(&event).expect("failed to serialize event");
        println!("{}", event_json);
    })
}
