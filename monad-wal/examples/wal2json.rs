use std::path::PathBuf;

use clap::Parser;
use monad_bls::BlsSignatureCollection;
use monad_crypto::certificate_signature::CertificateSignaturePubKey;
use monad_eth_types::EthExecutionProtocol;
use monad_executor_glue::LogFriendlyMonadEvent;
use monad_secp::SecpSignature;
use monad_types::Serializable;
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
        let serialized_event = event.serialize();

        let event_json =
            serde_json::to_string(&serialized_event).expect("failed to serialize event");
        println!("{}", event_json);
    })
}
