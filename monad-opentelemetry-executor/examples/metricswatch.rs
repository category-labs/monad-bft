use std::{path::PathBuf, time::Duration};

use clap::Parser;
use futures::FutureExt;
use futures_util::StreamExt;
use monad_bls::BlsSignatureCollection;
use monad_crypto::certificate_signature::CertificateSignaturePubKey;
use monad_executor_glue::{MetricsCommand, MonadEvent};
use monad_opentelemetry_executor::OpenTelemetryExecutor;
use monad_secp::SecpSignature;
use monad_triedb_utils::{TriedbEnv, TriedbResult};
use monad_updaters::{BoxUpdater, Updater};
use tokio::signal;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    pub triedb_path: PathBuf,
    #[arg(long)]
    pub otel_endpoint: String,
    #[arg(long)]
    pub record_metrics_interval_seconds: u64,
    #[arg(long)]
    pub node_name: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let triedb_reader = TriedbEnv::new(&args.triedb_path);
    let mut block_num = match triedb_reader.get_latest_block().await {
        TriedbResult::BlockNum(num) => num,
        _ => 0,
    };
    let mut previous_block_num = block_num;

    let mut metrics_executor: BoxUpdater<
        'static,
        MetricsCommand,
        MonadEvent<
            SecpSignature,
            BlsSignatureCollection<CertificateSignaturePubKey<SecpSignature>>,
        >,
    > = Updater::boxed(OpenTelemetryExecutor::new(
        args.otel_endpoint,
        args.node_name,
        Duration::from_secs(args.record_metrics_interval_seconds),
        /*enable_grpc_gzip=*/ false,
    ));

    let init_cmds = vec![MetricsCommand::RecordExecutionMetrics(
        block_num - previous_block_num,
    )];
    metrics_executor.exec(init_cmds);

    let mut ctrlc = Box::pin(signal::ctrl_c()).into_stream();
    loop {
        tokio::select! {
            _ = ctrlc.next() => {
                break;
            }
            _ = metrics_executor.next() => {
                block_num = match triedb_reader.get_latest_block().await {
                    TriedbResult::BlockNum(num) => num,
                    _ => previous_block_num,
                };

                metrics_executor.exec(vec![MetricsCommand::RecordExecutionMetrics(block_num - previous_block_num)]);

                previous_block_num = block_num;
            }
        }
    }
}
