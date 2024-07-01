use std::time;

use clap::Parser;
use futures::StreamExt;
use log::info;

#[derive(clap::Parser)]
pub struct Args {
    #[arg(long)]
    pub triedb_path: std::path::PathBuf,
    #[arg(long)]
    pub blockdb_path: std::path::PathBuf,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    env_logger::try_init().expect("failed to initialize logger");

    let deps = monad_stream::Deps::new(&args.blockdb_path).expect("database path is incorrect");
    let mut stream = monad_stream::BlockStream::new(deps, &args.triedb_path);

    let start = time::Instant::now();
    let mut num_blocks = 0;
    loop {
        tokio::select! {
            Some(block) = stream.next() => {
                info!("block {} has {} txns", block.block.number, block.block.body.len());
                num_blocks += 1;
            },
            _ = tokio::signal::ctrl_c() => {
                info!("ctrl-c received, shutting down");
                info!("processed {} blocks in {:?}", num_blocks, start.elapsed());
                break;
            }
        };
    }
}
