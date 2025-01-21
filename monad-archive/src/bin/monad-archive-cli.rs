use clap::Parser;
use monad_archive::{cli::ArchiveArgs, prelude::*};

#[derive(Debug, Parser)]
#[command(name = "monad-index-checker", about, long_about = None)]
pub struct Cli {
    #[arg(long, value_parser = clap::value_parser!(ArchiveArgs))]
    pub source: ArchiveArgs,

    #[arg(long)]
    pub block: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let args = Cli::parse();
    info!(?args);

    let block_data_source = args.source.build_archive_reader(&Metrics::none()).await?;

    let block = block_data_source.get_block_by_number(args.block).await?;

    dbg!(block);

    Ok(())
}
