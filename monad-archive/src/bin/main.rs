use clap::Parser;
use eyre::Result;
use tracing::Level;

mod archive;
mod block_writer;
mod check;
mod cli;
mod index;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    match cli::ArchiveCli::parse().command {
        cli::ArchiveCliCommand::Archive(args) => archive::run(args).await,
        cli::ArchiveCliCommand::Index(args) => index::run(args).await,
        cli::ArchiveCliCommand::Check(args) => check::run(args).await,
        cli::ArchiveCliCommand::BlockWriter(args) => block_writer::run(args).await,
    }
}
