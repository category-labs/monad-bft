use clap::{Parser, Subcommand};

use crate::{
    archive::cli::ArchiveRunCli, block_writer::cli::ArchiveBlockWriterCli,
    check::cli::ArchiveCheckCli, index::cli::ArchiveIndexCli,
};

#[derive(Debug, Parser)]
#[command(name = "monad-archive", about = "Monad archive tools")]
pub struct ArchiveCli {
    #[command(subcommand)]
    pub command: ArchiveCliCommand,
}

#[derive(Debug, Subcommand)]
pub enum ArchiveCliCommand {
    Archive(ArchiveRunCli),
    Index(ArchiveIndexCli),
    Check(ArchiveCheckCli),
    BlockWriter(ArchiveBlockWriterCli),
}
