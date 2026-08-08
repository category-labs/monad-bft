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

use clap::{Parser, Subcommand};
use monad_archive::cli::{ArchiveArgs, BlockDataReaderArgs};

#[derive(Debug, Parser)]
#[command(name = "monad-json-archive", about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub mode: Mode,
}

#[derive(Debug, Parser)]
pub struct SharedArgs {
    /// Source to read block data that will be indexed
    #[arg(long, value_parser = clap::value_parser!(BlockDataReaderArgs))]
    pub block_data_source: BlockDataReaderArgs,

    /// If reading from --block-data-source fails, attempts to read from
    /// this optional fallback
    #[arg(long, value_parser = clap::value_parser!(BlockDataReaderArgs))]
    pub fallback_block_data_source: Option<BlockDataReaderArgs>,

    /// Where archive data is written to
    /// For aws: 'aws <bucket_name> <concurrent_requests>'
    #[arg(long, value_parser = clap::value_parser!(ArchiveArgs))]
    pub archive_sink: ArchiveArgs,

    /// Maximum number of retries for failed blocks
    #[arg(long, default_value = "5")]
    pub max_retries: u32,

    /// Maximum number of concurrent block processing tasks
    #[arg(long, default_value = "1000")]
    pub concurrency: usize,

    /// MongoDB collection name to store the data
    #[arg(long, default_value = "block_json_data")]
    pub collection: String,
}

#[derive(Subcommand, Debug)]
pub enum Mode {
    WriteRange(WriteRangeArgs),
    Stream(StreamArgs),
    /// Set the start block marker and exit.
    /// Use this instead of --start-block to safely configure the starting point.
    SetStartBlock {
        /// Block number to set as the latest marker
        #[arg(long)]
        block: u64,

        /// Where archive data is written to (MongoDB)
        #[arg(long, value_parser = clap::value_parser!(ArchiveArgs))]
        archive_sink: ArchiveArgs,

        /// MongoDB collection name to store the data
        #[arg(long, default_value = "block_json_data")]
        collection: String,
    },
}

impl Mode {
    /// Returns the shared args for modes that have them.
    /// Panics if called on SetStartBlock (handle that case separately before calling this).
    pub fn shared(&self) -> &SharedArgs {
        match self {
            Mode::WriteRange(args) => &args.shared_args,
            Mode::Stream(args) => &args.shared_args,
            Mode::SetStartBlock { .. } => {
                panic!("SetStartBlock does not have shared args - handle it separately")
            }
        }
    }
}

#[derive(Debug, Parser)]
pub struct StreamArgs {
    #[command(flatten)]
    pub shared_args: SharedArgs,

    /// Sleep seconds between blocks
    #[arg(long, default_value = "1.0")]
    pub sleep_secs: f64,

    /// Maximum number of blocks to process per iteration
    #[arg(long, default_value = "1000")]
    pub max_blocks_per_iter: u64,
}

#[derive(Debug, Parser)]
pub struct WriteRangeArgs {
    /// Start block
    #[arg(long)]
    pub start_block: u64,

    /// Stop block
    #[arg(long)]
    pub stop_block: u64,

    #[command(flatten)]
    pub shared_args: SharedArgs,
}
