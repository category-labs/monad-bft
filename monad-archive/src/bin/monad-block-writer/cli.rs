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

#[derive(Debug, Parser)]
#[command(name = "monad-archive", about, long_about = None)]
pub struct Cli {
    /// S3 bucket name for storing checker state
    #[arg(long)]
    pub bucket: String,

    /// AWS region
    #[arg(long)]
    pub region: Option<String>,

    #[arg(long)]
    pub start_block: u64,

    /// Override block number to stop at
    #[arg(long)]
    pub stop_block: u64,

    #[arg(long)]
    pub dest_path: PathBuf,

    /// Maximum number of retries for failed blocks
    #[arg(long, default_value = "3")]
    pub max_retries: u32,

    /// Maximum number of concurrent block processing tasks
    #[arg(long, default_value = "50")]
    pub concurrency: usize,
}
