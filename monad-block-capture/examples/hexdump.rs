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
use itertools::Itertools;
use monad_block_capture::BlockCaptureBlockArchive;
use monad_event::BytesDecoder;
use monad_event_capture::{EventCaptureEventIter, EventCaptureNextResult};

#[derive(Debug, Parser)]
#[command(name = "monad-block-capture-hexdump", about, long_about = None)]
pub struct Cli {
    #[arg(long)]
    block_archive_path: PathBuf,

    #[arg(long)]
    block_number: u64,

    #[arg(short, long, default_value_t = 32)]
    width: usize,
}

#[unsafe(no_mangle)]
unsafe extern "C" fn monad_stack_backtrace_capture_and_print(
    buffer: *const char,
    size: libc::size_t,
    fd: i32,
    indent: i32,
    print_async_unsafe_info: bool,
) {
    todo!()
}

fn main() {
    let Cli {
        block_archive_path,
        block_number,
        width,
    } = Cli::parse();

    let block_archive_file = std::fs::File::open(block_archive_path).unwrap();

    let block_archive = BlockCaptureBlockArchive::new(&block_archive_file).unwrap();

    let mut event_capture_reader = block_archive.open_block(block_number).unwrap();

    while let Some(event_section) = event_capture_reader.next_event_section() {
        let mut event_iter: EventCaptureEventIter<'_, BytesDecoder> = event_section.open_iterator();

        loop {
            let (info, hexdump) = match event_iter.next_descriptor() {
                EventCaptureNextResult::Success(event_descriptor) => event_descriptor
                    .try_filter_map_raw(|info, bytes| {
                        Some((
                            info,
                            bytes
                                .iter()
                                .map(|byte| format!("{byte:02x?}"))
                                .collect_vec(),
                        ))
                    }),
                EventCaptureNextResult::End => break,
                EventCaptureNextResult::NoSeqno => panic!("no seqno"),
            }
            .unwrap();

            println!(
                "{:08x} {:02x} | {}",
                info.seqno,
                info.event_type,
                hexdump
                    .into_iter()
                    .chunks(width)
                    .into_iter()
                    .map(|mut chunk| chunk.join(" "))
                    .join("\n              ")
            );
        }
    }
}
