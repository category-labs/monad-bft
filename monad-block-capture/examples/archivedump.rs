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
use monad_block_capture::BlockCaptureBlockArchive;
use monad_event_capture::{EventCaptureEventSection, EventCaptureNextResult};
use monad_exec_events::{ExecEventCaptureEventIter, ExecutedBlock, ExecutedBlockBuilder};

#[derive(Debug, Parser)]
#[command(name = "monad-block-capture-archivedump", about, long_about = None)]
pub struct Cli {
    #[arg(long)]
    block_archive_path: PathBuf,

    #[arg(long)]
    start_block_number: u64,
}

fn main() {
    let Cli {
        block_archive_path,
        start_block_number,
    } = Cli::parse();

    let block_archive_dir = std::fs::File::open(block_archive_path).unwrap();
    let block_archive = BlockCaptureBlockArchive::new(&block_archive_dir).unwrap();

    for block_number in start_block_number.. {
        let mut event_capture_reader = loop {
            match block_archive.open_block(block_number) {
                Ok(event_capture_reader) => break event_capture_reader,
                Err(err) => match err.kind() {
                    std::io::ErrorKind::NotFound => {
                        std::thread::sleep(std::time::Duration::from_millis(100));
                    }
                    _ => panic!("{err:#?}"),
                },
            }
        };

        let event_section = event_capture_reader
            .next_event_section()
            .expect("BlockArchive evcap file contains an event section");

        let block = extract_block(event_section);

        let block_header = block.to_alloy_header();

        println!("[block {block_number}]: {block_header:#?}");
    }
}

fn extract_block(event_section: EventCaptureEventSection) -> ExecutedBlock {
    let mut event_iter: ExecEventCaptureEventIter<'_> = event_section.open_iterator();

    let mut block_builder = ExecutedBlockBuilder::new(true, true);

    let block = loop {
        match event_iter.next_descriptor() {
            EventCaptureNextResult::Success(event_descriptor) => {
                let Some(result) = block_builder.process_event_descriptor(&event_descriptor) else {
                    continue;
                };

                break result.expect(
                    "BlockBuilder succeeds because it was run on sequential event_descriptors",
                );
            }
            EventCaptureNextResult::End => {
                panic!("BlockArchive evcap file contains a single block, instead reached `End`")
            }
            EventCaptureNextResult::NoSeqno => {
                panic!("BlockArchive evcap file contains a single block, instaed reached `NoSeqno`")
            }
        }
    };

    assert!(
        matches!(event_iter.next_descriptor(), EventCaptureNextResult::End),
        "BlockArchive evcap file ends after block"
    );

    block
}
