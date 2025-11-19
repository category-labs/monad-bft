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

    // A "block archive" is a directory containing event capture files, where
    // one file contains the execution events for one finalized block. The block
    // archive _API_ is a convenience API that opens a "capture file reader",
    // given a finalized block number as input.
    let block_archive_dir = std::fs::File::open(block_archive_path).unwrap();
    let block_archive = BlockCaptureBlockArchive::new(&block_archive_dir).unwrap();

    // We start reading at the specified block number, then increment forever
    for block_number in start_block_number.. {
        // First we open an event capture file reader for the current block
        // number; this is a loop because once we are fully caught up, we have
        // to wait for the next block's capture file to be written. We don't
        // have to worry about it being partially-written: it will atomically
        // appear in the filesystem, fully created.
        //
        // If we get a file-not-found error, we'll sleep for 100 milliseconds
        // and then try again. If we get any other kind of error, it's fatal,
        // and we'll panic.
        //
        // Real applications need to do something more sophisticated when too
        // much time has gone by without a new file being produced. Category
        // Labs has a disaster recovery archive containing any blocks you might
        // be missing locally, due to operational outages.
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

        // Event capture files are organized into sections. This is because they
        // are also used for other purposes -- like profiling and research --
        // and can contain additional data that we don't care about for this use
        // case.
        //
        // For this use case, the execution event capture files contain a single
        // interesting "event section": a contiguous slab of memory that holds
        // all of the execution events that were written for a particular
        // finalized block. These event sections are usually zstd-compressed,
        // and opening the event section will decompress it (the section's Drop
        // trait implementation will free the de-compressed memory).
        let event_section = event_capture_reader
            .next_event_section()
            .expect("BlockArchive evcap file contains an event section");

        // Once we have an open event section, we can open an iterator to the
        // events it contains, and iterate through those events. This is done
        // in a helper function, which will use block builder utility to
        // aggregate all of the event data into a block-level update.
        let block = extract_block(event_section);

        // Finally, turn the block header into "alloy form" and Debug dump it,
        // so the user can see something interesting happen.
        let block_header = block.to_alloy_header();

        println!("[block {block_number}]: {block_header:#?}");
    }
}

fn extract_block(event_section: EventCaptureEventSection) -> ExecutedBlock {
    // Working with event capture files is similar to working with real-time
    // event rings: we open a new iterator and iterate through execution events.
    // The contents (the execution events) are exactly the same.
    //
    // The differences are that the event capture iterator cannot "gap", and
    // the iteration ends permenantly once we have seen all the events in the
    // capture file.
    //
    // The "descriptor" and "payload" concepts exist for event capture files
    // too, because they are forensic captures of the original events from the
    // event ring. The types are slightly different in the Rust API, because
    // some associated methods are implemented in a different way, but the
    // underlying data is the same (e.g., the timestamps are the original
    // timestamps from the EVM).
    let mut event_iter: ExecEventCaptureEventIter<'_> = event_section.open_iterator();

    // The block builder utility works with event capture descriptors too.
    // You should _not_ use the CommitStateBlockBuilder, because the finalized
    // block capture files don't store any consensus events. That is, there are
    // no BLOCK_QC, BLOCK_FINALIZED, or BLOCK_VERIFIED events stored in block
    // capture files (they were discarded by monad-blockcapd).
    //
    // If you used a CommitStateBlockBuilder here by accident, it would create
    // new in-progress proposals forever, waiting for a consensus event to
    // occur, which will never happen.
    let mut block_builder = ExecutedBlockBuilder::new(true, true);

    // Loop through all the events and feed them to the block builder
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
                // When you read all events in the capture file, it returns "End"; the reason
                // we panic here is that last event should cause the block builder to complete
                // first, which should stop the iteration, thus we should never reach here;
                // if we did, the file must have been malformed (e.g., missing the BLOCK_END event)
                panic!("BlockArchive evcap file contains a single block, instead reached `End`")
            }
            EventCaptureNextResult::NoSeqno => {
                // This return code is never returned by `next_descriptor()`; it is returned
                // by other APIs which are not available (and not needed yet) in this version
                // of the SDK
                panic!("BlockArchive evcap file contains a single block, instaed reached `NoSeqno`")
            }
        }
    };

    // This demonstrates that, if we were to read one more event after the
    // BLOCK_END that completed the block, it will formally return that the
    // event capture section has no more events
    assert!(
        matches!(event_iter.next_descriptor(), EventCaptureNextResult::End),
        "BlockArchive evcap file ends after block"
    );

    block
}
