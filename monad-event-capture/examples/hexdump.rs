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
use monad_event::BytesDecoder;
use monad_event_capture::{
    EventCaptureEventIter, EventCaptureFile, EventCaptureNextResult, EventCaptureSectionType,
};

#[derive(Debug, Parser)]
#[command(name = "monad-event-capture-hexdump", about, long_about = None)]
pub struct Cli {
    #[arg(long)]
    event_capture_path: PathBuf,

    #[arg(short, long, default_value_t = 32)]
    width: usize,
}

// extern "C" {
#[no_mangle]
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
        event_capture_path,
        width,
    } = Cli::parse();

    let event_capture_file = EventCaptureFile::open(event_capture_path).unwrap();

    let mut event_capture_reader = event_capture_file.create_reader().unwrap();

    while let Some(section_descriptor) =
        event_capture_reader.next_section(Some(EventCaptureSectionType::EventBundle))
    {
        assert_eq!(
            section_descriptor.section_type(),
            Some(EventCaptureSectionType::EventBundle)
        );

        let event_section = section_descriptor
            .open_event_section()
            .expect("EventBundle section");

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
                    .join("\n               ")
            );
        }
    }
}
