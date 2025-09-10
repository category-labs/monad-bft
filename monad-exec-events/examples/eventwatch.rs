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

use std::{ffi::CStr, path::PathBuf, time::Duration};

use chrono::{Local, TimeZone};
use clap::Parser;
use lazy_static::lazy_static;
use monad_event_ring::{
    DecodedEventRing, EventDescriptor, EventDescriptorInfo, EventNextResult, EventPayloadResult,
};
use monad_exec_events::{
    ffi::{g_monad_exec_event_metadata, DEFAULT_FILE_NAME, MONAD_EXEC_EVENT_COUNT},
    ExecEventDecoder, ExecEventDescriptorExt, ExecEventReaderExt, ExecEventRing, ExecEventType,
};

lazy_static! {
    static ref EXEC_EVENT_NAMES: [&'static str; MONAD_EXEC_EVENT_COUNT] =
        std::array::from_fn(|event_type| unsafe {
            CStr::from_ptr(g_monad_exec_event_metadata[event_type].c_name)
                .to_str()
                .unwrap()
        });
}

#[derive(Debug, Parser)]
#[command(name = "eventwatch", about, long_about = None)]
pub struct Cli {
    #[arg(long)]
    event_ring_path: Option<PathBuf>,

    #[arg(short, long)]
    dump_payload: bool,
}

/// Print a summary line of this event
/// <YYYY-MM-DDTHH:MM::SS.nanos-TZ> <event-c-name> [<event-type> <event-type-hex>]
///     SEQ: <sequence-no>
fn print_event(event: &EventDescriptor<ExecEventDecoder>, dump_payload: bool) -> bool {
    let EventDescriptorInfo {
        seqno,
        event_type,
        record_epoch_nanos,
        flow_info,
    } = event.info();

    let event_time_tz = Local
        .timestamp_nanos(record_epoch_nanos as i64)
        .format("%H:%M:%S.%9f");

    let event_name = EXEC_EVENT_NAMES[event_type as usize];

    // Format the fields present for all events
    print!("{event_time_tz} {event_name} [{event_type} {event_type:#x}] SEQ: {seqno}");

    // Some events have an associated block number and transaction number;
    // print those now
    if flow_info.block_seqno != 0 {
        let block_number = event.get_block_number().unwrap();
        print!(" BLK: {block_number}");
    }
    if let Some(i) = flow_info.txn_idx {
        print!(" TXN: {i}");
    }
    println!();

    let exec_event = match event.try_read() {
        EventPayloadResult::Expired => {
            eprintln!("ERROR: payload expired!");
            return false;
        }
        EventPayloadResult::Ready(exec_event) => exec_event,
    };

    if dump_payload {
        // One advantage of the Rust SDK over the C SDK is the #[derive(Debug)]
        // attribute on ExecEvent decoded representation; this is helpful for
        // debugging
        println!("Payload: {exec_event:x?}");
    }
    true
}

fn main() {
    let Cli {
        event_ring_path,
        dump_payload,
    } = Cli::parse();

    // The first step is to open an event ring file. Creating the event ring
    // object will also mmap(2) the event rings' shared memory segments into
    // our process' address space. The mappings will be removed when `drop()`
    // is called on the ring, so it must stay alive while we read from it
    let event_ring =
        ExecEventRing::new_from_path(event_ring_path.unwrap_or(PathBuf::from(DEFAULT_FILE_NAME)))
            .unwrap();

    // Create a reader, which allows us to poll for new events
    let mut event_reader = event_ring.create_reader();

    // Ensure we start on a block boundary
    event_reader.consensus_prev(Some(ExecEventType::BlockStart));

    // The event processing loop of the application
    loop {
        match event_reader.next_descriptor() {
            EventNextResult::Gap => {
                eprintln!("ERROR: event sequence number gap occurred!");
                event_reader.reset();
                continue;
            }
            EventNextResult::NotReady => {
                std::thread::sleep(Duration::from_micros(100));
                continue;
            }
            EventNextResult::Ready(event) => {
                if !print_event(&event, dump_payload) {
                    event_reader.reset(); // Payload expired
                }
            }
        };
    }
}
