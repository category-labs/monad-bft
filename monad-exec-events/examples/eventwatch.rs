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

use chrono::{DateTime, Local, TimeZone};
use clap::Parser;
use lazy_static::lazy_static;
use monad_event_ring::{
    DecodedEventRing, EventDescriptor, EventDescriptorInfo, EventNextResult, EventPayloadResult,
    EventRingPath,
};
use monad_exec_events::{
    ffi::{g_monad_exec_event_metadata, DEFAULT_FILE_NAME, MONAD_EXEC_EVENT_COUNT},
    ExecEventDecoder, ExecEventDescriptorExt, ExecEventReaderExt, ExecEventRing, ExecEventType,
    ExecSnapshotEventRing,
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
            // The payload buffer is a circular buffer, similar to how the
            // event descriptor FIFO queue is. Much like how the EventReader
            // can gap if you don't consume events fast enough, trying to read
            // a payload from a live event ring could return that it has expired
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

// This example program works with two different kinds of events rings:
//
//   1. "Live" event rings -- these are a source of real-time data. In the
//      case of an event ring containing execution events, the Category Labs
//      execution daemon is writing EVM event notifications into them in
//      real time
//
//   2. "Snapshot" event rings -- these are compressed snapshots taken
//      of an event ring file as it existed at a particular moment in
//      time; they implicitly "rewind" to the first event in the queue
//      and to replay a fixed set of historical execution events. Snapshots
//      and are useful for testing and development workflows, because you do
//      not need to be running an active monad node to use them
//
// Using either kind of ring is largely the same, but there are some
// important differences. Because a snapshot is an "offline" image of
// historical events, it (1) can never gap and (2) as soon as it replays
// all of it events, there can't be any more.
//
// A live event ring is more complex, since it needs to be polled
// moment-to-moment, and there are some classic inter-process communication
// complexities (you need some kind of a "timeout" mechanism to detect when
// the execution process appears to be dead or hung).
enum OpenEventRing {
    Live(ExecEventRing),
    Snapshot(ExecSnapshotEventRing),
}

impl OpenEventRing {
    fn new(event_ring_path: EventRingPath) -> Result<Self, String> {
        if event_ring_path.is_snapshot_file()? {
            let snapshot = ExecSnapshotEventRing::new_from_zstd_path(event_ring_path, None)?;
            Ok(OpenEventRing::Snapshot(snapshot))
        } else {
            let live = ExecEventRing::new(event_ring_path)?;
            Ok(OpenEventRing::Live(live))
        }
    }
}

fn main() {
    let Cli {
        event_ring_path,
        dump_payload,
    } = Cli::parse();

    // The event ring shared memory data structure typically lives inside
    // of a regular file; any process that wants shared access to it, first
    // locates it via the filesystem, then maps a shared view of it into the
    // process' virtual memory map using the mmap(2) system call
    //
    // Most real-time programs take a path to the event ring file as a CLI
    // input parameter, but also allow it to be absent, in which case the
    // default file name is used.
    let event_ring_path =
        EventRingPath::resolve(event_ring_path.unwrap_or(PathBuf::from(DEFAULT_FILE_NAME)))
            .unwrap();

    // Try to open the event ring file, and exit if we can't
    let event_ring = OpenEventRing::new(event_ring_path).unwrap();

    let mut event_reader = match event_ring {
        OpenEventRing::Live(ref live) => {
            let mut event_reader = live.create_reader();

            // The EventReader for a live event ring has its initial iteration
            // point set to the most recently produced event. If our listening
            // process starts after the execution daemon has already been
            // running, then the "last written event" will usually be in the
            // middle of a block.
            //
            // This is rarely what we want, because most blockchain data
            // processing is inherently block-oriented. This function will
            // rewind the reader's iteration point so that it will always start
            // on a block boundary
            event_reader.consensus_prev(Some(ExecEventType::BlockStart));
            event_reader
        }
        OpenEventRing::Snapshot(ref snapshot) => snapshot.create_reader(),
    };

    // This is used to detect when the execution daemon has died
    let mut last_event_timestamp_ns: u64 = 0;

    // The event processing loop of the application
    loop {
        match event_reader.next_descriptor() {
            EventNextResult::Gap => {
                // Event rings use circular buffers to hold their data, and
                // live event rings can gap if the consumer does not keep up
                eprintln!("ERROR: event sequence number gap occurred!");
                event_reader.reset();
                continue;
            }
            EventNextResult::NotReady => {
                match event_ring {
                    OpenEventRing::Snapshot(_) => {
                        // A snapshot is always "ready" until it runs out of events;
                        // the first time it's not ready, it's finished, so exit
                        return;
                    }
                    OpenEventRing::Live(_) => {
                        let now = Local::now();
                        let last_event_time =
                            DateTime::from_timestamp_nanos(last_event_timestamp_ns as i64);
                        if now.signed_duration_since(last_event_time).num_seconds() > 5 {
                            // If a live execution daemon does not write a new event for
                            // five seconds, it's almost certainly dead; this is good enough
                            // for our example, but in a production-grade real-time data
                            // processing program, you will probably want to use a more
                            // sophisticated death detection mechanism
                            std::process::exit(0);
                        }
                        std::thread::sleep(Duration::from_micros(100));
                    }
                }
                continue;
            }
            EventNextResult::Ready(event) => {
                // We got an event; remember the timestamp and print it to stdout
                last_event_timestamp_ns = event.info().record_epoch_nanos;
                if !print_event(&event, dump_payload) {
                    event_reader.reset(); // Payload expired
                }
            }
        };
    }
}
