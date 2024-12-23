//! Utility for printing monad execution events to stdout
//!
//! Given the slow performance of terminal devices, this exists primarily as
//! an example of how to use the monad-event library and as a "smoke test"
//! to check that everything is working, rather than as a practically useful
//! utility.

use std::{
    ffi::CStr,
    io::{Cursor, Seek, Write},
    path::PathBuf,
    process,
    sync::atomic::{AtomicU64, Ordering},
};

use chrono::{Local, TimeZone};
use clap::Parser;
use monad_exec_event::{
    event::{self, *},
    event_client::*,
    event_metadata,
    event_reader::*,
    event_types::{self, monad_event_type},
};

#[derive(Parser)]
#[command(about = "Utility for watching monad execution events")]
struct Cli {
    #[arg(short, long, help = "path to the server socket file")]
    #[arg(default_value = event::DEFAULT_SOCKET_PATH)]
    server: PathBuf,

    #[arg(short, long, help = "server socket timeout, in seconds; zero disables")]
    #[arg(default_value_t = 1)]
    timeout: u32,
}

fn print_event(
    reader: &mut EventReader,
    event: &monad_event_descriptor,
    thread_table: &[event_types::thread_info],
    block_header_table: &[event_types::block_exec_header],
    fmtcursor: &mut Cursor<&mut [u8]>,
    stdout_handle: &mut std::io::StdoutLock,
) {
    // Print a summary line of this event
    // <HH:MM::SS.nanos> <event-c-name> [<event-type> <event-type-hex>]
    //     SEQ: <sequence-no> LEN: <payload-length>
    //     SRC: <source-id> [<thread-name> <thread-id>]
    let event_time_tz = Local
        .timestamp_nanos(event.epoch_nanos as i64)
        .format("%H:%M:%S.%9f");
    let event_seqno = event.seqno.load(Ordering::Relaxed);

    // Unpack the event metadata
    let event_code = event.event_type as usize;
    let event_meta = &event_metadata::EVENT_METADATA[event_code];
    let event_name = event_meta.c_name;
    let event_length = event.get_length();

    // Unpack the thread information from the source ID
    let event_source_id = event.get_source_id();
    let thread_info = &thread_table[event_source_id as usize];
    let thread_id = thread_info.thread_id;
    let thread_name = CStr::from_bytes_until_nul(&thread_info.thread_name)
        .expect("malformed byte buffer?")
        .to_str()
        .expect("bad UTF-8 encoding?");

    // Format the fields present for all events
    let mut event_line = format!(
        "{event_time_tz} {event_name} [{event_code} \
{event_code:#x}] SEQ: {event_seqno} LEN: {event_length} SRC: {event_source_id} \
[{thread_name} {thread_id}]"
    );

    // Add extra information for events associated with blocks and transactions
    use monad_event_type::{TXN_END, TXN_LOG, TXN_START};
    let txn_num_opt: Option<u32> = match event.event_type {
        TXN_START | TXN_END | TXN_LOG => Some(event.get_txn_num()),
        _ => None,
    };
    match (event.get_block_flow_id(), txn_num_opt) {
        // Event is not associated with a block
        (0, None) => event_line.push('\n'),

        // Top-level block-related event (pertaining to the whole block but
        // not to any particular transaction, e.g., the BLOCK_START event).
        // Print the summary block info
        (block_flow_id, None) => {
            let block_header = &block_header_table[block_flow_id as usize];
            let (block_number, round) = (block_header.number, block_header.round);
            event_line.push_str(format!(" BLK: {block_number} [R: {round}]\n").as_str())
        }

        // Event is associated with a transaction in a block; print both
        // summary block info and the transaction number
        (block_flow_id, Some(txn_num)) => {
            let block_header = &block_header_table[block_flow_id as usize];
            let (block_number, round) = (block_header.number, block_header.round);
            event_line
                .push_str(format!(" BLK: {block_number} [R: {round}] TXN: {txn_num}\n").as_str())
        }
    }

    // payload_peek() must be called before reader.advance(), see the
    // commentary in the C++ version of this program (eventcap.cpp) for an
    // explanation
    let (payload, overwrite_seqno): (&[u8], &AtomicU64) = reader.payload_peek(event);
    if reader.advance() {
        let _ = stdout_handle.write(event_line.as_bytes());
    } else {
        let seqno = reader.last_seqno + 1;
        eprintln!("ERROR: event {seqno} lost during copy-out");
        return;
    }

    // Format a hexdump of the event payload
    let _ = fmtcursor.rewind();
    for (line, line_bytes) in payload.chunks(16).enumerate() {
        // Print one line of the dump, which is 16 bytes, in the form:
        // <offset> <8 byte chunk> <8 byte chunk>
        let _ = write!(fmtcursor, "{:#08x} ", line * 16);
        let mut c = line_bytes.chunks(8);
        if let Some(bytes) = c.next() {
            for b in bytes {
                let _ = write!(fmtcursor, "{:02x}", b);
            }
        }
        if let Some(bytes) = c.next() {
            let _ = write!(fmtcursor, " ");
            for b in bytes {
                let _ = write!(fmtcursor, "{:02x}", b);
            }
        }
        let _ = writeln!(fmtcursor);

        // Every 512 bytes (32 16-byte lines), check if the payload page data
        // is still valid; the initial line bias avoids checking on the first
        // iteration
        if (line + 1) % 32 == 0 && overwrite_seqno.load(Ordering::Acquire) > event_seqno {
            break; // Escape to the end, which checks the final time
        }
    }

    if overwrite_seqno.load(Ordering::Acquire) > event_seqno {
        eprintln!("ERROR: event {event_seqno} payload lost!");
    } else {
        let _ = stdout_handle.write(&fmtcursor.get_ref()[..fmtcursor.position() as usize]);
    }
}

fn event_loop(imported_ring: ImportedEventRing) {
    let mut not_ready_count: u64 = 0;
    let mut reader = EventReader::new(&imported_ring);
    let stdout = std::io::stdout();
    let mut stdout_handle = stdout.lock();

    // This is only for print_event, but given the strangeness of thread_local
    // in Rust, we just create this here and pass it in
    let mut fmtbuf = vec!(0u8; 1 << 25).into_boxed_slice();
    let mut fmtcursor: Cursor<&mut [u8]> = Cursor::new(fmtbuf.as_mut());
    let event_proc = &imported_ring.parent;

    loop {
        match reader.peek() {
            PeekResult::NotReady => {
                not_ready_count += 1;
                if not_ready_count & ((1 << 20) - 1) == 0 {
                    let _ = stdout_handle.flush();
                    if !event_proc.is_connected() {
                        break;
                    }
                }
            }
            PeekResult::Gap(event) => {
                eprintln!(
                    "event gap from {} to {}",
                    reader.last_seqno,
                    event.seqno.load(Ordering::Relaxed)
                );
                reader.reset();
                continue;
            }
            PeekResult::Ready(event) => {
                not_ready_count = 0;
                print_event(
                    &mut reader,
                    event,
                    event_proc.thread_table,
                    event_proc.block_header_table,
                    &mut fmtcursor,
                    &mut stdout_handle,
                )
            }
        }
    }
}

fn main() {
    let cli = Cli::parse();

    let connect_options = ConnectOptions {
        socket_path: cli.server,
        timeout: libc::timeval {
            tv_sec: cli.timeout as libc::time_t,
            tv_usec: 0,
        },
    };

    match EventProc::connect(&connect_options) {
        Err(e) => {
            eprintln!("fatal error during IPC connect: {e}");
            process::exit(1);
        }
        Ok(exec_proc) => {
            match ImportedEventRing::import(exec_proc, EventRingType::Exec) {
                Err(e) => {
                    eprintln!("fatal error during event ring import: {e}");
                    process::exit(1);
                }
                Ok(imported_ring) => event_loop(imported_ring)
            }
        }
    }
}
