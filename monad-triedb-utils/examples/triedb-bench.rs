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

use std::{
    collections::BTreeMap,
    ops::RangeInclusive,
    path::PathBuf,
    time::{Duration, Instant},
};

use clap::Parser;
use futures::{future::LocalBoxFuture, stream::FuturesUnordered, FutureExt, StreamExt};
use monad_triedb::TriedbHandle;
use monad_triedb_utils::triedb_env::{BlockKey, FinalizedBlockKey, Triedb, TriedbEnv};
use monad_types::SeqNum;
use rand::{rngs::StdRng, Rng, SeedableRng};
use rand_distr::Distribution;
use serde::{Deserialize, Serialize};

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    pub triedb_path: PathBuf,

    #[arg(long, default_value_t = 0)]
    pub seed: u64,

    #[arg(long, default_value_t = 5.0)]
    pub duration_s: f64,

    /// maximum number of blocks to sample requests to
    #[arg(long, default_value_t = 1_000)]
    pub max_num_blocks: u64,

    #[arg(long, default_value_t = 100 << 20)] // 100MB
    pub node_lru_max_mem: u64,

    #[arg(long, default_value_t = 20_000)]
    pub max_buffered_read_requests: usize,
    #[arg(long, default_value_t = 10_000)]
    pub max_async_read_concurrency: usize,
    #[arg(long, default_value_t = 40)]
    pub max_buffered_traverse_requests: usize,
    #[arg(long, default_value_t = 20)]
    pub max_async_traverse_concurrency: usize,
    #[arg(long, default_value_t = 200)]
    pub max_finalized_block_cache_len: usize,
    #[arg(long, default_value_t = 3)]
    pub max_voted_block_cache_len: usize,

    #[arg(long, default_value_t = 0.0)]
    pub tx_traverse_rps: f64,
    #[arg(long, default_value_t = 0.0)]
    pub receipts_traverse_rps: f64,
    #[arg(long, default_value_t = 0.0)]
    pub block_by_number_rps: f64,
}

fn generate_event_times(rng: impl Rng, test_duration: Duration, rps: f64) -> Vec<Duration> {
    if rps == 0.0 {
        return Vec::new();
    }

    let exp = rand_distr::Exp::new(rps).unwrap();

    let mut event_times = Vec::new();
    let mut accumulated_delay = Duration::ZERO;
    for delay in exp.sample_iter(rng) {
        accumulated_delay += Duration::from_secs_f64(delay);
        if accumulated_delay > test_duration {
            break;
        }

        event_times.push(accumulated_delay);
    }

    event_times
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let mut rng = StdRng::seed_from_u64(args.seed);

    let block_range = {
        let reader = TriedbHandle::try_new(&args.triedb_path, args.node_lru_max_mem)
            .unwrap_or_else(|| panic!("failed to open triedb path: {:?}", &args.triedb_path));
        let latest_block = reader
            .latest_finalized_block()
            .expect("no latest_finalized_block in db");
        let earliest_block = reader
            .earliest_finalized_block()
            .expect("no earliest_finalized_block in db")
            .max(latest_block.saturating_sub(args.max_num_blocks));
        earliest_block..=latest_block
    };

    let triedb_handle = TriedbEnv::new(
        &args.triedb_path,
        args.node_lru_max_mem,
        args.max_buffered_read_requests,
        args.max_async_read_concurrency,
        args.max_buffered_traverse_requests,
        args.max_async_traverse_concurrency,
        args.max_finalized_block_cache_len,
        args.max_voted_block_cache_len,
    );

    let test_duration = Duration::from_secs_f64(args.duration_s);

    let event_schedule = EventSchedule::create(
        &mut rng,
        &block_range,
        test_duration,
        args.tx_traverse_rps,
        args.receipts_traverse_rps,
        args.block_by_number_rps,
    );
    let num_events = event_schedule.all_events.len();

    println!(
        "sending {} requests in block_range={:?}, duration={:?}",
        num_events, block_range, test_duration
    );

    let mut futs = FuturesUnordered::new();
    let mut response_times: BTreeMap<TaggedRequest, Instant> = BTreeMap::new();
    // returns true if pending futures are empty
    let poll_futs = |futs: &mut FuturesUnordered<LocalBoxFuture<Result<TaggedRequest, String>>>,
                     response_times: &mut BTreeMap<TaggedRequest, Instant>| {
        while let Some(maybe_request_result) = futs.next().now_or_never() {
            let Some(request_result) = maybe_request_result else {
                // stream is terminated
                return true;
            };
            let request = request_result.expect("request failed");
            response_times.insert(request, Instant::now());
        }
        false
    };

    let start_time = Instant::now();
    for (event_time_delta, event) in event_schedule.all_events {
        let event_time = start_time + event_time_delta;
        while Instant::now() < event_time {
            let _done = poll_futs(&mut futs, &mut response_times);
        }
        let tagged_request: TaggedRequest = (Instant::now(), event);
        match event {
            Request::TxTraverse(block_num) => {
                let fut = triedb_handle
                    .get_transactions(BlockKey::Finalized(FinalizedBlockKey(SeqNum(block_num))));
                futs.push(
                    fut.map(move |result| result.map(move |_txs| tagged_request))
                        .boxed_local(),
                );
            }
            Request::ReceiptsTraverse(block_num) => {
                let fut = triedb_handle
                    .get_receipts(BlockKey::Finalized(FinalizedBlockKey(SeqNum(block_num))));
                futs.push(
                    fut.map(move |result| result.map(move |_receipts| tagged_request))
                        .boxed_local(),
                );
            }
            Request::BlockByNumber(block_num) => {
                let fut = triedb_handle
                    .get_block_header(BlockKey::Finalized(FinalizedBlockKey(SeqNum(block_num))));
                futs.push(
                    fut.map(move |result| result.map(move |_block| tagged_request))
                        .boxed_local(),
                );
            }
        }
    }

    while let Some(request_result) = futs.next().await {
        let request = request_result.expect("request failed");
        response_times.insert(request, Instant::now());
    }

    assert_eq!(num_events, response_times.len());

    let first_event_time = response_times.keys().next().expect("no requests sent").0;
    let mut trace_events = Vec::new();
    for (request_id, ((request_time, request), response_time)) in
        response_times.into_iter().enumerate()
    {
        let _elapsed = response_time - request_time;
        trace_events.push(
            TraceEvent::async_start(
                &format!("{:06}-{:?}", request_id, request),
                (request_time - first_event_time).as_micros() as u64,
                request_id as u64,
            )
            .with_category(request.category()),
        );
        trace_events.push(
            TraceEvent::async_end(
                &format!("{:06}-{:?}", request_id, request),
                (response_time - first_event_time).as_micros() as u64,
                request_id as u64,
            )
            .with_category(request.category()),
        );
    }
    trace_events.sort_by_key(|event| event.ts);
    let trace = TraceFile { trace_events };
    println!("{}", serde_json::to_string(&trace).unwrap());
}

struct EventSchedule {
    tx_traverse_events: Vec<Duration>,
    receipts_traverse_events: Vec<Duration>,
    block_by_number_events: Vec<Duration>,
    all_events: Vec<(Duration, Request)>,
}

impl EventSchedule {
    fn create(
        mut rng: impl Rng,
        block_range: &RangeInclusive<u64>,
        test_duration: Duration,
        tx_traverse_rps: f64,
        receipts_traverse_rps: f64,
        block_by_number_rps: f64,
    ) -> Self {
        let tx_traverse_events = generate_event_times(&mut rng, test_duration, tx_traverse_rps);
        let receipts_traverse_events =
            generate_event_times(&mut rng, test_duration, receipts_traverse_rps);
        let block_by_number_events =
            generate_event_times(&mut rng, test_duration, block_by_number_rps);

        let mut all_events = Vec::new();
        all_events.extend(tx_traverse_events.iter().map(|&event_time| {
            (
                event_time,
                Request::TxTraverse(rng.gen_range(block_range.clone())),
            )
        }));
        all_events.extend(receipts_traverse_events.iter().map(|&event_time| {
            (
                event_time,
                Request::ReceiptsTraverse(rng.gen_range(block_range.clone())),
            )
        }));
        all_events.extend(block_by_number_events.iter().map(|&event_time| {
            (
                event_time,
                Request::BlockByNumber(rng.gen_range(block_range.clone())),
            )
        }));
        all_events.sort();
        Self {
            tx_traverse_events,
            receipts_traverse_events,
            block_by_number_events,
            all_events,
        }
    }
}

type TaggedRequest = (Instant, Request);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum Request {
    TxTraverse(u64),
    ReceiptsTraverse(u64),
    BlockByNumber(u64),
}

impl Request {
    fn category(&self) -> &'static str {
        match self {
            Request::TxTraverse(_) => "TxTraverse",
            Request::ReceiptsTraverse(_) => "ReceiptsTraverse",
            Request::BlockByNumber(_) => "BlockByNumber",
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct TraceFile {
    #[serde(rename = "traceEvents")]
    trace_events: Vec<TraceEvent>,
}

#[derive(Serialize, Deserialize, Debug)]
struct TraceEvent {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    cat: Option<String>,
    ph: String,
    ts: u64, // microseconds
    #[serde(default = "default_pid")]
    pid: u32,
    #[serde(default = "default_tid")]
    tid: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dur: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    args: Option<serde_json::Value>,
}

fn default_pid() -> u32 {
    1
}
fn default_tid() -> u32 {
    1
}

impl TraceEvent {
    fn new(
        name: &str,
        phase: &str,
        timestamp: u64,
        id: Option<u64>,
        duration: Option<u64>,
        category: Option<String>,
        args: Option<serde_json::Value>,
    ) -> Self {
        TraceEvent {
            name: name.to_string(),
            cat: category,
            ph: phase.to_string(),
            ts: timestamp,
            pid: default_pid(),
            tid: default_tid(),
            id,
            dur: duration,
            args,
        }
    }

    // Simplified helper methods that only require essential parameters
    fn complete(name: &str, timestamp: u64, duration: u64) -> Self {
        Self::new(name, "X", timestamp, None, Some(duration), None, None)
    }

    fn async_start(name: &str, timestamp: u64, id: u64) -> Self {
        Self::new(name, "b", timestamp, Some(id), None, None, None)
    }

    fn async_end(name: &str, timestamp: u64, id: u64) -> Self {
        Self::new(name, "e", timestamp, Some(id), None, None, None)
    }

    // Add category and args if needed
    fn with_category(mut self, category: &str) -> Self {
        self.cat = Some(category.to_string());
        self
    }
}
