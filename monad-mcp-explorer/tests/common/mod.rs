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

#![allow(dead_code)]

use std::{
    fs,
    path::Path,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use actix_web::{
    App,
    body::MessageBody,
    dev::{Service, ServiceResponse},
    http::StatusCode,
    test, web,
};
use bytes::Bytes;
use monad_mcp_chorus::ledger::{
    BLOCKS_DIR, BlockMeta, CommittedLane, FinalizationPath, LEDGER_VERSION, LaneMeta, LedgerReader,
    LedgerWriter, META_FILE, NewBlock, NewLane, Tx, block_dir_name, encode_batch,
};
use monad_mcp_explorer::{
    api::{self, AppState},
    app_state,
    index::{Index, IndexConfig},
    loader::{Loader, LoaderConfig, Progress, SharedIndex},
};
use serde_json::Value;

pub const SLOT_NS: u128 = 100_000_000;
pub const GENESIS_NS: u128 = 1_700_000_000_000_000_000;

pub fn tx(sender: u8, nonce: u64, payload: impl Into<Bytes>) -> Tx {
    Tx {
        sender: [sender; 20],
        nonce,
        payload: payload.into(),
    }
}

pub fn finalized_at(slot: u64) -> u128 {
    GENESIS_NS + u128::from(slot) * SLOT_NS + 12_000_000
}

pub fn path_of(slot: u64) -> FinalizationPath {
    if slot.is_multiple_of(5) {
        FinalizationPath::Fallback
    } else {
        FinalizationPath::Fast
    }
}

pub enum Lane {
    Negative,
    Txs(Vec<Tx>),
    Raw(Bytes),
}

pub fn proof_of(slot: u64) -> Bytes {
    Bytes::from(format!("proof-for-{slot}").into_bytes())
}

// a block written through the real writer.
pub fn write_block(writer: &LedgerWriter, slot: u64, lanes: Vec<Lane>) -> BlockMeta {
    let lanes = (0u8..)
        .zip(lanes)
        .map(|(j, lane)| {
            let payload = match lane {
                Lane::Negative => {
                    return NewLane {
                        proposer: None,
                        committed: None,
                    };
                }
                Lane::Txs(txs) => encode_batch(&txs),
                Lane::Raw(raw) => raw,
            };
            NewLane {
                proposer: Some(100 + u64::from(j)),
                committed: Some(CommittedLane {
                    root: [j + 1; 20],
                    payload,
                }),
            }
        })
        .collect();
    writer
        .write(&NewBlock {
            slot,
            deadline_ns: Some(GENESIS_NS + u128::from(slot) * SLOT_NS),
            finalized_at_ns: finalized_at(slot),
            path: path_of(slot),
            lanes,
            proof: proof_of(slot),
        })
        .unwrap()
}

pub fn write_empty(writer: &LedgerWriter, slot: u64) -> BlockMeta {
    write_block(
        writer,
        slot,
        vec![Lane::Txs(vec![]), Lane::Negative, Lane::Txs(vec![])],
    )
}

// a txless block written without fsync, for large synthetic ledgers.
pub fn write_raw_empty(ledger_dir: &Path, slot: u64, num_lanes: u32) {
    let lanes = (0..num_lanes)
        .map(|index| LaneMeta {
            index,
            proposer: Some(u64::from(index)),
            root: (index % 2 == 0).then_some([7; 20]),
            payload_len: u32::from(index % 2 == 0),
            tx_count: 0,
            decode_error: false,
        })
        .collect();
    let meta = BlockMeta {
        version: LEDGER_VERSION,
        slot,
        deadline_ns: Some(GENESIS_NS + u128::from(slot) * SLOT_NS),
        finalized_at_ns: finalized_at(slot),
        path: path_of(slot),
        num_lanes,
        lanes,
    };
    let dir = ledger_dir.join(BLOCKS_DIR).join(block_dir_name(slot));
    fs::create_dir_all(&dir).unwrap();
    for index in (0..num_lanes).filter(|i| i % 2 == 0) {
        fs::write(dir.join(format!("lane-{index}.rlp")), [0xc0]).unwrap();
    }
    fs::write(dir.join("proof.rlp"), proof_of(slot)).unwrap();
    fs::write(dir.join(META_FILE), meta.to_rlp()).unwrap();
}

pub fn fast_loader() -> LoaderConfig {
    LoaderConfig {
        tail_interval: Duration::from_millis(20),
        ..LoaderConfig::default()
    }
}

pub struct Explorer {
    pub index: SharedIndex,
    pub progress: Arc<Progress>,
    pub state: web::Data<AppState>,
    pub loader: Loader,
}

pub fn start(ledger_dir: &Path, config: IndexConfig, loader: LoaderConfig) -> Explorer {
    let reader = LedgerReader::new(ledger_dir);
    let index = SharedIndex::new(Index::new(config));
    let progress = Arc::new(Progress::default());
    let loader = Loader::spawn(reader.clone(), index.clone(), progress.clone(), loader);
    let state = app_state(
        reader,
        index.clone(),
        progress.clone(),
        "http://rpc.test:1234".into(),
    );
    Explorer {
        index,
        progress,
        state,
        loader,
    }
}

pub fn wait_until(what: &str, timeout: Duration, mut cond: impl FnMut() -> bool) {
    let started = Instant::now();
    while !cond() {
        assert!(started.elapsed() < timeout, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(5));
    }
}

impl Explorer {
    pub fn wait_booted(&self) {
        wait_until("boot load", Duration::from_secs(30), || {
            self.progress.is_complete()
        });
    }

    pub async fn app(
        &self,
    ) -> impl Service<
        actix_http::Request,
        Response = ServiceResponse<impl MessageBody>,
        Error = actix_web::Error,
    > {
        test::init_service(
            App::new()
                .app_data(self.state.clone())
                .configure(api::configure),
        )
        .await
    }
}

pub async fn get<S, B>(app: &S, uri: &str) -> (StatusCode, Value, usize)
where
    S: Service<actix_http::Request, Response = ServiceResponse<B>, Error = actix_web::Error>,
    B: MessageBody,
{
    let resp = test::call_service(app, test::TestRequest::get().uri(uri).to_request()).await;
    let status = resp.status();
    let body = test::read_body(resp).await;
    let value = serde_json::from_slice(&body)
        .unwrap_or_else(|e| panic!("{uri}: non-json body ({e}): {body:?}"));
    (status, value, body.len())
}

pub async fn ok<S, B>(app: &S, uri: &str) -> Value
where
    S: Service<actix_http::Request, Response = ServiceResponse<B>, Error = actix_web::Error>,
    B: MessageBody,
{
    let (status, value, _) = get(app, uri).await;
    assert_eq!(status, StatusCode::OK, "{uri}: {value}");
    value
}
