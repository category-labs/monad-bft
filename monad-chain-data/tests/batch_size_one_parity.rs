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

use monad_chain_data::{
    Address, Bytes, FinalizedBlock, InMemoryBlobStore, InMemoryMetaStore, Log, LogData,
    MonadChainDataService, QueryLimits, B256,
};

mod common;

use common::test_header;
use monad_chain_data::EvmBlockHeader;

fn varied_log(seed: u64) -> Log {
    let addr = Address::repeat_byte((seed % 251) as u8);
    let topic = B256::repeat_byte((seed % 113) as u8);
    let data = Bytes::from(seed.to_be_bytes().to_vec());
    Log {
        address: addr,
        data: LogData::new_unchecked(vec![topic], data),
    }
}

fn make_chain(n: usize) -> Vec<FinalizedBlock> {
    let mut out: Vec<FinalizedBlock> = Vec::with_capacity(n);
    let mut parent = B256::ZERO;
    for i in 0..n {
        let header: EvmBlockHeader = test_header((i + 1) as u64, parent);
        parent = header.hash_slow();
        let log_count = ((i * 5) % 7) + 1;
        let logs: Vec<Log> = (0..log_count)
            .map(|j| varied_log((i * 31 + j) as u64))
            .collect();
        out.push(FinalizedBlock {
            header,
            logs_by_tx: vec![logs],
            txs: Vec::new(),
            traces: vec![],
        });
    }
    out
}

#[tokio::test(flavor = "current_thread")]
async fn batch_size_one_observationally_identical_to_batch_size_eight() {
    let blocks = make_chain(32);

    let meta_a = InMemoryMetaStore::default();
    let blob_a = InMemoryBlobStore::default();
    let service_a = MonadChainDataService::new(meta_a.clone(), blob_a.clone(), QueryLimits::UNLIMITED);
    for b in blocks.clone() {
        service_a
            .ingest_blocks(vec![b])
            .await
            .expect("batch_size=1 ingest");
    }

    let meta_b = InMemoryMetaStore::default();
    let blob_b = InMemoryBlobStore::default();
    let service_b = MonadChainDataService::new(meta_b.clone(), blob_b.clone(), QueryLimits::UNLIMITED);
    for chunk in blocks.chunks(8) {
        service_b
            .ingest_blocks(chunk.to_vec())
            .await
            .expect("batch_size=8 ingest");
    }

    assert_eq!(
        meta_a.kv_snapshot(),
        meta_b.kv_snapshot(),
        "kv rows diverged byte-for-byte between batch_size=1 and batch_size=8"
    );
    assert_eq!(
        meta_a.scan_snapshot(),
        meta_b.scan_snapshot(),
        "scan rows diverged byte-for-byte between batch_size=1 and batch_size=8"
    );
    // CAS row *values* must match byte-for-byte. The CAS *version* counter
    // is intentionally not compared: it increments once per ingest call, so
    // batch_size=1 reaches version=N while batch_size=8 reaches version=N/8.
    // That divergence is by design — fencing only cares about monotonicity,
    // not absolute count — and is observable to writers but not to readers.
    let cas_values_a: std::collections::BTreeMap<_, _> = meta_a
        .cas_snapshot()
        .into_iter()
        .map(|(k, (_, v))| (k, v))
        .collect();
    let cas_values_b: std::collections::BTreeMap<_, _> = meta_b
        .cas_snapshot()
        .into_iter()
        .map(|(k, (_, v))| (k, v))
        .collect();
    assert_eq!(
        cas_values_a, cas_values_b,
        "cas row values diverged byte-for-byte between batch_size=1 and batch_size=8"
    );
    assert_eq!(
        blob_a.blob_snapshot(),
        blob_b.blob_snapshot(),
        "blob rows diverged byte-for-byte between batch_size=1 and batch_size=8"
    );

    let head_a = service_a
        .publication()
        .load_published_head()
        .await
        .unwrap();
    let head_b = service_b
        .publication()
        .load_published_head()
        .await
        .unwrap();
    assert_eq!(head_a, head_b);
    assert_eq!(head_a, Some(blocks.len() as u64));
}
