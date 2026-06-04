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

//! Standby artifact-checksum verification: the per-block content-digest chain
//! is batch-independent, surfaced in every `BlockRecord` and the published
//! head, and detects divergence at the first offending block.

use monad_chain_data::{
    Address, Bytes, EvmBlockHeader, FinalizedBlock, InMemoryBlobStore, InMemoryMetaStore, Log,
    LogData, MonadChainDataService, QueryLimits, VerifyOutcome, B256,
};

mod common;

use common::test_header;

fn log(byte: u8) -> Log {
    Log {
        address: Address::repeat_byte(byte),
        data: LogData::new_unchecked(vec![B256::repeat_byte(byte)], Bytes::from(vec![byte, 2, 3])),
    }
}

/// A deterministic chain where block `i` (1-based) carries `i` logs with a
/// per-block marker byte. Pass `tamper_at` to perturb one block's log payload
/// without changing any header (so block hashes and contiguity are unchanged).
fn make_chain(n: usize, tamper_at: Option<u64>) -> Vec<FinalizedBlock> {
    let mut out: Vec<FinalizedBlock> = Vec::with_capacity(n);
    let mut parent = B256::ZERO;
    for i in 0..n {
        let number = (i + 1) as u64;
        let header: EvmBlockHeader = test_header(number, parent);
        parent = header.hash_slow();
        let marker = if tamper_at == Some(number) {
            0xff
        } else {
            (i + 1) as u8
        };
        out.push(FinalizedBlock {
            header,
            logs_by_tx: vec![std::iter::repeat_with(|| log(marker)).take(i + 1).collect()],
            txs: Vec::new(),
            traces: vec![],
        });
    }
    out
}

fn service() -> MonadChainDataService<InMemoryMetaStore, InMemoryBlobStore> {
    MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    )
}

/// The stored per-block checksums (and the published head checksum) are
/// identical regardless of how ingest was batched — the property that lets a
/// standby on a different batching scheme verify the primary's artifacts.
#[tokio::test(flavor = "current_thread")]
async fn checksum_chain_is_batch_independent() {
    let blocks = make_chain(6, None);

    // One big batch.
    let one_shot = service();
    one_shot
        .ingest_blocks(blocks.clone())
        .await
        .expect("batch ingest");

    // Single-block batches.
    let per_block = service();
    for b in blocks.clone() {
        per_block.ingest_block(b).await.expect("single ingest");
    }

    // Mixed batches (2 + 1 + 3).
    let mixed = service();
    mixed
        .ingest_blocks(blocks[0..2].to_vec())
        .await
        .expect("ingest 1..2");
    mixed
        .ingest_blocks(blocks[2..3].to_vec())
        .await
        .expect("ingest 3");
    mixed
        .ingest_blocks(blocks[3..6].to_vec())
        .await
        .expect("ingest 4..6");

    for number in 1..=blocks.len() as u64 {
        let a = one_shot
            .tables()
            .blocks()
            .load_record(number)
            .await
            .unwrap()
            .unwrap();
        let b = per_block
            .tables()
            .blocks()
            .load_record(number)
            .await
            .unwrap()
            .unwrap();
        let c = mixed
            .tables()
            .blocks()
            .load_record(number)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(a.artifact_checksum, b.artifact_checksum, "block {number}");
        assert_eq!(a.artifact_checksum, c.artifact_checksum, "block {number}");
    }

    // The published head checksum mirrors the head block's record checksum.
    let head_record = one_shot
        .tables()
        .blocks()
        .load_record(6)
        .await
        .unwrap()
        .unwrap();
    let (_, head_state) = one_shot.publication().load_state().await.unwrap().unwrap();
    assert_eq!(
        head_state.head_artifact_checksum,
        head_record.artifact_checksum
    );
    assert_ne!(head_record.artifact_checksum, B256::ZERO);
}

#[tokio::test(flavor = "current_thread")]
async fn verify_matches_when_inputs_agree() {
    let blocks = make_chain(5, None);
    let svc = service();
    svc.ingest_blocks(blocks.clone()).await.expect("ingest");

    // Whole run, and an arbitrary mid-chain slice — both verify.
    assert_eq!(
        svc.verify_artifact_checksums(&blocks).await.unwrap(),
        VerifyOutcome::Match { through_block: 5 }
    );
    assert_eq!(
        svc.verify_artifact_checksums(&blocks[2..4]).await.unwrap(),
        VerifyOutcome::Match { through_block: 4 }
    );
}

#[tokio::test(flavor = "current_thread")]
async fn verify_detects_divergent_artifacts() {
    let published = make_chain(5, None);
    let svc = service();
    svc.ingest_blocks(published).await.expect("ingest");

    // A standby that framed block 3 from different log data diverges there;
    // headers (and thus hashes/contiguity) are unchanged.
    let tampered = make_chain(5, Some(3));
    match svc.verify_artifact_checksums(&tampered).await.unwrap() {
        VerifyOutcome::Mismatch {
            block_number,
            published,
            computed,
        } => {
            assert_eq!(block_number, 3);
            assert_ne!(published, computed);
        }
        other => panic!("expected mismatch at block 3, got {other:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn verify_reports_pending_past_published_head() {
    let blocks = make_chain(4, None);
    let svc = service();
    // Publish only the first two blocks.
    svc.ingest_blocks(blocks[0..2].to_vec())
        .await
        .expect("ingest 1..2");

    match svc.verify_artifact_checksums(&blocks).await.unwrap() {
        VerifyOutcome::Pending { block_number } => assert_eq!(block_number, 3),
        other => panic!("expected pending at block 3, got {other:?}"),
    }
}
