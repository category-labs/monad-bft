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
    engine::tables::PublicationTables, Address, Family, FinalizedBlock, InMemoryBlobStore,
    InMemoryMetaStore, IngestTx, MonadChainDataService, QueryLimits, B256,
};

mod common;

use common::{chain_header, ingest_tx, test_header};

#[tokio::test(flavor = "current_thread")]
async fn get_transaction_returns_entry_for_ingested_hash() {
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );

    let alice = Address::repeat_byte(0xaa);
    let bob = Address::repeat_byte(0xbb);
    let recipient = Address::repeat_byte(0x11);
    let hash_alice = B256::repeat_byte(0x01);
    let hash_bob = B256::repeat_byte(0x02);

    service
        .ingest_block(FinalizedBlock {
            header: test_header(1, B256::ZERO),
            logs_by_tx: vec![vec![], vec![]],
            txs: vec![
                with_hash(ingest_tx(alice, Some(recipient), Vec::new()), hash_alice),
                with_hash(ingest_tx(bob, Some(recipient), Vec::new()), hash_bob),
            ],
        })
        .await
        .expect("ingest");

    let entry = service
        .get_transaction(hash_bob)
        .await
        .expect("lookup")
        .expect("hit");
    assert_eq!(entry.block_number, 1);
    assert_eq!(entry.tx_idx, 1);
    assert_eq!(entry.tx_hash, hash_bob);
    assert_eq!(entry.sender, bob);
}

#[tokio::test(flavor = "current_thread")]
async fn get_transaction_returns_none_for_unknown_hash() {
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );
    let sender = Address::repeat_byte(0xaa);
    let recipient = Address::repeat_byte(0x11);
    let known = B256::repeat_byte(0x01);
    let unknown = B256::repeat_byte(0xff);

    service
        .ingest_block(FinalizedBlock {
            header: test_header(1, B256::ZERO),
            logs_by_tx: vec![vec![]],
            txs: vec![with_hash(
                ingest_tx(sender, Some(recipient), Vec::new()),
                known,
            )],
        })
        .await
        .expect("ingest");

    assert!(service
        .get_transaction(unknown)
        .await
        .expect("lookup")
        .is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn get_transaction_resolves_contract_creation_tx() {
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );
    let sender = Address::repeat_byte(0xaa);
    let hash = B256::repeat_byte(0x07);

    service
        .ingest_block(FinalizedBlock {
            header: test_header(1, B256::ZERO),
            logs_by_tx: vec![vec![]],
            txs: vec![with_hash(ingest_tx(sender, None, Vec::new()), hash)],
        })
        .await
        .expect("ingest");

    let entry = service
        .get_transaction(hash)
        .await
        .expect("lookup")
        .expect("hit");
    assert_eq!(entry.tx_hash, hash);
    assert!(entry.to().expect("decode").is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn get_transaction_resolves_across_multiple_blocks() {
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );
    let sender = Address::repeat_byte(0xaa);
    let recipient = Address::repeat_byte(0x11);
    let hash_b1 = B256::repeat_byte(0x01);
    let hash_b2 = B256::repeat_byte(0x02);

    let h1 = test_header(1, B256::ZERO);
    service
        .ingest_block(FinalizedBlock {
            header: h1.clone(),
            logs_by_tx: vec![vec![]],
            txs: vec![with_hash(
                ingest_tx(sender, Some(recipient), Vec::new()),
                hash_b1,
            )],
        })
        .await
        .expect("ingest 1");

    service
        .ingest_block(FinalizedBlock {
            header: chain_header(2, &h1),
            logs_by_tx: vec![vec![]],
            txs: vec![with_hash(
                ingest_tx(sender, Some(recipient), Vec::new()),
                hash_b2,
            )],
        })
        .await
        .expect("ingest 2");

    let e1 = service
        .get_transaction(hash_b1)
        .await
        .expect("lookup 1")
        .expect("hit 1");
    let e2 = service
        .get_transaction(hash_b2)
        .await
        .expect("lookup 2")
        .expect("hit 2");
    assert_eq!(e1.block_number, 1);
    assert_eq!(e1.tx_idx, 0);
    assert_eq!(e2.block_number, 2);
    assert_eq!(e2.tx_idx, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn failed_tx_ingest_does_not_index_transaction_hash() {
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );
    let tx_hash = B256::repeat_byte(0x44);

    service
        .ingest_block(FinalizedBlock {
            header: test_header(1, B256::ZERO),
            logs_by_tx: vec![vec![]],
            txs: vec![IngestTx {
                tx_hash,
                signed_tx_bytes: vec![0x01].into(),
                ..Default::default()
            }],
        })
        .await
        .expect_err("invalid signed tx should fail ingest");

    assert!(service
        .get_transaction(tx_hash)
        .await
        .expect("lookup")
        .is_none());
    assert!(service
        .tables()
        .family(Family::Tx)
        .load_block_header(1)
        .await
        .expect("load tx header")
        .is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn get_transaction_ignores_index_hits_without_published_head() {
    let meta_store = InMemoryMetaStore::default();
    let service = MonadChainDataService::new(
        meta_store.clone(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );
    let sender = Address::repeat_byte(0xaa);
    let recipient = Address::repeat_byte(0x11);
    let tx_hash = B256::repeat_byte(0x55);

    service
        .ingest_block(FinalizedBlock {
            header: test_header(1, B256::ZERO),
            logs_by_tx: vec![vec![]],
            txs: vec![with_hash(
                ingest_tx(sender, Some(recipient), Vec::new()),
                tx_hash,
            )],
        })
        .await
        .expect("ingest");

    meta_store.clear_cas_key(
        PublicationTables::<InMemoryMetaStore>::PUBLICATION_STATE_TABLE,
        PublicationTables::<InMemoryMetaStore>::PUBLICATION_STATE_KEY,
    );

    assert!(service
        .get_transaction(tx_hash)
        .await
        .expect("lookup")
        .is_none());
    assert!(service
        .tables()
        .family(Family::Tx)
        .load_block_header(1)
        .await
        .expect("load tx header")
        .is_some());
}

fn with_hash(mut tx: IngestTx, tx_hash: B256) -> IngestTx {
    tx.tx_hash = tx_hash;
    tx
}
