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
    engine::tables::PublicationTables, Address, Family, InMemoryMetaStore, IngestTx, B256,
};

mod common;

use common::{block_with_txs, chain_header, ingest_tx, test_header, with_hash};

#[tokio::test(flavor = "current_thread")]
async fn get_transaction_returns_entry_for_ingested_hash() {
    let alice = Address::repeat_byte(0xaa);
    let bob = Address::repeat_byte(0xbb);
    let recipient = Address::repeat_byte(0x11);
    let hash_alice = B256::repeat_byte(0x01);
    let hash_bob = B256::repeat_byte(0x02);

    let store = common::populate::populate_via_engine(vec![block_with_txs(
        test_header(1, B256::ZERO),
        vec![
            with_hash(ingest_tx(alice, Some(recipient), Vec::new()), hash_alice),
            with_hash(ingest_tx(bob, Some(recipient), Vec::new()), hash_bob),
        ],
    )])
    .await;
    let service = store.reader();

    let (entry, header) = service
        .get_transaction(hash_bob)
        .await
        .expect("lookup")
        .expect("hit");
    assert_eq!(entry.block_number, 1);
    assert_eq!(entry.tx_index, 1);
    assert_eq!(entry.tx_hash, hash_bob);
    assert_eq!(entry.sender, bob);
    assert_eq!(header.number, 1, "entry pairs with its block's header");
}

#[tokio::test(flavor = "current_thread")]
async fn get_transaction_returns_none_for_unknown_hash() {
    let sender = Address::repeat_byte(0xaa);
    let recipient = Address::repeat_byte(0x11);
    let known = B256::repeat_byte(0x01);
    let unknown = B256::repeat_byte(0xff);

    let store = common::populate::populate_via_engine(vec![block_with_txs(
        test_header(1, B256::ZERO),
        vec![with_hash(
            ingest_tx(sender, Some(recipient), Vec::new()),
            known,
        )],
    )])
    .await;
    let service = store.reader();

    assert!(service
        .get_transaction(unknown)
        .await
        .expect("lookup")
        .is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn get_transaction_resolves_contract_creation_tx() {
    let sender = Address::repeat_byte(0xaa);
    let hash = B256::repeat_byte(0x07);

    let store = common::populate::populate_via_engine(vec![block_with_txs(
        test_header(1, B256::ZERO),
        vec![with_hash(ingest_tx(sender, None, Vec::new()), hash)],
    )])
    .await;
    let service = store.reader();

    let (entry, _) = service
        .get_transaction(hash)
        .await
        .expect("lookup")
        .expect("hit");
    assert_eq!(entry.tx_hash, hash);
    assert!(entry.to().is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn get_transaction_resolves_across_multiple_blocks() {
    let sender = Address::repeat_byte(0xaa);
    let recipient = Address::repeat_byte(0x11);
    let hash_b1 = B256::repeat_byte(0x01);
    let hash_b2 = B256::repeat_byte(0x02);

    let h1 = test_header(1, B256::ZERO);
    let h2 = chain_header(2, &h1);
    let store = common::populate::populate_via_engine(vec![
        block_with_txs(
            h1,
            vec![with_hash(
                ingest_tx(sender, Some(recipient), Vec::new()),
                hash_b1,
            )],
        ),
        block_with_txs(
            h2,
            vec![with_hash(
                ingest_tx(sender, Some(recipient), Vec::new()),
                hash_b2,
            )],
        ),
    ])
    .await;
    let service = store.reader();

    let (e1, header1) = service
        .get_transaction(hash_b1)
        .await
        .expect("lookup 1")
        .expect("hit 1");
    let (e2, header2) = service
        .get_transaction(hash_b2)
        .await
        .expect("lookup 2")
        .expect("hit 2");
    assert_eq!((e1.block_number, header1.number), (1, 1));
    assert_eq!(e1.tx_index, 0);
    assert_eq!((e2.block_number, header2.number), (2, 2));
    assert_eq!(e2.tx_index, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn failed_tx_ingest_aborts_the_block() {
    let tx_hash = B256::repeat_byte(0x44);

    let result = common::populate::try_populate_via_engine(vec![block_with_txs(
        test_header(1, B256::ZERO),
        vec![IngestTx {
            tx_hash,
            signed_tx_bytes: vec![0x01].into(),
            ..Default::default()
        }],
    )])
    .await;
    assert!(result.is_err(), "invalid signed tx should fail ingest");
}

#[tokio::test(flavor = "current_thread")]
async fn get_transaction_ignores_index_hits_without_published_head() {
    let sender = Address::repeat_byte(0xaa);
    let recipient = Address::repeat_byte(0x11);
    let tx_hash = B256::repeat_byte(0x55);

    let store = common::populate::populate_via_engine(vec![block_with_txs(
        test_header(1, B256::ZERO),
        vec![with_hash(
            ingest_tx(sender, Some(recipient), Vec::new()),
            tx_hash,
        )],
    )])
    .await;

    // Drop the published head but keep tx artifacts; a hash hit above head must not resolve.
    store.meta.clear_key(
        PublicationTables::<InMemoryMetaStore>::PUBLICATION_STATE_TABLE,
        PublicationTables::<InMemoryMetaStore>::PUBLICATION_STATE_KEY,
    );
    let service = store.reader();

    assert!(service
        .get_transaction(tx_hash)
        .await
        .expect("lookup")
        .is_none());
    assert!(service
        .tables()
        .family(Family::Tx)
        .load_blob_header(1)
        .await
        .expect("load tx header")
        .is_some());
}
