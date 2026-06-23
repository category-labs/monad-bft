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

//! Run with: `cargo test -p monad-chain-data --features s3 --test s3_blobstore -- --ignored`
//! (uses an in-process `s3s` + `s3s-fs` server; no external setup needed).
#![cfg(feature = "s3")]

use std::{net::SocketAddr, time::Duration};

use aws_sdk_s3::{
    config::{Credentials, Region},
    Client,
};
use bytes::Bytes;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder as ConnBuilder,
};
use monad_query_store::blob::{
    BlobStore, BlobTableId, BlobWriteOp, S3BlobStore, S3BlobStoreConfig, S3Credentials,
};
use s3s::{auth::SimpleAuth, service::S3ServiceBuilder};
use s3s_fs::FileSystem;
use tokio::net::TcpListener;

const ACCESS_KEY: &str = "test-access-key";
const SECRET_KEY: &str = "test-secret-key";
const REGION: &str = "us-east-1";
const BUCKET: &str = "chain-data-test";
const ROOT_PREFIX: &str = "chain-data";

struct TestServer {
    endpoint_url: String,
    _tmp: tempfile::TempDir,
}

async fn spawn_s3_server() -> TestServer {
    let tmp = tempfile::tempdir().expect("create temp dir for s3s-fs root");

    let fs = FileSystem::new(tmp.path()).expect("init s3s-fs filesystem backend");
    let service = {
        let mut b = S3ServiceBuilder::new(fs);
        b.set_auth(SimpleAuth::from_single(ACCESS_KEY, SECRET_KEY));
        b.build()
    };

    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind local s3s listener");
    let local_addr: SocketAddr = listener.local_addr().expect("read local addr");
    let endpoint_url = format!("http://{local_addr}");

    let http_server = ConnBuilder::new(TokioExecutor::new());
    tokio::spawn(async move {
        loop {
            let (socket, _) = match listener.accept().await {
                Ok(conn) => conn,
                Err(_) => continue,
            };
            let conn = http_server
                .serve_connection(TokioIo::new(socket), service.clone())
                .into_owned();
            tokio::spawn(async move {
                let _ = conn.await;
            });
        }
    });

    TestServer {
        endpoint_url,
        _tmp: tmp,
    }
}

async fn admin_client(endpoint_url: &str) -> Client {
    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(Region::new(REGION))
        .endpoint_url(endpoint_url)
        .credentials_provider(Credentials::new(ACCESS_KEY, SECRET_KEY, None, None, "test"))
        .load()
        .await;
    let conf = aws_sdk_s3::config::Builder::from(&sdk_config)
        .force_path_style(true)
        .build();
    Client::from_conf(conf)
}

async fn setup() -> (S3BlobStore, TestServer) {
    let server = spawn_s3_server().await;

    let client = admin_client(&server.endpoint_url).await;
    for attempt in 0..20 {
        match client.create_bucket().bucket(BUCKET).send().await {
            Ok(_) => break,
            Err(e) if attempt == 19 => panic!("failed to create test bucket: {e:?}"),
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }

    let store = S3BlobStore::new(S3BlobStoreConfig {
        bucket: BUCKET.to_string(),
        root_prefix: ROOT_PREFIX.to_string(),
        endpoint_urls: vec![server.endpoint_url.clone()],
        region: Some(REGION.to_string()),
        profile: None,
        force_path_style: true,
        max_concurrency: 8,
        create_bucket: false,
        credentials: Some(S3Credentials {
            access_key_id: ACCESS_KEY.to_string(),
            secret_access_key: SECRET_KEY.to_string(),
            session_token: None,
        }),
    })
    .await
    .expect("construct S3BlobStore against local server");

    (store, server)
}

const BLOCKS: BlobTableId = BlobTableId::new("blocks");
const RECEIPTS: BlobTableId = BlobTableId::new("receipts");

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires the in-process s3s server; run with --features s3 -- --ignored"]
async fn put_then_get_roundtrip_and_missing_key() {
    let (store, _server) = setup().await;

    let key = b"block-0001";
    let value = Bytes::from_static(b"hello s3 blob world");

    store
        .put_blob(BLOCKS, key, value.clone())
        .await
        .expect("put_blob");
    let got = store.get_blob(BLOCKS, key).await.expect("get_blob");
    assert_eq!(got.as_deref(), Some(value.as_ref()));

    let missing = store
        .get_blob(BLOCKS, b"does-not-exist")
        .await
        .expect("get_blob missing");
    assert_eq!(missing, None);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires the in-process s3s server; run with --features s3 -- --ignored"]
async fn read_range_server_side_slice_and_edges() {
    let (store, _server) = setup().await;

    let key = b"ranged-blob";
    let value: Vec<u8> = (0..=255u8).cycle().take(1024).collect();
    let value = Bytes::from(value);
    store
        .put_blob(BLOCKS, key, value.clone())
        .await
        .expect("put_blob");

    let mid = store
        .read_range(BLOCKS, key, 100, 200)
        .await
        .expect("read_range mid")
        .expect("present");
    assert_eq!(mid.as_ref(), &value[100..200]);

    let tail = store
        .read_range(BLOCKS, key, 1000, 5000)
        .await
        .expect("read_range tail")
        .expect("present");
    assert_eq!(tail.as_ref(), &value[1000..1024]);

    let empty = store
        .read_range(BLOCKS, key, 50, 50)
        .await
        .expect("read_range empty")
        .expect("present");
    assert!(empty.is_empty());

    let empty_missing = store
        .read_range(BLOCKS, b"nope", 0, 0)
        .await
        .expect("read_range empty missing");
    assert_eq!(empty_missing, None);

    let err = store.read_range(BLOCKS, key, 10, 5).await;
    assert!(err.is_err(), "start > end must error, got {err:?}");

    let range_missing = store
        .read_range(BLOCKS, b"nope", 0, 10)
        .await
        .expect("read_range range missing");
    assert_eq!(range_missing, None);

    let oob = store.read_range(BLOCKS, key, 2000, 3000).await;
    assert!(oob.is_err(), "start past EOF must error, got {oob:?}");
}

/// EOF boundary matrix, mirroring the dynamo backend's
/// `read_range_spans_chunks_and_matches_trait_contract`: the trait errors only
/// for `start` strictly past EOF and clamps everything else to an empty read,
/// while raw S3 answers 416 for any window starting at/after EOF (including
/// every window on an empty object) and cannot express a zero-length range.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires the in-process s3s server; run with --features s3 -- --ignored"]
async fn read_range_eof_boundaries_match_trait_contract() {
    let (store, _server) = setup().await;

    let key = b"eof-blob";
    let value = Bytes::from_static(b"abcdef");
    store
        .put_blob(BLOCKS, key, value.clone())
        .await
        .expect("put_blob");

    // A window starting exactly at EOF clamps to an empty read.
    assert_eq!(
        store.read_range(BLOCKS, key, 6, 10).await.unwrap(),
        Some(Bytes::new())
    );
    // Zero-length windows succeed up to and including EOF, error past it.
    assert_eq!(
        store.read_range(BLOCKS, key, 6, 6).await.unwrap(),
        Some(Bytes::new())
    );
    assert_eq!(
        store
            .read_range(BLOCKS, key, 12, 12)
            .await
            .unwrap_err()
            .to_string(),
        "decode error: invalid blob range"
    );
    // A non-empty window starting strictly past EOF errors.
    assert_eq!(
        store
            .read_range(BLOCKS, key, 7, 10)
            .await
            .unwrap_err()
            .to_string(),
        "decode error: invalid blob range"
    );

    // Present zero-length blob: every window starting at 0 is an empty read;
    // anything starting past 0 is out of bounds.
    let empty_key = b"empty-blob";
    store
        .put_blob(BLOCKS, empty_key, Bytes::new())
        .await
        .expect("put_blob empty");
    assert_eq!(
        store.read_range(BLOCKS, empty_key, 0, 10).await.unwrap(),
        Some(Bytes::new())
    );
    assert_eq!(
        store.read_range(BLOCKS, empty_key, 0, 0).await.unwrap(),
        Some(Bytes::new())
    );
    assert_eq!(
        store
            .read_range(BLOCKS, empty_key, 1, 1)
            .await
            .unwrap_err()
            .to_string(),
        "decode error: invalid blob range"
    );
    assert_eq!(
        store
            .read_range(BLOCKS, empty_key, 1, 5)
            .await
            .unwrap_err()
            .to_string(),
        "decode error: invalid blob range"
    );

    // Missing blobs stay `None` for zero-length windows at any offset.
    assert_eq!(store.read_range(BLOCKS, b"nope", 3, 3).await.unwrap(), None);

    // Every boundary read above is tracked as one range read; only the four
    // contract violations count as errors — HEAD-resolved successes complete
    // normally instead of inheriting the 416's error verdict.
    let stats = store.take_read_stats();
    assert_eq!(stats.started, 9);
    assert_eq!(stats.completed, 5);
    assert_eq!(stats.errors, 4);
    assert_eq!(stats.canceled, 0);
    assert_eq!(stats.range_gets, 9);
    assert_eq!(stats.full_gets, 0);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires the in-process s3s server; run with --features s3 -- --ignored"]
async fn apply_writes_across_tables() {
    let (store, _server) = setup().await;

    let writes = vec![
        BlobWriteOp {
            table: BLOCKS,
            key: b"b1".to_vec(),
            value: Bytes::from_static(b"block-one"),
        },
        BlobWriteOp {
            table: BLOCKS,
            key: b"b2".to_vec(),
            value: Bytes::from_static(b"block-two"),
        },
        BlobWriteOp {
            table: RECEIPTS,
            key: b"r1".to_vec(),
            value: Bytes::from_static(b"receipt-one"),
        },
        BlobWriteOp {
            table: RECEIPTS,
            key: vec![0x00, 0xab, 0xff, 0x10],
            value: Bytes::from_static(b"receipt-binary-key"),
        },
    ];

    store
        .apply_writes(writes.clone())
        .await
        .expect("apply_writes");

    for op in &writes {
        let got = store
            .get_blob(op.table, &op.key)
            .await
            .expect("get_blob after apply_writes");
        assert_eq!(
            got.as_deref(),
            Some(op.value.as_ref()),
            "mismatch for table {:?} key {:?}",
            op.table,
            op.key
        );
    }

    store
        .apply_writes(vec![])
        .await
        .expect("empty apply_writes");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires the in-process s3s server; run with --features s3 -- --ignored"]
async fn delete_blob_removes_object_and_is_idempotent() {
    let (store, _server) = setup().await;

    let key = b"block-0042";
    store
        .put_blob(BLOCKS, key, Bytes::from_static(b"to be deleted"))
        .await
        .expect("put_blob");

    store.delete_blob(BLOCKS, key).await.expect("delete_blob");
    assert_eq!(store.get_blob(BLOCKS, key).await.expect("get_blob"), None);

    // S3 DeleteObject on a missing key succeeds: idempotent no-op.
    store
        .delete_blob(BLOCKS, key)
        .await
        .expect("delete_blob missing");
}
