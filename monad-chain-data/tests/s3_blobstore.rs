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

//! Integration tests for [`S3BlobStore`] against a real S3 wire protocol.
//!
//! These run against an **in-process, filesystem-backed** S3 server (the
//! `s3s` + `s3s-fs` crates) bound to a local TCP port. Nothing external is
//! required: no MinIO, no AWS account, no network. The server speaks the real
//! S3 HTTP/SigV4 protocol, so the actual `aws-sdk-s3` client inside
//! `S3BlobStore` is exercised end to end, including the `force_path_style` +
//! explicit `endpoint_url` code path.
//!
//! The tests are gated two ways so the default offline CI build is unaffected:
//!   1. The whole file is `#![cfg(feature = "s3")]` -- without the `s3`
//!      feature it compiles to an empty test binary.
//!   2. Each test is `#[ignore]`d so it is skipped unless explicitly requested.
//!
//! Run them with:
//!
//! ```text
//! cargo test -p monad-chain-data --features s3 --test s3_blobstore -- --ignored
//! ```
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
use monad_chain_data::store::blob::{
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

/// A running in-process S3 server. Dropping it leaves a detached background
/// task; the `_tmp` dir is kept alive for the lifetime of the harness so the
/// filesystem backend's storage root survives.
struct TestServer {
    endpoint_url: String,
    _tmp: tempfile::TempDir,
}

/// Spawns the filesystem-backed `s3s` server on an ephemeral local port and
/// returns once it is accepting connections.
async fn spawn_s3_server() -> TestServer {
    let tmp = tempfile::tempdir().expect("create temp dir for s3s-fs root");

    let fs = FileSystem::new(tmp.path()).expect("init s3s-fs filesystem backend");
    let service = {
        let mut b = S3ServiceBuilder::new(fs);
        b.set_auth(SimpleAuth::from_single(ACCESS_KEY, SECRET_KEY));
        b.build()
    };

    // Bind to port 0 so the OS hands us a free ephemeral port.
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

/// Builds a raw `aws-sdk-s3` client pointed at the local server. Used by the
/// test harness for setup (create-bucket) since `S3BlobStore` itself never
/// creates buckets.
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

/// Spins up the server, creates the bucket, and returns a ready `S3BlobStore`
/// plus the live server handle (kept alive by the caller).
async fn setup() -> (S3BlobStore, TestServer) {
    let server = spawn_s3_server().await;

    // Create the bucket out of band for this fixture. Retry briefly in case the
    // accept loop hasn't fully warmed up yet.
    let client = admin_client(&server.endpoint_url).await;
    let mut last_err = None;
    for _ in 0..20 {
        match client.create_bucket().bucket(BUCKET).send().await {
            Ok(_) => {
                last_err = None;
                break;
            }
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    if let Some(e) = last_err {
        panic!("failed to create test bucket: {e:?}");
    }

    let store = S3BlobStore::new(S3BlobStoreConfig {
        bucket: BUCKET.to_string(),
        root_prefix: ROOT_PREFIX.to_string(),
        endpoint_url: Some(server.endpoint_url.clone()),
        region: Some(REGION.to_string()),
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

    // Round-trip hit.
    store
        .put_blob(BLOCKS, key, value.clone())
        .await
        .expect("put_blob");
    let got = store.get_blob(BLOCKS, key).await.expect("get_blob");
    assert_eq!(got.as_deref(), Some(value.as_ref()));

    // Missing key => Ok(None).
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
    // 0x00..=0xff repeated so any sub-slice is easy to verify.
    let value: Vec<u8> = (0..=255u8).cycle().take(1024).collect();
    let value = Bytes::from(value);
    store
        .put_blob(BLOCKS, key, value.clone())
        .await
        .expect("put_blob");

    // Server-side sub-slice of a larger blob.
    let mid = store
        .read_range(BLOCKS, key, 100, 200)
        .await
        .expect("read_range mid")
        .expect("present");
    assert_eq!(mid.as_ref(), &value[100..200]);

    // Range clamped past EOF: end beyond length returns up to EOF.
    let tail = store
        .read_range(BLOCKS, key, 1000, 5000)
        .await
        .expect("read_range tail")
        .expect("present");
    assert_eq!(tail.as_ref(), &value[1000..1024]);

    // start == end on an existing key => Some(empty).
    let empty = store
        .read_range(BLOCKS, key, 50, 50)
        .await
        .expect("read_range empty")
        .expect("present");
    assert!(empty.is_empty());

    // start == end on a missing key => None.
    let empty_missing = store
        .read_range(BLOCKS, b"nope", 0, 0)
        .await
        .expect("read_range empty missing");
    assert_eq!(empty_missing, None);

    // start > end => Decode error (validated before any network call).
    let err = store.read_range(BLOCKS, key, 10, 5).await;
    assert!(err.is_err(), "start > end must error, got {err:?}");

    // Non-empty range on a missing key => None.
    let range_missing = store
        .read_range(BLOCKS, b"nope", 0, 10)
        .await
        .expect("read_range range missing");
    assert_eq!(range_missing, None);

    // start at/after EOF with a non-empty range => 416 mapped to Decode error.
    let oob = store.read_range(BLOCKS, key, 2000, 3000).await;
    assert!(oob.is_err(), "start past EOF must error, got {oob:?}");
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
            // Arbitrary binary key, exercises the hex key encoding.
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

    // An empty batch is a no-op and must succeed.
    store
        .apply_writes(vec![])
        .await
        .expect("empty apply_writes");
}
