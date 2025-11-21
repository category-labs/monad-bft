use monad_block_persist::{block_id_to_hex_prefix, BlockPersist, FileBlockPersist};
use monad_node_config::{ExecutionProtocolType, SignatureCollectionType, SignatureType};
use monad_types::{BlockId, Hash};
use tokio::sync::mpsc;

use crate::{
    model::bft_archive_store::{
        BftArchiveStore, BftBlockBody, BftBlockHeader, BFT_BLOCK_HEADER_EXTENSION, BFT_BLOCK_PREFIX,
    },
    prelude::*,
};

type BftBlockPersist =
    FileBlockPersist<SignatureType, SignatureCollectionType, ExecutionProtocolType>;

pub async fn index_bft_block_ids(
    bft_archive_store: BftArchiveStore,
    start_block_id: BlockId,
    end_block_id: Option<BlockId>,
) -> Result<()> {
    map_header_chain_backwards(
        bft_archive_store.clone(),
        start_block_id,
        None,
        async |header| {
            // todo
            let id = header.get_id();

            if let Some(end_block_id) = end_block_id {
                if id == end_block_id {
                    return Err(eyre!("Reached end block ID {end_block_id:?}"));
                }
            }

            bft_archive_store
                .put_block_id_index(id, header.seq_num.0)
                .await
                .wrap_err_with(|| {
                    format!(
                        "Failed to put BFT block ID index {block_num}: {block_id}",
                        block_num = header.seq_num.0,
                        block_id = block_id_to_hex_prefix(&id.0),
                    )
                })
        },
    )
    .await
}

pub async fn hydrate_ledger_from_archive(
    bft_archive_store: BftArchiveStore,
    bft_block_persist: BftBlockPersist,
    start_block_id: BlockId,
    num_blocks: Option<usize>,
) -> Result<()> {
    map_bft_block_chain_backwards(
        bft_archive_store.clone(),
        start_block_id,
        num_blocks,
        |header, body| write_bft_block_to_ledger(bft_block_persist.clone(), header, body),
    )
    .await
}

pub async fn write_bft_block_to_ledger(
    mut bft_block_persist: BftBlockPersist,
    bft_block_header: Box<BftBlockHeader>,
    bft_block_body: Box<BftBlockBody>,
) -> Result<()> {
    tokio::task::spawn_blocking(move || {
        let id = bft_block_header.block_body_id;
        bft_block_persist
            .write_bft_body(&bft_block_body)
            .wrap_err("Failed to write BFT block body")?;
        bft_block_persist
            .write_bft_header(&bft_block_header)
            .wrap_err("Failed to write BFT block header")?;

        info!(
            block_body_id = block_id_to_hex_prefix(&id.0),
            "Wrote BFT block to ledger",
        );
        eyre::Ok(())
    })
    .await
    .wrap_err("BFT persist task failed to join")?
}

pub async fn map_header_chain_backwards<Fut>(
    bft_archive_store: BftArchiveStore,
    start_block_id: BlockId,
    mut num_blocks: Option<usize>,
    f: impl Fn(Box<BftBlockHeader>) -> Fut,
) -> Result<()>
where
    Fut: std::future::Future<Output = Result<()>>,
{
    let mut rx = walk_header_chain_backwards(bft_archive_store.clone(), start_block_id);

    while let Some(header) = rx.recv().await {
        if let Some(n) = num_blocks.as_mut() {
            if *n == 0 {
                break;
            }
            *n -= 1;
        }
        f(header).await?;
    }

    Ok(())
}

pub async fn map_bft_block_chain_backwards<Fut>(
    bft_archive_store: BftArchiveStore,
    start_block_id: BlockId,
    num_blocks: Option<usize>,
    f: impl Fn(Box<BftBlockHeader>, Box<BftBlockBody>) -> Fut,
) -> Result<()>
where
    Fut: std::future::Future<Output = Result<()>>,
{
    map_header_chain_backwards(
        bft_archive_store.clone(),
        start_block_id,
        num_blocks,
        async |header| {
            let body = bft_archive_store
                .get_body(header.block_body_id)
                .await
                .wrap_err("Failed to get BFT block body")?
                .ok_or_eyre("BFT block body not found")?;

            assert!(body.get_id() == header.block_body_id, "Computed block body ID mismatch. Corruption detected. Block body ID: {:?}, SeqNum: {:?}", block_id_to_hex_prefix(&header.get_id().0), header.seq_num.0);

            f(header, Box::new(body)).await
        },
    )
    .await
}

pub fn walk_header_chain_backwards(
    bft_archive_store: BftArchiveStore,
    start_block_id: BlockId,
) -> mpsc::Receiver<Box<BftBlockHeader>> {
    let (tx, rx) = mpsc::channel(100);

    tokio::spawn(async move {
        let mut block_id = start_block_id;

        loop {
            info!(
                block_id = block_id_to_hex_prefix(&block_id.0),
                "Fetching BFT block header"
            );
            match bft_archive_store.get_header(block_id).await {
                Ok(Some(header)) => {
                    assert!(header.get_id() == block_id, "Computed block ID mismatch. Corruption detected. Block ID: {block_id:?}, SeqNum: {}", header.seq_num.0);
                    block_id = header.get_parent_id();
                    if let Err(e) = tx.send(Box::new(header)).await {
                        warn!(?e, "Channel closed, stopping iteration");
                        break;
                    }
                }
                Ok(None) => {
                    info!("No bft block found at {block_id:?}");
                    break;
                }
                Err(e) => {
                    error!("Failed to get BFT block header: {e:?}");
                    break;
                }
            }
        }
    });

    rx
}

pub async fn index_all_bft_block_ids(bft_archive_store: BftArchiveStore) -> Result<()> {
    let prefixes = (0..1 << 16).map(|i| format!("{BFT_BLOCK_PREFIX}{i:04x}"));

    let (tx, mut rx) = mpsc::channel(10);
    let kv = bft_archive_store.kv.clone();
    tokio::spawn(async move {
        futures::stream::iter(prefixes)
            .for_each_concurrent(Some(3), |prefix| {
                dbg!(&prefix);
                let tx = tx.clone();
                let kv = kv.clone();
                async move {
                    // TODO: handle errors
                    for i in 0..5 {
                        match kv.scan_prefix(&prefix).await {
                            Ok(keys) => {
                                tx.send(keys).await.expect("Receiver dropped, fatal error");
                                break;
                            }
                            Err(e) => {
                                let backoff_ms = 128 << i;
                                error!(?e, backoff_ms, "Failed to scan prefix, retrying...");
                                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                            }
                        }
                    }
                }
            })
            .await;
    });

    while let Some(keys) = rx.recv().await {
        futures::stream::iter(keys)
            .map(|key| {
                let bft_archive_store = bft_archive_store.clone();
                async move {
                    let key_stripped = match key.strip_prefix(BFT_BLOCK_PREFIX) {
                        Some(s) => s,
                        None => return,
                    };
                    let Some(hex_hash) = key_stripped.strip_suffix(BFT_BLOCK_HEADER_EXTENSION)
                    else {
                        // skip non-header keys
                        return;
                    };

                    // Parse hex -> BlockId and use the archive store's compat decoding
                    let mut raw = [0u8; 32];
                    if let Err(e) = hex::decode_to_slice(hex_hash, &mut raw) {
                        warn!(?e, key=?key, "Failed to parse block id from key; skipping");
                        return;
                    }
                    let block_id = BlockId(Hash(raw));

                    async fn get_header_and_put_index(
                        bft_archive_store: BftArchiveStore,
                        block_id: BlockId,
                        key_stripped: &str,
                    ) -> Result<()> {
                        match bft_archive_store.get_header(block_id).await {
                            Ok(Some(header)) => {
                                dbg!(&block_id, &header.seq_num.0, key_stripped);
                                bft_archive_store
                                    .put_block_id_index(block_id, header.seq_num.0)
                                    .await
                            }
                            Ok(None) => {
                                info!(?block_id, "Header missing for key in listing");
                                Ok(())
                            }
                            Err(e) => {
                                error!(?e, ?block_id, "Failed to get header from archive store");
                                Err(e)
                            }
                        }
                    }

                    for i in 0..5 {
                        match get_header_and_put_index(
                            bft_archive_store.clone(),
                            block_id,
                            key_stripped,
                        )
                        .await
                        {
                            Ok(()) => {
                                break;
                            }
                            Err(e) => {
                                let backoff_ms = 128 << i;
                                error!(
                                    ?e,
                                    backoff_ms, "Failed to get header and put index, retrying..."
                                );
                                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                            }
                        }
                    }
                }
            })
            .buffer_unordered(100)
            .collect::<()>()
            .await;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use monad_types::Hash;

    use super::*;
    use crate::cli::ArchiveArgs;

    #[tokio::test]
    #[ignore]
    async fn test_hydrate_ledger_from_archive() {
        let bucket = ArchiveArgs::from_str("aws testnet-ltu-032-0")
            .unwrap()
            .build_block_data_archive(&Metrics::none())
            .await
            .unwrap()
            .store;
        let bft_archive_store = BftArchiveStore::new(bucket);
        let dir = std::path::PathBuf::from("/home/jhow/monad-bft/monad-archive/test_ledger");
        let bft_block_persist = FileBlockPersist::new(dir);
        let mut x = [0u8; 32];
        hex::decode_to_slice(
            "826e40421e1c7fb9f587a54693b3b614c3b3770e26687d920314b5d02a30db1a",
            &mut x,
        )
        .unwrap();
        dbg!(&x, x.len());
        let start_block_id = BlockId(Hash(x));
        let result = hydrate_ledger_from_archive(
            bft_archive_store,
            bft_block_persist,
            start_block_id,
            Some(10),
        )
        .await;
        assert!(result.is_ok());
    }

    fn block_id_from_hex(hex: &str) -> BlockId {
        let mut x = [0u8; 32];
        hex::decode_to_slice(hex, &mut x).unwrap();
        BlockId(Hash(x))
    }

    #[tokio::test]
    #[ignore]
    async fn test_index_bft_block_ids() {
        let bucket = ArchiveArgs::from_str("aws testnet-ltu-032-0")
            .unwrap()
            .build_block_data_archive(&Metrics::none())
            .await
            .unwrap()
            .store;

        let bft_archive_store = BftArchiveStore::new(bucket);

        let end_block_id =
            block_id_from_hex("d7368f1557f0bebfd324680afb53f5e7640e4173f6c143cfe01b84ed68fb8979");
        let start_block_id =
            block_id_from_hex("826e40421e1c7fb9f587a54693b3b614c3b3770e26687d920314b5d02a30db1a");

        let start_block_header = bft_archive_store
            .get_header(start_block_id)
            .await
            .unwrap()
            .unwrap();

        let result = index_bft_block_ids(
            bft_archive_store.clone(),
            start_block_id,
            Some(end_block_id),
        )
        .await;
        dbg!(&result);
        assert!(result.is_ok());

        let block_id = bft_archive_store
            .get_block_id_index(start_block_header.seq_num.0)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(block_id, start_block_id);
    }

    #[tokio::test]
    #[ignore]
    async fn test_index_all_bft_block_ids() {
        let bucket = ArchiveArgs::from_str("aws testnet-ltu-032-0")
            .unwrap()
            .build_block_data_archive(&Metrics::none())
            .await
            .unwrap()
            .store;
        let bft_archive_store = BftArchiveStore::new(bucket);
        let result = index_all_bft_block_ids(bft_archive_store).await;
        assert!(result.is_ok());
    }
}
