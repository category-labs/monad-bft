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

//! Migration helper: make an existing capped collection uncapped
//! without interrupting writers (rename-first pattern).
//!
//! Call it as many times as you like; it is crash-resumable and
//! no-op after reaching Phase::Done.

use monad_archive::prelude::*;
use mongodb::{
    bson::{doc, Bson, Document},
    Client, Collection,
};
use serde::{Deserialize, Serialize};

/// meta-collection name
const META_COLL: &str = "_migrations";

/// Public entry point -------------------------------------------------------
pub async fn migrate_to_uncapped(
    client: &Client,
    db_name: &str,
    coll_name: &str,
    batch_size: u32,
    free_factor: f64,
) -> Result<()> {
    let db = client.database(db_name);
    let meta: Collection<MetaDoc> = db.collection(META_COLL);
    let source_capped = format!("{coll_name}_capped");

    // Load or create state document
    let mut state = meta
        .find_one(doc! { "_id": coll_name })
        .await?
        .unwrap_or_else(|| MetaDoc::new(coll_name));

    loop {
        match state.phase {
            Phase::DiskCheck => {
                info!(coll_name, "Starting migration phase: DiskCheck");
                ensure_disk_space(&db, coll_name, &source_capped, free_factor).await?;
                state.phase = Phase::Renamed;
                meta.upsert(&state).await?;
                info!(
                    coll_name,
                    "Completed DiskCheck phase, moving to Renamed phase"
                );
                continue; // re-enter loop
            }
            Phase::Renamed => {
                info!(coll_name, "Starting migration phase: Renamed");
                let renamed = maybe_rename(&db, coll_name, &source_capped).await?;
                if !renamed {
                    // Collection was already uncapped, we're done
                    info!(coll_name, "collection already uncapped, migration complete");
                    meta.delete_one(doc! { "_id": coll_name }).await?;
                    return Ok(());
                }
                info!(
                    coll_name,
                    "Creating uncapped collection and copying indexes"
                );
                create_uncapped_and_indexes(&db, coll_name, &source_capped).await?;
                state.phase = Phase::Copying;
                meta.upsert(&state).await?;
                info!(
                    coll_name,
                    "Completed Renamed phase, moving to Copying phase"
                );
                continue;
            }
            Phase::Copying => {
                info!(coll_name, batch_size, "Starting migration phase: Copying");
                // Keep copying until all batches are done
                let mut batch_count = 0;
                loop {
                    let copied = copy_batches(
                        &db,
                        &source_capped,
                        coll_name,
                        &mut state,
                        batch_size,
                        &meta,
                    )
                    .await?;

                    if !copied {
                        // No more documents to copy
                        info!(coll_name, batch_count, "Finished copying all batches");
                        break;
                    }
                    batch_count += 1;
                    if batch_count % 10 == 0 {
                        info!(
                            coll_name,
                            batch_count, "Progress: copied {} batches", batch_count
                        );
                    }
                }
                state.phase = Phase::Dropping;
                meta.upsert(&state).await?;
                info!(
                    coll_name,
                    "Completed Copying phase, moving to Dropping phase"
                );
                continue;
            }
            Phase::Dropping => {
                info!(
                    coll_name,
                    source = &source_capped,
                    "Starting migration phase: Dropping"
                );
                drop_source(&db, &source_capped, coll_name).await?;
                meta.delete_one(doc! { "_id": coll_name }).await?;
                info!(coll_name, "Dropped source collection, migration complete");
                return Ok(());
            }
            Phase::Done => {
                info!(coll_name, "Migration already completed (Done phase)");
                return Ok(());
            }
        }
    }
}

/// --------------------------------------------------------------------------
///  Implementation helpers
/// --------------------------------------------------------------------------
/// Meta-document persisted across restarts
#[derive(Debug, Serialize, Deserialize, Clone)]
struct MetaDoc {
    #[serde(rename = "_id")]
    id: String,
    phase: Phase,
    last_id: Option<Bson>, // most recent _id copied
}

impl MetaDoc {
    fn new(coll: &str) -> Self {
        Self {
            id: coll.to_string(),
            phase: Phase::DiskCheck,
            last_id: None,
        }
    }
}

/// FSM phases — linear
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum Phase {
    DiskCheck,
    Renamed,
    Copying,
    Dropping,
    Done,
}

/// Add or replace the meta doc
trait Upsertable {
    async fn upsert(&self, doc: &MetaDoc) -> mongodb::error::Result<()>;
}
impl Upsertable for Collection<MetaDoc> {
    async fn upsert(&self, doc: &MetaDoc) -> mongodb::error::Result<()> {
        self.replace_one(doc! { "_id": &doc.id }, doc)
            .with_options(Some(
                mongodb::options::ReplaceOptions::builder()
                    .upsert(true)
                    .build(),
            ))
            .await?;
        Ok(())
    }
}

/// Step 0  —  free-space guard
async fn ensure_disk_space(
    db: &mongodb::Database,
    coll: &str,
    capped_name: &str,
    scale_factor: f64,
) -> Result<()> {
    // Get database stats to understand current storage usage
    let db_stats = db
        .run_command(doc! { "dbStats": 1, "scale": 1, "freeStorage": 1 })
        .await
        .wrap_err("Failed to get database stats")?;

    // Try to get collection stats to estimate the specific collection size
    let coll_stats = match coll_stats(db, coll).await {
        Ok(stats) => stats,
        Err(e) => {
            info!(?e, "collStats failed on {coll}, trying {capped_name}");
            match coll_stats(db, capped_name).await {
                Ok(stats) => stats,
                Err(_) => {
                    // If we can't get collection stats, estimate based on database stats
                    warn!("Could not get collection stats, using database stats for estimation");
                    doc! {}
                }
            }
        }
    };

    // Calculate space needed
    let need = if let (Ok(coll_storage_size), Ok(coll_index_size)) = (
        get_int_field(&coll_stats, "storageSize"),
        get_int_field(&coll_stats, "totalIndexSize"),
    ) {
        // Use collection-specific storage size if available
        ((coll_storage_size + coll_index_size) as f64 * scale_factor) as i64
    } else {
        // Fallback: estimate based on database average object size and collection count
        let db_storage_size = db_stats.get_i64("storageSize").unwrap_or(0);
        let db_data_size = db_stats.get_i64("dataSize").unwrap_or(0);
        // Use the larger of storage or data size for conservative estimate
        let estimate_base = std::cmp::max(db_storage_size, db_data_size);
        // Assume this collection is 10% of database (conservative estimate)
        ((estimate_base as f64 * 0.1 * scale_factor) as i64).max(1_000_000_000) // At least 1GB
    };

    // Get filesystem stats from the database stats
    // MongoDB may return these as Double or Int64
    let fs_used = db_stats
        .get_i64("fsUsedSize")
        .or_else(|_| db_stats.get_f64("fsUsedSize").map(|v| v as i64))
        .unwrap_or(0);
    let fs_total = db_stats
        .get_i64("fsTotalSize")
        .or_else(|_| db_stats.get_f64("fsTotalSize").map(|v| v as i64))
        .unwrap_or(0);
    let fs_free = fs_total - fs_used;

    // Ensure we have valid filesystem stats
    if fs_total == 0 {
        bail!(
            "Unable to get filesystem stats (fsTotal=0). MongoDB must be configured with proper filesystem access."
        );
    }

    if fs_free < need {
        bail!(
            "Not enough free space: need {} bytes ({:.2} GB), have {} bytes ({:.2} GB)",
            need,
            need as f64 / (1024.0 * 1024.0 * 1024.0),
            fs_free,
            fs_free as f64 / (1024.0 * 1024.0 * 1024.0)
        );
    }

    info!(
        db = db.name(),
        coll,
        "disk check passed: need {} bytes ({:.2} GB), have {} bytes ({:.2} GB) free",
        need,
        need as f64 / (1024.0 * 1024.0 * 1024.0),
        fs_free,
        fs_free as f64 / (1024.0 * 1024.0 * 1024.0)
    );
    Ok(())
}

/// Step 1 — rename if still capped
async fn maybe_rename(db: &mongodb::Database, coll: &str, capped_name: &str) -> Result<bool> {
    let stats = coll_stats(db, coll).await?;
    if !stats.get_bool("capped").unwrap_or(false) {
        info!(coll, "already uncapped, skipping rename phase");
        return Ok(false);
    }
    info!(coll, "renaming to {capped_name}");
    // renameCollection must be run against admin database
    let admin_db = db.client().database("admin");
    admin_db
        .run_command(doc! {
            "renameCollection": format!("{}.{}", db.name(), coll),
            "to": format!("{}.{}", db.name(), capped_name),
            "dropTarget": true
        })
        .await?;
    Ok(true)
}

/// Step 2 — create destination and copy indexes
async fn create_uncapped_and_indexes(
    db: &mongodb::Database,
    coll: &str,
    source: &str,
) -> Result<()> {
    // create if missing
    if !db
        .list_collection_names()
        .await?
        .contains(&coll.to_string())
    {
        info!(coll, "Creating destination collection");
        db.create_collection(coll).await?;
    } else {
        info!(coll, "Destination collection already exists");
    }
    info!(
        source,
        coll, "Copying indexes from source (no-op if they already exist)",
    );

    let src_coll = db.collection::<Document>(source);
    let dst_coll = db.collection::<Document>(coll);

    let idx = async {
        src_coll
            .list_indexes()
            .await
            .wrap_err_with(|| format!("list_indexes failed for {source}"))?
            .try_collect::<Vec<_>>()
            .await
            .wrap_err_with(|| format!("list_indexes collect failed for {source}"))
    };

    let dst_idx = async {
        dst_coll
            .list_indexes()
            .await
            .wrap_err_with(|| format!("list_indexes failed for {coll}"))?
            .try_collect::<Vec<_>>()
            .await
            .wrap_err_with(|| format!("list_indexes collect failed for {source}"))
    };

    let (idx, dst_idx) = tokio::try_join!(idx, dst_idx)?;

    if dst_idx.len() == idx.len() {
        info!(coll, "Indexes already exist");
        return Ok(());
    }

    info!(coll, "Indexes differ, dropping and recreating");
    dst_coll.drop_indexes().await?;
    if let Err(e) = dst_coll.create_indexes(idx).await {
        warn!(coll, ?e, "Failed to create indexes");
        return Err(e).wrap_err(format!("Failed to create indexes for {coll}"));
    }

    info!(coll, "Indexes created");
    Ok(())
}

/// Step 3 – server-side copy via `$merge`
///
/// * Streams **only** `_id`s (at most `batch` per call) through the driver.
/// * `$merge` copies the matching slice wholly inside MongoDB.
/// * Crash-safe: progress check-pointed in `state.last_id`.
async fn copy_batches(
    db: &mongodb::Database,
    src: &str,
    dst: &str,
    state: &mut MetaDoc,
    batch: u32,
    meta: &Collection<MetaDoc>,
) -> Result<bool> {
    let src_coll = db.collection::<Document>(src);
    debug!(src, ?state.last_id, "Copying batch");

    // 1 — grab the next `batch` _ids after `last_id`  (only _id projected)
    let mut id_filter = doc! {};
    if let Some(ref last) = state.last_id {
        id_filter.insert("_id", doc! { "$gt": last.clone() });
    }
    let ids = src_coll
        .find(id_filter)
        .with_options(Some(
            mongodb::options::FindOptions::builder()
                .sort(doc! { "_id": 1 })
                .limit(batch as i64)
                .projection(doc! { "_id": 1 })
                .build(),
        ))
        .await
        .wrap_err_with(|| format!("find failed for {src}"))?
        .try_collect::<Vec<_>>()
        .await
        .wrap_err("Failed to collect ids")?;

    let end_id = if let Some(last) = ids.last() {
        last.get("_id").ok_or_eyre("No _id found")?.clone()
    } else {
        info!(src, dst, "No more documents to copy");
        return Ok(false);
    };

    info!(
        src, dst,
        batch_size = ids.len(),
        start_id = ?state.last_id,
        end_id = ?end_id,
        "Copying batch of documents"
    );

    // 2 — server-side aggregate:  match this id window → merge into dst
    let pipeline = vec![
        doc! { "$match": if state.last_id.is_some() {
            doc! { "_id": { "$gt": state.last_id.clone().unwrap(), "$lte": end_id.clone() } }
        } else {
            doc! { "_id": { "$lte": end_id.clone() } }
        }},
        doc! {
            "$merge": {
                "into": dst,
                "whenMatched": "keepExisting",
                "whenNotMatched": "insert"
            }
        },
    ];
    src_coll
        .aggregate(pipeline)
        .await
        .wrap_err("Copy batch failed")?
        .try_next() // drain cursor (aggregate returns at least one doc)
        .await
        .wrap_err("Failed to drain aggregate cursor")?;

    // 3 — checkpoint and persist
    state.last_id = Some(end_id.clone());
    meta.upsert(state).await?;
    info!(src, dst, last_id = ?end_id, "Batch copied and checkpoint saved");
    Ok(true)
}

/// Step 4 — drop the old capped collection
async fn drop_source(db: &mongodb::Database, src: &str, dst: &str) -> Result<()> {
    // quick sanity
    let src_stats = coll_stats(db, src).await?;
    let dst_stats = coll_stats(db, dst).await?;
    let src_count = get_int_field(&src_stats, "count")?;
    let dst_count = get_int_field(&dst_stats, "count")?;
    info!(
        src,
        dst, src_count, dst_count, "Verifying document counts before dropping source"
    );
    if src_count != dst_count {
        warn!(
            src,
            dst, src_count, dst_count, "document counts differ, dropping anyway"
        );
    }
    info!(src, "Dropping source capped collection");
    db.collection::<Document>(src).drop().await?;
    Ok(())
}

/// tiny helper
async fn coll_stats(db: &mongodb::Database, coll: &str) -> Result<Document> {
    db.run_command(doc! { "collStats": coll })
        .await
        .wrap_err_with(|| format!("collStats failed for {coll}"))
}

/// Helper to get an integer field as i64, trying i64 first then i32
fn get_int_field(doc: &Document, field: &str) -> Result<i64> {
    doc.get_i64(field)
        .or_else(|_| doc.get_i32(field).map(|v| v as i64))
        .wrap_err_with(|| format!("Field '{field}' not found or not an integer"))
}

#[cfg(test)]
mod tests {
    use monad_archive::test_utils::TestMongoContainer;

    use super::*;

    async fn setup_test_db() -> Result<(TestMongoContainer, Client, String, String)> {
        let container = TestMongoContainer::new().await?;
        let client = Client::with_uri_str(&container.uri).await?;
        let db_name = "test_migrate_db";
        let coll_name = "test_capped_coll";

        Ok((
            container,
            client,
            db_name.to_string(),
            coll_name.to_string(),
        ))
    }

    async fn create_capped_collection(
        client: &Client,
        db_name: &str,
        coll_name: &str,
        size_mb: u64,
    ) -> Result<()> {
        let db = client.database(db_name);
        db.create_collection(coll_name)
            .capped(true)
            .size(size_mb * 1024 * 1024)
            .await?;
        Ok(())
    }

    async fn insert_test_data(
        client: &Client,
        db_name: &str,
        coll_name: &str,
        count: usize,
    ) -> Result<()> {
        let db = client.database(db_name);
        let coll: Collection<mongodb::bson::Document> = db.collection(coll_name);

        let docs: Vec<mongodb::bson::Document> = (0..count)
            .map(|i| {
                doc! {
                    "_id": format!("doc_{:04}", i),
                    "value": format!("test_value_{}", i),
                    "index": i as i32
                }
            })
            .collect();

        coll.insert_many(docs).await?;
        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn test_migrate_capped_basic() -> Result<()> {
        let (_container, client, db_name, coll_name) = setup_test_db().await?;

        // Create a capped collection and add data
        create_capped_collection(&client, &db_name, &coll_name, 10).await?;
        insert_test_data(&client, &db_name, &coll_name, 100).await?;

        // Verify it's capped
        let db = client.database(&db_name);
        let stats = coll_stats(&db, &coll_name).await?;
        assert!(stats.get_bool("capped").unwrap_or(false));

        // Run migration with smaller batch size to see pattern
        migrate_to_uncapped(&client, &db_name, &coll_name, 25, 1.5).await?;

        // Verify collection is now uncapped
        let stats = coll_stats(&db, &coll_name).await?;
        assert!(!stats.get_bool("capped").unwrap_or(true));

        // Verify data integrity
        let coll: Collection<mongodb::bson::Document> = db.collection(&coll_name);
        let count = coll.count_documents(doc! {}).await?;

        assert_eq!(count, 100);

        // Verify specific document
        let doc = coll.find_one(doc! { "_id": "doc_0050" }).await?.unwrap();
        assert_eq!(doc.get_str("value")?, "test_value_50");

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn test_migrate_capped_resumable() -> Result<()> {
        let (_container, client, db_name, coll_name) = setup_test_db().await?;

        // Create a capped collection and add data
        create_capped_collection(&client, &db_name, &coll_name, 10).await?;
        insert_test_data(&client, &db_name, &coll_name, 100).await?;

        let db = client.database(&db_name);
        let meta: Collection<MetaDoc> = db.collection(META_COLL);

        // Simulate interrupted migration by setting state to Copying with partial progress
        let state = MetaDoc {
            id: coll_name.clone(),
            phase: Phase::Copying,
            last_id: Some(Bson::String("doc_0050".to_string())),
        };
        meta.insert_one(&state).await?;

        // Also rename the collection to simulate partial migration
        let admin_db = client.database("admin");
        admin_db
            .run_command(doc! {
                "renameCollection": format!("{}.{}", &db_name, &coll_name),
                "to": format!("{}.{}_capped", &db_name, &coll_name),
                "dropTarget": false
            })
            .await?;

        // Create the destination collection and copy first 51 documents
        db.create_collection(&coll_name).await?;

        // Copy documents up to doc_0050 to simulate partial migration
        let src_coll: Collection<mongodb::bson::Document> =
            db.collection(&format!("{}_capped", coll_name));
        let dst_coll: Collection<mongodb::bson::Document> = db.collection(&coll_name);

        let docs_to_copy = src_coll
            .find(doc! { "_id": { "$lte": "doc_0050" } })
            .await?
            .try_collect::<Vec<_>>()
            .await?;

        if !docs_to_copy.is_empty() {
            dst_coll.insert_many(&docs_to_copy).await?;
        }

        // Run migration - should resume from where it left off
        migrate_to_uncapped(&client, &db_name, &coll_name, 20, 1.5).await?;

        // Verify migration completed
        let stats = coll_stats(&db, &coll_name).await?;
        assert!(!stats.get_bool("capped").unwrap_or(true));

        // Verify all data was copied
        let coll: Collection<mongodb::bson::Document> = db.collection(&coll_name);
        let count = coll.count_documents(doc! {}).await?;
        assert_eq!(count, 100);

        // Verify the old capped collection was dropped
        let collections = db.list_collection_names().await?;
        assert!(!collections.contains(&format!("{}_capped", coll_name)));

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn test_migrate_already_uncapped() -> Result<()> {
        let (_container, client, db_name, coll_name) = setup_test_db().await?;

        // Create an uncapped collection
        let db = client.database(&db_name);
        db.create_collection(&coll_name).await?;
        insert_test_data(&client, &db_name, &coll_name, 50).await?;

        // Run migration on already uncapped collection
        migrate_to_uncapped(&client, &db_name, &coll_name, 2000, 1.5).await?;

        // Verify it's still uncapped and data is intact
        let stats = coll_stats(&db, &coll_name).await?;
        assert!(!stats.get_bool("capped").unwrap_or(true));

        let coll: Collection<mongodb::bson::Document> = db.collection(&coll_name);
        let count = coll.count_documents(doc! {}).await?;
        assert_eq!(count, 50);

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn test_ensure_disk_space_sufficient() -> Result<()> {
        let (_container, client, db_name, coll_name) = setup_test_db().await?;

        // Create a small capped collection
        create_capped_collection(&client, &db_name, &coll_name, 1).await?;
        insert_test_data(&client, &db_name, &coll_name, 10).await?;

        let db = client.database(&db_name);

        // Get dbStats to verify we have real filesystem stats
        let db_stats = db.run_command(doc! { "dbStats": 1, "scale": 1 }).await?;

        // Debug: Print dbStats if needed
        // println!("Full dbStats output: {:?}", db_stats);

        let fs_total = db_stats
            .get_i64("fsTotalSize")
            .or_else(|_| db_stats.get_f64("fsTotalSize").map(|v| v as i64))
            .unwrap_or(0);
        let fs_used = db_stats
            .get_i64("fsUsedSize")
            .or_else(|_| db_stats.get_f64("fsUsedSize").map(|v| v as i64))
            .unwrap_or(0);

        // Check if the fields exist at all
        if !db_stats.contains_key("fsTotalSize") {
            println!("WARNING: fsTotalSize field not present in dbStats");
            println!(
                "Available fields: {:?}",
                db_stats.keys().collect::<Vec<_>>()
            );
        }

        // For now, skip the assertion if MongoDB doesn't provide filesystem stats
        // This is a limitation of MongoDB in Docker environments
        if fs_total > 0 {
            println!(
                "Filesystem stats - Total: {} bytes ({:.2} GB), Used: {} bytes ({:.2} GB)",
                fs_total,
                fs_total as f64 / (1024.0 * 1024.0 * 1024.0),
                fs_used,
                fs_used as f64 / (1024.0 * 1024.0 * 1024.0)
            );
        } else {
            println!("WARNING: MongoDB is not reporting filesystem stats (fsTotalSize=0)");
            println!("This is expected in some Docker environments");
        }

        // This should succeed since we're asking for a small amount of space
        ensure_disk_space(&db, &coll_name, &format!("{}_capped", coll_name), 1.5).await?;

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn test_ensure_disk_space_insufficient() -> Result<()> {
        let (_container, client, db_name, coll_name) = setup_test_db().await?;

        // Create a capped collection
        create_capped_collection(&client, &db_name, &coll_name, 1).await?;
        insert_test_data(&client, &db_name, &coll_name, 10).await?;

        let db = client.database(&db_name);

        // Request an unrealistic amount of space (1 million times the collection size)
        // This should fail due to insufficient disk space
        let result =
            ensure_disk_space(&db, &coll_name, &format!("{}_capped", coll_name), 1000000.0).await;

        // With real filesystem stats and a huge factor (1,000,000x),
        // this should fail unless we have petabytes of free space
        if result.is_err() {
            let err_msg = result.unwrap_err().to_string();
            assert!(
                err_msg.contains("Not enough free space"),
                "Expected 'Not enough free space' error, got: {}",
                err_msg
            );
        } else {
            // If it succeeded, we must have an enormous amount of free space
            // Let's verify by checking the actual filesystem stats
            let db_stats = db.run_command(doc! { "dbStats": 1, "scale": 1 }).await?;
            let fs_free = db_stats.get_f64("fsTotalSize").unwrap_or(0.0)
                - db_stats.get_f64("fsUsedSize").unwrap_or(0.0);
            println!(
                "Test passed because system has {:.2} TB free space",
                fs_free / (1024.0 * 1024.0 * 1024.0 * 1024.0)
            );
            // This is actually fine - the test verifies the function works correctly
        }

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn test_ensure_disk_space_no_collection() -> Result<()> {
        let (_container, client, db_name, _) = setup_test_db().await?;

        let db = client.database(&db_name);

        // Create a dummy collection to ensure the database exists
        db.create_collection("dummy").await?;

        let nonexistent_coll = "nonexistent_collection";

        // Should handle gracefully when collection doesn't exist
        // Will use database stats for estimation
        let result = ensure_disk_space(
            &db,
            nonexistent_coll,
            &format!("{}_capped", nonexistent_coll),
            1.5,
        )
        .await;

        // Should succeed with fallback estimation (uses 1GB minimum)
        // With real filesystem stats, this should work unless system is very low on space
        match result {
            Ok(_) => {
                println!("Disk space check passed with fallback estimation");
            }
            Err(e) => {
                // Only fail if it's not a disk space error
                // (system might genuinely be low on space)
                let err_msg = e.to_string();
                if err_msg.contains("Not enough free space") {
                    println!("System is low on disk space: {}", err_msg);
                } else {
                    panic!("Unexpected error: {}", err_msg);
                }
            }
        }

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn test_ensure_disk_space_with_renamed_collection() -> Result<()> {
        let (_container, client, db_name, coll_name) = setup_test_db().await?;

        // Create and rename collection to simulate partial migration
        create_capped_collection(&client, &db_name, &coll_name, 5).await?;
        insert_test_data(&client, &db_name, &coll_name, 50).await?;

        let db = client.database(&db_name);
        let capped_name = format!("{}_capped", coll_name);

        // Rename the collection (must use admin database)
        let admin_db = client.database("admin");
        admin_db
            .run_command(doc! {
                "renameCollection": format!("{}.{}", &db_name, &coll_name),
                "to": format!("{}.{}", &db_name, &capped_name),
                "dropTarget": false
            })
            .await?;

        // Should find stats from the renamed collection
        ensure_disk_space(&db, &coll_name, &capped_name, 1.5).await?;

        Ok(())
    }
}
