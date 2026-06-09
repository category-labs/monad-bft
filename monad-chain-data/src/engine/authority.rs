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

//! Single-writer publication head access.

use crate::{
    engine::{digest::ArtifactChecksum, tables::PublicationTables},
    error::Result,
    primitives::state::PublicationState,
    store::meta::MetaStore,
};

#[derive(Debug)]
pub struct HeadPublisher<M: MetaStore> {
    tables: PublicationTables<M>,
}

impl<M: MetaStore> HeadPublisher<M> {
    pub fn new(tables: PublicationTables<M>) -> Self {
        Self { tables }
    }

    /// Read the authoritative published head. A missing row is a cold start.
    pub async fn published_head(&self) -> Result<u64> {
        Ok(self.tables.load_published_head().await?.unwrap_or(0))
    }

    /// Publish the new reader-visible head. The checksum is retained while the
    /// digest chain cleanup is deferred.
    pub async fn publish(
        &self,
        new_head: u64,
        head_artifact_checksum: ArtifactChecksum,
    ) -> Result<()> {
        self.tables
            .store_state(PublicationState {
                indexed_finalized_head: new_head,
                head_artifact_checksum,
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::HeadPublisher;
    use crate::{
        engine::{digest::EMPTY_CHECKSUM, tables::PublicationTables},
        store::InMemoryMetaStore,
    };

    #[tokio::test]
    async fn head_publisher_reads_absent_and_publishes_head() {
        let publisher = HeadPublisher::new(PublicationTables::new(InMemoryMetaStore::default()));

        assert_eq!(publisher.published_head().await.unwrap(), 0);

        publisher.publish(7, EMPTY_CHECKSUM).await.unwrap();
        assert_eq!(publisher.published_head().await.unwrap(), 7);

        publisher.publish(9, EMPTY_CHECKSUM).await.unwrap();
        assert_eq!(publisher.published_head().await.unwrap(), 9);
    }
}
