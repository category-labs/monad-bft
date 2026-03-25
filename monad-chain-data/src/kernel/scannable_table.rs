use bytes::Bytes;

use crate::{
    error::Result,
    store::traits::{MetaStore, PutCond, ScannableKvTable},
};

pub struct ScannableFragmentTable<M: MetaStore> {
    table: ScannableKvTable<M>,
}

impl<M: MetaStore> ScannableFragmentTable<M> {
    pub fn new(table: ScannableKvTable<M>) -> Self {
        Self { table }
    }

    pub async fn load_partition_values(&self, partition: &[u8]) -> Result<Vec<Bytes>> {
        let mut cursor = None;
        let mut values = Vec::new();

        loop {
            let page = self
                .table
                .list_prefix(partition, b"", cursor.take(), 1_024)
                .await?;
            for clustering in page.keys {
                let Some(record) = self.table.get(partition, &clustering).await? else {
                    continue;
                };
                values.push(record.value);
            }
            if page.next_cursor.is_none() {
                break;
            }
            cursor = page.next_cursor;
        }

        Ok(values)
    }

    pub async fn put_value(&self, partition: &[u8], clustering: &[u8], value: Bytes) -> Result<()> {
        let _ = self
            .table
            .put(partition, clustering, value, PutCond::Any)
            .await?;
        Ok(())
    }
}
