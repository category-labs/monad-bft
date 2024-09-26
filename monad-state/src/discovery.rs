use std::collections::BTreeMap;

use monad_crypto::certificate_signature::PubKey;
use monad_executor_glue::MonadNameRecord;
use monad_types::NodeId;

#[derive(Debug)]
pub(crate) struct Discovery<PT: PubKey> {
    local_name_record: MonadNameRecord<PT>,
    peers: BTreeMap<NodeId<PT>, MonadNameRecord<PT>>,
}

impl<PT> Discovery<PT>
where
    PT: PubKey,
{
    pub fn new(local_name_record: MonadNameRecord<PT>) -> Self {
        Self {
            local_name_record,
            peers: BTreeMap::default(),
        }
    }

    pub fn add_peer(&mut self, peer: MonadNameRecord<PT>) -> Option<MonadNameRecord<PT>> {
        // TODO(rene): seq no logic here
        let node_id = peer.node_id;

        self.peers.insert(node_id, peer)
    }

    pub fn local_name_record(&self) -> MonadNameRecord<PT> {
        self.local_name_record.clone()
    }

    pub fn peers(&self) -> Vec<MonadNameRecord<PT>> {
        self.peers.values().cloned().collect()
    }
}
