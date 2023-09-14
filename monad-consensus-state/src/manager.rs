use std::collections::{hash_map::Entry, HashMap};

use monad_consensus_types::{
    quorum_certificate::QuorumCertificate, signature_collection::SignatureCollection,
};
use monad_types::{BlockId, Round};

#[derive(Debug, Clone)]
pub struct InFlightBlockSync<SCT> {
    pub qc: QuorumCertificate<SCT>, // qc responsible for this event
}

impl<SCT: SignatureCollection> InFlightBlockSync<SCT> {
    pub fn new(qc: QuorumCertificate<SCT>) -> Self {
        Self { qc }
    }
}

#[derive(Debug, Clone)]
pub struct BlockSyncManager<SCT> {
    requests: HashMap<BlockId, InFlightBlockSync<SCT>>,
}

impl<SCT: SignatureCollection> Default for BlockSyncManager<SCT> {
    fn default() -> Self {
        Self::new()
    }
}

impl<SCT: SignatureCollection> BlockSyncManager<SCT> {
    pub fn new() -> Self {
        Self {
            requests: HashMap::new(),
        }
    }

    pub fn request(&mut self, qc: &QuorumCertificate<SCT>) -> Option<&InFlightBlockSync<SCT>> {
        let id = &qc.info.vote.id;
        match self.requests.entry(*id) {
            Entry::Occupied(_) => None,
            Entry::Vacant(entry) => Some(entry.insert(InFlightBlockSync::new(qc.clone()))),
        }
    }

    pub fn prune(&mut self, round: Round) {
        self.requests.retain(|_, v| v.qc.info.vote.round <= round)
    }

    pub fn handle_request(&mut self, bid: &BlockId) -> Option<QuorumCertificate<SCT>> {
        match self.requests.remove(bid) {
            Some(req) => Some(req.qc),
            _ => None,
        }
    }
}
