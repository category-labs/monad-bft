use std::collections::BTreeMap;

use monad_crypto::{
    certificate_signature::{CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey},
    hasher::{Hasher, HasherType},
};
use monad_types::{NodeId, Stake};

#[derive(Clone)]
pub struct EpochValidators<ST>
where
    ST: CertificateSignatureRecoverable,
{
    pub validators: BTreeMap<NodeId<CertificateSignaturePubKey<ST>>, Validator>,
}

impl<ST> EpochValidators<ST>
where
    ST: CertificateSignatureRecoverable,
{
    /// Returns a view of the validator set without a given node. On ValidatorsView being dropped,
    /// the validator set is reverted back to normal.
    pub fn view_without(
        &mut self,
        without: Vec<&NodeId<CertificateSignaturePubKey<ST>>>,
    ) -> ValidatorsView<ST> {
        let mut removed = Vec::new();
        for without in without {
            if let Some(removed_validator) = self.validators.remove(without) {
                removed.push((*without, removed_validator));
            }
        }
        ValidatorsView {
            view: &mut self.validators,
            removed,
        }
    }
}

#[derive(Debug)]
pub struct ValidatorsView<'a, ST>
where
    ST: CertificateSignatureRecoverable,
{
    view: &'a mut BTreeMap<NodeId<CertificateSignaturePubKey<ST>>, Validator>,
    removed: Vec<(NodeId<CertificateSignaturePubKey<ST>>, Validator)>,
}
impl<ST> ValidatorsView<'_, ST>
where
    ST: CertificateSignatureRecoverable,
{
    pub fn view(&self) -> &BTreeMap<NodeId<CertificateSignaturePubKey<ST>>, Validator> {
        self.view
    }
}

impl<ST> Drop for ValidatorsView<'_, ST>
where
    ST: CertificateSignatureRecoverable,
{
    fn drop(&mut self) {
        while let Some((without, removed_validator)) = self.removed.pop() {
            self.view.insert(without, removed_validator);
        }
    }
}

#[derive(Debug, Clone)]
pub struct FullNodes<P: PubKey> {
    pub list: Vec<NodeId<P>>,
}

impl<P: PubKey> Default for FullNodes<P> {
    fn default() -> Self {
        Self {
            list: Default::default(),
        }
    }
}

impl<P: PubKey> FullNodes<P> {
    pub fn new(nodes: Vec<NodeId<P>>) -> Self {
        Self { list: nodes }
    }

    pub fn view(&self) -> FullNodesView<P> {
        FullNodesView(&self.list)
    }
}

#[derive(Debug, Clone)]
pub struct FullNodesView<'a, P: PubKey>(&'a Vec<NodeId<P>>);

impl<P: PubKey> FullNodesView<'_, P> {
    pub fn view(&self) -> &Vec<NodeId<P>> {
        self.0
    }
}

#[derive(Debug, Clone)]
pub struct Validator {
    pub stake: Stake,
}

#[derive(Debug)]
pub enum BuildTarget<'a, ST: CertificateSignatureRecoverable> {
    Broadcast(
        // validator stakes for given epoch_no, not including self
        // this MUST NOT BE EMPTY
        ValidatorsView<'a, ST>,
    ),
    Raptorcast(
        // validator stakes for given epoch_no, not including self
        // this MUST NOT BE EMPTY
        ValidatorsView<'a, ST>,
        FullNodesView<'a, CertificateSignaturePubKey<ST>>,
    ), // sharded raptor-aware broadcast
    PointToPoint(&'a NodeId<CertificateSignaturePubKey<ST>>),
}

pub fn compute_hash<PT>(id: &NodeId<PT>) -> NodeIdHash
where
    PT: PubKey,
{
    let mut hasher = HasherType::new();
    hasher.update(id.pubkey().bytes());
    HexBytes(hasher.hash().0[..20].try_into().expect("20 bytes"))
}

#[derive(Copy, Clone, Hash, Eq, Ord, PartialEq, PartialOrd)]
pub struct HexBytes<const N: usize>(pub [u8; N]);
impl<const N: usize> std::fmt::Debug for HexBytes<N> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "0x")?;
        for byte in self.0 {
            write!(f, "{:02x}", byte)?;
        }
        Ok(())
    }
}

pub type NodeIdHash = HexBytes<20>;
pub type AppMessageHash = HexBytes<20>;
