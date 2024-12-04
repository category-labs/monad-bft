use std::marker::PhantomData;

use monad_crypto::certificate_signature::PubKey;

use crate::{BootstrapPeer, Discovery};

pub struct NopDiscovery<PT: PubKey>(pub PhantomData<PT>);

impl<PT: PubKey> Default for NopDiscovery<PT> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<PT: PubKey> Discovery<PT> for NopDiscovery<PT> {
    fn bootstrap_peers(&self) -> Vec<BootstrapPeer<PT>> {
        vec![]
    }
}
