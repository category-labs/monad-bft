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

use std::{marker::PhantomData, num::NonZero};

use lru::LruCache;
use monad_consensus_types::{
    no_endorsement::NoEndorsementCertificate, quorum_certificate::QuorumCertificate,
    timeout::TimeoutCertificate,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_types::ExecutionProtocol;
use monad_validator::signature_collection::SignatureCollection;

pub struct CertificateCache<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    // TODO use QC/TC/NEC hash implementations once they exist
    qc_lru: LruCache<Vec<u8>, ()>,
    tc_lru: LruCache<Vec<u8>, ()>,
    nec_lru: LruCache<Vec<u8>, ()>,
    _pd: PhantomData<(ST, SCT, EPT)>,
}

const QC_CACHE_CAPACITY: usize = 1_000;
const TC_CACHE_CAPACITY: usize = 1_000;
const NEC_CACHE_CAPACITY: usize = 1_000;

impl<ST, SCT, EPT> Default for CertificateCache<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn default() -> Self {
        Self {
            qc_lru: LruCache::new(NonZero::new(QC_CACHE_CAPACITY).unwrap()),
            tc_lru: LruCache::new(NonZero::new(TC_CACHE_CAPACITY).unwrap()),
            nec_lru: LruCache::new(NonZero::new(NEC_CACHE_CAPACITY).unwrap()),
            _pd: PhantomData,
        }
    }
}

impl<ST, SCT, EPT> CertificateCache<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn cache_validated_qc(&mut self, qc: &QuorumCertificate<SCT>) {
        let qc_key = alloy_rlp::encode(qc);
        self.qc_lru.put(qc_key, ());
    }

    pub fn qc_is_cached_validated(&self, qc: &QuorumCertificate<SCT>) -> bool {
        let qc_key = alloy_rlp::encode(qc);
        self.qc_lru.contains(&qc_key)
    }

    pub fn cache_validated_tc(&mut self, tc: &TimeoutCertificate<ST, SCT, EPT>) {
        let tc_key = alloy_rlp::encode(tc);
        self.tc_lru.put(tc_key, ());
    }

    pub fn tc_is_cached_validated(&self, tc: &TimeoutCertificate<ST, SCT, EPT>) -> bool {
        let tc_key = alloy_rlp::encode(tc);
        self.tc_lru.contains(&tc_key)
    }

    pub fn cache_validated_nec(&mut self, nec: &NoEndorsementCertificate<SCT>) {
        let nec_key = alloy_rlp::encode(nec);
        self.nec_lru.put(nec_key, ());
    }

    pub fn nec_is_cached_validated(&self, nec: &NoEndorsementCertificate<SCT>) -> bool {
        let nec_key = alloy_rlp::encode(nec);
        self.nec_lru.contains(&nec_key)
    }
}

#[cfg(test)]
mod test {
    use monad_consensus_types::{
        block::MockExecutionProtocol,
        no_endorsement::{NoEndorsement, NoEndorsementCertificate},
        quorum_certificate::QuorumCertificate,
        timeout::{HighExtend, TimeoutCertificate},
        voting::Vote,
    };
    use monad_crypto::NopSignature;
    use monad_multi_sig::MultiSig;
    use monad_types::{Epoch, Round, GENESIS_BLOCK_ID};

    use crate::validation::certificate_cache::{
        CertificateCache, NEC_CACHE_CAPACITY, QC_CACHE_CAPACITY, TC_CACHE_CAPACITY,
    };

    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<SignatureType>;
    type ExecutionProtocolType = MockExecutionProtocol;

    fn fake_qc(id: usize) -> QuorumCertificate<SignatureCollectionType> {
        QuorumCertificate::new(
            Vote {
                round: Round(id as u64),
                epoch: Epoch(1),
                id: GENESIS_BLOCK_ID,
            },
            MultiSig::default(),
        )
    }

    fn fake_nec(id: usize) -> NoEndorsementCertificate<SignatureCollectionType> {
        NoEndorsementCertificate {
            msg: NoEndorsement {
                round: Round(id as u64 + 1),
                epoch: Epoch(1),
                tip_qc_round: Round(id as u64),
            },
            signatures: MultiSig::default(),
        }
    }

    fn fake_tc(
        id: usize,
    ) -> TimeoutCertificate<SignatureType, SignatureCollectionType, ExecutionProtocolType> {
        TimeoutCertificate {
            round: Round(id as u64),
            epoch: Epoch(1),
            tip_rounds: Vec::new(),
            high_extend: HighExtend::Qc(QuorumCertificate::genesis_qc()),
        }
    }

    #[test]
    fn test_qc() {
        let mut cert_cache = CertificateCache::<
            SignatureType,
            SignatureCollectionType,
            ExecutionProtocolType,
        >::default();

        let qcs: Vec<_> = (0..=QC_CACHE_CAPACITY).map(fake_qc).collect();
        for qc in &qcs {
            assert!(!cert_cache.qc_is_cached_validated(qc));
            cert_cache.cache_validated_qc(qc);
            assert!(cert_cache.qc_is_cached_validated(qc));
        }
        assert!(
            !cert_cache.qc_is_cached_validated(&qcs[0]),
            "oldest was not evicted"
        );
        assert!(
            cert_cache.qc_is_cached_validated(&qcs[1]),
            "unexpectedly evicted"
        );
    }

    #[test]
    fn test_tc() {
        let mut cert_cache = CertificateCache::<
            SignatureType,
            SignatureCollectionType,
            ExecutionProtocolType,
        >::default();

        let tcs: Vec<_> = (0..=TC_CACHE_CAPACITY).map(fake_tc).collect();
        for tc in &tcs {
            assert!(!cert_cache.tc_is_cached_validated(tc));
            cert_cache.cache_validated_tc(tc);
            assert!(cert_cache.tc_is_cached_validated(tc));
        }
        assert!(
            !cert_cache.tc_is_cached_validated(&tcs[0]),
            "oldest was not evicted"
        );
        assert!(
            cert_cache.tc_is_cached_validated(&tcs[1]),
            "unexpectedly evicted"
        );
    }

    #[test]
    fn test_nec() {
        let mut cert_cache = CertificateCache::<
            SignatureType,
            SignatureCollectionType,
            ExecutionProtocolType,
        >::default();

        let necs: Vec<_> = (0..=NEC_CACHE_CAPACITY).map(fake_nec).collect();
        for nec in &necs {
            assert!(!cert_cache.nec_is_cached_validated(nec));
            cert_cache.cache_validated_nec(nec);
            assert!(cert_cache.nec_is_cached_validated(nec));
        }
        assert!(
            !cert_cache.nec_is_cached_validated(&necs[0]),
            "oldest was not evicted"
        );
        assert!(
            cert_cache.nec_is_cached_validated(&necs[1]),
            "unexpectedly evicted"
        );
    }
}
