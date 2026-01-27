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

use std::{hash::Hash, marker::PhantomData, num::NonZeroUsize};

use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use lru::LruCache;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_types::NodeId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignatureVerifierError {
    /// The verifier's rate limit was exceeded for this key or source.
    RateLimited,
    /// The provided signature could not be verified.
    InvalidSignature,
}

pub struct SignatureVerifier<ST, CacheKey, SD>
where
    ST: CertificateSignatureRecoverable,
{
    signature_cache: Option<LruCache<CacheKey, NodeId<CertificateSignaturePubKey<ST>>>>,
    rate_limiter: Option<DefaultDirectRateLimiter>,
    _signing_domain: PhantomData<SD>,
}

impl<ST, CacheKey, SD> SignatureVerifier<ST, CacheKey, SD>
where
    ST: CertificateSignatureRecoverable,
{
    pub fn new() -> Self {
        Self {
            signature_cache: None,
            rate_limiter: None,
            _signing_domain: PhantomData,
        }
    }

    pub fn with_cache(mut self, size: usize) -> Self
    where
        CacheKey: Eq + Hash,
    {
        let cache_size = NonZeroUsize::new(size).expect("cache size must be non-zero");
        self.signature_cache = Some(LruCache::new(cache_size));
        self
    }

    pub fn with_rate_limit(mut self, rate_per_second: u32) -> Self {
        let quota = Quota::per_second(
            std::num::NonZero::new(rate_per_second).expect("rate limit must be non-zero"),
        );
        self.rate_limiter = Some(RateLimiter::direct(quota));
        self
    }

    /// Try to load author from cache only, without signature verification.
    pub fn load_cached(
        &mut self,
        cache_key: &CacheKey,
    ) -> Option<NodeId<CertificateSignaturePubKey<ST>>>
    where
        CacheKey: Eq + Hash,
    {
        self.signature_cache
            .as_mut()
            .and_then(|cache| cache.get(cache_key).copied())
    }

    pub fn verify(
        &self,
        signature: ST,
        data: &[u8],
    ) -> Result<NodeId<CertificateSignaturePubKey<ST>>, SignatureVerifierError>
    where
        SD: monad_crypto::signing_domain::SigningDomain,
    {
        // Check rate limit before expensive signature recovery
        if !self.check_rate_limit() {
            return Err(SignatureVerifierError::RateLimited);
        }

        self.verify_unchecked(signature, data)
    }

    // Verify signature, bypassing rate limit check
    pub fn verify_force(
        &self,
        signature: ST,
        data: &[u8],
    ) -> Result<NodeId<CertificateSignaturePubKey<ST>>, SignatureVerifierError>
    where
        SD: monad_crypto::signing_domain::SigningDomain,
    {
        // We still register the verification in the rate limiter, but
        // ignore the result.
        let _ = self.check_rate_limit();
        self.verify_unchecked(signature, data)
    }

    fn verify_unchecked(
        &self,
        signature: ST,
        data: &[u8],
    ) -> Result<NodeId<CertificateSignaturePubKey<ST>>, SignatureVerifierError>
    where
        SD: monad_crypto::signing_domain::SigningDomain,
    {
        let author_pubkey = signature
            .recover_pubkey::<SD>(data)
            .map_err(|_| SignatureVerifierError::InvalidSignature)?;
        let author = NodeId::new(author_pubkey);

        Ok(author)
    }

    pub fn save_cache(
        &mut self,
        cache_key: CacheKey,
        author: NodeId<CertificateSignaturePubKey<ST>>,
    ) where
        CacheKey: Eq + Hash,
    {
        if let Some(cache) = &mut self.signature_cache {
            cache.put(cache_key, author);
        }
    }

    fn check_rate_limit(&self) -> bool {
        self.rate_limiter
            .as_ref()
            .is_none_or(|rl| rl.check().is_ok())
    }
}

impl<ST, CacheKey, SD> Default for SignatureVerifier<ST, CacheKey, SD>
where
    ST: CertificateSignatureRecoverable,
{
    fn default() -> Self {
        Self {
            signature_cache: None,
            rate_limiter: None,
            _signing_domain: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use monad_crypto::{hasher::Hasher, signing_domain};
    use monad_secp::{KeyPair, SecpSignature};

    use super::*;
    use crate::packet::assembler::HEADER_LEN;

    type SignatureType = SecpSignature;
    type CacheKey = [u8; HEADER_LEN + 20];
    type TestSignatureVerifier =
        SignatureVerifier<SignatureType, CacheKey, signing_domain::RaptorcastChunk>;

    fn make_keypair(seed: u8) -> KeyPair {
        let mut hasher = monad_crypto::hasher::HasherType::new();
        hasher.update([seed]);
        let mut hash = hasher.hash();
        KeyPair::from_bytes(&mut hash.0).unwrap()
    }

    fn make_cache_key(seed: u8) -> CacheKey {
        let mut data = [0; _];
        data[0] = seed;
        data
    }

    #[test]
    fn test_cache() {
        const CACHE_SIZE: usize = 3;
        let mut verifier: TestSignatureVerifier = SignatureVerifier::new().with_cache(CACHE_SIZE);

        let authors: Vec<_> = (0..4)
            .map(|i| NodeId::new(make_keypair(i).pubkey()))
            .collect();
        let keys: Vec<_> = (0..4).map(make_cache_key).collect();

        // Save and retrieve first 3 entries
        for i in 0..CACHE_SIZE {
            verifier.save_cache(keys[i], authors[i]);
            assert_eq!(verifier.load_cached(&keys[i]), Some(authors[i]));
        }
        assert_eq!(verifier.load_cached(&keys[0]), Some(authors[0]));
        assert_eq!(verifier.load_cached(&keys[1]), Some(authors[1]));
        assert_eq!(verifier.load_cached(&keys[2]), Some(authors[2]));

        // Adding 4th entry should evict the first
        verifier.save_cache(keys[3], authors[3]);
        assert!(
            verifier.load_cached(&keys[0]).is_none(),
            "key 0 should be evicted"
        );
        assert_eq!(verifier.load_cached(&keys[3]), Some(authors[3]));
    }

    #[test]
    fn test_rate_limit() {
        // Rate limit: 1 verification/sec
        let verifier: TestSignatureVerifier = SignatureVerifier::new().with_rate_limit(1);
        let key = make_keypair(1);

        let data1 = b"message 1";
        let sig1 = key.sign::<signing_domain::RaptorcastChunk>(data1);
        let result1 = verifier.verify(sig1, data1);
        assert!(result1.is_ok(), "first verify should succeed");

        // Second verify should be rate limited
        let data2 = b"message 2";
        let sig2 = key.sign::<signing_domain::RaptorcastChunk>(data2);
        let result2 = verifier.verify(sig2, data2);
        assert!(
            matches!(result2, Err(SignatureVerifierError::RateLimited)),
            "second verify should be rate limited"
        );

        // With bypass, should succeed
        let result3 = verifier.verify_force(sig2, data2);
        assert!(result3.is_ok(), "verify with bypass should succeed");
    }

    #[test]
    fn test_correctness() {
        let verifier: TestSignatureVerifier = SignatureVerifier::new();
        let key = make_keypair(42);

        let data = b"foobar";
        let signature = key.sign::<signing_domain::RaptorcastChunk>(data);

        // Verify using SignatureVerifier
        let verifier_result = verifier
            .verify(signature, data)
            .expect("should verify successfully");
        assert_eq!(verifier_result.pubkey(), key.pubkey());

        // Compare with direct recover_pubkey
        let direct_pubkey = signature
            .recover_pubkey::<signing_domain::RaptorcastChunk>(data)
            .unwrap();
        assert_eq!(verifier_result.pubkey(), direct_pubkey);
    }
}
