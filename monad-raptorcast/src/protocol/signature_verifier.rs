use std::{hash::Hash, marker::PhantomData, num::NonZeroUsize};

use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use lru::LruCache;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_types::NodeId;

use crate::udp::MessageValidationError;

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
        CacheKey: Eq + Hash + Clone,
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
    pub fn try_cached(
        &mut self,
        cache_key: &CacheKey,
    ) -> Option<NodeId<CertificateSignaturePubKey<ST>>>
    where
        CacheKey: Eq + Hash + Clone,
    {
        self.signature_cache
            .as_mut()
            .and_then(|cache| cache.get(cache_key).copied())
    }

    pub fn verify(
        &self,
        signature: ST,
        data: &[u8],
        bypass_rate_limiter: bool,
    ) -> Result<NodeId<CertificateSignaturePubKey<ST>>, MessageValidationError>
    where
        SD: monad_crypto::signing_domain::SigningDomain,
    {
        // Check rate limit before expensive signature recovery
        if !(self.check_rate_limit() || bypass_rate_limiter) {
            return Err(MessageValidationError::RateLimited);
        }

        let author_pubkey = signature
            .recover_pubkey::<SD>(data)
            .map_err(|_| MessageValidationError::InvalidSignature)?;
        let author = NodeId::new(author_pubkey);

        Ok(author)
    }

    pub fn save_cache(
        &mut self,
        cache_key: CacheKey,
        author: NodeId<CertificateSignaturePubKey<ST>>,
    ) where
        CacheKey: Eq + Hash + Clone,
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
    CacheKey: Eq + Hash + Clone,
{
    fn default() -> Self {
        Self {
            signature_cache: None,
            rate_limiter: None,
            _signing_domain: PhantomData,
        }
    }
}
