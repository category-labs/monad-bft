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

#![allow(clippy::manual_range_contains)]
#![allow(clippy::identity_op)]
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    hash::Hash,
    num::NonZero,
    sync::Arc,
    time::{Duration, Instant},
};

use bitvec::prelude::*;
use bytes::Bytes;
use indexmap::IndexMap;
use lru::LruCache;
use monad_crypto::{
    certificate_signature::PubKey,
    hasher::{Hasher as _, HasherType},
};
use monad_raptor::{ManagedDecoder, SOURCE_SYMBOLS_MIN};
use monad_types::{Epoch, NodeId, Stake};
use rand::Rng as _;

use crate::{
    udp::{ValidatedMessage, MAX_REDUNDANCY},
    util::{compute_hash, AppMessageHash, HexBytes, NodeIdHash},
};

pub(crate) const RECENTLY_DECODED_CACHE_SIZE: usize = 10000;

pub(crate) const BROADCAST_TIER_CONFIG: SoftQuotaCacheConfig = SoftQuotaCacheConfig {
    total_slots: 1000,
    min_slots_per_author: 5,
    max_total_size_per_author: 5 * 4 * 1024 * 1024, // 5*4 MB
    min_slots_per_validator: None,
    max_total_size_per_validator: None,
};

pub(crate) const VALIDATOR_TIER_CONFIG: SoftQuotaCacheConfig = SoftQuotaCacheConfig {
    total_slots: 600,
    min_slots_per_author: 3,
    max_total_size_per_author: 1024 * 1024, // 1 MB
    min_slots_per_validator: None,
    max_total_size_per_validator: None,
};

pub(crate) const P2P_TIER_CONFIG: SoftQuotaCacheConfig = SoftQuotaCacheConfig {
    total_slots: 500,
    min_slots_per_author: 1,
    max_total_size_per_author: 1024 * 1024, // 1 MB
    min_slots_per_validator: None,
    max_total_size_per_validator: None,
};

/// The maximum total message size considered sane for a single cache
/// tier. Panic on startup if a cache's max_total_size exceeds this
/// limit.
pub const MAX_TOTAL_SIZE_LIMIT: usize = 20 * 1024 * 1024 * 1024; // 20 GB

// An abstract size of a message loosely corresponding to the memory
// usage of its corresponding cache entry. We currently use
// app_message_length as the value of this size.
//
// Required properties: (Copy, Add, Sub, Eq, Ord)
type MessageSize = usize;

#[derive(Debug, Clone)]
pub(crate) struct DecoderCacheConfig {
    // Number of entries to keep in recently decoded cache.
    pub recently_decoded_cache_size: usize,

    pub broadcast_tier: SoftQuotaCacheConfig,
    pub validator_tier: SoftQuotaCacheConfig,
    pub p2p_tier: SoftQuotaCacheConfig,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SoftQuotaCacheConfig {
    // Number of entries to keep.
    pub total_slots: usize,

    // The reserved cache slots for each author each tier
    pub min_slots_per_author: usize,

    // The cap on the total size of all pending messages per author
    pub max_total_size_per_author: MessageSize,

    // Set the following two fields to None to use the FixedSlot policy.
    // The minimal reserved cache slots for each validator
    pub min_slots_per_validator: Option<usize>,

    // The cap on the total size of all pending messages per validator
    pub max_total_size_per_validator: Option<MessageSize>,
}

impl Default for DecoderCacheConfig {
    fn default() -> Self {
        Self {
            recently_decoded_cache_size: RECENTLY_DECODED_CACHE_SIZE,
            broadcast_tier: BROADCAST_TIER_CONFIG,
            validator_tier: VALIDATOR_TIER_CONFIG,
            p2p_tier: P2P_TIER_CONFIG,
        }
    }
}

pub(crate) struct DecoderCache<PT>
where
    PT: PubKey,
{
    pending_messages: TieredCache<PT>,
    recently_decoded: LruCache<CacheKey, RecentlyDecodedState>,
}

impl<PT> Default for DecoderCache<PT>
where
    PT: PubKey,
{
    fn default() -> Self {
        Self::new(Default::default())
    }
}

impl<PT> DecoderCache<PT>
where
    PT: PubKey,
{
    pub fn new(config: DecoderCacheConfig) -> Self {
        let recently_decoded_cache_size = NonZero::new(config.recently_decoded_cache_size)
            .expect("recently_decoded_cache_size must be non-zero");

        Self {
            recently_decoded: LruCache::new(recently_decoded_cache_size),
            pending_messages: TieredCache::new(config),
        }
    }

    pub fn try_decode(
        &mut self,
        message: &ValidatedMessage<PT>,
        context: &DecodingContext<'_, PT>,
    ) -> Result<TryDecodeStatus<PT>, TryDecodeError> {
        let cache_key = CacheKey::from_message(message);
        let decoder_state = match self.decoder_state_entry(&cache_key, message, context) {
            Some(MessageCacheEntry::RecentlyDecoded(recently_decoded)) => {
                // the app message was recently decoded
                recently_decoded
                    .handle_message(message)
                    .map_err(TryDecodeError::InvalidSymbol)?;
                return Ok(TryDecodeStatus::RecentlyDecoded);
            }

            Some(MessageCacheEntry::Pending(decoder_state)) => {
                // the decoder is in pending state
                decoder_state
                    .handle_message(message)
                    .map_err(TryDecodeError::InvalidSymbol)?;
                decoder_state
            }

            None => {
                // the decoder state is not in cache, try create a new one
                let decoder_state = DecoderState::from_initial_message(message)
                    .map_err(TryDecodeError::InvalidSymbol)?;

                let Some(decoder_state) =
                    self.insert_decoder_state(&cache_key, message, decoder_state, context)
                else {
                    // the cache rejected the new entry
                    return Ok(TryDecodeStatus::RejectedByCache);
                };
                decoder_state
            }
        };

        if !decoder_state.decoder.try_decode() {
            return Ok(TryDecodeStatus::NeedsMoreSymbols);
        }

        let Some(mut decoded) = decoder_state.decoder.reconstruct_source_data() else {
            return Err(TryDecodeError::UnableToReconstructSourceData);
        };

        // decoding succeeds at this point.
        let app_message_len = message
            .app_message_len
            .try_into()
            .expect("usize smaller than u32");
        decoded.truncate(app_message_len);
        let decoded = Bytes::from(decoded);

        let decoder_state = self
            .remove_decoder_state(&cache_key, message, context)
            .expect("decoder state must exist");

        let decoded_app_message_hash = HexBytes({
            let mut hasher = HasherType::new();
            hasher.update(&decoded);
            hasher.hash().0[..20].try_into().unwrap()
        });
        if decoded_app_message_hash != message.app_message_hash {
            return Err(TryDecodeError::AppMessageHashMismatch {
                expected: message.app_message_hash,
                actual: decoded_app_message_hash,
            });
        }

        self.recently_decoded
            .put(cache_key, RecentlyDecodedState::from(decoder_state));

        Ok(TryDecodeStatus::Decoded {
            author: message.author,
            app_message: decoded,
        })
    }

    fn decoder_state_entry(
        &mut self,
        cache_key: &CacheKey,
        message: &ValidatedMessage<PT>,
        context: &DecodingContext<'_, PT>,
    ) -> Option<MessageCacheEntry<'_>> {
        if let Some(recently_decoded) = self.recently_decoded.get_mut(cache_key) {
            return Some(MessageCacheEntry::RecentlyDecoded(recently_decoded));
        }

        let cache = self.pending_messages.get_cache_tier(message, context);

        if let Some(decoder_state) = cache.get_mut(cache_key) {
            return Some(MessageCacheEntry::Pending(decoder_state));
        }

        None
    }

    fn insert_decoder_state(
        &mut self,
        cache_key: &CacheKey,
        message: &ValidatedMessage<PT>,
        decoder_state: DecoderState,
        context: &DecodingContext<'_, PT>,
    ) -> Option<&mut DecoderState> {
        let cache = self.pending_messages.get_cache_tier(message, context);
        cache.insert(cache_key, message, decoder_state, context);
        cache.get_mut(cache_key)
    }

    fn remove_decoder_state(
        &mut self,
        cache_key: &CacheKey,
        message: &ValidatedMessage<PT>,
        context: &DecodingContext<'_, PT>,
    ) -> Option<DecoderState> {
        let cache = self.pending_messages.get_cache_tier(message, context);
        cache.remove(cache_key).map(|(_author, state)| state)
    }

    #[cfg(test)]
    fn pending_len(&self, tier: MessageTier) -> usize {
        match tier {
            MessageTier::Broadcast => self.pending_messages.broadcast.len(),
            MessageTier::Validator => self.pending_messages.validator.len(),
            MessageTier::P2P => self.pending_messages.p2p.len(),
        }
    }

    #[cfg(test)]
    fn recently_decoded_len(&self) -> usize {
        self.recently_decoded.len()
    }

    // run extensive checks to verify the consistency of the
    // cache's state. only used in tests.
    #[cfg(test)]
    fn consistency_breaches(&self) -> Vec<String> {
        self.pending_messages.consistency_breaches()
    }
}

type ValidatorSet<PT> = BTreeMap<NodeId<PT>, Stake>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MessageTier {
    Broadcast,
    Validator,
    P2P,
}

impl MessageTier {
    fn from_message<PT>(message: &ValidatedMessage<PT>, context: &DecodingContext<'_, PT>) -> Self
    where
        PT: PubKey,
    {
        if message.maybe_broadcast_mode.is_some() {
            return MessageTier::Broadcast;
        }

        if let Some(validator_set) = context.validator_set {
            if validator_set.contains_key(&message.author) {
                return MessageTier::Validator;
            }
        }

        MessageTier::P2P
    }
}

struct TieredCache<PT>
where
    PT: PubKey,
{
    broadcast: SoftQuotaCache<PT>,
    validator: SoftQuotaCache<PT>,
    p2p: SoftQuotaCache<PT>,
}

impl<PT> TieredCache<PT>
where
    PT: PubKey,
{
    fn new(config: DecoderCacheConfig) -> Self {
        let prune_config = PruneConfig {
            // TODO: sync with config.udp_message_max_age_ms
            max_unix_ts_ms_delta: Some(10 * 1000), // 10 seconds
            max_epoch_delta: Some(2),              // 2 epochs
            pruning_min_ratio: 0.1,                // prune at least 10% of cache or enter cooldown
            pruning_cooldown: Duration::from_secs(10), // 10 seconds cooldown
        };

        let broadcast_cache = SoftQuotaCache::new(config.broadcast_tier, prune_config);
        let validator_cache = SoftQuotaCache::new(config.validator_tier, prune_config);
        let p2p_cache = SoftQuotaCache::new(config.p2p_tier, prune_config);

        Self {
            broadcast: broadcast_cache,
            validator: validator_cache,
            p2p: p2p_cache,
        }
    }

    fn get_cache_tier<'a>(
        &'a mut self,
        message: &ValidatedMessage<PT>,
        context: &DecodingContext<'_, PT>,
    ) -> &'a mut SoftQuotaCache<PT> {
        match MessageTier::from_message(message, context) {
            MessageTier::Broadcast => &mut self.broadcast,
            MessageTier::Validator => &mut self.validator,
            MessageTier::P2P => &mut self.p2p,
        }
    }

    #[cfg(test)]
    fn consistency_breaches(&self) -> Vec<String> {
        let mut breaches = vec![];
        breaches.extend(self.broadcast.consistency_breaches("broadcast"));
        breaches.extend(self.validator.consistency_breaches("validator"));
        breaches.extend(self.p2p.consistency_breaches("p2p"));
        breaches
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct CacheKey {
    inner: Arc<CacheKeyInner>,
}

impl CacheKey {
    fn from_message<PT: PubKey>(message: &ValidatedMessage<PT>) -> Self {
        let inner = CacheKeyInner {
            author_hash: compute_hash(&message.author),
            app_message_hash: message.app_message_hash,
            unix_ts_ms: message.unix_ts_ms,
        };
        Self {
            inner: Arc::new(inner),
        }
    }
}

type UnixTimestamp = u64;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct CacheKeyInner {
    author_hash: NodeIdHash,
    app_message_hash: AppMessageHash,
    unix_ts_ms: UnixTimestamp,
}

#[derive(Debug, Clone, Copy)]
struct Quota {
    // The cap on the maximum total message size
    pub max_size: MessageSize,
    // The cap on the maximum number of messages
    pub max_slots: usize,
}

trait QuotaPolicy<PT: PubKey>: Send + Sync {
    fn calc_quota(
        &self,
        message: &ValidatedMessage<PT>,
        context: &DecodingContext<PT>,
        total_slots: usize,
    ) -> Quota;
}

#[derive(Clone, Copy)]
pub struct PruneConfig {
    max_unix_ts_ms_delta: Option<u64>,
    max_epoch_delta: Option<usize>,

    // if a full pruning sweep only reclaims less than this
    // fraction of the cache, we throttle further pruning for a
    // cooldown period.
    pruning_min_ratio: f32,
    pruning_cooldown: Duration,
}

// Context about the current epoch number and the active validator
// set.
#[derive(Clone, Copy, Debug)]
pub struct DecodingContext<'a, PT: PubKey> {
    validator_set: Option<&'a ValidatorSet<PT>>,
    unix_ts_now: UnixTimestamp,
    current_epoch: Epoch,
}

impl<'a, PT: PubKey> DecodingContext<'a, PT> {
    pub fn new(
        validator_set: Option<&'a ValidatorSet<PT>>,
        unix_ts_now: UnixTimestamp,
        current_epoch: Epoch,
    ) -> Self {
        Self {
            validator_set,
            unix_ts_now,
            current_epoch,
        }
    }
}

struct SoftQuotaCache<PT: PubKey> {
    total_slots: usize,
    max_total_size: MessageSize,

    // Any updates to the cache must be atomic such that `cache_store`
    // and `author_index` remain consistent (*) before and after the
    // update. (*): Consistent in the sense that they always track the
    // same set of cache entries.
    cache_store: CacheStore<PT>,
    author_index: AuthorIndex<PT>,

    quota_policy: Box<dyn QuotaPolicy<PT> + Send + Sync>,
}

impl<PT: PubKey> SoftQuotaCache<PT> {
    pub fn new(config: SoftQuotaCacheConfig, prune_config: PruneConfig) -> Self {
        let quota_policy = quota_policy_from_config(&config);

        let approx_num_authors = (config.total_slots / config.min_slots_per_author).max(1);
        let max_total_size = config.max_total_size_per_author * approx_num_authors;

        if max_total_size > MAX_TOTAL_SIZE_LIMIT {
            panic!("SoftQuotaCache max_total_size ({max_total_size}) exceeds the sane limit of {MAX_TOTAL_SIZE_LIMIT} bytes");
        }

        Self {
            total_slots: config.total_slots,
            max_total_size,
            cache_store: CacheStore::new(config.total_slots),
            author_index: AuthorIndex::new(prune_config),
            quota_policy,
        }
    }

    pub fn insert(
        &mut self,
        cache_key: &CacheKey,
        message: &ValidatedMessage<PT>,
        decoder_state: DecoderState,
        context: &DecodingContext<PT>,
    ) {
        let quota = self
            .quota_policy
            .calc_quota(message, context, self.total_slots());

        // insert the new cache entry without checking quota.
        self.insert_unchecked(cache_key, message, decoder_state, quota);

        if !self.is_full() {
            // cache not full, nothing to evict.
            return;
        }

        if self.author_index.is_author_overquota(&message.author) {
            // current author is over quota, evict overquota cache entries
            // from this author.
            let evicted_keys = self.author_index.enforce_quota(&message.author, context);

            // evicted_keys should be non-empty. However, it may be
            // possible to be empty if the author's quota increases
            // on-the-fly should a by-stake quota policy be used. In
            // this case, we fall back to other eviction mechanisms
            // for safety.
            if !evicted_keys.is_empty() {
                self.cache_store.remove_many(&evicted_keys);
                debug_assert!(!self.is_full());
                tracing::debug!(
                    ?message,
                    ?context,
                    "enforced decoding cache quota for author"
                );
                return;
            }
        }

        // current author is below its quota, evict overquota entries
        // from all authors.
        let evicted_keys = self.author_index.enforce_quota_all(context);
        self.cache_store.remove_many(&evicted_keys);
        if !evicted_keys.is_empty() {
            // some keys are evicted, so the cache should no longer be full.
            debug_assert!(!self.is_full());
            tracing::debug!(
                ?message,
                ?context,
                "enforced decoding cache quota for all authors"
            );
            return;
        }

        // no other authors are over quota. we will prune any expired
        // keys.
        let expired_keys = self.author_index.prune_expired_all(context);

        self.cache_store.remove_many(&expired_keys);
        if !expired_keys.is_empty() {
            // some keys are evicted, so the cache should no longer be full.
            debug_assert!(!self.is_full());
            tracing::debug!(
                ?message,
                ?context,
                "evicted expired decoding cache entries for all authors"
            );
            return;
        }

        // at this point, all authors are within their quota, and no
        // keys are considered expired. but we need to make space for
        // the new entry. this may happen when there are many authors
        // with min_slots config. now we randomly evict a cache entry
        // to compensate for the new entry.
        let (key, author, _decoder_state) = self.cache_store.get_random().expect("cache not empty");
        self.author_index.remove(author, &key);
        self.cache_store.remove(&key);

        tracing::warn!(
            ?message,
            ?context,
            ?quota,
            "dropped a decoding cache entry randomly"
        );
    }

    pub fn remove(&mut self, key: &CacheKey) -> Option<(NodeId<PT>, DecoderState)> {
        let (author, decoder_state) = self.cache_store.remove(key)?;
        self.author_index.remove(&author, key);
        Some((author, decoder_state))
    }

    pub fn get_mut(&mut self, key: &CacheKey) -> Option<&mut DecoderState> {
        self.cache_store.get_decoder_state_mut(key)
    }

    // insert without checking for quota or current cache size.
    fn insert_unchecked(
        &mut self,
        cache_key: &CacheKey,
        message: &ValidatedMessage<PT>,
        decoder_state: DecoderState,
        quota: Quota,
    ) {
        let author = message.author;
        self.cache_store
            .insert(cache_key.clone(), (author, decoder_state));
        self.author_index
            .insert_unchecked(author, cache_key, message, quota);

        // we allow overshooting cache size by at most to allow the
        // newly inserted entry to be considered for eviction.
        debug_assert!(self.cache_store.len() <= self.total_slots + 1);
    }

    pub fn is_full(&self) -> bool {
        self.cache_store.len() > self.total_slots
            || self.author_index.used_size > self.max_total_size
    }

    pub fn total_slots(&self) -> usize {
        self.total_slots
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.cache_store.len()
    }

    #[cfg(test)]
    fn consistency_breaches(&self, prefix: &str) -> Vec<String> {
        let mut breaches = vec![];

        // check if cache size exceeds the total slots
        if self.cache_store.len() > self.total_slots {
            breaches.push(format!("{prefix}.cache-store-overflow"))
        }

        // check if cache store has the same author set as author index
        let author_set_1 = self
            .cache_store
            .store
            .values()
            .map(|(author, _)| *author)
            .collect::<HashSet<_>>();
        let author_set_2 = self
            .author_index
            .per_author_index
            .keys()
            .cloned()
            .collect::<HashSet<_>>();
        if author_set_1 != author_set_2 {
            breaches.push(format!("{prefix}.author-set-mismatch"));
        }

        let key_set_1 = self
            .cache_store
            .store
            .keys()
            .cloned()
            .collect::<HashSet<_>>();
        let key_set_2 = self
            .author_index
            .per_author_index
            .iter()
            .flat_map(|(_, index)| index.reverse_index.keys().cloned())
            .collect::<HashSet<_>>();

        if key_set_1 != key_set_2 {
            breaches.push(format!("{prefix}.key-set-mismatch"));
        }

        breaches.extend(self.author_index.consistency_breaches(prefix));

        breaches
    }
}

#[derive(Default)]
struct CacheStore<PT: PubKey> {
    store: IndexMap<CacheKey, (NodeId<PT>, DecoderState)>,
}

impl<PT: PubKey> CacheStore<PT> {
    pub fn new(max_size: usize) -> Self {
        assert!(max_size > 0, "max_size must be greater than 0");

        Self {
            store: IndexMap::with_capacity(max_size + 1),
        }
    }

    pub fn insert(&mut self, key: CacheKey, value: (NodeId<PT>, DecoderState)) {
        self.store.insert(key, value);
    }

    pub fn remove(&mut self, key: &CacheKey) -> Option<(NodeId<PT>, DecoderState)> {
        self.store.swap_remove(key)
    }

    pub fn get_decoder_state_mut(&mut self, key: &CacheKey) -> Option<&mut DecoderState> {
        self.store.get_mut(key).map(|entry| &mut entry.1)
    }

    pub fn get_random(&mut self) -> Option<(CacheKey, &NodeId<PT>, &DecoderState)> {
        if self.store.is_empty() {
            return None;
        }

        let random_index = rand::thread_rng().gen_range(0..self.store.len());
        let (key, (author, decoder_state)) =
            self.store.get_index(random_index).expect("cache not empty");

        Some((key.clone(), author, decoder_state))
    }

    pub fn len(&self) -> usize {
        self.store.len()
    }

    pub fn remove_many(&mut self, keys: &[CacheKey]) {
        for key in keys {
            self.remove(key);
        }
    }
}

struct AuthorIndex<PT: PubKey> {
    prune_config: PruneConfig,

    // The used size of all messages in the cache.
    used_size: MessageSize,

    // The chronological index of cache keys per author.
    per_author_index: HashMap<NodeId<PT>, PerAuthorIndex>,

    // Authors exceeding their quota are registered here for quick
    // eviction check.
    overquota_authors: HashSet<NodeId<PT>>,

    pruning_cooldown_until: Option<Instant>,
}

impl<PT: PubKey> AuthorIndex<PT> {
    pub fn new(prune_config: PruneConfig) -> Self {
        Self {
            used_size: 0,
            prune_config,
            per_author_index: HashMap::new(),
            overquota_authors: HashSet::new(),
            pruning_cooldown_until: None,
        }
    }

    pub fn is_author_overquota(&self, author: &NodeId<PT>) -> bool {
        self.overquota_authors.contains(author)
    }

    pub fn insert_unchecked(
        &mut self,
        author: NodeId<PT>,
        cache_key: &CacheKey,
        message: &ValidatedMessage<PT>,
        quota: Quota,
    ) {
        let index = self
            .per_author_index
            .entry(author)
            .or_insert_with(|| PerAuthorIndex::new(quota));

        let message_size = message.app_message_len as MessageSize;

        index.insert(
            cache_key.clone(),
            message.unix_ts_ms,
            Epoch(message.epoch),
            message_size,
        );
        self.used_size += message_size;

        if index.is_overquota() {
            self.overquota_authors.insert(author);
        } else {
            // the author's quota may be updated to a larger value
            // such that it is now under quota.
            self.overquota_authors.remove(&author);
        }
    }

    pub fn enforce_quota_all(&mut self, context: &DecodingContext<PT>) -> Vec<CacheKey> {
        let mut evicted_keys = vec![];
        let authors_to_evict: Vec<NodeId<PT>> = self.overquota_authors.iter().cloned().collect();

        for author in authors_to_evict {
            let evicted = self.enforce_quota(&author, context);
            evicted_keys.extend(evicted);
        }

        evicted_keys
    }

    // remove items from the index until the author is under quota.
    pub fn enforce_quota(
        &mut self,
        author: &NodeId<PT>,
        context: &DecodingContext<PT>,
    ) -> Vec<CacheKey> {
        let author_index = match self.per_author_index.get_mut(author) {
            Some(index) => index,
            None => return vec![],
        };

        if !author_index.is_overquota() {
            self.overquota_authors.remove(author);
            return vec![];
        }

        let unix_ts_threshold: Option<UnixTimestamp> = self
            .prune_config
            .max_unix_ts_ms_delta
            .and_then(|delta| context.unix_ts_now.checked_sub(delta));
        let epoch_threshold: Option<Epoch> = self
            .prune_config
            .max_epoch_delta
            .and_then(|delta| context.current_epoch.checked_sub(delta));

        let mut evicted_keys = PrunedKeys::empty();

        // we first try only pruning expired keys
        let expired_keys = author_index.prune_expired(unix_ts_threshold, epoch_threshold);
        evicted_keys.extend(expired_keys);

        // if still over quota, compact the cache to fit under quota
        if author_index.is_overquota() {
            evicted_keys.extend(author_index.compact());
        }

        // remove from overquota_authors list as it is now under
        // quota.
        debug_assert!(!author_index.is_overquota());
        self.overquota_authors.remove(author);

        if author_index.is_empty() {
            // cleanup the author index if empty
            self.per_author_index.remove(author);
        }

        self.reap_pruned(evicted_keys)
    }

    // Prune expired keys from all authors
    pub fn prune_expired_all(&mut self, context: &DecodingContext<PT>) -> Vec<CacheKey> {
        if let Some(until) = self.pruning_cooldown_until {
            if Instant::now() < until {
                return vec![];
            }
        }

        let unix_ts_threshold: Option<UnixTimestamp> = self
            .prune_config
            .max_unix_ts_ms_delta
            .and_then(|delta| context.unix_ts_now.checked_sub(delta));
        let epoch_threshold: Option<Epoch> = self
            .prune_config
            .max_epoch_delta
            .and_then(|delta| context.current_epoch.checked_sub(delta));

        let mut authors_to_drop = vec![];
        let mut total_slots = 0;

        let mut pruned_keys = PrunedKeys::empty();
        for (author, author_index) in &mut self.per_author_index {
            total_slots += author_index.len();
            pruned_keys.extend(author_index.prune_expired(unix_ts_threshold, epoch_threshold));

            if author_index.is_empty() {
                authors_to_drop.push(*author);
            }
            if !author_index.is_overquota() {
                self.overquota_authors.remove(author);
            }
        }

        for author in authors_to_drop {
            self.per_author_index.remove(&author);
        }

        // if we reclaimed less than the minimum ratio of the cache,
        // we throttle further pruning for a cooldown period.
        let expired_keys = self.reap_pruned(pruned_keys);
        let expected_pruning_count = total_slots as f32 * self.prune_config.pruning_min_ratio;
        if (expired_keys.len() as f32) < expected_pruning_count {
            self.pruning_cooldown_until = Some(Instant::now() + self.prune_config.pruning_cooldown);
        }

        expired_keys
    }

    // Decompose the pruned keys and update the used_size.
    pub fn reap_pruned(&mut self, pruned_keys: PrunedKeys) -> Vec<CacheKey> {
        let (keys, reclaimed_size) = pruned_keys.decompose();
        self.used_size -= reclaimed_size;
        keys
    }

    pub fn remove(&mut self, author: &NodeId<PT>, key: &CacheKey) {
        let Some(author_index) = self.per_author_index.get_mut(author) else {
            return;
        };

        let evicted_key = author_index.remove(key);
        if !author_index.is_overquota() {
            self.overquota_authors.remove(author);
            if author_index.is_empty() {
                self.per_author_index.remove(author);
            }
        }
        self.reap_pruned(evicted_key);
    }

    #[cfg(test)]
    fn consistency_breaches(&self, prefix: &str) -> Vec<String> {
        let mut breaches = vec![];

        for author in &self.overquota_authors {
            if !self.per_author_index.contains_key(author) {
                breaches.push(format!("{prefix}.overquota_authors invalid"));
            }
        }

        let mut used_size = self.used_size;
        for (author, author_index) in &self.per_author_index {
            if author_index.is_empty() {
                breaches.push(format!("{prefix}.empty-per-author-index"));
            }

            if author_index.is_overquota() && !self.overquota_authors.contains(author) {
                breaches.push(format!("{prefix}.overquota-set-false-negative"));
            }

            if !author_index.is_overquota() && self.overquota_authors.contains(author) {
                breaches.push(format!("{prefix}.overquota-set-false-positive"));
            }

            used_size -= author_index.used_size;
            breaches.extend(author_index.consistency_breaches(&format!("{prefix}.author")));
        }

        if used_size != 0 {
            breaches.push(format!("{prefix}.used_size_mismatch"));
        }

        breaches
    }
}

// A monoid representing a set of cache keys that allows efficient
// access to the combined message size of the pruned entries. The keys
// are assumed to be unique. A key in this set is considered removed
// from the PerAuthorIndex.
//
// Non-public methods are restricted to be used within PerAuthorIndex
// to maintain its type-level semantic. This type is must_use to
// ensure the AuthorIndex's used_size is properly updated.
#[derive(Debug)]
#[must_use]
struct PrunedKeys {
    keys: Vec<CacheKey>,
    reclaimed_size: MessageSize,
}

impl PrunedKeys {
    pub fn empty() -> Self {
        Self {
            keys: vec![],
            reclaimed_size: 0,
        }
    }

    pub fn extend(&mut self, other: PrunedKeys) {
        self.keys.extend(other.keys);
        self.reclaimed_size += other.reclaimed_size;
    }

    pub fn decompose(self) -> (Vec<CacheKey>, MessageSize) {
        (self.keys, self.reclaimed_size)
    }

    fn with_capacity(capacity: usize) -> Self {
        Self {
            keys: Vec::with_capacity(capacity),
            reclaimed_size: 0,
        }
    }

    fn singleton(key: CacheKey, size: MessageSize) -> Self {
        Self {
            keys: vec![key],
            reclaimed_size: size,
        }
    }
}

// A per-author state tracking the pending messages. Supports
// efficient pruning of cache keys based on unix timestamp or
// epoch. Can be efficiently trimmed down to a designated quota.
struct PerAuthorIndex {
    quota: Quota,
    used_size: MessageSize,
    time_index: BTreeSet<(UnixTimestamp, CacheKey)>,
    epoch_index: BTreeSet<(Epoch, CacheKey)>,
    reverse_index: HashMap<CacheKey, (UnixTimestamp, Epoch, MessageSize)>,
}

impl PerAuthorIndex {
    pub fn new(quota: Quota) -> Self {
        Self {
            quota,
            used_size: 0,
            time_index: BTreeSet::new(),
            epoch_index: BTreeSet::new(),
            reverse_index: HashMap::new(),
        }
    }

    pub fn is_overquota(&self) -> bool {
        self.len() > self.quota.max_slots || self.used_size > self.quota.max_size
    }

    pub fn is_empty(&self) -> bool {
        self.reverse_index.is_empty()
    }

    pub fn len(&self) -> usize {
        self.reverse_index.len()
    }

    pub fn remove(&mut self, key: &CacheKey) -> PrunedKeys {
        let Some((unix_ts_ms, epoch, size)) = self.reverse_index.remove(key) else {
            return PrunedKeys::empty();
        };

        self.time_index.remove(&(unix_ts_ms, key.clone()));
        self.epoch_index.remove(&(epoch, key.clone()));
        self.used_size -= size;
        PrunedKeys::singleton(key.clone(), size)
    }

    pub fn remove_many(&mut self, keys: &[CacheKey]) -> PrunedKeys {
        let mut pruned_keys = PrunedKeys::with_capacity(keys.len());
        for key in keys {
            pruned_keys.extend(self.remove(key));
        }
        pruned_keys
    }

    pub fn filter_oldest(&mut self, count: usize) -> Vec<CacheKey> {
        self.time_index
            .iter()
            .take(count)
            .map(|(_, key)| key.clone())
            .collect()
    }

    pub fn insert(
        &mut self,
        cache_key: CacheKey,
        unix_ts_ms: UnixTimestamp,
        epoch: Epoch,
        size: MessageSize,
    ) {
        self.time_index.insert((unix_ts_ms, cache_key.clone()));
        self.epoch_index.insert((epoch, cache_key.clone()));
        self.reverse_index
            .insert(cache_key, (unix_ts_ms, epoch, size));
        self.used_size += size;
    }

    // Remove expired entries.
    pub fn prune_expired(
        &mut self,
        unix_ts_threshold: Option<UnixTimestamp>,
        epoch_threshold: Option<Epoch>,
    ) -> PrunedKeys {
        let mut evicted_keys = PrunedKeys::empty();
        // first, we prune all expired keys
        if let Some(threshold) = unix_ts_threshold {
            evicted_keys.extend(self.prune_by_time(threshold));
        }
        if let Some(threshold) = epoch_threshold {
            evicted_keys.extend(self.prune_by_epoch(threshold));
        }
        evicted_keys
    }

    // Remove entries until under quota
    pub fn compact(&mut self) -> PrunedKeys {
        let mut evicted_keys = PrunedKeys::empty();

        if !self.is_overquota() {
            return evicted_keys;
        }

        // remove oldest entries until the number of used slots is
        // under quota.
        evicted_keys.extend(self.prune_by_slots(self.quota.max_slots));
        // remove oldest entries until the total size fits.
        evicted_keys.extend(self.prune_by_size(self.quota.max_size));

        debug_assert!(!self.is_overquota());

        evicted_keys
    }

    fn prune_by_time(&mut self, threshold: UnixTimestamp) -> PrunedKeys {
        let mut to_prune_keys = vec![];
        for (unix_ts, key) in &self.time_index {
            if *unix_ts >= threshold {
                break;
            }
            to_prune_keys.push(key.clone());
        }
        self.remove_many(&to_prune_keys)
    }

    fn prune_by_epoch(&mut self, epoch_threshold: Epoch) -> PrunedKeys {
        let mut to_prune_keys = vec![];
        for (epoch, key) in &self.epoch_index {
            if *epoch >= epoch_threshold {
                break;
            }
            to_prune_keys.push(key.clone());
        }
        self.remove_many(&to_prune_keys)
    }

    fn prune_by_slots(&mut self, target_len: usize) -> PrunedKeys {
        let slots_to_free_up = self.len().saturating_sub(target_len);
        if slots_to_free_up == 0 {
            // do nothing if target_len <= self.len()
            return PrunedKeys::empty();
        }

        let pruned_keys = self.filter_oldest(slots_to_free_up);
        self.remove_many(&pruned_keys)
    }

    fn prune_by_size(&mut self, target_size: MessageSize) -> PrunedKeys {
        let mut pruned_keys = PrunedKeys::empty();
        while self.used_size > target_size {
            let (_, key) = self.time_index.first().expect("author index empty");
            let key = key.clone();
            pruned_keys.extend(self.remove(&key));
        }
        pruned_keys
    }

    #[cfg(test)]
    fn consistency_breaches(&self, prefix: &str) -> Vec<String> {
        let mut breaches = vec![];
        if self.epoch_index.len() != self.reverse_index.len() {
            breaches.push(format!("{prefix}.epoch-index-size-mismatch"));
        }
        if self.time_index.len() != self.reverse_index.len() {
            breaches.push(format!("{prefix}.time-index-size-mismatch"));
        }

        let mut used_size = self.used_size;
        for (key, (unix_ts, epoch, _size)) in &self.reverse_index {
            if !self.time_index.contains(&(*unix_ts, key.clone())) {
                breaches.push(format!("{prefix}.time-index-missing-key"));
            }
            if !self.epoch_index.contains(&(*epoch, key.clone())) {
                breaches.push(format!("{prefix}.epoch-index-missing-key"));
            }
            used_size -= *_size;
        }

        if used_size != 0 {
            breaches.push(format!("{prefix}.used-size-mismatch"));
        }

        breaches
    }
}

#[derive(Clone, Copy)]
struct FixedQuota(Quota);
impl FixedQuota {
    fn new(max_slots: usize, max_size: MessageSize) -> Self {
        Self(Quota {
            max_slots,
            max_size,
        })
    }
}

impl<PT: PubKey> QuotaPolicy<PT> for FixedQuota {
    fn calc_quota(
        &self,
        _message: &ValidatedMessage<PT>,
        _context: &DecodingContext<PT>,
        total_slots: usize,
    ) -> Quota {
        Quota {
            max_slots: self.0.max_slots.min(total_slots),
            max_size: self.0.max_size,
        }
    }
}

#[derive(Clone, Copy)]
struct QuotaByStake {
    validator_min_slots: usize,
    non_validator_slots: usize,
    validator_max_size: MessageSize,
    non_validator_max_size: MessageSize,
}
impl QuotaByStake {
    pub fn new(
        validator_min_slots: usize,
        non_validator_slots: usize,
        validator_max_size: MessageSize,
        non_validator_max_size: MessageSize,
    ) -> Self {
        Self {
            validator_min_slots,
            non_validator_slots,
            validator_max_size,
            non_validator_max_size,
        }
    }
}

impl<PT: PubKey> QuotaPolicy<PT> for QuotaByStake {
    fn calc_quota(
        &self,
        message: &ValidatedMessage<PT>,
        context: &DecodingContext<PT>,
        total_slots: usize,
    ) -> Quota {
        // validator set not provided, defaults to non-validator slot.
        let Some(validator_set) = &context.validator_set else {
            return Quota {
                max_slots: self.non_validator_slots.min(total_slots),
                max_size: self.non_validator_max_size,
            };
        };

        // author is not validator, defaults to non-validator slot.
        let Some(stake) = validator_set.get(&message.author) else {
            return Quota {
                max_slots: self.non_validator_slots.min(total_slots),
                max_size: self.non_validator_max_size,
            };
        };

        // quota = proportional to stake
        let total_stake: Stake = validator_set.values().copied().sum();
        let stake_fraction = stake.checked_div(total_stake).unwrap_or(0.0);
        let calculated_slots = (stake_fraction * (total_slots as f64)).ceil() as usize;

        let max_slots = calculated_slots
            .max(self.validator_min_slots)
            .min(total_slots);
        Quota {
            max_slots,
            max_size: self.validator_max_size,
        }
    }
}

enum MessageCacheEntry<'a> {
    Pending(&'a mut DecoderState),
    RecentlyDecoded(&'a mut RecentlyDecodedState),
}

#[derive(Debug)]
pub(crate) enum TryDecodeError {
    InvalidSymbol(InvalidSymbol),
    UnableToReconstructSourceData,
    AppMessageHashMismatch {
        expected: AppMessageHash,
        actual: AppMessageHash,
    },
}

#[derive(Debug)]
pub(crate) enum TryDecodeStatus<PT: PubKey> {
    RejectedByCache,
    RecentlyDecoded,
    NeedsMoreSymbols,
    Decoded {
        author: NodeId<PT>,
        app_message: Bytes,
    },
}

#[derive(Debug)]
#[expect(clippy::enum_variant_names)]
pub(crate) enum InvalidSymbol {
    /// The symbol length does not match the expected length.
    InvalidSymbolLength {
        expected_len: usize,
        received_len: usize,
    },
    /// The encoding symbol id is out of bounds for the expected
    /// capacity.
    InvalidSymbolId {
        encoded_symbol_capacity: usize,
        encoding_symbol_id: usize,
    },
    /// The app message length is not consistent
    InvalidAppMessageLength {
        expected_len: usize,
        received_len: usize,
    },
    /// We have already seen a valid symbol with this encoding symbol
    /// id.
    DuplicateSymbol { encoding_symbol_id: usize },
    /// Error when creating a `ManagedDecoder` with invalid parameters (e.g., too many source symbols).
    InvalidDecoderParameter(std::io::Error),
}

impl InvalidSymbol {
    pub fn log<PT: PubKey>(&self, symbol: &ValidatedMessage<PT>, self_id: &NodeId<PT>) {
        match self {
            InvalidSymbol::InvalidSymbolLength {
                expected_len,
                received_len,
            } => {
                tracing::warn!(
                    ?self_id,
                    author =? symbol.author,
                    unix_ts_ms = symbol.unix_ts_ms,
                    app_message_hash =? symbol.app_message_hash,
                    encoding_symbol_id = symbol.chunk_id,
                    expected_len,
                    received_len,
                    "received invalid symbol length"
                );
            }

            InvalidSymbol::InvalidSymbolId {
                encoded_symbol_capacity,
                encoding_symbol_id,
            } => {
                tracing::warn!(
                    ?self_id,
                    author =? symbol.author,
                    unix_ts_ms = symbol.unix_ts_ms,
                    app_message_hash =? symbol.app_message_hash,
                    encoded_symbol_capacity,
                    encoding_symbol_id,
                    "received invalid symbol id"
                );
            }

            InvalidSymbol::InvalidAppMessageLength {
                expected_len,
                received_len,
            } => {
                tracing::warn!(
                    ?self_id,
                    author =? symbol.author,
                    unix_ts_ms = symbol.unix_ts_ms,
                    app_message_hash =? symbol.app_message_hash,
                    encoding_symbol_id = symbol.chunk_id,
                    expected_len,
                    received_len,
                    "received inconsistent app message length"
                );
            }

            InvalidSymbol::DuplicateSymbol { encoding_symbol_id } => {
                tracing::trace!(
                    ?self_id,
                    author =? symbol.author,
                    unix_ts_ms = symbol.unix_ts_ms,
                    app_message_hash =? symbol.app_message_hash,
                    encoding_symbol_id,
                    "received duplicate symbol"
                );
            }

            InvalidSymbol::InvalidDecoderParameter(err) => {
                tracing::error!(
                    ?self_id,
                    author =? symbol.author,
                    unix_ts_ms = symbol.unix_ts_ms,
                    app_message_hash =? symbol.app_message_hash,
                    encoding_symbol_id = symbol.chunk_id,
                    ?err,
                    "invalid parameter for ManagedDecoder::new"
                );
            }
        }
    }
}

struct DecoderState {
    decoder: ManagedDecoder,
    recipient_chunks: BTreeMap<NodeIdHash, usize>,
    encoded_symbol_capacity: usize,
    app_message_len: usize,
    seen_esis: BitVec<usize, Lsb0>,
}

impl DecoderState {
    pub fn from_initial_message<PT>(message: &ValidatedMessage<PT>) -> Result<Self, InvalidSymbol>
    where
        PT: PubKey,
    {
        let symbol_len = message.chunk.len();
        let app_message_len: usize = message
            .app_message_len
            .try_into()
            .expect("usize smaller than u32");

        // symbol_len is always greater than zero, so this division is safe
        let num_source_symbols = app_message_len.div_ceil(symbol_len).max(SOURCE_SYMBOLS_MIN);
        let encoded_symbol_capacity = MAX_REDUNDANCY
            .scale(num_source_symbols)
            .expect("redundancy-scaled num_source_symbols doesn't fit in usize");
        let decoder = ManagedDecoder::new(num_source_symbols, encoded_symbol_capacity, symbol_len)
            .map_err(InvalidSymbol::InvalidDecoderParameter)?;

        let mut decoder_state = DecoderState {
            decoder,
            recipient_chunks: BTreeMap::new(),
            encoded_symbol_capacity,
            app_message_len,
            seen_esis: bitvec![usize, Lsb0; 0; encoded_symbol_capacity],
        };

        decoder_state.handle_message(message)?;

        Ok(decoder_state)
    }

    pub fn handle_message<PT>(
        &mut self,
        message: &ValidatedMessage<PT>,
    ) -> Result<(), InvalidSymbol>
    where
        PT: PubKey,
    {
        self.validate_symbol(message)?;

        let symbol_id = message.chunk_id.into();
        self.seen_esis.set(symbol_id, true);
        self.decoder
            .received_encoded_symbol(&message.chunk, symbol_id);
        *self
            .recipient_chunks
            .entry(message.recipient_hash)
            .or_insert(0) += 1;

        Ok(())
    }

    pub fn validate_symbol<PT>(&self, message: &ValidatedMessage<PT>) -> Result<(), InvalidSymbol>
    where
        PT: PubKey,
    {
        validate_symbol(
            message,
            self.decoder.symbol_len(),
            self.encoded_symbol_capacity,
            self.app_message_len,
            &self.seen_esis,
        )
    }
}

struct RecentlyDecodedState {
    symbol_len: usize,
    encoded_symbol_capacity: usize,
    app_message_len: usize,
    seen_esis: BitVec<usize, Lsb0>,
    excess_chunk_count: usize,
}

impl RecentlyDecodedState {
    pub fn handle_message<PT>(
        &mut self,
        message: &ValidatedMessage<PT>,
    ) -> Result<(), InvalidSymbol>
    where
        PT: PubKey,
    {
        validate_symbol(
            message,
            self.symbol_len,
            self.encoded_symbol_capacity,
            self.app_message_len,
            &self.seen_esis,
        )?;

        let symbol_id = message.chunk_id.into();
        self.seen_esis.set(symbol_id, true);
        self.excess_chunk_count += 1;

        Ok(())
    }
}

impl From<DecoderState> for RecentlyDecodedState {
    fn from(decoder_state: DecoderState) -> Self {
        RecentlyDecodedState {
            symbol_len: decoder_state.decoder.symbol_len(),
            encoded_symbol_capacity: decoder_state.encoded_symbol_capacity,
            app_message_len: decoder_state.app_message_len,
            seen_esis: decoder_state.seen_esis,
            excess_chunk_count: 0,
        }
    }
}

fn validate_symbol<PT: PubKey>(
    parsed_message: &ValidatedMessage<PT>,
    symbol_len: usize,
    encoded_symbol_capacity: usize,
    app_message_len: usize,
    seen_esis: &BitVec,
) -> Result<(), InvalidSymbol> {
    let encoding_symbol_id: usize = parsed_message.chunk_id.into();

    if symbol_len != parsed_message.chunk.len() {
        return Err(InvalidSymbol::InvalidSymbolLength {
            expected_len: symbol_len,
            received_len: parsed_message.chunk.len(),
        });
    }

    if encoding_symbol_id >= encoded_symbol_capacity {
        return Err(InvalidSymbol::InvalidSymbolId {
            encoded_symbol_capacity,
            encoding_symbol_id,
        });
    }

    if parsed_message.app_message_len as usize != app_message_len {
        return Err(InvalidSymbol::InvalidAppMessageLength {
            expected_len: app_message_len,
            received_len: parsed_message.app_message_len as usize,
        });
    }

    if seen_esis[encoding_symbol_id] {
        return Err(InvalidSymbol::DuplicateSymbol { encoding_symbol_id });
    }

    Ok(())
}

fn quota_policy_from_config<PT: PubKey>(config: &SoftQuotaCacheConfig) -> Box<dyn QuotaPolicy<PT>> {
    match (
        config.min_slots_per_validator,
        config.max_total_size_per_validator,
    ) {
        (Some(min_slots), Some(max_size)) => Box::new(QuotaByStake::new(
            min_slots,
            config.min_slots_per_author,
            max_size,
            config.max_total_size_per_author,
        )),

        (Some(min_slots), _) => Box::new(QuotaByStake::new(
            min_slots,
            config.min_slots_per_author,
            config.max_total_size_per_author,
            config.max_total_size_per_author,
        )),

        (_, Some(max_size)) => Box::new(QuotaByStake::new(
            config.min_slots_per_author,
            config.min_slots_per_author,
            max_size,
            config.max_total_size_per_author,
        )),

        _ => Box::new(FixedQuota::new(
            config.min_slots_per_author,
            config.max_total_size_per_author,
        )),
    }
}

#[cfg(test)]
mod test {
    use bytes::BytesMut;
    use itertools::Itertools;
    use monad_types::{Epoch, Stake};
    use rand::seq::SliceRandom as _;

    use super::*;
    use crate::util::BroadcastMode;
    type PT = monad_crypto::NopPubKey;

    const EPOCH: Epoch = Epoch(1);
    const UNIX_TS_MS: u64 = 1_000_000;

    // default preset for messages
    const DATA_SIZE: usize = 20; // data per chunk
    const APP_MESSAGE_LEN: usize = 1000; // size of an app message
    const REDUNDANCY: usize = 2; // redundancy factor
    const MIN_DECODABLE_SYMBOLS: usize = APP_MESSAGE_LEN.div_ceil(DATA_SIZE);

    fn node_id(seed: u64) -> NodeId<PT> {
        NodeId::new(PT::from_bytes(&[seed as u8; 32]).unwrap())
    }

    fn empty_validator_set() -> ValidatorSet<PT> {
        BTreeMap::new()
    }
    fn add_validators(set: &mut ValidatorSet<PT>, ids: &[u64], stake: u64) {
        let stake = Stake::from(stake);
        for id in ids {
            let node_id = node_id(*id);
            set.insert(node_id, stake);
        }
    }

    fn make_cache(
        p2p_tier_cache_size: usize,
        validator_tier_cache_size: usize,
        broadcast_tier_cache_size: usize,
    ) -> DecoderCache<PT> {
        let mut config = DecoderCacheConfig::default();
        config.broadcast_tier.total_slots = broadcast_tier_cache_size;
        config.validator_tier.total_slots = validator_tier_cache_size;
        config.p2p_tier.total_slots = p2p_tier_cache_size;
        DecoderCache::new(config)
    }

    fn make_symbols(
        app_message: &Bytes,
        author: NodeId<PT>,
        unix_ts_ms: u64,
    ) -> Vec<ValidatedMessage<PT>> {
        let data_size = DATA_SIZE;
        let num_symbols = app_message.len().div_ceil(data_size) * REDUNDANCY;

        assert!(num_symbols >= app_message.len() / data_size);
        let app_message_hash = {
            let mut hasher = HasherType::new();
            hasher.update(app_message);
            HexBytes((hasher.hash().0[..20]).try_into().unwrap())
        };
        let encoder = monad_raptor::Encoder::new(app_message, data_size).unwrap();

        let mut messages = Vec::with_capacity(num_symbols);
        for symbol_id in 0..num_symbols {
            let mut chunk = BytesMut::zeroed(data_size);
            encoder.encode_symbol(&mut chunk, symbol_id);
            let message = ValidatedMessage {
                chunk_id: symbol_id as u16,
                author,
                app_message_hash,
                app_message_len: app_message.len() as u32,
                maybe_broadcast_mode: None,
                chunk: chunk.freeze(),
                // these fields are never touched in this module
                recipient_hash: HexBytes([0; 20]),
                message: Bytes::new(),
                epoch: EPOCH.0,
                unix_ts_ms,
            };
            messages.push(message);
        }
        messages
    }

    #[test]
    fn test_successful_decoding() {
        let app_message = Bytes::from(vec![1u8; APP_MESSAGE_LEN]);
        let author = node_id(0);
        let symbols = make_symbols(&app_message, author, UNIX_TS_MS);
        let context = DecodingContext::new(None, UNIX_TS_MS, EPOCH);

        for n in 0..MIN_DECODABLE_SYMBOLS {
            let mut cache = make_cache(10, 10, 10);
            let part_of_messages = symbols.iter().take(n);
            let res = try_decode_all(&mut cache, &context, part_of_messages)
                .expect("Decoding should succeed");
            assert!(res.is_empty(), "Should not decode any message yet");
        }

        for n in MIN_DECODABLE_SYMBOLS..symbols.len() {
            let mut cache = make_cache(10, 10, 10);
            let part_of_messages = symbols.iter().take(n);
            let res = try_decode_all(&mut cache, &context, part_of_messages)
                .expect("Decoding should succeed");
            assert!(res.len() <= 1);

            // >99.9% decoding successful rate
            if n >= MIN_DECODABLE_SYMBOLS * 12 / 10 {
                assert_eq!(res.len(), 1, "Should decode with enough symbols");
            }
        }

        let mut cache = make_cache(10, 10, 10);
        let all_messages = symbols.iter();
        let res =
            try_decode_all(&mut cache, &context, all_messages).expect("Decoding should succeed");
        assert!(cache.consistency_breaches().is_empty());

        assert_eq!(res.len(), 1, "Should decode one message");
        assert_eq!(
            res[0].0, author,
            "Decoded message should be from the correct author"
        );
        assert_eq!(
            res[0].1, app_message,
            "Decoded message should match the original app message"
        );
    }

    #[test]
    fn test_tiered_caches_work_independently() {
        let symbols_p2p = make_symbols(
            &Bytes::from(vec![1u8; APP_MESSAGE_LEN]),
            node_id(0),
            UNIX_TS_MS,
        );

        let mut symbols_broadcast = make_symbols(
            &Bytes::from(vec![2u8; APP_MESSAGE_LEN]),
            node_id(1),
            UNIX_TS_MS,
        );
        symbols_broadcast
            .iter_mut()
            .for_each(|msg| msg.maybe_broadcast_mode = Some(BroadcastMode::Primary));

        let symbols_validator = make_symbols(
            &Bytes::from(vec![3u8; APP_MESSAGE_LEN]),
            node_id(1),
            UNIX_TS_MS,
        );

        let mut validator_set = empty_validator_set();
        add_validators(&mut validator_set, &[1], 100);

        let mut all_symbols: Vec<_> = []
            .into_iter()
            .chain(symbols_broadcast)
            .chain(symbols_validator)
            .chain(symbols_p2p)
            .collect();
        all_symbols.shuffle(&mut rand::thread_rng());

        // single slot per tier is enough
        let mut cache = make_cache(1, 1, 1);

        let context = DecodingContext::new(Some(&validator_set), UNIX_TS_MS, EPOCH);
        let res = try_decode_all(&mut cache, &context, all_symbols.iter())
            .expect("Decoding should succeed");

        assert!(cache.consistency_breaches().is_empty());
        assert_eq!(res.len(), 3, "Should decode all three messages");
    }

    #[test]
    fn test_recently_decoded_message_handling() {
        let app_message = Bytes::from(vec![1u8; APP_MESSAGE_LEN]);
        let author = node_id(0);
        let symbols = make_symbols(&app_message, author, UNIX_TS_MS);
        let context = DecodingContext::new(None, UNIX_TS_MS, EPOCH);
        let mut cache = make_cache(10, 10, 10);

        // Decode a message completely.
        let res = try_decode_all(&mut cache, &context, symbols.iter().skip(1))
            .expect("Decoding should succeed");
        assert!(cache.consistency_breaches().is_empty());
        assert_eq!(res.len(), 1);
        assert_eq!(cache.recently_decoded_len(), 1);

        // Send one more symbol for the same message.
        let res = cache.try_decode(&symbols[0], &context).unwrap();
        assert!(cache.consistency_breaches().is_empty());
        assert!(matches!(res, TryDecodeStatus::RecentlyDecoded));
    }

    #[test]
    fn test_time_based_eviction() {
        let mut cache = make_cache(10, 10, 10);
        let old_ts = UNIX_TS_MS - 200000;
        let app_message = Bytes::from(vec![1u8; APP_MESSAGE_LEN]);
        let author = node_id(0);
        let symbols = make_symbols(&app_message, author, old_ts);
        let context = DecodingContext::new(None, old_ts, EPOCH);

        // Insert an old message.
        let _ = cache.try_decode(&symbols[0], &context);
        assert_eq!(cache.pending_len(MessageTier::P2P), 1);

        // Fill the cache to trigger pruning.
        for i in 0..10 {
            let new_app_message = Bytes::from(vec![2u8; APP_MESSAGE_LEN]);
            let new_author = node_id(i);
            let new_symbols = make_symbols(&new_app_message, new_author, UNIX_TS_MS);
            let new_context = DecodingContext::new(None, UNIX_TS_MS, EPOCH);
            let _ = cache.try_decode(&new_symbols[0], &new_context);
            assert!(cache.consistency_breaches().is_empty());
        }

        // The old message should be pruned.
        assert_eq!(cache.pending_len(MessageTier::P2P), 10);
        let key = CacheKey::from_message(&symbols[0]);
        assert!(cache
            .decoder_state_entry(&key, &symbols[0], &context)
            .is_none());
    }

    #[test]
    fn test_stake_based_quota_allocation() {
        let mut validator_set = empty_validator_set();

        // Author 0 has 80% of the stake, so should get 80% of the cache slots.
        // Author 1 has 20% of the stake, so should get 20% of the cache slots.
        add_validators(&mut validator_set, &[0], 80);
        add_validators(&mut validator_set, &[1], 20);

        // Part 1 is designed to be contain insufficient symbols
        let mut all_symbols_part_1 = vec![];
        let mut all_symbols_part_2 = vec![];

        // Insert 10 messages for each author
        for i in 0..10 {
            let app_msg = Bytes::from(vec![0 + i; APP_MESSAGE_LEN]);
            let mut symbols = make_symbols(&app_msg, node_id(0), UNIX_TS_MS);
            all_symbols_part_2.extend(symbols.split_off(MIN_DECODABLE_SYMBOLS));
            all_symbols_part_1.extend(symbols);

            let app_msg = Bytes::from(vec![10 + i; APP_MESSAGE_LEN]);
            let mut symbols = make_symbols(&app_msg, node_id(1), UNIX_TS_MS);
            all_symbols_part_2.extend(symbols.split_off(MIN_DECODABLE_SYMBOLS));
            all_symbols_part_1.extend(symbols);
        }

        let mut config = DecoderCacheConfig::default();
        config.validator_tier.total_slots = 10; // cache size: 10
        config.validator_tier.min_slots_per_validator = Some(2);

        let mut cache = DecoderCache::new(config);
        let context = DecodingContext::new(Some(&validator_set), UNIX_TS_MS, EPOCH);
        let res = try_decode_all(&mut cache, &context, all_symbols_part_1.iter())
            .expect("Decoding should succeed");
        assert!(cache.consistency_breaches().is_empty());
        assert_eq!(res.len(), 0);
        assert_eq!(cache.recently_decoded_len(), 0);

        // Cache is full but not exceeding max size
        assert_eq!(cache.pending_len(MessageTier::Validator), 10);

        let res = try_decode_all(&mut cache, &context, all_symbols_part_2.iter())
            .expect("Decoding should succeed");
        // Cache size is capped to 10, so only 10 messages can be decoded
        assert_eq!(res.len(), 10);

        assert!(cache.consistency_breaches().is_empty());
        let author_msg_count = res.into_iter().counts_by(|sym| sym.0);
        assert_eq!(author_msg_count[&node_id(0)], 8);
        assert_eq!(author_msg_count[&node_id(1)], 2);
    }

    #[test]
    fn test_quota_bounds() {
        // Scenario
        // ========
        //
        // Cache size: 10, validator min slot: 2, non-validator min slot: 1
        //
        // Author 0: 50% stake, no message (quota: 5)
        // Author 1: 49% stake, 10 pending messages (quota: 5)
        // Author 2: 1% stake, 10 pending messages (quota: 2)
        // Author 3: non-validator, 10 pending messages (quota: 1)
        //
        // The total messages exceeds the cache size of 10. So cache
        // slots will be evicted. However, since only 8 slots are
        // actually used, each author is guaranteed to occupy use
        // up to their quota.

        let mut config = DecoderCacheConfig::default();
        // cache size: 10, validators have at least 2 slots, non-validators have at least 1 slot
        config.broadcast_tier.total_slots = 10;
        config.broadcast_tier.min_slots_per_validator = Some(2);
        config.broadcast_tier.min_slots_per_author = 1;

        let mut validator_set = empty_validator_set();
        add_validators(&mut validator_set, &[0], 50);
        add_validators(&mut validator_set, &[1], 49);
        add_validators(&mut validator_set, &[2], 1);
        // Author 3 is not a validator.

        // Part 1 is designed to be contain insufficient symbols
        let mut all_symbols_part_1 = vec![];
        let mut all_symbols_part_2 = vec![];

        // Insert 10 messages for authors 1,2,3
        for i in 0..10 {
            let app_msg = Bytes::from(vec![0 + i; APP_MESSAGE_LEN]);
            let mut symbols = make_symbols(&app_msg, node_id(1), UNIX_TS_MS);
            all_symbols_part_2.extend(symbols.split_off(MIN_DECODABLE_SYMBOLS));
            all_symbols_part_1.extend(symbols);

            let app_msg = Bytes::from(vec![10 + i; APP_MESSAGE_LEN]);
            let mut symbols = make_symbols(&app_msg, node_id(2), UNIX_TS_MS);
            all_symbols_part_2.extend(symbols.split_off(MIN_DECODABLE_SYMBOLS));
            all_symbols_part_1.extend(symbols);

            let app_msg = Bytes::from(vec![20 + i; APP_MESSAGE_LEN]);
            let mut symbols = make_symbols(&app_msg, node_id(3), UNIX_TS_MS);
            all_symbols_part_2.extend(symbols.split_off(MIN_DECODABLE_SYMBOLS));
            all_symbols_part_1.extend(symbols);
        }

        // Use broadcast tier so that validator's and non-validator's
        // messages get mixed in the same cache.
        for msg in &mut all_symbols_part_1 {
            msg.maybe_broadcast_mode = Some(BroadcastMode::Primary);
        }
        for msg in &mut all_symbols_part_2 {
            msg.maybe_broadcast_mode = Some(BroadcastMode::Primary);
        }

        let mut cache = DecoderCache::new(config);
        let context = DecodingContext::new(Some(&validator_set), UNIX_TS_MS, EPOCH);
        let res = try_decode_all(&mut cache, &context, all_symbols_part_1.iter())
            .expect("Decoding should succeed");
        assert!(cache.consistency_breaches().is_empty());
        assert_eq!(res.len(), 0);
        assert_eq!(cache.recently_decoded_len(), 0);

        // Cache is full but not exceeding max size
        assert_eq!(cache.pending_len(MessageTier::Broadcast), 10);

        let res = try_decode_all(&mut cache, &context, all_symbols_part_2.iter())
            .expect("Decoding should succeed");
        assert!(cache.consistency_breaches().is_empty());

        let decoded = res.len();
        assert!(decoded <= 10 && decoded >= 8);

        let author_msg_count = res.iter().counts_by(|sym| sym.0);
        assert!(author_msg_count[&node_id(1)] >= 5);
        assert!(author_msg_count[&node_id(2)] >= 2);
        assert!(author_msg_count[&node_id(3)] >= 1);
    }

    #[test]
    fn test_fixed_slot_quota_allocation() {
        // part 1 is designed to be contain insufficient symbols
        let mut all_symbols_part_1 = vec![];
        let mut all_symbols_part_2 = vec![];

        // Insert 10 messages for each author
        for i in 0..10 {
            let app_msg = Bytes::from(vec![0 + i; APP_MESSAGE_LEN]);
            let mut symbols = make_symbols(&app_msg, node_id(0), UNIX_TS_MS);
            all_symbols_part_2.extend(symbols.split_off(MIN_DECODABLE_SYMBOLS));
            all_symbols_part_1.extend(symbols);

            let app_msg = Bytes::from(vec![10 + i; APP_MESSAGE_LEN]);
            let mut symbols = make_symbols(&app_msg, node_id(1), UNIX_TS_MS);
            all_symbols_part_2.extend(symbols.split_off(MIN_DECODABLE_SYMBOLS));
            all_symbols_part_1.extend(symbols);
        }

        let mut config = DecoderCacheConfig::default();
        config.p2p_tier.total_slots = 10; // cache size: 10
        config.p2p_tier.min_slots_per_author = 2; // each author gets at least 2 slots

        let mut cache = DecoderCache::new(config);
        let context = DecodingContext::new(None, UNIX_TS_MS, EPOCH);
        let res = try_decode_all(&mut cache, &context, all_symbols_part_1.iter())
            .expect("Decoding should succeed");
        assert!(cache.consistency_breaches().is_empty());
        assert_eq!(res.len(), 0);
        assert_eq!(cache.recently_decoded_len(), 0);

        // Cache is full but not exceeding max size
        assert_eq!(cache.pending_len(MessageTier::P2P), 10);

        let res = try_decode_all(&mut cache, &context, all_symbols_part_2.iter())
            .expect("Decoding should succeed");
        assert!(cache.consistency_breaches().is_empty());

        // cache size is capped to 10, so at most 10 messages can be decoded
        let decoded = res.len();
        assert!(decoded <= 10 && decoded >= 4);

        let author_msg_count = res.into_iter().counts_by(|sym| sym.0);
        assert!(author_msg_count[&node_id(0)] >= 2);
        assert!(author_msg_count[&node_id(1)] >= 2);
    }

    #[test]
    fn test_force_randomized_eviction() {
        // Scenario
        // ========
        //
        // Cache size: 10, min slot size: 5
        //
        // Author 0: 10 pending messages (quota: 5)
        // Author 1: 10 pending messages (quota: 5)
        // Author 2: 10 pending messages (quota: 5)
        //
        // The total number of messages (30) exceeds the cache size
        // (10). So cache entries will be evicted. In addition, the
        // sum of all quotas in use (15) exceeds the size of the cache
        // (10). Therefore, the cache will randomly evict entries.

        let mut config = DecoderCacheConfig::default();
        config.p2p_tier.total_slots = 10;
        config.p2p_tier.min_slots_per_author = 5;

        // Part 1 is designed to be contain insufficient symbols
        let mut all_symbols_part_1 = vec![];
        let mut all_symbols_part_2 = vec![];

        // Insert 10 messages for authors 0,1,2
        for i in 0..10 {
            let app_msg = Bytes::from(vec![0 + i; APP_MESSAGE_LEN]);
            let mut symbols = make_symbols(&app_msg, node_id(0), UNIX_TS_MS);
            all_symbols_part_2.extend(symbols.split_off(MIN_DECODABLE_SYMBOLS));
            all_symbols_part_1.extend(symbols);

            let app_msg = Bytes::from(vec![10 + i; APP_MESSAGE_LEN]);
            let mut symbols = make_symbols(&app_msg, node_id(1), UNIX_TS_MS);
            all_symbols_part_2.extend(symbols.split_off(MIN_DECODABLE_SYMBOLS));
            all_symbols_part_1.extend(symbols);

            let app_msg = Bytes::from(vec![20 + i; APP_MESSAGE_LEN]);
            let mut symbols = make_symbols(&app_msg, node_id(2), UNIX_TS_MS);
            all_symbols_part_2.extend(symbols.split_off(MIN_DECODABLE_SYMBOLS));
            all_symbols_part_1.extend(symbols);
        }

        let mut cache = DecoderCache::new(config);
        let context = DecodingContext::new(None, UNIX_TS_MS, EPOCH);
        let res = try_decode_all(&mut cache, &context, all_symbols_part_1.iter())
            .expect("Decoding should succeed");
        assert!(cache.consistency_breaches().is_empty());
        assert_eq!(res.len(), 0);

        // Cache is full but not exceeding max size
        assert_eq!(cache.pending_len(MessageTier::P2P), 10);

        let res = try_decode_all(&mut cache, &context, all_symbols_part_2.iter())
            .expect("Decoding should succeed");
        assert!(cache.consistency_breaches().is_empty());

        // Cache size is capped to 10, so at most 10 messages can be
        // decoded. However, due to the random eviction, it's likely
        // that fewer than 10 messages get decoded.
        let decoded = res.len();
        assert!(decoded <= 10);
    }

    #[test]
    fn test_invalid_symbol_rejection() {
        let mut cache = make_cache(10, 10, 10);
        let app_message = Bytes::from(vec![1u8; APP_MESSAGE_LEN]);
        let author = node_id(0);
        let symbols = make_symbols(&app_message, author, UNIX_TS_MS);
        let context = DecodingContext::new(None, UNIX_TS_MS, EPOCH);

        // Insert a valid symbol first.
        let _ = cache.try_decode(&symbols[0], &context);
        assert!(cache.consistency_breaches().is_empty());

        // Invalid symbol length.
        let mut invalid_symbol = symbols[1].clone();
        invalid_symbol.chunk_id = u16::MAX;
        let res = cache.try_decode(&invalid_symbol, &context);
        assert!(cache.consistency_breaches().is_empty());
        assert!(matches!(res, Err(TryDecodeError::InvalidSymbol(_))));

        // Invalid symbol id.
        let mut invalid_symbol = symbols[1].clone();
        invalid_symbol.chunk_id = 9999;
        let res = cache.try_decode(&invalid_symbol, &context);
        assert!(cache.consistency_breaches().is_empty());
        assert!(matches!(res, Err(TryDecodeError::InvalidSymbol(_))));

        // Symbol already seen.
        let res = cache.try_decode(&symbols[0], &context);
        assert!(cache.consistency_breaches().is_empty());
        assert!(matches!(res, Err(TryDecodeError::InvalidSymbol(_))));
    }

    #[test]
    fn test_cache_rejection_on_slots_full() {
        let author = node_id(0);
        let mut config = DecoderCacheConfig::default();
        config.p2p_tier.total_slots = 2;
        config.p2p_tier.min_slots_per_author = 2;

        let mut cache = DecoderCache::new(config);
        let context = DecodingContext::new(None, UNIX_TS_MS, EPOCH);

        // Fill the cache.
        let app_message0 = Bytes::from(vec![0u8; APP_MESSAGE_LEN]);
        let symbols0 = make_symbols(&app_message0, author, UNIX_TS_MS + 2);
        let _ = cache.try_decode(&symbols0[0], &context);
        assert!(cache.consistency_breaches().is_empty());

        let app_message1 = Bytes::from(vec![1u8; APP_MESSAGE_LEN]);
        let symbols1 = make_symbols(&app_message1, author, UNIX_TS_MS + 1);
        let _ = cache.try_decode(&symbols1[0], &context);
        assert!(cache.consistency_breaches().is_empty());

        assert_eq!(cache.pending_len(MessageTier::P2P), 2);

        // Try to insert an older message.
        let app_message2 = Bytes::from(vec![2u8; APP_MESSAGE_LEN]);
        let symbols2 = make_symbols(&app_message2, author, UNIX_TS_MS);
        let res = cache.try_decode(&symbols2[0], &context);
        assert!(cache.consistency_breaches().is_empty());
        assert_eq!(cache.pending_len(MessageTier::P2P), 2);

        assert!(matches!(res, Ok(TryDecodeStatus::RejectedByCache)));
    }

    #[test]
    fn test_cache_rejection_on_size_limit() {
        let author = node_id(0);
        let mut config = DecoderCacheConfig::default();
        // each author can store 2 full-sized messages before hitting
        // max_size quota
        config.p2p_tier.max_total_size_per_author = APP_MESSAGE_LEN * 2;
        // expected number of authors = 10/5 = 2
        // max total_size is 2*2*APP_MESSAGE_LEN (4 full-sized messages)
        config.p2p_tier.total_slots = 10;
        // each author can have 5 slots.
        config.p2p_tier.min_slots_per_author = 5;

        let mut cache = DecoderCache::new(config);
        let context = DecodingContext::new(None, UNIX_TS_MS, EPOCH);

        // take a single symbol for a given message
        let partial_symbol = |msg: u8, ts: UnixTimestamp| {
            let app_msg = Bytes::from(vec![msg; APP_MESSAGE_LEN]);
            make_symbols(&app_msg, author, ts)[0].clone()
        };

        // feed the cache four messages.
        for i in 1..=4 {
            let res = cache.try_decode(&partial_symbol(i, UNIX_TS_MS), &context);
            assert!(cache.consistency_breaches().is_empty());
            assert!(matches!(res, Ok(TryDecodeStatus::NeedsMoreSymbols)));
            assert!(cache.pending_len(MessageTier::P2P) == i as usize);
            assert!(cache.pending_len(MessageTier::P2P) < 10); // cache slots not full
        }

        // try to insert a 5th message. the cache rejects the message
        // despite the existence of empty slots, because the cache's
        // total_size limit is exceeded.
        let res = cache.try_decode(&partial_symbol(5, UNIX_TS_MS - 1), &context);
        assert!(cache.consistency_breaches().is_empty());
        assert!(matches!(res, Ok(TryDecodeStatus::RejectedByCache)));
        // the offending author's max_size quota is enforced
        assert!(cache.pending_len(MessageTier::P2P) == 2);
    }

    fn try_decode_all<'a>(
        cache: &mut DecoderCache<PT>,
        context: &DecodingContext<PT>,
        symbols: impl Iterator<Item = &'a ValidatedMessage<PT>>,
    ) -> Result<Vec<(NodeId<PT>, Bytes)>, TryDecodeError> {
        let mut decoded = Vec::new();
        for symbol in symbols {
            assert!(cache.consistency_breaches().is_empty());
            if let TryDecodeStatus::Decoded {
                author,
                app_message,
            } = cache.try_decode(symbol, context)?
            {
                decoded.push((author, app_message));
            }
        }
        Ok(decoded)
    }
}
