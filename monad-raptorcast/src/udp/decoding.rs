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

use super::{ValidatedMessage, MAX_REDUNDANCY};
use crate::util::{compute_hash, AppMessageHash, HexBytes, NodeIdHash};

pub const BROADCAST_TIER_CACHE_SIZE: usize = 10000;
pub const VALIDATOR_TIER_CACHE_SIZE: usize = 10000;
pub const P2P_TIER_CACHE_SIZE: usize = 1000;

pub const RECENTLY_DECODED_CACHE_SIZE: usize = 10000;

pub const VALIDATOR_MIN_SLOTS: usize = 3;
pub const NON_VALIDATOR_SLOTS: usize = 2;

pub const VALIDATOR_MAX_TOTAL_SIZE: MessageSize = 10 * 1024 * 1024; // 10 MB
pub const NON_VALIDATOR_MAX_TOTAL_SIZE: MessageSize = 3 * 1024 * 1024; // 3 MB

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

    // Number of entries to keep for each cache tier.
    pub broadcast_tier_cache_size: usize,
    pub validator_tier_cache_size: usize,
    pub p2p_tier_cache_size: usize,

    // The minimum reserved cache slots for each validator
    pub validator_min_slots: usize,
    // The reserved cache slots for each non-validator
    pub non_validator_slots: usize,

    // The cap of the total size of all pending messages per validator.
    pub validator_max_total_size: MessageSize,
    // The cap of the total size of all pending messages per non-validator.
    pub non_validator_max_total_size: MessageSize,
}

impl Default for DecoderCacheConfig {
    fn default() -> Self {
        Self {
            recently_decoded_cache_size: RECENTLY_DECODED_CACHE_SIZE,

            broadcast_tier_cache_size: BROADCAST_TIER_CACHE_SIZE,
            validator_tier_cache_size: VALIDATOR_TIER_CACHE_SIZE,
            p2p_tier_cache_size: P2P_TIER_CACHE_SIZE,

            validator_min_slots: VALIDATOR_MIN_SLOTS,
            non_validator_slots: NON_VALIDATOR_SLOTS,

            validator_max_total_size: VALIDATOR_MAX_TOTAL_SIZE,
            non_validator_max_total_size: NON_VALIDATOR_MAX_TOTAL_SIZE,
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
        if message.broadcast {
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
        let stake_based_quota = QuotaByStake::new(
            config.validator_min_slots,
            config.non_validator_slots,
            config.validator_max_total_size,
            config.non_validator_max_total_size,
        );
        let fixed_quota = FixedQuota::new(
            config.non_validator_slots,
            config.non_validator_max_total_size,
        );
        let prune_config = PruneConfig {
            // TODO: sync with config.udp_message_max_age_ms
            max_unix_ts_ms_delta: Some(10 * 1000), // 10 seconds
            max_epoch_delta: Some(2),              // 2 epochs
            pruning_min_ratio: 0.1,                // prune at least 10% of cache or enter cooldown
            pruning_cooldown: Duration::from_secs(10), // 10 seconds cooldown
        };

        let broadcast_cache = SoftQuotaCache::new(
            config.broadcast_tier_cache_size,
            prune_config,
            stake_based_quota,
        );
        let validator_cache = SoftQuotaCache::new(
            config.validator_tier_cache_size,
            prune_config,
            stake_based_quota,
        );
        let p2p_cache = SoftQuotaCache::new(config.p2p_tier_cache_size, prune_config, fixed_quota);

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
        cache_size: usize,
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
#[derive(Clone, Copy)]
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
    cache_store: CacheStore<PT>,
    author_index: AuthorIndex<PT>,
    quota_policy: Box<dyn QuotaPolicy<PT> + Send + Sync>,
}

impl<PT: PubKey> SoftQuotaCache<PT> {
    pub fn new<QP>(cache_size: usize, prune_config: PruneConfig, quota_policy: QP) -> Self
    where
        QP: QuotaPolicy<PT> + Send + Sync + 'static,
    {
        Self {
            cache_store: CacheStore::new(cache_size),
            author_index: AuthorIndex::new(prune_config),
            quota_policy: Box::new(quota_policy),
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
            .calc_quota(message, context, self.cache_size());

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
            self.cache_store.remove_many(&evicted_keys);
            debug_assert!(!self.is_full());
            return;
        }

        // current author is below its quota, evict overquota entries
        // from all authors.
        let evicted_keys = self.author_index.enforce_quota_all(context);
        self.cache_store.remove_many(&evicted_keys);
        if !evicted_keys.is_empty() {
            // some keys are evicted, so the cache should no longer be full.
            debug_assert!(!self.is_full());
            return;
        }

        // no other authors are over quota. we will prune any expired
        // keys.
        let expired_keys = self.author_index.prune_expired_all(context);

        self.cache_store.remove_many(&expired_keys);
        if !expired_keys.is_empty() {
            // some keys are evicted, so the cache should no longer be full.
            debug_assert!(!self.is_full());
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
        debug_assert!(self.cache_store.len() <= self.cache_store.max_size + 1);
    }

    pub fn is_full(&self) -> bool {
        self.cache_store.len() > self.cache_store.max_size
    }

    pub fn cache_size(&self) -> usize {
        self.cache_store.max_size
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.cache_store.len()
    }
}

#[derive(Default)]
struct CacheStore<PT: PubKey> {
    max_size: usize,
    store: IndexMap<CacheKey, (NodeId<PT>, DecoderState)>,
}

impl<PT: PubKey> CacheStore<PT> {
    pub fn new(max_size: usize) -> Self {
        assert!(max_size > 0, "max_size must be greater than 0");

        Self {
            max_size,
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

        let random_index = rand::random::<usize>() % self.store.len();
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
        index.update_quota(quota);
        index.insert(
            cache_key.clone(),
            message.unix_ts_ms,
            Epoch(message.epoch),
            message.app_message_len as usize,
        );

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

        let unix_ts_threshold: Option<UnixTimestamp> = context
            .unix_ts_now
            .checked_sub(self.prune_config.max_unix_ts_ms_delta.unwrap_or(0));
        let epoch_threshold: Option<Epoch> = context
            .current_epoch
            .checked_sub(self.prune_config.max_epoch_delta.unwrap_or(0));

        let mut evicted_keys = vec![];

        // we first try only pruning expired keys
        let expired_keys = author_index.prune_expired(unix_ts_threshold, epoch_threshold);
        evicted_keys.extend(expired_keys);

        // if still over quota, compact the cache to fit under quota
        if author_index.is_overquota() {
            evicted_keys.extend(author_index.compact());
        }

        // remove from overquota_authors list as it is now under
        // quota.
        assert!(!author_index.is_overquota());
        self.overquota_authors.remove(author);

        if author_index.is_empty() {
            // cleanup the author index if empty
            self.per_author_index.remove(author);
        }

        evicted_keys
    }

    // Prune expired keys from all authors
    pub fn prune_expired_all(&mut self, context: &DecodingContext<PT>) -> Vec<CacheKey> {
        if let Some(until) = self.pruning_cooldown_until {
            if Instant::now() < until {
                return vec![];
            }
        }

        let unix_ts_threshold: Option<u64> = self
            .prune_config
            .max_unix_ts_ms_delta
            .and_then(|delta| context.unix_ts_now.checked_sub(delta));
        let epoch_threshold: Option<Epoch> = self
            .prune_config
            .max_epoch_delta
            .and_then(|delta| context.current_epoch.checked_sub(delta));

        let mut expired_keys = vec![];
        let mut authors_to_drop = vec![];
        let mut total_cache_size = 0;

        for (author, author_index) in &mut self.per_author_index {
            total_cache_size += author_index.len();
            expired_keys.extend(author_index.prune_expired(unix_ts_threshold, epoch_threshold));

            if author_index.is_empty() {
                authors_to_drop.push(*author);
            }
        }
        for author in authors_to_drop {
            self.per_author_index.remove(&author);
        }

        // if we reclaimed less than the minimum ratio of the cache,
        // we throttle further pruning for a cooldown period.
        let expected_pruning_count = total_cache_size as f32 * self.prune_config.pruning_min_ratio;
        if (expired_keys.len() as f32) < expected_pruning_count {
            self.pruning_cooldown_until = Some(Instant::now() + self.prune_config.pruning_cooldown);
        }

        expired_keys
    }

    pub fn remove(&mut self, author: &NodeId<PT>, key: &CacheKey) {
        if let Some(author_index) = self.per_author_index.get_mut(author) {
            author_index.remove(key);
            if author_index.is_empty() {
                self.per_author_index.remove(author);
            }
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
    reverse_index: BTreeMap<CacheKey, (UnixTimestamp, Epoch, MessageSize)>,
}

impl PerAuthorIndex {
    pub fn new(quota: Quota) -> Self {
        Self {
            quota,
            used_size: 0,
            time_index: BTreeSet::new(),
            epoch_index: BTreeSet::new(),
            reverse_index: BTreeMap::new(),
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

    pub fn remove(&mut self, key: &CacheKey) {
        if let Some((unix_ts_ms, epoch, size)) = self.reverse_index.remove(key) {
            self.time_index.remove(&(unix_ts_ms, key.clone()));
            self.epoch_index.remove(&(epoch, key.clone()));
            self.used_size -= size;
        }
    }

    pub fn remove_many(&mut self, keys: &[CacheKey]) {
        for key in keys {
            self.remove(key);
        }
    }

    pub fn filter_oldest(&mut self, count: usize) -> Vec<CacheKey> {
        self.time_index
            .iter()
            .take(count)
            .map(|(_, key)| key.clone())
            .collect()
    }

    pub fn update_quota(&mut self, new_quota: Quota) {
        self.quota = new_quota;
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
    ) -> Vec<CacheKey> {
        let mut evicted_keys = vec![];
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
    pub fn compact(&mut self) -> Vec<CacheKey> {
        let mut evicted_keys = vec![];

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

    fn prune_by_time(&mut self, threshold: UnixTimestamp) -> Vec<CacheKey> {
        let mut pruned_keys = vec![];
        for (unix_ts, key) in &self.time_index {
            if *unix_ts >= threshold {
                break;
            }
            pruned_keys.push(key.clone());
        }
        self.remove_many(&pruned_keys);
        pruned_keys
    }

    fn prune_by_epoch(&mut self, epoch_threshold: Epoch) -> Vec<CacheKey> {
        let mut pruned_keys = vec![];
        for (epoch, key) in &self.epoch_index {
            if *epoch >= epoch_threshold {
                break;
            }
            pruned_keys.push(key.clone());
        }
        self.remove_many(&pruned_keys);
        pruned_keys
    }

    fn prune_by_slots(&mut self, target_len: usize) -> Vec<CacheKey> {
        let slots_to_free_up = self.len().saturating_sub(target_len);
        if slots_to_free_up == 0 {
            // do nothing if target_len <= self.len()
            return vec![];
        }

        let pruned_keys = self.filter_oldest(slots_to_free_up);
        self.remove_many(&pruned_keys);
        pruned_keys
    }

    fn prune_by_size(&mut self, target_size: MessageSize) -> Vec<CacheKey> {
        let mut pruned_keys = vec![];
        while self.used_size > target_size {
            let (_, key) = self.time_index.first().expect("author index empty");
            let key = key.clone();
            pruned_keys.push(key.clone());
            self.remove(&key);
        }
        pruned_keys
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
        cache_size: usize,
    ) -> Quota {
        Quota {
            max_slots: self.0.max_slots.min(cache_size),
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
            // TODO: fill in some values
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
        cache_size: usize,
    ) -> Quota {
        // validator set not provided, defaults to non-validator slot.
        let Some(validator_set) = &context.validator_set else {
            return Quota {
                max_slots: self.non_validator_slots.min(cache_size),
                max_size: self.non_validator_max_size,
            };
        };

        // author is not validator, defaults to non-validator slot.
        let Some(stake) = validator_set.get(&message.author) else {
            return Quota {
                max_slots: self.non_validator_slots.min(cache_size),
                max_size: self.non_validator_max_size,
            };
        };

        // quota = proportional to stake
        let total_stake: Stake = validator_set.values().copied().sum();
        let stake_fraction = stake.checked_div(total_stake).unwrap_or(0.0);
        let calculated_slots = (stake_fraction * (cache_size as f64)).ceil() as usize;

        let max_slots = calculated_slots
            .max(self.validator_min_slots)
            .min(cache_size);
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
        let symbol_id = message.chunk_id.into();

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

        decoder_state.validate_symbol(message)?;
        decoder_state.seen_esis.set(symbol_id, true);

        decoder_state
            .decoder
            .received_encoded_symbol(&message.chunk, symbol_id);
        *decoder_state
            .recipient_chunks
            .entry(message.recipient_hash)
            .or_insert(0) += 1;

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

#[cfg(test)]
mod test {
    use bytes::BytesMut;
    use itertools::Itertools;
    use monad_types::{Epoch, Stake};
    use rand::seq::SliceRandom as _;

    use super::*;
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
        DecoderCache::new(DecoderCacheConfig {
            broadcast_tier_cache_size,
            validator_tier_cache_size,
            p2p_tier_cache_size,
            recently_decoded_cache_size: RECENTLY_DECODED_CACHE_SIZE,
            ..Default::default()
        })
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
                broadcast: false,
                chunk: chunk.freeze(),
                // these fields are never touched in this module
                recipient_hash: HexBytes([0; 20]),
                message: Bytes::new(),
                epoch: EPOCH.0,
                unix_ts_ms,
                secondary_broadcast: false,
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
            &Bytes::from(vec![2u8; APP_MESSAGE_LEN]),
            node_id(0),
            UNIX_TS_MS,
        );
        let mut symbols_broadcast = make_symbols(
            &Bytes::from(vec![3u8; APP_MESSAGE_LEN]),
            node_id(1),
            UNIX_TS_MS,
        );

        for msg in &mut symbols_broadcast {
            msg.broadcast = true;
        }

        let symbols_validator = make_symbols(
            &Bytes::from(vec![4u8; APP_MESSAGE_LEN]),
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

        let mut cache = make_cache(2, 2, 2);
        let context = DecodingContext::new(Some(&validator_set), UNIX_TS_MS, EPOCH);
        let res = try_decode_all(&mut cache, &context, all_symbols.iter())
            .expect("Decoding should succeed");
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
        assert_eq!(res.len(), 1);
        assert_eq!(cache.recently_decoded_len(), 1);

        // Send one more symbol for the same message.
        let res = cache.try_decode(&symbols[0], &context).unwrap();
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

        let mut config = DecoderCacheConfig {
            validator_tier_cache_size: 10, // cache size: 10
            validator_min_slots: 2,
            ..Default::default()
        };
        let mut cache = DecoderCache::new(config);
        let context = DecodingContext::new(Some(&validator_set), UNIX_TS_MS, EPOCH);
        let res = try_decode_all(&mut cache, &context, all_symbols_part_1.iter())
            .expect("Decoding should succeed");
        assert_eq!(res.len(), 0);
        assert_eq!(cache.recently_decoded_len(), 0);

        // Cache is full but not exceeding max size
        assert_eq!(cache.pending_len(MessageTier::Validator), 10);

        let res = try_decode_all(&mut cache, &context, all_symbols_part_2.iter())
            .expect("Decoding should succeed");
        // Cache size is capped to 10, so only 10 messages can be decoded
        assert_eq!(res.len(), 10);

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

        let config = DecoderCacheConfig {
            broadcast_tier_cache_size: 10, // cache size: 10
            validator_min_slots: 2,        // All validators have at least 2 slots
            non_validator_slots: 1,        // non-validators have at least 1 slot
            ..Default::default()
        };

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
            msg.broadcast = true;
        }
        for msg in &mut all_symbols_part_2 {
            msg.broadcast = true;
        }

        let mut cache = DecoderCache::new(config);
        let context = DecodingContext::new(Some(&validator_set), UNIX_TS_MS, EPOCH);
        let res = try_decode_all(&mut cache, &context, all_symbols_part_1.iter())
            .expect("Decoding should succeed");
        assert_eq!(res.len(), 0);
        assert_eq!(cache.recently_decoded_len(), 0);

        // Cache is full but not exceeding max size
        assert_eq!(cache.pending_len(MessageTier::Broadcast), 10);

        let res = try_decode_all(&mut cache, &context, all_symbols_part_2.iter())
            .expect("Decoding should succeed");

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

        let config = DecoderCacheConfig {
            p2p_tier_cache_size: 10, // cache size: 10
            non_validator_slots: 2,  // each author gets at least 2 slots
            ..Default::default()
        };
        let mut cache = DecoderCache::new(config);
        let context = DecodingContext::new(None, UNIX_TS_MS, EPOCH);
        let res = try_decode_all(&mut cache, &context, all_symbols_part_1.iter())
            .expect("Decoding should succeed");
        assert_eq!(res.len(), 0);
        assert_eq!(cache.recently_decoded_len(), 0);

        // Cache is full but not exceeding max size
        assert_eq!(cache.pending_len(MessageTier::P2P), 10);

        let res = try_decode_all(&mut cache, &context, all_symbols_part_2.iter())
            .expect("Decoding should succeed");

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
        // The total messages exceeds the cache size of 10. So cache
        // slots will be evicted. But the sum of all quota in use
        // exceeds the size of the cache. Therefore, the cache will
        // randomly evict entries.

        let config = DecoderCacheConfig {
            p2p_tier_cache_size: 10, // Cache size: 10
            non_validator_slots: 5,  // All authors get at least 5 slots
            ..Default::default()
        };

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
        assert_eq!(res.len(), 0);

        // cache is full but not exceeding max size
        assert_eq!(cache.pending_len(MessageTier::P2P), 10);

        let res = try_decode_all(&mut cache, &context, all_symbols_part_2.iter())
            .expect("Decoding should succeed");

        // cache is full but not exceeding max size
        assert_eq!(cache.pending_len(MessageTier::P2P), 10);

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

        // Invalid symbol length.
        let mut invalid_symbol = symbols[1].clone();
        invalid_symbol.chunk_id = u16::MAX;
        let res = cache.try_decode(&invalid_symbol, &context);
        assert!(matches!(res, Err(TryDecodeError::InvalidSymbol(_))));

        // Invalid symbol id.
        let mut invalid_symbol = symbols[1].clone();
        invalid_symbol.chunk_id = 9999;
        let res = cache.try_decode(&invalid_symbol, &context);
        assert!(matches!(res, Err(TryDecodeError::InvalidSymbol(_))));

        // Symbol already seen.
        let res = cache.try_decode(&symbols[0], &context);
        assert!(matches!(res, Err(TryDecodeError::InvalidSymbol(_))));
    }

    #[test]
    fn test_cache_rejection_on_full() {
        let author = node_id(0);
        let mut cache = make_cache(2, 2, 2);
        let context = DecodingContext::new(None, UNIX_TS_MS, EPOCH);

        // Fill the cache.
        let app_message0 = Bytes::from(vec![0u8; APP_MESSAGE_LEN]);
        let symbols0 = make_symbols(&app_message0, author, UNIX_TS_MS + 2);
        let _ = cache.try_decode(&symbols0[0], &context);

        let app_message1 = Bytes::from(vec![1u8; APP_MESSAGE_LEN]);
        let symbols1 = make_symbols(&app_message1, author, UNIX_TS_MS + 1);
        let _ = cache.try_decode(&symbols1[0], &context);

        assert_eq!(cache.pending_len(MessageTier::P2P), 2);

        // Try to insert an older message.
        let app_message2 = Bytes::from(vec![2u8; APP_MESSAGE_LEN]);
        let symbols2 = make_symbols(&app_message2, author, UNIX_TS_MS);
        let res = cache.try_decode(&symbols2[0], &context);
        assert_eq!(cache.pending_len(MessageTier::P2P), 2);

        assert!(matches!(res, Ok(TryDecodeStatus::RejectedByCache)));
    }

    fn try_decode_all<'a>(
        cache: &mut DecoderCache<PT>,
        context: &DecodingContext<PT>,
        symbols: impl Iterator<Item = &'a ValidatedMessage<PT>>,
    ) -> Result<Vec<(NodeId<PT>, Bytes)>, TryDecodeError> {
        let mut decoded = Vec::new();
        for symbol in symbols {
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
