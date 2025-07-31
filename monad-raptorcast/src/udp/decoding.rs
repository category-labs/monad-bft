use std::{collections::BTreeMap, num::NonZero};

use bitvec::prelude::*;
use bytes::Bytes;
use lru::LruCache;
use monad_crypto::{
    certificate_signature::PubKey,
    hasher::{Hasher as _, HasherType},
};
use monad_raptor::{ManagedDecoder, SOURCE_SYMBOLS_MIN};
use monad_types::NodeId;

use super::{ValidatedMessage, MAX_REDUNDANCY};
use crate::util::{AppMessageHash, HexBytes, NodeIdHash, Validator};

// The maximum number of pending messages we allow per author.
pub const MAX_PENDING_MESSAGES_PER_AUTHOR: NonZero<usize> = NonZero::new(1000).unwrap();
// The maximum number of authors we allow in each cache.
pub const MAX_AUTHORS: NonZero<usize> = NonZero::new(1000).unwrap();
pub const MAX_RECENTLY_DECODED_SYMBOLS_RETAINED: NonZero<usize> = NonZero::new(10_000).unwrap();

type DecoderStateCache = LruCache<AppMessageHash, DecoderState>;
type ActorScopedDecoderStateCache<PK> = LruCache<NodeId<PK>, DecoderStateCache>;

type ValidatorSet<PK> = BTreeMap<NodeId<PK>, Validator>;

struct TieredCache<PK>
where
    PK: PubKey,
{
    config: DecoderCacheConfig,

    broadcast: ActorScopedDecoderStateCache<PK>,
    validator: ActorScopedDecoderStateCache<PK>,
    p2p: ActorScopedDecoderStateCache<PK>,
}

#[derive(Debug, Clone)]
pub(crate) struct DecoderCacheConfig {
    pub max_authors: NonZero<usize>,
    pub max_pending_messages_per_author: NonZero<usize>,
    pub max_recently_decoded_symbols_retained: NonZero<usize>,
}

impl Default for DecoderCacheConfig {
    fn default() -> Self {
        Self {
            max_authors: MAX_AUTHORS,
            max_pending_messages_per_author: MAX_PENDING_MESSAGES_PER_AUTHOR,
            max_recently_decoded_symbols_retained: MAX_RECENTLY_DECODED_SYMBOLS_RETAINED,
        }
    }
}

pub(crate) struct DecoderCache<PK>
where
    PK: PubKey,
{
    pending_messages: TieredCache<PK>,
    recently_decoded: LruCache<AppMessageHash, RecentlyDecodedState>,
}

impl<PK> Default for DecoderCache<PK>
where
    PK: PubKey,
{
    fn default() -> Self {
        Self::new(Default::default())
    }
}

impl<PK> DecoderCache<PK>
where
    PK: PubKey,
{
    pub fn new(config: DecoderCacheConfig) -> Self {
        Self {
            recently_decoded: LruCache::new(config.max_recently_decoded_symbols_retained),
            pending_messages: TieredCache::new(config),
        }
    }

    pub fn try_decode(
        &mut self,
        message: &ValidatedMessage<PK>,
        validator_set: Option<&ValidatorSet<PK>>,
    ) -> Result<TryDecodeStatus<PK>, TryDecodeError> {
        let decoder_state = match self.decoder_state_entry(message, validator_set) {
            Some(MessageCacheEntry::RecentlyDecoded(recently_decoded)) => {
                recently_decoded
                    .handle_message(message)
                    .map_err(TryDecodeError::InvalidSymbol)?;
                return Ok(TryDecodeStatus::RecentlyDecoded);
            }

            Some(MessageCacheEntry::Pending(decoder_state)) => {
                decoder_state
                    .handle_message(message)
                    .map_err(TryDecodeError::InvalidSymbol)?;
                decoder_state
            }

            None => {
                let decoder_state = DecoderState::from_initial_message(message)
                    .map_err(TryDecodeError::InvalidSymbol)?;
                self.insert_decoder_state(message, decoder_state, validator_set)
            }
        };

        if !decoder_state.decoder.try_decode() {
            return Ok(TryDecodeStatus::NeedsMoreSymbols);
        }

        let Some(mut decoded) = decoder_state.decoder.reconstruct_source_data() else {
            // TODO: should we delete the decoder state here?
            return Err(TryDecodeError::UnableToReconstructSourceData);
        };

        let app_message_len = message
            .app_message_len
            .try_into()
            .expect("usize smaller than u32");
        decoded.truncate(app_message_len);
        let decoded = Bytes::from(decoded);

        let decoder_state = self
            .delete_decoder_state(message, validator_set)
            .expect("DecoderState must exist in cache");

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

        self.recently_decoded.put(
            message.app_message_hash,
            RecentlyDecodedState::from(decoder_state),
        );

        Ok(TryDecodeStatus::Decoded {
            author: message.author,
            app_message: decoded,
        })
    }

    fn decoder_state_entry(
        &mut self,
        message: &ValidatedMessage<PK>,
        validator_set: Option<&ValidatorSet<PK>>,
    ) -> Option<MessageCacheEntry<'_>> {
        if let Some(recently_decoded) = self.recently_decoded.get_mut(&message.app_message_hash) {
            return Some(MessageCacheEntry::RecentlyDecoded(recently_decoded));
        }

        let decoder_cache = self
            .pending_messages
            .get_decoder_cache(message, validator_set);

        if let Some(decoder_state) = decoder_cache.get_mut(&message.app_message_hash) {
            return Some(MessageCacheEntry::Pending(decoder_state));
        }

        None
    }

    fn insert_decoder_state(
        &mut self,
        message: &ValidatedMessage<PK>,
        decoder_state: DecoderState,
        validator_set: Option<&ValidatorSet<PK>>,
    ) -> &mut DecoderState {
        let decoder_cache = self
            .pending_messages
            .get_decoder_cache(message, validator_set);

        decoder_cache.put(message.app_message_hash, decoder_state);
        decoder_cache
            .get_mut(&message.app_message_hash)
            .expect("PENDING_MESSAGE_CACHE_SIZE is 0")
    }

    fn delete_decoder_state(
        &mut self,
        message: &ValidatedMessage<PK>,
        validator_set: Option<&ValidatorSet<PK>>,
    ) -> Option<DecoderState> {
        let decoder_cache = self
            .pending_messages
            .get_author_slot(message, validator_set)
            .get_mut(&message.author);

        decoder_cache.and_then(|lru_cache| lru_cache.pop(&message.app_message_hash))
    }
}

impl<PK> TieredCache<PK>
where
    PK: PubKey,
{
    fn new(config: DecoderCacheConfig) -> Self {
        Self {
            broadcast: LruCache::new(config.max_authors),
            validator: LruCache::new(config.max_authors),
            p2p: LruCache::new(config.max_authors),
            config,
        }
    }

    fn get_decoder_cache<'a>(
        &'a mut self,
        message: &ValidatedMessage<PK>,
        validator_set: Option<&ValidatorSet<PK>>,
    ) -> &'a mut DecoderStateCache {
        let max_pending_messages_per_author = self.config.max_pending_messages_per_author;
        let author_slot = self.get_author_slot(message, validator_set);
        author_slot.get_or_insert_mut(message.author, || {
            DecoderStateCache::new(max_pending_messages_per_author)
        })
    }

    fn get_author_slot(
        &mut self,
        message: &ValidatedMessage<PK>,
        validator_set: Option<&ValidatorSet<PK>>,
    ) -> &mut ActorScopedDecoderStateCache<PK> {
        if message.broadcast {
            return &mut self.broadcast;
        }

        if let Some(validator_set) = validator_set {
            if validator_set.contains_key(&message.author) {
                return &mut self.validator;
            }
        }

        &mut self.p2p
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

pub(crate) enum TryDecodeStatus<PK: PubKey> {
    RecentlyDecoded,
    NeedsMoreSymbols,
    Decoded {
        author: NodeId<PK>,
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
    pub fn log<PK: PubKey>(&self, symbol: &ValidatedMessage<PK>, self_id: &NodeId<PK>) {
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
    fn from_initial_message<PK>(message: &ValidatedMessage<PK>) -> Result<Self, InvalidSymbol>
    where
        PK: PubKey,
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

    fn handle_message<PK>(&mut self, message: &ValidatedMessage<PK>) -> Result<(), InvalidSymbol>
    where
        PK: PubKey,
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

    fn validate_symbol<PK>(&self, message: &ValidatedMessage<PK>) -> Result<(), InvalidSymbol>
    where
        PK: PubKey,
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
    fn handle_message<PK>(&mut self, message: &ValidatedMessage<PK>) -> Result<(), InvalidSymbol>
    where
        PK: PubKey,
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

fn validate_symbol<PK: PubKey>(
    parsed_message: &ValidatedMessage<PK>,
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
    use std::collections::HashSet;

    use bytes::BytesMut;
    use monad_types::{Epoch, Stake};
    use rand::seq::SliceRandom as _;

    use super::*;
    type PK = monad_crypto::NopPubKey;

    const EPOCH: Epoch = Epoch(1);
    const UNIX_TS_MS: u64 = 1_000_000;

    // default preset for messages
    const DATA_SIZE: usize = 20; // data per chunk
    const APP_MESSAGE_SIZE: usize = 1000; // size of an app message
    const REDUNDANCY: usize = 2; // redundancy factor
    const MIN_DECODABLE_SYMBOLS: usize = APP_MESSAGE_SIZE.div_ceil(DATA_SIZE);
    const NUM_SYMBOLS: usize = MIN_DECODABLE_SYMBOLS * REDUNDANCY;

    fn node_id(seed: u64) -> NodeId<PK> {
        NodeId::new(PK::from_bytes(&[seed as u8; 32]).unwrap())
    }

    fn empty_validator_set() -> ValidatorSet<PK> {
        BTreeMap::new()
    }
    fn add_validators(set: &mut ValidatorSet<PK>, ids: &[u64]) {
        let stake = Stake(100);
        for id in ids {
            let node_id = node_id(*id);
            set.insert(node_id, Validator { stake });
        }
    }

    fn make_cache(max_authors: usize, max_messages_per_author: usize) -> DecoderCache<PK> {
        DecoderCache::new(DecoderCacheConfig {
            max_authors: NonZero::new(max_authors).unwrap(),
            max_pending_messages_per_author: NonZero::new(max_messages_per_author).unwrap(),
            max_recently_decoded_symbols_retained: MAX_RECENTLY_DECODED_SYMBOLS_RETAINED,
        })
    }

    fn make_symbols(app_message: &Bytes, author: NodeId<PK>) -> Vec<ValidatedMessage<PK>> {
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
                unix_ts_ms: UNIX_TS_MS,
            };
            messages.push(message);
        }
        messages
    }

    #[test]
    fn test_successful_decoding() {
        let app_message = Bytes::from(vec![1u8; APP_MESSAGE_SIZE]);
        let author = node_id(0);
        let symbols = make_symbols(&app_message, author);
        assert_eq!(symbols.len(), MIN_DECODABLE_SYMBOLS * REDUNDANCY);

        for n in 0..MIN_DECODABLE_SYMBOLS {
            let mut cache = make_cache(1, 1); // 1 author, 1 pending msg
            let part_of_messages = symbols.iter().take(n);
            let res = try_decode_all(&mut cache, None, part_of_messages)
                .expect("Decoding should succeed");
            assert!(res.is_empty(), "Should not decode any message yet");
        }

        for n in MIN_DECODABLE_SYMBOLS..symbols.len() {
            let mut cache = make_cache(1, 1); // 1 author, 1 pending msg
            let part_of_messages = symbols.iter().take(n);
            let res = try_decode_all(&mut cache, None, part_of_messages)
                .expect("Decoding should succeed");
            assert!(res.len() <= 1);

            if n >= MIN_DECODABLE_SYMBOLS * 12 / 10 {
                assert_eq!(res.len(), 1, "Should decode with enough symbols");
            }
        }

        let mut cache = make_cache(1, 1); // 1 author, 1 pending message
        let all_messages = symbols.iter();
        let res = try_decode_all(&mut cache, None, all_messages).expect("Decoding should succeed");

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
    fn test_successful_decoding_with_multiple_authors() {
        const N_AUTHORS: usize = 10;
        let all_symbols = (0..N_AUTHORS)
            .flat_map(|i| {
                let author = node_id(i as u64);
                make_symbols(&Bytes::from(vec![i as u8; APP_MESSAGE_SIZE]), author)
            })
            .collect::<Vec<_>>();
        let validator_set = empty_validator_set();

        let mut cache = make_cache(1, 1); // only one slot per author
        let res = try_decode_all(&mut cache, Some(&validator_set), all_symbols.iter())
            .expect("Decoding should succeed");
        assert!(res.len() == N_AUTHORS);
    }

    #[test]
    fn test_author_eviction() {
        const N_AUTHORS: usize = 10;
        let mut author_symbols = (0..N_AUTHORS)
            .map(|i| {
                let author = node_id(i as u64);
                make_symbols(&Bytes::from(vec![i as u8; APP_MESSAGE_SIZE]), author)
            })
            .collect::<Vec<Vec<_>>>();

        // the remaining half of symbols for each author is guaranteed undecodable
        let symbols_second_half: Vec<_> = author_symbols
            .iter_mut()
            .flat_map(|symbols| symbols.split_off(MIN_DECODABLE_SYMBOLS - 1))
            .rev() // reverse so the first author's symbols are seen first and last.
            .collect();
        let symbols_first_half: Vec<_> = author_symbols.into_iter().flatten().collect();
        let all_symbols: Vec<_> = symbols_first_half
            .into_iter()
            .chain(symbols_second_half)
            .collect::<Vec<_>>();
        assert_eq!(
            all_symbols.len(),
            N_AUTHORS * NUM_SYMBOLS,
            "Should have enough symbols for decoding all app messages"
        );

        // the cache has one author slot fewer than necessary
        let mut cache = make_cache(N_AUTHORS - 1, 1);
        let res =
            try_decode_all(&mut cache, None, all_symbols.iter()).expect("Decoding should succeed");

        assert_eq!(
            res.len(),
            N_AUTHORS - 1,
            "Should decode all but one authors"
        );
        let decoded_authors = res.iter().map(|(author, _)| author).collect::<HashSet<_>>();

        for i in 1..N_AUTHORS {
            assert!(decoded_authors.contains(&node_id(i as u64)));
        }
        // first author (0) should not be decoded due to the LRU policy
        assert!(!decoded_authors.contains(&node_id(0)));
    }

    #[test]
    fn test_decoder_cache_eviction() {
        let author = node_id(0);
        let app_message0 = Bytes::from(vec![0u8; APP_MESSAGE_SIZE]);
        let symbols0 = make_symbols(&app_message0, author);

        let app_message1 = Bytes::from(vec![1u8; APP_MESSAGE_SIZE]);
        let symbols1 = make_symbols(&app_message1, author);

        // Cache with only 1 author slot and 1 pending message per author
        let mut cache = make_cache(1, 1);
        let validator_set = empty_validator_set();

        let res = try_decode_all(
            &mut cache,
            Some(&validator_set),
            symbols0.iter().take(MIN_DECODABLE_SYMBOLS - 1),
        )
        .expect("Decoding should succeed");
        assert!(res.is_empty(), "Should not decode yet");

        let res = try_decode_all(&mut cache, Some(&validator_set), symbols1.iter())
            .expect("Decoding should succeed");
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].1, app_message1);

        let res = try_decode_all(
            &mut cache,
            Some(&validator_set),
            symbols0
                .iter()
                .skip(MIN_DECODABLE_SYMBOLS - 1)
                .take(MIN_DECODABLE_SYMBOLS - 1),
        )
        .expect("Decoding should succeed");
        assert!(
            res.is_empty(),
            "Should not decode due to evicted decoder state"
        );
    }

    #[test]
    fn test_tiered_caches_work_independently() {
        let symbols_p2p = make_symbols(&Bytes::from(vec![2u8; APP_MESSAGE_SIZE]), node_id(0));
        let symbols_broadcast = make_symbols(&Bytes::from(vec![3u8; APP_MESSAGE_SIZE]), node_id(1))
            .into_iter()
            .map(|mut msg| {
                msg.broadcast = true;
                msg
            })
            .collect::<Vec<_>>();
        let symbols_validator = make_symbols(&Bytes::from(vec![4u8; APP_MESSAGE_SIZE]), node_id(1));

        let mut validator_set = empty_validator_set();
        add_validators(&mut validator_set, &[1]);

        let mut all_symbols: Vec<_> = symbols_p2p
            .into_iter()
            .chain(symbols_broadcast)
            .chain(symbols_validator)
            .collect();
        all_symbols.shuffle(&mut rand::thread_rng());

        // one author slot (per tier), each with one pending message
        let mut cache = make_cache(1, 1);
        let res = try_decode_all(&mut cache, Some(&validator_set), all_symbols.iter())
            .expect("Decoding should succeed");
        assert_eq!(res.len(), 3, "Should decode all three messages");
    }

    fn try_decode_all<'a>(
        cache: &mut DecoderCache<PK>,
        validator_set: Option<&ValidatorSet<PK>>,
        symbols: impl Iterator<Item = &'a ValidatedMessage<PK>>,
    ) -> Result<Vec<(NodeId<PK>, Bytes)>, TryDecodeError> {
        let mut decoded = Vec::new();
        for symbol in symbols {
            match cache.try_decode(symbol, validator_set)? {
                TryDecodeStatus::Decoded {
                    author,
                    app_message,
                } => {
                    decoded.push((author, app_message));
                }
                TryDecodeStatus::NeedsMoreSymbols => {}
                TryDecodeStatus::RecentlyDecoded => {}
            }
        }
        Ok(decoded)
    }
}
