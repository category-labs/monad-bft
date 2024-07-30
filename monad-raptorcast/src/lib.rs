use std::{
    collections::{hash_map::Entry, BTreeMap, HashMap, VecDeque},
    marker::PhantomData,
    net::SocketAddr,
    ops::{DerefMut, Range},
    pin::Pin,
    task::{Context, Poll, Waker},
};

use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt};
use itertools::Itertools;
use monad_crypto::{
    certificate_signature::{
        CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
    },
    hasher::{Hasher, HasherType},
};
use monad_dataplane::event_loop::{BroadcastMsg, Dataplane, UnicastMsg};
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{Message, RouterCommand};
use monad_merkle::{MerkleHash, MerkleProof, MerkleTree};
use monad_types::{Deserializable, Epoch, NodeId, Round, RouterTarget, Serializable, Stake};
use raptor_code::SourceBlockDecoder;

pub struct RaptorCastConfig<ST>
where
    ST: CertificateSignatureRecoverable,
{
    // TODO support dynamic updating
    pub known_addresses: HashMap<NodeId<CertificateSignaturePubKey<ST>>, SocketAddr>,

    pub key: ST::KeyPairType,
    /// amount of redundancy to send
    /// a value of 2 == send 2x total payload size total
    pub redundancy: u8,

    pub local_addr: String,
}

pub struct RaptorCast<ST, M, OM>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Deserializable<Bytes>,
    OM: Serializable<Bytes> + Into<M> + Clone,
{
    key: ST::KeyPairType,
    redundancy: u8,

    epoch_validators: BTreeMap<Epoch, EpochValidators<ST>>,
    known_addresses: HashMap<NodeId<CertificateSignaturePubKey<ST>>, SocketAddr>,

    current_epoch: Epoch,
    current_round: Round,

    // TODO add a cap on max number of chunks that will be forwarded per message? so that a DOS
    // can't be induced by spamming broadcast chunks to any given node
    // TODO we also need to cap the max number chunks that are decoded - because an adversary could
    // generate a bunch of linearly dependent chunks and cause unbounded memory usage.
    // TODO GC these based on time elapsed since first chunk? Right now we do no GC - we can't do
    // this by round because some node may be trying to sync to us with a much older round.
    message_cache: BTreeMap<MessageCacheKey<CertificateSignaturePubKey<ST>>, SourceBlockDecoder>,
    signature_cache:
        HashMap<[u8; HEADER_LEN as usize - 65 + 20], NodeId<CertificateSignaturePubKey<ST>>>,

    dataplane: Dataplane,
    pending_events: VecDeque<M::Event>,

    waker: Option<Waker>,
    metrics: ExecutorMetrics,
    _phantom: PhantomData<OM>,
}

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

impl<'a, ST> Drop for ValidatorsView<'a, ST>
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
pub struct Validator {
    pub stake: Stake,
}

#[derive(PartialEq, PartialOrd, Eq, Ord)]
struct MessageCacheKey<PT>
where
    PT: PubKey,
{
    // round should be first for Ord derive implementation
    round: Round,
    author: NodeId<PT>,
    app_message_hash: [u8; 20],
    app_message_len: usize,
}

impl<ST, M, OM> RaptorCast<ST, M, OM>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Deserializable<Bytes>,
    OM: Serializable<Bytes> + Into<M> + Clone,
{
    pub fn new(config: RaptorCastConfig<ST>) -> Self {
        let dataplane = Dataplane::new(&config.local_addr);
        Self {
            epoch_validators: Default::default(),
            known_addresses: config.known_addresses,

            key: config.key,
            redundancy: config.redundancy,

            current_epoch: Epoch(0),
            current_round: Round(0),

            message_cache: Default::default(),
            signature_cache: Default::default(),

            dataplane,
            pending_events: Default::default(),

            waker: None,
            metrics: Default::default(),
            _phantom: PhantomData,
        }
    }
}

impl<ST, M, OM> Executor for RaptorCast<ST, M, OM>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Deserializable<Bytes>,
    OM: Serializable<Bytes> + Into<M> + Clone,
{
    type Command = RouterCommand<CertificateSignaturePubKey<ST>, OM>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        let self_id = NodeId::new(self.key.pubkey());
        for command in commands {
            match command {
                RouterCommand::UpdateCurrentRound(epoch, round) => {
                    assert!(epoch >= self.current_epoch);
                    self.current_epoch = epoch;
                    assert!(round >= self.current_round);
                    self.current_round = round;
                    while let Some(entry) = self.epoch_validators.first_entry() {
                        if *entry.key() + Epoch(1) < self.current_epoch {
                            entry.remove();
                        } else {
                            break;
                        }
                    }
                }
                RouterCommand::AddEpochValidatorSet {
                    epoch,
                    validator_set,
                } => {
                    if let Some(epoch_validators) = self.epoch_validators.get(&epoch) {
                        assert_eq!(validator_set.len(), epoch_validators.validators.len());
                        assert!(validator_set.into_iter().all(
                            |(validator_key, validator_stake)| epoch_validators
                                .validators
                                .get(&validator_key)
                                .map(|v| v.stake)
                                == Some(validator_stake)
                        ));
                        tracing::warn!(
                            "duplicate validator set update (this is safe but unexpected)"
                        )
                    } else {
                        let removed = self.epoch_validators.insert(
                            epoch,
                            EpochValidators {
                                validators: validator_set
                                    .into_iter()
                                    .map(|(validator_key, validator_stake)| {
                                        (
                                            validator_key,
                                            Validator {
                                                stake: validator_stake,
                                            },
                                        )
                                    })
                                    .collect(),
                            },
                        );
                        assert!(removed.is_none());
                    }
                }
                RouterCommand::Publish { target, message } => {
                    let app_message = message.serialize();
                    // send message to self if applicable
                    let (epoch, round, build_target) = match &target {
                        RouterTarget::Broadcast(epoch, round) => {
                            if &self.current_round != round {
                                tracing::error!("tried to publish message outside of current_round window - was RouterCommand::UpdateCurrentRound omitted somewhere?")
                            }
                            let Some(epoch_validators) = self.epoch_validators.get_mut(epoch)
                            else {
                                tracing::error!(
                                    "don't have epoch validators populated for epoch: {:?}",
                                    epoch
                                );
                                continue;
                            };

                            if epoch_validators.validators.contains_key(&self_id) {
                                let message: M = message.into();
                                self.pending_events.push_back(message.event(self_id));
                                if let Some(waker) = self.waker.take() {
                                    waker.wake()
                                }
                            }
                            let epoch_validators_without_self =
                                epoch_validators.view_without(vec![&self_id]);
                            if epoch_validators_without_self.view.is_empty() {
                                // this is degenerate case where the only validator is self
                                continue;
                            }

                            (
                                epoch,
                                round,
                                BuildTarget::Broadcast(epoch_validators_without_self),
                            )
                        }
                        RouterTarget::Raptorcast(epoch, round) => {
                            if &self.current_round != round {
                                tracing::error!("tried to publish message outside of current_round window - was RouterCommand::UpdateCurrentRound omitted somewhere?")
                            }
                            let Some(epoch_validators) = self.epoch_validators.get_mut(epoch)
                            else {
                                tracing::error!(
                                    "don't have epoch validators populated for epoch: {:?}",
                                    epoch
                                );
                                continue;
                            };

                            if epoch_validators.validators.contains_key(&self_id) {
                                let message: M = message.into();
                                self.pending_events.push_back(message.event(self_id));
                                if let Some(waker) = self.waker.take() {
                                    waker.wake()
                                }
                            }
                            let epoch_validators_without_self =
                                epoch_validators.view_without(vec![&self_id]);
                            if epoch_validators_without_self.view.is_empty() {
                                // this is degenerate case where the only validator is self
                                continue;
                            }

                            (
                                epoch,
                                round,
                                BuildTarget::Raptorcast(epoch_validators_without_self),
                            )
                        }
                        RouterTarget::PointToPoint(to) => {
                            if to == &self_id {
                                let message: M = message.into();
                                self.pending_events.push_back(message.event(self_id));
                                if let Some(waker) = self.waker.take() {
                                    waker.wake()
                                }
                                continue;
                            } else {
                                (
                                    &self.current_epoch,
                                    &self.current_round,
                                    BuildTarget::PointToPoint(to),
                                )
                            }
                        }
                    };

                    let messages = build_messages::<ST>(
                        &self.key,
                        app_message,
                        self.redundancy,
                        epoch.0,
                        round.0,
                        build_target,
                        &self.known_addresses,
                    );

                    self.dataplane.unicast(UnicastMsg { msgs: messages });
                }
            }
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        self.metrics.as_ref().into()
    }
}

impl<ST, M, OM> Stream for RaptorCast<ST, M, OM>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Deserializable<Bytes>,
    OM: Serializable<Bytes> + Into<M> + Clone,

    Self: Unpin,
{
    type Item = M::Event;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        if this.waker.is_none() {
            this.waker = Some(cx.waker().clone());
        }

        if let Some(event) = this.pending_events.pop_front() {
            return Poll::Ready(Some(event));
        }

        let self_id = NodeId::new(this.key.pubkey());

        while let Poll::Ready(Some(message)) = this.dataplane.poll_next_unpin(cx) {
            let mut broadcast_batcher = BroadcastBatcher {
                dataplane: &mut this.dataplane,
                message: &message.payload,
                batch: None,
            };

            for payload_start_idx in (0..message.payload.len()).step_by(message.stride) {
                let mut batch_guard = BatcherGuard {
                    batcher: &mut broadcast_batcher,
                    flush_batch: true,
                };

                let payload_end_idx = payload_start_idx + message.stride.min(message.payload.len());
                let payload = message.payload.slice(payload_start_idx..payload_end_idx);
                let parsed_message = match parse_message::<ST>(&mut this.signature_cache, payload) {
                    Ok(message) => message,
                    Err(err) => {
                        tracing::warn!("unable to parse message, err={:?}", err);
                        continue;
                    }
                };

                let self_hash = compute_hash(&self_id);
                if parsed_message.broadcast {
                    let Some(epoch_validators) =
                        this.epoch_validators.get_mut(&Epoch(parsed_message.epoch))
                    else {
                        tracing::error!(
                            "don't have epoch validators populated for round: {:?}",
                            parsed_message.round
                        );
                        continue;
                    };
                    if !epoch_validators
                        .validators
                        .contains_key(&parsed_message.author)
                    {
                        tracing::error!("not in validator set: {:?}", parsed_message.author);
                        continue;
                    }
                    if self_hash == parsed_message.recipient_hash {
                        let targets =
                            epoch_validators.view_without(vec![&parsed_message.author, &self_id]);
                        batch_guard.queue_broadcast(
                            payload_start_idx,
                            payload_end_idx,
                            &parsed_message.author,
                            || {
                                targets
                                    .view
                                    .keys()
                                    .filter_map(|validator| {
                                        this.known_addresses.get(validator).copied()
                                    })
                                    .collect()
                            },
                        )
                    }
                } else if self_hash != parsed_message.recipient_hash {
                    tracing::error!("dropping spoofed message");
                    continue;
                }

                let app_message_len: usize = parsed_message.app_message_len.try_into().unwrap();
                let entry = this
                    .message_cache
                    .entry(MessageCacheKey {
                        round: Round(parsed_message.round),
                        author: parsed_message.author,
                        app_message_hash: parsed_message.app_message_hash,
                        app_message_len,
                    })
                    .or_insert_with(|| {
                        // data_size is always greater than zero, so this division is safe
                        let num_source_symbols =
                            app_message_len.div_ceil(parsed_message.chunk.len());
                        SourceBlockDecoder::new(num_source_symbols)
                    });

                if entry.fully_specified() {
                    // already decoded
                    continue;
                }

                entry.push_encoding_symbol(parsed_message.chunk, parsed_message.chunk_id.into());
                if let Some(decoded) = entry.decode(app_message_len) {
                    let Ok(message) = M::deserialize(&decoded) else {
                        tracing::error!("failed to deserialize message");
                        continue;
                    };
                    this.pending_events
                        .push_back(message.event(parsed_message.author));
                }
            }
            if let Some(event) = this.pending_events.pop_front() {
                return Poll::Ready(Some(event));
            }
        }

        Poll::Pending
    }
}

#[derive(Debug)]
pub enum BuildTarget<'a, ST: CertificateSignatureRecoverable> {
    Broadcast(
        // validator stakes for given round_no, not including self
        // this MUST NOT BE EMPTY
        ValidatorsView<'a, ST>,
    ),
    Raptorcast(
        // validator stakes for given round_no, not including self
        // this MUST NOT BE EMPTY
        ValidatorsView<'a, ST>,
    ), // sharded raptor-aware broadcast
    PointToPoint(&'a NodeId<CertificateSignaturePubKey<ST>>),
}

/// Stuff to include:
/// - 65 bytes => Signature of sender over hash(rest of message up to merkle proof, concatenated
///               with merkle root)
/// - 2 bytes => Version: bumped on protocol updates
/// - 8 bytes (u64) => Epoch #
/// - 8 bytes (u64) => Round #
/// - 20 bytes => first 20 bytes of hash of AppMessage
///   - this isn't technically necessary if payload_len is small enough to fit in 1 chunk, but keep
///     for simplicity
/// - 4 bytes (u32) => Serialized AppMessage length (bytes)
/// - 1 bit => broadcast or not
/// - 7 bits => Merkle tree depth
/// - 20 bytes * (merkle_tree_depth - 1) => merkle proof (leaves include everything that follows,
///   eg hash(chunk_recipient + chunk_byte_offset + chunk_len + payload))
///
/// - 1 byte => Chunk's merkle leaf idx
/// - 20 bytes => first 20 bytes of hash of chunk's first hop recipient
///   - we set this even if broadcast bit is not set so that it's known if a message was intended
///     to be sent to self
/// - 2 bytes (u16) => This chunk's id
/// - 2 bytes => Merkle chunk payload len
/// - (merkle_chunk_payload_len bytes) => data
///
//
//
//
// pub struct M {
//     signature: [u8; 65],
//     version: u16,
//     epoch: u64,
//     round: u64,
//     app_message_id: [u8; 20],
//     app_message_len: u32,
//     broadcast: bool,
//
//     merkle_tree_depth: u8,
//     merkle_proof: Vec<[u8; 20]>,
//
//     chunk_merkle_leaf_idx: u8,
//     chunk_recipient: [u8; 20],
//     chunk_id: u16,
//     chunk_len: u16,
//
//     data: Bytes,
// }
const HEADER_LEN: u16 = 65  // Sender signature
            + 2  // Version
            + 8  // Epoch #
            + 8  // Round #
            + 20 // AppMessage hash
            + 4 // AppMessage length
            + 1; // Broadcast bit, 7 bits for Merkle Tree Depth
const CHUNK_HEADER_LEN: u16 = 1 // Chunk's merkle leaf idx
            + 20 // Chunk recipient hash
            + 2 // Chunk idx
            + 2; // Chunk data length

fn compute_hash<PT>(id: &NodeId<PT>) -> [u8; 20]
where
    PT: PubKey,
{
    let mut hasher = HasherType::new();
    hasher.update(id.pubkey().bytes());
    hasher.hash().0[..20].try_into().expect("20 bytes")
}

// We compute these as consts so that the desired Raptor symbol length is also a const, which
// then allows passing it into the Raptor encoder as a const generic.
const _: () = assert!(monad_dataplane::network::MONAD_GSO_SIZE <= (u16::MAX as usize));
const GSO_SIZE: u16 = monad_dataplane::network::MONAD_GSO_SIZE as u16;

const BODY_SIZE: u16 = GSO_SIZE - HEADER_LEN - CHUNK_HEADER_LEN;

// TODO make this more sophisticated
const TREE_DEPTH: u8 = 6;
const _: () = assert!(
    TREE_DEPTH & (1 << 7) == 0,
    "tree depth doesn't fit in 7 bits"
);

const PROOF_SIZE: u16 = 20 * ((TREE_DEPTH as u16) - 1);

const DATA_SIZE: u16 = BODY_SIZE - PROOF_SIZE;

pub fn build_messages<ST>(
    key: &ST::KeyPairType,
    app_message: Bytes,
    redundancy: u8, // 2 == send 1 extra packet for every 1 original
    epoch_no: u64,
    round_no: u64,
    build_target: BuildTarget<ST>,

    known_addresses: &HashMap<NodeId<CertificateSignaturePubKey<ST>>, SocketAddr>,
) -> Vec<(SocketAddr, Bytes)>
where
    ST: CertificateSignatureRecoverable,
{
    let app_message_len: u32 = app_message.len().try_into().expect("message too big");

    let is_broadcast = matches!(
        build_target,
        BuildTarget::Broadcast(_) | BuildTarget::Raptorcast(_)
    );

    let chunks_per_merkle_batch: u8 = 2_u8
        .checked_pow(u32::from(TREE_DEPTH) - 1)
        .expect("tree depth too big");

    let is_raptor_broadcast = matches!(build_target, BuildTarget::Raptorcast(_));

    let num_packets: u16 = {
        let mut num_packets: u16 = (app_message_len.div_ceil(DATA_SIZE.into())
            * u32::from(redundancy))
        .try_into()
        .expect("is redundancy too high? doesn't fit in u16");

        if let BuildTarget::Broadcast(epoch_validators) = &build_target {
            num_packets = num_packets
                .checked_mul(epoch_validators.view.len() as u16)
                .expect("num_packets doesn't fit in u16")
        }

        num_packets
    };

    let mut message = BytesMut::zeroed(GSO_SIZE as usize * num_packets as usize);

    let mut chunk_datas = message
        .chunks_mut(GSO_SIZE.into())
        .map(|chunk| &mut chunk[(HEADER_LEN + PROOF_SIZE).into()..])
        .collect_vec();
    assert_eq!(chunk_datas.len(), num_packets as usize);

    // the GSO-aware indices into `message`
    let mut outbound_gso_idx: Vec<(SocketAddr, Range<usize>)> = Vec::new();
    // popuate chunk_recipient and outbound_gso_idx
    match build_target {
        BuildTarget::PointToPoint(to) => {
            let Some(addr) = known_addresses.get(to) else {
                tracing::warn!("not sending to {:?}, address unknown", to);
                return Vec::new();
            };
            outbound_gso_idx.push((*addr, 0..GSO_SIZE as usize * num_packets as usize));
            for chunk_data in &mut chunk_datas {
                // populate chunk_recipient
                chunk_data[1..1 + 20].copy_from_slice(&compute_hash(to));
            }
        }
        BuildTarget::Broadcast(epoch_validators) => {
            assert!(is_broadcast && !is_raptor_broadcast);
            let total_validators = epoch_validators.view.len();
            let mut running_validator_count = 0;
            for (node_id, validator) in epoch_validators.view.iter() {
                let start_idx: usize =
                    num_packets as usize * running_validator_count / total_validators;
                running_validator_count += 1;
                let end_idx: usize =
                    num_packets as usize * running_validator_count / total_validators;

                if start_idx == end_idx {
                    continue;
                }
                if let Some(addr) = known_addresses.get(node_id) {
                    outbound_gso_idx.push((
                        *addr,
                        start_idx * GSO_SIZE as usize..end_idx * GSO_SIZE as usize,
                    ));
                } else {
                    tracing::warn!("not sending to {:?}, address unknown", node_id)
                }
                for chunk_data in &mut chunk_datas[start_idx..end_idx] {
                    // populate chunk_recipient
                    chunk_data[1..1 + 20].copy_from_slice(&compute_hash(node_id));
                }
            }
        }
        BuildTarget::Raptorcast(epoch_validators) => {
            assert!(is_broadcast && is_raptor_broadcast);
            // FIXME should self be included in total_stake?
            let total_stake: i64 = epoch_validators
                .view
                .values()
                .map(|validator| validator.stake.0)
                .sum();
            let mut running_stake = 0;
            for (node_id, validator) in epoch_validators.view.iter() {
                let start_idx: usize = (num_packets as i64 * running_stake / total_stake) as usize;
                running_stake += validator.stake.0;
                let end_idx: usize = (num_packets as i64 * running_stake / total_stake) as usize;

                if start_idx == end_idx {
                    continue;
                }
                if let Some(addr) = known_addresses.get(node_id) {
                    outbound_gso_idx.push((
                        *addr,
                        start_idx * GSO_SIZE as usize..end_idx * GSO_SIZE as usize,
                    ));
                } else {
                    tracing::warn!("not sending to {:?}, address unknown", node_id)
                }
                for chunk_data in &mut chunk_datas[start_idx..end_idx] {
                    // populate chunk_recipient
                    chunk_data[1..1 + 20].copy_from_slice(&compute_hash(node_id));
                }
            }
        }
    };

    // populates the following chunk-specific stuff
    // - chunk_id: u16
    // - chunk_len: u16
    // - chunk_payload
    let encoder = monad_raptor::Encoder::<{ DATA_SIZE as usize }>::new(&app_message).unwrap();
    for (chunk_id, mut chunk_data) in chunk_datas.iter_mut().enumerate() {
        let chunk_id = chunk_id as u16;
        let chunk_len: u16 = DATA_SIZE;

        let cursor = &mut chunk_data;
        let (cursor_chunk_merkle_leaf_idx, cursor) = cursor.split_at_mut(1);
        let (cursor_chunk_recipient, cursor) = cursor.split_at_mut(20);
        let (cursor_chunk_id, cursor) = cursor.split_at_mut(2);
        cursor_chunk_id.copy_from_slice(&chunk_id.to_le_bytes());
        let (cursor_chunk_payload_len, cursor) = cursor.split_at_mut(2);
        cursor_chunk_payload_len.copy_from_slice(&chunk_len.to_le_bytes());
        let (cursor_chunk_payload, cursor) = cursor.split_at_mut(chunk_len.into());
        encoder.encode_symbol(
            (&mut cursor_chunk_payload[..chunk_len.into()])
                .try_into()
                .unwrap(),
            chunk_id.into(),
        );
    }

    // At this point, everything BELOW chunk_merkle_leaf_idx is populated
    // populate merkle trees/roots/leaf_idx + signatures (cached)
    let version: u16 = 0;
    let epoch_no: u64 = epoch_no;
    let round_no: u64 = round_no;
    let app_message_hash: [u8; 20] = {
        let mut hasher = HasherType::new();
        hasher.update(app_message);
        hasher.hash().0[..20].try_into().unwrap()
    };
    message
        // .par_chunks_mut(GSO_SIZE as usize * chunks_per_merkle_batch as usize)
        .chunks_mut(GSO_SIZE as usize * chunks_per_merkle_batch as usize)
        .for_each(|merkle_batch| {
            let mut merkle_batch = merkle_batch.chunks_mut(GSO_SIZE as usize).collect_vec();
            let merkle_leaves = merkle_batch
                .iter_mut()
                .enumerate()
                .map(|(chunk_idx, chunk)| {
                    let chunk_payload = &mut chunk[(HEADER_LEN + PROOF_SIZE).into()..];
                    assert_eq!(
                        chunk_payload.len(),
                        CHUNK_HEADER_LEN as usize + DATA_SIZE as usize
                    );
                    // populate merkle_leaf_idx
                    chunk_payload[0] = chunk_idx.try_into().expect("chunk idx doesn't fit in u8");

                    let mut hasher = HasherType::new();
                    hasher.update(chunk_payload);
                    hasher.hash()
                })
                .collect_vec();
            let merkle_tree = MerkleTree::new_with_depth(&merkle_leaves, TREE_DEPTH);
            let mut header_with_root = {
                let mut data = [0_u8; HEADER_LEN as usize + 20];
                let cursor = &mut data;
                let (cursor_signature, cursor) = cursor.split_at_mut(65);
                let (cursor_version, cursor) = cursor.split_at_mut(2);
                cursor_version.copy_from_slice(&version.to_le_bytes());
                let (cursor_epoch_no, cursor) = cursor.split_at_mut(8);
                cursor_epoch_no.copy_from_slice(&epoch_no.to_le_bytes());
                let (cursor_round_no, cursor) = cursor.split_at_mut(8);
                cursor_round_no.copy_from_slice(&round_no.to_le_bytes());
                let (cursor_app_message_hash, cursor) = cursor.split_at_mut(20);
                cursor_app_message_hash.copy_from_slice(&app_message_hash);
                let (cursor_app_message_len, cursor) = cursor.split_at_mut(4);
                cursor_app_message_len.copy_from_slice(&app_message_len.to_le_bytes());
                let (cursor_broadcast_merkle_depth, cursor) = cursor.split_at_mut(1);
                cursor_broadcast_merkle_depth[0] = ((is_raptor_broadcast as u8) << 7) | TREE_DEPTH;

                cursor.copy_from_slice(merkle_tree.root());
                // 65  // Sender signature
                // 2  // Version
                // 8  // Epoch #
                // 8  // Round #
                // 20 // AppMessage hash
                // 4 // AppMessage length
                // 1 // Broadcast bit, 7 bits for Merkle Tree Depth
                // --
                // 20 // Merkle root

                data
            };
            let signature = ST::sign(&header_with_root[65..], key).serialize();
            assert_eq!(signature.len(), 65);
            header_with_root[..65].copy_from_slice(&signature);
            let header = &header_with_root[..HEADER_LEN as usize];
            for (leaf_idx, chunk) in merkle_batch.into_iter().enumerate() {
                chunk[..HEADER_LEN as usize].copy_from_slice(header);
                for (proof_idx, proof) in merkle_tree
                    .proof(leaf_idx as u8)
                    .siblings()
                    .iter()
                    .enumerate()
                {
                    let offset = HEADER_LEN as usize + 20 * proof_idx;
                    chunk[offset..offset + 20].copy_from_slice(proof);
                }
            }
        });

    let message = message.freeze();

    outbound_gso_idx
        .into_iter()
        .map(|(addr, range)| (addr, message.slice(range)))
        .collect()
}

pub struct ValidatedMessage<PT>
where
    PT: PubKey,
{
    pub message: Bytes,

    pub author: NodeId<PT>,
    pub epoch: u64,
    pub round: u64,
    pub app_message_hash: [u8; 20],
    pub app_message_len: u32,
    pub broadcast: bool,
    pub recipient_hash: [u8; 20],
    pub chunk_id: u16,
    pub chunk: Bytes, // raptor-coded portion
}

#[derive(Debug)]
pub enum MessageValidationError {
    UnknownVersion,
    TooShort,
    InvalidSignature,
    InvalidTreeDepth,
    InvalidMerkleProof,
}

/// - 65 bytes => Signature of sender over hash(rest of message up to merkle proof, concatenated
///               with merkle root)
/// - 2 bytes => Version: bumped on protocol updates
/// - 8 bytes (u64) => Epoch #
/// - 8 bytes (u64) => Round #
/// - 20 bytes => first 20 bytes of hash of AppMessage
///   - this isn't technically necessary if payload_len is small enough to fit in 1 chunk, but keep
///     for simplicity
/// - 4 bytes (u32) => Serialized AppMessage length (bytes)
/// - 1 bit => broadcast or not
/// - 7 bits => Merkle tree depth
/// - 20 bytes * (merkle_tree_depth - 1) => merkle proof (leaves include everything that follows,
///   eg hash(chunk_recipient + chunk_byte_offset + chunk_len + payload))
///
/// - 1 byte => Chunk's merkle leaf idx
/// - 20 bytes => first 20 bytes of hash of chunk's first hop recipient
///   - we set this even if broadcast bit is not set so that it's known if a message was intended
///     to be sent to self
/// - 2 bytes (u16) => This chunk's id
/// - 2 bytes => Merkle chunk payload len
/// - (merkle_chunk_payload_len bytes) => data
pub fn parse_message<ST>(
    signature_cache: &mut HashMap<
        [u8; HEADER_LEN as usize - 65 + 20],
        NodeId<CertificateSignaturePubKey<ST>>,
    >,
    message: Bytes,
) -> Result<ValidatedMessage<CertificateSignaturePubKey<ST>>, MessageValidationError>
where
    ST: CertificateSignatureRecoverable,
{
    let mut cursor: Bytes = message.clone();
    let mut split_off = |mid| {
        if mid > cursor.len() {
            Err(MessageValidationError::TooShort)
        } else {
            Ok(cursor.split_to(mid))
        }
    };
    let cursor_signature = split_off(65)?;
    let signature =
        ST::deserialize(&cursor_signature).map_err(|_| MessageValidationError::InvalidSignature)?;

    let cursor_version = split_off(2)?;
    let version = u16::from_le_bytes(cursor_version.as_ref().try_into().expect("u16 is 2 bytes"));
    if version != 0 {
        return Err(MessageValidationError::UnknownVersion);
    }

    let cursor_epoch = split_off(8)?;
    let epoch = u64::from_le_bytes(cursor_epoch.as_ref().try_into().expect("u64 is 8 bytes"));

    let cursor_round = split_off(8)?;
    let round = u64::from_le_bytes(cursor_round.as_ref().try_into().expect("u64 is 8 bytes"));

    let cursor_app_message_hash = split_off(20)?;
    let app_message_hash: [u8; 20] = cursor_app_message_hash
        .as_ref()
        .try_into()
        .expect("Hash is 20 bytes");

    let cursor_app_message_len = split_off(4)?;
    let app_message_len = u32::from_le_bytes(
        cursor_app_message_len
            .as_ref()
            .try_into()
            .expect("u32 is 4 bytes"),
    );

    let cursor_broadcast_tree_depth = split_off(1)?[0];
    let broadcast = (cursor_broadcast_tree_depth >> 7) != 0;
    let tree_depth = cursor_broadcast_tree_depth & !(1 << 7);

    if tree_depth < 1 {
        return Err(MessageValidationError::InvalidTreeDepth);
    }
    let proof_size: u16 = 20 * (u16::from(tree_depth) - 1);

    let mut merkle_proof = Vec::new();
    for _ in 0..tree_depth - 1 {
        let cursor_sibling = split_off(20)?;
        let sibling =
            MerkleHash::try_from(cursor_sibling.as_ref()).expect("MerkleHash is 20 bytes");
        merkle_proof.push(sibling);
    }
    let cursor_merkle_idx = split_off(1)?[0];
    let merkle_proof = MerkleProof::new_from_leaf_idx(merkle_proof, cursor_merkle_idx)
        .ok_or(MessageValidationError::InvalidMerkleProof)?;

    let cursor_recipient = split_off(20)?;
    let recipient_hash: [u8; 20] = cursor_recipient
        .as_ref()
        .try_into()
        .expect("Hash is 20 bytes");

    let cursor_chunk_id = split_off(2)?;
    let chunk_id = u16::from_le_bytes(cursor_chunk_id.as_ref().try_into().expect("u16 is 2 bytes"));

    let cursor_payload_len = split_off(2)?;
    let payload_len = u16::from_le_bytes(
        cursor_payload_len
            .as_ref()
            .try_into()
            .expect("u16 is 2 bytes"),
    );
    if payload_len == 0 {
        // handle the degenerate case
        return Err(MessageValidationError::TooShort);
    }

    let cursor_payload = split_off(payload_len as usize)?;

    let leaf_hash = {
        let mut hasher = HasherType::new();
        hasher.update(
            &message[HEADER_LEN as usize + proof_size as usize..
                // HEADER_LEN as usize
                //     + proof_size as usize
                //     + CHUNK_HEADER_LEN as usize
                //     + payload_len as usize
                ],
        );
        hasher.hash()
    };
    let root = merkle_proof
        .compute_root(&leaf_hash)
        .ok_or(MessageValidationError::InvalidMerkleProof)?;
    let mut signed_over = [0_u8; HEADER_LEN as usize - 65 + 20];
    // TODO can avoid this copy if necessary
    signed_over[..HEADER_LEN as usize - 65].copy_from_slice(&message[65..HEADER_LEN as usize]);
    signed_over[HEADER_LEN as usize - 65..].copy_from_slice(&root);

    let author = match signature_cache.entry(signed_over) {
        Entry::Occupied(entry) => *entry.get(),
        Entry::Vacant(entry) => {
            let author = signature
                .recover_pubkey(&signed_over)
                .map_err(|_| MessageValidationError::InvalidSignature)?;
            *entry.insert(NodeId::new(author))
        }
    };

    if signature_cache.len() > 10_000 {
        // FIXME this is a super jank way of bounding size of signature_cache
        // should switch this to LRU eviction
        signature_cache.clear();
    }

    Ok(ValidatedMessage {
        message,

        author,
        epoch,
        round,
        app_message_hash,
        app_message_len,
        broadcast,
        recipient_hash,
        chunk_id,
        chunk: cursor_payload,
    })
}

struct BroadcastBatch<PT: PubKey> {
    author: NodeId<PT>,
    targets: Vec<SocketAddr>,

    start_idx: usize,
    end_idx: usize,
}
struct BroadcastBatcher<'a, PT: PubKey> {
    dataplane: &'a mut Dataplane,
    message: &'a Bytes,

    batch: Option<BroadcastBatch<PT>>,
}
impl<'a, PT: PubKey> Drop for BroadcastBatcher<'a, PT> {
    fn drop(&mut self) {
        self.flush()
    }
}
impl<'a, PT: PubKey> BroadcastBatcher<'a, PT> {
    fn flush(&mut self) {
        if let Some(batch) = self.batch.take() {
            self.dataplane.broadcast(BroadcastMsg {
                targets: batch.targets,
                payload: self.message.slice(batch.start_idx..batch.end_idx),
            })
        }
    }
}
struct BatcherGuard<'a: 'g, 'g, PT: PubKey> {
    batcher: &'g mut BroadcastBatcher<'a, PT>,
    flush_batch: bool,
}
impl<'a: 'g, 'g, PT: PubKey> BatcherGuard<'a, 'g, PT> {
    fn queue_broadcast(
        &mut self,
        payload_start_idx: usize,
        payload_end_idx: usize,
        author: &NodeId<PT>,
        targets: impl FnOnce() -> Vec<SocketAddr>,
    ) {
        self.flush_batch = false;
        if self
            .batcher
            .batch
            .as_ref()
            .is_some_and(|batch| &batch.author == author)
        {
            let batch = self.batcher.batch.as_mut().unwrap();
            assert_eq!(batch.end_idx, payload_start_idx);
            batch.end_idx = payload_end_idx;
        } else {
            self.batcher.flush();
            self.batcher.batch = Some(BroadcastBatch {
                author: *author,
                targets: targets(),

                start_idx: payload_start_idx,
                end_idx: payload_end_idx,
            })
        }
    }
}
impl<'a: 'g, 'g, PT: PubKey> Drop for BatcherGuard<'a, 'g, PT> {
    fn drop(&mut self) {
        if self.flush_batch {
            self.batcher.flush();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use bytes::{Bytes, BytesMut};
    use itertools::Itertools;
    use monad_crypto::hasher::{Hasher, HasherType};
    use monad_secp::SecpSignature;
    use monad_types::{NodeId, Stake};

    use crate::{build_messages, parse_message, BuildTarget, EpochValidators, Validator, GSO_SIZE};

    #[test]
    fn test_roundtrip() {
        let keys = (0_u8..100_u8)
            .map(|n| {
                let mut hasher = HasherType::new();
                hasher.update(n.to_le_bytes());
                let mut hash = hasher.hash();
                monad_secp::KeyPair::from_bytes(&mut hash.0).unwrap()
            })
            .collect_vec();

        let mut validators = EpochValidators {
            validators: keys
                .iter()
                .map(|key| (NodeId::new(key.pubkey()), Validator { stake: Stake(1) }))
                .collect(),
        };
        let epoch_validators = validators.view_without(vec![&NodeId::new(keys[0].pubkey())]);

        let known_addresses = keys
            .iter()
            .map(|key| {
                (
                    NodeId::new(key.pubkey()),
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
                )
            })
            .collect();

        let app_message: Bytes = vec![1_u8; 1024 * 1024].into();
        let app_message_hash = {
            let mut hasher = HasherType::new();
            hasher.update(&app_message);
            hasher.hash()
        };

        const EPOCH: u64 = 5;
        const ROUND: u64 = 5;
        let messages = build_messages::<SecpSignature>(
            &keys[0],
            app_message.clone(),
            2,     // redundancy,
            EPOCH, // epoch_no
            ROUND, // round_no
            BuildTarget::Raptorcast(epoch_validators),
            &known_addresses,
        );

        let mut signature_cache = Default::default();

        for (to, mut aggregate_message) in messages {
            while !aggregate_message.is_empty() {
                let message = aggregate_message.split_to(GSO_SIZE.into());
                let parsed_message =
                    parse_message::<SecpSignature>(&mut signature_cache, message.clone())
                        .expect("valid message");
                assert_eq!(parsed_message.message, message);
                assert_eq!(parsed_message.app_message_hash, app_message_hash.0[..20]);
                assert_eq!(parsed_message.round, ROUND);
                assert!(parsed_message.broadcast);
                assert_eq!(parsed_message.app_message_len, app_message.len() as u32);
                assert_eq!(parsed_message.author, NodeId::new(keys[0].pubkey()));
            }
        }
    }

    #[test]
    fn test_bit_flip_parse_failure() {
        let keys = (0_u8..100_u8)
            .map(|n| {
                let mut hasher = HasherType::new();
                hasher.update(n.to_le_bytes());
                let mut hash = hasher.hash();
                monad_secp::KeyPair::from_bytes(&mut hash.0).unwrap()
            })
            .collect_vec();

        let mut validators = EpochValidators {
            validators: keys
                .iter()
                .map(|key| (NodeId::new(key.pubkey()), Validator { stake: Stake(1) }))
                .collect(),
        };
        let epoch_validators = validators.view_without(vec![&NodeId::new(keys[0].pubkey())]);

        let known_addresses = keys
            .iter()
            .map(|key| {
                (
                    NodeId::new(key.pubkey()),
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
                )
            })
            .collect();

        let app_message: Bytes = vec![1_u8; 1024 * 2].into();

        const EPOCH: u64 = 5;
        const ROUND: u64 = 5;
        let messages = build_messages::<SecpSignature>(
            &keys[0],
            app_message,
            2,     // redundancy,
            EPOCH, // epoch_no
            ROUND, // round_no
            BuildTarget::Raptorcast(epoch_validators),
            &known_addresses,
        );

        let mut signature_cache = Default::default();

        for (to, mut aggregate_message) in messages {
            while !aggregate_message.is_empty() {
                let mut message: BytesMut =
                    aggregate_message.split_to(GSO_SIZE.into()).as_ref().into();
                // try flipping each bit
                for bit_idx in 0..message.len() * 8 {
                    let old_byte = message[bit_idx / 8];
                    // flip bit
                    message[bit_idx / 8] = old_byte ^ (1 << (bit_idx % 8));
                    let maybe_parsed = parse_message::<SecpSignature>(
                        &mut signature_cache,
                        message.clone().into(),
                    );

                    // check that decoding fails
                    assert!(
                        maybe_parsed.is_err()
                            || maybe_parsed.unwrap().author != NodeId::new(keys[0].pubkey())
                    );

                    // reset bit
                    message[bit_idx / 8] = old_byte;
                }
            }
        }
    }
}
