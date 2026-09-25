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

use std::{fmt, str::FromStr, sync::Arc, time::SystemTime};

use actix_web::{
    HttpRequest, HttpResponse, ResponseError,
    http::{
        StatusCode,
        header::{self, CacheControl, CacheDirective, ContentType},
    },
    web,
};
use monad_mcp_chorus::ledger::{
    Address, BlockMeta, Hash, LedgerError, LedgerReader, PROOF_FILE, Tx, decode_batch,
};
use serde::{Deserialize, Serialize};

use crate::{
    assets,
    index::{BlockSummary, Index, Page, PageOf, TxEntry, TxKey},
    loader::{Progress, ProgressSnapshot, SharedIndex},
    stats::{self, WINDOWS, WindowStats, round3},
};

pub const DEFAULT_LIMIT: usize = 20;
pub const MAX_LIMIT: usize = 100;
// txs embedded per lane card in the block response.
pub const LANE_PREVIEW_TXS: usize = 5;
pub const SPARKLINE_BLOCKS: usize = 100;
pub const LIVE_INTERVAL_MS: u64 = 1000;

pub struct AppState {
    pub index: SharedIndex,
    pub reader: LedgerReader,
    pub progress: Arc<Progress>,
    pub rpc_url: String,
}

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
        }
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl ResponseError for ApiError {
    fn status_code(&self) -> StatusCode {
        self.status
    }

    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status)
            .insert_header(CacheControl(vec![CacheDirective::NoStore]))
            .json(serde_json::json!({ "error": self.message }))
    }
}

type ApiResult = Result<HttpResponse, ApiError>;

fn json(value: impl Serialize) -> ApiResult {
    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoStore]))
        .json(value))
}

async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> T + Send + 'static,
) -> Result<T, ApiError> {
    web::block(f)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))
}

fn hex0x(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

pub fn parse_hex<const N: usize>(s: &str) -> Option<[u8; N]> {
    let s = s.trim();
    let s = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s);
    let mut out = [0; N];
    hex::decode_to_slice(s, &mut out).ok()?;
    Some(out)
}

fn hash_param(s: &str) -> Result<Hash, ApiError> {
    parse_hex(s).ok_or_else(|| ApiError::bad_request("expected a 32-byte hex hash"))
}

fn unix_ms(ns: u64) -> u64 {
    ns / 1_000_000
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

// text shown for payloads that are printable utf8; a truncated tail char is dropped.
fn utf8_text(bytes: &[u8], truncated: bool) -> Option<String> {
    let text = match std::str::from_utf8(bytes) {
        Ok(text) => text,
        Err(e) if truncated && e.error_len().is_none() => {
            std::str::from_utf8(&bytes[..e.valid_up_to()]).ok()?
        }
        Err(_) => return None,
    };
    text.chars()
        .all(|c| !c.is_control() || c == '\n' || c == '\t')
        .then(|| text.to_owned())
}

#[derive(Debug, Default, Deserialize)]
pub struct ListQuery {
    before: Option<String>,
    after: Option<String>,
    cursor: Option<String>,
    limit: Option<String>,
}

impl ListQuery {
    fn limit(&self) -> Result<usize, ApiError> {
        match &self.limit {
            None => Ok(DEFAULT_LIMIT),
            Some(l) => l
                .parse::<usize>()
                .map(|l| l.clamp(1, MAX_LIMIT))
                .map_err(|_| ApiError::bad_request("limit must be a non-negative integer")),
        }
    }

    fn page<K: FromStr>(&self) -> Result<Page<K>, ApiError>
    where
        K::Err: fmt::Display,
    {
        let parse = |s: &str| {
            s.parse::<K>()
                .map_err(|e| ApiError::bad_request(format!("bad cursor {s:?}: {e}")))
        };
        match (&self.before, &self.after) {
            (Some(_), Some(_)) => Err(ApiError::bad_request("pass before or after, not both")),
            (Some(b), None) => Ok(Page::Before(parse(b)?)),
            (None, Some(a)) => Ok(Page::After(parse(a)?)),
            (None, None) => Ok(Page::Latest),
        }
    }

    // `cursor` pages backwards: items older than it.
    fn cursor<K: FromStr>(&self) -> Result<Page<K>, ApiError>
    where
        K::Err: fmt::Display,
    {
        match &self.cursor {
            None => Ok(Page::Latest),
            Some(c) => c
                .parse()
                .map(Page::Before)
                .map_err(|e| ApiError::bad_request(format!("bad cursor {c:?}: {e}"))),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct BlockJson {
    pub slot: u64,
    pub finalized_at_ms: u64,
    pub deadline_ms: Option<u64>,
    pub latency_ms: Option<f64>,
    pub path: &'static str,
    pub num_lanes: u16,
    pub positive_lanes: u16,
    pub lanes_with_txs: u16,
    pub decode_errors: u16,
    pub tx_count: u32,
    pub payload_bytes: u32,
}

impl From<&BlockSummary> for BlockJson {
    fn from(b: &BlockSummary) -> Self {
        Self {
            slot: b.slot,
            finalized_at_ms: unix_ms(b.finalized_at_ns),
            deadline_ms: b.deadline_ns().map(unix_ms),
            latency_ms: b.latency_ns().map(|l| round3(l as f64 / 1e6)),
            path: b.path.as_str(),
            num_lanes: b.num_lanes,
            positive_lanes: b.positive_lanes,
            lanes_with_txs: b.lanes_with_txs,
            decode_errors: b.decode_errors,
            tx_count: b.tx_count,
            payload_bytes: b.payload_bytes,
        }
    }
}

// list form of a tx: no full payload, only a PREVIEW_LEN-byte prefix.
#[derive(Debug, Serialize)]
pub struct TxJson {
    pub hash: String,
    pub sender: String,
    pub nonce: u64,
    pub slot: u64,
    pub lane: u32,
    pub pos: u32,
    pub size: u32,
    // exactly one preview: text when the prefix is printable utf8, else hex.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preview_text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preview: Option<String>,
    pub finalized_at_ms: Option<u64>,
    pub cursor: String,
}

fn tx_json(index: &Index, key: &TxKey, tx: &TxEntry) -> TxJson {
    let preview = tx.preview();
    let text = utf8_text(preview, preview.len() < tx.payload_len as usize);
    TxJson {
        hash: hex0x(&tx.hash),
        sender: hex0x(&tx.sender),
        nonce: tx.nonce,
        slot: key.slot,
        lane: key.lane,
        pos: key.pos,
        size: tx.payload_len.into(),
        preview: text.is_none().then(|| hex0x(preview)),
        preview_text: text,
        finalized_at_ms: index.block(key.slot).map(|b| unix_ms(b.finalized_at_ns)),
        cursor: key.to_string(),
    }
}

fn tx_jsons<'a>(
    index: &Index,
    items: impl IntoIterator<Item = (&'a TxKey, &'a TxEntry)>,
) -> Vec<TxJson> {
    items
        .into_iter()
        .map(|(k, t)| tx_json(index, k, t))
        .collect()
}

fn keyed_tx_jsons(index: &Index, keys: &[TxKey]) -> Vec<TxJson> {
    keys.iter()
        .filter_map(|k| index.tx(k).map(|t| tx_json(index, k, t)))
        .collect()
}

#[derive(Serialize)]
struct StatsJson {
    indexing: ProgressSnapshot,
    head: Option<u64>,
    oldest: Option<u64>,
    retained_blocks: usize,
    retain_slots: usize,
    retained_txs: usize,
    missing_slots: u64,
    totals: crate::index::Totals,
    windows: Vec<WindowStats>,
    // the last SPARKLINE_BLOCKS block times, oldest first.
    block_times_ms: Vec<f64>,
    now_ms: u64,
}

async fn stats(state: web::Data<AppState>) -> ApiResult {
    let index = state.index.read();
    let largest = WINDOWS.iter().copied().max().unwrap_or(0);
    let recent: Vec<&BlockSummary> = index.recent(largest).collect();
    let windows = WINDOWS
        .iter()
        .filter_map(|&size| stats::window(size, &recent[recent.len().saturating_sub(size)..]))
        .collect();
    let spark = &recent[recent.len().saturating_sub(SPARKLINE_BLOCKS + 1)..];
    let (head, oldest) = (
        index.newest().map(|b| b.slot),
        index.oldest().map(|b| b.slot),
    );
    json(StatsJson {
        indexing: state.progress.snapshot(),
        head,
        oldest,
        retained_blocks: index.len(),
        retain_slots: index.config().retain_slots,
        retained_txs: index.tx_len(),
        missing_slots: match (oldest, head) {
            (Some(o), Some(h)) => h - o + 1 - index.len() as u64,
            _ => 0,
        },
        totals: index.totals(),
        windows,
        block_times_ms: stats::block_times_ms(spark.iter().copied())
            .into_iter()
            .map(round3)
            .collect(),
        now_ms: now_ms(),
    })
}

#[derive(Serialize)]
struct BlocksJson {
    blocks: Vec<BlockJson>,
    has_more: bool,
    head: Option<u64>,
}

async fn blocks(state: web::Data<AppState>, query: web::Query<ListQuery>) -> ApiResult {
    let (page, limit) = (query.page::<u64>()?, query.limit()?);
    let index = state.index.read();
    let PageOf { items, has_more } = index.blocks_page(page, limit);
    json(BlocksJson {
        blocks: items.into_iter().map(BlockJson::from).collect(),
        has_more,
        head: index.newest().map(|b| b.slot),
    })
}

fn slot_param(s: &str) -> Result<u64, ApiError> {
    s.parse()
        .map_err(|_| ApiError::bad_request("slot must be a non-negative integer"))
}

fn not_indexed(slot: u64) -> ApiError {
    ApiError::not_found(format!("slot {slot} is not indexed"))
}

#[derive(Serialize)]
struct LaneJson {
    index: u32,
    positive: bool,
    proposer: Option<u64>,
    root: Option<String>,
    payload_len: u32,
    tx_count: u32,
    decode_error: bool,
    txs: Vec<TxJson>,
    more_txs: bool,
}

#[derive(Serialize)]
struct BlockDetailJson {
    #[serde(flatten)]
    block: BlockJson,
    // exact ns, as strings since they exceed 2^53.
    finalized_at_ns: String,
    deadline_ns: Option<String>,
    // false once the pruner has deleted the block dir; lanes are then summary-only.
    on_disk: bool,
    lanes: Vec<LaneJson>,
    proof_size: Option<u64>,
    prev: Option<u64>,
    next: Option<u64>,
    // slots between prev and this one with no block.
    gap_before: u64,
}

async fn block(state: web::Data<AppState>, path: web::Path<String>) -> ApiResult {
    let slot = slot_param(&path)?;
    if !state.index.read().contains(slot) {
        return Err(not_indexed(slot));
    }
    let reader = state.reader.clone();
    let (meta, proof_size) = blocking(move || {
        let meta = reader.read_meta(slot);
        let proof = std::fs::metadata(reader.block_dir(slot).join(PROOF_FILE));
        (meta, proof.ok().map(|m| m.len()))
    })
    .await?;
    let meta = match meta {
        Ok(meta) => Some(meta),
        Err(LedgerError::NotFound(_)) => None,
        Err(e) => return Err(ApiError::internal(e.to_string())),
    };
    let index = state.index.read();
    let summary = index.block(slot).ok_or_else(|| not_indexed(slot))?;
    let (prev, next) = index.neighbors(slot);
    let lanes = meta
        .as_ref()
        .map(|meta: &BlockMeta| {
            meta.lanes
                .iter()
                .map(|lane| {
                    let page = index.lane_txs(slot, lane.index, 0, LANE_PREVIEW_TXS);
                    LaneJson {
                        index: lane.index,
                        positive: lane.is_positive(),
                        proposer: lane.proposer,
                        root: lane.root.as_ref().map(|r| hex0x(r)),
                        payload_len: lane.payload_len,
                        tx_count: lane.tx_count,
                        decode_error: lane.decode_error,
                        txs: tx_jsons(&index, page.items),
                        more_txs: page.has_more,
                    }
                })
                .collect()
        })
        .unwrap_or_default();
    json(BlockDetailJson {
        block: summary.into(),
        finalized_at_ns: summary.finalized_at_ns.to_string(),
        deadline_ns: summary.deadline_ns().map(|d| d.to_string()),
        on_disk: meta.is_some(),
        lanes,
        proof_size,
        prev: prev.map(|b| b.slot),
        next: next.map(|b| b.slot),
        gap_before: prev.map_or(0, |p| slot - p.slot - 1),
    })
}

#[derive(Serialize)]
struct LaneTxsJson {
    slot: u64,
    index: u32,
    tx_count: usize,
    txs: Vec<TxJson>,
    next_cursor: Option<u32>,
}

async fn lane(
    state: web::Data<AppState>,
    path: web::Path<(String, String)>,
    query: web::Query<ListQuery>,
) -> ApiResult {
    let slot = slot_param(&path.0)?;
    let lane: u32 = path
        .1
        .parse()
        .map_err(|_| ApiError::bad_request("lane must be a non-negative integer"))?;
    let from = match &query.cursor {
        None => 0,
        Some(c) => c
            .parse()
            .map_err(|_| ApiError::bad_request("lane cursor must be a tx position"))?,
    };
    let limit = query.limit()?;
    let index = state.index.read();
    let block = index.block(slot).ok_or_else(|| not_indexed(slot))?;
    if lane >= u32::from(block.num_lanes) {
        return Err(ApiError::not_found(format!(
            "slot {slot} has no lane {lane}"
        )));
    }
    let page = index.lane_txs(slot, lane, from, limit);
    let next_cursor = page
        .has_more
        .then(|| page.items.last().map(|(k, _)| k.pos + 1))
        .flatten();
    json(LaneTxsJson {
        slot,
        index: lane,
        tx_count: index.lane_tx_count(slot, lane),
        txs: tx_jsons(&index, page.items),
        next_cursor,
    })
}

async fn proof(state: web::Data<AppState>, path: web::Path<String>) -> ApiResult {
    let slot = slot_param(&path)?;
    if !state.index.read().contains(slot) {
        return Err(not_indexed(slot));
    }
    let reader = state.reader.clone();
    match blocking(move || reader.read_proof(slot)).await? {
        Ok(proof) => json(serde_json::json!({
            "slot": slot,
            "size": proof.len(),
            "proof": hex0x(&proof),
        })),
        Err(LedgerError::NotFound(_)) => Err(ApiError::not_found(format!(
            "slot {slot} has been pruned from the ledger"
        ))),
        Err(e) => Err(ApiError::internal(e.to_string())),
    }
}

#[derive(Serialize)]
struct TxsJson {
    txs: Vec<TxJson>,
    has_more: bool,
}

async fn txs(state: web::Data<AppState>, query: web::Query<ListQuery>) -> ApiResult {
    let (page, limit) = (query.page::<TxKey>()?, query.limit()?);
    let index = state.index.read();
    let PageOf { items, has_more } = index.txs_page(page, limit);
    json(TxsJson {
        txs: tx_jsons(&index, items),
        has_more,
    })
}

#[derive(Serialize)]
struct InclusionJson {
    slot: u64,
    lane: u32,
    pos: u32,
}

#[derive(Serialize)]
struct TxDetailJson {
    #[serde(flatten)]
    summary: TxJson,
    payload_hash: String,
    payload: Option<String>,
    payload_utf8: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload_error: Option<String>,
    // every block the tx landed in, oldest first; a resent tx can land twice.
    inclusions: Vec<InclusionJson>,
}

fn read_tx(reader: &LedgerReader, key: TxKey, hash: Hash) -> Result<Tx, String> {
    let payload = reader
        .read_lane(key.slot, key.lane)
        .map_err(|e| e.to_string())?
        .ok_or("lane is negative")?;
    let tx = decode_batch(&payload)
        .map_err(|e| e.to_string())?
        .into_iter()
        .nth(key.pos as usize)
        .ok_or("tx position out of range")?;
    if tx.hash() != hash {
        return Err("lane file does not hold this tx".into());
    }
    Ok(tx)
}

async fn tx(state: web::Data<AppState>, path: web::Path<String>) -> ApiResult {
    let hash = hash_param(&path)?;
    let (key, summary, payload_hash, inclusions) = {
        let index = state.index.read();
        let keys: Vec<TxKey> = index.tx_inclusions(&hash).collect();
        let (key, entry) = keys
            .first()
            .and_then(|k| index.tx(k).map(|t| (*k, t)))
            .ok_or_else(|| ApiError::not_found(format!("tx {} is not indexed", hex0x(&hash))))?;
        let inclusions = keys
            .iter()
            .map(|k| InclusionJson {
                slot: k.slot,
                lane: k.lane,
                pos: k.pos,
            })
            .collect();
        (
            key,
            tx_json(&index, &key, entry),
            entry.payload_hash,
            inclusions,
        )
    };
    let reader = state.reader.clone();
    let read = blocking(move || read_tx(&reader, key, hash)).await?;
    let (payload, payload_utf8, payload_error) = match read {
        Ok(tx) => (
            Some(hex0x(&tx.payload)),
            utf8_text(&tx.payload, false),
            None,
        ),
        Err(e) => (None, None, Some(e)),
    };
    json(TxDetailJson {
        summary,
        payload_hash: hex0x(&payload_hash),
        payload,
        payload_utf8,
        payload_error,
        inclusions,
    })
}

#[derive(Serialize)]
struct KeyedTxsJson {
    // the payload hash or sender the txs share.
    id: String,
    tx_count: usize,
    txs: Vec<TxJson>,
    next_cursor: Option<String>,
}

fn keyed_txs(index: &Index, id: String, total: usize, page: PageOf<TxKey>) -> KeyedTxsJson {
    let next_cursor = page
        .has_more
        .then(|| page.items.last().map(TxKey::to_string))
        .flatten();
    KeyedTxsJson {
        id,
        tx_count: total,
        txs: keyed_tx_jsons(index, &page.items),
        next_cursor,
    }
}

async fn payload(
    state: web::Data<AppState>,
    path: web::Path<String>,
    query: web::Query<ListQuery>,
) -> ApiResult {
    let hash = hash_param(&path)?;
    let (page, limit) = (query.cursor::<TxKey>()?, query.limit()?);
    let index = state.index.read();
    let total = index.payload_tx_count(&hash);
    let txs = index.payload_txs(&hash, page, limit);
    json(keyed_txs(&index, hex0x(&hash), total, txs))
}

async fn sender(
    state: web::Data<AppState>,
    path: web::Path<String>,
    query: web::Query<ListQuery>,
) -> ApiResult {
    let sender: Address =
        parse_hex(&path).ok_or_else(|| ApiError::bad_request("expected a 20-byte hex address"))?;
    let (page, limit) = (query.cursor::<TxKey>()?, query.limit()?);
    let index = state.index.read();
    let total = index.sender_tx_count(&sender);
    let txs = index.sender_txs(&sender, page, limit);
    json(keyed_txs(&index, hex0x(&sender), total, txs))
}

#[derive(Deserialize)]
struct SearchQuery {
    q: String,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum SearchHit {
    Block(u64),
    Tx(String),
    Payload(String),
    Sender(String),
    None,
}

pub fn search_index(index: &Index, q: &str) -> SearchHit {
    let q = q.trim();
    if let Ok(slot) = q.parse::<u64>() {
        return if index.contains(slot) {
            SearchHit::Block(slot)
        } else {
            SearchHit::None
        };
    }
    if let Some(hash) = parse_hex::<32>(q) {
        if index.has_tx(&hash) {
            return SearchHit::Tx(hex0x(&hash));
        }
        if index.has_payload(&hash) {
            return SearchHit::Payload(hex0x(&hash));
        }
    }
    if let Some(sender) = parse_hex::<20>(q)
        && index.has_sender(&sender)
    {
        return SearchHit::Sender(hex0x(&sender));
    }
    SearchHit::None
}

async fn search(state: web::Data<AppState>, query: web::Query<SearchQuery>) -> ApiResult {
    json(search_index(&state.index.read(), &query.q))
}

async fn config(state: web::Data<AppState>) -> ApiResult {
    let index = state.index.read().config();
    json(serde_json::json!({
        "rpc_url": state.rpc_url,
        "retain_slots": index.retain_slots,
        "max_txs": index.max_txs,
        "max_limit": MAX_LIMIT,
        "live_interval_ms": LIVE_INTERVAL_MS,
    }))
}

async fn asset(req: HttpRequest, asset: &'static assets::Asset) -> HttpResponse {
    let etag = header::EntityTag::new_strong(asset.etag.clone());
    let fresh = <header::IfNoneMatch as header::Header>::parse(&req)
        .ok()
        .is_some_and(|m| match m {
            header::IfNoneMatch::Any => true,
            header::IfNoneMatch::Items(tags) => tags.iter().any(|t| t.weak_eq(&etag)),
        });
    let mut resp = if fresh {
        HttpResponse::NotModified()
    } else {
        HttpResponse::Ok()
    };
    resp.insert_header(header::ETag(etag))
        .insert_header(CacheControl(vec![CacheDirective::NoCache]));
    if fresh {
        return resp.finish();
    }
    let gzip = <header::AcceptEncoding as header::Header>::parse(&req)
        .ok()
        .and_then(|a| a.negotiate([header::Encoding::gzip(), header::Encoding::identity()].iter()))
        == Some(header::Encoding::gzip());
    resp.insert_header(ContentType(asset.content_type.clone()))
        .insert_header((header::VARY, "accept-encoding"));
    if gzip {
        resp.insert_header(header::ContentEncoding::Gzip)
            .body(asset.gzip.as_slice())
    } else {
        resp.body(asset.body)
    }
}

async fn not_found_api() -> ApiResult {
    Err(ApiError::not_found("no such endpoint"))
}

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/api")
            .route("/stats", web::get().to(stats))
            .route("/blocks", web::get().to(blocks))
            .route("/block/{slot}", web::get().to(block))
            .route("/block/{slot}/lane/{lane}", web::get().to(lane))
            .route("/block/{slot}/proof", web::get().to(proof))
            .route("/txs", web::get().to(txs))
            .route("/tx/{hash}", web::get().to(tx))
            .route("/payload/{hash}", web::get().to(payload))
            .route("/sender/{address}", web::get().to(sender))
            .route("/search", web::get().to(search))
            .route("/config", web::get().to(config))
            .default_service(web::to(not_found_api)),
    )
    .route("/", web::get().to(|r| asset(r, &assets::INDEX_HTML)))
    .route("/app.css", web::get().to(|r| asset(r, &assets::APP_CSS)))
    .route("/app.js", web::get().to(|r| asset(r, &assets::APP_JS)));
}
