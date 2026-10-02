'use strict';
(() => {
  const LIVE_KEY = 'mcp-explorer.live';
  const THEME_KEY = 'mcp-explorer.theme';
  const ROWS = 20;
  const MAX_PAYLOAD = 1024;
  const PENDING_GIVE_UP_MS = 120000;

  const $ = (sel, root = document) => root.querySelector(sel);
  const ESC = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' };
  const esc = (v) => String(v ?? '').replace(/[&<>"']/g, (c) => ESC[c]);
  const store = {
    get(k) { try { return localStorage.getItem(k); } catch { return null; } },
    set(k, v) { try { localStorage.setItem(k, v); } catch { /* storage unavailable */ } },
  };

  const S = {
    live: store.get(LIVE_KEY) !== 'off',
    tickMs: 1000,
    rpcUrl: '',
    view: null,
    token: 0,
    inflight: 0,
    busy: null,
    queued: null,
    updatedAt: null,
    skew: 0,
    pending: new Map(),
    landed: new Set(),
    polling: false,
    send: { state: 'idle' },
  };

  class ApiError extends Error {
    constructor(status, message) { super(message); this.status = status; }
  }

  async function api(path) {
    let res;
    try {
      res = await fetch('/api' + path, { cache: 'no-store' });
    } catch (e) {
      throw new ApiError(0, 'explorer unreachable');
    }
    let body = null;
    try { body = await res.json(); } catch { /* non-json error */ }
    if (!res.ok) throw new ApiError(res.status, (body && body.error) || res.statusText);
    return body;
  }

  // ---- formatting
  const short = (h, n = 6) => (h && h.length > 2 * n + 4 ? `${h.slice(0, n + 2)}…${h.slice(-n)}` : h ?? '');
  const num = (n) => (n == null ? '—' : Number(n).toLocaleString('en-US'));
  const ms = (v) => (v == null ? '—' : `${Math.abs(v) >= 100 ? Math.round(v) : v.toFixed(1)} ms`);
  const pct = (r) => (r == null ? '—' : `${(r * 100).toFixed(1)}%`);
  const bytes = (n) => (n < 1024 ? `${n} B` : `${(n / 1024).toFixed(1)} KiB`);
  const plural = (n, w) => `${num(n)} ${w}${n === 1 ? '' : 's'}`;
  const clock = (d) => d.toLocaleTimeString('en-GB', { hour12: false });
  const stamp = (t) => (t == null ? '—' : new Date(t).toISOString().replace('T', ' ').replace('Z', ' UTC'));

  function ago(t) {
    if (t == null) return '';
    const s = Math.max(0, (Date.now() + S.skew - t) / 1000);
    if (s < 10) return `${s.toFixed(1)}s ago`;
    if (s < 60) return `${Math.floor(s)}s ago`;
    if (s < 3600) return `${Math.floor(s / 60)}m ago`;
    if (s < 86400) return `${Math.floor(s / 3600)}h ago`;
    return `${Math.floor(s / 86400)}d ago`;
  }
  const timeEl = (t) => `<time data-t="${t ?? ''}" title="${esc(stamp(t))}">${ago(t)}</time>`;
  function updateAges() {
    for (const el of document.querySelectorAll('time[data-t]')) {
      if (el.dataset.t) el.textContent = ago(Number(el.dataset.t));
    }
  }

  const utf8Len = (s) => new TextEncoder().encode(s).length;
  function previewText(t) {
    if (!t.size) return '(empty)';
    if (t.preview_text != null) {
      const more = t.size > utf8Len(t.preview_text) ? '…' : '';
      return `“${esc(t.preview_text.replace(/\s+/g, ' '))}${more}”`;
    }
    return esc(short(t.preview, 8));
  }
  const pathBadge = (p) => `<span class="badge ${esc(p)}" data-testid="path-badge">${esc(p)}</span>`;
  function laneBar(pos, total) {
    let out = '';
    for (let i = 0; i < Math.min(total, 16); i++) out += `<i class="${i < pos ? '' : 'off'}"></i>`;
    return `<span class="lanebar" aria-hidden="true">${out}</span>`;
  }
  const blockLink = (s) => `<a class="mono" href="#/block/${s}">${s}</a>`;
  const txLink = (h, n) => `<a class="mono" href="#/tx/${esc(h)}" title="${esc(h)}">${esc(n ? short(h, n) : h)}</a>`;
  const emptyRow = (text) => `<li class="empty-note">${esc(text)}</li>`;

  function blockRow(b) {
    return `<li class="row" data-testid="block-row" data-slot="${b.slot}">
      <span class="ico">Bk</span>
      <div class="cell">${blockLink(b.slot)}${timeEl(b.finalized_at_ms)}</div>
      <div class="cell"><span>${laneBar(b.positive_lanes, b.num_lanes)}${b.positive_lanes}/${b.num_lanes} lanes</span>
        <span class="sub">${plural(b.tx_count, 'tx')} · latency ${ms(b.latency_ms)}</span></div>
      ${pathBadge(b.path)}</li>`;
  }

  function txRow(t) {
    return `<li class="row${S.landed.has(t.hash) ? ' is-new' : ''}" data-testid="tx-row" data-hash="${esc(t.hash)}">
      <span class="ico">Tx</span>
      <div class="cell">${txLink(t.hash, 6)}${timeEl(t.finalized_at_ms)}</div>
      <div class="cell"><span>slot ${blockLink(t.slot)} · lane ${t.lane}</span>
        <span class="sub mono" title="${esc(t.sender)}">from <a href="#/sender/${esc(t.sender)}">${esc(short(t.sender, 4))}</a></span></div>
      <div class="preview" data-testid="tx-preview">${previewText(t)}</div>
      <span class="size">${bytes(t.size)}</span></li>`;
  }

  // ---- header state
  function renderLive() {
    const btn = $('#live-toggle');
    btn.setAttribute('aria-pressed', String(S.live));
    btn.querySelector('.live-label').textContent = S.live ? 'Live' : 'Paused';
    const at = S.updatedAt ? clock(S.updatedAt) : 'never';
    $('#live-status').textContent = S.live ? `updated ${at}` : `Paused, updated ${at}`;
  }

  function renderHeader(stats) {
    S.skew = stats.now_ms - Date.now();
    $('#head-slot').textContent = stats.head == null ? 'slot —' : `slot ${stats.head}`;
    const ix = stats.indexing;
    const banner = $('#indexing-banner');
    banner.hidden = ix.complete;
    if (!ix.complete) {
      const frac = ix.total ? Math.min(1, ix.done / ix.total) : 0;
      banner.innerHTML = `Indexing ledger: <span class="num">${num(ix.done)} / ${num(ix.total)}</span> blocks
        <div class="progress"><span style="width:${(frac * 100).toFixed(1)}%"></span></div>`;
    }
  }

  function setError(e) {
    const el = $('#error-banner');
    el.hidden = !e;
    if (e) el.textContent = `Could not load data: ${e.message}`;
  }

  // ---- home
  function tile(id, label, value, sub, extra = '', wide = false) {
    return `<div class="tile${wide ? ' wide' : ''}" data-testid="stat-${id}">
      <div class="tile-label">${esc(label)}</div>
      <div class="tile-value" data-testid="stat-${id}-value">${esc(value)}</div>
      <div class="tile-sub">${esc(sub)}</div>${extra}</div>`;
  }

  function sparkline(values) {
    if (values.length < 2) return '<svg class="spark" data-testid="sparkline"></svg>';
    const lo = Math.min(...values), hi = Math.max(...values);
    const span = Math.max(hi - lo, 1e-3);
    const w = 100, h = 30, pad = 2;
    const y = (v) => (pad + (1 - (v - lo) / span) * (h - 2 * pad)).toFixed(2);
    const pts = values.map((v, i) => `${((i / (values.length - 1)) * w).toFixed(2)},${y(v)}`);
    const avg = y(values.reduce((a, b) => a + b, 0) / values.length);
    return `<svg class="spark" data-testid="sparkline" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none" role="img"
      aria-label="last ${values.length} block times, ${ms(lo)} to ${ms(hi)}"><line x1="0" x2="${w}" y1="${avg}" y2="${avg}"></line>
      <polyline points="${pts.join(' ')}"></polyline></svg>`;
  }

  function renderTiles(st) {
    const [w100, w1000] = [st.windows.find((w) => w.size === 100), st.windows.find((w) => w.size === 1000)];
    const bt = w100 && w100.block_time_ms;
    const lat = w100 && w100.latency_ms;
    const t = st.totals;
    $('#tiles').innerHTML = [
      tile('head', 'Head slot', st.head ?? '—', `${num(st.retained_blocks)} blocks indexed`),
      tile('block-time', 'Block time · last 100', bt ? ms(bt.avg) : '—', bt ? `p50 ${ms(bt.p50)} · p95 ${ms(bt.p95)}` : '', sparkline(st.block_times_ms), true),
      tile('latency', 'Finalization latency', lat ? ms(lat.p50) : '—', lat ? `p50 · p95 ${ms(lat.p95)} · max ${ms(lat.max)}` : 'finalized − deadline'),
      tile('fast', 'Fast path · last 1000', pct(w1000 && w1000.fast_ratio), `${num(t.fallback)} fallback of ${num(t.blocks)}`),
      tile('tps', 'Tx / s · last 100', w100 && w100.tx_per_s != null ? w100.tx_per_s.toFixed(2) : '—', `${num(w1000 ? w1000.txs : 0)} txs in last 1000 blocks`),
      tile('total-txs', 'Total txs indexed', num(t.txs), `${bytes(t.payload_bytes)} of payload`, '', true),
    ].join('');
  }

  function Home() {
    const v = { blocks: [], txs: [] };
    async function page(kind, after, key) {
      const base = `/${kind}?limit=${ROWS}`;
      if (after == null) return { rows: (await api(base))[kind], fresh: true };
      const r = await api(`${base}&after=${encodeURIComponent(after)}`);
      if (!r.has_more) return { rows: r[kind], fresh: false };
      return { rows: (await api(base))[kind], fresh: true };
    }
    const merge = (old, got) => (got.fresh ? got.rows : got.rows.concat(old)).slice(0, ROWS);
    return {
      name: 'home',
      html: () => `<section class="tiles" id="tiles" data-testid="stats"></section>
        <section class="card" data-testid="send-panel">
          <div class="card-head">Send a transaction <span class="muted" data-testid="send-target">via rpc ${esc(S.rpcUrl || '(not configured)')}</span></div>
          <div class="card-body">
            <form class="send-form" id="send-form" data-testid="send-form" autocomplete="off">
              <textarea id="send-payload" data-testid="send-payload" rows="1" placeholder="payload, e.g. hello chorus" aria-label="Payload"></textarea>
              <select id="send-lane" data-testid="send-lane" aria-label="Lane">
                <option value="">Any lane</option>${[0, 1, 2, 3, 4].map((l) => `<option value="${l}">Lane ${l}</option>`).join('')}</select>
              <input id="send-sender" data-testid="send-sender" placeholder="sender 0x… (optional, 20 bytes)" aria-label="Sender" spellcheck="false">
              <button class="primary" type="submit" id="send-submit" data-testid="send-submit">Send tx</button>
            </form>
            <div class="send-meta"><span class="send-size" id="send-size" data-testid="send-size">0 / ${MAX_PAYLOAD} B</span>
              <span class="send-result" id="send-result" data-testid="send-result" data-state="idle"></span></div>
          </div>
        </section>
        <div class="grid2">
          <section class="card"><div class="card-head">Latest blocks <a href="#/blocks">View all blocks →</a></div>
            <ol class="rows" id="recent-blocks" data-testid="recent-blocks"><li class="empty-note">Loading…</li></ol></section>
          <section class="card"><div class="card-head">Latest transactions <a href="#/txs">View all txs →</a></div>
            <ol class="rows tx" id="recent-txs" data-testid="recent-txs"><li class="empty-note">Loading…</li></ol></section>
        </div>`,
      mount: mountSend,
      async refresh(full, alive) {
        const incr = !full;
        const [stats, blocks, txs] = await Promise.all([
          api('/stats'),
          page('blocks', incr && v.blocks.length ? v.blocks[0].slot : null),
          page('txs', incr && v.txs.length ? v.txs[0].cursor : null),
        ]);
        if (!alive()) return;
        renderHeader(stats);
        renderTiles(stats);
        v.blocks = merge(v.blocks, blocks);
        v.txs = merge(v.txs, txs);
        $('#recent-blocks').innerHTML = v.blocks.map(blockRow).join('') || emptyRow('No blocks indexed yet.');
        $('#recent-txs').innerHTML = v.txs.map(txRow).join('') || emptyRow('No transactions yet. Send one above.');
      },
    };
  }

  // ---- send panel
  function renderSend() {
    const el = $('#send-result');
    if (!el) return;
    const s = S.send;
    el.dataset.state = s.state;
    el.dataset.hash = s.hash || '';
    const link = s.hash ? `<a data-testid="send-result-link" href="#/tx/${esc(s.hash)}">${esc(short(s.hash))}</a>` : '';
    el.innerHTML = {
      idle: '',
      sending: 'Sending…',
      pending: `${link} ${esc(s.detail || '')} · waiting for inclusion…`,
      landed: `${link} landed in slot ${blockLink(s.slot)} lane ${s.lane}`,
      error: esc(s.detail || 'send failed'),
    }[s.state];
    const submit = $('#send-submit');
    if (submit) submit.disabled = s.state === 'sending';
  }

  function mountSend() {
    const payload = $('#send-payload');
    const size = $('#send-size');
    const update = () => {
      const len = utf8Len(payload.value);
      const bad = len > MAX_PAYLOAD;
      size.textContent = `${len} / ${MAX_PAYLOAD} B`;
      size.classList.toggle('bad', bad);
      return !bad;
    };
    payload.addEventListener('input', update);
    $('#send-form').addEventListener('submit', async (ev) => {
      ev.preventDefault();
      if (!update()) return;
      if (!S.rpcUrl) {
        S.send = { state: 'error', detail: 'no rpc url configured' };
        return renderSend();
      }
      const body = { payload_utf8: payload.value };
      const lane = $('#send-lane').value;
      if (lane !== '') body.lane = Number(lane);
      const sender = $('#send-sender').value.trim();
      if (sender) {
        if (!/^(0x)?[0-9a-fA-F]{40}$/.test(sender)) {
          S.send = { state: 'error', detail: 'sender must be 20 bytes of hex' };
          return renderSend();
        }
        body.sender = '0x' + sender.replace(/^0x/i, '').toLowerCase();
      }
      S.send = { state: 'sending' };
      renderSend();
      try {
        const res = await fetch('/api/tx', {
          method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body),
        });
        let reply = null;
        try { reply = await res.json(); } catch { /* non-json */ }
        if (!res.ok || !reply || !reply.tx_hash) throw new Error((reply && reply.error) || `explorer answered ${res.status}`);
        const hash = '0x' + String(reply.tx_hash).replace(/^0x/i, '').toLowerCase();
        const target = reply.target_slot != null ? `target slot ${reply.target_slot}` : '';
        S.pending.set(hash, Date.now());
        S.send = { state: 'pending', hash, detail: [reply.status, target].filter(Boolean).join(', ') };
        renderSend();
        pollPending();
      } catch (e) {
        S.send = { state: 'error', detail: `send failed: ${e.message}` };
        renderSend();
      }
    });
    renderSend();
  }

  // polled even while paused, until each sent tx lands.
  async function pollPending() {
    if (S.polling) return;
    S.polling = true;
    try {
      for (const [hash, since] of S.pending) {
        let tx;
        try {
          tx = await api(`/tx/${hash}`);
        } catch (e) {
          if (Date.now() - since < PENDING_GIVE_UP_MS) continue;
          S.pending.delete(hash);
          if (S.send.hash === hash) {
            S.send = { state: 'error', hash, detail: `tx ${short(hash)} not seen in the ledger after ${PENDING_GIVE_UP_MS / 1000}s` };
          }
          renderSend();
          continue;
        }
        S.pending.delete(hash);
        S.landed.add(hash);
        if (S.send.hash === hash) S.send = { state: 'landed', hash, slot: tx.slot, lane: tx.lane };
        renderSend();
        if (S.view && (S.view.name === 'home' || S.view.hash === hash)) load(false);
      }
    } finally {
      S.polling = false;
    }
  }

  // ---- block
  function laneCard(slot, l) {
    const cls = !l.positive ? 'empty' : l.decode_error ? 'bad' : l.tx_count ? '' : 'quiet';
    const tag = !l.positive ? 'negative' : l.decode_error ? 'undecodable' : plural(l.tx_count, 'tx');
    const txs = l.txs.map((t) => `<li class="${S.landed.has(t.hash) ? 'is-new' : ''}" data-testid="lane-tx-row" data-hash="${esc(t.hash)}">
        ${txLink(t.hash, 6)}<span class="preview">${previewText(t)}</span></li>`).join('');
    return `<article class="lane ${cls}" data-testid="lane-card" data-lane-index="${l.index}" data-positive="${l.positive}" data-tx-count="${l.tx_count}">
      <header><span class="lane-no">Lane ${l.index}</span><span class="lane-tag">${tag}</span></header>
      <dl><dt>Proposer</dt><dd data-testid="lane-proposer">${l.proposer ?? '—'}</dd>
        <dt>Root</dt><dd data-testid="lane-root" title="${esc(l.root || '')}">${l.root ? esc(short(l.root, 8)) : '—'}</dd>
        <dt>Size</dt><dd>${l.positive ? bytes(l.payload_len) : '—'}</dd></dl>
      ${txs ? `<ul class="lane-txs">${txs}</ul>` : ''}
      ${l.more_txs ? `<a class="more" data-testid="lane-more" href="#/block/${slot}/lane/${l.index}">All ${plural(l.tx_count, 'tx')} →</a>` : ''}
    </article>`;
  }

  function BlockView(slot) {
    const v = { json: null, data: null, proof: null };
    function render(b) {
      const root = $('#block');
      const nav = `<span class="nav-pills">${b.prev != null ? `<a data-testid="block-prev" href="#/block/${b.prev}">← ${b.prev}</a>` : '<span>← none</span>'}
        ${b.next != null ? `<a data-testid="block-next" href="#/block/${b.next}">${b.next} →</a>` : '<span>newest</span>'}</span>`;
      const proof = v.proof
        ? `<pre class="hex" data-testid="proof-hex">${esc(v.proof)}</pre>`
        : '';
      root.innerHTML = `<h1>Block <span class="mono">#${b.slot}</span> <span data-testid="block-path">${pathBadge(b.path)}</span>${nav}</h1>
        ${b.gap_before ? `<p class="gap" data-testid="block-gap">${plural(b.gap_before, 'slot')} without a block before this one</p>` : ''}
        <section class="card"><dl class="kv">
          <dt>Slot</dt><dd class="mono">${b.slot}</dd>
          <dt>Finalization path</dt><dd>${pathBadge(b.path)}</dd>
          <dt>Deadline</dt><dd data-testid="block-deadline">${b.deadline_ms != null ? esc(stamp(b.deadline_ms)) : 'unknown'}</dd>
          <dt>Finalized at</dt><dd data-testid="block-finalized">${esc(stamp(b.finalized_at_ms))} (${timeEl(b.finalized_at_ms)})</dd>
          <dt>Finalized − deadline</dt><dd data-testid="block-latency">${ms(b.latency_ms)}</dd>
          <dt>Mini-proposals</dt><dd>${b.positive_lanes} of ${b.num_lanes} lanes committed</dd>
          <dt>Transactions</dt><dd data-testid="block-tx-count">${num(b.tx_count)}</dd>
          <dt>Payload</dt><dd>${bytes(b.payload_bytes)}${b.decode_errors ? ` · <span class="gap">${plural(b.decode_errors, 'undecodable lane')}</span>` : ''}</dd>
          <dt>Commit proof</dt><dd data-testid="block-proof-size">${b.proof_size != null ? bytes(b.proof_size) : 'unavailable'}
            ${b.proof_size != null ? `<button type="button" class="ghost" data-testid="proof-toggle" id="proof-toggle">${v.proof ? 'Hide' : 'Show'} proof hex</button>` : ''}</dd>
        </dl>${proof}</section>
        <h2 class="section">Mini-proposals</h2>
        ${b.on_disk ? `<div class="lanes" data-testid="lane-cards">${b.lanes.map((l) => laneCard(b.slot, l)).join('')}</div>`
          : '<p class="muted" data-testid="block-pruned">This block has been pruned from the ledger; only its summary is kept.</p>'}`;
      const toggle = $('#proof-toggle');
      if (toggle) {
        toggle.addEventListener('click', async () => {
          if (v.proof) { v.proof = null; return render(v.data); }
          try { v.proof = (await api(`/block/${slot}/proof`)).proof; } catch (e) { return setError(e); }
          render(v.data);
        });
      }
    }
    return {
      name: 'block',
      html: () => `<section data-testid="block-detail" data-slot="${slot}" id="block"><div class="loading">Loading block ${slot}…</div></section>`,
      async refresh(full, alive) {
        const need = full || !v.data || v.data.next == null;
        const [stats, got] = await Promise.all([api('/stats'), need ? api(`/block/${slot}`).catch((e) => e) : null]);
        if (!alive()) return;
        renderHeader(stats);
        if (got instanceof Error) {
          if (got.status !== 404) throw got;
          if (!v.data) {
            $('#block').innerHTML = `<h1>Block <span class="mono">#${slot}</span></h1><section class="card"><div class="waiting" data-testid="not-found">
              Slot ${slot} is not indexed${stats.head != null && slot > stats.head ? ' yet (head is ' + stats.head + ')' : ''}.</div></section>`;
          }
          return;
        }
        if (got) {
          const json = JSON.stringify(got);
          if (json !== v.json) { v.json = json; v.data = got; render(got); }
        }
      },
    };
  }

  // demo(tx-timeline): mempool (rpc received -> sealed), proposing (-> slot deadline),
  // fast voting (-> fast block formed), finalizing (-> finalized)
  const PHASES = { mempool: 'Mempool', proposing: 'Proposing', fast_voting: 'Fast voting', finalizing: 'Finalizing' };
  // demo(tx-timeline)
  function timelineCard(i) {
    const ph = i.phases || [];
    if (!ph.length) return '';
    const spans = ph.map((p) => Math.max(p.duration_ms, 0));
    const total = spans.reduce((a, b) => a + b, 0);
    const label = (p) => PHASES[p.name] || p.name;
    const tod = (t) => new Date(t).toISOString().slice(11, 23);
    const segs = ph.map((p, k) => `<span class="tl-seg tl-${esc(p.name)}" data-testid="timeline-seg" style="flex-grow:${total ? spans[k] / total : 1}"
      title="${esc(label(p))}: ${esc(ms(p.duration_ms))}"></span>`).join('');
    const rows = ph.map((p) => `<tr data-testid="timeline-row" data-phase="${esc(p.name)}"><td data-testid="timeline-name"><i class="tl-dot tl-${esc(p.name)}"></i>${esc(label(p))}</td>
      <td class="mono" title="${esc(stamp(p.start_ms))}">${esc(tod(p.start_ms))}</td><td class="mono" title="${esc(stamp(p.end_ms))}">${esc(tod(p.end_ms))}</td>
      <td class="mono num" data-testid="timeline-duration">${esc(ms(p.duration_ms))}</td></tr>`).join('');
    const e2e = ph[ph.length - 1].end_ms - ph[0].start_ms;
    return `<section class="card timeline" data-testid="tx-timeline" data-slot="${i.slot}">
      <h2><span>Latency · slot ${blockLink(i.slot)}</span><span class="muted" data-testid="timeline-total">end to end ${ms(e2e)}</span></h2>
      <div class="card-body"><div class="tl-bar">${segs}</div>
        <table class="tl-table"><thead><tr><th>Phase</th><th>Start (UTC)</th><th>End (UTC)</th><th>Duration</th></tr></thead><tbody>${rows}</tbody></table>
        <p class="muted tl-note">Clocks: received is the rpc host's, sealed the proposer's, the deadline the slot schedule's, fast block and finalized this explorer's node; skew can make a phase negative.</p></div></section>`;
  }

  // ---- tx
  function TxView(hash) {
    const v = { data: null };
    function render(t) {
      const extra = t.inclusions.length > 1
        ? `<dt>Also included in</dt><dd>${t.inclusions.slice(1).map((i) => `slot ${blockLink(i.slot)} lane ${i.lane}`).join(', ')}</dd>` : '';
      const payload = t.payload == null
        ? `<span class="gap">unavailable: ${esc(t.payload_error || 'unknown')}</span>`
        : `${t.payload_utf8 != null ? `<pre class="payload-text" data-testid="tx-payload-utf8">${esc(t.payload_utf8)}</pre>` : '<span class="muted" data-testid="tx-payload-utf8">(not utf-8 text)</span>'}`;
      $('#tx').innerHTML = `<h1>Transaction</h1>
        <section class="card"><dl class="kv">
          <dt>Tx hash</dt><dd class="mono" data-testid="tx-hash">${esc(t.hash)}</dd>
          <dt>Status</dt><dd><span class="badge fast" data-testid="tx-status">committed</span></dd>
          <dt>Block</dt><dd data-testid="tx-block"><a class="mono" data-testid="tx-block-link" href="#/block/${t.slot}">${t.slot}</a> · ${timeEl(t.finalized_at_ms)}</dd>
          <dt>Lane / position</dt><dd data-testid="tx-lane">lane ${t.lane} · position ${t.pos}</dd>
          ${extra}
          <dt>Sender</dt><dd><a class="mono" data-testid="tx-sender" href="#/sender/${esc(t.sender)}">${esc(t.sender)}</a></dd>
          <dt>Nonce</dt><dd class="mono" data-testid="tx-nonce">${t.nonce}</dd>
          <dt>Payload size</dt><dd data-testid="tx-size">${bytes(t.size)}</dd>
          <dt>Payload hash</dt><dd><a class="mono" data-testid="tx-payload-hash" href="#/payload/${esc(t.payload_hash)}">${esc(t.payload_hash)}</a></dd>
          <dt>Payload (text)</dt><dd>${payload}</dd>
          <dt>Payload (hex)</dt><dd><p class="payload-hex" data-testid="tx-payload-hex">${esc(t.payload ?? '—')}</p></dd>
        </dl></section>${t.inclusions.map(timelineCard).join('')}`; // demo(tx-timeline)
    }
    return {
      name: 'tx',
      hash,
      html: () => `<section data-testid="tx-detail" data-hash="${esc(hash)}" id="tx"><div class="loading">Loading tx…</div></section>`,
      async refresh(full, alive) {
        const [stats, got] = await Promise.all([api('/stats'), full || !v.data ? api(`/tx/${hash}`).catch((e) => e) : null]);
        if (!alive()) return;
        renderHeader(stats);
        if (got instanceof Error) {
          if (got.status !== 404) throw got;
          const why = S.pending.has(hash) ? 'Waiting for it to land…' : 'It has not landed yet, or it was pruned.';
          $('#tx').innerHTML = `<h1>Transaction</h1><section class="card"><div class="waiting" data-testid="not-found">
            <div class="mono">${esc(hash)}</div><p>Not indexed. ${why}</p></div></section>`;
          return;
        }
        if (got) { v.data = got; render(got); }
      },
    };
  }

  // ---- paged lists
  function ListView({ name, title, sub, path, rows, next, href, cls = '', row, first }) {
    return {
      name,
      html: () => `<h1>${title}</h1><section class="card"><div class="card-head"><span data-testid="list-sub">${sub || ''}</span>
          ${first ? '' : `<a href="${href(null)}">Newest</a>`}</div>
        <ol class="rows ${cls}" id="list" data-testid="${name}-list"><li class="empty-note">Loading…</li></ol>
        <div class="pager" id="pager"></div></section>`,
      async refresh(full, alive) {
        if (!full && !first) return false;
        const [stats, r] = await Promise.all([api('/stats'), api(path)]);
        if (!alive()) return;
        renderHeader(stats);
        const items = rows(r);
        $('#list').innerHTML = items.map(row).join('') || emptyRow('Nothing here.');
        if (r.tx_count != null) $('[data-testid="list-sub"]').textContent = plural(r.tx_count, 'tx');
        const n = next(r, items);
        $('#pager').innerHTML = n != null ? `<a data-testid="pager-older" href="${href(n)}">Older →</a>` : '';
      },
    };
  }

  const q = (limit, key, val) => `?limit=${limit}${val != null ? `&${key}=${encodeURIComponent(val)}` : ''}`;
  const routes = [
    [/^#?\/?$/, () => Home()],
    [/^#\/block\/(\d+)$/, (m) => BlockView(m[1])],
    [/^#\/block\/(\d+)\/lane\/(\d+)(?:\/from\/(\d+))?$/, ([, s, j, from]) => ListView({
      name: 'lane', title: `Block <a class="mono" href="#/block/${s}">#${s}</a> · lane ${j}`, cls: 'tx', row: txRow, first: from == null,
      path: `/block/${s}/lane/${j}${q(ROWS, 'cursor', from)}`, rows: (r) => r.txs, next: (r) => r.next_cursor,
      href: (c) => `#/block/${s}/lane/${j}${c != null ? `/from/${c}` : ''}`,
    })],
    [/^#\/tx\/(0x[0-9a-fA-F]{64})$/, (m) => TxView(m[1].toLowerCase())],
    [/^#\/(payload|sender)\/(0x[0-9a-fA-F]{40}(?:[0-9a-fA-F]{24})?)(?:\/before\/([\d.]+))?$/, ([, kind, id, before]) => ListView({
      name: kind, title: `${kind === 'payload' ? 'Payload' : 'Sender'} <span class="mono">${esc(short(id, 10))}</span>`,
      sub: `<span class="mono">${esc(id)}</span>`, cls: 'tx', row: txRow, first: before == null,
      path: `/${kind}/${id}${q(ROWS, 'cursor', before)}`, rows: (r) => r.txs, next: (r) => r.next_cursor,
      href: (c) => `#/${kind}/${id}${c != null ? `/before/${c}` : ''}`,
    })],
    [/^#\/blocks(?:\/before\/(\d+))?$/, ([, before]) => ListView({
      name: 'blocks', title: 'Blocks', row: blockRow, first: before == null,
      path: `/blocks${q(ROWS, 'before', before)}`, rows: (r) => r.blocks,
      next: (r, items) => (r.has_more && items.length ? items[items.length - 1].slot : null),
      href: (c) => `#/blocks${c != null ? `/before/${c}` : ''}`,
    })],
    [/^#\/txs(?:\/before\/([\d.]+))?$/, ([, before]) => ListView({
      name: 'txs', title: 'Transactions', cls: 'tx', row: txRow, first: before == null,
      path: `/txs${q(ROWS, 'before', before)}`, rows: (r) => r.txs,
      next: (r, items) => (r.has_more && items.length ? items[items.length - 1].cursor : null),
      href: (c) => `#/txs${c != null ? `/before/${c}` : ''}`,
    })],
  ];

  function resolve(hash) {
    for (const [re, make] of routes) {
      const m = hash.match(re);
      if (m) return make(m);
    }
    return {
      name: 'missing',
      html: () => `<section class="card"><div class="waiting" data-testid="not-found">No such page. <a href="#/">Back to the dashboard</a></div></section>`,
      refresh: async () => false,
    };
  }

  // one load per view at a time: concurrent incremental refreshes read the same cursor and merge rows twice.
  async function load(full) {
    const token = S.token;
    if (S.busy === token) { S.queued = { full: full || !!S.queued?.full }; return; }
    S.busy = token;
    const view = S.view;
    S.inflight++;
    try {
      // false: nothing was fetched.
      const fetched = await view.refresh(full, () => token === S.token);
      if (token === S.token && fetched !== false) { S.updatedAt = new Date(); setError(null); }
    } catch (e) {
      if (token === S.token) setError(e);
    } finally {
      S.inflight--;
      if (S.busy === token) S.busy = null;
      renderLive();
    }
    const next = S.queued;
    if (next && token === S.token) { S.queued = null; load(next.full); }
  }

  function navigate() {
    S.token++;
    S.queued = null;
    S.view = resolve(location.hash);
    $('#view').innerHTML = S.view.html();
    if (S.view.mount) S.view.mount();
    window.scrollTo(0, 0);
    load(true);
  }

  // fetch before aging, so the newest rows are not shown a tick old just before they are replaced
  async function tick() {
    if (!document.hidden) {
      if (S.pending.size) pollPending();
      if (S.live && S.inflight === 0) await load(false);
    }
    updateAges();
  }

  function setLive(on) {
    S.live = on;
    store.set(LIVE_KEY, on ? 'on' : 'off');
    renderLive();
    if (on && S.inflight === 0) load(false);
  }

  function bindChrome() {
    $('#live-toggle').addEventListener('click', () => setLive(!S.live));
    $('#refresh-button').addEventListener('click', () => load(true));
    $('#theme-toggle').addEventListener('click', () => {
      const next = document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark';
      document.documentElement.dataset.theme = next;
      store.set(THEME_KEY, next);
    });
    $('#search-form').addEventListener('submit', async (ev) => {
      ev.preventDefault();
      const input = $('#search-input');
      const query = input.value.trim();
      const msg = $('#search-message');
      msg.hidden = true;
      if (!query) return;
      try {
        const hit = await api(`/search?q=${encodeURIComponent(query)}`);
        const target = { block: `#/block/${hit.id}`, tx: `#/tx/${hit.id}`, payload: `#/payload/${hit.id}`, sender: `#/sender/${hit.id}` }[hit.kind];
        if (target) {
          input.value = '';
          location.hash = target;
        } else {
          msg.textContent = `Nothing indexed matches “${query}”.`;
          msg.hidden = false;
        }
      } catch (e) {
        setError(e);
      }
    });
    window.addEventListener('hashchange', () => { $('#search-message').hidden = true; navigate(); });
    document.addEventListener('visibilitychange', () => { if (!document.hidden) tick(); });
  }

  async function start() {
    bindChrome();
    renderLive();
    try {
      const cfg = await api('/config');
      S.rpcUrl = (cfg.rpc_url || '').replace(/\/+$/, '');
      S.tickMs = cfg.live_interval_ms || S.tickMs;
    } catch (e) {
      setError(e);
    }
    navigate();
    setInterval(tick, S.tickMs);
  }

  start();
})();
