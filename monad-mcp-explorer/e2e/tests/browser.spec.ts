import { expect, test, type Page, type Request } from '@playwright/test';
import { randomBytes } from 'node:crypto';
import path from 'node:path';
import { explorerTx, hexOf, randomSender, stack } from './stack';

// every explorer api request the page makes, in order.
function apiLog(page: Page) {
  const seen: string[] = [];
  page.on('request', (req: Request) => {
    const url = new URL(req.url());
    if (url.pathname.startsWith('/api/')) seen.push(url.pathname);
  });
  return {
    get count() { return seen.length; },
    since(n: number) { return seen.slice(n); },
  };
}

const quiet = async (page: Page, ms: number) => page.waitForTimeout(ms);

test('L6b: send from the panel, follow it to its block and payload, then Live off/Refresh/reload', async ({ page }) => {
  const errors: string[] = [];
  page.on('pageerror', (e) => errors.push(e.message));
  const sender = randomSender();
  const payload = `hello chorus from playwright ${randomBytes(4).toString('hex')}`;
  // list rows preview the first 32 bytes; only the tx page carries the whole payload
  const preview = `“${payload.slice(0, 32)}…”`;

  await page.goto('/');
  await expect(page.getByTestId('head-slot')).toHaveText(/^slot \d+$/);
  await expect(page.getByTestId('send-target')).toContainText(stack.rpc);
  await expect(page.getByTestId('live-toggle')).toHaveAttribute('aria-pressed', 'true');

  await page.getByTestId('send-payload').fill(payload);
  await expect(page.getByTestId('send-size')).toHaveText(`${payload.length} / 1024 B`);
  await page.getByTestId('send-sender').fill(sender);
  // a lone validator holds lane 0
  await page.getByTestId('send-lane').selectOption('0');
  // same-origin: the explorer forwards to the rpc, so a tunnel to the explorer alone suffices
  const sent = page.waitForResponse((r) => r.request().method() === 'POST' && r.url() === `${stack.explorer}/api/tx`);
  await page.getByTestId('send-submit').click();
  expect((await sent).status()).toBe(200);
  // demo(tx-timeline): the page stamps its send as an integer of unix ns
  const posted = (await sent).request();
  expect(posted.postDataJSON()).toEqual({ payload_utf8: payload, sender, lane: 0, sent_at_ns: expect.any(Number) });
  expect(posted.postData()).toMatch(/"sent_at_ns":\d{19}[,}]/);

  const result = page.getByTestId('send-result');
  await expect(result).toHaveAttribute('data-state', 'landed');
  const hash = (await result.getAttribute('data-hash'))!;
  expect(hash).toMatch(/^0x[0-9a-f]{64}$/);
  await expect(result).toContainText(/landed in slot \d+ lane \d+/);
  const [, slot, lane] = (await result.textContent())!.match(/slot (\d+) lane (\d+)/)!;

  const row = page.locator(`[data-testid="recent-txs"] [data-testid="tx-row"][data-hash="${hash}"]`).first();
  await expect(row).toBeVisible();
  await expect(row).toHaveClass(/is-new/);
  await expect(row.getByTestId('tx-preview')).toHaveText(preview);
  await expect(row).toContainText(`slot ${slot} · lane ${lane}`);
  await page.locator(`[data-testid="recent-blocks"] [data-testid="block-row"][data-slot="${slot}"]`).first().waitFor();
  await page.screenshot({ path: path.join(stack.shots, 'e2e-home.png'), fullPage: true });

  await row.locator(`a[href="#/block/${slot}"]`).click();
  await expect(page).toHaveURL(new RegExp(`#/block/${slot}$`));
  await expect(page.getByTestId('block-detail')).toHaveAttribute('data-slot', slot);
  await expect(page.getByTestId('block-path')).toHaveText(/fast|fallback/);
  const blockPath = (await page.getByTestId('block-path').textContent())!.trim(); // demo(tx-timeline)
  const card = page.locator(`[data-testid="lane-card"][data-lane-index="${lane}"]`);
  await expect(card).toHaveAttribute('data-positive', 'true');
  expect(Number(await card.getAttribute('data-tx-count'))).toBeGreaterThanOrEqual(1);
  await expect(card.getByTestId('lane-proposer')).toHaveText('0');
  await expect(card.getByTestId('lane-root')).toHaveAttribute('title', /^0x[0-9a-f]{40}$/);
  await expect(card.getByTestId('lane-decoded')).toHaveText(/^[+-]\d+(\.\d)? ms from deadline$/); // demo(tx-timeline)
  const laneTx = card.locator(`[data-testid="lane-tx-row"][data-hash="${hash}"]`);
  await expect(laneTx).toBeVisible();
  await expect(laneTx.locator('.preview')).toHaveText(preview);
  await page.getByTestId('proof-toggle').click();
  await expect(page.getByTestId('proof-hex')).toHaveText(/^0x[0-9a-f]+$/);
  await card.scrollIntoViewIfNeeded();
  await page.screenshot({ path: path.join(stack.shots, 'e2e-block.png'), fullPage: true });

  await laneTx.locator(`a[href="#/tx/${hash}"]`).click();
  const detail = page.getByTestId('tx-detail');
  await expect(detail).toHaveAttribute('data-hash', hash);
  await expect(page.getByTestId('tx-hash')).toHaveText(hash);
  await expect(page.getByTestId('tx-status')).toHaveText('committed');
  await expect(page.getByTestId('tx-block-link')).toHaveText(slot);
  await expect(page.getByTestId('tx-lane')).toContainText(`lane ${lane}`);
  await expect(page.getByTestId('tx-sender')).toHaveText(sender);
  await expect(page.getByTestId('tx-size')).toHaveText(`${payload.length} B`);
  await expect(page.getByTestId('tx-payload-utf8')).toHaveText(payload);
  await expect(page.getByTestId('tx-payload-hex')).toHaveText(hexOf(payload));
  // demo(tx-timeline): sent by the page through the rpc and sealed by a mempool proposer, so every
  // phase before the seal is known; only a fast block has a fast voting phase
  const timeline = page.getByTestId('tx-timeline');
  await expect(timeline).toHaveAttribute('data-slot', slot);
  const phases = ['submit', 'forwarding', 'mempool', 'proposing', ...(blockPath === 'fast' ? ['fast_voting'] : []), 'finalizing'];
  const names = {
    submit: 'Submit', forwarding: 'Forwarding', mempool: 'Mempool', proposing: 'Proposing', fast_voting: 'Fast voting', finalizing: 'Finalizing',
  };
  const rows = timeline.getByTestId('timeline-row');
  await expect(rows).toHaveCount(phases.length);
  expect(await rows.evaluateAll((els) => els.map((e) => e.getAttribute('data-phase')))).toEqual(phases);
  await expect(timeline.getByTestId('timeline-name')).toHaveText(phases.map((p) => names[p as keyof typeof names]));
  await expect(timeline.getByTestId('timeline-seg')).toHaveCount(phases.length);
  for (const d of await timeline.getByTestId('timeline-duration').allTextContents()) expect(d).toMatch(/^-?\d+(\.\d)? ms$/);
  for (const row of await rows.all()) await expect(row.locator('td')).toHaveCount(5);
  await expect(timeline.getByTestId('timeline-total')).toHaveText(/^end to end -?\d+(\.\d)? ms$/);
  // the axis starts at the send and ends at finalized, so its last label is the end to end total
  const ticks = timeline.getByTestId('timeline-tick-ms');
  await expect(ticks.first()).toHaveText('0 ms');
  const total = (await timeline.getByTestId('timeline-total').textContent())!.replace('end to end ', '');
  await expect(ticks.last()).toHaveText(total);
  // the lane decode is a milestone: a marker on the bar and its own row, offset from the seal
  await expect(timeline.getByTestId('timeline-decoded-marker')).toBeVisible();
  const decoded = timeline.getByTestId('timeline-decoded');
  await expect(decoded).toContainText('Lane decoded (this node)');
  await expect(decoded.getByTestId('timeline-decoded-offset')).toHaveText(/^[+-]\d+(\.\d)? ms after sealed$/);
  await timeline.scrollIntoViewIfNeeded();
  await page.screenshot({ path: path.join(stack.shots, 'e2e-tx-timeline.png'), fullPage: true });
  // end demo(tx-timeline)

  // live off: silence, one fetch per Refresh, and the choice survives a reload
  await page.goto('/#/');
  await expect(page.getByTestId('recent-txs').getByTestId('tx-row').first()).toBeVisible();
  const api = apiLog(page);
  const toggle = page.getByTestId('live-toggle');
  await toggle.click();
  await expect(toggle).toHaveAttribute('aria-pressed', 'false');
  await expect(toggle).toHaveText(/Paused/);
  await expect(page.getByTestId('live-status')).toHaveText(/^Paused, updated \d\d:\d\d:\d\d$/);
  await quiet(page, 500);
  const paused = api.count;
  await quiet(page, 3000);
  expect(api.since(paused), 'no api requests while paused').toEqual([]);

  const statusBefore = await page.getByTestId('live-status').textContent();
  await page.getByTestId('refresh-button').click();
  await quiet(page, 1500);
  expect(api.since(paused).sort(), 'one refresh fetches each home resource once').toEqual(['/api/blocks', '/api/stats', '/api/txs']);
  await quiet(page, 1000);
  expect(await page.getByTestId('live-status').textContent()).not.toBe(statusBefore);
  await quiet(page, 2000);
  expect(api.since(paused)).toHaveLength(3);
  expect(await page.evaluate(() => localStorage.getItem('mcp-explorer.live'))).toBe('off');

  await page.reload();
  await expect(toggle).toHaveAttribute('aria-pressed', 'false');
  await expect(page.getByTestId('live-status')).toHaveText(/^Paused, updated \d\d:\d\d:\d\d$/);
  const reloaded = api.count;
  await quiet(page, 3000);
  expect(api.since(reloaded), 'still paused after reload').toEqual([]);

  await toggle.click();
  await expect(toggle).toHaveAttribute('aria-pressed', 'true');
  const resumed = api.count;
  await quiet(page, 2500);
  expect(api.since(resumed).filter((p) => p === '/api/stats').length).toBeGreaterThanOrEqual(2);
  await page.reload();
  await expect(toggle).toHaveAttribute('aria-pressed', 'true');
  await expect(page.getByTestId('live-status')).toHaveText(/^updated \d\d:\d\d:\d\d$/);
  expect(errors).toEqual([]);
});

test('L6b: a lane with no upcoming leader is refused; any lane leaves lane out', async ({ page }) => {
  await page.goto('/');
  await expect(page.getByTestId('send-lane').locator('option')).toHaveText(['Any lane', 'Lane 0', 'Lane 1', 'Lane 2', 'Lane 3', 'Lane 4']);
  await page.getByTestId('send-payload').fill(`lane 3 ${randomBytes(4).toString('hex')}`);
  await page.getByTestId('send-lane').selectOption('3');
  const refused = page.waitForResponse((r) => r.request().method() === 'POST' && r.url() === `${stack.explorer}/api/tx`);
  await page.getByTestId('send-submit').click();
  expect((await refused).status()).toBe(503);
  const result = page.getByTestId('send-result');
  await expect(result).toHaveAttribute('data-state', 'error');
  await expect(result).toContainText('no proposer on lane 3');

  const payload = `any lane ${randomBytes(4).toString('hex')}`;
  await page.getByTestId('send-payload').fill(payload);
  await page.getByTestId('send-lane').selectOption('');
  const sent = page.waitForResponse((r) => r.request().method() === 'POST' && r.url() === `${stack.explorer}/api/tx`);
  await page.getByTestId('send-submit').click();
  expect((await sent).request().postDataJSON()).toEqual({ payload_utf8: payload, sent_at_ns: expect.any(Number) }); // demo(tx-timeline)
  await expect(result).toHaveAttribute('data-state', 'landed');
});

test('L6b: home rows stay unique when the landing refresh overlaps a live tick', async ({ page }) => {
  // a tx already listed, so later tx refreshes are incremental (after=cursor) merges
  const seed = await fetch(`${stack.rpc}/tx`, { method: 'POST', body: JSON.stringify({ payload_utf8: 'overlap seed' }) });
  expect(seed.status).toBe(200);
  await explorerTx((await seed.json()).tx_hash);
  // slow list responses so the refresh a landing triggers runs while the tick's is still in flight
  await page.route(/\/api\/(blocks|txs)\?/, async (route) => {
    await new Promise((r) => setTimeout(r, 700));
    await route.continue();
  });
  await page.goto('/');
  await expect(page.getByTestId('recent-txs').getByTestId('tx-row').first()).toBeVisible();
  await page.getByTestId('send-payload').fill(`overlap ${randomBytes(4).toString('hex')}`);
  await page.getByTestId('send-submit').click();
  await expect(page.getByTestId('send-result')).toHaveAttribute('data-state', 'landed');
  const dups = { blocks: '', txs: '' };
  for (let until = Date.now() + 3000; Date.now() < until; await quiet(page, 100)) {
    const slots = await page.getByTestId('recent-blocks').getByTestId('block-row').evaluateAll((els) => els.map((e) => e.getAttribute('data-slot')));
    const hashes = await page.getByTestId('recent-txs').getByTestId('tx-row').evaluateAll((els) => els.map((e) => e.getAttribute('data-hash')));
    if (!dups.blocks && new Set(slots).size < slots.length) dups.blocks = slots.join(',');
    if (!dups.txs && new Set(hashes).size < hashes.length) dups.txs = hashes.map((h) => h!.slice(0, 10)).join(',');
  }
  expect(dups, 'recent rows duplicated by overlapping refreshes').toEqual({ blocks: '', txs: '' });
});
