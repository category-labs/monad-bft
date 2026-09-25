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
  await page.getByTestId('send-submit').click();

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
  const card = page.locator(`[data-testid="lane-card"][data-lane-index="${lane}"]`);
  await expect(card).toHaveAttribute('data-positive', 'true');
  expect(Number(await card.getAttribute('data-tx-count'))).toBeGreaterThanOrEqual(1);
  await expect(card.getByTestId('lane-proposer')).toHaveText('0');
  await expect(card.getByTestId('lane-root')).toHaveAttribute('title', /^0x[0-9a-f]{40}$/);
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
