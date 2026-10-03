import { expect, test } from '@playwright/test';
import { randomBytes } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { blockDir, explorerTx, getJson, randomSender, stack, waitFor } from './stack';

// run.sh --swarm: N validators, rpc (colocated with node 0) and explorer on node 0
const nodes = Number(process.env.E2E_SWARM);
const SLOT_MS = Number(process.env.E2E_SLOT_MS);

async function meta(slot: number, ledger = stack.ledger): Promise<any | undefined> {
  try {
    return JSON.parse(await readFile(path.join(blockDir(slot, ledger), 'meta.json'), 'utf8'));
  } catch {
    return undefined;
  }
}

const finalized = (slot: number, within = 15_000) =>
  waitFor(`node 0 to finalize slot ${slot}`, within, () => meta(slot)).catch(() => undefined);

// proposers per lane index as the ledger recorded them from the schedule
const proposersOf = (m: any): (number | null)[] => m.lanes.map((l: any) => l.proposer);

// the rule the rpc must follow, recomputed from the ledger: at `target`, the
// proposer keeping its lane for the most slots (up to `horizon`), ties to the lower lane
async function mostTenured(target: number, horizon: number): Promise<{ lane: number; leader: number } | undefined> {
  const sets: (number | null)[][] = [];
  for (let k = 0; k < horizon; k++) {
    const m = await finalized(target + k);
    if (!m) return undefined;
    sets.push(proposersOf(m));
  }
  let best: { lane: number; leader: number; run: number } | undefined;
  for (const [lane, leader] of sets[0].entries()) {
    if (leader === null) continue;
    let run = 0;
    while (run < horizon && sets[run][lane] === leader) run++;
    if (!best || run > best.run) best = { lane, leader, run };
  }
  return best && { lane: best.lane, leader: best.leader };
}

// run.sh --latency: node 0's RTT row and the rpc's min tenure; unset = route by tenure
const rttRow = process.env.E2E_LATENCY_ROW?.split(',').map(Number);
const minTenure = Number(process.env.E2E_MIN_TENURE ?? 2);

// with a matrix: the nearest proposer keeping its lane >= minTenure slots (ties: more
// tenure, then the lower lane); none qualifies -> the most tenured
async function nearest(target: number, horizon: number): Promise<{ lane: number; leader: number } | undefined> {
  const sets: (number | null)[][] = [];
  for (let k = 0; k < horizon; k++) {
    const m = await finalized(target + k);
    if (!m) return undefined;
    sets.push(proposersOf(m));
  }
  let best: { lane: number; leader: number; run: number; rtt: number } | undefined;
  for (const [lane, leader] of sets[0].entries()) {
    if (leader === null) continue;
    let run = 0;
    while (run < horizon && sets[run][lane] === leader) run++;
    if (run < minTenure) continue;
    const rtt = rttRow![leader];
    if (!best || rtt < best.rtt || (rtt === best.rtt && run > best.run)) best = { lane, leader, run, rtt };
  }
  return best ? { lane: best.lane, leader: best.leader } : mostTenured(target, horizon);
}

interface Sent {
  i: number;
  hash: string;
  target: number;
  leader: number;
  lane: number;
}

interface Landed extends Sent {
  slot: number;
  landedLane: number;
  landedProposer: number | null;
  attempts: number;
  history: any[];
}

test('swarm: the rpc sends each tx to the proposer its routing rule picks, and it lands in that lane', async ({ page }) => {
  expect(nodes, 'run through run.sh --swarm').toBeGreaterThanOrEqual(2);
  expect(SLOT_MS, 'E2E_SLOT_MS from run.sh').toBeGreaterThan(0);
  const health = (await getJson(`${stack.rpc}/health`)).body;
  const schedule = health.schedule;
  expect(schedule, `the rpc reports the schedule it routes with: ${JSON.stringify(health)}`).toBeTruthy();
  const { concurrent_proposers: K, observation_cutoff: y, rotation_slack: z, horizon } = schedule;
  expect(horizon).toBe(K * (y + z));
  // a full cycle: every lane handed over once
  const cycle = K * (y + z);
  const span = cycle + (y + z);
  const sendMs = 3 * span * SLOT_MS;
  test.setTimeout(sendMs + (horizon + 60) * SLOT_MS + 120_000);
  console.log(`K=${K} y=${y} z=${z} nodes=${nodes}: sending one tx per slot until the targets span ${span} slots`);

  const sender = randomSender();
  const tag = randomBytes(4).toString('hex');
  const start = Date.now();
  const sent: Sent[] = [];
  for (let i = 0; sent.length === 0 || sent[sent.length - 1].target - sent[0].target < span; i++) {
    expect(Date.now() - start, `targets spanned ${sent[0]?.target}..${sent[sent.length - 1]?.target} after ${i} txs`).toBeLessThan(sendMs);
    const wait = start + i * SLOT_MS - Date.now();
    if (wait > 0) await new Promise((r) => setTimeout(r, wait));
    const res = await fetch(`${stack.rpc}/tx`, {
      method: 'POST',
      body: JSON.stringify({ sender, nonce: i, payload_utf8: `swarm ${tag} ${i}` }),
    });
    const body: any = await res.json();
    expect(res.status, JSON.stringify(body)).toBe(200);
    expect(body.status, JSON.stringify(body)).toBe('sent');
    for (const field of ['target_slot', 'leader', 'target_lane']) {
      expect(typeof body[field], `${field} in ${JSON.stringify(body)}`).toBe('number');
    }
    sent.push({ i, hash: body.tx_hash, target: body.target_slot, leader: body.leader, lane: body.target_lane });
  }
  const sendEnd = Date.now();

  // every tx commits, per the rpc following node 0's ledger
  const landed: Landed[] = [];
  for (const s of sent) {
    const view = await waitFor(`tx ${s.i} to commit`, Math.max(1, sendEnd + 20_000 - Date.now()), async () => {
      const got = await getJson(`${stack.rpc}/tx/${s.hash}`);
      return got.body?.state === 'committed' ? got.body : undefined;
    });
    const at = await meta(view.slot);
    landed.push({
      ...s,
      slot: view.slot,
      landedLane: view.lane,
      landedProposer: at.lanes[view.lane].proposer,
      attempts: view.attempts,
      history: view.history,
    });
  }
  for (const t of landed) {
    console.log(`tx ${t.i}: target ${t.target} -> leader ${t.leader} lane ${t.lane}; ` +
      `landed slot ${t.slot} lane ${t.landedLane} proposer ${t.landedProposer} attempts=${t.attempts}`);
  }

  // exactly the reported proposer and lane took the tx; a resent tx may land from any of its sends
  const strayed = landed.filter((t) => {
    const sends = t.attempts === 1 ? [{ leader: t.leader, target_lane: t.lane }] : t.history;
    return !sends.some((a: any) => a.leader === t.landedProposer && a.target_lane === t.landedLane);
  });
  expect(strayed.map((t) => `tx ${t.i}: sent to ${t.leader} lane ${t.lane}, landed at ${t.landedProposer} lane ${t.landedLane}`),
    'txs that landed in a lane the rpc did not send them to').toEqual([]);
  expect(landed.filter((t) => t.attempts === 1).length, 'txs committed from their first send').toBeGreaterThan(landed.length / 2);

  // the reported leader is the one the routing rule picks at the reported target, per the ledger
  const misrouted: string[] = [];
  for (const t of landed) {
    const rule = rttRow ? await nearest(t.target, horizon) : await mostTenured(t.target, horizon);
    if (!rule) {
      misrouted.push(`tx ${t.i}: node 0 has not finalized ${t.target}..${t.target + horizon - 1}`);
    } else if (rule.leader !== t.leader || rule.lane !== t.lane) {
      misrouted.push(`tx ${t.i}: target ${t.target} sent to ${t.leader} lane ${t.lane}, rule says ${rule.leader} lane ${rule.lane}`);
    }
  }
  expect(misrouted, `leaders the ${rttRow ? 'nearest' : 'most tenured'} rule does not pick at their target`).toEqual([]);

  // over a cycle the choice moves between proposers
  const lanes = new Set(landed.map((t) => t.landedLane));
  const proposers = new Set(landed.map((t) => t.landedProposer));
  expect(lanes.size, `lanes ${[...lanes]}`).toBeGreaterThanOrEqual(2);
  expect(proposers.size, `proposers ${[...proposers]}`).toBeGreaterThanOrEqual(2);

  // every node finalized the same blocks over the run, empty ones included: lane proposers, roots and payload bytes
  const lo = Math.min(...landed.map((t) => t.slot));
  const hi = Math.max(...landed.map((t) => t.slot));
  const slots = Array.from({ length: hi - lo + 1 }, (_, k) => lo + k);
  const disagreements: string[] = [];
  const localDiffs: string[] = [];
  for (const slot of slots) {
    const ref = await finalized(slot);
    if (!ref) {
      disagreements.push(`slot ${slot}: node 0 has no block`);
      continue;
    }
    for (const [n, ledger] of stack.ledgers.entries()) {
      if (n === 0) continue;
      const other = await waitFor(`node ${n} to finalize slot ${slot}`, 15_000, () => meta(slot, ledger)).catch(() => undefined);
      if (!other) {
        disagreements.push(`slot ${slot}: node ${n} has no block`);
        continue;
      }
      const consensus = (m: any) => ({ slot: m.slot, num_lanes: m.num_lanes, tx_count: m.tx_count, lanes: m.lanes });
      if (JSON.stringify(consensus(other)) !== JSON.stringify(consensus(ref))) {
        disagreements.push(`slot ${slot}: node ${n} meta ${JSON.stringify(consensus(other))} != node 0 ${JSON.stringify(consensus(ref))}`);
      }
      for (const l of ref.lanes.filter((l: any) => l.root !== null)) {
        const file = `lane-${l.index}.rlp`;
        const [a, b] = await Promise.all([readFile(path.join(blockDir(slot), file)), readFile(path.join(blockDir(slot, ledger), file)).catch(() => undefined)]);
        if (!b || !a.equals(b)) disagreements.push(`slot ${slot}: node ${n} ${file} ${b ? 'differs' : 'missing'}`);
      }
      // node-local views: how this node finalized and which deadline it knew
      if (other.path !== ref.path || other.deadline_ns !== ref.deadline_ns) {
        localDiffs.push(`slot ${slot}: node ${n} path=${other.path} deadline_ns=${other.deadline_ns} vs node 0 path=${ref.path} deadline_ns=${ref.deadline_ns}`);
      }
    }
  }
  if (localDiffs.length) console.log(`node-local meta differences (not asserted):\n${localDiffs.join('\n')}`);
  expect(disagreements, `ledgers of ${nodes} nodes agree on ${slots.length} slots`).toEqual([]);

  // the explorer on node 0 shows a tx in the lane card of the proposer the rpc chose
  const shown = landed.find((t) => t.attempts === 1 && t.landedProposer !== 0) ?? landed[0];
  await explorerTx(shown.hash);
  await page.goto(`/#/block/${shown.slot}`);
  await expect(page.getByTestId('block-detail')).toHaveAttribute('data-slot', String(shown.slot));
  const card = page.locator(`[data-testid="lane-card"][data-lane-index="${shown.landedLane}"]`);
  await expect(card).toHaveAttribute('data-positive', 'true');
  await expect(card.getByTestId('lane-proposer')).toHaveText(String(shown.landedProposer));
  await expect(card.locator(`[data-testid="lane-tx-row"][data-hash="${shown.hash}"]`)).toBeVisible();
  await card.scrollIntoViewIfNeeded();
  await page.screenshot({ path: path.join(stack.shots, 'udp-swarm-block.png'), fullPage: true });

  await page.goto('/');
  await expect(page.getByTestId('head-slot')).toHaveText(/^slot \d+$/);
  await expect(page.getByTestId('recent-txs').getByTestId('tx-row').first()).toBeVisible();
  await page.screenshot({ path: path.join(stack.shots, 'udp-swarm-home.png'), fullPage: true });
});
