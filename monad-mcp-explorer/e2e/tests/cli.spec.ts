import { expect, test } from '@playwright/test';
import { randomBytes } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import type { AddressInfo } from 'node:net';
import path from 'node:path';
import { explorerTx, getJson, hexOf, jsonLines, laneJson, mcpTx, randomSender, run, stack } from './stack';

// the ledger's own record of a tx must agree with the explorer's.
async function expectInLedger(tx: any) {
  const lane = await laneJson(tx.slot, tx.lane);
  expect(lane.slot).toBe(tx.slot);
  expect(lane.index).toBe(tx.lane);
  const hits = lane.txs.filter((t: any) => t.hash === tx.hash);
  expect(hits).toHaveLength(1);
  expect(lane.txs.indexOf(hits[0])).toBe(tx.pos);
  expect(hits[0]).toEqual({
    hash: tx.hash, sender: tx.sender, nonce: tx.nonce, payload: tx.payload, payload_hash: tx.payload_hash,
  });
}

async function expectInBlock(tx: any) {
  const got = await getJson(`${stack.explorer}/api/block/${tx.slot}`);
  expect(got.status).toBe(200);
  const card = got.body.lanes.find((l: any) => l.index === tx.lane);
  expect(card).toMatchObject({ positive: true, decode_error: false });
  expect(card.tx_count).toBeGreaterThan(tx.pos);
  if (tx.pos < card.txs.length) expect(card.txs[tx.pos].hash).toBe(tx.hash);
  else expect(card.more_txs).toBeTruthy();
}

test('L6a: mcp-tx via the rpc x5, each found in the explorer, the rpc and the ledger', async () => {
  const sender = randomSender();
  const nonce = Date.now() * 1000;
  const sentAt = Date.now();
  const r = await mcpTx(['send', '--rpc', stack.rpc, '--sender', sender, '--nonce', String(nonce),
    '--count', '5', '--interval', '20', '--wait', '20']);
  expect(r.code, r.stderr).toBe(0);
  const lines = jsonLines(r.stdout);
  expect(lines).toHaveLength(10);
  const [submits, finals] = [lines.slice(0, 5), lines.slice(5)];
  for (const [i, s] of submits.entries()) {
    expect(s).toMatchObject({ known: false, sender, nonce: nonce + i, payload_len: `mcp-tx ${nonce + i}`.length });
    // over udp the rpc only knows where it sent the tx: a lone validator leads lane 0
    expect(s).toMatchObject({ status: 'sent', leader: 0, target_lane: 0 });
    expect(typeof s.target_slot).toBe('number');
  }
  expect(new Set(submits.map((s) => s.tx_hash)).size).toBe(5);
  expect(finals.map((f) => f.tx_hash)).toEqual(submits.map((s) => s.tx_hash));
  for (const f of finals) expect(f.state).toBe('committed');

  for (const [i, s] of submits.entries()) {
    const hash = s.tx_hash;
    const tx = await explorerTx(hash, Math.max(1, sentAt + 20_000 - Date.now()));
    const rpc = await getJson(`${stack.rpc}/tx/${hash}`);
    expect(rpc.status).toBe(200);
    expect(rpc.body).toMatchObject({ state: 'committed', slot: tx.slot, lane: tx.lane });
    expect(tx).toMatchObject({
      hash, sender, nonce: nonce + i, payload_utf8: `mcp-tx ${nonce + i}`, payload: hexOf(`mcp-tx ${nonce + i}`),
      payload_hash: s.payload_hash, size: s.payload_len, inclusions: [{ slot: tx.slot, lane: tx.lane, pos: tx.pos }],
    });
    const block = await getJson(`${stack.explorer}/api/block/${tx.slot}`);
    expect(rpc.body.path).toBe(block.body.path);
    expect(rpc.body.finalized_at_ms).toBe(block.body.finalized_at_ms);
    await expectInLedger(tx);
    await expectInBlock(tx);

    const bare = await getJson(`${stack.explorer}/api/tx/${hash.slice(2).toUpperCase()}`);
    expect(bare.body.hash).toBe(hash);
    expect((await getJson(`${stack.explorer}/api/search?q=${hash}`)).body).toEqual({ kind: 'tx', id: hash });
  }

  const bySender = await getJson(`${stack.explorer}/api/sender/${sender}?limit=100`);
  expect(bySender.body.tx_count).toBe(5);
  expect(new Set(bySender.body.txs.map((t: any) => t.hash))).toEqual(new Set(submits.map((s) => s.tx_hash)));
});

test('L6c: a direct udp send to the node shows up in the explorer, bypassing the rpc', async () => {
  const sender = randomSender();
  const nonce = 7;
  const payload = `l6c via udp ${randomBytes(4).toString('hex')}`;
  const args = ['send', '--node', stack.node, '--sender-id', '0', '--sender', sender, '--nonce', String(nonce), '--payload', payload];
  const r = await mcpTx(args);
  expect(r.code, r.stderr).toBe(0);
  const [reply] = jsonLines(r.stdout);
  expect(reply).toMatchObject({ status: 'sent', node: stack.node, sender_id: 0 });

  const tx = await explorerTx(reply.tx_hash);
  expect(tx).toMatchObject({ hash: reply.tx_hash, sender, nonce, payload_utf8: payload, payload: hexOf(payload) });
  await expectInLedger(tx);
  await expectInBlock(tx);
  expect((await getJson(`${stack.rpc}/tx/${reply.tx_hash}`)).status).toBe(404);

  // no reply over udp: a second send goes out, and the node keeps the tx once
  const again = await mcpTx(args);
  expect(again.code, again.stderr).toBe(0);
  expect(jsonLines(again.stdout)[0]).toMatchObject({ tx_hash: reply.tx_hash, status: 'sent' });
  await new Promise((r) => setTimeout(r, 2_000));
  const after = await getJson(`${stack.explorer}/api/tx/${reply.tx_hash}`);
  expect(after.body.inclusions).toMatchObject([{ slot: tx.slot, lane: tx.lane, pos: tx.pos }]); // demo(tx-timeline)
  expect(after.body.inclusions).toHaveLength(1); // demo(tx-timeline)
  // demo(tx-timeline): a udp send skips the rpc, so there is no received time and no mempool phase
  const phases = after.body.inclusions[0].phases.map((p: any) => p.name);
  expect(phases).not.toContain('mempool');
  expect(phases[0]).toBe('proposing');
  expect(phases[phases.length - 1]).toBe('finalizing');
});

test('L6d: a 1025-byte payload is refused by mcp-tx before connecting, and by the rpc', async () => {
  let connections = 0;
  const trap = createServer((_, res) => res.end('{}'));
  trap.on('connection', () => connections++);
  await new Promise<void>((r) => trap.listen(0, '127.0.0.1', r));
  const trapUrl = `http://127.0.0.1:${(trap.address() as AddressInfo).port}`;
  const refusal = 'mcp-tx: payload is 1025 bytes, over the 1024-byte limit (MAX_TX_PAYLOAD); not sending';
  const big = 'x'.repeat(1025);
  const file = path.join(test.info().outputDir, 'big.bin');
  await mkdir(path.dirname(file), { recursive: true });
  await writeFile(file, Buffer.alloc(1025, 0xab));
  try {
    for (const body of [['--payload', big], ['--payload-hex', hexOf(Buffer.alloc(1025, 1))], ['--payload-file', file]]) {
      for (const target of [['--rpc', trapUrl], ['--node', stack.node, '--sender-id', '0']]) {
        const r = await mcpTx(['send', ...target, ...body]);
        expect(r.code, `${target[0]} ${body[0]}`).toBe(1);
        expect(r.stderr.trim()).toBe(refusal);
        expect(r.stdout).toBe('');
      }
    }
    expect(connections).toBe(0);
  } finally {
    trap.close();
  }

  const health = async () => (await getJson(`${stack.rpc}/health`)).body.tracked;
  const trackedBefore = await health();
  const curl = (json: string) => run('curl', ['-sS', '-o', '-', '-w', '\n%{http_code}', '-X', 'POST',
    '-H', 'content-type: application/json', '--data-binary', json, `${stack.rpc}/tx`]);
  for (const json of [JSON.stringify({ payload_utf8: big }), JSON.stringify({ payload_hex: hexOf(Buffer.alloc(1025, 2)) })]) {
    const r = await curl(json);
    expect(r.code, r.stderr).toBe(0);
    const [body, code] = r.stdout.split('\n');
    expect(code).toBe('413');
    expect(JSON.parse(body).error).toMatch(/1024/);
  }
  expect(await health()).toBe(trackedBefore);

  // the boundary itself goes through and commits
  const max = 'y'.repeat(1024);
  const ok = await curl(JSON.stringify({ payload_utf8: max }));
  const [okBody, okCode] = ok.stdout.split('\n');
  expect(okCode).toBe('200');
  const tx = await explorerTx(JSON.parse(okBody).tx_hash);
  expect(tx).toMatchObject({ size: 1024, payload_utf8: max });
  await expectInLedger(tx);
});
