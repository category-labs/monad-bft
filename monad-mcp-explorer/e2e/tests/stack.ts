import { execFile } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import path from 'node:path';

function env(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`${name} is unset; run the suites through e2e/run.sh`);
  return v;
}

export const stack = {
  bin: env('E2E_BIN_DIR'),
  rpc: env('E2E_RPC_URL'),
  explorer: env('E2E_EXPLORER_URL'),
  // node 0's udp address; the rpc is colocated with it
  node: env('E2E_NODE_ADDR'),
  ledger: env('E2E_LEDGER_DIR'),
  // one per node, node 0 first; node 0's is `ledger`
  ledgers: env('E2E_LEDGER_DIRS').split(':'),
  shots: env('E2E_SCREENSHOT_DIR'),
};

export interface Run {
  code: number;
  stdout: string;
  stderr: string;
}

export function run(file: string, args: string[], timeoutMs = 60_000): Promise<Run> {
  return new Promise((resolve) => {
    execFile(file, args, { timeout: timeoutMs, maxBuffer: 1 << 24 }, (err, stdout, stderr) => {
      const code = err ? (typeof err.code === 'number' ? err.code : -1) : 0;
      resolve({ code, stdout, stderr });
    });
  });
}

export const mcpTx = (args: string[]) => run(path.join(stack.bin, 'mcp-tx'), args);

export const jsonLines = (out: string): any[] =>
  out.split('\n').filter((l) => l.trim()).map((l) => JSON.parse(l));

export interface Got {
  status: number;
  body: any;
}

export async function getJson(url: string): Promise<Got> {
  const res = await fetch(url, { cache: 'no-store' });
  const text = await res.text();
  let body: any = null;
  try { body = JSON.parse(text); } catch { body = text; }
  return { status: res.status, body };
}

// polls until `probe` returns a value, or throws with the last observation.
export async function waitFor<T>(what: string, timeoutMs: number, probe: () => Promise<T | undefined>): Promise<T> {
  const until = Date.now() + timeoutMs;
  let last: unknown;
  for (;;) {
    try {
      const v = await probe();
      if (v !== undefined) return v;
    } catch (e) {
      last = e;
    }
    if (Date.now() > until) throw new Error(`timed out after ${timeoutMs} ms waiting for ${what}${last ? `: ${last}` : ''}`);
    await new Promise((r) => setTimeout(r, 100));
  }
}

// the explorer's view of a tx once indexed.
export function explorerTx(hash: string, timeoutMs = 20_000): Promise<any> {
  return waitFor(`explorer to index ${hash}`, timeoutMs, async () => {
    const got = await getJson(`${stack.explorer}/api/tx/${hash}`);
    if (got.status === 200) return got.body;
    if (got.status !== 404) throw new Error(`/api/tx answered ${got.status}: ${JSON.stringify(got.body)}`);
    return undefined;
  });
}

export const blockDir = (slot: number, ledger = stack.ledger) =>
  path.join(ledger, 'blocks', String(slot).padStart(12, '0'));

export async function laneJson(slot: number, lane: number): Promise<any> {
  return JSON.parse(await readFile(path.join(blockDir(slot), `lane-${lane}.json`), 'utf8'));
}

export const randomSender = () => '0x' + randomBytes(20).toString('hex');
export const hexOf = (s: string | Buffer) => '0x' + Buffer.from(s).toString('hex');
