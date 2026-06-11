# Handoff — SEV-SNP attestation track

Context for a Claude Code instance (or engineer) picking up the **attestation**
workstream of the remote-signer PoC. The rest of the PoC is built and validated;
your job is to make the trust in the enclave *verifiable* via SEV-SNP attestation.

## What this project is

Offload a monad validator's secp256k1 + BLS12-381 private keys off the node host
into a **remote signer** running inside an **AMD SEV-SNP confidential VM** on the
same box, reached over `AF_VSOCK`. A root attacker on the host can't read the
CVM's encrypted RAM, so the keys survive host compromise; the local crossing is
fast enough for the hot RaptorCast signing path.

Read `monad-remote-signer/README.md` first — architecture, crates, provisioning
model, build/run. This doc assumes it.

## What's already done (don't redo)

- `monad-remote-signer-proto` — wire protocol, pooled client, vsock+unix transport.
- `monad-secp` / `monad-bls` — `Local | Remote` enums (feature `remote-signer`);
  every caller unchanged; tests pass.
- `monad-signer` (enclave service), `monad-keyloader` (provisioning), `monad-signer-bench`.
- `monad-node` wired with `--remote-signer-vsock-*` flags.
- Validated end-to-end over a unix socket AND over real AF_VSOCK loopback on
  swe-006: provision (decrypt-in-enclave) → secp sign recovers to provisioned
  pubkey, BLS verifies, ECDH matches. Latency over vsock: **p50 ~54µs single-thread,
  ~70k signs/sec saturated**.
- `Op::Attest` exists in the protocol; `src/attest.rs` computes the binding
  `report_data = sha256(nonce || secp_pub || bls_pub)`. The server's `Attest`
  handler calls `attest::attestation_report()`, which currently **returns an
  error** ("attestation unavailable") — that's your entry point.

## Your task

Make `Attest` return a real signed SNP report, and make the keyloader verify it
**before** releasing keys (attested key release). Concretely:

### 1. Stand up a real SEV-SNP guest (ops)
swe-006 has **no `/dev/sev-guest`** (`ls /dev/sev*` is empty) — it's not an SNP
guest. You need a machine with SNP enabled in BIOS + an SNP host kernel, then
boot a confidential guest exposing `/dev/sev-guest`. This is the long pole;
start here. (Coordinate with the team for SNP-capable hardware.)

### 2. Implement the report fetch — `monad-remote-signer/src/attest.rs`
Replace the stub `attestation_report(nonce, pubkeys)`:
- Add the `sev` crate (Linux-only target dep) to `monad-remote-signer/Cargo.toml`.
- Set `report_data = report_data(nonce, pubkeys)` (the binding is already written).
- `sev::firmware::guest::Firmware::open()` → `get_report(None, Some(report_data), 0)`.
- Return the raw report bytes. Keep a non-SNP fallback (the current error) so the
  rest of the stack still builds/runs off-SNP-hardware — gate the real path on a
  `#[cfg(...)]` or a runtime `/dev/sev-guest` existence check.

### 3. Verify on the keyloader — `monad-remote-signer/src/bin/monad-keyloader.rs`
The client method `RemoteSigner::attest(&nonce)` already exists. Add:
- `--attest` and `--expected-measurement <hex>` flags.
- Flow when `--attest`: generate a random 32-byte nonce → `signer.attest(&nonce)`
  → parse the report (`sev::firmware::guest::AttestationReport`) → check
  `report.measurement == expected` AND `report.report_data == sha256(nonce ||
  secp_pub || bls_pub)` (fetch pubkeys via `signer.pubkeys()` after, or have the
  server include them) → only then call `provision`.
- Reject + exit non-zero on any mismatch. Log the measurement.

### 4. Stretch — confidential provisioning channel
Today the bundle (ciphertext + password) crosses plain vsock; a host MITM during
provisioning could capture it. Close it: have the enclave include an ephemeral
ECDH pubkey in `report_data`, the keyloader verify the report, then encrypt the
provision bundle to that attested key. This makes provisioning safe even against
a malicious host. (Design first; it touches the protocol + both ends.)

### 5. Tests / docs
- A mock attestation path (feature-gated) so `cargo test` works without SNP.
- Document the measurement-management / re-provisioning-on-rebuild policy in the
  README (every signer rebuild changes the measurement → the keyloader's
  `--expected-measurement` must be updated; this is real ops).

## Work items checklist (plenty here)
- [ ] SNP-capable host + confidential guest with `/dev/sev-guest`
- [ ] `sev` crate dep + real `attestation_report()`
- [ ] keyloader `--attest` / `--expected-measurement` + verification + gating
- [ ] report parsing, measurement check, report_data binding check
- [ ] VCEK → AMD root cert-chain verification (full trust chain)
- [ ] confidential provisioning channel (ECDH bound in report_data)
- [ ] mock path + tests; measurement-management docs
- [ ] end-to-end demo on the SNP guest: tamper measurement → keyloader refuses

## File boundaries (avoid conflicts with the node-integration track)
You own: `src/attest.rs`, `src/bin/monad-keyloader.rs`, `Cargo.toml` (signer crate
only), README attestation section. **Don't touch** `monad-node/`, `monad-secp/`,
`monad-bls/`, or `src/lib.rs`'s dispatch (the `Attest` arm already calls your fn).
If you need the server to send pubkeys alongside the report, extend the `Attest`
*response payload* (not the request) and update `RemoteSigner::attest` to parse it.

## Build / test / run
```bash
# Linux binaries via Docker (no host toolchain):
./monad-remote-signer/scripts/build-linux.sh        # -> target-linux/release/

# tests (off-SNP):
cargo test -p monad-remote-signer-proto
cargo test -p monad-remote-signer

# on the SNP guest, once /dev/sev-guest exists:
./monad-signer --vsock-port 5005 --generate &
./monad-keyloader --secp ks-secp.json --bls ks-bls.json \
    --vsock-cid <guest-cid> --vsock-port 5005 \
    --attest --expected-measurement <hex>
```

## Protocol reference (from `protocol.rs`)
```
0x05 Attest   req: 32B nonce   resp: SNP report bytes (extend to also carry pubkeys if needed)
```
`report_data = sha256(nonce || secp_pubkey(33B) || bls_pubkey(48B compressed))`.
