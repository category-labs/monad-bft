# monad-remote-signer

Offload a validator's secp256k1 + BLS12-381 private keys off the node host into
a **remote signer** — designed for an **AMD SEV-SNP confidential VM** on the same
physical box, reached over `AF_VSOCK`. A compromised host (root / hypervisor)
cannot read the CVM's encrypted memory, so the keys survive host compromise,
while the local crossing is fast enough for the hot RaptorCast signing path
(unlike a network remote signer, which RTT rules out).

## Why this shape

Every signing caller in monad-bft bottoms out in methods on the concrete
`monad_secp::KeyPair` / `monad_bls::BlsKeyPair`:

- RaptorCast per-merkle-batch header — `monad-raptorcast/.../regular.rs`
- consensus votes — `monad-state`
- wireauth ECDH — `monad-wireauth/.../crypto.rs` (calls `KeyPair::ecdh` directly)

So both key types became `Local | Remote` enums (feature `remote-signer`). The
`Remote` variant holds only the public key + a connection; `sign`/`ecdh`/`pubkey`
dispatch to the signer. **Every existing caller compiles unchanged.**

The node hashes (`secp`) / prefixes (`bls`) host-side and sends only the digest /
prefixed message, so the enclave is **domain-agnostic**: it holds keys and signs
opaque blobs, nothing more. Smallest possible trusted surface.

> Caveat: because the host hands the enclave pre-hashed blobs, SNP protects
> against **key extraction**, not against a compromised host requesting a
> signature. Tightening that (moving message construction into the enclave) is
> future work.

## Crates / binaries

- `monad-remote-signer-proto` — wire protocol, pooled client, transport
  (vsock on Linux, unix socket for dev). No `monad-secp` dep (avoids a cycle).
- `monad-signer` — the enclave service. Keys live in RAM only.
- `monad-keyloader` — one-time provisioning client.
- `monad-signer-bench` — hot-path throughput/latency benchmark.

## Provisioning model (keyloader)

```
1. operator: monad-keyloader --secp ks-secp.json --bls ks-bls.json --password … --vsock-cid N --vsock-port 5005
2. keyloader --> enclave:  Provision { ciphertext + password }
3. enclave decrypts INSIDE protected memory  → keys held in RAM, returns pubkeys
4. operator: shred the keystore files on the host
5. node starts with --remote-signer-*  → no key material on the host
```

The keyloader forwards **ciphertext + password**; decryption happens inside the
enclave, so the plaintext secret never exists in host RAM. Keys are ephemeral
(RAM only) — SNP encrypts RAM, not disk; re-provision on enclave reboot.

## Build (Linux, via Docker — no host toolchain)

```bash
./monad-remote-signer/scripts/build-linux.sh   # -> target-linux/release/{monad-signer,monad-keyloader,monad-signer-bench}
```

## Run + benchmark over real vsock loopback (single host)

```bash
# on the Linux host, with the three binaries present:
./monad-remote-signer/scripts/run-vsock-loopback.sh
```

Dev (no Linux/vsock): use `--unix /tmp/signer.sock` on both server and client.

## Node integration

```bash
monad-node … --remote-signer-vsock-cid <guest-cid> --remote-signer-vsock-port 5005
# (omit --secp-identity/--bls-identity keystores; keys come from the signer)
```

## Attestation (stretch)

`src/attest.rs` computes the provisioning binding `sha256(nonce || secp_pub ||
bls_pub)`. Fetching a signed SNP report from `/dev/sev-guest` (via the `sev`
crate) so the keyloader can verify the measurement *before* releasing the key is
the documented TODO — it needs a real SEV-SNP guest.

## Tests

```bash
cargo test -p monad-remote-signer-proto
cargo test -p monad-remote-signer            # incl. e2e over a unix socket
```
