# Adaptive Proposal Lead Time (proposal planner)

Status: design of the adaptive proposal planner. The implementation is off
by default (`[proposal.adaptive]` in the node config; how to run it:
`monad-mcp-node/docs/adaptive-lead.md`). Code references are to `mcp-dev`.
The evaluation combines a standalone Monte Carlo model of the timing in §3
(§7.1–§7.11) with the node itself in the simulation harness (§7.12).
Appendix B lists background literature.

The proposal planner (`monad-mcp-cadence/src/proposing.rs`) decides when a
proposer seals its proposal and hands it to DA. Today that is a fixed
`lead` before the slot deadline. This document proposes four changes:

1. **Adaptive lead.** Replace the fixed lead with a per-proposer,
   self-calibrating one. The proposer models, per validator, how long its
   chunk share needs to arrive before that validator's deadline. It learns
   this from two signals it already receives: the arrival times of everyone's
   batch votes, in every slot, and the votes on its own proposals. It then
   picks the latest seal time for which the probability that its own lane
   splits the fast path stays below a configured bound.
2. **Send order.** Send the shares in a latency-aware order.
3. **Uplink priority.** Give the proposal's own shares priority over all
   other protocol traffic on the node's uplink.
4. **Model-based withhold.** Replace the fixed withhold window with a rule
   derived from the same model.

Consensus is unchanged. The planner stays outside the consensus core: a bad
lead costs a proposal or, at worst, one slot's fast path. It never costs
safety.

## Notation

All times are in milliseconds and all stake is normalized: stake fractions
sum to 1. Every validator is assumed to have at least 1 Gb/s of bandwidth in
each direction, the requirement planned for the Cadence rollout (§6.7).
Symbols are listed in order of first use.

| symbol | meaning |
|---|---|
| $s$ | slot |
| $j$ | lane (a proposer's slot in the per-slot proposal set; formerly "proposal index"), $j \in \{0, \dots, K-1\}$ |
| $K$ | concurrent proposals per slot (deployed: 5) |
| $\mathcal{V}$, $n$ | validator set, $n = \lvert\mathcal{V}\rvert$ |
| $p$ | the proposer this document is about (the node running the planner) |
| $v$ | any other validator, $v \in \mathcal{V} \setminus \{p\}$ |
| $w_v$ | stake fraction of $v$; $\sum_{v \in \mathcal{V}} w_v = 1$ |
| $f$ | bound on Byzantine stake, $f < 1/3$ |
| $D_s$ | deadline of slot $s$ (agreed value; each node acts on its own clock) |
| $\tau$ | slot interval, $D_{s+1} - D_s$ (deployed: 100 ms) |
| $\Delta$ | network delay bound of the protocol (deployed: 150 ms) |
| $o_v$ | clock offset of $v$: local time = true time + $o_v$ |
| $L$ | lead: $p$ seals at its local time $D_s - L$ |
| $P$ | payload size in bytes |
| $\kappa$ | coded expansion: bytes sent on the first hop per payload byte (§6.7) |
| $B$ | uplink rate of $p$ in bytes per ms |
| $e(P)$ | encode time |
| $\pi$ | send order: a permutation of $\mathcal{V} \setminus \{p\}$ |
| $u_v(P, \pi)$ | time from the end of encoding until $v$'s share has left $p$'s uplink |
| $d_{p\to v}$ | one-way network latency from $p$ to $v$ in this slot, jitter included |
| $R_v$ | required lead: the smallest $L$ for which $v$ votes yes (§3) |
| $k_v$ | number of chunks of $p$'s proposal assigned to $v$ (author-routed, stake-proportional) |
| $Y_v$ | $v$'s vote on $p$'s lane: 1 = yes (positive entry), 0 = no |
| $N$ | negative stake on $p$'s lane: $N = \sum_{v} w_v (1 - Y_v)$ |
| $\theta$, $\varepsilon$ | target bound on $N$, and the accepted risk of exceeding it (§2) |
| $m_v$ | arrival time of $v$'s batch vote at $p$, relative to $p$'s local $D_s$ (§4.2) |
| $c_v$ | $v$'s processing time between its deadline and sending its vote |
| $\mu_v$, $\delta_v$ | running mean and mean absolute deviation of $m_v$ (§5.1) |
| $b_v$ | vote-calibrated correction of $v$'s estimate (§5.1) |
| $\hat r_v$ | estimated latency-plus-offset term of $R_v$: $\hat r_v = \mu_v + b_v$ |
| $\sigma_v$ | estimated spread of $R_v$ |
| $\hat R_v$ | predicted required lead (§5.2) |
| $\hat q_v(L)$ | predicted probability that $v$ votes no at lead $L$ |
| $M(L)$, $V(L)$ | predicted mean and variance of $N$ at lead $L$ |
| $\rho$ | reserve: extra negative stake held back for unpredictable no-voters |
| $\Phi$, $\varphi$ | standard normal CDF and density |
| $z_{1-\varepsilon}$ | $\Phi^{-1}(1-\varepsilon)$, e.g. 3.09 for $\varepsilon = 10^{-3}$ |
| $\lambda(x)$ | inverse Mills ratio, $\varphi(x) / (1 - \Phi(x))$ |
| $\alpha$, $\eta$ | learning rates of the passive estimate and of the vote calibration |
| $Q(L)$ | queueing delay of $p$'s shares behind other traffic on $p$'s uplink, if not prioritized (§5.6) |

## 1. Background

### 1.1 The slot timeline

The proposer of lane $j$ seals at its local $D_s - L$. The node hands the
payload to DA (`monad-mcp-node/src/router.rs`), which encodes it with
the d33 scheme (`monad-mcp-da/src/runtime.rs`, `encoding_scheme/d33.rs`,
`wire/proposal.rs`). The node then unicasts each validator its share, back to back over
its uplink (`monad-mcp-node/src/router.rs`). Each chunk is one UDP datagram of
at most 1472 bytes (`monad-mcp-node/src/network/stub.rs`,
`MAX_FRAME_LEN`). Encoding runs on the DA task, so it can queue behind other
DA work; that wait is part of $e(P)$.

At its local $D_s$ every validator votes. A fallback happens only on a split
(§1.3): a fallback vote at $D_s + \Delta$, and MVBA from $D_s + 2\Delta$.

### 1.2 What a positive vote means

At its **local** $D_s$, every validator broadcasts one batch vote with one
entry per lane (`slot/fast.rs`, `on_deadline`). The entry for $j$ is
`Positive(pid)`, carrying the proposal id (a commitment to the whole header),
iff the validator holds at least $\lceil 10\,k_v / 11 \rceil$ of the $k_v$
chunks assigned to it, before that instant:

- `availability.rs` `fetch_proposal` returns the proposal once
  `ProposerObligationFulfilled` has fired;
- `monad-mcp-da/src/instance_rc/mod.rs` emits that event, and
  `obligation_tracker.rs` trims the obligation by the scheme's
  packet-loss resistance (11/10, `PACKET_LOSS_RESISTANCE` in `d33.rs`).

So a validator tolerates $\lfloor k_v/11 \rfloor$ missing chunks: none with
up to 10 assigned chunks, one with 11–21, and so on. At $n = 190$ and
400 KB an average-stake validator has 7 chunks and still needs all of them;
large-stake validators and large payloads have slack. Deployed proposals come
from the mempool and are at most 64 KiB, which at $n = 190$ gives every
validator a single chunk. Under d33 every staked validator holds at least
one chunk: with $S$ shares in total and $(q, r) = \operatorname{divmod}(w_v S, W)$,
$k_v = q + [r > 0]$. A decoded proposal also counts as fulfilled. Before $D_s$ the
only source of these chunks is the proposer (pull-based recovery starts at
$D_s + \Delta$), so the rule is a first-hop rule. The paper states the
all-chunks version (`alg_da.tex:33`, `alg_voting.tex:11-18`).

Decoding is not required. Owners rebroadcast only after $D_s$
(`egress.rs`), so the time that matters is one hop: encoding, upload, wire,
and the voter's clock. The vote is cast once; a share that arrives later
does not flip it. A validator that sees a fast block for the slot before its
local $D_s$ casts no batch vote at all (`fast.rs`), so a missing vote
is not a no.

### 1.3 Outcomes per lane

A strong QC needs stake strictly above $2/3$ and a weak QC strictly above
$1/3$ (`types.rs`, on integer stake). In terms of the negative stake
$N$ on lane $j$:

| negative stake $N$ | outcome |
|---|---|
| $N < 1/3$ | strong QC on the positive entry: the lane's proposal is included, fast path |
| $N > 2/3$ | strong QC on the negative entry: the lane finalizes empty, the **slot stays fast** |
| $1/3 \le N \le 2/3$ | **split**: no fast QC for $j$, so the **whole slot** falls back |

The fast block needs a strong QC for *every* lane (`fast.rs`). A
split on one proposer's lane therefore costs every proposer of the slot the
fast path. In the fallback the proposal is likely still included: a FallbackQC
needs more than $1/3$ positive stake plus decoding. The splitting proposer
itself loses little.

### 1.4 Accountability (paper)

`p1_practical_considerations.tex:10-47`:

- When the fast path fails, the split votes on the offending lane form a
  transferable proof of partial dissemination. With at most $f/2$ Byzantine
  validators, $f/2 + 1$ signed yes plus $f/2 + 1$ signed no suffice.
- "A proposer that provably and repeatedly partially disseminates can be
  penalized."
- Accountability is tied to **forcing the fallback**. A proposer whose lane
  still gets a fast QC did nothing wrong, even if many validators voted no.

So the protocol's intent is that proposers avoid causing the fallback.
Nothing in `mcp-dev` collects or acts on such proofs yet. This document
therefore takes a high probability of the fast path as the objective (§2),
and treats any stricter reading as a parameter choice (§6).

### 1.5 The current planner

`PlannerConfig` has three fields:

- `lead`: a fixed $L$;
- `min_lead`: a withhold window; a seal closer than this to $D_s$ is dropped;
- `observation_cutoff`: the chaining gate.

Deployed values (`deploy/config/*/node.toml`):

- `propose_before_deadline = 150` ms, i.e. $L$;
- `withhold_before_deadline = 100` ms;
- $\Delta = 150$ ms, $\tau = 100$ ms, $K = 5$.

Bandwidth: mainnet requires 300 Mb/s today. This document assumes the 1 Gb/s
minimum planned for the Cadence rollout (§6.7).

The module docs already name this the seam for "adaptive leads from DA
feedback" (`proposing.rs`).

A fixed lead cannot be right for everyone. In the model at $n = 190$ (§7),
the lead that just meets the same target varies by more than 100 ms:

- by proposer location: 92 ms for the German testnet host (`deu`), 190 ms
  for São Paulo;
- by payload size and current routes.

Relative to that, 150 ms is:

- about 15 ms too early for Frankfurt and New York, and 56 ms too early for
  `deu`;
- too late for São Paulo: 5.8% split slots;
- too late with 1 MB payloads from Singapore: 12% split slots.

## 2. Problem statement

For each own slot, choose the lead as small as possible, i.e. the seal time
$D_s - L$ as late as possible, subject to

```math
\Pr\left[\, N > \theta \,\right] \;\le\; \varepsilon .
```

The bound $\theta < 1/3$ keeps a margin below the split threshold;
$\varepsilon$ is the accepted risk per own slot. The defaults proposed in
§6.1 are $\theta = 1/4$ and $\varepsilon = 10^{-3}$.

Sealing late is the proposer's interest: a fresher payload. The constraint
is the system's interest: no split.

The planner must:

- **approach the optimum from the safe side.** Overshooting costs the slot.
- **work when the proposer rotates in.** A proposer holds its lane for
  about 495 slots (rotation length $y + z = 100$ slots, held for $K$
  rotations, minus the vacancy), and is off duty for about 18,500 slots at
  $n = 190$.
- **work for $n \ge 100$**, optimally at the mainnet size of 180–200. It must
  still work, less optimally, for small $n$ such as the 8-node testnet.

## 3. Timing model

$p$ seals at its local $D_s - L$, i.e. at true time $D_s - L - o_p$. The
part of $v$'s share that $v$ needs (§1.2) arrives at true time
$D_s - L - o_p + e(P) + u_v(P, \pi) + d_{p\to v}$, with $u_v$ the time until
that part has left $p$'s uplink. $v$ votes at its local $D_s$, i.e. at true
time $D_s - o_v$. So $v$ votes yes iff that part arrives no later than that:

```math
Y_v = 1 \iff L \ge R_v, \qquad
R_v = e(P) + u_v(P,\pi) + d_{p\to v} + (o_v - o_p).
```

With shares sent back to back in order $\pi$, stake-proportional and
normalized over $\mathcal{V} \setminus \{p\}$:

```math
u_v(P,\pi) \;=\; \frac{\kappa P}{B} \cdot \frac{\sum_{v' \preceq_\pi v} w_{v'}}{1 - w_p}
```

where $v' \preceq_\pi v$ means "$v'$ is sent no later than $v$". This
assumes each share is sent as a block. Today chunks go out in chunk-id order,
dealt round-robin over a seeded shuffle of validators, so shares interleave
chunk by chunk (§5.5).

The negative stake at lead $L$ is

```math
N(L) = \sum_{v \ne p} w_v \, \mathbb{1}\!\left[ R_v > L \right],
```

plus the stake of Byzantine validators that vote no regardless. Choosing
$L$ is choosing a stake-weighted order statistic of the $R_v$. The farthest
stake does not have to be reached, only enough that $N \le \theta$.

Typical magnitudes in the model:

| term | magnitude |
|---|---|
| $d_{p\to v}$ | 1–170 ms |
| $u_v$ | up to $\kappa P / B$: 10 ms for 400 KB at 1 Gb/s, 48 ms for 2 MB |
| $e(P)$ | about 2 ms per MB |
| $o_v - o_p$ | NTP-dependent: ±1–10 ms |
| jitter in $d_{p\to v}$ | lognormal, about 3% of the latency, plus rare spikes |

## 4. Signals available to the proposer

### 4.1 Votes on its own lane

$p$ receives every validator's batch vote (all-to-all broadcast). At $p$ its
own header is always known, so all of them count at once (`fast.rs`;
`VotePool` at `types.rs`, `buckets()` at `types.rs`). Per own
slot this gives $Y_v = \mathbb{1}[R_v \le L]$ for every $v$ that voted. A
validator that saw a fast block before its local deadline sends no batch
vote; the learner treats it as missing, not as a no.

The signal is **censored**: $Y_v = 1$ says $R_v \le L$, not by how much.
With a comfortable lead every vote is yes and the signal carries no gradient.

### 4.2 Batch-vote arrival times (passive)

Every validator sends its batch vote at its own local $D_s$. Measured on
$p$'s clock and relative to $p$'s $D_s$, $v$'s vote arrives at

```math
m_v = d_{v\to p} + c_v - (o_v - o_p).
```

This is available **in every slot, for every validator, whether or not $p$
is proposing**, so it is always warm. Consensus does not pass the receive
time on (`Chorus::handle_message` takes none), so the planner takes it from
the node's inbound path instead (§8, item 1).

Note the sign of the clock term. Comparing with §3:

```math
R_v - m_v \;=\; 2\,(o_v - o_p) \;+\; (d_{p\to v} - d_{v\to p}) \;+\; e(P) + u_v(P,\pi) - c_v .
```

The probe sees the clock offset with the **opposite** sign from the one that
decides the vote. So it is a prior, not a controller:

- unless clocks are tight, only votes can calibrate the residual;
- with clock offsets of standard deviation 4 ms, using $m_v$ alone produces
  up to 25% of own slots with $N > 1/6$, and 0.8% splits;
- with 1 ms it is accurate enough on its own (§7.5), but it can then be
  manipulated (§6.6).

### 4.3 What is not available or not useful

- **DA acknowledgements, receipts or RTT measurements:** none exist.
- **The proposal header's `unix_ts`:** whole seconds.
- **Deadline-agreement timestamps:** every node proposes the same value
  today.
- **Votes on other proposers' lanes:** they describe other proposers'
  geometry, not $p$'s.

### 4.4 Possible protocol extension (not required)

A voter could include its arrival margin $D_s - t_{\text{arrival}}$,
coarsely quantized, in each positive entry. That removes the censoring, but
adds a field validators could misreport: under-reporting pushes proposers
earlier. The design below does not depend on it.

## 5. Algorithm

### 5.1 State

Per validator $v$ the state is three numbers. They describe the network
from $p$'s position, not a slot or a term.

| state | meaning |
|---|---|
| $\mu_v$ | running mean of the vote arrivals $m_v$ |
| $\delta_v$ | running mean absolute deviation of $m_v$ from $\mu_v$ |
| $b_v$ | correction learned from own-slot votes; it absorbs everything $m_v$ gets wrong (clock sign, route asymmetry, processing) |

A validator without any vote-arrival sample is predicted to vote no
($\hat q_v = 1$). In a small validator set, one such validator can exceed
$\theta$ alone, so the target stays infeasible (static lead, §5.4) until
every peer has been heard from.

Derived: $\hat r_v = \mu_v + b_v$ and
$\sigma_v = \max(\sigma_{\min},\, 1.25\, \delta_v)$. The factor 1.25 turns a
mean absolute deviation into a standard deviation for normal noise.

Warm-up: $\mu_v$ starts at the first sample and $\delta_v$ at 5 ms. For the
first 50 vote-arrival samples, the step is the plain average
$1/(k+1)$ of the $k$ samples so far; after that it is $\alpha$.

### 5.2 Prediction

For payload $P$, send order $\pi$ and queueing delay $Q(L)$ (zero with
uplink priority, §5.6):

```math
\hat R_v \;=\; \hat r_v + e(P) + u_v(P,\pi) + Q(L),
\qquad
\hat q_v(L) \;=\; \Pr[\,Y_v = 0 \mid L\,] \;=\; 1 - \Phi\!\left(\frac{L - \hat R_v}{\sigma_v}\right).
```

$P$ is known at seal time, so the upload term is exact up to the uplink
rate. It is not optional: without it, the lead is about 14 ms later than
needed with 1 MB payloads and still less reliable (§7.6).

### 5.3 Learning

**Passive, every slot**, from the vote arrivals:

```math
\mu_v \leftarrow \mu_v + \alpha\,(m_v - \mu_v), \qquad
\delta_v \leftarrow \delta_v + \alpha\,\big(\lvert m_v - \mu_v\rvert - \delta_v\big),
\qquad \alpha = 0.02,
```

with running averages for the first samples. A variant with a larger
$\alpha$ for samples far above $\mu_v$ tracks sudden latency increases
faster, but over-reacts to spikes (§6.5). It is off by default.

**From votes, after each own slot**, with $L$ the lead actually used: one
stochastic-gradient step on the probit log-likelihood of the observed
$Y_v$:

```math
z_v = \frac{L - \hat R_v}{\sigma_v}, \qquad
g_v = \begin{cases}
\phantom{-}\lambda(z_v) & \text{if } Y_v = 0,\\
-\lambda(-z_v) & \text{if } Y_v = 1,
\end{cases}
\qquad
b_v \leftarrow b_v + \eta \,\operatorname{clip}(g_v, -3, 3), \qquad \eta = 4\ \text{ms}.
```

$g_v$ is the derivative of the log-likelihood with respect to $\hat R_v$, in
units of $1/\sigma_v$. This is a Robbins–Monro step on a censored threshold:

- A yes far inside the deadline ($z_v \gg 0$) gives $g_v \approx 0$. That is
  correct, because it carries no information.
- An unexpected no ($z_v \gg 0$, $Y_v = 0$) gives the largest step,
  $\lambda(z_v) \approx z_v$, clipped at 3. $\lambda$ must be computed in a
  numerically stable way for large arguments.
- A validator that always votes no is driven to $\hat q_v \approx 1$. Its
  stake then counts against $\theta$, which handles Byzantine or broken
  voters without special casing.

### 5.4 Decision rule

$N(L)$ is a stake-weighted sum of Bernoulli variables, approximated as
normal. Votes are not independent: validators that share routes, typically
those of one region, cross the deadline together. The variance therefore
treats validators with nearly equal predicted thresholds as one perfectly
correlated block. With $B$ the partition of validators by
$\lfloor \hat r_v / \beta \rfloor$ (block width $\beta$, default 5 ms):

```math
M(L) = \sum_{v\ne p} w_v\, \hat q_v(L), \qquad
V(L) = \sum_{b \in B} \Big( \sum_{v \in b,\, v\ne p} w_v \sqrt{\hat q_v(L)\,\big(1 - \hat q_v(L)\big)} \Big)^{2}.
```

With singleton blocks this is the independent (weighted Poisson binomial)
variance $\sum_v w_v^2\, \hat q_v (1 - \hat q_v)$. The blocks need no region
labels and cost one extra pass over the validators. §7.10 has the evidence.

**Common clock term.** The passive signal sees the proposer's own clock offset
with the opposite sign from the lead:
$m_v = d_{v\to p} + o_p - o_v$ while $R_v = d_{p\to v} + o_v - o_p + u_v$.
Before own-slot votes calibrate $b_v$, every threshold is therefore off by
$2(o_v - o_p)$. Its common part $-2 o_p$ moves all validators together, and
neither independent nor block variance can see it. A proposer whose clock is
behind under-leads everyone at once. So until calibrated, $V$ gets one more,
fully correlated term:

```math
V(L) \mathrel{+}= \Big(\sum_{v\ne p} w_v\, \frac{\varphi(z_v)}{\sigma_v}\Big)^2 \sigma_c^2,
\qquad \sigma_c = \frac{\sigma_{c,0}}{\sqrt{1 + k/k_0}},
```

where the bracket is $dM/dL$, $k$ counts the own slots whose votes calibrated
the model, $\sigma_{c,0} = 3\sigma_{\text{clock}}$ and $k_0 = 4$. The term fades
as $b_v$ absorbs the offset. §7.12 has the evidence.

**Self-calibrating margin.** The variance is still a model of the network,
and a real network can be worse. Keep the standardized residuals
$s_t = (N_t - M_t)/\sqrt{V_t}$ of the last $W = 1000$ own slots, each
evaluated at the lead actually used. Then replace $z_{1-\varepsilon}$ by
$\max(z_{1-\varepsilon}, \hat s)$, where $\hat s$ is their empirical
$1 - \varepsilon$ tail: the $(\lfloor \varepsilon W \rfloor + 1)$-th largest
residual, used from 50 samples on. The residuals of a proposer's first 8
calibrated own slots stay out of the sample: they carry the warm-up bias that
the common clock term covers, and with fewer than $1/\varepsilon$ residuals
$\hat s$ is the largest one, so a single warm-up outlier would otherwise pin
the margin for up to $W$ own slots (§7.12). When the model is right, $\hat s$ stays
below $z$ and nothing changes. When it understates the tail, the margin widens
by itself. The planner already logs $N$, $M$ and the bound per own slot
(§8.6). §7.11 has the evidence.

```math
L^* \;=\; \min\Big\{\, L \in [L_{\min}, \Delta] \;:\; M(L) + z_{1-\varepsilon}\sqrt{V(L)} \;\le\; \theta - \rho \,\Big\}.
```

With $Q \equiv 0$, $M + z\sqrt{V}$ is decreasing in $L$, so $L^*$ is found by
bisection. The reference implementation bisects over $[0, 600]$ ms and clamps
the result to $[L_{\min}, L_{\max}]$ afterwards; forty iterations of $O(n)$
are negligible at $n = 200$. If no $L \le \Delta$ qualifies, the target is
infeasible: use $\Delta$ and log it (§6.2). Until $p$ has 20 vote-arrival
samples, use the static lead. Only seals made while warm, that is in the
planner's own send order, feed the vote calibration. A cold seal goes out in
DA's default order, which the upload term does not model, so its votes would
calibrate $b_v$ against the wrong upload time.

$L^*$ depends on the payload size $P$, but the payload is drawn only when the
proposal is sealed, after the due time was set. The planner therefore uses an
upper estimate: the proposing source's own size limit (exact for a fixed-size
source; for the mempool source its limit, 64 KiB). It never uses the global
1 MiB message limit, which would overstate the upload for every deployed
proposal.

### 5.5 Send order

Shares go out back to back, so the order $\pi$ changes $u_v$. Let $C$ be the
set of validators that must make it. For $C$, minimizing
$\max_{v \in C}\big(u_v + t_v\big)$, where $t_v = d_{p\to v} + o_v - o_p$ is
the tail, is single-machine scheduling with delivery times. **Jackson's
rule** applies: longest tail first is optimal (Appendix A). Validators
outside $C$ go last, so their bytes never delay $C$.

The rule (`target_far`):

1. Sort validators by $\hat r_v$ ascending.
2. Take the shortest prefix $C$ whose stake reaches $1 - (\theta - \rho)
   + 0.03$. The extra 3% is a margin for jitter at the boundary.
3. Send $C$ in descending $\hat r_v$, then the rest in ascending $\hat r_v$.

Nearest-first is the worst order, worse than arbitrary. Near validators
have slack, and their bytes fit inside the far validators' wire time (§7.6).

Each chunk is a single UDP datagram and the uplink sends datagrams one after
another, so the send order is the only degree of freedom.

Today the order is chunk-id order (`assignment.rs`), and chunks are
dealt round-robin over a seeded shuffle of validators (`assignment.rs`;
ChaCha20 seeded by slot, timestamp window and author), so shares interleave
chunk by chunk: validator $v$'s last chunk leaves after about
$\sum_u \min(k_u, k_v)$ chunks. The `target_far` order has to be applied
where the first-hop disseminations are produced, in DA
(`FirstHopDissemination::into_disseminations`,
`monad-mcp-da/src/wire/proposal.rs`).

The core set follows the estimates. After a no from a far core validator,
its $b_v$ grows, the validator can drop out of $C$, and its stake then counts
as an accepted no within $\theta$. The next lead can therefore be *shorter*
(in a unit-test fixture, 129 → 111 ms). This is intended: the planner spends
the $\theta$ budget on the validators that are most expensive to reach.

### 5.6 Uplink priority

At each deadline every node forwards its owned chunks of all $K$ proposals
to every other validator (second hop, `egress.rs`, released at $D_s$;
`assignment.rs`: rebroadcast targets "all other nodes"). For an
average-stake node that is about $\kappa K P$ bytes per slot, roughly 6 MB
at $P$ = 400 KB, or about 48 ms of a 1 Gb/s uplink (§6.7). In the current
transport it shares one FIFO with the proposal shares.

A proposer whose lead is a bit below $\tau$ seals just after
$D_{s-1}$. Its shares then wait behind the forwarding burst of slot $s-1$,
which delays them by

```math
Q(L) \;=\; \max\!\left(0,\; \frac{F}{B} - \big(\tau - L\big)\right) \quad \text{for } L \le \tau,
```

where $F$ is the burst size. Leads just below $\tau$ are exactly where the
adaptive lead lands for well-connected proposers: about 97 ms for Frankfurt
and 86 ms on the testnet at $\theta = 1/4$. In the model this causes 2.7–3.2%
splits (§7.7).

**Rule: $p$'s own first-hop shares get strict priority over all other
outbound protocol traffic** (forwarding, chunk requests, votes,
transactions). Then $Q \equiv 0$ and the planner is unaffected by
forwarding. The cost to the rest is bounded by the share burst itself,
$e(P) + \kappa P / B$ (10 ms at 400 KB and 1 Gb/s), once per own slot.

If a transport cannot prioritize, the planner must use the known backlog
instead: $Q(L)$ is computable locally from the node's own queue. The rule of
§5.4 is then evaluated on a grid of $L$, because $M + z\sqrt V$ is no longer
monotone. This removes the splits but forces $L > \tau$ (§7.7).

Pull-based recovery adds load later in the slot. From $D_s + \Delta$,
validators that missed chunks request them, and the proposer, as the initial
"decoded holder", serves missing first-hop shares again
(`monad-mcp-da/src/runtime.rs`, `proposer_rc/recovery.rs`;
rate-limited per requester by `serving_tracker.rs`). Pull passes run on a
tick every $\Delta$, so the first request goes out in
$[D_s + \Delta, D_s + 2\Delta)$. Proposers keep their lane for many
slots, so this traffic can coincide with a later own seal. Recovery replies
rank below the node's own fresh first hop.

Two more transport properties matter for the first hop:

- **Pacing.** A first-hop chunk lost to a burst drop means a no vote. Pace
  the share burst at the uplink rate instead of bursting into a shallow
  buffer.
- **Batched writes.** One write per datagram limits the effective rate at
  ≥ 10 Gb/s. Batching (for example one segmented write per validator share)
  keeps $B$ at line rate.

The legacy `monad-dataplane` UDP path already has priority queues
(`UdpPriority`), pacing to a configured `up_bandwidth_mbps`, and batched
segment writes. `monad-mcp-node` uses its own single-socket transport
(`network/stub.rs`) and needs either the dataplane or these three features.

The receiving side carries the same forwarding volume. Spread over a
100–200 ms window, it adds a few ms of queueing at 1 Gb/s, which the
per-validator spread $\sigma_v$ absorbs. It matters only when downlinks are
saturated, which is a capacity problem in its own right (§6.7). It is not
modeled further here.

### 5.7 Withhold rule

The fixed `min_lead` becomes model-based. At poll time $t$, with remaining
time $L_t = D_s - t$ (e.g. after a late chaining-gate release), withhold iff

```math
M(L_t) + z_{1-\varepsilon_w}\sqrt{V(L_t)} \;>\; 1/3 ,
```

that is, iff sending now risks a split. A proposal nobody receives is agreed
negative and costs only its proposer; a partially received one costs the
slot. The rule applies only to a seal that is already due when it is polled
(for example after a late gate release); an early poll is unaffected. On the testnet the right lead for `deu` is about 86–92 ms, *below* the
deployed `withhold_before_deadline = 100`, so the fixed window would block
every proposal.

### 5.8 Rotation, cold start, persistence

- The state is kept across terms. It describes the network from $p$'s
  position and drifts slowly.
- The passive part keeps learning off duty; the vote part learns only on
  duty. On rotation-in the state is therefore warm. In simulation the first
  50 own slots of a term are as good as steady state, even after a +40 ms
  route change during the off-duty gap.
- Cold start (node start, no state): use the static lead until about 20
  vote-arrival samples exist. That is about 20 slots, available before the
  node's first term.
- Persisting the state is cheap ($3n$ numbers). Discard it after hours of
  staleness.
- **Known weak spot: a step in $p$'s own clock.** It shifts every $m_v$ by
  $+\Delta o_p$ and every $R_v$ by $-\Delta o_p$. The planner becomes
  over-conservative, which is safe, until votes correct it (+14 ms over 50
  slots in simulation). Refinement: treat a shift common to all $m_v$ as an
  own-clock change with the opposite sign. Measure the common shift as a
  stake-weighted median, so that manipulated arrivals cannot fake it (§6.6).

### 5.9 Parameters

| parameter | default | meaning |
|---|---|---|
| $\theta$ | 1/4 | bound on negative stake; must be < 1/3 |
| $\varepsilon$ | $10^{-3}$ | risk of exceeding $\theta$ per own slot |
| $\varepsilon_w$ | $10^{-3}$ (= $\varepsilon$) | withhold risk (§5.7) |
| $\beta$ | 5 ms | block width for correlated thresholds in $V$ (§5.4, §7.10) |
| $W$ | 1000 own slots | residual window of the self-calibrating margin (§5.4, §7.11) |
| $\sigma_{c,0}$, $k_0$ | $3\sigma_{\text{clock}}$ (12 ms at a 4 ms clock spread), 4 | common clock term before calibration (§5.4, §7.12) |
| $\rho$ | 0 | reserve for no-voters without history |
| $\alpha$ | 0.02 | passive learning rate |
| $\eta$ | 4 ms | vote-calibration step |
| $\sigma_{\min}$ | 5 ms | spread floor (the value all model results use; with vote arrivals, the reference implementation holds a spread of 5 ms that acts as the floor) |
| core margin | 0.03 | extra stake in the send-order core set $C$ |
| $L_{\min}$, $L_{\max}$ | 20 ms, $\Delta$ | clamps |
| static lead | 150 ms | cold-start fallback and kill switch |

## 6. Trade-offs and parameter choice

### 6.1 Target $\theta$ and risk $\varepsilon$

Results at $n = 190$, mean lead in ms / split rate in %, pooled over 3
seeds and all own slots, cold term included.

| $\theta$ | $\varepsilon$ | FRA | SGP | SAO | SGP 1 MB | SGP spikes | SGP clock σ 12 ms | FRA +40 ms on duty | SGP 10% always-no |
|---|---|---|---|---|---|---|---|---|---|
| 1/12 | 1e-02 | 135 / 0.00 | 139 / 0.00 | 189 / 0.00 | 143 / 0.00 | 149 / 0.00 | 149 / 0.00 | 159 / 0.00 | 457 / 0.00 |
| 1/12 | 1e-03 | 136 / 0.00 | 140 / 0.00 | 194 / 0.00 | 144 / 0.00 | 159 / 0.00 | 150 / 0.00 | 161 / 0.00 | 459 / 0.00 |
| 1/12 | 1e-04 | 137 / 0.00 | 141 / 0.00 | 197 / 0.00 | 145 / 0.00 | 167 / 0.00 | 151 / 0.00 | 162 / 0.00 | 461 / 0.00 |
| 1/6 | 1e-02 | 118 / 0.00 | 126 / 0.00 | 172 / 0.00 | 130 / 0.00 | 130 / 0.00 | 129 / 0.00 | 135 / 0.00 | 138 / 0.00 |
| 1/6 | 1e-03 | 119 / 0.00 | 127 / 0.00 | 176 / 0.00 | 131 / 0.00 | 132 / 0.00 | 130 / 0.00 | 137 / 0.00 | 140 / 0.00 |
| 1/6 | 1e-04 | 120 / 0.00 | 128 / 0.00 | 178 / 0.00 | 132 / 0.00 | 134 / 0.00 | 132 / 0.00 | 139 / 0.00 | 141 / 0.00 |
| 1/4 | 1e-02 | 96 / 0.00 | 112 / 0.00 | 147 / 0.00 | 117 / 0.01 | 118 / 0.00 | 117 / 0.06 | 117 / 0.10 | 124 / 0.01 |
| 1/4 | 1e-03 | 98 / 0.00 | 112 / 0.00 | 149 / 0.00 | 118 / 0.01 | 120 / 0.00 | 118 / 0.04 | 118 / 0.09 | 125 / 0.01 |
| 1/4 | 1e-04 | 99 / 0.00 | 113 / 0.00 | 152 / 0.00 | 119 / 0.01 | 121 / 0.00 | 119 / 0.04 | 119 / 0.08 | 127 / 0.00 |
| 0.29 | 1e-02 | 92 / 0.00 | 106 / 0.01 | 140 / 0.00 | 112 / 0.04 | 111 / 0.01 | 114 / 0.09 | 111 / 0.12 | 115 / 0.06 |
| 0.29 | 1e-03 | 92 / 0.01 | 107 / 0.01 | 142 / 0.00 | 113 / 0.03 | 113 / 0.02 | 115 / 0.07 | 113 / 0.12 | 116 / 0.06 |
| 0.29 | 1e-04 | 93 / 0.01 | 107 / 0.00 | 143 / 0.00 | 114 / 0.03 | 114 / 0.01 | 115 / 0.07 | 115 / 0.11 | 117 / 0.04 |
| 1/3 | 1e-02 | 88 / 4.53 | 103 / 4.39 | 131 / 3.82 | 109 / 5.35 | 105 / 3.83 | 109 / 4.34 | 104 / 3.13 | 108 / 3.58 |
| 1/3 | 1e-03 | 90 / 2.22 | 104 / 2.45 | 132 / 1.74 | 110 / 2.54 | 106 / 1.93 | 110 / 1.74 | 107 / 1.35 | 109 / 1.67 |
| 1/3 | 1e-04 | 91 / 1.39 | 104 / 1.20 | 133 / 0.89 | 110 / 1.40 | 108 / 1.02 | 111 / 0.97 | 109 / 0.91 | 110 / 1.00 |

- **Aiming at the cliff fails.** $\theta = 1/3$ splits 1–5% of slots.
- **$\theta = 1/4$ is the proposed default.** No splits in steady conditions,
  at most 0.09% under stress (clock σ 12 ms, a +40 ms jump mid-term). It
  saves 26–45 ms against $\theta = 1/12$.
- **$\theta = 1/6$ is the conservative alternative.** No splits in any
  scenario tested; gives back 13–27 ms.
- **$\varepsilon$ is cheap.** At $\theta = 1/4$, going from $10^{-2}$ to
  $10^{-4}$ costs 1–5 ms.
- Most residual risk is systematic (clock steps, sudden route changes), not
  sampling noise. A margin in $\theta$ buys more than a small $\varepsilon$.
- At $\theta = 1/12$ with 10% always-no stake the target is infeasible
  (457–461 ms is the unclamped search bound; the planner clamps at
  $\Delta$).

### 6.2 Byzantine voters

Validators that always vote no are learned as such (§5.3) and counted
against $\theta$. Validators that turn against the proposer *without
history* cannot be predicted; $\rho$ is the knob for that, with effective
target $\theta - \rho$. With $\theta = 1/4$, new no-voters holding less
than $1/12$ of stake cannot split the slot on their own, because
$1/4 + 1/12 = 1/3$.

If persistent no-voters alone exceed $\theta - \rho$, the target is
infeasible. The lead is then clamped to $\Delta$, which deserves a warning.

### 6.3 Reading of accountability

- If evidence were judged on every slot, not just on fallbacks (§1.4), the
  target would have to be $\theta \le 1/6$. Leads would be about 15–40 ms
  longer.
- At $\theta = 1/4$, more of the network learns the proposal only after
  $D_s$ through forwarding. Fewer validators can then vote early (paper:
  early voting), which delays early finality somewhat.

This is a protocol-policy question; the parameter makes it explicit.

### 6.4 Externality

A split costs every proposer of the slot, while the splitting proposer is
likely included through the fallback. Without an enforced penalty for
repeated fallbacks, the private optimum sits closer to the cliff than the
system optimum. The algorithm takes $\theta$ as given; the incentive belongs
to the protocol.

### 6.5 Fast-up passive tracking

A larger $\alpha$ for samples above $\mu_v + 3\delta_v$ helps after sudden
latency increases. It also mistakes spikes for regime changes: with 2%
spikes of about 60 ms, the lead goes up 10–25 ms. It is off by default. A
robust change detector (a median over the last $k$ samples, or CUSUM) is the
better tool.

### 6.6 Manipulation of vote timing

Validators control when their batch votes leave. They can delay them to look
far away, or send them early to look close. A delay cannot be told apart
from congestion, so it can be neither proven nor penalized; the algorithm has
to tolerate it.

**Why the damage is bounded.** The model is per validator: $m_v$ only feeds
$v$'s own estimate. A manipulator can only misrepresent its *own* stake:

| manipulation | effect on $p$ | correction |
|---|---|---|
| delay, then vote honestly | $v$ predicted far, counted as a likely no; $p$ seals earlier | $Y_v = 1$ against the prediction pulls $b_v$ down |
| delay, then vote no | same as voting no | already covered: always-no learning, $\theta$ margin, $\rho$ (§6.2) |
| send early, vote honestly | $v$ predicted close; $p$ seals later, $v$ genuinely receives late and votes no | at most $w_v$ of unexpected negative stake (the reserve case); $b_v$ moves up |
| only while $p$ is off duty | stale prior at the start of $p$'s term | as above, within a few slots |

Voting no regardless of delivery is free and undetectable. So vote-timing
manipulation gives an adversary no power beyond what the existing
assumptions already budget for. Two design choices could break that bound,
and the design must avoid both:

- **Aggregates across validators.** The own-clock refinement (§5.8) reads a
  shift common to all $m_v$. Use a stake-weighted median, which stake below
  $1/3$ cannot move; a mean could be faked by a small coordinated group.
- **Running without vote calibration.** Arrivals only is attractive with
  tight clocks (§7.5), but nothing then corrects a misreported arrival. Keep
  the calibration on.

Simulation: SGP proposer, $n = 190$, $\theta = 1/4$, $\varepsilon = 10^{-3}$.
Mean lead in ms / split rate; clock σ 4 ms unless noted
.

| manipulation | proposed (arrivals + votes) | arrivals only | votes only |
|---|---|---|---|
| none | 112 / 0 | 112 / 2.1% | 113 / 0 |
| 10% of stake delays votes by 80 ms | 113 / 0 | 125 / 0 | 113 / 0 |
| 30% of stake delays votes by 80 ms | 113 / 0 | **160** / 0 | 113 / 0 |
| farthest 10% send votes 40 ms early | 112 / 0 | 107 / **9.7%** | 113 / 0 |
| farthest 15% early while $p$ is off duty, clock σ 1 ms | 111 / 0 | 111 / **0.9%** in the first 50 slots of a term | 112 / 0 |

Optional hardening: discard physically implausible arrivals (with
synchronized clocks, a vote arriving before $D_s$ plus a minimum one-way
latency was sent early), and cap how far the passive estimate alone may move
$\hat r_v$ between own slots.

**Interaction with early voting.** If the paper's early voting is
implemented, honest votes no longer leave at $D_s$, and $m_v$ stops
measuring latency. The probe must then use a message still sent exactly at
$D_s$ (in the paper, the decryption share), or votes must carry their send
time.

### 6.7 Uplink bandwidth

**Baseline: 1 Gb/s.** The bandwidth requirement for mainnet validators is
expected to rise from 300 Mb/s to 1 Gb/s with the Cadence rollout. This
document uses 1 Gb/s as the minimum for every validator, in both directions,
and all results assume it unless they say otherwise.

With uplink priority (§5.6), only $p$'s own upload $\kappa P / B$ enters the
lead. Mean lead of the proposed algorithm, $\theta = 1/4$,
$\varepsilon = 10^{-3}$, $n = 190$:

| proposer, payload | 1 Gb/s | 2.5 Gb/s | 10 Gb/s |
|---|---|---|---|
| FRA, 0.4 MB | 96.9 | 96.5 | 96.4 |
| FRA, 1 MB | 99.6 | 98.5 | 98.0 |
| FRA, 2 MB | 107.6 | 101.8 | 100.8 |
| SGP, 0.4 MB | 111.8 | 111.3 | 111.2 |
| SGP, 1 MB | 116.5 | 113.4 | 112.8 |
| SGP, 2 MB | 128.3 | 117.8 | 115.6 |
| SAO, 0.4 MB | 149.1 | 149.0 | 149.5 |
| SAO, 1 MB | 152.8 | 150.8 | 150.8 |
| SAO, 2 MB | 161.8 | 154.4 | 153.4 |

Split rates are 0 except 0.03% for SAO with 2 MB at 1 Gb/s. Payloads above
1 MiB exceed today's `MAX_MESSAGE_LEN` and are shown for scale only.

Going from 1 to 10 Gb/s saves under 1 ms at 0.4 MB, 2–4 ms at 1 MB and
7–13 ms at 2 MB. For the lead alone, more than 1 Gb/s pays off only with
large payloads.

The binding constraint at 1 Gb/s is capacity. The second hop is sustained
traffic in both directions; for an average-stake validator it is about

```math
\frac{\kappa K P}{\tau}
\;=\;
\begin{cases}
0.5\ \text{Gb/s} & P = 0.4\ \text{MB}\\
1.2\ \text{Gb/s} & P = 1\ \text{MB}
\end{cases}
\qquad (\kappa = 3,\ K = 5,\ \tau = 100\ \text{ms}).
```

It scales with stake: a validator with twice the average stake forwards
twice as much. With $\kappa = 3$ these figures are lower bounds. The d33
scheme scales the source count by 1.1, which pays for each node's obligation
being trimmed to 10/11, and then by 3.0, so $S = \lceil 3.0 \lceil 1.1 k \rceil\rceil$
shares. Rounding gives each validator at most one extra, partial chunk. Each
chunk also travels as a 1477-byte datagram (1440-byte segment, frame and
IP/UDP headers) for 1090–1270 bytes of symbol. Counted on the wire, at
$n = 190$ with equal stakes, the first hop sends $\kappa \approx 4.3$ bytes per
payload byte at 64 KiB, 4.9 at 400 KB and 4.5 at 1 MiB (up to 5.2 just past a
depth step, e.g. at 256 KiB, where rounding chunks add up). In the second hop,
d33 forwards a whole chunk to every other non-author but a partial chunk only
to a stake-proportional fraction of them. At 400 KB an average-stake
validator holds 6 whole chunks and a partial worth 0.28, so it forwards about
6.3 × 188 chunks per lane, and the second hop is about 0.7 Gb/s rather than
0.5. Assumptions: $n = 190$, equal stakes, $K = 5$ lanes each carrying a
400 KB proposal every $\tau = 100$ ms, $S = 1194$ shares, 1477 bytes per
chunk. The count covers only the forwarding uplink of a non-author, without
its own first hop, votes, headers or recovery; the downlink carries the same
load. Full rebroadcast of the partial chunk would give 0.78 Gb/s. The saving
from the partial multicast ranges from zero to almost one chunk per lane as
$P$ varies. Near the capacity limit below it is small: d33 and full
rebroadcast both top out at about 574 KB. The load scales with stake. With
log-normal stakes ($\sigma = 1$, $n = 190$, 400 KB) the median validator
forwards 0.49 Gb/s, while the largest, at 7.7 × the average stake, forwards
5.35 Gb/s.

At $B$ = 1 Gb/s an average-stake validator can therefore sustain payloads up
to

```math
P \;\le\; \frac{B\,\tau}{\kappa' K} \;\approx\; 0.55\text{–}0.6\ \text{MB}
\qquad (\kappa' \approx 4.4\text{–}4.5\ \text{forwarded bytes per payload byte}),
```

and half of that with twice the average stake. Payloads near the 1 MiB limit
need more than 1 Gb/s. Deployed mempool proposals (at most 64 KiB) are far
below this. The lead tables in this section and in §7 use the fluid model with
$\kappa = 3$. The chunk-level check in §7.9 shows that the proposed
algorithm's lead moves by at most 1.4 ms with the real chunk counts and wire
ratio, so the tables stand for it; the static baseline is worse than they
show.

## 7. Evaluation

### 7.1 Model

A standalone Monte Carlo model of exactly the quantities in §3.

**Topology:**

- 190 validators over 10 regions (FRA, AMS, LON, NYC, CHI, LAX, SGP, TYO,
  SYD, SAO), with public-internet RTTs, last-mile latency and ±4% route
  asymmetry;
- lognormal stake;
- or the measured 8-node testnet matrix (`deploy/config/latency.toml`).

**Delays and clocks:**

- per-message lognormal jitter plus 0.3% exponential spikes (mean 40 ms);
- clock offsets with standard deviation 4 ms (and 1 ms), slowly drifting.

**Payload and uplink:**

- payload lognormal with median 400 KB;
- $\kappa = 3$ (a slight underestimate, §6.7);
- 1 Gb/s uplink (the assumed minimum, §6.7), stake-proportional shares,
  priority for proposal shares
  unless stated otherwise.

**Outcomes:** $Y_v = \mathbb{1}[R_v \le L]$, outcomes per §1.3.

**Schedule:** terms of 495 own slots separated by about 18,500 off-duty
slots, 6 terms, 4 seeds.

The clairvoyant lead is the per-slot minimum $L$ that meets the target with
the realized delays. No strategy can reach it.

What the model leaves out:

- the chaining gate;
- CPU and encode jitter beyond a linear term;
- kernel and NIC queueing;
- packet loss;
- receiver-side queueing (§5.6).

### 7.2 Strategies compared

| name | description |
|---|---|
| static-150 | deployed |
| static oracle | best fixed lead fitted per seed, i.e. knowing the clock offsets, to at most 1% of slots with $N > 1/6$; not achievable offline |
| votes: AIMD | baseline: raises the lead when the no-count exceeds a threshold, lowers it slowly otherwise |
| votes: probit | §5 without vote arrivals |
| passive only | §5 without vote calibration |
| hybrid | §5, the proposal |

### 7.3 Steady state

Mean lead in ms (excess over clairvoyant), at a common conservative target
$\theta = 1/12$, $\varepsilon = 10^{-2}$, so that the strategies are
comparable. "Evidence" means own slots with $N > 1/6$. Adaptive rows have no
splits and at most 0.5% evidence unless noted.

| scenario | static-150 | static oracle | votes: AIMD | votes: probit | passive only | hybrid |
|---|---|---|---|---|---|---|
| FRA | 150 (+14) | 137 | 153 (+17) | 135 (+5) | 130 | 135 (+5) |
| NYC | 150 (+16) | 144 | 151 (+17) | 133 (+5) | 130 | 132 (+5) |
| SGP | 150, 2.4% evidence | 153 | 164 (+21) | 139 (+4) | 136 | 139 (+4) |
| SAO | 150, **5.8% split** | 198 | 208 (+17) | 192 (+6) | 188 | 191 (+5) |
| testnet, deu | 150 (+56) | 104 | 115 (+21) | 92 (+5) | 94, 25% evidence | 92 (+5) |
| testnet, sgp | 150 (+9) | 145 | 170 (+28) | 137 (+6) | 132, **0.8% split** | 137 (+6) |
| SGP, 1 MB payloads | 150, **12% split** | 196 | 209 (+49) | 144 (+4) | 139, 13% evidence | 143 (+4) |
| SGP, 2% 60 ms spikes | 150, 3.2% evidence | 154 | 168 (+23) | 151 (+11) | 138 | 151 (+11) |
| FRA, +40 ms APAC off duty | 150, 26% evidence | — | 184 (+16) | 168 (+6) | 163 | 168 (+5) |
| SGP, clock σ 12 ms | 150, 5.1% evidence | 159 | 172 (+20) | 150 (+4) | 137, **2.2% split** | 150 (+4) |
| SGP, 10% always-no | 150, 14.6% evidence | 171 | **diverges** | infeasible¹ | 136, 49% evidence | infeasible¹ |

¹At $\theta = 1/12$ the always-no stake (7–10%) leaves no room for honest
no-votes. The model-based strategies learn this and run to the search bound;
a real planner clamps at $\Delta$ (§5.4). At $\theta = 1/4$ the hybrid needs
125 ms with 0.01% splits (§6.1).

The model-based strategies stay 4–6 ms above the clairvoyant lead. With
frequent latency spikes they run at +11 ms, because an unexpected no moves
the estimate up hard. The right static lead spans 104–198 ms across
scenarios and depends on clock offsets that cannot be known offline.

### 7.4 Warm-up

First 50 own slots: excess over clairvoyant / evidence slots.

| scenario | votes: probit | hybrid |
|---|---|---|
| SGP, cold start | +5.1 / 0% | +4.0 / 0% |
| testnet deu, cold start (8 voters) | **+48** / 0% | +3.6 / 0% |
| FRA, +40 ms APAC during the off-duty gap | +2.2 / 3.0% | +5.0 / 0% |
| FRA, +40 ms APAC mid-term | +2.1 / 3.5% | +7.3 / 3.5% |
| SGP, own clock +10 ms during the off-duty gap | +6.4 / 0% | +14.0 / 0% |
| spec-minimal 35-slot terms, cold | +5.0 / 0% | +3.9 / 0% |

With 190 voters, votes alone re-converge within a few slots. The passive
prior matters for small validator sets and for changes during the off-duty
gap.

### 7.5 Clock synchronization

Comparing clock σ 4 ms with 1 ms:

- The vote-calibrated strategies change by only ±1–3 ms.
- Arrivals only becomes accurate:
  - testnet deu: 25% evidence slots → 0%;
  - testnet sgp: 0.8% splits → 0%;
  - SGP 1 MB: 13% evidence → 0.2%.

With tight clocks the passive estimate alone keeps the planner warm, and
votes act as a low-gain safety net, which must stay on (§6.6). The design
does not depend on tight clocks: NTP's error is bounded by half the path
asymmetry, and the protocol cannot verify it. A per-node clock offset is all
a simulator needs for this question; rate errors are negligible (10 ppm over
100 ms is 1 µs).

### 7.6 Send order and size awareness

Pure scheduling comparison: true mean latencies, paired
draws, $\theta = 1/12$. Mean clairvoyant lead in ms (p99):

| proposer, payload, uplink | node-id order | nearest first | farthest first | `target_far` |
|---|---|---|---|---|
| FRA 1 MB, 1 Gb/s | 149.7 (197) | 161.5 (229) | 135.2 (146) | 133.9 (143) |
| SGP 2 MB, 1 Gb/s | 180.7 (294) | 193.8 (325) | 151.2 (227) | 148.2 (222) |
| SAO 2 MB, 1 Gb/s | 227.8 (330) | 245.8 (378) | 197.8 (257) | 194.9 (251) |
| FRA 1 MB, 10 Gb/s | 134.8 (145) | 135.6 (148) | 133.0 (140) | 132.9 (141) |

The gain of `target_far` over plain farthest-first grows with $\theta$: the
validators outside $C$ carry about 5% of the bytes at $\theta = 1/12$ and
about 22% at $\theta = 1/4$.

End to end in the hybrid (SGP, 1 MB median, $\theta = 1/12$):

| configuration | mean lead | notes |
|---|---|---|
| `target_far` | 143 ms | proposed |
| farthest first | 145 ms | |
| node-id order | 164 ms | |
| nearest first | 171 ms | |
| size-blind | 157 ms | 1.2% evidence slots |
| node-id order, size-blind | 190 ms | 7.3% evidence slots |

### 7.7 Contention with forwarding

The proposer's uplink carries a forwarding burst of $F/B = 48$ ms (an
average-stake node, $P$ = 400 KB, 1 Gb/s) at each deadline. $\theta = 1/4$,
$\varepsilon = 10^{-3}$, 3000 own slots × 3 seeds. Mean lead
in ms / split rate:

| | FRA, $n = 190$ | testnet deu |
|---|---|---|
| no forwarding traffic | 97 / 0 | 86 / 0.04% |
| FIFO, planner unaware of $Q$ | 106 / **2.7%** | 105 / **3.2%** |
| FIFO, planner adds $Q(L)$ | 101 / 0 | 101 / 0 |
| priority for proposal shares (§5.6) | 97 / 0 | 86 / 0.04% |

With priority, forwarding has no effect on the lead. Without it, a planner
that ignores its own queue splits slots, and one that accounts for it is
pushed above $\tau$. On the node the FIFO case is worse still: 5.9% splits
(§7.12).

### 7.8 Validation status

§7.1–§7.7 and §7.9–§7.11 are model results. The node itself was validated in
the `monad-mcp-node` scenario harness on monad-sim (§7.12, scenario
`validation`): the real planner, Chorus fast path, MVBA and DA (d33); a
per-link latency matrix with region presets and seeded placement; per-node
egress with priority classes; per-node clock offsets; the parallel engine.
Next are runs at $n = 190$ and over many terms, which hollow chunks make
affordable (§7.12), and the testnet (§8).

### 7.9 Chunk-level first hop

The results above use a fluid model: each validator's share is stake ×
$\kappa$ × $P$ with $\kappa = 3$, and the yes vote needs the whole share. The
node differs in three ways, all of which the model's chunk-level mode
covers:
- d33 chunk counts per validator, $k_v = q + [r > 0]$ (§1.2), at least one
  per staked validator;
- 1477-byte datagrams, a wire ratio of 4.6–6.6 on the geo topology with
  log-normal stakes (more at 64 KiB, where rounding gives every validator a
  chunk);
- the trimmed vote rule $\lceil 10\,k_v/11\rceil$.

The baseline order is today's interleaved order. The geo topology has
$n = 190$, $\theta = 1/4$ and $\varepsilon = 10^{-3}$, with 3 seeds × 2
steady terms. Mean lead in ms; the clairvoyant lead is in
brackets:

| proposer, payload | static 150, today's order | proposed, fluid | proposed, chunks, `target_far` | proposed, chunks, today's order |
|---|---|---|---|---|
| FRA, 64 KiB (deployed) | 150 (92.4) | 96.4 | 96.1 | 99.2 |
| FRA, 400 KB | 150 (106.2) | 97.8 | 97.7 | 113.2 |
| FRA, 1 MiB | 150 (125.9) | 99.5 | 100.0 | 130.5 |
| SGP, 64 KiB | 150 (108.7) | 110.5 | 111.1 | 112.9 |
| SGP, 400 KB | 150 (119.6) | 112.1 | 112.9 | 124.0 |
| SGP, 1 MiB | 150 (135.8) | 115.2 | 116.6 | 140.0 |
| SAO, 64 KiB | 150 (144.0) | 148.1 | 147.8 | 150.3 |
| SAO, 400 KB | 150 (155.9), **19.5% splits** | 149.4 | 149.4 | 161.6 |
| SAO, 1 MiB | 150 (171.7), **100% splits** | 152.4 | 151.6 | 178.2 |

The proposed algorithm has no splits in any case.

The planner above knows the real per-validator chunk counts. The node
implementation instead estimates its upload as stake × $\kappa$ × $P$
(`[proposal.adaptive]`, $\kappa = 4.5$), so the cadence planner does not
depend on DA's assignment. Under chunk-level delivery and fixed payloads, that
estimate leaves the lead within 2.2 ms of the chunk-aware planner in all
nine cases, always on the conservative side, with no splits. Even
$\kappa = 3$ is within 2.2 ms. The vote calibration $b_v$
absorbs the difference.

- **The proposed algorithm is unaffected.** With `target_far` its lead moves
  by at most 1.4 ms from the fluid model. The target set goes first, so the
  extra bytes land in the tail beyond it.
- **The deployed configuration is worse than the fluid model shows.** With
  today's interleaved order, the clairvoyant lead is 6–18 ms higher at
  400 KB–1 MiB. A São Paulo proposer splits 19.5% of its slots at 400 KB
  (4.2% in the fluid model) and every slot at 1 MiB.
- **The send order matters more.** Keeping today's order costs the proposed
  algorithm 11–31 ms of lead at 400 KB–1 MiB. At the deployed 64 KiB, where
  most validators hold one chunk, it costs 2–3 ms.
- **At the deployed 64 KiB,** the adaptive lead is 54 ms (FRA), 39 ms (SGP)
  and 2 ms (SAO) below the static 150 ms.

### 7.10 Correlated thresholds

The node's first validation run found a failure mode the model had hidden
(scenario `adaptive`, $n = 64$, $\theta = 1/4$, independent variance). It had 2 fallback slots out of about 85. The mechanism:
- In the scenario harness, a region's validators share one latency, with
  under 1 ms of jitter and no clock offsets.
- Under all-yes verdicts the probit steps lowered $L^*$ by about 0.5 ms per
  slot while the realized $N$ stayed at 0.20.
- At about 75 ms nine validators of one region flipped together, and $N$
  jumped to 0.34, past 1/3.

The independent variance cannot see such a step. The model's per-validator
jitter and clock offsets had smoothed it away.

The model reproduces it with region-correlated noise.
Settings: $n = 64$, equal stakes, 256 KiB fixed, chunk-level first hop,
$\varepsilon = 10^{-3}$, 3 seeds × 2 steady terms of 495 own slots. Mean lead
in ms and split rate:

| network | proposer | $\theta = 1/4$, independent $V$ | $\theta = 1/4$, block $V$ ($\beta$ = 5 ms) | $\theta = 0.2$, independent $V$ |
|---|---|---|---|---|
| region latency + shared per-slot delay (mean 2 ms) | FRA | 86.7, 0 | 91.4, 0 | 89.0, 0 |
| | NYC | 64.3, 0 | 70.8, 0 | 89.2, 0 |
| | SGP | 117.2, **2.0%** | 129.3, 0 | 122.8, 0.07% |
| | SAO | 112.1, **2.7%** | 118.1, 0.1% | 136.8, 0 |
| default model network | FRA | 91.5, 0 | 93.5, 0 | 95.9, 0 |
| | NYC | 67.8, 0 | 72.5, 0 | 94.2, 0 |
| | SGP | 124.7, 0 | 130.5, 0 | 129.2, 0 |
| | SAO | 123.0, 0 | 127.2, 0 | 138.2, 0 |

- **Region latency alone does not split** in the model (0 of 2970 for every
  proposer); a delay shared by a region per slot does. The shared delay is a
  stand-in for shared routes, and its size is not measured.
- **The block variance removes the splits** (at most 0.1%), for 2–6 ms of
  lead on the default network at $n = 64$ and 5–12 ms with correlated noise.
  At $n = 190$ on the default network (400 KB, chunk-level) it costs 1.4 ms
  (FRA), 10.4 ms (SGP) and 6.5 ms (SAO).
- **Lowering $\theta$ to 0.2 also works,** but costs up to 25 ms for
  proposers whose cheap validators sit in one large region (NYC, SAO).
- **The block width is not critical.** At 2.5–20 ms the splits stay at
  0–0.2%, and the cost grows with the width. 5 ms equals the spread floor
  $\sigma_{\min}$.

On the node (the same $n = 64$ scenario,
120 slots, one seed) the block variance at $\theta = 1/4$ removed both
fallbacks:

| run | mean lead | steady fallbacks |
|---|---|---|
| independent $V$, $\theta = 1/4$ | 91.0 ms | 2 |
| independent $V$, $\theta = 0.2$ | 97.4 ms | 0 |
| block $V$, $\theta = 1/4$ | 100.3 ms | 0 |

There the block variance costs 9 ms, more than in the model, because the
harness's correlation is nearly total: no clock offsets and under 1 ms of
jitter. The realized $N$ averaged 0.167, with a maximum of 0.266 over 393 own
slots. This is a thin sample; §7.12 tests it properly: the rate of
$N > \theta$ against $\varepsilon$, over thousands of own slots and several
seeds.

The normal approximation is still optimistic when a single block decides
the tail, because $N$ then has two outcomes rather than a bell curve. In a
worst-case example, the planner is at $\theta = 1/4$, $\varepsilon = 10^{-3}$
and $\sigma = 5$ ms. Nine validators of 1/64 stake share a threshold of
76 ms, and 0.20 of negative stake is already fixed, so a late region means
$N = 0.34$. The rules then pick:
- independent variance: 83.1 ms, region late in 7.9% of slots;
- block variance: 87.2 ms, 1.3%;
- the exact two-outcome rule: 91.5 ms, 0.1%.

An exact tail over the blocks would close that gap: each block is one
two-outcome vote, and the blocks are combined by convolution on a stake grid,
which is cheap at $n \approx 200$. That is open (§9).

The block variance is therefore part of the proposed rule (§5.4). The lead
tables elsewhere in this document still use the independent variance. With
blocks they rise by the amounts above: up to about 10 ms at $n = 190$, plus
2–3 ms for the self-calibrating margin (§7.11).

### 7.11 Robustness to a wrong risk model, and parameter sensitivity

The learning part measures each validator directly, so it carries over to
networks the model does not describe. The tail of the risk rule does not: it
assumes Gaussian per-validator spreads, combined as a normal over blocks. A
real network can have heavier tails and stronger correlation. The
self-calibrating margin (§5.4) is the guard against that (
3 seeds × 2 steady terms of 495 own slots, $\theta = 1/4$,
$\varepsilon = 10^{-3}$).

Mean lead in ms and splits; the correlated network is the one from §7.10,
$n = 64$:

| network | proposer | independent | independent + margin | block | block + margin |
|---|---|---|---|---|---|
| correlated | FRA | 86.7, 0 | 93.2, 0 | 91.4, 0 | 94.5, 0 |
| | SGP | 117.2, **60/2970** | 130.7, 0 | 129.3, 0 | 129.3, 0 |
| | SAO | 112.1, **79/2970** | 144.3, 0 | 118.1, 3/2970 | 136.7, 0 |
| default, $n = 64$ | FRA | 91.5, 0 | 95.0, 0 | 93.5, 0 | 96.2, 0 |
| | SGP | 124.7, 0 | 129.3, 0 | 130.5, 0 | 130.5, 0 |
| | SAO | 123.0, 0 | 127.4, 0 | 127.2, 0 | 129.4, 0 |
| default, $n = 190$, 400 KB | FRA | 97.7, 0 | 100.5, 0 | 99.1, 0 | 102.3, 0 |
| | SGP | 112.9, 0 | 118.3, 0 | 123.3, 0 | 125.3, 0 |
| | SAO | 149.4, 0 | 155.4, 0 | 155.9, 0 | 157.7, 0 |

- **The margin catches a wrong variance.** Even with the independent variance
  on the correlated network it removes every split. With blocks it removes the
  last three.
- **It is cheap when the model is right:** 0–3 ms on top of the block
  variance on the default networks.
- **Block variance and margin overlap.** The margin alone is equally safe in
  these runs and often cheaper (SGP at $n = 190$), sometimes not (SAO at
  $n = 64$). The margin needs about 50 own slots of history, while the blocks
  work from the first warm slot. Both are kept: together they cost 5–12 ms
  against the bare rule at $n = 190$, with no splits on any network tested.

The learning parameters were chosen on the model, so they were swept one at
a time with the block variance on. The lead changed by at most 1.5 ms for
$\eta \in \{2, 8\}$ ms (default 4) and $\alpha \in \{0.01, 0.05\}$ (default 0.02),
with the same splits. The exception is SAO on the correlated network, where
$\eta = 8$ is 15 ms more conservative. A spread floor of 10 ms instead of 5
costs 0.5–6 ms at $n = 190$ and 26 ms for SAO on the correlated network,
where it also removes the last three splits. No parameter sits on a knife
edge.

On a real network, start with $\theta = 1/6$ and the margin on. From the
per-slot log, check that $N > \theta$ and $N > M + z\sqrt{V}$ occur at about
the rate $\varepsilon$ promises before moving to $\theta = 1/4$ (§8).

### 7.12 Validation on the node

The node's planner ran in the scenario harness (scenario `validation`):
- $n = 64$, the public-internet preset with seeded placement, equal stakes;
- 1 Gb/s uplinks with first-hop priority, jitter uniform in [0, 1] ms,
  clock offsets with $\sigma = 4$ ms;
- 256 KiB proposals, $\theta = 1/4$, $\varepsilon = 10^{-3}$, blocks
  ($\beta$ = 5 ms) and the margin ($W$ = 1000) on.

Main runs are 2 seeds × 2000 slots, about 17,000 own slots each; variants are
2 × 1000 slots. Counts start at slot 30. Rates are per own slot.

| run | mean lead | $N > \theta$ | $N >$ bound | splits |
|---|---|---|---|---|
| static 150 ms | 150 ms | 3.0e-2 | | 2.5e-2 |
| adaptive, without the common clock term, re-planning and margin warm-up | 98.4 ms | 2.5e-3 | 5.0e-3 | 5 (2.9e-4) |
| + common clock term | 101.5 ms | 1.2e-3 | 2.9e-3 | 0 |
| **+ re-planning on votes and margin warm-up (the design in §5)** | **100.7 ms** | **7.6e-4** | 2.8e-3 | **0** |
| same, with hollow chunks | equal per proposer | 8.1e-4 | | 0 |
| without first-hop priority (FIFO) | 112.3 ms | 9.4e-2 | 8.8e-2 | 5.9e-2 |
| $\theta = 1/6$ (without the clock term) | 112.5 ms | 6.9e-3 | 5.0e-3 | 0 |
| clock offsets 0 | 101.7 ms | 6.3e-3 | 6.1e-3 | 0 |
| +20 ms step on links into one region, one direction | absorbed: +0.3–1.3 ms, no split | | | |

Per-proposer leads run from 79 ms (FRA) to 147 ms (SYD).

**The design holds on the node.** It had no fallback in about 17,000 own
slots, exceeded $\theta$ below the promised rate, and its lead averaged
100.7 ms against 150 ms static.

What the node runs showed:
- **The proposer's clock offset.** Vote arrivals see the proposer's own
  clock offset with the opposite sign from what the lead needs (§5.4). Before
  the common clock term existed, every split came from proposers whose clock
  was behind (all with $o_p < -2$ ms), in their first 8 own slots. The term
  removed them for about 3 ms of mean lead.
- **Late feedback.** The next own seal is due before the previous slot's
  votes are in: before a slot's median vote arrives the proposer has sealed
  1–2 more own slots. With due times re-planned only once per slot, 16% of
  seals used a lead below the current plan, and those exceeded $\theta$
  about 3× as often. Re-planning pending dues on every own-slot vote (§8)
  brought that to 0.3%.
- **Margin pinning.** With fewer than $1/\varepsilon$ residuals, $\hat s$ is
  the largest residual seen, so one warm-up outlier pinned a proposer's
  margin. Leaving the first 8 calibrated own slots out of the sample (§5.4)
  fixes it.
- **Steady exceedances** are almost all by one validator: $N = 17/64$
  against $\theta = 16/64$, because with equal stakes $N$ moves in steps of
  1/64. A split needs 22/64.
- **First-hop priority is required.** Without it, the d33 forwarding bursts
  of all lanes queue in front of the shares: 5.9% splits, worse than the
  model's 2.7–3.2% (§7.7).
- **The static baseline** cascaded in one seed (5.3% splits) because the
  harness sets `withhold_before_deadline = 0` (deployed: 100), so its late
  seals were not withheld.

**Hollow chunks** (`monad-mcp-node/docs/simulation-data.md`) give
per-proposer leads equal to d33's (correlation 1.0000) and the same
exceedance count, at under a sixth of the memory. They carry the larger and
longer runs.

**Model against node.** The model of §7.1, run on each network the harness
exported (the same latencies, clock offsets, jitter and d33 chunks, with the
node's feedback timing), reproduces the node's per-proposer mean leads within
a median 0.6–0.7 ms (correlation 0.997–0.999) and its splits. The node's
steady $N > \theta$ rate (0.06–0.07%) matches the model's (0.02–0.08%).

## 8. Integration plan

Implemented (off by default unless noted):

1. **Facts in, no consensus change.**
   - Verdicts and arrival times come from the node's inbound path, not from
     consensus. A decided slot drops further messages (`slot/chorus.rs`), and
     closed slots are dropped in `CadenceRuntime::handle_message`. So after a
     fast finalization the remaining batch votes never reach
     `FastPath.votes[j]`, and those late votes come from the farthest
     validators, the ones the learner needs most.
   - A proposing component that wants them (`ProposingComponent::wants_vote_facts`)
     gets every inbound batch vote from `NodeRouter` as
     `ProposingInput::BatchVote`, with its arrival time and per-lane positive
     flags. The planner keeps the first copy per validator and slot (the
     repeater re-broadcasts). `ProposingInput::Finalized` carries the
     finalization path.
   - A validator with no batch vote by the end of the observation window is
     *missing*, not *no* (it may have seen a fast block before its deadline).
   - The entries are read before signature verification. That is fine with
     an authenticating transport and in simulation; in production either the
     transport authenticates the sender, or the planner only counts votes
     that consensus later verified.
2. **Planner** (`monad-mcp-cadence/src/adaptive_lead.rs`, `proposing.rs`):
   the per-validator estimator, the risk rule with block variance, common
   clock term and self-calibrating margin, and the §5.7 withhold. The due
   time is $D_s - L^*(P)$. The conductor opens a whole deadline-agreement
   window of slots at once, so due times not yet reached are re-planned at
   every cap advance and whenever an own-slot vote updates $b_v$; a due time
   moved into the past seals at once, subject to §5.7.
3. **Send order.** DA takes a caller-provided first-hop order
   (`DAInput::Propose { send_order }`, `AuthoredProposal::into_disseminations_in`)
   and sends each share as a block in it; the planner provides `target_far`.
   Without an order, chunk-id order as before.
4. **Transport, in simulation.** DA tags its own first hop
   (`DAOutput::FirstHop`), the router maps it to an `OutboundClass`, and
   `monad-sim-swarm` gives egress priority classes
   (`Network::egress(..).priority`).
5. **Config** `[proposal.adaptive]`: $\theta$, $\varepsilon$,
   $\varepsilon_w$, $\rho$, clamps, block width, clock spread, margin window
   and warm-up. The static lead stays as the cold-start value and as the kill
   switch.
6. **Observability.** One record per own slot: planned and used lead,
   predicted $M$ and bound, realized negative stake, missing votes, path.

Open:

7. **Transport in production.** Strict priority for the node's own
   first-hop shares over all other outbound traffic, pacing, and batched
   writes (§5.6), in the real send path (`Node.outbox` → `Link` →
   `network/stub.rs`, FIFO with one `send_to` per datagram): either move
   `monad-mcp-node` onto `monad-dataplane`, which has priority, pacing and
   batching, or add them to `network/stub.rs`. §7.12 shows it is required.
8. **Validation at scale,** then the testnet: start at $\theta = 1/6$ and
   move to $1/4$ once the measured rates of $N > \theta$ and $N > M +
   z\sqrt{V}$ match $\varepsilon$. On the testnet, compare $m_v$ with RTT/2
   from `latency.toml` and the clock offsets from `report.sh`.

## 9. Open questions

1. **Who sets $\theta$ and $\varepsilon$:** node-local policy, or a
   recommended protocol constant? Should repeated fallbacks caused by one
   lane be penalized (§6.3, §6.4)?
2. **Early voting:** if it is implemented, which message carries the $D_s$
   timing for the passive probe (§6.6)? Does the lower positive count at
   $\theta = 1/4$ matter for early finality?
3. **Should votes carry an arrival margin (§4.4)?** It would remove the
   censoring, at the cost of a value validators could misreport.
4. **Several proposals from one node:** if a node holds several lanes, or
   proposals overlap, the uplink is shared and the upload model needs the
   combined queue.
5. **Deadline agreement:** once the median ACS uses local times, its
   timestamps become another passive clock signal.
6. **Bandwidth beyond the 1 Gb/s minimum:** at 1 Gb/s the second-hop load
   (§6.7) limits an average-stake validator to payloads of about
   0.55–0.6 MB at $K = 5$ (counted on the wire), and less with more stake.
   Should the requirement scale with stake, or should payloads be capped
   below the 1 MiB limit?
7. **Exact tail over correlated blocks:** replace the normal approximation
   of $N$ by a convolution over the blocks of §5.4 (one two-outcome vote per
   block) if validation shows exceedances above $\varepsilon$ that the
   self-calibrating margin does not absorb (§7.10, §7.11).
8. **Remaining model gaps** after the chunk-level check (§7.9): missing
   votes (a validator that saw a fast block early), and pull-recovery
   traffic on the proposer's uplink.

## Appendix A. Why longest tail first

The shares of $C$ leave the uplink back to back. Validator $v$'s share
finishes upload at $F_v$, and $v$ votes yes iff $F_v + t_v \le L$, with
$t_v$ the tail (wire latency plus clock offset).

Take any order in which $u$ directly precedes $w$ with $t_u < t_w$, and swap
them. $w$ now finishes earlier, and $u$ finishes at the old $F_w$. Since
$F_w + t_u < F_w + t_w$, the maximum over the pair does not increase.
Repeated swaps reach the descending-tail order without ever increasing
$\max_{v \in C}(F_v + t_v)$. Any validator placed before $C$ only shifts
every $F_v$ in $C$ later, so the rest goes last.

## Appendix B. Further reading

The combination is specific to this problem. Each part is a standard
technique; these are the classic sources.

**Learning a threshold from yes/no votes (§5.3).** Each own slot tests $v$'s
unknown threshold $R_v$ at one lead $L$ and returns yes or no. The update is
a Robbins–Monro step with a constant gain, which tracks a drifting threshold
instead of converging to a fixed one.

- H. Robbins, S. Monro. A Stochastic Approximation Method. *Annals of
  Mathematical Statistics* 22(3), 1951. doi:10.1214/aoms/1177729586
- H. J. Kushner, G. G. Yin. *Stochastic Approximation and Recursive
  Algorithms and Applications*, 2nd ed. Springer, 2003. doi:10.1007/b97441.
  Constant step sizes and tracking.
- W. J. Dixon, A. M. Mood. A Method for Obtaining and Analyzing Sensitivity
  Data. *JASA* 43, 1948. doi:10.1080/01621459.1948.10483254. The up-and-down
  method.
- C. F. J. Wu. Efficient Sequential Designs with Binary Data. *JASA* 80,
  1985. doi:10.1080/01621459.1985.10478213. Sequential fits of a response
  curve.
- V. R. Joseph. Efficient Robbins–Monro Procedure for Binary Data.
  *Biometrika* 91(2), 2004. doi:10.1093/biomet/91.2.461. Extreme quantiles.

**Censoring and the probit likelihood (§5.3).** A vote only says
$R_v \le L$ or $R_v > L$. The derivative of the censored-normal
log-likelihood is the inverse Mills ratio $\lambda$.

- J. Tobin. Estimation of Relationships for Limited Dependent Variables.
  *Econometrica* 26(1), 1958. doi:10.2307/1907382
- P. McCullagh, J. A. Nelder. *Generalized Linear Models*, 2nd ed. Chapman &
  Hall, 1989. The probit link and score equations.

**Risk rule (§5.4).** A chance constraint on a stake-weighted Poisson
binomial, approximated as normal. The exact distribution, by convolution or
DFT, is cheap at $n \approx 200$ if the tail needs it.

- A. Charnes, W. W. Cooper. Chance-Constrained Programming. *Management
  Science* 6(1), 1959. doi:10.1287/mnsc.6.1.73
- Y. Hong. On Computing the Distribution Function for the Poisson Binomial
  Distribution. *Computational Statistics & Data Analysis* 59, 2013.
  doi:10.1016/j.csda.2012.10.006

**Passive estimate and change detection (§5.3, §6.5).**

- S. W. Roberts. Control Chart Tests Based on Geometric Moving Averages.
  *Technometrics* 1(3), 1959. doi:10.1080/00401706.1959.10489860. EWMA.
- E. S. Page. Continuous Inspection Schemes. *Biometrika* 41(1–2), 1954.
  doi:10.1093/biomet/41.1-2.100. CUSUM.

**Send order (§5.5, Appendix A).** Single-machine scheduling with delivery
times, $1 \mid q_j \mid L_{\max}$. Longest delivery time first is
earliest-due-date ordering (Jackson's rule).

- J. R. Jackson. Scheduling a Production Line to Minimize Maximum Tardiness.
  Research Report 43, Management Science Research Project, UCLA, 1955.
- M. L. Pinedo. *Scheduling: Theory, Algorithms, and Systems*. Springer,
  single-machine chapter.

**Clocks (§7.5).**

- D. Mills et al. Network Time Protocol Version 4. RFC 5905, 2010. The offset
  error bound of half the path asymmetry.
