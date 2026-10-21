# Lessons (pq-cadence)

- Work in the main checkout, not a separate worktree, unless asked (user, 2026-10-21). Commit path-scoped with `jj commit <paths>`.
- Quick pre-commit gate for monad-pq-* work: precommit-mcp style scoped to `^monad-pq` crates, not the full `precommit` suite.
- Before reporting a local single-node latency, compare against the monad-mcp baseline under identical params/load; single-node tails can be shared schedule artifacts (lane rotation vacancy), not regressions.
- Don't trust an agent's "agreement OK" from a script without spot-checking the check can fail (summarize.py read the wrong lane key and always passed).
- Remote demo ports: check the host's existing nginx proxy targets (sgp-008 proxies `/` to 127.0.0.1:8081) before picking new service ports.
