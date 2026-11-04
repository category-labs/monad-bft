# Submodule Pin Gating

monad-bft records the `monad-execution` submodule as a gitlink: a bare commit
SHA in the tree. Nothing in git makes that SHA follow a branch, so a pin is only
as durable as the commit it names. This document describes how CI constrains
which pins may reach `master`, and why the check is implemented in-tree rather
than with an off-the-shelf action.

## The two ways a pin goes bad

**The commit stops existing.** The normal cross-repo workflow is to open an
execution PR, pin its head in a bft PR, then merge both. While that execution PR
is open its branch may be force-pushed or deleted, which orphans the pinned
commit. GitHub keeps serving an orphan until it is garbage-collected, so the
breakage does not surface when the bft PR merges — it surfaces weeks later, when
`git submodule update` starts failing for everyone on `master` and the commit is
unrecoverable.

`category-labs/monad` merges by rebase, which makes this the *expected* outcome
rather than an edge case: merging an execution PR rewrites every commit on its
branch, so a pin taken from a PR head is permanently unreachable from `main`
afterwards. The pin must be moved to the rewritten commit, which is the PR's
`merge_commit_sha`.

**The commit goes backwards.** A gitlink is one line of tree data with no
readable diff, so a mistaken conflict resolution can silently resurrect an older
pin. Because the older pin is a perfectly valid commit on execution `main`, no
reachability check catches it; merging then reverts execution bumps other people
already landed.

## What CI enforces

`.github/workflows/exec-pin.yml` runs `exec-pin.sh`, which reads the gitlink
recorded in `HEAD` and requires both:

1. **Reachability** — the pin is `identical` to, or `behind`, execution `main`
   (GitHub's compare status for "head is an ancestor of base"). This rejects
   pins that exist only on an unmerged PR branch, orphans, and commits merged
   into some other execution branch.
2. **Progression** — the pin is not `behind` the pin already on `master`. This
   rejects a rollback that reachability alone would accept.

Both run against the GitHub API, so the job needs no submodule checkout. That is
deliberate: a recursive checkout dies on precisely the unfetchable pin the job
exists to report, before it can report it.

Failures distinguish "the pin is wrong" from "the check could not run". Only a
definitive 404 is reported as a missing commit; a rate limit, an auth failure or
a 5xx is reported as a CI fault, with `gh`'s stderr kept in the log. An earlier
revision conflated the two and told authors their pin had been force-pushed away
whenever the API hiccuped.

The submodule path is read from `.gitmodules` rather than hardcoded, so renaming
or moving the submodule fails the check instead of silently disabling it. The
repository named there is asserted against an expected value rather than
trusted: `.gitmodules` comes from the tree under test, so a PR that repoints the
submodule at a fork must not redirect the gate along with it.

### Scope

The workflow is restricted to PRs targeting `master`, because a release branch
may legitimately pin an execution cherry-pick that never lands on execution
`main`; `EXEC_BASE` selects a different execution branch for such a variant.

There is deliberately no `paths:` filter. A skipped run posts no status, so a
path-filtered required check never reports on PRs that leave the gitlink alone,
and GitHub's merge queue then waits on it indefinitely. The job is two API calls
and costs nothing to run unconditionally.

## Why not something off the shelf

**No native GitHub feature gates a gitlink.** Rulesets and branch protection
cover status checks, commit metadata, push rules, file paths, file size and
branch names. Submodule pointers are not among them.

**`actions/checkout` with `submodules: recursive` does not catch it.** GitHub
serves unreachable-but-un-garbage-collected objects, so both a force-push orphan
and a rebase-merge orphan still fetch successfully by SHA. A recursive checkout
only breaks once GC happens, which is long after the offending PR merged.

**[`jtmullen/submodule-branch-check-action`](https://github.com/jtmullen/submodule-branch-check-action)
cannot be a required check here.** It implements the same two checks and is
otherwise sound, but its entrypoint recognises only `pull_request` and `push`
event payloads and exits non-zero on anything else. A `merge_group` payload
carries neither `.pull_request` nor `.after`, so every merge-queue run would
fail, and GitHub requires a required check to report on `merge_group`. The
action accepts an `event_path` override that could be fed a synthesised payload,
but it is documented for testing only. `exec-pin.sh` is event-agnostic by
construction — it reads `HEAD` and never parses the event — which is what makes
it usable in the queue.
