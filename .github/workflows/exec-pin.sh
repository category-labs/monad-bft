#!/usr/bin/env bash
# Reject a monad-execution gitlink that is not reachable from execution main.
# A pin reachable only from a PR branch vanishes when that branch is
# force-pushed or deleted, breaking `git submodule update` for everyone on
# master.
#
# The gate only has teeth as a required check: a run that never happens posts
# no status, and a PR can edit this file.
#
# No -e: nearly every command here is one whose failure is a distinct verdict,
# so each exit status is examined at its call site instead.
set -uo pipefail

SUBMODULE=monad-execution
EXEC_BASE="${EXEC_BASE:-main}"
BASE_BRANCH="${BASE_BRANCH:-master}"
# .gitmodules is read from the tree under test, so the repository it names is
# asserted rather than trusted: a PR that repoints the submodule at a fork must
# not redirect the gate along with it.
EXEC_REPO_EXPECTED=category-labs/monad

err=$(mktemp)
trap 'rm -f "$err"' EXIT

summary() { printf '%s\n' "$@" >>"${GITHUB_STEP_SUMMARY:-/dev/null}" || true; }

fail() {
    printf '%s\n' "::error::$1" "" "$2"
    summary "## Execution pin rejected" "" "$1" "" "$2"
    exit 1
}

ci_fault() {
    printf '%s\n' "::error::$1" "" "$2"
    summary "## Execution pin not verified" "" "$1" "" "$2"
    exit 1
}

# 44 = the object is definitively absent, 1 = the query could not be completed.
# gh writes the error body to stdout and the status line to stderr and exits
# non-zero for both, so neither stream alone tells those two cases apart.
exec_api() {
    local body
    if body=$(gh api "repos/$EXEC_REPO/$1" 2>"$err"); then
        printf '%s' "$body" | jq -r "$2"
        return
    fi
    cat "$err" >&2
    if grep -q 'HTTP 404' "$err"; then
        return 44
    fi
    return 1
}

# A mistyped gitlink conflict resolution rolls the pin backwards silently, which
# reverts execution bumps already on the base branch.
check_progression() {
    if ! git fetch -q --depth=1 origin "$BASE_BRANCH"; then
        ci_fault "could not fetch $BASE_BRANCH to compare its $path pin against $short." \
            "git fetch failed, see above. Re-run the job."
    fi
    local base_pin base_tree direction rc=0
    if ! base_tree=$(git ls-tree FETCH_HEAD -- "$path"); then
        ci_fault "could not read the $path gitlink from $BASE_BRANCH." \
            "git ls-tree failed, see above. Re-run the job."
    fi
    base_pin=$(printf '%s\n' "$base_tree" | awk '$2 == "commit" { print $3 }')
    if [ -z "$base_pin" ]; then
        printf '%s\n' "::warning::$BASE_BRANCH records no $path gitlink, so there is no previous pin to compare against."
        return
    fi
    if [ "$base_pin" = "$pin" ]; then
        echo "$path pin is unchanged from $BASE_BRANCH"
        return
    fi
    direction=$(exec_api "compare/$base_pin...$pin" '.status') || rc=$?
    if [ "$rc" -ne 0 ]; then
        ci_fault "could not compare the $path pin $short against ${base_pin:0:9} from $BASE_BRANCH." \
            "The GitHub query failed, see above. Re-run the job."
    fi
    case "$direction" in
    ahead | identical)
        echo "$path pin advances from ${base_pin:0:9} on $BASE_BRANCH"
        ;;
    behind | diverged)
        fail "$path pin $short moves backwards: $BASE_BRANCH already pins ${base_pin:0:9}." \
            "Merging this would revert execution bumps already on $BASE_BRANCH, which is usually a mistyped gitlink conflict resolution. Rebase on $BASE_BRANCH and keep its pin, or re-pin forwards."
        ;;
    *)
        ci_fault "could not classify the $path pin $short against $BASE_BRANCH: compare returned status '${direction:-<empty>}'." \
            "Re-run the job, and report this if it persists."
        ;;
    esac
}

url=$(git config -f .gitmodules --get "submodule.$SUBMODULE.url") || url=
path=$(git config -f .gitmodules --get "submodule.$SUBMODULE.path") || path=
if [ -z "$url" ] || [ -z "$path" ]; then
    fail "no $SUBMODULE submodule is declared in .gitmodules." \
        "If it was renamed or removed, update SUBMODULE in .github/workflows/exec-pin.sh in the same PR."
fi
EXEC_REPO=$(printf '%s' "$url" | sed -E 's#^.*github\.com[:/]##; s#\.git$##')
if [ "$EXEC_REPO" != "$EXEC_REPO_EXPECTED" ]; then
    fail "the $SUBMODULE submodule points at $EXEC_REPO, not $EXEC_REPO_EXPECTED." \
        "This gate only validates pins against $EXEC_REPO_EXPECTED. If the submodule legitimately moved, update EXEC_REPO_EXPECTED in .github/workflows/exec-pin.sh in the same PR."
fi

if ! tree=$(git ls-tree HEAD -- "$path"); then
    ci_fault "could not read the $path gitlink from HEAD." \
        "git ls-tree failed, see above. Check the checkout step, then re-run."
fi
pin=$(printf '%s\n' "$tree" | awk '$2 == "commit" { print $3 }')
if [ -z "$pin" ]; then
    fail ".gitmodules declares $SUBMODULE at $path, but HEAD records no gitlink there." \
        "If the submodule moved, update SUBMODULE in .github/workflows/exec-pin.sh in the same PR."
fi
short=${pin:0:9}

# compare/<base>...<pin> answers 404 for an unknown base as well as an unknown
# pin, so the base is confirmed first and that 404 then means only the pin.
rc=0
exec_api "branches/$EXEC_BASE" '.name' >/dev/null || rc=$?
case $rc in
0) ;;
44)
    ci_fault "$EXEC_REPO has no branch $EXEC_BASE, so the $path pin was never checked." \
        "Fix EXEC_BASE in .github/workflows/exec-pin.sh."
    ;;
*)
    ci_fault "could not reach $EXEC_REPO to confirm the $EXEC_BASE branch exists." \
        "The GitHub query failed, see above. Re-run the job."
    ;;
esac

rc=0
compare=$(exec_api "compare/$EXEC_BASE...$pin" '[.status, .behind_by] | @tsv') || rc=$?
case $rc in
0) ;;
44)
    fail "$path is pinned to $short, which does not exist in $EXEC_REPO." \
        "It was force-pushed away and garbage-collected, or was never pushed. Re-pin to a commit on $EXEC_BASE."
    ;;
*)
    ci_fault "could not verify the $path pin $short against $EXEC_REPO $EXEC_BASE." \
        "The GitHub query failed, see above. Re-run the job; if it persists, check the API rate limit and that $EXEC_REPO/$EXEC_BASE still exists."
    ;;
esac

read -r status behind <<<"$compare"

case "$status" in
identical | behind)
    echo "$path pin $short is on $EXEC_REPO $EXEC_BASE ($behind commits behind tip)"
    summary "Execution pin \`$short\` is on \`$EXEC_REPO\` \`$EXEC_BASE\`, $behind commits behind tip."
    check_progression
    exit 0
    ;;
ahead | diverged) ;;
*)
    ci_fault "could not classify the $path pin $short: $EXEC_REPO compare returned status '${status:-<empty>}'." \
        "The compare response was not in the expected form. Re-run the job, and report this if it persists."
    ;;
esac

rc=0
prs=$(exec_api "commits/$pin/pulls" '.') || rc=$?
if [ "$rc" -ne 0 ]; then
    fail "$path is pinned to $short, which is not on $EXEC_REPO $EXEC_BASE." \
        "The execution-PR lookup for $short also failed, so this report cannot name where it came from. The pin is still wrong: re-pin to a commit on $EXEC_BASE."
fi

merged=$(printf '%s' "$prs" | jq -r --arg base "$EXEC_BASE" \
    '[.[] | select(.merged_at != null and .base.ref == $base)][0] // empty
     | "  \(.html_url) was merged as \(.merge_commit_sha)"')
if [ -n "$merged" ]; then
    fail "$path is pinned to $short, whose execution PR is already merged -- but $short itself is not on $EXEC_REPO $EXEC_BASE." \
        "$(printf '%s\n' "$EXEC_REPO rebase-merges, so merging rewrote that commit and left $short behind:" "$merged" "" \
            "Re-pin to the merged commit, not to the PR branch head.")"
fi

open=$(printf '%s' "$prs" | jq -r '.[] | select(.state == "open") | "  \(.html_url) \(.title)"')
if [ -n "$open" ]; then
    fail "$path is pinned to $short, which is not on $EXEC_REPO $EXEC_BASE yet." \
        "$(printf '%s\n' "It belongs to:" "$open" "" \
            "Merge that execution work into $EXEC_BASE first, then re-pin to the resulting commit.")"
fi

other=$(printf '%s' "$prs" | jq -r \
    '.[] | "  \(.html_url) (\(if .merged_at then "merged into " + .base.ref else .state end))"')
if [ -n "$other" ]; then
    fail "$path is pinned to $short, which is not on $EXEC_REPO $EXEC_BASE and was never merged there." \
        "$(printf '%s\n' "It belongs to:" "$other" "" \
            "Re-pin to a commit on $EXEC_BASE.")"
fi

fail "$path is pinned to $short, which exists in $EXEC_REPO but is not on $EXEC_BASE and belongs to no execution PR." \
    "It was most likely force-pushed off its PR branch, in which case it will be garbage-collected and the pin will break master. Re-pin to the current head of that execution PR, or open a PR for the commit and merge it into $EXEC_BASE first."
