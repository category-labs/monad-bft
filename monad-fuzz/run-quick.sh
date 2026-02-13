#!/usr/bin/env bash
set -euo pipefail
shopt -s nullglob

GLOBAL_STATE_DIR="/tmp/fuzz-state"
DEFAULT_TIMEOUT=5m

# The script assumes running under monad-bft/monad-fuzz
cd "$(dirname "$0")"

build() {
    echo "Building fuzz targets"
    cargo afl build --release
}

apply_env() {
    # Apply environments from lines like "// KEY=VALUE # comment"
    local src="$1" target="$2" tmp
    tmp="$(mktemp)"
    sed -nE 's/\s*\/\/\s*(\w*)=([^#]*).*/\1=\2/p' "$src" > "$tmp"
    echo "[$target] Applying the following environments:"
    sed "s/^/[$target] /" "$tmp"

    set -o allexport
    source "$tmp"
    set +o allexport
    rm -f "$tmp"
}

# log to file, print up to the first N lines to stdout
log_head() {
    local max_count="$1" log_file="$2"
    tee "$log_file" | {
        head -n "$max_count"
        echo "... (log truncated, see $log_file for full log) ..."
        cat >/dev/null # absorb the rest of the input
    }
}

# Note: start a subshell for independent environments
fuzz() (
    local target="$1"
    local state_dir="$2"
    local target_src="fuzz_targets/${target}.rs"
    apply_env "$target_src" "$target"

    CORPUS_FILTER="${CORPUS_FILTER:-*}"
    EXTRA_OPTIONS="${EXTRA_OPTIONS:-}"
    TIMEOUT_QUICK="${TIMEOUT_QUICK:-$DEFAULT_TIMEOUT}"

    local corpus_dir="$state_dir/corpus"
    mkdir -p "$corpus_dir"
    echo "[$target] Preparing corpus in $corpus_dir"
    eval cp corpus/$CORPUS_FILTER "$corpus_dir"

    if [[ -z "$(find "$corpus_dir" -type f -maxdepth 1)" ]]; then
        echo "[$target] No corpus file matches CORPUS_FILTER"
        exit 1
    fi

    local binary="../target/release/$target"
    local afl_args=(-i "$corpus_dir" -o "$state_dir")

    if [[ -n "$EXTRA_OPTIONS" ]]; then
        # shellword-split EXTRA_OPTIONS preserving quoted sequences
        read -r -a extra_arr <<< "$EXTRA_OPTIONS"
        for x in "${extra_arr[@]}"; do
            afl_args+=("$x")
        done
    fi

    afl_args+=("--" "$binary")

    # allow running fuzzer without system tuning
    export AFL_SKIP_CPUFREQ=1
    export AFL_I_DONT_CARE_ABOUT_MISSING_CRASHES=1

    # enable cache for better performance
    export AFL_TESTCACHE_SIZE="${AFL_TESTCACHE_SIZE:-200}"

    # disable TUI
    export AFL_NO_UI="${AFL_NO_UI:-1}"

    # improve startup time
    export AFL_FAST_CAL="${AFL_FAST_CAL:-1}"

    local log_file="$state_dir/afl_fuzz.log"
    echo "[$target] Fuzzing started (timeout: $TIMEOUT_QUICK)"
    timeout --preserve-status --kill-after=5s --signal=INT "$TIMEOUT_QUICK" \
            cargo afl fuzz "${afl_args[@]}" 2>&1 | \
        log_head 500 "$log_file" | \
        sed "s/^/[$target]/" || true
)

fuzz_all() {
    # prepare a fresh global state directory
    rm -rf "$GLOBAL_STATE_DIR"
    mkdir -p "$GLOBAL_STATE_DIR"

    local fuzz_target_file target
    for fuzz_target_file in ./fuzz_targets/*.rs; do
        target="$(basename -s .rs "$fuzz_target_file")"

        state_dir="$GLOBAL_STATE_DIR/$target"
        mkdir -p "$state_dir"

        fuzz "$target" "$state_dir" &
    done

    local pids=($(jobs -p))
    echo "Waiting for fuzz workers (pid: ${pids[@]}) to finish..."
    [[ "${#pids[@]}" -eq 0 ]] || wait "${pids[@]}"
}

analyze() {
    local state_dir target stats_file saved_hangs saved_crashes
    state_dir="$1"
    target="$(basename "$state_dir")"

    stats_file="$state_dir/default/fuzzer_stats"

    if [[ ! -f "$stats_file" ]]; then
        echo "No fuzzer_stats found for $target, fuzzer may have failed to start."
        return 1
    fi

    saved_hangs="$(grep -oP 'saved_hangs.*\K\d+' "$stats_file")"
    saved_crashes="$(grep -oP 'saved_crashes.*\K\d+' "$stats_file")"

    if [[ "$saved_hangs" -eq 0 && "$saved_crashes" -eq 0 ]]; then
        echo "Fuzzer didn't find anything for $target!"

        # don't keep successful state directory, leave the failed ones
        # to archive for reproduction.
        rm -rf "$state_dir"
        return 0
    else
        # print all the detailed stats
        echo "Fuzzer found hangs/crashes for $target!"
        echo "=== Fuzzer stats summary ==="
        cat "$stats_file"

        # keep the original binary for reproduction
        cp "../target/release/$target" "$state_dir"

        return 1
    fi
}

analyze_all() {
    local failed_jobs=0
    for state_dir in $GLOBAL_STATE_DIR/*; do
        analyze "$state_dir" || failed_jobs=$((failed_jobs + 1))
    done

    [[ "$failed_jobs" -eq 0 ]] && return 0

    return 1
}

kill_bg_fuzzers() {
    local pids=($(jobs -p))
    [[ "${#pids[@]}" -eq 0 ]] || kill -INT "${pids[@]}"
}

main() {
    trap kill_bg_fuzzers SIGINT

    build
    fuzz_all
    analyze_all || exit 1
    exit 0
}

main
