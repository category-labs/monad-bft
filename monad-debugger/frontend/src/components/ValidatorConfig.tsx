import { Component, createEffect, createMemo, createSignal, Index, Show } from "solid-js";
import { SimulationQuery } from "../generated/graphql";

const minValidators = 1;
const maxValidators = 5;
const minStake = 1;
const maxStake = 1_000_000;

const parseStake = (raw: string): number | null => {
    const text = raw.trim();
    if (!/^\d+$/.test(text)) {
        return null;
    }
    const stake = Number(text);
    return stake >= minStake && stake <= maxStake ? stake : null;
};

const formatShare = (share: number) => share < 0.05 ? "<0.1%" : `${share.toFixed(1)}%`;

const ValidatorConfig: Component<{
    data: SimulationQuery,
    onApply: (stakes: number[]) => void,
}> = (props) => {
    const [draftStakes, setDraftStakes] = createSignal<string[]>([]);
    let appliedSignature = "";

    createEffect(() => {
        const stakes = props.data.validatorConfig.map((validator) => validator.stake);
        const signature = stakes.join(":");
        if (signature !== appliedSignature) {
            appliedSignature = signature;
            setDraftStakes(stakes.map(String));
        }
    });

    const parsedStakes = createMemo(() => draftStakes().map(parseStake));
    const valid = createMemo(() => parsedStakes().every((stake) => stake !== null));
    const total = createMemo(() => parsedStakes().reduce<number>((sum, stake) => sum + (stake ?? 0), 0));
    // Strict two thirds of total stake, matching the weighted validator set the
    // simulation builds from these numbers.
    const quorum = createMemo(() => total() > 0 ? Math.floor((2 * total()) / 3) + 1 : 0);

    const rows = createMemo(() => {
        const count = draftStakes().length;
        const sum = total();
        const need = quorum();
        return parsedStakes().map((stake) => {
            if (stake === null) {
                return { valid: false, share: 0, reachesQuorum: false, blocksQuorum: false };
            }
            // Derived from the same integer quorum as the readout: comparing a rounded
            // share against a third is off by one when the stakes divide evenly.
            return {
                valid: true,
                share: sum > 0 ? (stake / sum) * 100 : 0,
                reachesQuorum: count > 1 && stake >= need,
                blocksQuorum: count > 1 && sum - stake < need,
            };
        });
    });

    const concentration = createMemo(() => {
        let worst: { label: string, share: number, critical: boolean } | undefined;
        rows().forEach((row, index) => {
            if (!row.reachesQuorum && !row.blocksQuorum) {
                return;
            }
            if (!worst || row.share > worst.share) {
                worst = { label: `N${index + 1}`, share: row.share, critical: row.reachesQuorum };
            }
        });
        return worst;
    });

    const dirty = createMemo(() => (
        valid() && (
            parsedStakes().some(
                (stake, index) => stake !== props.data.validatorConfig[index]?.stake
            ) || draftStakes().length !== props.data.validatorConfig.length
        )
    ));

    const updateStake = (index: number, value: string) => {
        setDraftStakes((stakes) => stakes.map((stake, stakeIndex) =>
            stakeIndex === index ? value : stake
        ));
    };

    const removeStake = (index: number) => {
        setDraftStakes((stakes) => stakes.length <= minValidators
            ? stakes
            : stakes.filter((_, stakeIndex) => stakeIndex !== index));
    };

    const addStake = () => {
        setDraftStakes((stakes) => stakes.length >= maxValidators
            ? stakes
            : [...stakes, String(minStake)]);
    };

    const apply = () => {
        if (!valid()) {
            return;
        }
        try {
            props.onApply(parsedStakes() as number[]);
        } catch (err) {
            alert(err instanceof Error ? err.message : String(err));
        }
    };

    const shareClass = (index: number) => {
        const row = rows()[index];
        if (!row?.valid) {
            return "text-neutral-400";
        }
        if (row.reachesQuorum) {
            return "font-semibold text-red-700";
        }
        if (row.blocksQuorum) {
            return "font-semibold text-amber-700";
        }
        return "text-neutral-600";
    };

    const barClass = (index: number) => {
        const row = rows()[index];
        if (row?.reachesQuorum) {
            return "bg-red-600";
        }
        if (row?.blocksQuorum) {
            return "bg-amber-500";
        }
        return "bg-indigo-500";
    };

    return (
        <aside class="flex w-80 shrink-0 flex-col border-l border-neutral-300 bg-white">
            <div class="shrink-0 border-b border-neutral-300 p-3">
                <div class="flex items-center gap-2">
                    <h2 class="grow text-base font-semibold leading-tight">Validators</h2>
                    <span class="text-xs tabular-nums text-neutral-600">
                        {draftStakes().length} of {maxValidators}
                    </span>
                    <button
                        class="flex h-7 w-7 shrink-0 items-center justify-center rounded border border-neutral-400 text-neutral-700 hover:bg-neutral-100 disabled:cursor-not-allowed disabled:opacity-40 disabled:hover:bg-white"
                        aria-label="Add validator"
                        title="Add validator"
                        disabled={draftStakes().length >= maxValidators}
                        onClick={addStake}
                    >
                        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" aria-hidden="true">
                            <path d="M12 5 L12 19" />
                            <path d="M5 12 L19 12" />
                        </svg>
                    </button>
                </div>
                <div class="mt-1 text-xs leading-4 text-neutral-600">
                    {valid()
                        ? `Quorum needs ${quorum().toLocaleString()} of ${total().toLocaleString()} stake`
                        : "Fix the highlighted stakes to see quorum"}
                </div>
            </div>

            <div class="min-h-0 grow overflow-auto">
                <Index each={draftStakes()}>{(stake, index) => (
                    <div class="border-b border-neutral-200 px-3 py-2">
                        <div class="flex items-center gap-2">
                            <span class="w-6 shrink-0 text-sm font-semibold">N{index + 1}</span>
                            <input
                                class={`h-8 w-[5.5rem] shrink-0 rounded border px-2 text-right text-sm tabular-nums ${
                                    rows()[index]?.valid
                                        ? "border-neutral-400 text-neutral-950"
                                        : "border-red-600 bg-red-50 text-red-800"
                                }`}
                                type="text"
                                inputmode="numeric"
                                autocomplete="off"
                                spellcheck={false}
                                aria-label={`Stake for N${index + 1}`}
                                value={stake()}
                                onInput={(event) => updateStake(index, event.currentTarget.value)}
                            />
                            <span class={`grow text-right text-xs tabular-nums ${shareClass(index)}`}>
                                {rows()[index]?.valid ? formatShare(rows()[index].share) : "--"}
                            </span>
                            <button
                                class="flex h-7 w-7 shrink-0 items-center justify-center rounded text-neutral-500 hover:bg-red-50 hover:text-red-700 disabled:cursor-not-allowed disabled:text-neutral-300 disabled:hover:bg-transparent disabled:hover:text-neutral-300"
                                aria-label={`Remove N${index + 1}`}
                                title={`Remove N${index + 1}`}
                                disabled={draftStakes().length <= minValidators}
                                onClick={() => removeStake(index)}
                            >
                                <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
                                    <path d="M6 6 L18 18" />
                                    <path d="M18 6 L6 18" />
                                </svg>
                            </button>
                        </div>
                        <div class="mt-1.5 h-[3px] overflow-hidden rounded-sm bg-neutral-200">
                            <div
                                class={`h-full rounded-sm ${barClass(index)}`}
                                style={{ width: `${(rows()[index]?.share ?? 0).toFixed(2)}%` }}
                            />
                        </div>
                    </div>
                )}</Index>
            </div>

            <div class="shrink-0 border-t border-neutral-300 p-3">
                <Show when={concentration()}>
                    {(worst) => (
                        <div class={`mb-2 rounded border px-2 py-1.5 text-xs leading-4 ${
                            worst().critical
                                ? "border-red-300 bg-red-50 text-red-800"
                                : "border-amber-300 bg-amber-50 text-amber-800"
                        }`}>
                            {worst().label} holds {formatShare(worst().share)} of the stake — it can{" "}
                            {worst().critical ? "reach quorum on its own" : "block every quorum on its own"}.
                        </div>
                    )}
                </Show>
                <Show when={!valid()}>
                    <div class="mb-2 rounded border border-red-300 bg-red-50 px-2 py-1.5 text-xs leading-4 text-red-800">
                        Each stake must be a whole number from {minStake.toLocaleString()} to {maxStake.toLocaleString()}.
                    </div>
                </Show>
                <button
                    class="h-9 w-full rounded bg-indigo-600 px-3 text-sm font-semibold text-white hover:bg-indigo-700 disabled:cursor-not-allowed disabled:bg-neutral-300"
                    disabled={!valid() || !dirty()}
                    onClick={apply}
                >
                    {dirty() ? "Apply & reset" : "Applied"}
                </button>
                <div class="mt-2 text-xs leading-5 text-neutral-500">
                    Applying clears simulation history and restores the default network topology.
                </div>
            </div>
        </aside>
    );
};

export default ValidatorConfig;
