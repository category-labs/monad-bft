import { Component, createEffect, createMemo, createSignal, For } from "solid-js";
import { SimulationQuery } from "../generated/graphql";

const minValidators = 1;
const maxValidators = 8;
const minStake = 1;
const maxStake = 1_000_000;

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

    const parsedStakes = createMemo(() => draftStakes().map((value) => Number(value)));
    const valid = createMemo(() => parsedStakes().every((stake) =>
        Number.isInteger(stake) && stake >= minStake && stake <= maxStake
    ));
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

    const apply = () => {
        if (!valid()) {
            return;
        }
        try {
            props.onApply(parsedStakes());
        } catch (err) {
            alert(err instanceof Error ? err.message : String(err));
        }
    };

    return (
        <aside class="w-80 shrink-0 overflow-auto border-l border-neutral-300 bg-white p-4">
            <h2 class="text-base font-semibold">Validators</h2>
            <p class="mt-1 text-xs leading-5 text-neutral-600">
                Relative stake controls quorum power and leader selection. Changes apply together at tick zero.
            </p>

            <div class="mt-4 grid gap-2">
                <For each={draftStakes()}>{(stake, index) => (
                    <label class="flex items-center justify-between gap-3 rounded border border-neutral-200 px-3 py-2 text-sm">
                        <span class="font-semibold">N{index() + 1}</span>
                        <span class="flex items-center gap-2 text-neutral-600">
                            Stake
                            <input
                                class="h-8 w-28 rounded border border-neutral-400 px-2 text-right text-neutral-950"
                                type="number"
                                min={minStake}
                                max={maxStake}
                                step="1"
                                value={stake}
                                aria-label={`Stake for N${index() + 1}`}
                                onInput={(event) => updateStake(index(), event.currentTarget.value)}
                            />
                        </span>
                    </label>
                )}</For>
            </div>

            <div class="mt-3 flex gap-2">
                <button
                    class="h-8 grow rounded border border-neutral-400 px-2 text-sm font-medium hover:bg-neutral-100 disabled:cursor-not-allowed disabled:opacity-40"
                    disabled={draftStakes().length >= maxValidators}
                    onClick={() => setDraftStakes((stakes) => [...stakes, "1"])}
                >
                    Add validator
                </button>
                <button
                    class="h-8 grow rounded border border-neutral-400 px-2 text-sm font-medium hover:bg-neutral-100 disabled:cursor-not-allowed disabled:opacity-40"
                    disabled={draftStakes().length <= minValidators}
                    onClick={() => setDraftStakes((stakes) => stakes.slice(0, -1))}
                >
                    Remove last
                </button>
            </div>

            <p class={`mt-3 min-h-5 text-xs ${valid() ? "text-neutral-600" : "font-medium text-red-700"}`}>
                {valid()
                    ? `${draftStakes().length} validator${draftStakes().length === 1 ? "" : "s"}; stake must be ${minStake.toLocaleString()}–${maxStake.toLocaleString()}.`
                    : `Each stake must be a whole number from ${minStake.toLocaleString()} to ${maxStake.toLocaleString()}.`}
            </p>

            <button
                class="mt-3 h-9 w-full rounded bg-indigo-600 px-3 text-sm font-semibold text-white hover:bg-indigo-700 disabled:cursor-not-allowed disabled:bg-neutral-300"
                disabled={!valid() || !dirty()}
                onClick={apply}
            >
                Apply &amp; reset
            </button>
            <p class="mt-2 text-xs leading-5 text-neutral-500">
                Applying clears simulation history and restores the default network topology.
            </p>
        </aside>
    );
};

export default ValidatorConfig;
