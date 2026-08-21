import { Component, createEffect, createMemo, createSignal, onCleanup, Show } from 'solid-js';
import { createStore, reconcile } from "solid-js/store"
import { SimulationDocument } from '../generated/graphql';
import { Simulation } from '../wasm'
import NetworkCanvas, { BlockSample } from './NetworkCanvas';
import NetworkMatrix from './NetworkMatrix';
import ValidatorConfig from './ValidatorConfig';
import { throttle } from '@solid-primitives/scheduled';

const maxTick = 2000;
const simThrottleMs = 16;
const simTimeScale = 1/40;
const playbackSpeeds = [0.25, 0.5, 1, 2];

const Sim: Component = () => {
    const simulation = new Simulation();
    onCleanup(() => {
        simulation.free();
    });
    const fetchSimulationData = () => simulation.fetchUnchecked(SimulationDocument);

    const [simData, setSimData] = createStore(fetchSimulationData())
    const [simulationVersion, setSimulationVersion] = createSignal(0);
    const refreshSimulationData = () => {
        setSimData(reconcile(fetchSimulationData(), { merge: true, key: 'id' }));
        setSimulationVersion(version => version + 1);
    };
    const [vizTick, setVizTick] = createSignal(0);
    const throttledUpdateSimData = throttle((simTick: number) => {
        simulation.setTick(simTick);
        refreshSimulationData();
    }, simThrottleMs);
    createEffect(() => {
        const simTick = Math.round(vizTick());
        throttledUpdateSimData(simTick);
    });

    const simulationSignal = () => {
        const _ = simData.currentTick;
        const __ = simulationVersion();
        return simulation;
    };

    const [blockSamples, setBlockSamples] = createSignal<BlockSample[]>([]);
    const finalizedRoot = createMemo(() => {
        const roots = simData.nodes.map((node) => node.root);
        return roots.length === 0 ? 0 : Math.max(...roots);
    });
    createEffect(() => {
        const tick = simData.currentTick;
        const root = finalizedRoot();
        setBlockSamples((samples) => {
            let next = samples.filter((sample) => sample.tick <= tick && sample.root <= root);
            const last = next.at(-1);
            if ((!last || root > last.root) && tick >= 0) {
                next = [...next, { tick, root }];
            }
            return next.slice(-32);
        });
    });

    const [playing, setPlaying] = createSignal(false);
    const [playbackSpeed, setPlaybackSpeed] = createSignal(1);
    let lastTimeMs = Date.now();
    let animationId;
    const animate = (currentTimeMs: number) => {
        if (playing()) {
            const scaledDiff = (currentTimeMs - lastTimeMs) * simTimeScale * playbackSpeed();
            setVizTick(Math.min(maxTick, vizTick() + scaledDiff));
        }
        lastTimeMs = currentTimeMs;
        animationId = requestAnimationFrame(animate);
    };
    animationId = requestAnimationFrame(animate);
    onCleanup(() => cancelAnimationFrame(animationId));

    const [openPanel, setOpenPanel] = createSignal<"network" | "validators">();

    const resetSimulation = () => {
        simulation.reset();
        setPlaying(false);
        setVizTick(0);
        setBlockSamples([]);
        refreshSimulationData();
    };

    const restartSimulation = () => {
        simulation.restart();
        setPlaying(false);
        setVizTick(0);
        setBlockSamples([]);
        refreshSimulationData();
    };

    const applyValidatorConfig = (stakes: number[]) => {
        simulation.applyValidatorConfig(stakes);
        setPlaying(false);
        setVizTick(0);
        setBlockSamples([]);
        refreshSimulationData();
    };

    return (
        <div class="flex h-full min-h-0 flex-col bg-neutral-100 text-neutral-950">
            <header class="flex shrink-0 items-center gap-3 border-b border-neutral-300 bg-white px-3 py-2">
                <div class="min-w-36 text-sm font-semibold">
                    Tick {Math.round(vizTick())}
                </div>
                <input
                    class="h-2 grow accent-indigo-600"
                    type="range"
                    min="0"
                    max={maxTick}
                    value={vizTick()}
                    onInput={e => setVizTick(parseInt(e.currentTarget.value))}
                />
                <div class="flex items-center gap-2">
                    <label class="flex h-8 items-center gap-1 rounded-md border border-neutral-400 px-2 text-sm" title="Playback speed">
                        <span class="sr-only">Playback speed</span>
                        <select
                            class="bg-transparent font-medium outline-none"
                            value={playbackSpeed()}
                            onChange={(e) => setPlaybackSpeed(Number(e.currentTarget.value))}
                            aria-label="Playback speed"
                        >
                            {playbackSpeeds.map((speed) => <option value={speed}>{speed}×</option>)}
                        </select>
                    </label>
                    <Show
                        when={!playing()}
                        fallback={
                            <button class="h-8 rounded-md border border-neutral-400 px-3 text-sm font-medium hover:bg-neutral-100" onClick={() => setPlaying(false)}>Stop</button>
                        }
                        >
                        <button class="h-8 rounded-md border border-neutral-400 px-3 text-sm font-medium hover:bg-neutral-100" onClick={() => setPlaying(true)}>Play</button>
                    </Show>
                    <button class="h-8 rounded-md border border-neutral-400 px-3 text-sm font-medium hover:bg-neutral-100" onClick={restartSimulation} title="Restart from the beginning with the current network configuration">Restart</button>
                    <button class="h-8 rounded-md border border-neutral-400 px-3 text-sm font-medium hover:bg-neutral-100" onClick={resetSimulation} title="Reset the simulation and network configuration">Reset</button>
                    <button
                        class={`h-8 rounded-md border px-3 text-sm font-medium ${openPanel() === "validators" ? "border-indigo-500 bg-indigo-50 text-indigo-700" : "border-neutral-400 hover:bg-neutral-100"}`}
                        onClick={() => setOpenPanel((panel) => panel === "validators" ? undefined : "validators")}
                    >
                        Validators
                    </button>
                    <button
                        class={`h-8 rounded-md border px-3 text-sm font-medium ${openPanel() === "network" ? "border-indigo-500 bg-indigo-50 text-indigo-700" : "border-neutral-400 hover:bg-neutral-100"}`}
                        onClick={() => setOpenPanel((panel) => panel === "network" ? undefined : "network")}
                    >
                        Network Config
                    </button>
                </div>
            </header>
            <div class="flex min-h-0 grow flex-row">
                <NetworkCanvas
                    simulation={simulationSignal()}
                    data={simData}
                    vizTick={vizTick()}
                    blockSamples={blockSamples()}
                    onChange={refreshSimulationData}
                />
                <Show when={openPanel() === "validators"}>
                    <ValidatorConfig data={simData} onApply={applyValidatorConfig} />
                </Show>
                <Show when={openPanel() === "network"}>
                    <NetworkMatrix
                        simulation={simulation}
                        data={simData}
                        onChange={refreshSimulationData}
                        onReset={resetSimulation}
                    />
                </Show>
            </div>
        </div>
    )
};
export default Sim;
