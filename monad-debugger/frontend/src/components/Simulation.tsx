import { Component, createEffect, createMemo, createSignal, onCleanup, Show } from 'solid-js';
import { createStore, reconcile } from "solid-js/store"
import { SimulationDocument } from '../generated/graphql';
import { Simulation } from '../wasm'
import EventLog from './EventLog';
import Graph from './Graph'
import QueryEditor from './QueryEditor';
import { throttle } from '@solid-primitives/scheduled';

const maxTick = 2000;
const simThrottleMs = 10;
const simTimeScale = 1/40;

const Sim: Component = () => {
    const simulation = new Simulation();
    onCleanup(() => {
        simulation.free();
    });
    const fetchSimulationData = () => simulation.fetchUnchecked(SimulationDocument);

    const [simData, setSimData] = createStore(fetchSimulationData())
    const [vizTick, setVizTick] = createSignal(0);
    const throttledUpdateSimData = throttle((simTick: number) => {
        simulation.setTick(simTick);
        setSimData(reconcile(fetchSimulationData(), { merge: true, key: 'id' }));
    }, simThrottleMs);
    createEffect(() => {
        const simTick = Math.round(vizTick());
        throttledUpdateSimData(simTick);
    });

    const simulationSignal = () => {
        const _ = simData.currentTick;
        return simulation;
    };

    const [playing, setPlaying] = createSignal(false);
    let lastTimeMs = Date.now();
    let animationId;
    const animate = (currentTimeMs: number) => {
        if (playing()) {
            const scaledDiff = (currentTimeMs - lastTimeMs) * simTimeScale;
            setVizTick((vizTick() + scaledDiff) % maxTick);
        }
        lastTimeMs = currentTimeMs;
        animationId = requestAnimationFrame(animate);
    };
    animationId = requestAnimationFrame(animate);
    onCleanup(() => cancelAnimationFrame(animationId));

    const [showEventLog, setShowEventLog] = createSignal(false);
    const toggleEventLog = () => setShowEventLog(!showEventLog())

    const [showQueryEditor, setShowQueryEditor] = createSignal(false);
    const toggleQueryEditor = () => setShowQueryEditor(!showQueryEditor())

    return (
        <>
            <input type="range" min="0" max={maxTick} value={vizTick()} onInput={e => setVizTick(parseInt(e.target.value))} />
            <div class="flex justify-between">
                <div class="min-w-40">
                    <span class="ml-2 mr-2">Current Tick: {Math.round(vizTick())}</span>
                </div>
                <div>
                </div>
                <div>
                    {/* <button class="border ml-2 mr-2" onClick={toggleQueryEditor}>Toggle Query Editor</button>
                    <button class="border ml-2 mr-2" onClick={toggleEventLog}>Toggle Event Log</button> */}
                    <Show
                        when={!playing()}
                        fallback={
                            <button class="border ml-2 mr-2" onClick={() => setPlaying(false)}>Stop</button>
                        }
                        >
                        <button class="border ml-2 mr-2" onClick={() => setPlaying(true)}>Play</button>
                    </Show>
                </div>
            </div>
            <div class="grow flex flex-row min-h-0">
                {/* <Show when={showQueryEditor()}>
                    <QueryEditor simulation={simulationSignal()} />
                </Show> */}
                <Graph vizTick={vizTick()} simulation={simulationSignal()} />
                {/* <Show when={showEventLog()}>
                    <EventLog simulation={simulationSignal()} />
                </Show> */}
            </div>
        </>
    )
};
export default Sim;