import { Component, createMemo, createSignal, onCleanup, Show } from 'solid-js';
import { SimulationDocument } from '../generated/graphql';
import { Simulation } from '../wasm'
import EventLog from './EventLog';
import Graph from './Graph'
import QueryEditor from './QueryEditor';

const Sim: Component = () => {
    const simulation = new Simulation();
    onCleanup(() => {
        simulation.free();
    });

    let epoch = 0;
    const [epochWatcher, setEpochWatcher] = createSignal(epoch);

    const setTick = (tick: number) => {
        simulation.setTick(tick);
        epoch += 1;
        setEpochWatcher(epoch);
    };

    const step = () => {
        simulation.step();
        epoch += 1;
        setEpochWatcher(epoch);
    };

    // force refresh of this whenever epoch changes
    const simulationSignal = createMemo(() => {
        epochWatcher()
        return simulation;
    }, simulation, { equals: false });

    const simData = createMemo(() => simulationSignal().fetchUnchecked(SimulationDocument));
    const tick = () => simData().currentTick;

    const [showEventLog, setShowEventLog] = createSignal(true);
    const toggleEventLog = () => setShowEventLog(!showEventLog())

    const [showQueryEditor, setShowQueryEditor] = createSignal(true);
    const toggleQueryEditor = () => setShowQueryEditor(!showQueryEditor())

    return (
        <>
            <input type="range" min="0" max="2000" value={tick()} onInput={(e) => setTick(parseInt(e.target.value))} />
            <div class="flex justify-between">
                <span>Current Tick: {tick()}</span>
                <button class="border" onClick={() => step()}>Step</button>
                <div>
                    <button class="border" onClick={toggleQueryEditor}>Toggle Query Editor</button>
                    <button class="border" onClick={toggleEventLog}>Toggle Event Log</button>
                </div>
            </div>
            <div class="grow flex flex-row min-h-0">
                <Show when={showQueryEditor()}>
                    <QueryEditor simulation={simulationSignal()} />
                </Show>
                <Graph simulation={simulationSignal()} />
                <Show when={showEventLog()}>
                    <EventLog simulation={simulationSignal()} />
                </Show>
            </div>
        </>
    )
};
export default Sim;