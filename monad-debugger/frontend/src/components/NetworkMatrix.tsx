import { Component, createMemo, For, onCleanup, Show } from "solid-js";
import { SimulationQuery } from "../generated/graphql";
import { Simulation } from "../wasm";

type NetworkLink = SimulationQuery["networkConfig"]["links"][number];

const pairKey = (fromId: string, toId: string) => `${fromId}:${toId}`;

const shortNodeId = (id: string) => `${id.slice(0, 6)}...${id.slice(-4)}`;

const parseLatency = (value: string): number | undefined => {
    const latency = Number.parseInt(value, 10);
    if (!Number.isFinite(latency) || latency <= 0) {
        return undefined;
    }
    return latency;
};

const NetworkMatrix: Component<{
    simulation: Simulation,
    data: SimulationQuery,
    onChange: () => void,
    onReset: () => void,
}> = (props) => {
    const nodes = () => props.data.nodes;
    const linkByPair = createMemo(() => {
        const links: Record<string, NetworkLink> = {};
        for (const link of props.data.networkConfig.links) {
            links[pairKey(link.fromId, link.toId)] = link;
        }
        return links;
    });
    const latestCommand = () => props.data.networkCommandLog.at(-1);
    const commitTimers = new Map<string, number>();

    onCleanup(() => {
        for (const timer of commitTimers.values()) {
            window.clearTimeout(timer);
        }
    });

    const apply = (change: () => void) => {
        try {
            change();
            props.onChange();
        } catch (err) {
            alert(err instanceof Error ? err.message : String(err));
        }
    };

    const commitDefaultLatency = (value: string) => {
        if (value.trim() === "") {
            return;
        }
        const latency = parseLatency(value);
        if (latency === undefined) {
            alert("Latency must be greater than zero");
            return;
        }
        if (latency !== props.data.networkConfig.defaultLatency) {
            apply(() => props.simulation.setDefaultLatency(latency));
        }
    };

    const commitLinkLatency = (link: NetworkLink, value: string) => {
        if (value.trim() === "") {
            return;
        }
        const latency = parseLatency(value);
        if (latency === undefined) {
            alert("Latency must be greater than zero");
            return;
        }
        if (latency !== link.latency) {
            apply(() => props.simulation.setLinkLatency(link.fromId, link.toId, latency));
        }
    };

    const scheduleCommit = (key: string, commit: () => void) => {
        const existing = commitTimers.get(key);
        if (existing !== undefined) {
            window.clearTimeout(existing);
        }
        const timer = window.setTimeout(() => {
            commitTimers.delete(key);
            commit();
        }, 300);
        commitTimers.set(key, timer);
    };

    return (
        <aside class="w-[34rem] max-w-[62vw] shrink-0 border-l border-neutral-300 bg-white overflow-auto">
            <div class="sticky top-0 z-20 border-b border-neutral-300 bg-white p-3">
                <div class="flex items-center justify-between gap-3">
                    <div>
                        <h2 class="text-base font-semibold leading-tight">Network Matrix</h2>
                        <div class="text-xs text-neutral-600">{props.data.networkCommandLog.length} commands</div>
                    </div>
                    <button
                        class="h-8 rounded border border-neutral-400 px-3 text-sm hover:bg-neutral-100"
                        onClick={props.onReset}
                    >
                        Reset
                    </button>
                </div>
                <label class="mt-3 flex items-center gap-2 text-sm">
                    <span class="w-28 text-neutral-700">Default latency</span>
                    <input
                        class="h-8 w-24 rounded border border-neutral-400 px-2 text-right"
                        type="number"
                        min="1"
                        value={props.data.networkConfig.defaultLatency}
                        onInput={(e) => {
                            const value = e.currentTarget.value;
                            scheduleCommit("default", () => commitDefaultLatency(value));
                        }}
                        onKeyDown={(e) => {
                            if (e.key === "Enter") {
                                e.preventDefault();
                                e.currentTarget.blur();
                            }
                        }}
                    />
                    <span class="text-neutral-600">ms</span>
                </label>
                <Show when={latestCommand()}>
                    {(command) => (
                        <div class="mt-2 truncate text-xs text-neutral-600">
                            Last: {command().kind} @ {command().tick}ms
                        </div>
                    )}
                </Show>
            </div>
            <table class="w-full border-collapse text-sm">
                <thead>
                    <tr>
                        <th class="border-b border-r border-neutral-300 bg-white p-2 text-left font-medium">
                            from / to
                        </th>
                        <For each={nodes()}>{(node, idx) => (
                            <th class="border-b border-neutral-300 bg-white p-2 text-center font-medium" title={node.id}>
                                N{idx() + 1}
                            </th>
                        )}</For>
                    </tr>
                </thead>
                <tbody>
                    <For each={nodes()}>{(fromNode, rowIdx) => (
                        <tr>
                            <th class="border-b border-r border-neutral-300 bg-white p-2 text-left font-medium" title={fromNode.id}>
                                <div>N{rowIdx() + 1}</div>
                                <div class="text-xs font-normal text-neutral-500">{shortNodeId(fromNode.id)}</div>
                            </th>
                            <For each={nodes()}>{(toNode) => (
                                <Show
                                    when={fromNode.id !== toNode.id}
                                    fallback={<td class="border-b border-neutral-200 bg-neutral-100 p-2 text-center text-neutral-400">-</td>}
                                >
                                    <NetworkMatrixCell
                                        link={linkByPair()[pairKey(fromNode.id, toNode.id)]}
                                        onLatency={(link, value) => {
                                            scheduleCommit(pairKey(link.fromId, link.toId), () => {
                                                commitLinkLatency(link, value);
                                            });
                                        }}
                                        onDropped={(link, dropped) => apply(() => props.simulation.setLinkDropped(link.fromId, link.toId, dropped))}
                                        onClear={(link) => apply(() => props.simulation.clearLinkRule(link.fromId, link.toId))}
                                    />
                                </Show>
                            )}</For>
                        </tr>
                    )}</For>
                </tbody>
            </table>
        </aside>
    );
};

const NetworkMatrixCell: Component<{
    link: NetworkLink,
    onLatency: (link: NetworkLink, value: string) => void,
    onDropped: (link: NetworkLink, dropped: boolean) => void,
    onClear: (link: NetworkLink) => void,
}> = (props) => (
    <td class={`border-b border-neutral-200 p-2 align-top ${props.link.dropped ? "bg-red-50" : props.link.overridden ? "bg-amber-50" : "bg-white"}`}>
        <div class="flex items-center gap-2">
            <input
                class="h-7 w-16 rounded border border-neutral-400 px-1 text-right text-sm"
                type="number"
                min="1"
                value={props.link.latency}
                disabled={props.link.dropped}
                onInput={(e) => {
                    const value = e.currentTarget.value;
                    props.onLatency(props.link, value);
                }}
                onKeyDown={(e) => {
                    if (e.key === "Enter") {
                        e.preventDefault();
                        e.currentTarget.blur();
                    }
                }}
            />
            <span class="text-xs text-neutral-500">ms</span>
        </div>
        <label class="mt-2 flex items-center gap-2 text-xs">
            <input
                type="checkbox"
                checked={props.link.dropped}
                onChange={(e) => props.onDropped(props.link, e.currentTarget.checked)}
            />
            Drop
        </label>
        <button
            class="mt-2 h-7 rounded border border-neutral-300 px-2 text-xs disabled:cursor-not-allowed disabled:opacity-40"
            disabled={!props.link.overridden}
            onClick={() => props.onClear(props.link)}
        >
            Clear
        </button>
    </td>
);

export default NetworkMatrix;
