import { Component, createMemo, For, Show } from "solid-js";
import { GraphDocument } from "../generated/graphql";
import { Simulation } from "../wasm";
import { formatNodeId } from "../utils";

const Graph: Component<{
    simulation: Simulation,
}> = (props) => {
    const graph = createMemo(() => props.simulation.fetchUnchecked(GraphDocument));
    const currentTick = () => graph().currentTick;
    const nodes = () => graph().nodes;
    const unitPositions = createMemo(() => {
        const positions: {
            [id: string]: [number, number]
        } = {};
        for (const [i, node] of nodes().entries()) {
            const fraction = i / nodes().length;
            const angle = fraction * 2 * Math.PI;
            const unitPosition: [number, number] = [Math.sin(angle), Math.cos(angle)];
            positions[node.id] = unitPosition;
        }
        return positions;
    });

    const positionTransform = (unitPosition: [number, number]) => {
        // cqw/cqh is a new feature that lets you size based on parent container-type
        const scale = (value: number) => `calc(max(-40cqw, -40cqh) * ${value})`;
        return `transform: translate(${scale(unitPosition[0])}, ${scale(unitPosition[1])})`;
    };

    const interpolatePosition = (scale: number, pos1: [number, number], pos2: [number, number]) => {
        const newX = pos1[0] + scale * (pos2[0] - pos1[0]);
        const newY = pos1[1] + scale * (pos2[1] - pos1[1]);
        const newPos: [number, number] = [newX, newY];
        return newPos;
    }

    return (
        <div class="h-full grow" style="container-type: size">
            <For each={nodes()}>{node =>
                <div class="absolute left-1/2 top-1/2">
                    <div class="absolute" style={positionTransform(unitPositions()[node.id])}>
                        <div class="-translate-x-1/2 -translate-y-1/2 border">
                            <div>
                                Node {formatNodeId(node.id)}
                            </div>
                            <div>
                                # pending: {node.pendingMessages.length}
                            </div>
                        </div>
                    </div>
                    <For each={node.pendingMessages}>{message =>
                        <Show when={node.id != message.fromId}>
                            <div class="absolute" style={
                                positionTransform(
                                    interpolatePosition(
                                        (currentTick() - message.fromTick) / (message.rxTick - message.fromTick),
                                        unitPositions()[message.fromId],
                                        unitPositions()[node.id]
                                    )
                                )
                            }>
                                <div class="-translate-x-1/2 -translate-y-1/2 border rounded-full" style ={
                                    `font-size: ${Math.log(message.size) / 5}rem`
                                }>
                                    {message.size}
                                </div>
                            </div>
                        </Show>
                    }</For>
                </div>
            }</For>
        </div>
    );
};

export default Graph;
