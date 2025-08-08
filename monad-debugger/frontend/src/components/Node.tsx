import { Component } from "solid-js";
import { NodeFragment } from "../generated/graphql";

const Node: Component<{
    currentTick: number,
    node: NodeFragment,
    idx: number,
    isCurrentLeader: boolean,
    isNextLeader: boolean,
}> = (props) => {
    const node = () => props.node;
    const timeoutProgress = () => (props.currentTick - node().roundTimerStartedAt) / (node().roundTimerEndsAt - node().roundTimerStartedAt);
    const scaledTimeoutProgress = () => 1 - Math.exp(-1 * timeoutProgress());
    return (
        <div class="p-0.5 rounded-2xl transition-all" style={{
            '--progress': `${100 * timeoutProgress()}%`,
            background: `conic-gradient(from 0deg, rgba(255,0,0,${scaledTimeoutProgress()}) 0%, rgba(255,0,0,${scaledTimeoutProgress()}) var(--progress), transparent var(--progress))`,
            transform: props.isCurrentLeader ? 'scale(1.10)' : props.isNextLeader ? 'scale(1)' : 'scale(0.95)',
            filter: props.node.isBlocked ? 'blur(1px) contrast(.5)' : '',
        }}>
            <div class="min-w-28 lg:min-w-56 p-2 lg:p-4 text-nowrap rounded-2xl border-2 border-black shadow-md" style={{
                'background-color': props.isCurrentLeader ? 'rgb(165 180 252 / var(--tw-bg-opacity, 1))' : props.isNextLeader ? 'rgb(224 231 255 / var(--tw-bg-opacity, 1))' : 'rgb(229 231 235 / var(--tw-bg-opacity, 1))',
            }}>
                <div class="text-xl lg:text-3xl center">
                    {/* Node {formatNodeId(node().id)} */}
                    Node {props.idx + 1}
                </div>
                <div>
                    <span class="hidden lg:inline">Current round: </span>
                    <span class="inline lg:hidden">Round: </span>
                    {node().currentRound}
                </div>
                <div>
                    <span class="hidden lg:inline">Latest finalized: </span>
                    <span class="inline lg:hidden">Finalized: </span>
                    {node().root}
                </div>
                <div class="hidden lg:block">
                    Timeout progress: {Math.round(100 * timeoutProgress())}%
                </div>
            </div>
        </div>
    )
};

export default Node;
