import { Component, createEffect, createMemo, createSignal, For, onCleanup, onMount, Show, untrack } from "solid-js";
import { SimulationQuery } from "../generated/graphql";
import { Simulation } from "../wasm";

export type BlockSample = {
    tick: number,
    root: number,
};

type NetworkNode = SimulationQuery["networkConfig"]["nodes"][number];
type NetworkLink = SimulationQuery["networkConfig"]["links"][number];
type LinkPair = {
    fromId: string,
    toId: string,
    links: (NetworkLink & { latency: number })[],
};
type SimNode = SimulationQuery["nodes"][number];
type PendingMessage = SimNode["pendingMessages"][number];
type LedgerBlock = SimNode["ledgerBlocks"][number];
type Blockish = {
    id: string,
    parentId?: string | null,
    bodyId?: string | null,
    seqNum?: number,
    round?: number,
    authorId?: string | null,
};
type MergedLedgerBlock = LedgerBlock & {
    seenBy: string[],
    finalizedBy: string[],
    coherentBy: string[],
    votedBy: string[],
};
type LinkMode = "both" | "forward" | "reverse" | "neither";
type MessageTone = "proposal" | "vote" | "timeout" | "blocksync" | "advance" | "tx" | "state" | "neutral";
type MessageFilterKey =
    | "proposal"
    | "vote"
    | "timeout"
    | "advance"
    | "roundRecovery"
    | "noEndorsement"
    | "blockSyncRequest"
    | "blockSyncResponse"
    | "forwardedTx"
    | "stateSync"
    | "other";
type Position = { x: number, y: number };
type PixelOffset = { x: number, y: number };
type ViewScale = { x: number, y: number };
type WireLaneTone = "neutral" | "amber";
type WireLane = {
    key: string,
    latency: number,
    // +1 travels fromId -> toId, -1 travels toId -> fromId
    travel: 1 | -1,
    // perpendicular lane: 0 keeps the label on the wire, ±1 straddles it
    side: number,
    arrow: boolean,
    tone: WireLaneTone,
};
type InFlightDestination = {
    toId: string,
    messages: Array<{ fromId: string, message: PendingMessage }>,
};
type TimeoutState = {
    progress: number,
    remainingMs: number,
    percent: number,
};

const topologyViewSize = 1000;
const topologyMin = -500;
const topologyMax = 1500;
const clickSlopPx = 4;
const minCanvasZoom = 0.55;
const maxCanvasZoom = 1.6;
const zoomButtonStep = 0.1;
const wheelZoomSensitivity = 0.004;
const pinchZoomSensitivity = 0.007;
const wheelIgnoreSelector = "[data-canvas-wheel-ignore]";
const linkHitAreaWidth = 32;
const cutCableGap = 42;
const cutCableStrands = [-22, -14, -7, 0, 8, 15, 22];
// Wire label geometry, in unzoomed screen pixels
const wireArrowLength = 52;
const wireArrowHeight = 12;
const wireArrowAlong = 26;
const wireChipAlong = 32;
const wireLaneOffset = 15;
// Wires shorter than this shrink their labels so the pair stays readable
const wireLabelFitLength = 150;
const wireChipClass = "rounded-md border px-2 py-0.5 text-center text-[12px] font-semibold tabular-nums shadow-sm";
// Packets ride beside their wire, offset in screen pixels
const packetLane = 10;
const packetLaneStagger = 3.5;

const clamp = (value: number, min: number, max: number) => Math.max(min, Math.min(max, value));

// Hovering a wire swaps the pointer for the action it will perform, so the
// native cursor never sits on top of a separate badge.
const actionCursorSize = 40;
const actionCursor = (color: string, shapes: string, viewBox = 24, stroke = 2.4, halo = 5) => {
    const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="${actionCursorSize}" height="${actionCursorSize}" viewBox="0 0 ${viewBox} ${viewBox}" fill="none" stroke-linecap="round" stroke-linejoin="round"><g stroke="#ffffff" stroke-width="${halo}" opacity="0.85">${shapes}</g><g stroke="${color}" stroke-width="${stroke}">${shapes}</g></svg>`;
    const hotspot = actionCursorSize / 2;
    return `url("data:image/svg+xml,${encodeURIComponent(svg)}") ${hotspot} ${hotspot}, pointer`;
};

const scissorsShapes = '<circle cx="6" cy="6" r="3"/><circle cx="6" cy="18" r="3"/><path d="M20 4 8.12 15.88"/><path d="M14.47 14.48 20 20"/><path d="M8.12 8.12 12 12"/>';
const wrenchShapes = '<path d="M14.7 6.3a1 1 0 0 0 0 1.4l1.6 1.6a1 1 0 0 0 1.4 0l3.77-3.77a6 6 0 0 1-7.94 7.94l-6.91 6.91a2.12 2.12 0 0 1-3-3l6.91-6.91a6 6 0 0 1 7.94-7.94l-3.76 3.76z"/>';
const cutCursor = actionCursor("#dc2626", scissorsShapes);
const repairCursor = actionCursor("#16a34a", wrenchShapes);

// Taking a node down is the node-level analogue of severing a wire, so it gets
// the same cursor treatment: one cable, pulled apart or mated. Laid out on the
// diagonal so the cable spans the cursor box corner to corner rather than
// leaving the top and bottom thirds empty.
const cableCursorBox = 32;
const onDiagonal = (shapes: string) => `<g transform="rotate(-45 16 16)">${shapes}</g>`;
const unplugShapes = onDiagonal('<path d="M2 16h5"/><rect x="7" y="11" width="9" height="10" rx="2"/><path d="M16 13.5h3"/><path d="M16 18.5h3"/><rect x="22.5" y="11" width="6.5" height="10" rx="2.5"/><path d="M29 16h1.5"/>');
const plugShapes = onDiagonal('<path d="M2 16h6.25"/><rect x="8.25" y="11" width="9" height="10" rx="2"/><rect x="17.25" y="11" width="6.5" height="10" rx="2.5"/><path d="M23.75 16h6.75"/>');
const takeDownCursor = actionCursor("#dc2626", unplugShapes, cableCursorBox, 2.6, 5.5);
const bringUpCursor = actionCursor("#16a34a", plugShapes, cableCursorBox, 2.6, 5.5);

// The topology svg is stretched to the canvas (preserveAspectRatio="none"), so
// view units are not square on screen. Work out the wire's on-screen frame and
// convert pixel offsets back into view units, otherwise "perpendicular" and
// arrow angles drift as the canvas aspect changes.
const screenFrame = (from: Position, to: Position, scale: ViewScale) => {
    const dx = (to.x - from.x) * scale.x;
    const dy = (to.y - from.y) * scale.y;
    const length = Math.max(1, Math.hypot(dx, dy));
    const tangent = { x: dx / length, y: dy / length };
    const normal = { x: -tangent.y, y: tangent.x };
    return {
        angle: Math.atan2(dy, dx),
        length,
        offset: (base: Position, along: number, out: number): Position => ({
            x: base.x + (tangent.x * along + normal.x * out) / scale.x,
            y: base.y + (tangent.y * along + normal.y * out) / scale.y,
        }),
    };
};

const pointAlongLine = (from: Position, to: Position, distanceFromMidpoint: number) => {
    const dx = to.x - from.x;
    const dy = to.y - from.y;
    const length = Math.max(1, Math.hypot(dx, dy));
    return {
        x: (from.x + to.x) / 2 + (dx / length) * distanceFromMidpoint,
        y: (from.y + to.y) / 2 + (dy / length) * distanceFromMidpoint,
    };
};

const pairKey = (fromId: string, toId: string) => [fromId, toId].sort().join(":");

const shortNodeId = (id: string) => `${id.slice(0, 6)}...${id.slice(-4)}`;

const shortBlockId = (id: string) => `${id.slice(0, 4)}..${id.slice(-4)}`;

const messageFilterOptions: { key: MessageFilterKey, label: string }[] = [
    { key: "proposal", label: "Proposals" },
    { key: "vote", label: "Votes" },
    { key: "timeout", label: "Timeouts" },
    { key: "advance", label: "Advance round" },
    { key: "roundRecovery", label: "Round recovery" },
    { key: "noEndorsement", label: "No endorsement" },
    { key: "blockSyncRequest", label: "Block sync requests" },
    { key: "blockSyncResponse", label: "Block sync responses" },
    { key: "forwardedTx", label: "Forwarded tx" },
    { key: "stateSync", label: "State sync" },
    { key: "other", label: "Other" },
];

const defaultMessageFilters: Record<MessageFilterKey, boolean> = {
    proposal: true,
    vote: true,
    timeout: true,
    advance: false,
    roundRecovery: true,
    noEndorsement: true,
    blockSyncRequest: true,
    blockSyncResponse: true,
    forwardedTx: true,
    stateSync: true,
    other: true,
};

const messageFilterKey = (message: PendingMessage["message"]): MessageFilterKey => {
    if (message.__typename !== "GraphQLConsensusMessage") {
        switch (message.__typename) {
            case "GraphQLBlockSyncRequestMessage":
                return "blockSyncRequest";
            case "GraphQLBlockSyncResponseMessage":
                return "blockSyncResponse";
            case "GraphQLForwardedTxMessage":
                return "forwardedTx";
            case "GraphQLStateSyncMessage":
                return "stateSync";
            default:
                return "other";
        }
    }

    switch (message.message.__typename) {
        case "GraphQLProposal":
            return "proposal";
        case "GraphQLVote":
            return "vote";
        case "GraphQLTimeout":
            return "timeout";
        case "GraphQLAdvanceRound":
            return "advance";
        case "GraphQLRoundRecovery":
            return "roundRecovery";
        case "GraphQLNoEndorsement":
            return "noEndorsement";
        default:
            return "other";
    }
};

const messageTone = (message: PendingMessage["message"]): MessageTone => {
    if (message.__typename !== "GraphQLConsensusMessage") {
        switch (message.__typename) {
            case "GraphQLBlockSyncRequestMessage":
            case "GraphQLBlockSyncResponseMessage":
                return "blocksync";
            case "GraphQLForwardedTxMessage":
                return "tx";
            case "GraphQLStateSyncMessage":
                return "state";
            default:
                return "neutral";
        }
    }

    switch (message.message.__typename) {
        case "GraphQLProposal":
            return "proposal";
        case "GraphQLVote":
            return "vote";
        case "GraphQLTimeout":
            return "timeout";
        case "GraphQLAdvanceRound":
        case "GraphQLRoundRecovery":
            return "advance";
        default:
            return "neutral";
    }
};

const messageSummary = (message: PendingMessage["message"]): string => {
    if (message.__typename !== "GraphQLConsensusMessage") {
        switch (message.__typename) {
            case "GraphQLBlockSyncRequestMessage":
                return message.requestType === "HEADERS"
                    ? `Block sync · headers ×${message.blockRange?.numBlocks ?? "?"}`
                    : "Block sync · payload";
            case "GraphQLBlockSyncResponseMessage":
                return `Block sync · ${message.responseType.toLowerCase()} · ${message.available ? "found" : "missing"}`;
            case "GraphQLForwardedTxMessage":
                return "Forwarded tx";
            case "GraphQLStateSyncMessage":
                return "State sync";
            default:
                return "Message";
        }
    }

    switch (message.message.__typename) {
        case "GraphQLProposal":
            return `Proposal · r${message.round} · P${message.message.seqNum}`;
        case "GraphQLVote":
            return `Vote · r${message.message.round}`;
        case "GraphQLTimeout":
            return `Timeout · r${message.message.round} · ${message.message.highExtendType.toLowerCase()}`;
        case "GraphQLAdvanceRound":
            return `Advance · r${message.message.round}`;
        case "GraphQLRoundRecovery":
            return `Round recovery · r${message.message.round}`;
        case "GraphQLNoEndorsement":
            return `No endorsement · r${message.round}`;
        default:
            return `Consensus · r${message.round}`;
    }
};

const packetToneClass = (tone: MessageTone) => {
    switch (tone) {
        case "proposal":
            return "border-indigo-950 bg-indigo-500";
        case "vote":
            return "border-sky-950 bg-sky-500";
        case "timeout":
            return "border-red-950 bg-red-500";
        case "blocksync":
            return "border-amber-950 bg-amber-500";
        case "advance":
            return "border-violet-950 bg-violet-500";
        case "tx":
            return "border-emerald-950 bg-emerald-500";
        case "state":
            return "border-teal-950 bg-teal-500";
        default:
            return "border-neutral-950 bg-neutral-400";
    }
};

const hashString = (value: string) => {
    let hash = 2166136261;
    for (let index = 0; index < value.length; index += 1) {
        hash ^= value.charCodeAt(index);
        hash = Math.imul(hash, 16777619);
    }
    return hash >>> 0;
};

const blockVisual = (id: string) => {
    const hash = hashString(id);
    const hue = hash % 360;
    const shape = hash % 3;
    return {
        fill: `hsl(${hue} 78% 72%)`,
        stroke: `hsl(${hue} 78% 32%)`,
        text: `hsl(${hue} 78% 22%)`,
        shapeClass: shape === 0 ? "rounded-sm" : shape === 1 ? "rounded-full" : "rotate-45 rounded-[3px]",
    };
};

const messageToneClass = (tone: MessageTone) => {
    switch (tone) {
        case "proposal":
            return "border-indigo-700 bg-indigo-50 text-indigo-950";
        case "vote":
            return "border-sky-700 bg-sky-50 text-sky-950";
        case "timeout":
            return "border-red-700 bg-red-50 text-red-950";
        case "blocksync":
            return "border-amber-700 bg-amber-50 text-amber-950";
        case "advance":
            return "border-violet-700 bg-violet-50 text-violet-950";
        case "tx":
            return "border-emerald-700 bg-emerald-50 text-emerald-950";
        case "state":
            return "border-teal-700 bg-teal-50 text-teal-950";
        default:
            return "border-neutral-700 bg-white text-neutral-950";
    }
};

const sortBlocks = <T extends { seqNum: number, round: number, id: string }>(blocks: T[]) => (
    [...blocks].sort((a, b) => a.seqNum - b.seqNum || a.round - b.round || a.id.localeCompare(b.id))
);

const timeoutState = (node: SimNode, tick: number): TimeoutState | undefined => {
    const startsAt = node.roundTimerStartedAt;
    const endsAt = node.roundTimerEndsAt;
    if (startsAt === null || startsAt === undefined || endsAt === null || endsAt === undefined) {
        return undefined;
    }
    const duration = endsAt - startsAt;
    if (duration <= 0) {
        return undefined;
    }
    const progress = clamp((tick - startsAt) / duration, 0, 1);
    return {
        progress,
        remainingMs: Math.max(0, Math.round(endsAt - tick)),
        percent: Math.round(progress * 100),
    };
};

const timeoutText = (state: TimeoutState) => (
    state.remainingMs <= 250 ? `Timeout ${state.remainingMs}ms` : `Timeout ${state.percent}%`
);

const latencyFromPositions = (from: Position, to: Position) => {
    const dx = from.x - to.x;
    const dy = from.y - to.y;
    const distance = Math.sqrt(dx * dx + dy * dy);
    return clamp(Math.round(5 + distance * 0.08), 1, 250);
};

const linkForDirection = (pair: LinkPair, fromId: string, toId: string) => (
    pair.links.find((link) => link.fromId === fromId && link.toId === toId)
);

const linkMode = (pair: LinkPair): LinkMode => {
    const forwardDropped = linkForDirection(pair, pair.fromId, pair.toId)?.dropped ?? true;
    const reverseDropped = linkForDirection(pair, pair.toId, pair.fromId)?.dropped ?? true;
    if (!forwardDropped && !reverseDropped) {
        return "both";
    }
    if (!forwardDropped && reverseDropped) {
        return "forward";
    }
    if (forwardDropped && !reverseDropped) {
        return "reverse";
    }
    return "neither";
};

const NetworkCanvas: Component<{
    simulation: Simulation,
    data: SimulationQuery,
    vizTick: number,
    blockSamples: BlockSample[],
    onChange: () => void,
}> = (props) => {
    let canvasRef: HTMLDivElement | undefined;
    const [localPositions, setLocalPositions] = createSignal<Record<string, Position>>({});
    const [frozenTimeouts, setFrozenTimeouts] = createSignal<Record<string, TimeoutState>>({});
    const [canvasZoom, setCanvasZoom] = createSignal(1);
    const [canvasPan, setCanvasPan] = createSignal<PixelOffset>({ x: 0, y: 0 });
    const [isPanning, setIsPanning] = createSignal(false);
    const [showBlockView, setShowBlockView] = createSignal(true);
    const [showMessageFilters, setShowMessageFilters] = createSignal(false);
    const [messageFilters, setMessageFilters] = createSignal<Record<MessageFilterKey, boolean>>(defaultMessageFilters);
    const [canvasSize, setCanvasSize] = createSignal({ width: topologyViewSize, height: topologyViewSize });
    const [draggingNodeId, setDraggingNodeId] = createSignal<string>();
    const lastCommittedPosition = new Map<string, Position>();

    onMount(() => {
        if (!canvasRef) {
            return;
        }
        const observer = new ResizeObserver(([entry]) => {
            const { width, height } = entry.contentRect;
            if (width > 0 && height > 0) {
                setCanvasSize({ width, height });
            }
        });
        observer.observe(canvasRef);
        onCleanup(() => observer.disconnect());
    });

    const viewScale = (): ViewScale => {
        const size = canvasSize();
        return { x: size.width / topologyViewSize, y: size.height / topologyViewSize };
    };

    const networkNodes = () => props.data.networkConfig.nodes;
    const simNodes = () => props.data.nodes;
    const visibleMessage = (message: PendingMessage) => messageFilters()[messageFilterKey(message.message)];
    const setMessageFilter = (key: MessageFilterKey, visible: boolean) => {
        setMessageFilters((filters) => ({ ...filters, [key]: visible }));
    };

    const nodeIndexById = createMemo(() => {
        const indexes: Record<string, number> = {};
        for (const [index, node] of networkNodes().entries()) {
            indexes[node.id] = index;
        }
        return indexes;
    });

    const nodeLabel = (nodeId?: string | null) => {
        if (!nodeId) {
            return "none";
        }
        const index = nodeIndexById()[nodeId];
        return index === undefined ? shortNodeId(nodeId) : `N${index + 1}`;
    };

    const networkNodeById = createMemo(() => {
        const nodes: Record<string, NetworkNode> = {};
        for (const node of networkNodes()) {
            nodes[node.id] = node;
        }
        return nodes;
    });

    const simNodeById = createMemo(() => {
        const nodes: Record<string, SimNode> = {};
        for (const node of simNodes()) {
            nodes[node.id] = node;
        }
        return nodes;
    });

    const positionsById = createMemo(() => {
        const local = localPositions();
        const positions: Record<string, Position> = {};
        for (const node of networkNodes()) {
            positions[node.id] = local[node.id] ?? { x: node.x, y: node.y };
        }
        return positions;
    });

    createEffect(() => {
        const tick = props.vizTick;
        const simById = simNodeById();
        const next = { ...untrack(frozenTimeouts) };
        for (const networkNode of networkNodes()) {
            const state = timeoutState(simById[networkNode.id], tick);
            if (networkNode.online) {
                if (state) {
                    next[networkNode.id] = state;
                } else {
                    delete next[networkNode.id];
                }
            } else if (!next[networkNode.id] && state) {
                next[networkNode.id] = state;
            }
        }
        setFrozenTimeouts(next);
    });

    const displayLinks = createMemo(() => {
        const local = localPositions();
        const positions = positionsById();
        return props.data.networkConfig.links.map((link) => {
            const hasLocalPosition = local[link.fromId] !== undefined || local[link.toId] !== undefined;
            const from = positions[link.fromId];
            const to = positions[link.toId];
            return {
                ...link,
                latency: hasLocalPosition && from && to ? latencyFromPositions(from, to) : link.latency,
            };
        });
    });

    const linkPairs = createMemo(() => {
        const pairs: Record<string, LinkPair> = {};
        for (const link of displayLinks()) {
            const key = pairKey(link.fromId, link.toId);
            if (!pairs[key]) {
                const ids = [link.fromId, link.toId].sort();
                pairs[key] = { fromId: ids[0], toId: ids[1], links: [] };
            }
            pairs[key].links.push(link);
        }
        return Object.values(pairs);
    });
    const inFlightDestinations = createMemo(() => {
        const destinations = new Map<string, InFlightDestination>();
        for (const node of simNodes()) {
            for (const message of node.pendingMessages) {
                if (node.id === message.fromId || !visibleMessage(message)) {
                    continue;
                }
                const destination = destinations.get(node.id) ?? {
                    toId: node.id,
                    messages: [],
                };
                destination.messages.push({ fromId: message.fromId, message });
                destinations.set(node.id, destination);
            }
        }
        for (const destination of destinations.values()) {
            destination.messages.sort((left, right) => (
                left.message.rxTick - right.message.rxTick || left.message.id - right.message.id
            ));
        }
        return [...destinations.values()];
    });

    const applyLinkMode = (pair: LinkPair, mode: LinkMode) => {
        const forwardDropped = mode === "reverse" || mode === "neither";
        const reverseDropped = mode === "forward" || mode === "neither";
        try {
            props.simulation.setLinkDropped(pair.fromId, pair.toId, forwardDropped);
            props.simulation.setLinkDropped(pair.toId, pair.fromId, reverseDropped);
            props.onChange();
        } catch (err) {
            alert(err instanceof Error ? err.message : String(err));
        }
    };

    const togglePairConnection = (event: Event, pair: LinkPair) => {
        event.preventDefault();
        event.stopPropagation();
        applyLinkMode(pair, linkMode(pair) === "neither" ? "both" : "neither");
    };

    const handleLinkKeyDown = (event: KeyboardEvent, pair: LinkPair) => {
        if (event.key === "Enter" || event.key === " ") {
            togglePairConnection(event, pair);
        }
    };

    const mergedLedger = createMemo(() => {
        if (!showBlockView()) {
            return [];
        }
        const byId = new Map<string, MergedLedgerBlock>();
        for (const node of simNodes()) {
            for (const block of node.ledgerBlocks) {
                let merged = byId.get(block.id);
                if (!merged) {
                    merged = {
                        ...block,
                        seenBy: [],
                        finalizedBy: [],
                        coherentBy: [],
                        votedBy: [],
                    };
                    byId.set(block.id, merged);
                }
                merged.seenBy.push(node.id);
                if (block.finalized) {
                    merged.finalized = true;
                    merged.finalizedBy.push(node.id);
                }
                if (block.coherent) {
                    merged.coherent = true;
                    merged.coherentBy.push(node.id);
                }
                if (block.voted) {
                    merged.voted = true;
                    merged.votedBy.push(node.id);
                }
            }
        }
        return sortBlocks([...byId.values()]);
    });

    const canvasPoint = (clientX: number, clientY: number): Position => {
        const rect = canvasRef?.getBoundingClientRect();
        if (!rect || rect.width === 0 || rect.height === 0) {
            return { x: 500, y: 500 };
        }
        const zoom = canvasZoom();
        const pan = canvasPan();
        const normalizedX = (((clientX - rect.left - pan.x) / rect.width) - 0.5) / zoom + 0.5;
        const normalizedY = (((clientY - rect.top - pan.y) / rect.height) - 0.5) / zoom + 0.5;
        return {
            x: clamp(Math.round(normalizedX * topologyViewSize), topologyMin, topologyMax),
            y: clamp(Math.round(normalizedY * topologyViewSize), topologyMin, topologyMax),
        };
    };

    const setZoom = (nextZoom: number) => {
        setCanvasZoom(clamp(nextZoom, minCanvasZoom, maxCanvasZoom));
    };

    const zoomBy = (amount: number) => {
        setCanvasZoom((zoom) => clamp(zoom + amount, minCanvasZoom, maxCanvasZoom));
    };

    const wheelDeltaPixels = (event: WheelEvent): PixelOffset => {
        const deltaMultiplier = event.deltaMode === WheelEvent.DOM_DELTA_LINE
            ? 16
            : event.deltaMode === WheelEvent.DOM_DELTA_PAGE
              ? 400
              : 1;
        return {
            x: event.deltaX * deltaMultiplier,
            y: event.deltaY * deltaMultiplier,
        };
    };

    const handleWheel = (event: WheelEvent) => {
        if (event.target instanceof Element && event.target.closest(wheelIgnoreSelector)) {
            return;
        }
        event.preventDefault();
        event.stopPropagation();
        const delta = wheelDeltaPixels(event);
        if (event.ctrlKey) {
            setCanvasZoom((zoom) => clamp(
                zoom * Math.exp(-delta.y * pinchZoomSensitivity),
                minCanvasZoom,
                maxCanvasZoom,
            ));
            return;
        }
        setCanvasPan((pan) => ({
            x: pan.x - delta.x,
            y: pan.y - delta.y,
        }));
    };

    const resetView = () => {
        setZoom(1);
        setCanvasPan({ x: 0, y: 0 });
    };

    const commitPosition = (nodeId: string, position: Position) => {
        const next = { x: Math.round(position.x), y: Math.round(position.y) };
        const previous = lastCommittedPosition.get(nodeId);
        if (previous?.x === next.x && previous?.y === next.y) {
            return;
        }
        try {
            props.simulation.setNodePosition(nodeId, next.x, next.y);
            lastCommittedPosition.set(nodeId, next);
            props.onChange();
        } catch (err) {
            alert(err instanceof Error ? err.message : String(err));
        }
    };

    const toggleNode = (nodeId: string) => {
        const node = networkNodeById()[nodeId];
        if (!node) {
            return;
        }
        try {
            props.simulation.setNodeOnline(nodeId, !node.online);
            props.onChange();
        } catch (err) {
            alert(err instanceof Error ? err.message : String(err));
        }
    };

    type DragState = {
        nodeId: string,
        offset: Position,
        startClientX: number,
        startClientY: number,
        dragging: boolean,
        lastCommitMs: number,
    };
    let dragState: DragState | undefined;

    type PanState = {
        startClientX: number,
        startClientY: number,
        startPan: PixelOffset,
    };
    let panState: PanState | undefined;

    const startPanPointer = (event: PointerEvent) => {
        if (event.button !== 0) {
            return;
        }
        event.preventDefault();
        panState = {
            startClientX: event.clientX,
            startClientY: event.clientY,
            startPan: canvasPan(),
        };
        setIsPanning(true);
        (event.currentTarget as HTMLElement).setPointerCapture(event.pointerId);
    };

    const movePanPointer = (event: PointerEvent) => {
        if (!panState) {
            return;
        }
        setCanvasPan({
            x: panState.startPan.x + event.clientX - panState.startClientX,
            y: panState.startPan.y + event.clientY - panState.startClientY,
        });
    };

    const endPanPointer = (event: PointerEvent) => {
        if (!panState) {
            return;
        }
        panState = undefined;
        setIsPanning(false);
        try {
            (event.currentTarget as HTMLElement).releasePointerCapture(event.pointerId);
        } catch {
            // Pointer capture may already be released by the browser.
        }
    };

    const startNodePointer = (event: PointerEvent, node: NetworkNode) => {
        event.preventDefault();
        event.stopPropagation();
        const point = canvasPoint(event.clientX, event.clientY);
        const current = positionsById()[node.id] ?? { x: node.x, y: node.y };
        dragState = {
            nodeId: node.id,
            offset: { x: point.x - current.x, y: point.y - current.y },
            startClientX: event.clientX,
            startClientY: event.clientY,
            dragging: false,
            lastCommitMs: 0,
        };
        (event.currentTarget as HTMLElement).setPointerCapture(event.pointerId);
    };

    const moveNodePointer = (event: PointerEvent) => {
        event.stopPropagation();
        if (!dragState) {
            return;
        }
        const movedPx = Math.hypot(
            event.clientX - dragState.startClientX,
            event.clientY - dragState.startClientY,
        );
        if (!dragState.dragging && movedPx < clickSlopPx) {
            return;
        }
        dragState.dragging = true;
        setDraggingNodeId(dragState.nodeId);
        const point = canvasPoint(event.clientX, event.clientY);
        const position = {
            x: clamp(point.x - dragState.offset.x, topologyMin, topologyMax),
            y: clamp(point.y - dragState.offset.y, topologyMin, topologyMax),
        };
        setLocalPositions((positions) => ({ ...positions, [dragState!.nodeId]: position }));

        const now = performance.now();
        if (now - dragState.lastCommitMs >= 100) {
            dragState.lastCommitMs = now;
            commitPosition(dragState.nodeId, position);
        }
    };

    const endNodePointer = (event: PointerEvent) => {
        event.stopPropagation();
        if (!dragState) {
            return;
        }
        const state = dragState;
        dragState = undefined;
        setDraggingNodeId(undefined);
        try {
            (event.currentTarget as HTMLElement).releasePointerCapture(event.pointerId);
        } catch {
            // Pointer capture may already be released by the browser.
        }

        if (!state.dragging) {
            toggleNode(state.nodeId);
            return;
        }

        const point = canvasPoint(event.clientX, event.clientY);
        const position = {
            x: clamp(point.x - state.offset.x, topologyMin, topologyMax),
            y: clamp(point.y - state.offset.y, topologyMin, topologyMax),
        };
        commitPosition(state.nodeId, position);
        setLocalPositions((positions) => {
            const next = { ...positions };
            delete next[state.nodeId];
            return next;
        });
    };

    const cancelNodePointer = (event: PointerEvent) => {
        event.stopPropagation();
        dragState = undefined;
        setDraggingNodeId(undefined);
        try {
            (event.currentTarget as HTMLElement).releasePointerCapture(event.pointerId);
        } catch {
            // Pointer capture may already be released by the browser.
        }
    };

    // laneOffset is in screen pixels: measuring it in view units would let the
    // stretched viewBox render the same lane twice as wide on a steep wire as on
    // a shallow one, which reads as packets riding the wire in some cases and
    // floating away from it in others.
    const directedLinkPoint = (
        fromNodeId: string,
        toNodeId: string,
        progress: number,
        laneOffset: number,
    ) => {
        const from = positionsById()[fromNodeId];
        const to = positionsById()[toNodeId];
        if (!from || !to) {
            return undefined;
        }
        const travelled = {
            x: from.x + progress * (to.x - from.x),
            y: from.y + progress * (to.y - from.y),
        };
        return screenFrame(from, to, viewScale()).offset(travelled, 0, laneOffset);
    };

    const messagePosition = (toNodeId: string, message: PendingMessage) => {
        const duration = Math.max(1, message.rxTick - message.fromTick);
        const progress = clamp((props.vizTick - message.fromTick) / duration, 0, 1);
        // Stagger concurrent packets so a burst on one wire stays countable
        const packetOffset = ((message.id % 3) - 1) * packetLaneStagger;
        return directedLinkPoint(message.fromId, toNodeId, progress, packetLane + packetOffset);
    };

    const destinationPanelPosition = (toNodeId: string) => {
        const destination = positionsById()[toNodeId];
        if (!destination) {
            return undefined;
        }
        const dx = 500 - destination.x;
        const dy = 500 - destination.y;
        const length = Math.max(1, Math.hypot(dx, dy));
        return {
            x: destination.x + (dx / length) * 170,
            y: destination.y + (dy / length) * 145,
        };
    };

    return (
        <div ref={canvasRef} class="relative h-full min-h-0 grow overflow-hidden bg-neutral-100" onWheel={handleWheel}>
            <div
                class="absolute left-4 top-4 z-50 flex items-center overflow-hidden rounded-lg border border-neutral-300 bg-white/95 text-sm font-semibold shadow-lg backdrop-blur"
                data-canvas-wheel-ignore="true"
            >
                <button
                    type="button"
                    class="h-8 w-9 border-r border-neutral-300 text-lg leading-none hover:bg-neutral-100 disabled:text-neutral-300 disabled:hover:bg-white"
                    aria-label="Zoom out"
                    title="Zoom out"
                    disabled={canvasZoom() <= minCanvasZoom}
                    onClick={() => zoomBy(-zoomButtonStep)}
                >
                    -
                </button>
                <button
                    type="button"
                    class="h-8 w-14 border-r border-neutral-300 text-xs tabular-nums hover:bg-neutral-100"
                    aria-label="Reset view"
                    title="Reset view"
                    onClick={resetView}
                >
                    {Math.round(canvasZoom() * 100)}%
                </button>
                <button
                    type="button"
                    class="h-8 w-9 text-lg leading-none hover:bg-neutral-100 disabled:text-neutral-300 disabled:hover:bg-white"
                    aria-label="Zoom in"
                    title="Zoom in"
                    disabled={canvasZoom() >= maxCanvasZoom}
                    onClick={() => zoomBy(zoomButtonStep)}
                >
                    +
                </button>
            </div>
            <button
                type="button"
                class={`absolute left-4 top-14 z-50 rounded-lg border px-3 py-1.5 text-sm font-semibold shadow-lg backdrop-blur ${showBlockView() ? "border-indigo-500 bg-indigo-50 text-indigo-700" : "border-neutral-300 bg-white/95 text-neutral-800 hover:bg-neutral-100"}`}
                data-canvas-wheel-ignore="true"
                onClick={() => setShowBlockView((show) => !show)}
            >
                Block View
            </button>
            <button
                type="button"
                class={`absolute left-4 top-24 z-50 rounded-lg border px-3 py-1.5 text-sm font-semibold shadow-lg backdrop-blur ${showMessageFilters() ? "border-indigo-500 bg-indigo-50 text-indigo-700" : "border-neutral-300 bg-white/95 text-neutral-800 hover:bg-neutral-100"}`}
                data-canvas-wheel-ignore="true"
                onClick={() => setShowMessageFilters((show) => !show)}
            >
                Messages
            </button>
            <Show when={showMessageFilters()}>
                <div
                    class="absolute left-4 top-36 z-50 w-56 rounded-lg border border-neutral-300 bg-white/95 p-3 text-sm shadow-lg backdrop-blur"
                    data-canvas-wheel-ignore="true"
                >
                    <div class="mb-2 flex items-center justify-between gap-3">
                        <h2 class="text-sm font-semibold">Message Types</h2>
                        <button
                            type="button"
                            class="rounded border border-neutral-300 px-2 py-0.5 text-xs font-semibold text-neutral-700 hover:bg-neutral-100"
                            onClick={() => setMessageFilters(defaultMessageFilters)}
                        >
                            Reset
                        </button>
                    </div>
                    <div class="grid gap-1.5">
                        <For each={messageFilterOptions}>{(option) => (
                            <label class="flex items-center gap-2 rounded px-1 py-0.5 text-xs font-medium text-neutral-800 hover:bg-neutral-100">
                                <input
                                    type="checkbox"
                                    class="h-3.5 w-3.5 accent-indigo-600"
                                    checked={messageFilters()[option.key]}
                                    onInput={(event) => setMessageFilter(option.key, event.currentTarget.checked)}
                                />
                                <span>{option.label}</span>
                            </label>
                        )}</For>
                    </div>
                </div>
            </Show>

            <div
                class={`absolute inset-0 origin-center ${isPanning() ? "cursor-grabbing" : "cursor-grab"}`}
                style={{
                    transform: `translate(${canvasPan().x}px, ${canvasPan().y}px) scale(${canvasZoom()})`,
                    "will-change": "transform",
                }}
                onPointerDown={startPanPointer}
                onPointerMove={movePanPointer}
                onPointerUp={endPanPointer}
                onPointerCancel={endPanPointer}
            >
            <svg class="absolute inset-0 h-full w-full overflow-visible" viewBox={`0 0 ${topologyViewSize} ${topologyViewSize}`} preserveAspectRatio="none">
                <For each={linkPairs()}>{(pair) => {
                    const from = () => positionsById()[pair.fromId];
                    const to = () => positionsById()[pair.toId];
                    const mode = () => linkMode(pair);
                    const cut = () => mode() === "neither";
                    const anyDropped = () => mode() !== "both";
                    const anyOffline = () => pair.links.some((link) => link.offline);
                    const anyOverridden = () => pair.links.some((link) => link.overridden);
                    const leftCut = () => pointAlongLine(from()!, to()!, -cutCableGap);
                    const rightCut = () => pointAlongLine(from()!, to()!, cutCableGap);
                    const strandEnd = (cutEnd: Position, toward: Position, offset: number) => {
                        const dx = toward.x - cutEnd.x;
                        const dy = toward.y - cutEnd.y;
                        const length = Math.max(1, Math.hypot(dx, dy));
                        const tangentX = dx / length;
                        const tangentY = dy / length;
                        const normalX = -tangentY;
                        const normalY = tangentX;
                        const spread = Math.abs(offset) * 0.45;
                        return {
                            x: cutEnd.x + tangentX * (18 + spread) + normalX * offset,
                            y: cutEnd.y + tangentY * (18 + spread) + normalY * offset,
                        };
                    };
                    return (
                        <Show when={from() && to()}>
                            <>
                                <Show
                                    when={!cut()}
                                    fallback={
                                        <>
                                            <line x1={from()?.x ?? 0} y1={from()?.y ?? 0} x2={leftCut().x} y2={leftCut().y} stroke="#dc2626" stroke-width="10" opacity={anyOffline() ? 0.4 : 0.9} />
                                            <line x1={to()?.x ?? 0} y1={to()?.y ?? 0} x2={rightCut().x} y2={rightCut().y} stroke="#dc2626" stroke-width="10" opacity={anyOffline() ? 0.4 : 0.9} />
                                            <For each={cutCableStrands}>{(offset) => {
                                                const leftEnd = () => strandEnd(leftCut(), rightCut(), offset);
                                                const rightEnd = () => strandEnd(rightCut(), leftCut(), -offset);
                                                return (
                                                    <>
                                                        <line x1={leftCut().x} y1={leftCut().y} x2={leftEnd().x} y2={leftEnd().y} stroke={offset % 2 === 0 ? "#f97316" : "#9a3412"} stroke-width="2" />
                                                        <line x1={rightCut().x} y1={rightCut().y} x2={rightEnd().x} y2={rightEnd().y} stroke={offset % 2 === 0 ? "#f97316" : "#9a3412"} stroke-width="2" />
                                                    </>
                                                );
                                            }}</For>
                                        </>
                                    }
                                >
                                    <line
                                        x1={from()?.x ?? 0}
                                        y1={from()?.y ?? 0}
                                        x2={to()?.x ?? 0}
                                        y2={to()?.y ?? 0}
                                        stroke={anyDropped() || anyOverridden() ? "#d97706" : "#4b5563"}
                                        stroke-width={anyOverridden() || anyDropped() ? 4 : 3}
                                        opacity={anyOffline() ? 0.45 : 0.82}
                                    />
                                </Show>
                                <line
                                    x1={from()?.x ?? 0}
                                    y1={from()?.y ?? 0}
                                    x2={to()?.x ?? 0}
                                    y2={to()?.y ?? 0}
                                    stroke="rgba(0, 0, 0, 0.001)"
                                    style={{
                                        "pointer-events": "stroke",
                                        "stroke-linecap": "round",
                                        "stroke-width": `${linkHitAreaWidth}px`,
                                        cursor: cut() ? repairCursor : cutCursor,
                                    }}
                                    role="button"
                                    tabIndex={0}
                                    aria-label={`${cut() ? "Repair" : "Cut"} connection between ${nodeLabel(pair.fromId)} and ${nodeLabel(pair.toId)}`}
                                    onPointerDown={(event) => event.stopPropagation()}
                                    onClick={(event) => togglePairConnection(event, pair)}
                                    onKeyDown={(event) => handleLinkKeyDown(event, pair)}
                                />
                            </>
                        </Show>
                    );
                }}</For>
            </svg>

            <For each={linkPairs()}>{(pair) => {
                const from = () => positionsById()[pair.fromId];
                const to = () => positionsById()[pair.toId];
                const mid = () => ({
                    x: ((from()?.x ?? 0) + (to()?.x ?? 0)) / 2,
                    y: ((from()?.y ?? 0) + (to()?.y ?? 0)) / 2,
                });
                const mode = () => linkMode(pair);
                const frame = () => screenFrame(from() ?? mid(), to() ?? mid(), viewScale());
                const fit = () => clamp(frame().length / wireLabelFitLength, 0.5, 1);
                const forward = () => linkForDirection(pair, pair.fromId, pair.toId);
                const reverse = () => linkForDirection(pair, pair.toId, pair.fromId);
                // A severed wire has no latency to report, so its label steps
                // aside to keep the frayed cable ends legible.
                const severedPosition = () => frame().offset(mid(), 0, 78);
                const lanes = (): WireLane[] => {
                    const forwardLink = forward();
                    const reverseLink = reverse();
                    switch (mode()) {
                        case "both":
                            if (forwardLink && reverseLink && forwardLink.latency !== reverseLink.latency) {
                                return [
                                    { key: "forward", latency: forwardLink.latency, travel: 1, side: 1, arrow: true, tone: "neutral" },
                                    { key: "reverse", latency: reverseLink.latency, travel: -1, side: -1, arrow: true, tone: "neutral" },
                                ];
                            }
                            return [{
                                key: "symmetric",
                                latency: forwardLink?.latency ?? reverseLink?.latency ?? 0,
                                travel: 1,
                                side: 0,
                                arrow: false,
                                tone: "neutral",
                            }];
                        case "forward":
                            return [{ key: "forward", latency: forwardLink?.latency ?? 0, travel: 1, side: 0, arrow: true, tone: "amber" }];
                        case "reverse":
                            return [{ key: "reverse", latency: reverseLink?.latency ?? 0, travel: -1, side: 0, arrow: true, tone: "amber" }];
                        case "neither":
                            return [];
                    }
                };
                return (
                    <Show when={from() && to()}>
                        <Show when={mode() === "neither"}>
                            <div
                                class="pointer-events-none absolute z-10"
                                style={{
                                    left: `${severedPosition().x / 10}%`,
                                    top: `${severedPosition().y / 10}%`,
                                    transform: "translate(-50%, -50%)",
                                }}
                            >
                                <div class={`${wireChipClass} border-red-300 bg-red-50 text-red-700`}>Severed</div>
                            </div>
                        </Show>
                        <For each={lanes()}>{(lane) => {
                            const arrowCenter = () => frame().offset(
                                mid(),
                                -wireArrowAlong * fit() * lane.travel,
                                wireLaneOffset * fit() * lane.side,
                            );
                            const chipCenter = () => frame().offset(
                                mid(),
                                wireChipAlong * fit() * lane.travel,
                                wireLaneOffset * fit() * lane.side,
                            );
                            // The arrow is html, not canvas svg, so it rotates by the
                            // wire's on-screen angle and keeps its shape undistorted.
                            const rotation = () => frame().angle + (lane.travel === 1 ? 0 : Math.PI);
                            const arrowColor = () => lane.tone === "amber" ? "#b45309" : "#334155";
                            return (
                                <>
                                    <Show when={lane.arrow}>
                                        <div
                                            class="pointer-events-none absolute z-10"
                                            style={{
                                                left: `${arrowCenter().x / 10}%`,
                                                top: `${arrowCenter().y / 10}%`,
                                                transform: `translate(-50%, -50%) rotate(${rotation()}rad)`,
                                            }}
                                            aria-hidden="true"
                                        >
                                            <svg
                                                width={wireArrowLength * fit()}
                                                height={wireArrowHeight * fit()}
                                                viewBox={`0 0 ${wireArrowLength} ${wireArrowHeight}`}
                                                fill="none"
                                            >
                                                <path d="M2 6 L38 6" stroke={arrowColor()} stroke-width="2.5" stroke-linecap="round" />
                                                <path d="M36 1.2 L50 6 L36 10.8 Z" fill={arrowColor()} />
                                            </svg>
                                        </div>
                                    </Show>
                                    <div
                                        class="pointer-events-none absolute z-10"
                                        style={{
                                            left: `${chipCenter().x / 10}%`,
                                            top: `${chipCenter().y / 10}%`,
                                            transform: "translate(-50%, -50%)",
                                        }}
                                    >
                                        <div class={`${wireChipClass} ${lane.tone === "amber" ? "border-amber-400 bg-amber-50 text-amber-800" : "border-neutral-300 bg-white/95 text-neutral-800"}`}>
                                            {lane.latency}ms
                                        </div>
                                    </div>
                                </>
                            );
                        }}</For>
                    </Show>
                );
            }}</For>

            <For each={simNodes()}>{(node) => (
                <For each={node.pendingMessages}>{(message) => {
                    const position = () => messagePosition(node.id, message);
                    return (
                        <Show when={node.id !== message.fromId && visibleMessage(message) && position()}>
                            <span
                                class={`pointer-events-none absolute z-20 h-3 w-3 -translate-x-1/2 -translate-y-1/2 rotate-45 rounded-[2px] border-2 shadow-md ${packetToneClass(messageTone(message.message))}`}
                                style={{
                                    left: `${(position()?.x ?? 0) / 10}%`,
                                    top: `${(position()?.y ?? 0) / 10}%`,
                                }}
                                title={messageSummary(message.message)}
                                aria-hidden="true"
                            />
                        </Show>
                    );
                }}</For>
            )}</For>

            <For each={inFlightDestinations()}>{(destination) => {
                const position = () => destinationPanelPosition(destination.toId);
                const visibleMessages = () => destination.messages.slice(0, 4);
                const overflow = () => Math.max(0, destination.messages.length - visibleMessages().length);
                return (
                    <Show when={position()}>
                        <div
                            class="pointer-events-none absolute z-20 w-52 -translate-x-1/2 -translate-y-1/2 rounded-lg border border-neutral-300 bg-white/90 p-1.5 shadow-md backdrop-blur-sm"
                            style={{
                                left: `${(position()?.x ?? 0) / 10}%`,
                                top: `${(position()?.y ?? 0) / 10}%`,
                            }}
                        >
                            <div class="mb-1 flex items-center justify-between gap-2 px-0.5 text-[10px] font-bold uppercase tracking-wide text-neutral-500">
                                <span>Incoming to {nodeLabel(destination.toId)}</span>
                                <Show when={overflow() > 0}>
                                    <span>+{overflow()}</span>
                                </Show>
                            </div>
                            <div class="grid gap-1">
                                <For each={visibleMessages()}>{(entry) => (
                                    <MessageSummaryChip
                                        message={entry.message.message}
                                        sourceLabel={nodeLabel(entry.fromId)}
                                    />
                                )}</For>
                            </div>
                        </div>
                    </Show>
                );
            }}</For>

            <For each={networkNodes()}>{(networkNode) => {
                const position = () => positionsById()[networkNode.id] ?? { x: networkNode.x, y: networkNode.y };
                const node = () => simNodeById()[networkNode.id];
                const timeout = () => {
                    const simNode = node();
                    if (!networkNode.online) {
                        return frozenTimeouts()[networkNode.id];
                    }
                    return simNode ? timeoutState(simNode, props.vizTick) : undefined;
                };
                const isCurrentLeader = () => props.data.currentLeader === networkNode.id;
                const isNextLeader = () => props.data.nextLeader === networkNode.id;
                const nodeStyle = () => ({
                    left: `${position().x / 10}%`,
                    top: `${position().y / 10}%`,
                    cursor: draggingNodeId() === networkNode.id
                        ? "grabbing"
                        : networkNode.online ? takeDownCursor : bringUpCursor,
                });
                return (
                    <button
                        type="button"
                        class={`absolute z-30 w-40 -translate-x-1/2 -translate-y-1/2 select-none rounded-lg text-left shadow-lg transition ${networkNode.online ? "" : "opacity-70"} ${isCurrentLeader() ? "ring-4 ring-indigo-400" : isNextLeader() ? "ring-2 ring-indigo-200" : ""}`}
                        style={nodeStyle()}
                        aria-label={`${nodeLabel(networkNode.id)} ${networkNode.online ? "live; click to take down" : "down; click to bring up"}; drag to move`}
                        title={`${networkNode.online ? "Click to take down" : "Click to bring up"}; drag to move`}
                        onPointerDown={(event) => startNodePointer(event, networkNode)}
                        onPointerMove={moveNodePointer}
                        onPointerUp={endNodePointer}
                        onPointerCancel={cancelNodePointer}
                    >
                        <div class={`relative overflow-hidden rounded-lg border px-3 py-2 ${networkNode.online ? "border-neutral-900 bg-white" : "border-red-500 bg-red-50"}`}>
                            <div class="flex items-center justify-between gap-2">
                                <div class="text-lg font-semibold leading-6">{nodeLabel(networkNode.id)}</div>
                                <div class={`rounded px-1.5 py-0.5 text-xs font-semibold ${networkNode.online ? "bg-emerald-100 text-emerald-800" : "bg-red-100 text-red-800"}`}>
                                    {networkNode.online ? "Live" : "Down"}
                                </div>
                            </div>
                            <Show when={node()}>
                                {(simNode) => (
                                    <>
                                        <div class="mt-1 text-sm text-neutral-700">Round {simNode().currentRound}</div>
                                        <div class="text-sm text-neutral-700">Finalized {simNode().root}</div>
                                        <Show when={timeout()}>
                                            {(state) => (
                                                <div class="mt-1 flex items-center gap-2 text-xs font-semibold text-red-700">
                                                    <span>{timeoutText(state())}</span>
                                                    <span class="h-1.5 min-w-0 flex-1 overflow-hidden rounded-full bg-red-100">
                                                        <span
                                                            class="block h-full rounded-full bg-red-600"
                                                            style={{ width: `${state().percent}%` }}
                                                        />
                                                    </span>
                                                </div>
                                            )}
                                        </Show>
                                    </>
                                )}
                            </Show>
                        </div>
                    </button>
                );
            }}</For>

            </div>

            <MetricsPanel data={props.data} blockSamples={props.blockSamples} nodeLabel={nodeLabel} />
            <Show when={showBlockView()}>
                <BlockViewPanel blocks={mergedLedger()} nodeLabel={nodeLabel} />
            </Show>
        </div>
    );
};

const BlockGlyph: Component<{ id: string, compact?: boolean }> = (props) => {
    const visual = () => blockVisual(props.id);
    return (
        <span
            class={`inline-block shrink-0 border-2 ${props.compact ? "h-2.5 w-2.5" : "h-3.5 w-3.5"} ${visual().shapeClass}`}
            style={{
                "background-color": visual().fill,
                "border-color": visual().stroke,
            }}
        />
    );
};

const BlockChip: Component<{
    block?: Blockish,
    blockId?: string,
    label?: string,
    compact?: boolean,
    muted?: boolean,
}> = (props) => {
    const id = () => props.block?.id ?? props.blockId ?? "";
    const visual = () => blockVisual(id());
    const label = () => props.label ?? (props.block?.seqNum !== undefined ? `P${props.block.seqNum}` : shortBlockId(id()));
    const finalized = () => Boolean((props.block as LedgerBlock | undefined)?.finalized);
    return (
        <span
            class={`inline-flex max-w-full items-center gap-1 rounded border bg-white/85 font-semibold shadow-sm ${props.compact ? "px-1 py-0.5 text-[10px] leading-3" : "px-1.5 py-1 text-[11px] leading-4"} ${props.muted ? "opacity-45" : ""} ${finalized() ? "ring-1 ring-neutral-950" : ""}`}
            style={{
                color: visual().text,
                "border-color": visual().stroke,
            }}
            title={id()}
        >
            <BlockGlyph id={id()} compact={props.compact} />
            <span class="truncate tabular-nums">{label()}</span>
        </span>
    );
};

const MessageSummaryChip: Component<{
    message: PendingMessage["message"],
    sourceLabel: string,
}> = (props) => (
    <div
        class={`flex min-w-0 items-center gap-2 rounded-md border px-2 py-1 text-[11px] font-semibold leading-4 shadow-sm ${messageToneClass(messageTone(props.message))}`}
        title={messageSummary(props.message)}
    >
        <span class={`h-2 w-2 shrink-0 rotate-45 rounded-[1px] border ${packetToneClass(messageTone(props.message))}`} />
        <span class="shrink-0 rounded bg-white/75 px-1 text-[9px] font-bold leading-4">{props.sourceLabel}</span>
        <span class="truncate">{messageSummary(props.message)}</span>
    </div>
);

const BlockViewPanel: Component<{
    blocks: MergedLedgerBlock[],
    nodeLabel: (nodeId?: string | null) => string,
}> = (props) => {
    let scrollContainer: HTMLDivElement | undefined;
    const [isPinnedRight, setIsPinnedRight] = createSignal(true);
    const scrolledToRight = (element: HTMLDivElement) => (
        element.scrollWidth - element.clientWidth - element.scrollLeft <= 8
    );
    const updateRightPin = () => {
        if (scrollContainer) {
            setIsPinnedRight(scrolledToRight(scrollContainer));
        }
    };

    const layout = createMemo(() => {
        const blocks = sortBlocks(props.blocks);
        const blocksBySeq = new Map<number, MergedLedgerBlock[]>();
        for (const block of blocks) {
            const seqBlocks = blocksBySeq.get(block.seqNum) ?? [];
            seqBlocks.push(block);
            blocksBySeq.set(block.seqNum, seqBlocks);
        }

        const seqNums = [...blocksBySeq.keys()].sort((a, b) => a - b);
        const cardWidth = 116;
        const cardHeight = 54;
        const columnWidth = 154;
        const rowHeight = 76;
        const margin = 28;
        const positions: Record<string, { x: number, y: number }> = {};
        let maxRows = 1;

        for (const [seqIndex, seqNum] of seqNums.entries()) {
            const seqBlocks = sortBlocks(blocksBySeq.get(seqNum) ?? []);
            maxRows = Math.max(maxRows, seqBlocks.length);
            for (const [rowIndex, block] of seqBlocks.entries()) {
                positions[block.id] = {
                    x: margin + seqIndex * columnWidth,
                    y: margin + rowIndex * rowHeight,
                };
            }
        }

        return {
            blocks,
            cardHeight,
            cardWidth,
            height: Math.max(170, margin * 2 + maxRows * rowHeight),
            positions,
            width: Math.max(480, margin * 2 + Math.max(1, seqNums.length) * columnWidth),
        };
    });

    const status = (block: MergedLedgerBlock) => (
        block.finalized ? "finalized" : block.voted ? "voted" : "proposed"
    );

    createEffect(() => {
        const _blockCount = props.blocks.length;
        const _layoutWidth = layout().width;
        const shouldFollowRight = untrack(isPinnedRight);
        const frame = requestAnimationFrame(() => {
            if (scrollContainer && shouldFollowRight) {
                scrollContainer.scrollLeft = scrollContainer.scrollWidth;
                setIsPinnedRight(true);
            } else if (scrollContainer) {
                setIsPinnedRight(scrolledToRight(scrollContainer));
            }
        });
        onCleanup(() => cancelAnimationFrame(frame));
    });

    return (
        <aside
            class="absolute bottom-4 right-4 z-40 max-h-[48%] w-[38rem] max-w-[calc(100%-2rem)] overflow-hidden rounded-lg border border-neutral-300 bg-white/95 p-3 text-sm shadow-lg backdrop-blur"
            data-canvas-wheel-ignore="true"
        >
            <div class="flex items-center justify-between gap-3">
                <h2 class="text-base font-semibold">Block View</h2>
                <span class="rounded bg-neutral-100 px-2 py-1 text-xs font-medium text-neutral-700">
                    {props.blocks.length} blocks
                </span>
            </div>
            <div ref={scrollContainer} class="mt-2 max-h-80 overflow-auto rounded border border-neutral-200 bg-neutral-50" onScroll={updateRightPin}>
            <Show
                when={props.blocks.length > 0}
                fallback={<div class="p-3 text-xs text-neutral-500">No blocks yet</div>}
            >
                <div
                    class="relative"
                    style={{
                        height: `${layout().height}px`,
                        width: `${layout().width}px`,
                    }}
                >
                    <svg
                        class="pointer-events-none absolute inset-0"
                        viewBox={`0 0 ${layout().width} ${layout().height}`}
                        preserveAspectRatio="none"
                    >
                        <defs>
                            <marker id="block-view-arrow" markerHeight="7" markerWidth="7" orient="auto" refX="6" refY="3.5">
                                <path d="M0,0 L7,3.5 L0,7 z" fill="#6b7280" />
                            </marker>
                        </defs>
                        <For each={layout().blocks}>{(block) => {
                            const child = () => layout().positions[block.id];
                            const parent = () => block.parentId ? layout().positions[block.parentId] : undefined;
                            const path = () => {
                                const childPos = child();
                                const parentPos = parent();
                                if (!childPos || !parentPos) {
                                    return "";
                                }
                                const y1 = childPos.y + layout().cardHeight / 2;
                                const y2 = parentPos.y + layout().cardHeight / 2;
                                const x1 = childPos.x;
                                const x2 = parentPos.x + layout().cardWidth;
                                const midX = (x1 + x2) / 2;
                                return `M ${x1} ${y1} C ${midX} ${y1}, ${midX} ${y2}, ${x2} ${y2}`;
                            };
                            return (
                                <Show when={parent()}>
                                    <path
                                        d={path()}
                                        fill="none"
                                        marker-end="url(#block-view-arrow)"
                                        stroke="#6b7280"
                                        stroke-width="2"
                                    />
                                </Show>
                            );
                        }}</For>
                    </svg>
                    <For each={layout().blocks}>{(block) => {
                        const position = () => layout().positions[block.id];
                        const seenBy = () => block.seenBy.map(props.nodeLabel).join(" ");
                        return (
                            <div
                                class={`absolute rounded-md border bg-white p-2 shadow-sm ${block.finalized ? "border-neutral-900" : block.voted ? "border-indigo-300" : "border-neutral-300 opacity-70"}`}
                                style={{
                                    height: `${layout().cardHeight}px`,
                                    left: `${position()?.x ?? 0}px`,
                                    top: `${position()?.y ?? 0}px`,
                                    width: `${layout().cardWidth}px`,
                                }}
                                title={`${block.id}\nseen by ${seenBy()}`}
                            >
                                <div class="flex items-center justify-between gap-1">
                                    <BlockChip block={block} label={block.finalized ? `F${block.seqNum}` : `P${block.seqNum}`} compact muted={!block.voted} />
                                    <span class={`rounded px-1 py-0.5 text-[10px] font-semibold ${block.finalized ? "bg-neutral-900 text-white" : block.voted ? "bg-indigo-100 text-indigo-800" : "bg-neutral-100 text-neutral-600"}`}>
                                        {status(block)}
                                    </span>
                                </div>
                                <div class="mt-1 truncate text-[10px] text-neutral-500">
                                    r{block.round} {seenBy()}
                                </div>
                            </div>
                        );
                    }}</For>
                </div>
            </Show>
            </div>
        </aside>
    );
};

const MetricsPanel: Component<{
    data: SimulationQuery,
    blockSamples: BlockSample[],
    nodeLabel: (nodeId?: string | null) => string,
}> = (props) => {
    const metric = createMemo(() => {
        const roots = props.data.nodes.map((node) => node.root);
        const finalized = roots.length === 0 ? 0 : Math.max(...roots);
        const pendingMessages = props.data.nodes.reduce(
            (sum, node) => sum + node.pendingMessages.length,
            0,
        );
        const createdQc = props.data.nodes.reduce(
            (sum, node) => sum + node.metrics.consensusCreatedQc,
            0,
        );
        const localTimeout = props.data.nodes.reduce(
            (sum, node) => sum + node.metrics.consensusLocalTimeout,
            0,
        );
        const handledProposal = props.data.nodes.reduce(
            (sum, node) => sum + node.metrics.consensusHandleProposal,
            0,
        );
        const liveNodes = props.data.networkConfig.nodes.filter((node) => node.online).length;

        const samples = props.blockSamples;
        const last = samples.at(-1);
        const previous = samples.length >= 2 ? samples.at(-2) : undefined;
        const recentMsPerBlock = last && previous && last.root > previous.root
            ? Math.round((last.tick - previous.tick) / (last.root - previous.root))
            : undefined;
        const first = samples[0];
        const rollingMsPerBlock = first && last && last.root > first.root
            ? Math.round((last.tick - first.tick) / (last.root - first.root))
            : undefined;

        return {
            finalized,
            pendingMessages,
            createdQc,
            localTimeout,
            handledProposal,
            liveNodes,
            recentMsPerBlock,
            rollingMsPerBlock,
        };
    });

    const displayMs = (value: number | undefined) => value === undefined ? "n/a" : `${value}ms`;

    return (
        <aside
            class="absolute right-4 top-4 z-40 w-64 rounded-lg border border-neutral-300 bg-white/95 p-3 text-sm shadow-lg backdrop-blur"
            data-canvas-wheel-ignore="true"
        >
            <div class="mb-2 flex items-center justify-between gap-3">
                <h2 class="text-base font-semibold">Live Metrics</h2>
                <span class="rounded bg-neutral-100 px-2 py-1 text-xs font-medium text-neutral-700">
                    {metric().liveNodes}/{props.data.networkConfig.nodes.length} live
                </span>
            </div>
            <dl class="grid grid-cols-2 gap-x-3 gap-y-2">
                <Metric label="Finalized" value={metric().finalized} />
                <Metric label="Leader" value={props.nodeLabel(props.data.currentLeader)} />
                <Metric label="Recent block" value={displayMs(metric().recentMsPerBlock)} />
                <Metric label="Rolling block" value={displayMs(metric().rollingMsPerBlock)} />
                <Metric label="Pending msgs" value={metric().pendingMessages} />
                <Metric label="Created QC" value={metric().createdQc} />
                <Metric label="Timeouts" value={metric().localTimeout} />
                <Metric label="Proposals" value={metric().handledProposal} />
            </dl>
        </aside>
    );
};

const Metric: Component<{ label: string, value: string | number }> = (props) => (
    <div>
        <dt class="text-xs text-neutral-500">{props.label}</dt>
        <dd class="text-base font-semibold leading-5 text-neutral-900">{props.value}</dd>
    </div>
);

export default NetworkCanvas;
