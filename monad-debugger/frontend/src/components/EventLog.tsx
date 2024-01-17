import { Component, createMemo, For } from "solid-js";
import { EventLogDocument } from "../generated/graphql";
import { formatNodeId } from "../utils";
import { Simulation } from "../wasm";

const EventLog: Component<{
    simulation: Simulation,
}> = (props) => {
    const eventLog = createMemo(() => props.simulation.fetchUnchecked(EventLogDocument).eventLog);

    return (
        <div class="overflow-auto">
            <For each={eventLog()}>{event =>
                <div>
                    {`${event.tick} => ${formatNodeId(event.id)} => ${event.event.__typename}`}
                </div>
            }</For>
        </div>
    );
};

export default EventLog;
