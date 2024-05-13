import { Component, createEffect, createMemo, createSignal, For } from "solid-js";
import { EventLogDocument } from "../generated/graphql";
import { formatNodeId, formatEvent } from "../utils";
import { Simulation } from "../wasm";

const EventLog: Component<{
    simulation: Simulation,
}> = (props) => {
    const eventLog = createMemo(() => {
        return props.simulation.fetchUnchecked(EventLogDocument).eventLog.reverse();
    });

    return (
        <div class="overflow-auto" style={{'display': 'flex', 'flex-direction': 'column-reverse'}}>
            <For each={eventLog()}>{event =>
                <div style={{
                    'border-bottom': '1px solid #000',
                    'font-size': '10px',
                    'white-space': 'pre'
                }}>
                    {`${event.tick} => ${formatNodeId(event.id)} => ${formatEvent(event.event.debug)}`}
                </div>
            }</For>
        </div>
    );
};

export default EventLog;
