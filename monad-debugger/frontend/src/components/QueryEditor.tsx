import { Component, createSignal, onCleanup, onMount } from "solid-js";
import { Simulation } from "../wasm";

import { EditorView, keymap } from '@codemirror/view';
import { indentWithTab } from '@codemirror/commands';
import { acceptCompletion } from '@codemirror/autocomplete';
import { basicSetup } from 'codemirror';
import { vim } from "@replit/codemirror-vim"
import { graphql } from 'cm6-graphql';
import { buildSchema } from "graphql";

const QueryEditor: Component<{
    simulation: Simulation,
}> = (props) => {
    let queryEditor: HTMLDivElement;
    const graphqlExtension = graphql(buildSchema(props.simulation.schema()));

    const [query, setQuery] = createSignal("query {\n  nextTick\n}");

    const view = new EditorView({
        doc: query(),
        extensions: [
            vim(), 
            basicSetup,
            keymap.of([
                {
                    run: acceptCompletion,
                    key: 'Tab',
                }, indentWithTab
            ]),
            graphqlExtension,
            EditorView.updateListener.of(function(e) {
                setQuery(e.state.doc.toString());
            })
        ],
    });

    onMount(() => {
        queryEditor.appendChild(view.dom);
        view.dom.addEventListener("update", () => {
            alert("updated");
            setQuery(view.state.doc.toString());
        });
    });

    onCleanup(() => {
        view.destroy();
    });

    return (
        <div class="w-1/4 flex flex-col">
            <div class="h-1/2" ref={queryEditor}></div>
            <pre class="overflow-auto">
                { JSON.stringify(props.simulation.query(query()), null, 4) }
            </pre>
        </div>
    );
};

export default QueryEditor;
