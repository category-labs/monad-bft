import * as monad from "../../../pkg/monad_debugger";

import { TypedDocumentNode } from "@graphql-typed-document-node/core";
import { print } from 'graphql'


type Result<T, E> = {
    Ok?: T;
    Err?: E;
};

export class Simulation {
    private simulation: number;

    constructor() {
        this.simulation = monad.simulation_make();
    }

    /// Must be called before last reference is dropped
    /// Otherwise will leak memory
    public free() {
        monad.simulation_free(this.simulation);
    }

    public schema(): string {
        return monad.simulation_schema(this.simulation);
    }

    public query(query: string): Result<any, String> {
        return JSON.parse(monad.simulation_query(this.simulation, query));
    }

    public fetch<TData = any, TVariables = Record<string, any>>(
        operation: TypedDocumentNode<TData, TVariables>,
        variables?: TVariables
    ): Result<TData, String> {
        if (variables) {
            alert("TODO support variables")
        }
        return this.query(print(operation));
    }

    public fetchUnchecked<TData = any, TVariables = Record<string, any>>(
        operation: TypedDocumentNode<TData, TVariables>,
        variables?: TVariables
    ): TData {
        const result = this.fetch(operation, variables);
        if (result.Err) {
            alert(result.Err);
        }
        return result.Ok!;
    }

    public setTick(tickMs: number) {
        monad.simulation_set_tick(this.simulation, tickMs);
    }

    public step() {
        monad.simulation_step(this.simulation);
    }

    public reset() {
        monad.simulation_reset(this.simulation);
    }

    public setDefaultLatency(latencyMs: number) {
        this.command(monad.simulation_set_default_latency(this.simulation, latencyMs));
    }

    public setLinkLatency(fromId: string, toId: string, latencyMs: number) {
        this.command(monad.simulation_set_link_latency(this.simulation, fromId, toId, latencyMs));
    }

    public setLinkDropped(fromId: string, toId: string, dropped: boolean) {
        this.command(monad.simulation_set_link_dropped(this.simulation, fromId, toId, dropped));
    }

    public clearLinkRule(fromId: string, toId: string) {
        this.command(monad.simulation_clear_link_rule(this.simulation, fromId, toId));
    }

    public setNodePosition(nodeId: string, x: number, y: number) {
        this.command(monad.simulation_set_node_position(this.simulation, nodeId, x, y));
    }

    public setNodeOnline(nodeId: string, online: boolean) {
        this.command(monad.simulation_set_node_online(this.simulation, nodeId, online));
    }

    private command(resultJson: string) {
        const result: Result<void, string> = JSON.parse(resultJson);
        if (result.Err !== undefined) {
            throw new Error(result.Err);
        }
    }
}
