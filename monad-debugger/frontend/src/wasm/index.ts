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
}