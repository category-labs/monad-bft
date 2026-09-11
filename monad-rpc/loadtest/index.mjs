export async function handleLogs(context, events) {
    context.ws.onmessage = (e) => {
        let result = JSON.parse(e.data);
        console.assert(result.jsonrpc, "2.0");

        if (result.method != "eth_subscription") {
            return;
        }

        console.assert(result.hasOwnProperty("method"));
        console.assert(result.hasOwnProperty("params"));

        let params = result.params;
        console.assert(params.hasOwnProperty("subscription"));
        console.assert(params.hasOwnProperty("result"));

        let subscription = params.result;
        events.emit('counter', 'monad.logs', 1);
    };
}
