const formatNodeId = (nodeId: string) => nodeId.slice(0, 7);
const formatEvent = (event: string) => event.replace('GraphQL', '');

export {
    formatNodeId,
    formatEvent
}