import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocument } from "@aws-sdk/lib-dynamodb";

const dynamoClient = DynamoDBDocument.from(new DynamoDBClient({}));

const isValidTxhash = (txhash) => {
    return txhash.length === 64;
};

async function getTxData(txhash, table) {
    try {
        if (!isValidTxhash(txhash)) {
            return [400, { error: `Invalid transaction hash format, txhash ${txhash}, table ${table}` }];
        }

        // Strip 0x prefix for consistency with existing implementation
        txhash = txhash.toLowerCase().replace('0x', '');

        const response = await dynamoClient.get({
            TableName: table,
            Key: {
                tx_hash: txhash
            }
        });

        console.log(response);

        if (!response.Item) {
            return [404, { error: `Transaction ${txhash} not found` }];
        }

        // Convert DynamoDB Binary type to base64 string
        const processedItem = {};
        for (const [key, value] of Object.entries(response.Item)) {
            if (value?.type === 'Buffer' || ArrayBuffer.isView(value)) {
                processedItem[key] = Buffer.from(value).toString('base64');
            } else {
                processedItem[key] = value;
            }
        }

        return [200, processedItem];

    } catch (error) {
        console.error("DynamoDB error:", error);
        return [500, { error: "Internal server error" }];
    }
}

export const handler = async (event) => {
    try {
        console.log(event);
        const txhash = event.queryStringParameters?.txhash;
        const table = event.queryStringParameters?.table;
        console.log(txhash, table);

        if (!txhash) {
            return {
                statusCode: 400,
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ error: 'Missing txhash parameter' })
            };
        }

        // Get data from DynamoDB
        const [statusCode, responseBody] = await getTxData(txhash, table);

        return {
            statusCode,
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(responseBody)
        };

    } catch (error) {
        console.error("Unexpected error:", error);
        return {
            statusCode: 500,
            body: JSON.stringify({
                error: `Internal server error. ${error.message}`
            })
        };
    }
};