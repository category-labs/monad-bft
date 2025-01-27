import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocument, GetCommand, PutCommand } from "@aws-sdk/lib-dynamodb";
import { APIGatewayClient, CreateApiKeyCommand, CreateUsagePlanKeyCommand } from "@aws-sdk/client-api-gateway";

const dynamoClient = DynamoDBDocument.from(new DynamoDBClient({}));
const apiGatewayClient = new APIGatewayClient({});

export async function handler(event) {
  try {
    // Extract organizationId from event context
    const orgId = event.requestContext?.identity?.principalOrgId;
    if (!orgId) {
      console.error('Missing principalOrgId in request context');
      return {
        statusCode: 400,
        body: JSON.stringify({ error: 'Missing organization ID' })
      };
    }

    // Check if org already has an API key
    const existingKey = await dynamoClient.send(new GetCommand({
      TableName: process.env.TABLE_NAME,
      Key: { orgId: orgId }
    }));

    if (existingKey.Item) {
      return {
        statusCode: 200,
        body: JSON.stringify({ apiKey: existingKey.Item.apiKey })
      };
    }

    // Create new API key
    const createKeyResponse = await apiGatewayClient.send(new CreateApiKeyCommand({
      name: `org-${orgId}-${Date.now()}`,
      description: `API key for organization ${orgId}`,
      enabled: true
    }));

    // Associate key with usage plan
    await apiGatewayClient.send(new CreateUsagePlanKeyCommand({
      usagePlanId: process.env.USAGE_PLAN_ID,
      keyId: createKeyResponse.id,
      keyType: 'API_KEY'
    }));

    // Store mapping in DynamoDB
    await dynamoClient.send(new PutCommand({
      TableName: process.env.TABLE_NAME,
      Item: {
        orgId: orgId,
        apiKey: createKeyResponse.value
      }
    }));

    return {
      statusCode: 200,
      body: JSON.stringify({ apiKey: createKeyResponse.value })
    };

  } catch (error) {
    console.error('Error generating API key:', error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: 'Internal server error' })
    };
  }
};