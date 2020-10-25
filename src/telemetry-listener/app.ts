import { createTableService, ServiceResponse, TableUtilities, TableService } from 'azure-storage';
import { EventHubConsumerClient, PartitionContext, ReceivedEventData } from "@azure/event-hubs";
import { WebSocket } from 'ws';
import { v4 as uuidV4 } from 'uuid';
import * as csv from 'csv-string';
const { entityGenerator } = TableUtilities;


function log(message: string | any, severity?: string) {
  severity = severity ? severity : 'INFO';
  const timestamp = new Date().toISOString();
  const messageAsJson = (typeof message === 'string') ? JSON.stringify({ message: message }) : JSON.stringify(message);
  const logEntry = csv.stringify([timestamp, severity, messageAsJson]);
  console.log(logEntry.trim()); //Trim to remove the newline in the end
}

log('Starting up');

const eventHubCompatibleConnectionStringEnvironmentVariableName = 'CATBELL_LISTENER_IOT_HUB_EVENTHUB_CONNECTIONSTRING';
const eventHubCompatibleConnectionString = process.env[eventHubCompatibleConnectionStringEnvironmentVariableName];
if (!eventHubCompatibleConnectionString) {
  const message = `No eventhub compatible connection string configured. Please set the '${eventHubCompatibleConnectionStringEnvironmentVariableName}' environment variable to a connection string to the event hub compatible endpoint to listen to.`;
  log(message, 'ERROR');
  throw new Error(message);
}

const storageAccountConnectionStringEnvironmentVariableName = 'CATBELL_LISTENER_STORAGE_CONNECTION_STRING';
const storageAccountConnectionString = process.env[storageAccountConnectionStringEnvironmentVariableName];
if (!eventHubCompatibleConnectionString) {
  const message = `No storage account connection string configured. Please set the '${storageAccountConnectionStringEnvironmentVariableName}' environment variable to a connection string to the storage account to use.`;
  log(message, 'ERROR');
  throw new Error(message);
}

function promisedTableService<T>(action: (callback: (error: Error, result: T, response: ServiceResponse) => void) => void): Promise<T> {
  return new Promise((resolve, reject) => {
    try {
      action((error, result, response) => {
        if (error) {
          reject(error);
        }
        else {
          resolve(result);
        }
      })
    }
    catch (error) {
      reject(error);
    }
  });
}


async function main() {

  const tableService = createTableService(storageAccountConnectionString);
  const tableName = process.env['CATBELL_LISTENER_STORAGE_TABLE_NAME'] || 'proximity';

  log('Checking table storage');

  if ((await promisedTableService<TableService.TableResult>(cb => tableService.doesTableExist(tableName, cb))).exists) {
    log('Table storage connection made and table exists');
  }
  else {
    const message = `The table ${tableName} does not exist in the list of existing tables. Please create it before starting the listener.`;
    log(message, 'ERROR');
    throw new Error(message);
  }

  async function handleEvents(events: ReceivedEventData[], context: PartitionContext): Promise<void> {
    for (const event of events) {
      if (event.properties.messageType === 'ProximityInfo') {
        const body = event.body;
        var storageEntity = {
          PartitionKey: entityGenerator.String(event.systemProperties['iothub-connection-device-id']),
          RowKey: entityGenerator.String(`${new Date().toISOString()}-${body.deviceTimestamp}-${uuidV4()}`),
          enqueueTimeUtc: entityGenerator.DateTime(event.enqueuedTimeUtc),
          isProximityDetected: entityGenerator.Boolean(body.isProximityDetected),
          reason: entityGenerator.String(body.reason),
          body: entityGenerator.String(JSON.stringify(body)),
          properties: entityGenerator.String(JSON.stringify(event.properties)),
          systemProperties: entityGenerator.String(JSON.stringify(event.systemProperties))
        }
        log({ message: 'ProximityInfo event received', event, storageEntity });
        await promisedTableService(cb => tableService.insertEntity(tableName, storageEntity, cb));
      }
    }
  }

  const clientOptions = {
    webSocketOptions: {
      webSocket: WebSocket
    }
  };

  const consumerClient = new EventHubConsumerClient("storageconsumer", eventHubCompatibleConnectionString, clientOptions);

  log('Connecting to event hub');
  consumerClient.subscribe({
    processEvents: handleEvents,
    processError: (err) => {
      log({ error: err }, 'ERROR');
      return Promise.resolve();
    }
  });
}

main().catch((error) => {
  console.error("Error running sample:", error);
});
