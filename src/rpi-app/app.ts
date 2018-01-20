import { Client, Message } from 'azure-iot-device'
import { MqttWs as IoTHubProtocol } from 'azure-iot-device-mqtt'
import * as process from 'process'
import * as csv from 'csv-string'
import { MessageEnqueued } from 'azure-iot-common/lib/results';

log("Catbell application starting");

const iotHubConnectionstringEnvironmentVariableName = 'IOT_HUB_CONNECTIONSTRING';

const iotHubConnectionstring = process.env[iotHubConnectionstringEnvironmentVariableName];
if (!(iotHubConnectionstring)) {
    throw new Error(`Not connectionstring configured. Please ensure the '${iotHubConnectionstringEnvironmentVariableName}' environment variable is set`)
}

const client = Client.fromConnectionString(iotHubConnectionstring, IoTHubProtocol);

client.open(err => {
    if (err) {
        throw new Error(`Could not connect to IoTHub. Error response given: ${err}`);
    }
    else {
        onIotHubConnected();
    }
});

function log(message: string | any, severity?: string) {
    severity = severity ? severity : 'INFO';
    const timestamp = new Date().toISOString();
    const messageAsJson = typeof (message === 'string') ? JSON.stringify({ message: message }) : JSON.stringify(message);
    const logEntry = csv.stringify([timestamp, severity, messageAsJson]);
    console.log(logEntry.trim()); //Trim to remove the newline in the end
}

function onIotHubConnected() {
    log('Connected to IoT Hub');

    sendProximityInfo(true);
}

function createLoggingHandlerFor(op: string) {
    return function logResult(err?: Error, res?: any) {
        if (err) log(op + ' error: ' + err.toString(), 'ERROR');
        if (res) log(op + ' status: ' + res.constructor.name);
    };
}

function sendProximityInfo(isProximityDetected: boolean, callback?: (err?: Error, result?: MessageEnqueued) => void) {
    const messageType = 'ProximityInfo';
    var data = {
        deviceTimestamp: new Date().toISOString(),
        messageType: messageType,
        isProximityDetected: isProximityDetected
    };
    var message = new Message(JSON.stringify(data));
    message.properties.add('MessageType', messageType);
    log({message: "Sending proximity info", data: data});
    if (!callback){
        callback = createLoggingHandlerFor('sendProximityInfo');
    }
    client.sendEvent(message, callback);
}