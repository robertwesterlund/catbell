import { Client, Message } from 'azure-iot-device'
import { MqttWs as IoTHubProtocol } from 'azure-iot-device-mqtt'
import * as process from 'process'
import * as csv from 'csv-string'
import { MessageEnqueued } from 'azure-iot-common/dist/results';
import { Gpio } from 'onoff';

function log(message: string | any, severity?: string) {
    severity = severity ? severity : 'INFO';
    const timestamp = new Date().toISOString();
    const messageAsJson = (typeof message === 'string') ? JSON.stringify({ message: message }) : JSON.stringify(message);
    const logEntry = csv.stringify([timestamp, severity, messageAsJson]);
    console.log(logEntry.trim()); //Trim to remove the newline in the end
}

log("Catbell application starting");

const iotHubConnectionstringEnvironmentVariableName = 'CATBELL_DEVICE_IOT_HUB_CONNECTIONSTRING';

const iotHubConnectionstring = process.env[iotHubConnectionstringEnvironmentVariableName];
if (!(iotHubConnectionstring)) {
    throw new Error(`No connectionstring configured. Please ensure the '${iotHubConnectionstringEnvironmentVariableName}' environment variable is set`)
}

const sleep = (milliseconds: number) => new Promise((resolve, reject) => {
    setTimeout(resolve, milliseconds);
});

// Written to be used with the HC-SR501 sensor (a passive infrared movement sensor)
class PirSensor {
    private gpioPortNumber: number;
    private sensor: Gpio;
    private intervalMs: number;
    private debounceMs: number;
    private lastTimeSensorFoundMovement: Date | undefined = new Date();
    private listeners: Array<({ isProximityDetected: boolean }) => void | Promise<void>> = [];

    constructor(config: { gpioPortNumber: number, intervalMs: number, debounceMs: number }) {
        log({ message: 'Creating PirSensor', ...config });
        this.gpioPortNumber = config.gpioPortNumber;
        this.sensor = new Gpio(this.gpioPortNumber, 'in');
        this.intervalMs = config.intervalMs;
        this.debounceMs = config.debounceMs;
    }

    start() {
        this.lastTimeSensorFoundMovement = new Date();
        setInterval(() => this.checkSensor(), this.intervalMs);
    }

    private checkSensor() {
        const sensorValue = this.sensor.readSync();
        const currentTime = new Date();
        if (sensorValue === 1) {
            if (this.lastTimeSensorFoundMovement === undefined || currentTime.getTime() - this.lastTimeSensorFoundMovement.getTime() > this.debounceMs) {
                this.alertListeners(true);
            }
            this.lastTimeSensorFoundMovement = currentTime;
        }
        else {
            if (this.lastTimeSensorFoundMovement !== undefined && currentTime.getTime() - this.lastTimeSensorFoundMovement.getTime() > this.debounceMs) {
                this.alertListeners(false);
                this.lastTimeSensorFoundMovement = undefined;
            }
        }
    }

    private alertListeners(isProximityDetected: boolean) {
        this.listeners.forEach(listener => listener({ isProximityDetected }));
    }

    onChange(listener: ({ isProximityDetected: boolean }) => void | Promise<void>) {
        this.listeners.push(listener);
    }
}

function sendProximityInfoToIotHub(proximityInfo: { isProximityDetected: boolean, reason: string }): Promise<void> {
    const messageType = 'ProximityInfo';
    var data = {
        deviceTimestamp: new Date().toISOString(),
        messageType: messageType,
        isProximityDetected: proximityInfo.isProximityDetected
    };
    var message = new Message(JSON.stringify(data));
    message.properties.add('messageType', messageType);
    log({ message: "Sending proximity info", data: data });
    return new Promise((resolve, reject) => {
        client.sendEvent(message, (err, _) => {
            if (err) { reject(err); }
            else { resolve(); }
        });
    });
}

async function listenToSensorsAndSendData() {
    const sensorPort = parseInt(process.env['CATBELL_DEVICE_SENSOR_GPIO_PORT'], 10) || 24;
    log('Connecting to sensor');
    var sensor = new PirSensor({ gpioPortNumber: sensorPort, intervalMs: 100, debounceMs: 20000 });
    let lastSentResult = false;
    let lastResultSentAt = new Date();
    const sendProximityInfo = async (info: { isProximityDetected, reason }) => {
        await sendProximityInfoToIotHub(info);
        log({
            message: 'Sensor value sent',
            reason: info.reason,
            previous: { value: lastSentResult, sentAt: lastResultSentAt.toISOString() },
            current: { value: info.isProximityDetected, sentAt: new Date().toISOString() }
        });
        lastSentResult = info.isProximityDetected;
        lastResultSentAt = new Date();
    };
    sensor.onChange(({ isProximityDetected }) => sendProximityInfo({ isProximityDetected, reason: 'change' }));
    const heartbeatInterval = 60000;
    setInterval(() => {
        const currentTime = new Date();
        if (currentTime.getTime() - lastResultSentAt.getTime() > heartbeatInterval) {
            sendProximityInfo({ isProximityDetected: lastSentResult, reason: 'heartbeat' });
        }
    }, heartbeatInterval);
    sensor.start();
}

log('Connecting to IoTHub');
const client = Client.fromConnectionString(iotHubConnectionstring, IoTHubProtocol);

client.open(err => {
    if (err) {
        throw new Error(`Could not connect to IoTHub. Error response given: ${err}`);
    }
    else {
        log('Connected to IoT Hub');

        const messageType = 'DeviceStarted';
        const data = {
            messageType: messageType,
            deviceTimestamp: new Date().toISOString()
        };
        var message = new Message(JSON.stringify(data));
        message.properties.add('messageType', messageType);
        log(data);
        client.sendEvent(message, (err, _) => {
            if (err) {
                const errorMessage = `Failed sending device started message due to: ${JSON.stringify(err)}`;
                log(errorMessage, 'ERROR');
                throw new Error(errorMessage);
            }
            else {
                listenToSensorsAndSendData();
            }
        });
    }
});