require('dotenv').config();
const fs = require("fs");
const express = require('express');
const { Kafka } = require('kafkajs');
const { Server: WebSocketServer } = require('ws');

// Load environment variables
const PORT = process.env.PORT || 8080;
const KAFKA_BROKER = process.env.KAFKA_BROKER;
const KAFKA_USERNAME = process.env.KAFKA_USERNAME;
const KAFKA_PASSWORD = process.env.KAFKA_PASSWORD;
const TOPIC_NAME = process.env.TOPIC_NAME || 'sigma-band-data';
const CA_PEM = process.env.CA_PEM || 'ca.pem';
const GROUP_ID = process.env.KAFKA_GROUP_ID || 'flutter-group';

// Initialize Express app
const app = express();
const server = app.listen(PORT, () => {
    console.log(`Express server running on port ${PORT}`);
});

// Create WebSocket server
const wss = new WebSocketServer({ server });
console.log(`WebSocket server started on port ${PORT}`);

// Store clients mapped by `device_id`
const deviceClients = {};

// WebSocket Connection Handling
wss.on('connection', (ws) => {
    console.log('New WebSocket connection established');

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);

            if (data.device_id) {
                deviceClients[data.device_id] = deviceClients[data.device_id] || new Set();
                deviceClients[data.device_id].add(ws);
                console.log(`Client subscribed to device_id: ${data.device_id}`);
            }
        } catch (error) {
            console.error("Invalid message received:", message);
        }
    });

    ws.on('close', () => {
        console.log('WebSocket connection closed');

        // Remove closed connection from `deviceClients`
        Object.keys(deviceClients).forEach((device_id) => {
            deviceClients[device_id].delete(ws);
            if (deviceClients[device_id].size === 0) {
                delete deviceClients[device_id];
            }
        });
    });
});

// Kafka Configuration
const kafka = new Kafka({
    clientId: 'kafka-proxy',
    brokers: [KAFKA_BROKER],
    ssl: {
        ca: [CA_PEM],
    },
    sasl: {
        mechanism: 'plain',
        username: KAFKA_USERNAME,
        password: KAFKA_PASSWORD,
    },
    retry: {
        initialRetryTime: 100,
        retries: 8,
    },
});

const consumer = kafka.consumer({ groupId: GROUP_ID });

// Kafka Consumer
const runKafkaConsumer = async () => {
    try {
        await consumer.connect();
        await consumer.subscribe({ topic: TOPIC_NAME, fromBeginning: false });
        console.log(`Subscribed to Kafka topic: ${TOPIC_NAME}`);

        await consumer.run({
            eachMessage: async ({ message }) => {
                const messageData = message.value.toString();
                console.log(`Received Kafka message: ${messageData}`);

                try {
                    const data = JSON.parse(messageData);
                    const { device_code } = data;

                    if (deviceClients[device_code]) {
                        deviceClients[device_code].forEach((client) => {
                            if (client.readyState === client.OPEN) {
                                client.send(messageData);
                            }
                        });
                    }
                } catch (error) {
                    console.error("Error processing Kafka message:", error);
                }
            },
        });
    } catch (error) {
        console.error('Error in Kafka consumer:', error);
    }
};

// Start Kafka Consumer
runKafkaConsumer().catch((err) => {
    console.error('Error starting Kafka consumer:', err);
});
