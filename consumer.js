// server.js
require('dotenv').config();
const fs = require("fs");
const express = require('express');
const { Kafka } = require('kafkajs');
const { Server: WebSocketServer } = require('ws');

// Load environment variables from .env
const PORT = process.env.PORT || 8080;
const KAFKA_BROKER = process.env.KAFKA_BROKER;
const KAFKA_USERNAME = process.env.KAFKA_USERNAME;
const KAFKA_PASSWORD = process.env.KAFKA_PASSWORD;
const TOPIC_NAME = process.env.TOPIC_NAME || 'sensor-data';
const GROUP_ID = process.env.KAFKA_GROUP_ID || 'flutter-group';

// Initialize Express app
const app = express();

// Start the Express server
const server = app.listen(PORT, () => {
    console.log(`Express server running on port ${PORT}`);
});

// Create a WebSocket server using the existing Express server
const wss = new WebSocketServer({ server });
console.log(`WebSocket server started on port ${PORT}`);

// Log new WebSocket connections
wss.on('connection', (ws) => {
    console.log('New WebSocket connection established');
    ws.on('close', () => console.log('WebSocket connection closed'));
});

// Configure KafkaJS
const kafka = new Kafka({
    clientId: 'kafka-proxy',
    brokers: [KAFKA_BROKER],
    ssl: {
        ca: [fs.readFileSync("ca.pem", "utf-8")], // Load the CA certificate
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

// Function to run the Kafka consumer
const runKafkaConsumer = async () => {
    try {
        await consumer.connect();
        await consumer.subscribe({ topic: TOPIC_NAME, fromBeginning: false });
        console.log(`Subscribed to Kafka topic: ${TOPIC_NAME}`);

        await consumer.run({
            eachMessage: async ({ message }) => {
                const messageData = message.value.toString();
                console.log(`Received message: ${messageData}`);

                // Broadcast the message to all connected WebSocket clients
                wss.clients.forEach((client) => {
                    if (client.readyState === client.OPEN) {
                        client.send(messageData);
                    }
                });
            },
        });
    } catch (error) {
        console.error('Error in Kafka consumer:', error);
    }
};

runKafkaConsumer().catch((err) => {
    console.error('Error starting Kafka consumer:', err);
});
