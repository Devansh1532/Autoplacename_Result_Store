const { Kafka } = require("kafkajs");
require("dotenv").config();

const kafka = new Kafka({
  clientId: "my-app",
  brokers: [...process.env.KAFKA_BROKERS.split(",")],
  connectionTimeout: 50000,
  requestTimeout: 30000,
  // retry: {
  //       initialRetryTime: 300,
  //       retries: 10,
  //     }
});

module.exports = kafka;
