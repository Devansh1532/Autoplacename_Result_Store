import { Kafka } from "kafkajs";
import "dotenv/config"

const brokers = process.env.KAFKA_BROKERS;

const kafka =
  process.env.NODE_ENV === "production"
    ? new Kafka({
        clientId: "drivado-api",
        brokers: process?.env?.KAFKA_BROKERS?.split(","),
        ssl: true,
        ssl: {
          rejectUnauthorized: false,
        },
        sasl: {
          mechanism: "scram-sha-256",
          username: process.env.KAFKA_USERNAME,
          password: process.env.KAFKA_PASSWORD,
        },
        connectionTimeout: 50000,
        requestTimeout: 30000,
        retry: {
          initialRetryTime: 300,
          retries: 10,
        },
      })
    : new Kafka({
        clientId: "drivado-api-test",
        brokers: process?.env?.KAFKA_BROKERS?.split(","),
        ssl: false,
        // ssl: {
        //   rejectUnauthorized: false,
        // },
        // sasl: {
        //         mechanism: "scram-sha-256",
        //         username: process.env.KAFKA_USERNAME,
        //         password: process.env.KAFKA_PASSWORD,
        //       },
          // process.env.NODE_ENV === "production"
          //   ? {
          //       mechanism: "scram-sha-256",
          //       username: process.env.KAFKA_USERNAME,
          //       password: process.env.KAFKA_PASSWORD,
          //     }
          //   : {},

        connectionTimeout: 50000,
        requestTimeout: 30000,
        retry: {
          initialRetryTime: 300,
          retries: 10,
        },
      });

export {kafka}