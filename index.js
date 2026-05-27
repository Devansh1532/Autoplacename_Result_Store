import express from "express"
import { runConsumer }  from "./kafka/consumer.js";
import {kafka} from "./kafka.js";
import {KAFKA_CONSUMERS} from "./kafka/topics.js";
import "dotenv/config"

const app = express();
app.use(express.json());

(async () => {
  for (const consumer of KAFKA_CONSUMERS){
    await runConsumer({
      kafka,
      groupId: consumer['GROUP'],
      topic: consumer['TOPIC']
    })
  }
})()

app.listen(process.env.PORT, '0.0.0.0', () => console.log("Server Started"));










