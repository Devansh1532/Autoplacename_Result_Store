
import express from "express"
import { connectMongoDB} from "./connections.js";
import { runConsumer }  from "./kafka/consumer.js";
import {kafka} from "./kafka.js";
import {TOPICS} from "./kafka/topics.js";
import {drizzle} from "drizzle-orm/mysql2";
// import {connectMySQL} from './connections.js';
import "dotenv/config"

const app = express();

app.use(express.json());


const mongourl = process.env.MONGO_URL
console.log(mongourl)
connectMongoDB(mongourl).then(() => {
  console.log("MongoDB connected");
});

(async () => {
  await runConsumer({
    kafka,
    groupId: "drivado.search.orsdata.group",
    topic: TOPICS.ORS_RESPONSE

  });
  await runConsumer({
    kafka,
    groupId: "drivado.search.clickedvalue.group",
    topic: TOPICS.CLICKED_VALUE

  });
  await runConsumer({
    kafka,
    groupId: "drivado.search.backupdata.group",
    topic: TOPICS.EXTERNAL_DATA

  });
  
})();

app.listen(process.env.PORT, '0.0.0.0', () => console.log("Server Started"));










