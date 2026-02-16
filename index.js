/*

This server acts as the convergence point for all that is happening in this kafka_test folder

*/
const express = require("express");
const { connectMongoDB } = require("./connections");
const { startConsumer } = require("./kafka/consumer");

// const { TOPICS } = require("./topics");
const { receiveMessage } = require("./kafka/consumer");
require("dotenv").config()
const app = express();
// const app2 = express();


app.use(express.json());
// app.use("/results", router);
// // app.listen(process.env.PORT, () => console.log("Server Started"));
app.listen(process.env.PORT, '0.0.0.0', () => console.log("Server Started"));

(async () => {
  console.log(process.env.KAFKA_BROKERS)
  await startConsumer();
})();

// const mongourl = "mongodb://${MONGO_INITDB_ROOT_USERNAME}:${MONGO_INITDB_ROOT_PASSWORD}"+"@${MONGO_HOST}/${MONGO_DB}?authSource=admin"
// const mongourl = "mongodb://mongo:27017/user-database"

const mongourl = process.env.MONGO_URL
console.log(mongourl)
connectMongoDB(mongourl).then(() => {
  console.log("MongoDB connected");
});

(async () => {
  await receiveMessage();
})();
