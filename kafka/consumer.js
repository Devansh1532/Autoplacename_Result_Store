/*

This is the code for Consumer in Kafka's server
The job of Consumer is to accpet data from the Kafka pipeline and save it into MongoDB

*/

const kafka = require("../kafka");
const { TOPICS } = require("./topics");
const { saveData } = require("../controllers/saveData");
// const { kafka } = require("kafkajs");

const consumer = kafka.consumer({ groupId: "drivado.search.group" });

async function startConsumer() {
  await consumer.connect();

  console.log("Consumer connected");
}

async function receiveMessage() {
  try {
    await consumer.subscribe({
      topics: [TOPICS.CLICKED_VALUE, TOPICS.GOOGLE_RESULT],
      fromBeginning: false,
    });
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log(
          `Recieved message: ${topic} ${partition} ${message.toString()}`,
        );
        await saveData(topic, JSON.parse(message.value));
      },
    });
  } catch (error) {
    console.error("Consumer error:", error);
  }
}

module.exports = { startConsumer, receiveMessage };