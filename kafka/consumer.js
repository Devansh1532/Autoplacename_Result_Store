import { saveData } from "../controllers/saveData.js";

async function runConsumer({ kafka, groupId, topic }) {
  try {
    const consumer = kafka.consumer({ groupId, heartbeatInterval : 3000 });
    await consumer.connect();
    console.log(`Consumer receiving topic ${topic} started`);
    await consumer.subscribe({
      topic,
      fromBeginning: false,
    });
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log(
          `Recieved message: ${topic} ${partition} ${message.value}`,
        );
        await saveData(topic, JSON.parse(message.value));
      },
    });
  } catch (error) {
    console.error(`Consumer running ${topic} ran into an error`, error);
  }
}

export { runConsumer };










