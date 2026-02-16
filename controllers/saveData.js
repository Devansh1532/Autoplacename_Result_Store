const { clickValue, googleValue } = require("../models/schema");

const { TOPICS } = require("../kafka/topics");

async function saveData(topic, messages) {
  if (topic == TOPICS.CLICKED_VALUE) {
    await clickValue.create({
     searchQuery: messages.searchQuery,    
     timestamp: messages.timestamp,
     place: messages.place,
     source : messages.source

    }
    );
    console.log("Saved click data to MongoDB", messages.value);
  } else if (topic == TOPICS.GOOGLE_RESULT) {
    await googleValue.create({
      success: messages.success,
      query: messages.query,
      count: messages.count,
      result: messages.result,
      source: messages.source,
    });

    console.log("Saved google data to MongoDB");
  }
}

module.exports = { saveData };
