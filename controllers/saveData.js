const { backupData, clickValue, countValue } = require("../models/schema");

const { TOPICS } = require("../kafka/topics");



async function saveData(topic, messages) {
  if (topic == TOPICS.CLICKED_VALUE) {
    await clickValue.create({
     searchQuery: messages.searchQuery,  
     placeId: messages.place.placeId,  
     timestamp: messages.timestamp
    

    }
    );
    const count =  countValue.findOne({placeId: messages.place.placeId})
    const q = await count.exec()
    console.log(q)
    if(q == null){
      await countValue.create({
        placeId: messages.place.placeId,
        count: 1,
        timestamp: messages.timestamp
    })}
    else {

      await countValue.updateOne({placeId: messages.place.placeId},{$inc: {count: 1}})

    }
    console.log("Saved click data to MongoDB", messages);
  } else if (topic == TOPICS.BACKUP_DATA) {
    await backupData.create({
      searchQuery: messages.searchQuery,    
      timestamp: messages.timestamp,
      place: messages.place,
      source: messages.source,
    });

    console.log("Saved backup data to MongoDB");
  }
}

module.exports = { saveData };

