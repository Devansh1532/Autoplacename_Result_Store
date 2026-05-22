import "dotenv/config";
import { pgdb } from "../connections.js";
import { sql } from "drizzle-orm";
import {format} from "node-pg-format"

import {
  externalDataTable,
  clickResultTable,
  placeCountTable,
  routeCountTable,
  orsDataTable,
} from "../models/orm_schema.js";
import {
  externalData,
  clickValue,
  placeCountValue,
  routeCountValue,
  orsData,
} from "../models/schema.js";
import { z } from "zod";
import { TOPICS } from "../kafka/topics.js";
// import { text } from "drizzle-orm/gel-core";

const externalDataValid = z.object({
  placeId: z.string(),
  timestamp: z.string(),
  lat: z.float32(),
  lng: z.float32(),
  postcode: z.string(),
  address: z.string(),
  source: z.string(),
});

const clickResultValid = z.object({
  searchQuery: z.string(),
  timestamp: z.string(),
  placeId: z.string(),
});

const orsDataValid = z.object({
  from_lat: z.float32(),
  from_lng: z.float32(),
  to_lat: z.float32(),
  to_lng: z.float32(),
})

const click_buffer = [];
const external_buffer = [];
const ors_buffer = [];

async function cacheData(topic, messages) {
  if (topic == TOPICS.CLICKED_VALUE) click_buffer.push(messages);
  else if (topic == TOPICS.EXTERNAL_DATA) external_buffer.push(messages);
  else if (topic == TOPICS.ORS_RESPONSE) ors_buffer.push(messages);
}

async function updatePlaceCount(savedPlaceBatch) {
  let i = savedPlaceBatch.length - 1;
  try {
    while (i >= 0) {
      await placeCountValue.updateOne(
        { placeId: savedPlaceBatch[i]["placeId"] },
        { $inc: { count: 1 } },
        { upsert: true },
      );
      await pgdb
        .insert(placeCountTable)
        .values({
          placeId: savedPlaceBatch[i]["placeId"],
          timestamp: savedPlaceBatch[i]["insertedAt"],
          count: 1,
        })
        .onConflictDoUpdate({ target: placeCountTable.placeId, targetWhere: sql`${placeCountTable.placeId} = ${savedPlaceBatch[i--]}`, set: { count: sql`${placeCountTable.count}+ 1` } });
    }
    console.log("Saved count value to Postgres successfully!");
  } catch (error) {
    console.log(
      "\n\bERROR: There was a problem while sending the count value to Postgres, check the message below ",
    );
    console.log(error);
  }
  
}

async function updateRouteCount(savedRouteBatch) {
  let i = savedRouteBatch.length - 1;
  try {
    while (i >= 0) {
      await routeCountValue.updateOne(
        { route_id: savedRouteBatch[i]["route_id"] },
        { $inc: { count: 1 } },
        { upsert: true },
      );
      await pgdb
        .insert(routeCountTable)
        .values({
          route_id: savedRouteBatch[i]["route_id"],
          timestamp: savedRouteBatch[i]["timestamp"],
          count: 1,
        })
        .onConflictDoUpdate({ target: routeCountTable.route_id, targetWhere: sql`${routeCountTable.route_id} = ${savedRouteBatch[i--]}`, set: { count: sql`${routeCountTable.count}+ 1` } });
    }
    console.log("Saved route count value to Postgres successfully!");
  } catch (error) {
    console.log(
      "\n\bERROR: There was a problem while sending the route count value to Postgres, check the message below ",
    );
    console.log(error);
  }
  
}

async function flushORSBuffer() {
  if (ors_buffer.length == 0) return;
  let orsBatchData = ors_buffer.splice(0, ors_buffer.length);
  console.log(orsBatchData);
  const updatedORSBatch = orsBatchData.map((op, i) => {
    if (orsBatchData[i]["distance_km"] || orsBatchData[i]["duration_min"] != null) {
      op = {
        ...op,
        route_id:
          "Drv_" +
          (
            Math.abs(
              (Number(orsBatchData[i]["from_lng"]) +
                Number(orsBatchData[i]["from_lat"]) +
                Number(orsBatchData[i]["distance_km"])) *
                10000,
            ).toFixed(0) +
              Math.abs(
                (Number(orsBatchData[i]["to_lng"]) +
                  Number(orsBatchData[i]["to_lat"]) +
                  Number(orsBatchData[i++]["duration_min"])) *
                  10000,
              ).toFixed(0)
          )
      };
    } else {
      op = { ...op, route_id: "Drv_" + "null_" + (
            Math.abs(
              (Number(orsBatchData[i]["from_lng"]) +
                Number(orsBatchData[i]["from_lat"])) *
                10000,
            ).toFixed(0) +
              Math.abs(
                (Number(orsBatchData[i]["to_lng"]) +
                  Number(orsBatchData[i]["to_lat"])) *
                  10000,
              ).toFixed(0)
          ) };
    }
    return op;
  });
  // const updatedBatchData = await Promise.all(fetchAllPromises)
  console.log(updatedORSBatch);
  updateRouteCount(updatedORSBatch);
  // dbStore()
  await orsData.insertMany(updatedORSBatch);
  console.log("Saved ORS data to MongoDB");
  try {
    await pgdb.insert(orsDataTable).values(updatedORSBatch)
    console.log("Sent the ORS data to PostgreSQL!");
  } catch (error) {
    console.log(
     "\n\bERROR: There was a problem while sending the ORS data to PostgreSQL, check the message below",
    );
    console.log(error);
   
  }
}

async function flushExternalBuffer() {
  if (external_buffer.length == 0) return;
  const insertedAt = Date.now().toString();

  let extBatchData = external_buffer.splice(0, external_buffer.length);

  const extUpdatedData = extBatchData.map((op) => {
    let updated_op = { ...op, insertedAt: insertedAt };

    return updated_op;
  });
  console.log(extUpdatedData);
  await externalData.insertMany(extUpdatedData);
  console.log("Saved external data to MongoDB");
  try {
    await pgdb.insert(externalDataTable).values(extUpdatedData)
    console.log("Saved the external location details to PostgreSQL");
  } catch (error) {
    console.log(
      "\n\bERROR: There was a problem while sending the external data to PostgreSQL, check the message below",
    );
    console.log(error);
  }
}

async function flushClickBuffer() {
  if (click_buffer.length == 0) return;
  const insertedAt = Date.now().toString();

  let batchData = click_buffer.splice(0, click_buffer.length);

  const updatedBatchData = batchData.map((op) => {
    let updated_op = { ...op, insertedAt: insertedAt };

    return updated_op;
  });

  updatePlaceCount(updatedBatchData);
  await clickValue.insertMany(updatedBatchData);
  try {
    await pgdb.insert(clickResultTable).values(updatedBatchData)
    console.log("Saved the clicked location details to PostgreSQL");
  } catch (error) {
    console.log(
      "\n\bERROR: There was a problem while sending the clicked option data to PostgreSQL, check the message below",
    );
    console.log(error);
  }
}

async function saveData(topic, messages) {
  if (topic == TOPICS.CLICKED_VALUE) {
    const result = clickResultValid.safeParse(messages);
    if (!result.success) {
      console.log(result.error);
    } else {
      cacheData(topic, messages);
      setTimeout(() => {
        flushClickBuffer();
      }, 60000);
    }
  } else if (topic == TOPICS.EXTERNAL_DATA) {
    const result = externalDataValid.safeParse(messages);
    if (!result.success) {
      console.log(result.error);
    } else {
      cacheData(topic, messages);
      setTimeout(() => {
        flushExternalBuffer();
      }, 60000);
    }
  } else if (topic == TOPICS.ORS_RESPONSE) {
    const result = orsDataValid.safeParse(messages);
    if(!result.success){
      console.log(result.error);
    } else {
      cacheData(topic, messages);
      setTimeout(() => {
        flushORSBuffer();
      }, 60000);
    }
  }
}

export { saveData };
