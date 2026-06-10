import "dotenv/config";
import { pgdb } from "../connections.js";
import { sql } from "drizzle-orm";
import crypto from 'crypto';
import {
  externalDataTable,
  clickResultTable,
  placeCountTable,
  routeCountTable,
  orsDataTable,
} from "../models/orm_schema.js";

import { KAFKA_CONSUMERS } from "../kafka/topics.js";

import {
  externalSchema,
  clickSchema,
  orsSchema
} from "../validation/validate.js"

async function updatePlaceCount(savedPlace) {
  try {
      await pgdb
        .insert(placeCountTable)
        .values({
          placeId: savedPlace["placeId"],
          timestamp: savedPlace["insertedAt"],
          count: 1,
        })
        .onConflictDoUpdate({ target: placeCountTable.placeId, targetWhere: sql`${placeCountTable.placeId} = ${savedPlace}`, set: { count: sql`${placeCountTable.count}+ 1` } });
    
    console.log("Saved count value to Postgres successfully!");
  } catch (error) {
    console.log(
      "\n\bERROR: There was a problem while sending the count value to Postgres, check the message below ",
    );
    console.log(error);
  }
  
}

async function updateRouteCount(savedRoute) {
  try {
      await pgdb
        .insert(routeCountTable)
        .values({
          route_id: savedRoute["route_id"],
          timestamp: savedRoute["timestamp"],
          count: 1,
        })
        .onConflictDoUpdate({ target: routeCountTable.route_id, targetWhere: sql`${routeCountTable.route_id} = ${savedRoute}`, set: { count: sql`${routeCountTable.count}+ 1` } });
    
    console.log("Saved route count value to Postgres successfully!");
  } catch (error) {
    console.log(
      "\n\bERROR: There was a problem while sending the route count value to Postgres, check the message below ",
    );
    console.log(error);
  }
  
}

async function flushORSBuffer(orsMessage) {
  if (orsMessage["distance_km"] || orsMessage["duration_min"] != null) {
      orsMessage = {
        ...orsMessage,
        route_id:
          "Drv_" +
          (
            Math.abs(
              (Number(orsMessage["from_lng"]) +
                Number(orsMessage["from_lat"]) +
                Number(orsMessage["distance_km"])) *
                10000,
            ).toFixed(0) +
              Math.abs(
                (Number(orsMessage["to_lng"]) +
                  Number(orsMessage["to_lat"]) +
                  Number(orsMessage["duration_min"])) *
                  10000,
              ).toFixed(0)
          ),
          serial_id: crypto.randomUUID()
      };
  } else {
    orsMessage = { ...orsMessage, route_id: "Drv_" + "null_" + (
          Math.abs(
            (Number(orsMessage["from_lng"]) +
              Number(orsMessage["from_lat"])) *
              10000,
          ).toFixed(0) +
            Math.abs(
              (Number(orsMessage["to_lng"]) +
                Number(orsMessage["to_lat"])) *
                10000,
            ).toFixed(0)
        ),
      serial_id: crypto.randomUUID() };
  }
  console.log(orsMessage);
  updateRouteCount(orsMessage);

  try {
    await pgdb.insert(orsDataTable).values(orsMessage)
    console.log("Sent the ORS data to PostgreSQL!");
  } catch (error) {
    console.log(
     "\n\bERROR: There was a problem while sending the ORS data to PostgreSQL, check the message below",
    );
    console.log(error);
   
  }
}

async function flushExternalBuffer(extMessage) {
  try {
    await pgdb.insert(externalDataTable).values(extMessage)
    console.log("Saved the external location details to PostgreSQL");
  } catch (error) {
    console.log(
      "\n\bERROR: There was a problem while sending the external data to PostgreSQL, check the message below",
    );
    console.log(error);
  }
}

async function flushClickBuffer(clickMessage) {
  const insertedAt = Date.now().toString();
  let updatedClickMessage = {...clickMessage, insertedAt:insertedAt, serialID: crypto.randomUUID()}
  updatePlaceCount(updatedClickMessage);
  try {
    await pgdb.insert(clickResultTable).values(updatedClickMessage)
    console.log("Saved the clicked location details to PostgreSQL");
  } catch (error) {
    console.log(
      "\n\bERROR: There was a problem while sending the clicked option data to PostgreSQL, check the message below",
    );
    console.log(error);
  }
}

async function saveData(topic, messages) {
  if (topic == KAFKA_CONSUMERS[0]['TOPIC']) {
    const result = clickSchema.safeParse(messages);
    if (!result.success) {
      console.log(result.error);
    } else {
      flushClickBuffer(messages);
    }
  } else if (topic == KAFKA_CONSUMERS[1]['TOPIC']) {
    const result = externalSchema.safeParse(messages);
    if (!result.success) {
      console.log(result.error);
    } else {
      flushExternalBuffer(messages);
    }
  } else if (topic == KAFKA_CONSUMERS[2]['TOPIC']) {
    const result = orsSchema.safeParse(messages);
    if(!result.success){
      console.log(result.error);
    } else {
      flushORSBuffer(messages);
    }
  }
}

export { saveData };
