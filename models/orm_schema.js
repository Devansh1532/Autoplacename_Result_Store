// import {int, mysqlTable, float, varchar, datetime}  from "drizzle-orm/mysql-core";
import {serial, integer, text, doublePrecision, pgTable} from 'drizzle-orm/pg-core'

// const externalDataTable = mysqlTable("backupData-Table", {
//   placeId: varchar({ length: 255 }).notNull().unique().primaryKey(),
//   timestamp: varchar({ length: 255 }).notNull(),
//   placenameEN: varchar({ length: 255 }),
//   placenameAR: varchar({ length: 255 }),
//   placenameES: varchar({ length: 255 }),
//   lat: float().notNull(),
//   lng: float().notNull(),
//   type: varchar({ length: 255 }),
//   iata: varchar({ length: 255 }),
//   postcode: varchar({ length: 255 }),
//   address: varchar({ length: 255 }),
//   source: varchar({ length: 255 }),
// });
// const clickResultTable = mysqlTable("clickResult-Table", {
//   searchQuery: varchar({ length: 255 }).notNull(),
//   timestamp: varchar({ length: 255 }).notNull(),
//   placeId: varchar({ length: 255 }).notNull(),
//   source: varchar({length:255}).notNull(),
//   insertedAt: varchar({ length: 255 }).notNull(),
// });
// const placeCountTable = mysqlTable("placeCount-Table", {
//   placeId: varchar({ length: 255 }).unique().primaryKey(),
//   timestamp: varchar({ length: 255 }).notNull(),
//   count: int().notNull(),
// });
// const routeCountTable = mysqlTable("routeCount-Table", {
//   route_id: varchar({length:255}).unique().primaryKey(),
//   timestamp: varchar({length:255}).notNull(),
//   count: int().notNull()

// });
// const orsDataTable = mysqlTable("orsData-Table", {
//   route_id: varchar({length:255}),
//   from_lat: float().notNull(),
//   from_lng: float().notNull(),
//   to_lat: float().notNull(),
//   to_lng: float().notNull(),
//   distance_km: float(),
//   duration_min: float()
// });

const externalDataTable = pgTable("backupdata_table", {
  placeId: text().notNull().unique().primaryKey(),
  timestamp: text().notNull(),
  placenameEN: text(),
  placenameAR: text(),
  placenameES: text(),
  lat: doublePrecision().notNull(),
  lng: doublePrecision().notNull(),
  type: text(),
  iata: text(),
  postcode: text(),
  address: text(),
  source: text(),
});
const clickResultTable = pgTable("clickresult_table", {
  searchQuery: text().notNull(),
  timestamp: text().notNull(),
  placeId: text().notNull(),
  source: text().notNull(),
  insertedAt: text().notNull(),
});
const placeCountTable = pgTable("placecount_table", {
  placeId: text().unique().primaryKey(),
  timestamp: text().notNull(),
  count: integer().notNull(),
});
const routeCountTable = pgTable("routecount_table", {
  route_id: text().unique().primaryKey(),
  timestamp: text().notNull(),
  count: integer().notNull()

});
const orsDataTable = pgTable("orsdata_table", {
  route_id: text().notNull(),
  from_lat: doublePrecision().notNull(),
  from_lng: doublePrecision().notNull(),
  to_lat: doublePrecision().notNull(),
  to_lng: doublePrecision().notNull(),
  distance_km: doublePrecision(),
  duration_min: doublePrecision(),
  source: text()
});

export  {externalDataTable, clickResultTable, placeCountTable, routeCountTable, orsDataTable};
