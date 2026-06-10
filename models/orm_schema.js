import {serial, integer, text, doublePrecision, pgTable} from 'drizzle-orm/pg-core'

const externalDataTable = pgTable("backupdata_table", {
  placeId: text().notNull().primaryKey(),
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
  serialId: text().notNull().primaryKey(),
  searchQuery: text().notNull(),
  timestamp: text().notNull(),
  placeId: text().notNull(),
  source: text().notNull(),
  insertedAt: text().notNull(),
});
const placeCountTable = pgTable("placecount_table", {
  placeId: text().primaryKey(),
  timestamp: text().notNull(),
  count: integer().notNull(),
});
const routeCountTable = pgTable("routecount_table", {
  route_id: text().primaryKey(),
  timestamp: text().notNull(),
  count: integer().notNull()

});
const orsDataTable = pgTable("orsdata_table", {
  serial_id: text().notNull().primaryKey(),
  route_id: text().notNull(),
  from_lat: doublePrecision().notNull(),
  from_lng: doublePrecision().notNull(),
  to_lat: doublePrecision().notNull(),
  to_lng: doublePrecision().notNull(),
  distance_km: doublePrecision(),
  duration_min: doublePrecision(),
  source: text(),
  error: text(),
  reason: text()
});

export  {externalDataTable, clickResultTable, placeCountTable, routeCountTable, orsDataTable};
