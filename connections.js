import mongoose from "mongoose";
import mysql from "mysql2/promise";
// import {drizzle} from "drizzle-orm/mysql2"
import { drizzle } from "drizzle-orm/node-postgres";
import pg from "pg";
import "dotenv/config";

const { Client } = pg;

mongoose.set("strictQuery", true);
async function connectMongoDB(url) {
  try {
    const result = await mongoose.connect(url);
    return result;
  } catch (err) {
    return err;
  }
}

// const connection = await mysql.createConnection({
//   host: process.env.MYSQLHOST,
//   port: process.env.MYSQLPORT,
//   user: process.env.MYSQLUSER,
//   password: process.env.MYSQLPASSWORD,
//   database: process.env.MYSQLDATABASE,
// })
// const sqldb = drizzle(connection);

const client = new Client({
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  host: process.env.PGHOST,
  port: process.env.PGPORT,
  database: process.env.PGDATABASE,
  sslmode: "require",
  ssl: {
    rejectUnauthorized: false,
  },
});
async function connectPG(client) {
  try {
    await client.connect().then(() => {
      console.log("PostgreSQL connected");
    });
  } catch (err) {
      console.log(err);
  }
}
connectPG(client);
const pgdb = drizzle({ client: client });

// export { connectMongoDB, sqldb, pgdb };
export {connectMongoDB, pgdb}
