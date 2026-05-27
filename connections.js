import { drizzle } from "drizzle-orm/node-postgres";
import pg from "pg";
import "dotenv/config";

const { Client } = pg;


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

export {pgdb}
