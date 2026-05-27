import 'dotenv/config'
import {defineConfig} from 'drizzle-kit'

export default defineConfig({
    out: './drizzle',
    schema: './models/orm_schema.js',
    dialect: 'mysql',
    dbCredentials: {
        host: process.env.MYSQLHOST ,
        port: process.env.MYSQLPORT,
        user: process.env.MYSQLUSER,
        password: process.env.MYSQLPASSWORD,
        database: process.env.MYSQLDATABASE,
        // uri: process.env.DATABASE_URL,
    }
})