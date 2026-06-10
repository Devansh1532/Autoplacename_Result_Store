import 'dotenv/config'
import {defineConfig} from 'drizzle-kit'

// export default defineConfig({
//     out: './drizzle',
//     schema: './models/orm_schema.js',
//     dialect: 'mysql',
//     dbCredentials: {
//         host: process.env.MYSQLHOST ,
//         port: process.env.MYSQLPORT,
//         user: process.env.MYSQLUSER,
//         password: process.env.MYSQLPASSWORD,
//         database: process.env.MYSQLDATABASE,
//         // uri: process.env.DATABASE_URL,
//     }
// })

export default defineConfig({
    out: './drizzle',
    schema: './models/orm_schema.js',
    dialect: 'postgresql',
    dbCredentials: {
        host: process.env.PGHOST ,
        port: process.env.PGPORT,
        user: process.env.PGUSER,
        password: process.env.PGPASSWORD,
        database: process.env.PGDATABASE,
        ssl: 'require',
        // uri: process.env.DATABASE_URL,
    }
})