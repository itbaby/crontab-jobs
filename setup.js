import { open } from 'lmdb';
import { parse } from "ini";
import postgres from 'postgres';
import { readFileSync } from "node:fs";
const config = parse(readFileSync("./config/config_basic.ini", "utf-8"));
const { host, port, user, password, database } = config.database;
let calcdb = open({path: './db', compress: true })
const sql = postgres({ host, port, user, password, database });
for (let v of calcdb.getValues()) {
    console.log(v)
    await sql` insert into public.spams ${sql(v)} `;
}