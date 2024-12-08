// External dependencies
import fs from "fs";

// Third-party packages
import { client } from 'pg';
import chokidar from "chokidar";
import { format } from "date-fns";
import ini from "ini";
import pino from "pino";
import retry from "async-retry";
import { createStream } from "rotating-file-stream";

//config and init logger 
//
const config = ini.parse(fs.readFileSync("./config.ini", "utf-8"));


const stream = createStream("logs.txt", { size: "1K", interval: "1d", path: "./logs" });
const logger = pino({ stream: stream });

const pgClient = new client({
  user: config.database.user,
  host: config.database.host,
  database: config.database.database,
  password: config.database.password,
  port: config.database.port,
});

let parseRec2DB = async (path) => {
  //parse record and insert to DB
  //do logger for success and error
}

let process = async (path) => {
  logger.error(`File ${path} was modified at ${format(Date.now(), "yyyy-MM-dd HH:mm:ss")}`);
  logger.flush();
  await retry(async (bail) => {
    parseRec2DB(path);
  }, { retries: 3 });
}

chokidar.watch(config.path.watchdir).on('add', process).on('change', process);
