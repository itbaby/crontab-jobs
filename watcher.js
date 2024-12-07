import pino from "pino";
import { client } from 'pg';
import chokidar from "chokidar";
import retry from "async-retry";
import { format } from "date-fns";
import { createStream } from "rotating-file-stream";
//config and init logger 
//
const stream = createStream("logs.txt", { size: "1K", interval: "1d", path: "./logs" });
const logger = pino({ stream: stream });

const client = new client({
  user: "postgres",
  host: "localhost",
  database: "postgres",
  password: "postgres",
  port: 5432,
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


chokidar.watch('./').on('add', process).on('change', process);

