// External dependencies
import { createReadStream, readFileSync } from "node:fs";
import pino from "pino";
import mitt from 'mitt';
import walk from "walk";
import knex from "knex";
import { parse } from "ini";
import * as csv from 'fast-csv';
import retry from "async-retry";
import { sortBy, remove } from "lodash-es";
import { DownloaderHelper } from "node-downloader-helper";
//config and init logger 
const config = parse(readFileSync("./config.ini", "utf-8"));
const logger = pino(pino.destination({ dest: './logs', sync: true }));
const emitter = mitt();
const { host, port, user, password, database } = config.database;
let client = knex({
  client: 'pg',
  connection: { host, port, user, password, database },
  pool: { min: 2, max: 10 }
});


let parseCsv2PG = (filePath, client) => {
  let chunkRecs = [];
  console.log('----------------', filePath);
  createReadStream(filePath)
    .pipe(csv.parse({ headers: true }))
    .on('error', error => console.error(error))
    .on('data', row => {
      if (['MAYBE', 'PROBABLY', 'ALMOST_CERTAINLY', 'DEFINITELY'].includes(row['SpamScore'])) {
        chunkRecs.push({
          number: row['Number']
        });
      }
    })
    .on('end', rows => {
      client.batchInsert('public.spams', chunkRecs, 1000).returning('number').then((numbers) => {
        console.log('-------------');
        console.log(`Parsed ${numbers.length} rows, total ${rows} readed`);
      }).catch((e) => {
        console.log(e);
      }).finally(() => {
        emitter.emit('end', { fn: filePath });
      });

    });

}


let files = []
let counter = 0;
const walker = walk.walk(config.path.watchdir);

let handler = async (root, stats, next) => {
  files.push({
    name: `${root}\\${stats.name}`,
    mtime: stats.mtime
  })
  next();
}

walker.on("file", handler);
walker.on("end", () => {
  files = sortBy(files, "mtime");
  files.map(({ name, mtime }) => {
    parseCsv2PG(name, client);
  })
})

emitter.on('end', (data) => {
  if (++counter == files.length) {
    process.exit(0);
  }
})

/*
console.log(config.path.downloadURL);

if (config.path.downloadURL) {
  const downloader = new DownloaderHelper(config.path.downloadURL, config.path.downloadDir);
  downloader.on('end', () => {
    logger.info('Download completed');
  });
  downloader.on('error', (err) => {

  })
  downloader.start();
}
*/
