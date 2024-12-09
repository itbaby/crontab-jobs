import { createReadStream, readFileSync } from "node:fs";
import pino from "pino";
import walk from "walk";
import knex from "knex";
import { everySeries } from "async";
import { parse } from "ini";
import * as csv from 'fast-csv';
import { sortBy } from "lodash-es";
//config and init logger 
const config = parse(readFileSync("./config.ini", "utf-8"));
const logger = pino(pino.destination({ dest: './logs', sync: true }));
const { host, port, user, password, database } = config.database;
let client = knex({
  client: 'pg',
  connection: { host, port, user, password, database },
  pool: { min: 2, max: 10 }
});

let parseCsv2PG = (filePath, callback) => {
  let chunkRecs = [];
  const { name, mtime } = filePath;
  createReadStream(name)
    .pipe(csv.parse({ headers: true }))
    .on('error', error => callback(error, null))
    .on('data', row => {
      if (['MAYBE', 'PROBABLY', 'ALMOST_CERTAINLY', 'DEFINITELY'].includes(row['SpamScore'])) {
        chunkRecs.push({
          number: row['Number']
        });
      }
    })
    .on('end', rows => {
      client.batchInsert('public.spams', chunkRecs, 1000).returning('number').then((numbers) => {
        console.log(`Parsed ${numbers.length} rows, total ${rows} readed`);
        callback(null, { fn: filePath });
      }).catch((e) => {
        callback(e, null);
      });
    });

}


let files = []
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
  everySeries(sortBy(files, "mtime"), parseCsv2PG, (err, result) => {
    process.exit(0);
  });
})

