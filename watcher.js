import { join } from "node:path";
import { createReadStream, readFileSync } from "node:fs";
import walk from "walk";
import knex from "knex";
import { everySeries, mapLimit } from "async";
import { parse } from "ini";
import * as csv from 'fast-csv';
import { omit, has } from "lodash-es";

import { LowSync } from 'lowdb';
import { JSONFileSync } from 'lowdb/node'

const cursordb = new LowSync(new JSONFileSync(`logs.json`), { records: {}, lstnum: '' });
const config = parse(readFileSync("./config/config_basic.ini", "utf-8"));
const { host, port, user, password, database } = config.database;
let client = knex({
  client: 'pg',
  connection: { host, port, user, password, database },
  pool: { min: 2, max: 10 }
});

let syncToDB = (row, callback) => {
  const { filename, number } = row;
  cursordb.update(({ records, lstnum }) => {
    if (!has(records, filename)) {
      records[filename] = 0;
    }
    records[filename]++;
    lstnum = number;
  })
  cursordb.write();
  row = omit(row, 'filename');
  client('public.spams').insert(row).then(result => {
    callback(null, { S: `${row}` });
  }).catch(error => {
    console.log(error)
    callback(null, { F: `${row}` });
  });
}


let parseCsv2PG = (filePath, callback) => {
  let chunkRecs = [];
  const { name, } = filePath;
  createReadStream(`${join(config.path.watchdir, name)}`)
    .pipe(csv.parse({ headers: true }))
    .on('error', error => callback(error, null))
    .on('data', row => {
      if (['MAYBE', 'PROBABLY', 'ALMOST_CERTAINLY', 'DEFINITELY'].includes(row['SpamScore'])) {
        chunkRecs.push({
          number: row['Number'],
          filename: name
        });
      }
    })
    .on('end', rows => {
      mapLimit(chunkRecs, 10, syncToDB, (err, result) => {
        callback(null, `${filePath} - ${result}`);
      })
    });
}

let files = [];
const walker = walk.walk(config.path.watchdir);
let handler = async (root, stats, next) => {
  const { name } = stats
  files.push(name)
  next();
}

walker.on("file", handler);
walker.on("end", () => {
  files = files.sort((a, b) => {
    return parseInt(a.match(/\d+/) || '0') - parseInt(b.match(/\d+/) || '0');
  })


  everySeries(files, parseCsv2PG, (err, result) => {
    process.exit(0);
  });
})

