import { join } from "node:path";
import { createReadStream, readFileSync } from "node:fs";
import walk from "walk";
import knex from "knex";
import { everySeries } from "async";
import { parse } from "ini";
import * as csv from 'fast-csv';
import { has, takeRight } from "lodash-es";
import { LowSync } from 'lowdb';
import { JSONFileSync } from 'lowdb/node'
import { parse as parseDate, format } from 'date-fns';
const cursordb = new LowSync(new JSONFileSync(`logs.json`), { records: {} });
const config = parse(readFileSync("./config/config_basic.ini", "utf-8"));
const { host, port, user, password, database } = config.database;
let client = knex({
  client: 'pg',
  connection: { host, port, user, password, database },
  pool: { min: 2, max: 10 },
  acquireConnectionTimeout: 60 * 60 * 1000
});


let parseCsv2PG = (fn, callback) => {
  let recs = [];
  createReadStream(`${join(config.path.watchdir, fn)}`)
    .pipe(csv.parse({ headers: true }))
    .on('error', error => callback(error, null))
    .on('data', row => {
      let { SpamScore, FraudProbability, TCPAFraudProbability } = row;
      let rec = { number: row['Number'] }, _date = parseDate(takeRight(fn.split(/[_|\.]/), 2).shift(), `yyyyMMdd`, new Date());
      if (FraudProbability > 0) {
        rec.first_fraud_on = _date;
        rec.last_fraud_on = _date;
      }
      if (TCPAFraudProbability > 0) {
        rec.first_tcpa_on = _date;
        rec.last_tcpa_on = _date;
      }
      if (['MAYBE', 'PROBABLY', 'ALMOST_CERTAINLY', 'DEFINITELY'].includes(SpamScore)) {
        rec.first_spam_on = _date;
        rec.last_spam_on = _date;
        recs.push(rec)
      }
    })
    .on('end', async rs => {
      cursordb.read();
      let _rec = cursordb.data.records;
      if (has(_rec, fn) && _rec[fn] === rs) { callback(null, `${fn}:${rs}`); return; }
      cursordb.update(({ records }) => { if (!has(records, fn)) { records[fn] = 0; } });
      console.log('Cointue to pocesse', fn);
      await client.transaction(async (trx) => {
        try {
          recs.map(async rec => {

            let record = await trx('public.spams').where({ number: rec.number });
            console.log(record)
            console.log('>>')
          })
          //
          // await trx.batchInsert('public.spams', recs, 100)
          await trx.commit();
          cursordb.update(({ records }) => { records[fn] = recs.length; })
          callback(null, `${fn}:${recs.length}`);
        } catch (err) {
          cursordb.update(({ records }) => { records[fn] = 0; })
          await trx.rollback();
          callback(null, `${fn}:0`);
        }
      })
    });
}

let files = [];
const walker = walk.walk(config.path.watchdir);
let handler = async (_root, stats, next) => {
  files.push(stats.name)
  next();
}

walker.on("file", handler);
walker.on("end", () => {
  files = files.sort((a, b) => {
    return parseInt(a.match(/\d+/) || '0') - parseInt(b.match(/\d+/) || '0');
  })
  everySeries(files, parseCsv2PG, (err, result) => {
    console.log(err, result)
    process.exit(0);
  });
})

