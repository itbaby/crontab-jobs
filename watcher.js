import { join } from "node:path";
import { createReadStream, readFileSync } from "node:fs";
import walk from "walk";
import { everySeries } from "async";
import { parse } from "ini";
import * as csv from 'fast-csv';
import postgres from 'postgres'
import { has, takeRight } from "lodash-es";
import { Low } from 'lowdb';
import { JSONFile } from 'lowdb/node'
import { parse as parseDate, format, compareAsc, compareDesc } from 'date-fns';
const cursordb = new Low(new JSONFile(`logs.json`), { records: {} });
const config = parse(readFileSync("./config/config_basic.ini", "utf-8"));
const { host, port, user, password, database } = config.database;
const sql = postgres({ host, port, user, password, database });

let parseCsv2PG = (fn, callback) => {
  let recs = [];
  createReadStream(`${join(config.path.watchdir, fn)}`)
    .pipe(csv.parse({ headers: true }))
    .on('error', error => callback(error, null))
    .on('data', row => {
      let { SpamScore, FraudProbability, TCPAFraudProbability } = row;
      let rec = { number: row['Number'], spam_count: 0, fraud_count: 0, tcpa_count: 0 }, _date = format(parseDate(takeRight(fn.split(/[_|\.]/), 2).shift(), `yyyyMMdd`, new Date()), 'yyyy-MM-dd');
      if (FraudProbability > 0) {
        rec.first_fraud_on = _date;
        rec.last_fraud_on = _date;
        rec.fraud_count = 1;
      }
      if (TCPAFraudProbability > 0) {
        rec.first_tcpa_on = _date;
        rec.last_tcpa_on = _date;
        rec.tcpa_count = 1;
      }
      if (['MAYBE', 'PROBABLY', 'ALMOST_CERTAINLY', 'DEFINITELY'].includes(SpamScore)) {
        rec.first_spam_on = _date;
        rec.last_spam_on = _date;
        rec.spam_count = 1;
        rec.created_on = _date;
        recs.push(rec)
      }
    })
    .on('end', async rs => {
      await cursordb.read();
      let _rec = cursordb.data.records;
      if (has(_rec, fn) && _rec[fn] === rs) { callback(null, `${fn}:${rs}`); return; }
      await cursordb.update(({ records }) => { if (!has(records, fn)) { records[fn] = 0; } });
      console.log('Cointue to pocesse', fn);
      let processed = 0;
      for (const rec of recs) {
        if (_rec[fn] > 0 && processed++ < _rec[fn]) {
          console.log(`skip ${fn} ${rec.number}`);
          continue;
        }
        const olds = await sql`select * from public.spams where number = ${rec.number}`;
        if (olds.length == 0) {
          await sql` insert into public.spams ${sql(rec)} `;
        } else {
          let o = olds[0];
          const updater = {
            number: rec.number,
            spam_count: o.spam_count + 1,
            first_spam_on: format([o.first_spam_on, rec.first_spam_on].sort(compareAsc)[0], 'yyyy-MM-dd'),
            last_spam_on: format([o.last_spam_on, rec.last_spam_on].sort(compareDesc)[0], 'yyyy-MM-dd'),
            first_fraud_on: format([o.first_fraud_on, rec.first_fraud_on].sort(compareAsc)[0], 'yyyy-MM-dd'),
            last_fraud_on: format([o.last_fraud_on, rec.last_fraud_on].sort(compareDesc)[0], 'yyyy-MM-dd'),
            first_tcpa_on: format([o.first_tcpa_on, rec.first_tcpa_on].sort(compareAsc)[0], 'yyyy-MM-dd'),
            last_tcpa_on: format([o.last_tcpa_on, rec.last_tcpa_on].sort(compareDesc)[0], 'yyyy-MM-dd'),
            fraud_count: o.fraud_count + rec.fraud_count,
            tcpa_count: o.tcpa_count + rec.tcpa_count
          }
          await sql` update public.spams set ${sql(updater)}  where number = ${updater.number}`;
        }
        await cursordb.update(({ records }) => {
          records[fn] = records[fn] + 1;
          console.log(`Processing ${fn}  ${rec.number} , total ${records[fn]}/${recs.length}`);
        })
      }
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

