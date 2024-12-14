import mitt from 'mitt';
import { join } from "node:path";
import { createReadStream, readFileSync } from "node:fs";
import walk from "walk";
import { mapLimit } from "async";
import { parse } from "ini";
import * as csv from 'fast-csv';
import postgres from 'postgres'
import { has, unset, find } from "lodash-es";
import { Low, Memory } from 'lowdb';
import { JSONFile } from 'lowdb/node';
import { pool } from 'workerpool';
import { parse as parseDate, format } from 'date-fns';
const logs = new Low(new JSONFile(`logs.json`), { records: {} });

const worker = pool();

const emitter = mitt();
const config = parse(readFileSync("./config/config_basic.ini", "utf-8"));
const { host, port, user, password, database } = config.database;

function calculate(rows, fn, rs, logs) {
  let arr = {};
  for (let row of rows) {
    let opt = arr[row.number];
    if (opt) {
      opt.fraud_count = opt.fraud_count + row.fraud_count;
      opt.tcpa_count = opt.tcpa_count + row.tcpa_count;
      opt.spam_count++
      if (!opt.first_fraud_on && row.first_fraud_on) {
        opt.first_fraud_on = row.first_fraud_on;
      }
      if (!opt.first_tcpa_on && row.first_tcpa_on) {
        opt.first_tcpa_on = row.first_tcpa_on;
      }

      if (row.last_fraud_on) {
        opt.last_fraud_on = opt.last_fraud_on ? getLastDate(opt.last_fraud_on, row.last_fraud_on) : row.last_fraud_on;
      }
      if (row.last_tcpa_on) {
        opt.last_tcpa_on = opt.last_tcpa_on ? getLastDate(opt.last_tcpa_on, row.last_tcpa_on) : row.last_tcpa_on;
      }
    } else {
      arr[row.number] = row;
    }
    //    await logs.update(({ records }) => { records[fn] = Object.keys(arr).length; });
    console.log(`Processing ${fn}  ${row.number} , total ${Object.keys(arr).length}/${rs}`);
  }

  return arr;
}


let getLastDate = (a, b) => {
  let na = new Date(a), nb = new Date(b);
  return na > nb ? na : nb;
}
let getFristDate = (a, b) => {
  let na = new Date(a), nb = new Date(b);
  return na < nb ? na : nb;
}

emitter.on('syncdb', async (data) => {
  console.log('syncdb', data.rows.length)
  await logs.write();
})
//
let parseCsv2PG = (fn, callback) => {
  let rows = [];
  createReadStream(`${join(config.path.watchdir, fn)}`)
    .pipe(csv.parse({ headers: true }))
    .on('error', error => callback(error, null))
    .on('data', row => {
      let { SpamScore, FraudProbability, TCPAFraudProbability } = row;
      let cdate = format(parseDate(fn.match(/\d{8}/)[0], 'yyyyMMdd', new Date()), 'yyyy-MM-dd');
      let r = { number: row['Number'], spam_count: 0, fraud_count: 0, tcpa_count: 0 };

      if (FraudProbability > 0) {
        r.first_fraud_on = cdate;
        r.last_fraud_on = cdate;
        r.fraud_count = 1;
      }
      if (TCPAFraudProbability > 0) {
        r.first_tcpa_on = cdate;
        r.last_tcpa_on = cdate;
        r.tcpa_count = 1;
      }
      if (['MAYBE', 'PROBABLY', 'ALMOST_CERTAINLY', 'DEFINITELY'].includes(SpamScore)) {
        r.first_spam_on = cdate;
        r.last_spam_on = cdate;
        r.spam_count = 1;
        r.created_on = cdate;
        rows.push(r);
      }
    })
    .on('end', async rs => {
      await logs.read();
      let log = logs.data.records;
      let arr = {};
      if (log[fn]) {
        rows = rows.slice(log[fn])
      }
      console.log(`${fn} has ${rows.length} rows to process`);
      worker.exec(calculate, [rows, fn, rs, logs]).then((result) => {
        emitter.emit('syncdb', { rows: arr });
        callback(null, `${fn}:${rs}`);
      }).catch((error) => {
        console.log(error);
        callback(null, `${fn}:${rs}`);
      }).then(() => {
        worker.terminate();
      });
    });
}
//



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
  mapLimit(files, 2, parseCsv2PG, async (err, result) => {
    // const sql = postgres({ host, port, user, password, database });
    // for (const r of Object.values(recordb.data.records)) { await sql`insert into public.spams ${sql(r, Object.keys(r))}`; }
    console.log(err, err ?? 'all set')
    process.exit(0);
  });
})

