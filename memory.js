import mitt from 'mitt';
import { join } from "node:path";
import { createReadStream, readFileSync } from "node:fs";
import walk from "walk";
import { mapLimit } from "async";
import { parse } from "ini";
import * as csv from 'fast-csv';
import postgres from 'postgres'
import { has, unset } from "lodash-es";
import { Low, Memory } from 'lowdb';
import { JSONFile } from 'lowdb/node'
import { parse as parseDate, format } from 'date-fns';

const emitter = mitt();
const config = parse(readFileSync("./config/config_basic.ini", "utf-8"));
const { host, port, user, password, database } = config.database;

const cursor = new Low(new JSONFile(`logs.json`), { records: {} });



emitter.on('syncdb', async (data) => {
  const sql = postgres({ host, port, user, password, database });
  for (let r of Object.values(data.rows)) {
    console.log('>>>>>>>>>>', data.fn)
    console.log(r)
    // await sql`insert into public.spams ${sql(r, Object.keys(r))}`.execute();
  }
})
//
let parseCsv2PG = (fn, callback) => {
  const rows = new Low(new Memory(), { records: [] });
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
        rows.data.records.push(r)
      }
    })
    .on('end', async rs => {
      const recordb = new Low(new Memory(), { records: {} });
      await rows.read();
      await recordb.read();
      await cursor.write();

      let _rows = rows.data.records;
      let _cursor = cursor.data.records;
      console.log(_cursor)
      console.log(`${fn} has ${rows.data.records.length} rows, still has ${_rows.length - (_cursor[fn] || 0)} rows to process`);

      for (const _rrow of _rows.slice(_cursor[fn] ?? 0)) {
        if (has(recordb.data.records, _rrow.number)) {
          let _drow = recordb.data.records[_rrow.number];
          unset(_rrow, 'first_spam_on');
          unset(_rrow, 'created_on');
          _drow.first_fraud_on ?? unset(_rrow, 'first_fraud_on');
          _drow.first_tcpa_on ?? unset(_rrow, 'first_tcpa_on');
          _rrow.fraud_count = _drow.fraud_count + _rrow.fraud_count;
          _rrow.tcpa_count = _drow.tcpa_count + _rrow.tcpa_count;
          _rrow.spam_count = _drow.spam_count + _rrow.spam_count;
          await recordb.update(({ records }) => {
            records[_rrow.number] = Object.assign(_drow, _rrow);
          });
        } else {
          await recordb.update(({ records }) => { records[_rrow.number] = _rrow; });
        }
        cursor.update(({ records }) => {
          records[fn] = records[fn] ? records[fn] + 1 : 1;
          console.log(`Processing ${fn}  ${_rrow.number} , total ${records[fn]}/${rs}`);
        });
      }
      await recordb.write();
      await cursor.write();
      emitter.emit('syncdb', { fn, rows: recordb.data.records });
      callback(null, `${fn}:${rs}`);
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

