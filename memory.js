import { join } from "node:path";
import { createReadStream, readFileSync } from "node:fs";
import walk from "walk";
import { mapLimit } from "async";
import { parse } from "ini";
import * as csv from 'fast-csv';
import postgres from 'postgres'
import { getLock } from 'p-lock';
import Emittery from 'emittery';
import { parse as parseDate, format } from 'date-fns';

const emitter = new Emittery();
const config = parse(readFileSync("./config/config_basic.ini", "utf-8"));
const { host, port, user, password, database } = config.database;


import { open } from 'lmdb';

let calcdb = open({
  path: './db',
  compress: true
})

const lock = getLock();

function calculate(row, opt) {
  opt.fraud_count = opt.fraud_count + row.fraud_count;
  opt.tcpa_count = opt.tcpa_count + row.tcpa_count;
  opt.spam_count++
  if (row.first_fraud_on) {
    opt.first_fraud_on = !opt.first_fraud_on ? row.first_fraud_on : getFristDate(opt.first_fraud_on, row.first_fraud_on);
  }
  if (row.first_tcpa_on) {
    opt.first_tcpa_on = !opt.first_tcpa_on ? row.first_tcpa_on : getFristDate(opt.first_tcpa_on, row.first_tcpa_on);
  }
  if (row.last_fraud_on) {
    opt.last_fraud_on = opt.last_fraud_on ? getLastDate(opt.last_fraud_on, row.last_fraud_on) : row.last_fraud_on;
  }
  if (row.last_tcpa_on) {
    opt.last_tcpa_on = opt.last_tcpa_on ? getLastDate(opt.last_tcpa_on, row.last_tcpa_on) : row.last_tcpa_on;
  }
  if (row.last_spam_on) {
    opt.last_spam_on = opt.last_spam_on ? getLastDate(opt.last_spam_on, row.last_spam_on) : row.last_spam_on;
  }


  return opt;
}


let getLastDate = (a, b) => {
  let na = new Date(a), nb = new Date(b);
  return na > nb ? format(na, 'yyyy-MM-dd') : format(nb, 'yyyy-MM-dd');
}
let getFristDate = (a, b) => {
  let na = new Date(a), nb = new Date(b);
  return na < nb ? format(na, 'yyyy-MM-dd') : format(nb, 'yyyy-MM-dd');
}

let counter = {};
emitter.on('syncdb', async (data) => {
  let { fn, r } = data;
  let o = calcdb.get(r.number);
  if (o) {
    calcdb.putSync(r.number, calculate(r, o));
  } else {
    calcdb.putSync(r.number, r);
  }
  emitter.emit('log', { fn, r });
})
emitter.on('log', (data) => {
  let { fn, r } = data;
  lock('lock').then((release) => {
    counter[fn]++;
    console.log(`${fn} processed ${counter[fn]}, ${r.number}`)
    release();
  })
})

//
let processing = (fn, callback) => {
  counter[fn] = 0;
  let cdate = format(parseDate(fn.match(/\d{8}/)[0], 'yyyyMMdd', new Date()), 'yyyy-MM-dd');
  createReadStream(`${join(config.path.watchdir, fn)}`)
    .pipe(csv.parse({ headers: true }))
    .on('error', error => callback(error, null))
    .on('data', row => {
      let { SpamScore, FraudProbability, TCPAFraudProbability } = row;
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
        emitter.emit('syncdb', { fn, r });
      }
    })
    .on('end', async rs => {
      console.log(`${fn} processed ${rs} records`);
      callback(null, `${fn}:${rs}`);
    });
}
//



let files = [];
console.log('start app, pls wait...');
console.time('analysis work done in');
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
  mapLimit(files, 2, processing, async (err, result) => {
    console.timeEnd('analysis work done in')
    console.log('start populate data...');

    console.log(result);
    const sql = postgres({ host, port, user, password, database });
    for (let v of calcdb.getValues()) {
      console.log(v);
      await sql` insert into public.spams ${sql(v)} `;
    }
    calcdb.close();
    process.exit(0);
  });
})

