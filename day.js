"use strict";
import pgk from 'pg';
import axios from 'axios';
import { parse } from "ini";
import postgres from 'postgres';
import * as csv from 'fast-csv';
import { join } from "node:path";
import { QueryIterablePool } from 'pg-iterator';

import { createReadStream, readFileSync, createWriteStream } from "node:fs";
import { format, subDays } from 'date-fns';
import { DownloaderHelper } from "node-downloader-helper";

const { Pool } = pgk;
const config = parse(readFileSync("./config/config_basic.ini", "utf-8"));
const { host, port, user, password, database } = config.database;

const sql = postgres({ host, port, user, password, database });
let _date = format(subDays(new Date(), 1), 'yyyyMMdd');
let cdate = format(subDays(new Date(), 1), 'yyyy-MM-dd');
let today = `FULL_spam-number-file_${_date}.csv`
let dump = `Youmail_Aggregate_Report${_date}.csv`
async function msg(text) {
  try {
    const response = await axios.post(config.path.webhookurl, { text }, { headers: { 'Content-Type': 'application/json', } });
    console.log(`Log posted successfully: ${response.statusText}`);
  } catch (err) {
    console.error('Error posting log to Mattermost:', err.message);
  } finally {
    process.exit(0)
  }
}

async function backupdb() {
  console.log('start to backup data..., due to records too big , it will cost time and disk to bakup');
  const ws = createWriteStream(`${join(config.path.watchdir, dump)}`);
  const pool = new Pool({ host, port, user, password, database });
  const stream = csv.format({ headers: true, quote: ',' });
  try {
    const query = new QueryIterablePool(pool);
    const result = query.query(`SELECT * FROM public.spams`);
    for await (const u of result) {
      console.log(JSON.stringify(u));
      stream.pipe(ws);
      stream.write(u);
    }
  } finally {
    stream.end();
    pool.end();
  }
}

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


let processing = () => {
  createReadStream(`${join(config.path.watchdir, today)}`)
    .pipe(csv.parse({ headers: true }))
    .on('error', error => { console.log(error) })
    .on('data', async row => {
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
        const has = await sql`select * from public.spams where number = ${r.number}`;
        console.log(`processing ${r.number}`)
        if (has.length == 0) {
          await sql` insert into public.spams ${sql(r)} `;
        } else {
          let rec = calculate(r, has[0])
          await sql` update public.spams set ${sql(rec)}  where number = ${rec.number}`;
        }
      }
    })
    .on('end', async rs => {
      await backupdb();
      console.log(`finished parsing ${today} , and update db with ${rs} records already`)
      msg(`You mail file ${today} is successfully processed with ${rs} lines.  Dump full result to ${join(config.path.watchdir, dump)}`);
    });
}

let url = `https://youmail.s3.us-east-1.amazonaws.com/${today}`;
console.log(`start downling file : ${url}`);

const downloader = new DownloaderHelper(url, config.path.downloadDir, { override: true, retry: { maxRetries: 10, delay: 1000 }, removeOnFail: true, resumeOnIncomplete: true, resumeOnIncompleteMaxRetry: 10 });
downloader.on('end', () => {
  console.log(`${url} Download completed`);
  processing();
});
downloader.on('error', (err) => {
  console.error(err);
})
downloader.on('progress', (progress) => {
  console.log(`${progress.name}, ${progress.progress.toFixed(2)}%, ${parseInt(progress.speed / 2024)} MB/s ...`);
})
downloader.start().catch(err => {
  console.log(err)
});
