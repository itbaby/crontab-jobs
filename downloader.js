import { parse } from "ini";
import { readFileSync } from "node:fs";
import { DownloaderHelper } from "node-downloader-helper";
const config = parse(readFileSync("./config/config_basic.ini", "utf-8"));

function getDatesBetween(start, end) {
  const startDate = new Date(start.substring(0, 4), start.substring(4, 6) - 1, start.substring(6, 8));
  const endDate = new Date(end.substring(0, 4), end.substring(4, 6) - 1, end.substring(6, 8));
  const dates = [];
  let currentDate = new Date(startDate);

  while (currentDate <= endDate) {

    dates.push(`https://youmail.s3.us-east-1.amazonaws.com/FULL_spam-number-file_${currentDate.toISOString().substring(0, 10).replace(/-/g, '')}.csv`);
    currentDate.setDate(currentDate.getDate() + 1);
  }

  return dates;
}

const days = getDatesBetween('20240113', '20240114')


days.map(url => {
  try {
    const downloader = new DownloaderHelper(url, config.path.downloadDir, { retry: {maxRetries:10,delay:1000}, removeOnFail: true, resumeOnIncomplete: true,resumeOnIncompleteMaxRetry: 10 });
    downloader.on('end', () => {
      console.log(`${url} Download completed`);
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
  } catch (err) {
    console.log(`url : ${url} failed`);
  }
})

