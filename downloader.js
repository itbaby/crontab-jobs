import { parse } from "ini";
import { readFileSync } from "node:fs";
import { DownloaderHelper } from "node-downloader-helper";
import urls from './config/config_download_urls.json' assert {type: "json"};
const config = parse(readFileSync("./config/config_basic.ini", "utf-8"));
urls.map(url => {
  const downloader = new DownloaderHelper(url, config.path.downloadDir);
  downloader.on('end', () => {
    console.log(`${url} Download completed`);
  });
  downloader.on('error', (err) => {
    console.error(err);
  })
  downloader.on('progress', (progress) => {
    console.log(`${progress.name}, ${progress.progress.toFixed(2)}%, ${progress.speed} KB/s ...`);
  })
  downloader.start();
})
