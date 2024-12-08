import { parse } from "ini";
import { readFileSync } from "node:fs";
import { DownloaderHelper } from "node-downloader-helper";
import urls from './config_download_urls.json' assert {type: "json"};
const config = parse(readFileSync("./config.ini", "utf-8"));
urls.map(url => {
  const downloader = new DownloaderHelper(url, config.path.downloadDir);
  downloader.on('end', () => {
    console.log('Download completed');
  });
  downloader.on('error', (err) => {
    console.error(err);
  })
  downloader.start();
})
