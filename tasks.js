import { execa } from 'execa';
import { schedule } from 'node-cron';
console.log('start working...');
schedule('0 2 * * *', async () => {
  let { stdout } = await execa`node day.js`;
  console.log(stdout)
});
