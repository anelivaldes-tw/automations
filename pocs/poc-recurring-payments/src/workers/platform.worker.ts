import { Worker } from '@temporalio/worker';
import * as activities from '../activities';
import * as path from 'path';

async function run() {
  const worker = await Worker.create({
    workflowsPath: path.resolve(__dirname, '../workflows'),
    activities,
    taskQueue: 'payments-platform',
  });

  console.log('🚀 Platform Worker started (task queue: payments-platform)');
  await worker.run();
}

run().catch((err) => {
  console.error('❌ Platform worker failed:', err);
  process.exit(1);
});
