import { Worker } from '@temporalio/worker';
import * as activities from '../activities';
import * as path from 'path';

async function run() {
  const worker = await Worker.create({
    workflowsPath: path.resolve(__dirname, '../workflows'),
    activities,
    taskQueue: 'payments-bill',
  });

  console.log('🚀 Bill Worker started (task queue: payments-bill)');
  await worker.run();
}

run().catch((err) => {
  console.error('❌ Bill worker failed:', err);
  process.exit(1);
});
