import { Worker } from '@temporalio/worker';
import * as activities from '../activities';
import * as path from 'path';

async function run() {
  const worker = await Worker.create({
    workflowsPath: path.resolve(__dirname, '../workflows'),
    activities,
    taskQueue: 'payments-p2p',
  });

  console.log('🚀 P2P Worker started (task queue: payments-p2p)');
  await worker.run();
}

run().catch((err) => {
  console.error('❌ P2P worker failed:', err);
  process.exit(1);
});
