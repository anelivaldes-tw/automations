import { Worker } from '@temporalio/worker';
import * as billActivities from '../activities/bill.activities';
import * as path from 'path';

async function run() {
  const worker = await Worker.create({
    workflowsPath: path.resolve(__dirname, '../workflows'),
    activities: billActivities,
    taskQueue: 'payments-bill',
  });

  console.log('🚀 Bill Worker started (task queue: payments-bill)');
  console.log('   Activities: validateBiller, executeCharge, publishBillEvent');
  await worker.run();
}

run().catch((err) => {
  console.error('❌ Bill worker failed:', err);
  process.exit(1);
});
